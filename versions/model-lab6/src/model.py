import json
import random
from pathlib import Path

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, StandardScalerModel, VectorAssembler

from logger import get_logger
from spark_session import create_spark, plan_resources, stop_spark
from datasource import MsSqlDataSource
from utils import load_config

logger = get_logger(__name__)


class ModelKMEANS:
    """PySpark KMeans model."""

    def __init__(self):
        self.config = load_config()

    def get_best_model(self, data):
        """Choose best model with k."""
        scores = []

        evaluator = ClusteringEvaluator(
            predictionCol="prediction",
            featuresCol="scaledFeatures",
            metricName="silhouette",
            distanceMeasure="squaredEuclidean",
        )

        best_k, best_score, best_model = None, -1.0, None
        for i in range(self.config.model.k_min, self.config.model.k_max):
            kmeans = KMeans(featuresCol="scaledFeatures", k=i)
            model = kmeans.fit(data)
            predictions = model.transform(data)
            score = evaluator.evaluate(predictions)
            scores.append({"k": i, "silhouette": score})
            print("Silhouette Score for k =", i, "is", score)
            if best_score < score:
                best_k, best_score, best_model = i, score, model

        return best_k, best_score, best_model, scores

    def save_results(
        self, best_k, best_score, best_model, scaler_model, predictions, scores
    ):
        """Сохраняет обученную модель, скейлер, предсказания по образцам и отчёт с метриками."""
        model_path = self.config.model.model_path
        scaler_path = self.config.model.scaler_path
        predictions_path = self.config.model.predictions_path
        metrics_path = Path(self.config.model.metrics_path)

        best_model.write().overwrite().save(model_path)
        logger.info("Модель сохранена в %s", model_path)

        scaler_model.write().overwrite().save(scaler_path)
        logger.info("Скейлер сохранён в %s", scaler_path)

        predictions.select(*self.config.features.meta, "prediction").write.mode(
            "overwrite"
        ).parquet(predictions_path)
        logger.info("Предсказания сохранены в %s", predictions_path)

        cluster_sizes = (
            predictions.groupBy("prediction").count().orderBy("prediction").collect()
        )

        report = {
            "silhouette_by_k": scores,
            "best_k": best_k,
            "best_silhouette": best_score,
            "cluster_sizes": {
                str(row["prediction"]): row["count"] for row in cluster_sizes
            },
            "cluster_centers": [
                center.tolist() for center in best_model.clusterCenters()
            ],
            "features_order": self.config.features.numeric,
        }
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        logger.info("Отчёт с метриками сохранён в %s", metrics_path)

    def train(self):
        """Train KMeans model."""
        ds = MsSqlDataSource()
        resources = plan_resources(self.config.spark)
        spark = create_spark(self.config.spark, resources)

        params = {
            "k_min": self.config.model.k_min,
            "k_max": self.config.model.k_max,
            "features": self.config.features.numeric,
        }
        run_id = ds.start_run("train", self.config.model.scaler_path, params)
        logger.info("Запуск обучения зарегистрирован: run_id=%d", run_id)
        try:
            numeric_features = self.config.features.numeric
            vec_assembler = VectorAssembler(
                inputCols=numeric_features, outputCol="features"
            )
            scaler = StandardScaler(
                inputCol="features",
                outputCol="scaledFeatures",
                withStd=True,
                withMean=False,
            )
            df = ds.fetch_training_data(spark)
            if df.isEmpty():
                raise ValueError(
                    "raw.processed_data пуста — сначала запустите `python src/main.py preprocess`"
                )

            final_data = vec_assembler.transform(df)
            final_data.select("features").show(5)

            scalerModel = scaler.fit(final_data)
            final_data = scalerModel.transform(final_data)
            final_data.select("scaledFeatures").show(5)

            best_k, best_score, best_model, scores = self.get_best_model(final_data)
            predictions = best_model.transform(final_data)

            model_path = f"{self.config.model.model_path}/run_{run_id}"
            scaler_path = f"{self.config.model.scaler_path}/run_{run_id}"
            best_model.write().overwrite().save(model_path)
            scalerModel.write().overwrite().save(scaler_path)
            logger.info("Модель сохранена в %s, скейлер в %s", model_path, scaler_path)

            ds.save_predictions(predictions, run_id)
            ds.finish_run(
                run_id,
                "SUCCESS",
                rows_in=df.count(),
                best_k=best_k,
                best_silhouette=best_score,
                params=params | {"silhouette_by_k": scores, "model_path": model_path},
                scaler_path=scaler_path,
            )
            logger.info(
                "Запуск %d завершён: best_k=%d, silhouette=%.4f",
                run_id,
                best_k,
                best_score,
            )
        except Exception as exc:
            logger.exception("Запуск %d упал", run_id)
            ds.finish_run(run_id, "FAILED", error_message=repr(exc))
            raise
        finally:
            stop_spark(spark)

    def predict(self, predictions_path, train_run_id=None, df=None):
        """Применяет модель запуска обучения (по умолчанию последнего успешного) к данным из БД."""
        ds = MsSqlDataSource()
        train_run = ds.get_train_run(train_run_id)
        logger.info("Использую модель запуска обучения run_id=%d", train_run["run_id"])

        resources = plan_resources(self.config.spark)
        spark = create_spark(self.config.spark, resources)

        params = {"train_run_id": train_run["run_id"], "model_path": train_run["model_path"]}
        run_id = ds.start_run("predict", train_run["scaler_path"], params)
        logger.info("Запуск предсказания зарегистрирован: run_id=%d", run_id)
        try:
            if df is None:
                df = ds.fetch_training_data(spark)
            if df.isEmpty():
                raise ValueError(
                    "Нет данных для предсказания — сначала запустите `python src/main.py preprocess`"
                )

            vec_assembler = VectorAssembler(
                inputCols=self.config.features.numeric, outputCol="features"
            )
            scaler_model = StandardScalerModel.load(train_run["scaler_path"])
            kmeans_model = KMeansModel.load(train_run["model_path"])

            features_df = vec_assembler.transform(df)
            scaled_df = scaler_model.transform(features_df)
            predictions = kmeans_model.transform(scaled_df)

            ds.save_predictions(predictions, run_id)
            ds.finish_run(
                run_id, "SUCCESS", rows_in=df.count(), best_k=kmeans_model.getK()
            )

            result = ds.fetch_predictions(spark, run_id)
            result.groupBy("cluster_id").count().orderBy("cluster_id").show()
            result.show(5)

            result.write.mode("overwrite").parquet(predictions_path)
            logger.info("Запуск %d завершён, предсказания выгружены в %s", run_id, predictions_path)
        except Exception as exc:
            logger.exception("Запуск %d упал", run_id)
            ds.finish_run(run_id, "FAILED", error_message=repr(exc))
            raise
        finally:
            stop_spark(spark)
