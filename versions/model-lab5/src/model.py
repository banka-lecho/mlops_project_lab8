import json
from pathlib import Path

from pyspark.ml.clustering import KMeans, KMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml.feature import StandardScaler, StandardScalerModel, VectorAssembler

from logger import get_logger
from spark_session import create_spark, plan_resources, stop_spark
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
        try:
            resources = plan_resources(self.config.spark)
            spark = create_spark(self.config.spark, resources)

            sample_path = self.config.data.sample_path
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
            df = spark.read.parquet(sample_path)

            final_data = vec_assembler.transform(df)
            final_data.select("features").show(5)

            scalerModel = scaler.fit(final_data)
            final_data = scalerModel.transform(final_data)
            final_data.select("scaledFeatures").show(5)

            best_k, best_score, best_model, scores = self.get_best_model(final_data)
            predictions = best_model.transform(final_data)
            self.save_results(
                best_k, best_score, best_model, scalerModel, predictions, scores
            )
        finally:
            stop_spark(spark)

    def predict(self, predictions_path, df=None):
        """Применяет уже обученную модель, сохраняет предсказания и останавливает сессию."""
        try:
            resources = plan_resources(self.config.spark)
            spark = create_spark(self.config.spark, resources)

            if df is None:
                df = spark.read.parquet(self.config.data.sample_path)

            vec_assembler = VectorAssembler(
                inputCols=self.config.features.numeric, outputCol="features"
            )
            scaler_model = StandardScalerModel.load(self.config.model.scaler_path)
            kmeans_model = KMeansModel.load(self.config.model.model_path)

            features_df = vec_assembler.transform(df)
            scaled_df = scaler_model.transform(features_df)
            predictions = kmeans_model.transform(scaled_df)

            result = predictions.select(*self.config.features.meta, "prediction")
            result.show(5)

            result.write.mode("overwrite").parquet(predictions_path)
            logger.info("Предсказания сохранены в %s", predictions_path)
        finally:
            stop_spark(spark)
