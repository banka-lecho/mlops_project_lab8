import json
import os
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from logger import get_logger
from utils import load_config

logger = get_logger(__name__)

STATUS = "status"
DATA = "data"
ERROR = "error"
STATUS_OK = "ok"


class DataMartClient:
    """Единственный способ модели получить данные и отдать результаты: HTTP-запросы к витрине."""

    def __init__(self, base_url: str | None = None):
        config = load_config().datamart
        url = base_url or os.environ.get("DATAMART_URL") or config.url
        self.base_url = url.rstrip("/")
        self.timeout_sec = config.timeout_sec
        self.startup_retries = config.startup_retries

    def start_run(self, command: str, scaler_path: str, params: dict) -> int:
        """Регистрирует запуск и возвращает выданный базой run_id."""
        data = self._post(
            "/v1/runs/start",
            {"command": command, "scalerPath": scaler_path, "params": params},
        )
        return int(data["runId"])

    def finish_run(
        self,
        run_id: int,
        status: str,
        rows_in: int | None = None,
        best_k: int | None = None,
        best_silhouette: float | None = None,
        params: dict | None = None,
        error_message: str | None = None,
        scaler_path: str | None = None,
    ) -> None:
        """Закрывает запуск: статус, метрики и путь к скейлеру."""
        self._post(
            "/v1/runs/finish",
            {
                "runId": run_id,
                "status": status,
                "rowsIn": rows_in,
                "bestK": best_k,
                "bestSilhouette": best_silhouette,
                "params": params,
                "errorMessage": error_message,
                "scalerPath": scaler_path,
            },
        )

    def get_train_run(self, run_id: int | None = None) -> dict:
        """Запуск обучения, чью модель можно применить: указанный или последний успешный."""
        data = self._post("/v1/runs/train", {"runId": run_id})
        return {
            "run_id": int(data["runId"]),
            "model_path": data["modelPath"],
            "scaler_path": data["scalerPath"],
        }

    def get_features(
        self, spark: SparkSession, limit: int | None = None
    ) -> tuple[DataFrame, list[str]]:
        """Предобработанные признаки от витрины: DataFrame и список колонок-признаков."""
        data = self._post("/v1/features", {"limit": limit})
        feature_cols = data["featureCols"]
        logger.info("Витрина отдала %d строк, признаки: %s", data["rowCount"], feature_cols)
        return self._to_dataframe(spark, data["rows"], feature_cols), feature_cols

    def save_predictions(self, predictions: DataFrame, run_id: int) -> int:
        """Отправляет витрине кластер каждого товара: колонки code и prediction."""
        rows = [
            {"code": row["code"], "cluster_id": int(row["prediction"])}
            for row in predictions.select("code", "prediction").toLocalIterator()
        ]
        data = self._post("/v1/predictions", {"runId": run_id, "rows": rows})
        return int(data["saved"])

    def get_predictions(self, spark: SparkSession, run_id: int) -> DataFrame:
        """Сохранённые предсказания запуска вместе с признаками."""
        data = self._post("/v1/predictions/get", {"runId": run_id})
        schema = self._schema(data["featureCols"], with_cluster=True)
        return self._to_dataframe(spark, data["rows"], data["featureCols"], schema)

    def _post(self, path: str, payload: dict) -> dict:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(
            f"{self.base_url}{path}",
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )

        last_error = None
        for attempt in range(self.startup_retries):
            try:
                with urlopen(request, timeout=self.timeout_sec) as response:
                    return self._unpack(json.loads(response.read().decode("utf-8")))
            except HTTPError as error:  # витрина ответила ошибкой — тело тоже в формате протокола
                self._unpack(json.loads(error.read().decode("utf-8")))
            except URLError as error:  # витрина ещё не поднялась
                last_error = error
                if attempt == 0:
                    logger.info("Жду витрину на %s", self.base_url)
                time.sleep(2)

        raise RuntimeError(f"Витрина данных недоступна: {self.base_url} ({last_error})")

    @staticmethod
    def _unpack(parsed: dict) -> dict:
        """Разворачивает единый формат витрины: data при ok, иначе исключение с текстом ошибки."""
        if parsed.get(STATUS) != STATUS_OK:
            raise RuntimeError(f"Витрина данных: {parsed.get(ERROR, 'неизвестная ошибка')}")
        return parsed[DATA]

    @staticmethod
    def _schema(feature_cols: list[str], with_cluster: bool = False) -> StructType:
        fields = [StructField("code", StringType(), False)]
        if with_cluster:
            fields.append(StructField("cluster_id", IntegerType(), False))
        fields += [StructField(c, DoubleType(), True) for c in feature_cols]
        return StructType(fields)

    def _to_dataframe(
        self,
        spark: SparkSession,
        rows: list[dict],
        feature_cols: list[str],
        schema: StructType | None = None,
    ) -> DataFrame:
        schema = schema or self._schema(feature_cols)
        names = [field.name for field in schema.fields]
        return spark.createDataFrame([[row[name] for name in names] for row in rows], schema)
