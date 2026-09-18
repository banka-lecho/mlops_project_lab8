import json
import os

import pymssql
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from spark_session import SparkSession
from logger import get_logger
from utils import load_config

logger = get_logger(__name__)

CONFIG_TO_DB = {
    "energy-kcal_100g": "energy_kcal_100g",
    "saturated-fat_100g": "saturated_fat_100g",
}


class MsSqlDataSource:
    """Class for connection between DB amd model."""

    def __init__(self):
        self.config = load_config()
        db_config = self.config.database
        self.host = os.environ.get("MSSQL_HOST", db_config.host)
        self.port = os.environ.get("MSSQL_PORT", db_config.port)
        self.raw_schema = db_config.raw_schema
        self.ml_schema = db_config.ml_schema
        self.db_name = db_config.database
        self.num_partitions = db_config.num_partitions

    def _connection_opts(self) -> dict:
        return {
            "url": f"jdbc:sqlserver://{self.host}:{self.port};databaseName={self.db_name}"
            ";encrypt=true;trustServerCertificate=true",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "user": os.environ["MSSQL_USER"],
            "password": os.environ["MSSQL_PASSWORD"],
        }

    def write_processed(self, df: DataFrame) -> None:
        """Кладёт очищенные признаки в raw.processed_data."""
        out = df
        for cfg_name, db_name in CONFIG_TO_DB.items():
            out = out.withColumnRenamed(cfg_name, db_name)

        numeric_db = [CONFIG_TO_DB.get(c, c) for c in self.config.features.numeric]
        out = out.select("code", *numeric_db)

        (
            out.write.format("jdbc")
            .options(**self._connection_opts())
            .option("dbtable", f"{self.raw_schema}.processed_data")
            .option("batchsize", str(10000))
            .option("numPartitions", str(self.num_partitions or 1))
            .option("truncate", "true")
            .mode("overwrite")
            .save()
        )

    def _partition_opts(self, spark: SparkSession, table: str) -> dict:
        opts = {"fetchsize": "10000"}
        num_partitions = int(self.num_partitions or 1)
        if num_partitions <= 1:
            return opts

        bounds = self._read(
            spark, f"(SELECT MIN(product_id) lo, MAX(product_id) hi FROM {table}) b"
        ).first()
        if bounds is None or bounds["lo"] is None:
            return opts

        return opts | {
            "partitionColumn": "product_id",
            "lowerBound": str(bounds["lo"]),
            "upperBound": str(bounds["hi"]),
            "numPartitions": str(num_partitions),
        }

    def _read(self, spark: SparkSession, dbtable: str, **extra) -> DataFrame:
        return (
            spark.read.format("jdbc")
            .options(**self._connection_opts())
            .option("dbtable", dbtable)
            .options(**extra)
            .load()
        )

    def fetch_training_data(self, spark: SparkSession) -> DataFrame:
        """Стянуть данные."""
        table = f"{self.raw_schema}.processed_data"
        numeric_db = [CONFIG_TO_DB.get(c, c) for c in self.config.features.numeric]
        columns = ", ".join(["product_id", "code", *numeric_db])

        not_null = " AND ".join(f"{c} IS NOT NULL" for c in numeric_db)
        query = f"(SELECT {columns} FROM {table} WHERE {not_null}) AS t"

        df = self._read(spark, query, **self._partition_opts(spark, table))

        for db_name, cfg_name in CONFIG_TO_DB.items():
            df = df.withColumnRenamed(cfg_name, db_name)
        return df.drop("product_id")

    def _execute(self, sql: str, params: tuple) -> tuple | None:
        """Одиночный запрос вне Spark: JDBC-источник Spark не умеет возвращать сгенерированные ключи."""
        with pymssql.connect(
            server=self.host,
            port=self.port,
            user=os.environ["MSSQL_USER"],
            password=os.environ["MSSQL_PASSWORD"],
            database=self.db_name,
        ) as conn:
            with conn.cursor() as cursor:
                cursor.execute(sql, params)
                row = cursor.fetchone() if cursor.description else None
            conn.commit()
        return row

    def start_run(self, command: str, scaler_path: str, params: dict) -> int:
        """Регистрирует запуск в ml.model_runs со статусом RUNNING и возвращает выданный базой run_id."""
        row = self._execute(
            f"INSERT INTO {self.ml_schema}.model_runs "
            "(command, status, started_at, params, scaler_path) "
            "OUTPUT INSERTED.run_id "
            "VALUES (%s, 'RUNNING', SYSDATETIME(), %s, %s)",
            (command, json.dumps(params), scaler_path),
        )
        return int(row[0])

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
        """Закрывает запуск: статус, время окончания и итоговые метрики."""
        self._execute(
            f"UPDATE {self.ml_schema}.model_runs SET "
            "status = %s, finished_at = SYSDATETIME(), rows_in = %s, best_k = %s, "
            "best_silhouette = %s, params = COALESCE(%s, params), error_message = %s, "
            "scaler_path = COALESCE(%s, scaler_path) "
            "WHERE run_id = %s",
            (
                status,
                rows_in,
                best_k,
                best_silhouette,
                json.dumps(params) if params is not None else None,
                error_message[:1000] if error_message else None,
                scaler_path,
                run_id,
            ),
        )

    def get_train_run(self, run_id: int | None = None) -> dict:
        """Успешный запуск обучения с сохранённой моделью: указанный или последний."""
        row = self._execute(
            f"SELECT TOP 1 run_id, scaler_path, JSON_VALUE(params, '$.model_path') "
            f"FROM {self.ml_schema}.model_runs "
            "WHERE command = 'train' AND status = 'SUCCESS' "
            "AND JSON_VALUE(params, '$.model_path') IS NOT NULL "
            "AND (%s IS NULL OR run_id = %s) "
            "ORDER BY run_id DESC",
            (run_id, run_id),
        )
        if row is None:
            which = f"run_id={run_id}" if run_id else "ни одного"
            raise ValueError(
                f"Не найден успешный запуск обучения с сохранённой моделью ({which}) — "
                "запустите `python src/main.py train`"
            )
        return {"run_id": row[0], "scaler_path": row[1], "model_path": row[2]}

    def fetch_predictions(self, spark: SparkSession, run_id: int) -> DataFrame:
        """Предсказания запуска вместе с признаками товаров из raw.processed_data."""
        numeric_db = [CONFIG_TO_DB.get(c, c) for c in self.config.features.numeric]
        columns = ", ".join(["p.code", "p.cluster_id", *(f"d.{c}" for c in numeric_db)])
        query = (
            f"(SELECT {columns} FROM {self.ml_schema}.predictions p "
            f"JOIN {self.raw_schema}.processed_data d ON d.code = p.code "
            f"WHERE p.run_id = {int(run_id)}) AS t"
        )
        df = self._read(spark, query, fetchsize="10000")

        for db_name, cfg_name in CONFIG_TO_DB.items():
            df = df.withColumnRenamed(cfg_name, db_name)
        return df

    def save_predictions(self, predictions: DataFrame, run_id: int) -> None:
        """Дописывает кластеры товаров этого запуска в ml.predictions."""
        (
            predictions.select(
                F.lit(run_id).cast("int").alias("run_id"),
                "code",
                F.col("prediction").cast("int").alias("cluster_id"),
            )
            .write.format("jdbc")
            .options(**self._connection_opts())
            .option("dbtable", f"{self.ml_schema}.predictions")
            .option("batchsize", str(10000))
            .mode("append")
            .save()
        )
