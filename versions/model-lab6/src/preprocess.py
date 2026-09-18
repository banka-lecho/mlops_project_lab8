import json
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

from datasource import MsSqlDataSource
from logger import get_logger
from spark_session import RESERVED_MEMORY_MB, create_spark, plan_resources
from utils import load_config

logger = get_logger(__name__)

CleaningStep = tuple[str, Callable[[DataFrame], DataFrame]]


@dataclass(frozen=True)
class SampleSize:
    """Data class for DF sample."""

    total_rows: int
    target_rows: int
    limited_by: str


class PreProcessor:
    """Class for data preprocessing."""

    def __init__(self, datasource: MsSqlDataSource):
        self.datasource = datasource

    @staticmethod
    def read_raw(spark: SparkSession, path: str, columns: list[str]) -> DataFrame:
        """Читает только нужные колонки."""
        df = (
            spark.read.option("header", True)
            .option("sep", "\t")
            .option("mode", "PERMISSIVE")
            .csv(path)
        )
        return df.select(*columns)

    @staticmethod
    def cast_numeric(df: DataFrame, numeric_features: list[str]) -> DataFrame:
        """Кастуем к даблам"""
        for c in numeric_features:
            df = df.withColumn(c, F.col(c).cast("double"))
        return df

    @staticmethod
    def cleaning_steps(features: list[str]) -> list[CleaningStep]:
        """Шаги очистки для признаков вида _100g.
        Отсекаем не только пропуски, но и физически невозможные значения:
        отрицательные значения, сахар не может быть больше углеводов, БЖУ не может суммарно превышать 100г.
        """

        def drop_nulls(df):
            return df.dropna(subset=features)

        def drop_negatives(df):
            cond = F.lit(True)
            for c in features:
                cond = cond & (F.col(c) >= 0)
            return df.filter(cond)

        def drop_all_zero(df):
            nonzero = F.lit(False)
            for c in features:
                nonzero = nonzero | (F.col(c) > 0)
            return df.filter(nonzero)

        def drop_sugar_over_carbs(df):
            return df.filter(F.col("sugars_100g") <= F.col("carbohydrates_100g"))

        def drop_macro_over_100(df):
            macro_sum = (
                F.col("fat_100g") + F.col("carbohydrates_100g") + F.col("proteins_100g")
            )
            return df.filter(macro_sum <= 100)

        return [
            (
                "duplicates",
                lambda df: df.filter(F.col("code").isNotNull() & (F.length("code") <= 64))
                .dropDuplicates(["code"]),
            ),
            ("nulls", drop_nulls),
            ("negative", drop_negatives),
            ("zeros", drop_all_zero),
            ("sugar_gt_carbs", drop_sugar_over_carbs),
            ("macro_sum", drop_macro_over_100),
        ]

    @staticmethod
    def clean(df: DataFrame, steps: list[CleaningStep]):
        """Прогоняет df через шаги очистки, считая статистику."""
        df = df.cache()
        stats = [{"step": "raw", "rows": df.count()}]
        for name, step in steps:
            df = step(df)
            stats.append({"step": name, "rows": df.count()})
        return df, stats

    @staticmethod
    def storage_memory_mb(
        driver_memory_gb: float, memory_fraction: float, storage_fraction: float
    ) -> int:
        """Сколько памяти driver'а Spark резервирует под кэш (spark.memory.fraction/storageFraction)."""
        usable_mb = driver_memory_gb * 1024 - RESERVED_MEMORY_MB
        return int(usable_mb * memory_fraction * storage_fraction)

    def sample_size(
        self,
        total_rows: int,
        num_features: int,
        resources: dict,
        storage_mb: float,
        rows_per_core: int,
        copies_in_memory: int,
        memory_overhead: int,
    ) -> SampleSize:
        """Берёт минимум из: сколько строк реально есть, что укладывается по CPU-времени
        и что влезает в память кэша (num_features * 8 байт(double) * копии * оверхед на строку)."""
        cpu_target = rows_per_core * resources.machine_cores
        bytes_per_row = num_features * 8 * copies_in_memory * memory_overhead
        memory_target = int(storage_mb * 2**20) // bytes_per_row

        target_rows = min(total_rows, cpu_target, memory_target)
        if target_rows == total_rows:
            limited_by = "все чистые данные"
        elif target_rows == cpu_target:
            limited_by = "CPU"
        else:
            limited_by = "память"

        return SampleSize(
            total_rows=total_rows, target_rows=target_rows, limited_by=limited_by
        )

    @staticmethod
    def draw_sample(df: DataFrame, size: SampleSize, seed: int) -> DataFrame:
        """Берем сэмпл"""
        if size.target_rows >= size.total_rows:
            return df
        fraction = min(1.0, size.target_rows / size.total_rows * 1.1)
        return df.sample(fraction=fraction, seed=seed).limit(size.target_rows)

    def run(self):
        """Основной запуск."""
        config = load_config()
        resources = plan_resources(config.spark)
        spark = create_spark(config.spark, resources)

        numeric_features = config.features.numeric
        meta_columns = config.features.meta
        columns = meta_columns + numeric_features

        raw_path = config.data.raw_path
        interim_path = config.data.interim_path
        report_path = Path(config.data.report_path)

        logger.info("Читаю %s, оставляю %d из колонок файла", raw_path, len(columns))
        df = self.read_raw(spark, raw_path, columns)
        df = self.cast_numeric(df, numeric_features)
        df.write.mode("overwrite").parquet(interim_path)
        logger.info("Промежуточные данные сохранены в %s", interim_path)

        df = spark.read.parquet(interim_path)

        steps = self.cleaning_steps(numeric_features)
        cleaned, stats = self.clean(df, steps) 
        for row in stats:
            logger.info("%s: %d строк", row["step"], row["rows"])

        total_rows = stats[-1]["rows"]
        memory_fraction = float(spark.conf.get("spark.memory.fraction", "0.6"))
        storage_fraction = float(spark.conf.get("spark.memory.storageFraction", "0.5"))
        storage_mb = self.storage_memory_mb(
            resources.driver_memory_gb, memory_fraction, storage_fraction
        )

        size = self.sample_size(
            total_rows,
            len(numeric_features),
            resources,
            storage_mb=storage_mb,
            rows_per_core=config.sampling.rows_per_core,
            copies_in_memory=config.sampling.copies_in_memory,
            memory_overhead=config.sampling.memory_overhead,
        )

        logger.info(
            "Итоговая выборка: %d из %d строк (ограничение: %s)",
            size.target_rows,
            size.total_rows,
            size.limited_by,
        )

        sample = self.draw_sample(cleaned, size, seed=config.sampling.seed)
        self.datasource.write_processed(sample)
        logger.info("Данные записаны в %s.processed_data", config.database.raw_schema)

        report = {
            "cleaning": stats,
            "sample": {
                "total_rows": size.total_rows,
                "target_rows": size.target_rows,
                "limited_by": size.limited_by,
            },
        }
        report_path.parent.mkdir(parents=True, exist_ok=True)
        with open(report_path, "w", encoding="utf-8") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)

        spark.stop()
