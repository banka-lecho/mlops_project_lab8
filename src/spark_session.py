import os
import sys
from dataclasses import dataclass

from pyspark.sql import SparkSession

from logger import get_logger
from utils import SparkConfig

logger = get_logger(__name__)

@dataclass(frozen=True)
class SparkResources:
    machine_cores: int
    machine_ram_gb: float
    cores: int
    driver_memory_gb: int


def plan_resources(config: SparkConfig) -> SparkResources:
    """Определяет ядра и память машины и решает, сколько отдать Spark."""
    ram_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    machine_cores = os.cpu_count() or 1

    cores = config.cores or machine_cores
    driver_memory_gb = ram_gb * config.ram_fraction
    driver_memory_gb = max(driver_memory_gb, config.driver_memory_min_gb)
    driver_memory_gb = min(driver_memory_gb, config.driver_memory_max_gb)

    return SparkResources(
        machine_cores=machine_cores,
        machine_ram_gb=round(ram_gb, 1),
        cores=cores,
        driver_memory_gb=int(driver_memory_gb),
    )


def create_spark(config: SparkConfig, resources: SparkResources) -> SparkSession:
    """SparkSession в local mode под выделенные ресурсы."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    shuffle_partitions = resources.cores * config.shuffle_partitions_per_core

    spark = (
        SparkSession.builder.appName(config.app_name)
        .master(f"local[{resources.cores}]")
        .config("spark.driver.memory", f"{resources.driver_memory_gb}g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.log_level)

    logger.info(
        "Spark %s: local[%d], driver memory %dg (машина: %d ядер, %.1f ГБ RAM)",
        spark.version,
        resources.cores,
        resources.driver_memory_gb,
        resources.machine_cores,
        resources.machine_ram_gb,
    )
    return spark
