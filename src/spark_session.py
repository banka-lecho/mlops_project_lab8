import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession

from logger import get_logger
from utils import SparkConfig

logger = get_logger(__name__)

CGROUP_MEMORY_LIMIT = Path("/sys/fs/cgroup/memory.max")

@dataclass(frozen=True)
class SparkResources:
    machine_cores: int
    machine_ram_gb: float
    cores: int
    driver_memory_gb: int


def memory_limit_gb() -> float | None:
    """Лимит памяти контейнера в ГБ или None, если его нет (ноутбук, контейнер без лимита)."""
    try:
        value = CGROUP_MEMORY_LIMIT.read_text().strip()
    except OSError:
        return None
    return None if value == "max" else int(value) / 2**30


def plan_resources(config: SparkConfig) -> SparkResources:
    """Определяет ядра и доступную память и решает, сколько отдать Spark."""
    ram_gb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**30
    limit_gb = memory_limit_gb()
    if limit_gb is not None:
        ram_gb = min(ram_gb, limit_gb)
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
    """SparkSession: local mode под выделенные ресурсы или кластер k8s, если задан SPARK_MASTER."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    shuffle_partitions = resources.cores * config.shuffle_partitions_per_core
    master = os.environ.get("SPARK_MASTER", f"local[{resources.cores}]")

    spark = (
        SparkSession.builder.appName(config.app_name)
        .master(master)
        .config("spark.driver.memory", f"{resources.driver_memory_gb}g")
        .config("spark.sql.shuffle.partitions", str(shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel(config.log_level)

    logger.info(
        "Spark %s: %s, driver memory %dg (доступно: %d ядер, %.1f ГБ RAM)",
        spark.version,
        master,
        resources.driver_memory_gb,
        resources.machine_cores,
        resources.machine_ram_gb,
    )
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Останавливает сессию. SPARK_UI_HOLD_SEC держит Spark UI открытым для демонстрации."""
    hold_sec = int(os.environ.get("SPARK_UI_HOLD_SEC", "0"))
    if hold_sec > 0:
        logger.info("Spark UI открыт ещё %d с (SPARK_UI_HOLD_SEC)", hold_sec)
        time.sleep(hold_sec)
    spark.stop()
