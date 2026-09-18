import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SparkConfig:
    app_name: str
    ram_fraction: float
    driver_memory_min_gb: int
    driver_memory_max_gb: int
    cores: int
    shuffle_partitions_per_core: int
    log_level: str


@dataclass(frozen=True)
class ModelConfig:
    model_path: str
    scaler_path: str
    predictions_path: str
    k_min: int
    k_max: int


@dataclass(frozen=True)
class DataMartConfig:
    url: str
    timeout_sec: int
    startup_retries: int


@dataclass(frozen=True)
class Config:
    spark: SparkConfig
    model: ModelConfig
    datamart: DataMartConfig


def load_config(config_path: str | None = None) -> Config:
    """Загрузка конфига из config.json."""
    path = (
        Path(config_path)
        if config_path
        else Path(__file__).resolve().parent / "config.json"
    )

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    return Config(
        spark=SparkConfig(**data["spark"]),
        model=ModelConfig(**data["model"]),
        datamart=DataMartConfig(**data["datamart"]),
    )
