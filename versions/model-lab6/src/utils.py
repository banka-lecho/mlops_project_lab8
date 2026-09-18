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
class DataConfig:
    raw_path: str
    interim_path: str
    sample_path: str
    report_path: str


@dataclass(frozen=True)
class FeaturesConfig:
    numeric: list[str]
    meta: list[str]


@dataclass(frozen=True)
class CleaningConfig:
    energy_abs_tol_kcal: float
    energy_rel_tol: float


@dataclass(frozen=True)
class SamplingConfig:
    seed: int
    rows_per_core: int
    copies_in_memory: int
    memory_overhead: int


@dataclass(frozen=True)
class ModelConfig:
    model_path: str
    scaler_path: str
    predictions_path: str
    metrics_path: str
    k_min: int
    k_max: int


@dataclass(frozen=True)
class DBConfig:
    host: str
    port: str
    database: str
    raw_schema: str
    ml_schema: str
    num_partitions: int
    tables: list[str]


@dataclass(frozen=True)
class Config:
    spark: SparkConfig
    data: DataConfig
    features: FeaturesConfig
    cleaning: CleaningConfig
    sampling: SamplingConfig
    model: ModelConfig
    database: DBConfig


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
        data=DataConfig(**data["data"]),
        features=FeaturesConfig(**data["features"]),
        cleaning=CleaningConfig(**data["cleaning"]),
        sampling=SamplingConfig(**data["sampling"]),
        model=ModelConfig(**data["model"]),
        database=DBConfig(**data["datasource"]),
    )
