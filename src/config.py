"""Configuration loading for the daily stock pipeline."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus

import yaml
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "config.yaml"


@dataclass(frozen=True)
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def sqlalchemy_url(self) -> str:
        user = quote_plus(self.user)
        password = quote_plus(self.password)
        host = self.host
        return f"mysql+pymysql://{user}:{password}@{host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class AlphaVantageConfig:
    api_key: str
    base_url: str
    function: str
    outputsize: str
    timeout_seconds: int


@dataclass(frozen=True)
class PipelineConfig:
    default_symbol: str
    symbols: list[str]
    request_delay_seconds: float
    log_dir: Path
    log_file: str
    raw_table: str
    run_log_table: str


@dataclass(frozen=True)
class AppConfig:
    alpha_vantage: AlphaVantageConfig
    mysql: MySQLConfig
    pipeline: PipelineConfig


def _required_env(name: str) -> str:
    from os import getenv

    value = getenv(name)
    if not value:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


def _read_yaml(config_path: Path) -> dict:
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as file:
        loaded = yaml.safe_load(file) or {}

    if not isinstance(loaded, dict):
        raise ValueError(f"Config file must contain a YAML mapping: {config_path}")
    return loaded


def _read_symbols(pipeline_config: dict) -> list[str]:
    configured_symbols = pipeline_config.get("symbols")
    if configured_symbols is None:
        configured_symbols = [pipeline_config.get("default_symbol", "IBM")]

    if not isinstance(configured_symbols, list) or not configured_symbols:
        raise ValueError("pipeline.symbols must be a non-empty YAML list")

    symbols = []
    for symbol in configured_symbols:
        symbol_text = str(symbol).strip().upper()
        if not symbol_text:
            raise ValueError("pipeline.symbols cannot contain blank values")
        symbols.append(symbol_text)
    return symbols


def load_config(config_path: Path | None = None) -> AppConfig:
    """Load .env and YAML configuration into typed settings."""

    load_dotenv(PROJECT_ROOT / ".env")

    config_path = config_path or DEFAULT_CONFIG_PATH
    config_data = _read_yaml(config_path)

    alpha_config = config_data.get("alpha_vantage", {})
    pipeline_config = config_data.get("pipeline", {})
    database_config = config_data.get("database", {})

    return AppConfig(
        alpha_vantage=AlphaVantageConfig(
            api_key=_required_env("ALPHA_VANTAGE_API_KEY"),
            base_url=alpha_config.get("base_url", "https://www.alphavantage.co/query"),
            function=alpha_config.get("function", "TIME_SERIES_DAILY_ADJUSTED"),
            outputsize=alpha_config.get("outputsize", "compact"),
            timeout_seconds=int(alpha_config.get("timeout_seconds", 30)),
        ),
        mysql=MySQLConfig(
            host=_required_env("MYSQL_HOST"),
            port=int(_required_env("MYSQL_PORT")),
            user=_required_env("MYSQL_USER"),
            password=_required_env("MYSQL_PASSWORD"),
            database=_required_env("MYSQL_DATABASE"),
        ),
        pipeline=PipelineConfig(
            default_symbol=str(pipeline_config.get("default_symbol", "IBM")).upper(),
            symbols=_read_symbols(pipeline_config),
            request_delay_seconds=float(pipeline_config.get("request_delay_seconds", 15)),
            log_dir=PROJECT_ROOT / pipeline_config.get("log_dir", "logs"),
            log_file=pipeline_config.get("log_file", "daily_stock_pipeline.log"),
            raw_table=database_config.get("raw_table", "stock_prices_raw"),
            run_log_table=database_config.get("run_log_table", "pipeline_run_log"),
        ),
    )
