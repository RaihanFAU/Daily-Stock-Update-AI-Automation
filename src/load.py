"""MySQL loading utilities for stock prices and pipeline run logs."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pandas as pd
from sqlalchemy import Engine, create_engine, text

from config import MySQLConfig


_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _quote_identifier(identifier: str) -> str:
    """Quote a trusted MySQL identifier after validating its shape."""

    if not _IDENTIFIER_PATTERN.fullmatch(identifier):
        raise ValueError(f"Invalid MySQL identifier: {identifier}")
    return f"`{identifier}`"


def _utc_now_naive() -> datetime:
    """Return UTC as a naive datetime for MySQL DATETIME columns."""

    return datetime.now(timezone.utc).replace(tzinfo=None)


def create_mysql_engine(config: MySQLConfig) -> Engine:
    """Create a SQLAlchemy engine backed by the PyMySQL driver."""

    return create_engine(config.sqlalchemy_url, pool_pre_ping=True, future=True)


def upsert_stock_prices(engine: Engine, df: pd.DataFrame, table_name: str) -> int:
    """Upsert stock rows by unique key (symbol, trade_date)."""

    if df.empty:
        return 0

    records = df.to_dict(orient="records")
    quoted_table = _quote_identifier(table_name)
    statement = text(
        f"""
        INSERT INTO {quoted_table} (
            `symbol`,
            `trade_date`,
            `open`,
            `high`,
            `low`,
            `close`,
            `adjusted_close`,
            `volume`,
            `dividend_amount`,
            `split_coefficient`,
            `source_loaded_at`
        )
        VALUES (
            :symbol,
            :trade_date,
            :open,
            :high,
            :low,
            :close,
            :adjusted_close,
            :volume,
            :dividend_amount,
            :split_coefficient,
            :source_loaded_at
        )
        ON DUPLICATE KEY UPDATE
            `open` = VALUES(`open`),
            `high` = VALUES(`high`),
            `low` = VALUES(`low`),
            `close` = VALUES(`close`),
            `adjusted_close` = VALUES(`adjusted_close`),
            `volume` = VALUES(`volume`),
            `dividend_amount` = VALUES(`dividend_amount`),
            `split_coefficient` = VALUES(`split_coefficient`),
            `source_loaded_at` = VALUES(`source_loaded_at`),
            `updated_at` = CURRENT_TIMESTAMP
        """
    )

    with engine.begin() as connection:
        connection.execute(statement, records)

    return len(records)


def insert_pipeline_run_log(
    engine: Engine,
    table_name: str,
    *,
    symbol: str,
    status: str,
    rows_loaded: int = 0,
    error_message: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> None:
    """Persist one pipeline execution status row."""

    started_at = started_at or _utc_now_naive()
    finished_at = finished_at or _utc_now_naive()
    quoted_table = _quote_identifier(table_name)

    statement = text(
        f"""
        INSERT INTO {quoted_table} (
            `symbol`,
            `status`,
            `rows_loaded`,
            `error_message`,
            `started_at`,
            `finished_at`
        )
        VALUES (
            :symbol,
            :status,
            :rows_loaded,
            :error_message,
            :started_at,
            :finished_at
        )
        """
    )
    params: dict[str, Any] = {
        "symbol": symbol.upper(),
        "status": status,
        "rows_loaded": rows_loaded,
        "error_message": error_message,
        "started_at": started_at,
        "finished_at": finished_at,
    }

    with engine.begin() as connection:
        connection.execute(statement, params)
