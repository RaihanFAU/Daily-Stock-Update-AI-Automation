"""Transform Alpha Vantage payloads into database-ready data frames."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


ALPHA_VANTAGE_DAILY_KEY = "Time Series (Daily)"


def transform_daily_prices(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize Alpha Vantage daily data for MySQL loading.

    TIME_SERIES_DAILY does not include adjusted close, dividends, or split
    coefficient. For that free endpoint, keep the schema stable by storing
    adjusted_close as close, dividend_amount as 0, and split_coefficient as 1.
    """

    time_series = payload.get(ALPHA_VANTAGE_DAILY_KEY)
    if not isinstance(time_series, dict) or not time_series:
        raise ValueError("No daily time series rows found in Alpha Vantage payload")

    rows = []
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None)
    for trade_date, values in time_series.items():
        rows.append(
            {
                "symbol": symbol.upper(),
                "trade_date": trade_date,
                "open": values.get("1. open"),
                "high": values.get("2. high"),
                "low": values.get("3. low"),
                "close": values.get("4. close"),
                "adjusted_close": values.get("5. adjusted close", values.get("4. close")),
                "volume": values.get("6. volume", values.get("5. volume")),
                "dividend_amount": values.get("7. dividend amount", 0),
                "split_coefficient": values.get("8. split coefficient", 1),
                "source_loaded_at": loaded_at,
            }
        )

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="raise").dt.date

    decimal_columns = [
        "open",
        "high",
        "low",
        "close",
        "adjusted_close",
        "dividend_amount",
        "split_coefficient",
    ]
    for column in decimal_columns:
        df[column] = pd.to_numeric(df[column], errors="raise")

    df["volume"] = pd.to_numeric(df["volume"], errors="raise").astype("int64")
    return df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def transform_daily_adjusted(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    """Backward-compatible wrapper for the original adjusted transform name."""

    return transform_daily_prices(symbol, payload)
