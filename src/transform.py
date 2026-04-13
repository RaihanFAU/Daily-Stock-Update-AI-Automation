"""Transform Alpha Vantage payloads into database-ready data frames."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pandas as pd


ALPHA_VANTAGE_DAILY_KEY = "Time Series (Daily)"


def transform_daily_adjusted(symbol: str, payload: dict[str, Any]) -> pd.DataFrame:
    """Normalize TIME_SERIES_DAILY_ADJUSTED data for MySQL loading."""

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
                "adjusted_close": values.get("5. adjusted close"),
                "volume": values.get("6. volume"),
                "dividend_amount": values.get("7. dividend amount"),
                "split_coefficient": values.get("8. split coefficient"),
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
