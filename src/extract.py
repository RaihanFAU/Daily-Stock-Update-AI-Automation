"""Alpha Vantage extraction logic."""

from __future__ import annotations

import logging
from typing import Any

import requests

from config import AlphaVantageConfig


class AlphaVantageExtractor:
    """Fetch daily time series data from Alpha Vantage."""

    def __init__(self, config: AlphaVantageConfig, logger: logging.Logger) -> None:
        self.config = config
        self.logger = logger

    def fetch_daily_series(self, symbol: str) -> dict[str, Any]:
        params = {
            "function": self.config.function,
            "symbol": symbol,
            "apikey": self.config.api_key,
            "outputsize": self.config.outputsize,
        }

        self.logger.info("Fetching Alpha Vantage data for symbol=%s", symbol)
        try:
            response = requests.get(
                self.config.base_url,
                params=params,
                timeout=self.config.timeout_seconds,
            )
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise RuntimeError(f"Alpha Vantage request failed for {symbol}") from exc
        except ValueError as exc:
            raise RuntimeError("Alpha Vantage returned invalid JSON") from exc

        api_error = payload.get("Error Message") or payload.get("Information")
        rate_limit_note = payload.get("Note")
        if api_error:
            raise RuntimeError(f"Alpha Vantage API error: {api_error}")
        if rate_limit_note:
            raise RuntimeError(f"Alpha Vantage API rate-limit response: {rate_limit_note}")
        if "Time Series (Daily)" not in payload:
            raise RuntimeError("Alpha Vantage response is missing Time Series (Daily)")

        return payload

    def fetch_daily_adjusted(self, symbol: str) -> dict[str, Any]:
        """Backward-compatible wrapper for the original adjusted endpoint name."""

        return self.fetch_daily_series(symbol)
