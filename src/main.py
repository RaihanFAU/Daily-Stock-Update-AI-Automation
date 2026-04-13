"""Entry point for the daily stock market pipeline."""

from __future__ import annotations

from datetime import datetime, timezone

from config import load_config
from extract import AlphaVantageExtractor
from load import create_mysql_engine, insert_pipeline_run_log, upsert_stock_prices
from logger import setup_logging
from transform import transform_daily_adjusted


def run() -> None:
    config = load_config()
    logger = setup_logging(config.pipeline.log_dir, config.pipeline.log_file)
    engine = create_mysql_engine(config.mysql)

    symbol = config.pipeline.default_symbol
    started_at = datetime.now(timezone.utc).replace(tzinfo=None)
    rows_loaded = 0

    logger.info("Pipeline started for symbol=%s", symbol)
    try:
        extractor = AlphaVantageExtractor(config.alpha_vantage, logger)
        payload = extractor.fetch_daily_adjusted(symbol)
        transformed = transform_daily_adjusted(symbol, payload)
        rows_loaded = upsert_stock_prices(
            engine,
            transformed,
            config.pipeline.raw_table,
        )

        insert_pipeline_run_log(
            engine,
            config.pipeline.run_log_table,
            symbol=symbol,
            status="SUCCESS",
            rows_loaded=rows_loaded,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
        )
        logger.info("Pipeline completed for symbol=%s rows_loaded=%s", symbol, rows_loaded)
    except Exception as exc:
        logger.exception("Pipeline failed for symbol=%s", symbol)
        try:
            insert_pipeline_run_log(
                engine,
                config.pipeline.run_log_table,
                symbol=symbol,
                status="FAILED",
                rows_loaded=rows_loaded,
                error_message=str(exc)[:2000],
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).replace(tzinfo=None),
            )
        except Exception:
            logger.exception("Failed to write pipeline failure status")
        raise


if __name__ == "__main__":
    run()
