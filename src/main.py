"""Entry point for the daily stock market pipeline."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from config import load_config
from extract import AlphaVantageExtractor
from load import create_mysql_engine, insert_pipeline_run_log, upsert_stock_prices
from logger import setup_logging
from transform import transform_daily_prices


def _utc_now_naive() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _run_symbol(
    *,
    symbol: str,
    extractor: AlphaVantageExtractor,
    engine,
    config,
    logger,
) -> int:
    started_at = _utc_now_naive()
    rows_loaded = 0

    logger.info("Pipeline started for symbol=%s", symbol)
    try:
        payload = extractor.fetch_daily_series(symbol)
        transformed = transform_daily_prices(symbol, payload)
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
            finished_at=_utc_now_naive(),
        )
        logger.info("Pipeline completed for symbol=%s rows_loaded=%s", symbol, rows_loaded)
        return rows_loaded
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
                finished_at=_utc_now_naive(),
            )
        except Exception:
            logger.exception("Failed to write pipeline failure status for symbol=%s", symbol)
        raise


def run() -> None:
    config = load_config()
    logger = setup_logging(config.pipeline.log_dir, config.pipeline.log_file)
    engine = create_mysql_engine(config.mysql)
    extractor = AlphaVantageExtractor(config.alpha_vantage, logger)

    failures = []
    total_rows_loaded = 0

    logger.info("Batch pipeline started for %s symbols", len(config.pipeline.symbols))
    for index, symbol in enumerate(config.pipeline.symbols):
        if index > 0 and config.pipeline.request_delay_seconds > 0:
            logger.info(
                "Waiting %s seconds before next Alpha Vantage request",
                config.pipeline.request_delay_seconds,
            )
            time.sleep(config.pipeline.request_delay_seconds)

        try:
            total_rows_loaded += _run_symbol(
                symbol=symbol,
                extractor=extractor,
                engine=engine,
                config=config,
                logger=logger,
            )
        except Exception as exc:
            failures.append((symbol, exc))

    if failures:
        failed_symbols = ", ".join(symbol for symbol, _ in failures)
        logger.error("Batch pipeline completed with failures: %s", failed_symbols)
        raise RuntimeError(f"Pipeline failed for {len(failures)} symbols: {failed_symbols}")

    logger.info(
        "Batch pipeline completed successfully symbols=%s total_rows_loaded=%s",
        len(config.pipeline.symbols),
        total_rows_loaded,
    )


if __name__ == "__main__":
    run()
