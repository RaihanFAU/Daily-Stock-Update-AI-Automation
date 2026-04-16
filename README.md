# Daily Stock Update AI Automation

Python pipeline for extracting daily adjusted stock prices from Alpha Vantage and loading them into MySQL.

## What It Does

- Reads Alpha Vantage and MySQL credentials from `.env`.
- Reads the stock symbols and runtime settings from `config/config.yaml`.
- Calls Alpha Vantage with the free `TIME_SERIES_DAILY` endpoint.
- Transforms the response into a clean tabular format with pandas.
- Upserts rows into `stock_prices_raw` using unique key `(symbol, trade_date)`.
- Writes each pipeline run status to `pipeline_run_log`.
- Logs to both the console and `data/logs/pipeline.log`.

Adjusted close, dividend, and split fields are kept in the table schema for a
later switch to `TIME_SERIES_DAILY_ADJUSTED`. While using `TIME_SERIES_DAILY`,
the pipeline stores `adjusted_close` as `close`, `dividend_amount` as `0`, and
`split_coefficient` as `1`.

## Setup

Install dependencies:

```powershell
pip install -r requirements.txt
```

Update `.env` with your real credentials:

```text
ALPHA_VANTAGE_API_KEY=your_key
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=your_user
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=stock_market
```

Create the MySQL tables by running:

```sql
source sql/create_tables.sql;
```

## Run

```powershell
python src/main.py
```

The default symbol is configured in `config/config.yaml`.

The current configuration fetches 20 large U.S.-listed companies by market cap.
Each symbol is loaded independently and gets its own row in `pipeline_run_log`.
The batch waits between Alpha Vantage requests using
`pipeline.request_delay_seconds` to avoid free-plan pacing errors.
