# n8n Automation Workflow

This folder documents the planned `n8n` automation layer for the Daily Stock Update AI Automation project.

## Purpose

The `n8n` workflow is used after the Python pipeline finishes loading stock data into MySQL.
Its job is to automate reporting, alerts, and team communication.

## Overall Architecture

```text
Python -> MySQL -> SQL Analysis -> Power BI
                      |
                      -> n8n -> Email / Slack / Microsoft Teams
```

## What n8n Does

- Runs on a schedule after the Python pipeline completes.
- Connects to MySQL and reads the latest stock data.
- Reads pipeline status from `pipeline_run_log`.
- Builds an end-of-day summary.
- Sends the summary to managers, team leads, and teammates.
- Sends alerts if any stock symbols fail during the pipeline run.

## Recommended Workflow

```text
Schedule Trigger
-> MySQL: pipeline status query
-> MySQL: latest stock summary query
-> MySQL: gainers/losers query
-> Format report
-> IF failed symbols exist
   -> Send warning notification
-> ELSE
   -> Send normal daily summary
```

## Data Sources

The workflow should connect to the same MySQL database used by the Python pipeline.

Main tables:

- `stock_prices_raw`
- `pipeline_run_log`

## Suggested Reporting Logic

The daily report can include:

- total symbols processed
- successful symbols
- failed symbols
- latest trade date
- top gainers
- top losers
- latest close price per symbol
- pipeline failure messages, if any

## Delivery Channels

The workflow can send reports through:

- Email
- Slack
- Microsoft Teams

## Scheduling Recommendation

- Run Python ETL first
- Run `n8n` 10 to 20 minutes later

Example:

- Python pipeline: `18:00`
- n8n summary workflow: `18:20`

This gives the database enough time to finish all symbol loads before reporting starts.

## Best Practice

- Keep Python responsible for data ingestion only.
- Keep MySQL as the central source of truth.
- Keep SQL responsible for analysis-ready queries.
- Keep Power BI focused on dashboards and KPIs.
- Keep `n8n` focused on automation and communication.

## Future Expansion

Later, the `n8n` workflow can be extended to support:

- HTML email reports
- PDF report attachments
- multi-team recipient groups
- retry alerts
- weekly summary reports
- manager-only exception summaries

## Notes

- Do not store secrets directly inside workflow nodes if avoidable.
- Use `n8n` credentials for MySQL and email/slack integrations.
- Test queries in MySQL before adding them into production workflows.
