# NEM Tracker

Real-time dashboard for Australia's National Electricity Market (NEM), powered by DuckDB-WASM.

**Live:** [nemtracker.github.io](https://nemtracker.github.io)

This repo is the dashboard-only subset of [analytics-as-code](https://github.com/djouallah/analytics-as-code), which handles the full pipeline: data ingestion, transformation (dbt-duckdb), and storage (Iceberg REST catalog). This repo only imports the processed data and serves the dashboard.
