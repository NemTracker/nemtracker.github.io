# NEM Tracker

Real-time dashboard for Australia's National Electricity Market (NEM), powered by DuckDB-WASM.

**Live:** [nemtracker.github.io](https://nemtracker.github.io)

This repo is the dashboard-only subset of [analytics-as-code](https://github.com/djouallah/analytics-as-code), which handles the full pipeline: data ingestion, transformation (dbt-duckdb), and storage (Iceberg REST catalog). This repo only imports the processed data and serves the dashboard.

## How It Works

- A GitHub Actions workflow imports data from the Iceberg catalog every 30 minutes
- Data is exported as compact DuckDB files and deployed to GitHub Pages
- The dashboard loads these files in the browser via DuckDB-WASM — no backend API

## Data Refresh

- **Every 30 minutes:** Intraday data (today's SCADA + prices)
- **Daily at 6am AEST:** Full historical refresh including dimensions and half-year data splits

## Project Structure

```
├── dashboard/
│   ├── index.html              # DuckDB-WASM dashboard
│   └── coi-serviceworker.js    # CORS isolation service worker
├── scripts/
│   └── cache_catalog.py        # Iceberg → DuckDB export
└── .github/workflows/
    ├── build.yml               # Deploy dashboard to GitHub Pages
    └── import_data.yml         # Import data from Iceberg (30-min + daily)
```

## Setup

Requires three GitHub Actions secrets:

| Secret | Description |
|--------|-------------|
| `ICEBERG_REST_ENDPOINT` | REST catalog URL |
| `ICEBERG_TOKEN` | Bearer token for catalog auth |
| `ICEBERG_WAREHOUSE` | Warehouse path in the catalog |
