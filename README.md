# NEM Tracker

Real-time dashboard for Australia's National Electricity Market (NEM), powered by DuckDB-WASM.

**Live:** [nemtracker.github.io](https://nemtracker.github.io)

## How It Works

- A GitHub Actions workflow imports data from an Iceberg REST catalog every 30 minutes
- Data is exported as compact DuckDB files and deployed to GitHub Pages
- The dashboard loads these files in the browser via DuckDB-WASM and queries them client-side
- No backend API — everything runs in your browser

## Architecture

| Iceberg Catalog | → | Import (every 30 min) | → | GitHub Pages | → | DuckDB-WASM Dashboard |
|:---------------:|---|:---------------------:|---|:------------:|---|:---------------------:|
| *persistent data* | | *Iceberg → native DuckDB files* | | *static file hosting* | | *client-side queries in browser* |

## Data Refresh

- **Every 30 minutes:** Intraday data (today's SCADA + prices) is refreshed
- **Daily at 6am AEST:** Full historical refresh including all dimensions and half-year data splits

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

Data processing (ingestion + transformation via dbt) runs in a separate repository. This repo is read-only — it only imports and visualizes.
