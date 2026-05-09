# Scraper & Matching Project

## Overview
This project is a robust system for scraping competitor websites and reconciling their product data with an internal system. It includes matching logic, a reconciliation pipeline, and a dashboard for management.

## Directory Structure
- **`src/`**: Core application logic.
    - **`matching/`**: Reconciliation pipeline and product matching scripts.
    - **`ai/`**: AI/LLM related services (FastAPI/TinyLlama).
    - **`utils/`**: Shared helper functions.
- **`scrapers/`**: Centralized scraper implementations.
    - **`shopify/`**: General Shopify scraper scripts.
    - **`custom/`**: Site-specific custom scrapers.
- **`dashboard/`**: Flask/Dash application for data visualization.
- **`data/`**: Project data (Git-ignored).
    - **`raw/`**: Unprocessed CSV/ZIP downloads.
    - **`processed/`**: Intermediate results.
    - **`exports/`**: Final CSVs ready for system import.
    - **`sql/`**: SQL export files.
    - **`history/`**: Pipeline run history and logs.
- **`scripts/`**: Operational and maintenance scripts.
- **`docs/`**: Documentation and command references.
- **`tests/`**: Unit and integration tests.
- **`archive/`**: Legacy or experimental scripts.

## Running the Scrapers
Use the unified entry point:
```bash
# List all configured competitors
python src/scrape.py --list

# Run a specific competitor scraper (uses sitemaps)
python src/scrape.py --competitor <key>

# Scrape a single product URL (auto-identifies competitor)
python src/scrape.py --url <url>
```

## GitHub Actions
All workflows in `.github/workflows/` have been updated to reflect the new directory structure. You can trigger them from the GitHub Actions tab as usual.


## Running Reconciliation
The pipeline scripts are located in `src/matching/`. Refer to `docs/commands.sh` for common execution patterns.

## Configuration
- **`competitors.json`**: Defines all competitor targets and their scraper paths.
- **`.env`**: Local environment variables (API keys, worker counts, etc.).
