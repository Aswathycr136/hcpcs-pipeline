# hcpcs-pipeline
# HCPCS Pipeline PoC

PoC pipeline to scrape HCPCS codes, store in SQLite, run validations, and execute via GitHub Actions manually.


## Run locally
1. Create a virtualenv and install dependencies in `scraper/` and `loader/`.
2. Run scraper: `python scraper/scrape_hcpcs.py`
3. Run loader: `python loader/load_to_sqlite.py`
4. Run validations: `python validation/validate.py`

## Run on GitHub
Push to GitHub. Use Actions → "HCPCS pipeline — manual" → Run workflow.

Artifacts: JSON files (scraper_output) and `hcpcs.db` SQLite file.
