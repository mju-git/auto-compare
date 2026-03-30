"""
One-command local runner:
  - scrape a mobile.de search URL into SQLite (raw)
  - build the cleaned parquet (processed)

Usage:
  python run_pipeline.py "<mobile.de search url>"

Output:
  data/processed/cars_clean.parquet   (upload this into the Streamlit app)
"""

from __future__ import annotations

import sys
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
PARQUET_OUT = BASE_DIR / "data" / "processed" / "cars_clean.parquet"


def main() -> None:
    search_url = sys.argv[1] if len(sys.argv) > 1 else ""
    if not search_url.strip():
        print("Usage: python run_pipeline.py <mobile.de search URL>")
        sys.exit(1)

    # Step 1: scrape
    from scraper import run_scraper  # lazy import (selenium deps)

    run_scraper(search_url)

    # Step 2: clean
    from scripts.clean_cars import main as clean_main

    clean_main()

    print("")
    print("Done.")
    print(f"Upload this file into the Streamlit app: {PARQUET_OUT}")


if __name__ == "__main__":
    main()

