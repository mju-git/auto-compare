# Car Comparison

A Python project for scraping car listings from mobile.de, cleaning the data, and preparing for analysis and comparison.

## Project Structure

```
Car comparison/
├── data/
│   ├── raw/                    # Scraper output (untouched)
│   │   ├── cars_market.db      # SQLite database
│   │   └── cars_market_data.json
│   └── processed/              # Cleaned data (right place for analysis)
│       ├── cars_clean.parquet
│       └── cars_clean.csv
├── scripts/
│   └── clean_cars.py           # Raw → processed pipeline
├── notebooks/                  # Not tracked (local only; see notebooks/README.md)
├── scraper.py
├── main.py
└── requirements.txt
```

## Data Flow

| Stage | Location | Purpose |
|-------|----------|---------|
| **Raw** | `data/raw/` | Scraper output. Never modify. |
| **Processed** | `data/processed/` | Cleaned data. Use this for analysis. |
| **Analysis** | `notebooks/` | Load from `data/processed/` only. |

### What happens during scraping (end-to-end)

```mermaid
flowchart TD
  start[Start] --> openBrowser["Open Chrome (undetected-chromedriver)"]
  openBrowser --> srpPhase["Phase_1: Search Results (SRP)"]
  srpPhase --> collectLinks[Collect listing detail URLs]
  collectLinks --> markSeen["Mark all seen listing IDs\n(last_seen_at = run_timestamp)"]
  markSeen --> soldMark["Mark missing listings as sold\n(last_seen_at = 'sold' for this search fingerprint)"]
  collectLinks --> srpSnapshot["Extract SRP snapshot\nbrand/model/price/price_rating/vehicle_condition/ad_online_since"]
  srpSnapshot --> upsertSrp[Upsert SRP fields into SQLite]
  upsertSrp --> phase2["Phase_2: Detail pages\n(only new car_id rows)"]
  phase2 --> extractDetail[Extract technical + seller fields]
  extractDetail --> saveDb[Upsert row into SQLite (cars)]
  saveDb --> exportJson[Export DB to JSON for notebooks/cleaning]
  exportJson --> endNode[End]
```

Key rules:

- **Price source of truth**: SRP price is stored as `price_current_eur` (integer EUR). Detail page price is kept only as `detail_price_raw` for debugging.
- **Accident-free source of truth**: `is_accident_free` is derived deterministically from `vehicle_condition` (explicit accident-free) OR `mileage_km < 100`.
- **Sold listings**: a listing that was previously seen for the same `source_search` (search fingerprint) but is missing in a later run is **kept** and marked with `last_seen_at = 'sold'`.

## Installation

```bash
pip install -r requirements.txt
```

**Requirements:** Python 3.9+, Google Chrome (latest)

## Usage

### 1. Scrape data

```bash
python main.py "https://suchen.mobile.de/fahrzeuge/search.html?..."
```

Saves to `data/raw/cars_market.db` and `cars_market_data.json`.

Optional: limit detail scraping to a few cars (PowerShell):

```powershell
$env:TEST_MAX_CARS='3'
python main.py "https://suchen.mobile.de/fahrzeuge/search.html?..."
```

### 2. Clean data

```bash
python scripts/clean_cars.py
```

Reads from `data/raw/`, applies cleaning rules, writes to `data/processed/cars_clean.parquet` and `.csv`.

### 3. Analyze

Use your own local notebook (not committed) and load:

`data/processed/cars_clean.parquet`

## Where is the "right" data stored?

**Cleaned data** lives in `data/processed/`:

- **`cars_clean.parquet`** – primary format for analytics (efficient, preserves types)
- **`cars_clean.csv`** – human-readable backup

You do **not** need a separate database for cleaned data. Parquet is the standard for analytics. If you build a web app later, you can load parquet into memory or connect to a proper DB.

## Customizing the cleaning script

Edit `scripts/clean_cars.py` to:

- Drop columns you don't need
- Add validation rules
- Parse dates, standardize fuel types, etc.

Re-run the script after each scrape to refresh `data/processed/`.

## Collected Fields (raw)

`car_id`, `brand`, `model`, `price_current_eur`, `mileage_km`, `first_registration`, `power_hp`, `power_kw`, `fuel_type`, `transmission`, `number_of_owners`, `cubic_capacity`, `color`, `color_manufacturer`, `interior_design`, `trim`, `origin`, `hu`, `climatisation`, `equipment`, `description`, `seller_type`, `seller_rating`, `price_rating`, `vehicle_condition`, `is_accident_free`, `ad_online_since`, `source_search`, and more.

## Next phase: simple online analyzer

Suggested direction:

- Build a small web app that reads **`data/processed/cars_clean.parquet`** (or a derived subset) and provides:
  - filters (price, km, year, power)
  - comparisons across trims/years
  - plots (Plotly) and summary tables


## License

For personal/educational use only. Respect mobile.de's Terms of Service.
