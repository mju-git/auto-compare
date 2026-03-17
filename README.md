# Mobile.de Car Scraper

A Python scraper for collecting car listings from mobile.de, designed for market research and price comparison.

## Features

- **Anti-bot evasion**: Uses `undetected-chromedriver` to bypass detection
- **Pagination handling**: Automatically navigates through all search result pages
- **Comprehensive data extraction**: Collects 25+ fields per car listing
- **Bilingual support**: Works with both German and English mobile.de interfaces
- **Robust extraction**: Uses a multi-tier approach to handle different page layouts
- **Deduplication**: Prevents duplicate entries using unique car IDs
- **Persistent storage**: SQLite database for crash-safe data collection
- **Resume capability**: Skips already-scraped cars on subsequent runs

## Collected Fields

| Field | Description |
|-------|-------------|
| `car_id` | Unique identifier from mobile.de |
| `title` | Car listing title |
| `price` | Listed price |
| `mileage_km` | Odometer reading |
| `first_registration` | First registration date |
| `power_hp` / `power_kw` | Engine power |
| `fuel_type` | Fuel type (Petrol, Diesel, Hybrid, Electric) |
| `transmission` | Gearbox type |
| `number_of_owners` | Previous owners |
| `cubic_capacity` | Engine displacement |
| `color` / `color_manufacturer` | Exterior color |
| `interior_design` | Interior material/color |
| `is_accident_free` | Accident history |
| `price_rating` | Mobile.de price evaluation |
| `vehicle_id` | Dealer's internal ID |
| `trim` | Model variant/trim level |
| `origin` | Country of origin |
| `hu` | Next inspection date |
| `climatisation` | A/C type |
| `equipment` | Full equipment list |
| `description` | Seller's description |
| `seller_type` | Dealer or private |
| `seller_rating` | Dealer rating (if applicable) |
| `ad_online_since` | Listing date |

## Installation

```bash
pip install -r requirements.txt
```

**Requirements:**
- Python 3.9+
- Google Chrome browser (latest version recommended)

## Usage

1. Go to [mobile.de](https://www.mobile.de) and create your search filters
2. Copy the search results URL
3. Run the scraper:

```bash
python main.py "https://suchen.mobile.de/fahrzeuge/search.html?..."
```

The scraper will:
1. Open a Chrome browser window (not headless, for CAPTCHA handling)
2. Navigate through all search result pages
3. Visit each car listing and extract data
4. Save results to `cars_market.db` (SQLite) and `cars_market_data.json`

### Multiple Searches

You can run multiple searches to build up your dataset. The scraper automatically:
- Detects cars already in the database
- Skips duplicates (based on car ID, not URL)
- Adds only new listings

## Output

### SQLite Database (`cars_market.db`)

Persistent storage with `car_id` as primary key. Use for:
- Incremental data collection
- SQL queries and analysis

### JSON Export (`cars_market_data.json`)

Auto-generated after each run. Ready for:
- Pandas DataFrames
- Jupyter notebooks
- Other analysis tools

## Example Analysis

```python
import pandas as pd

df = pd.read_json("cars_market_data.json")
print(df[["title", "price", "mileage_km", "first_registration"]].head())
```

## Configuration

In `scraper.py`, you can modify:

```python
CLEAR_BEFORE_RUN = False  # Set True to clear DB before each run
```

## Notes

- The browser window must remain visible (headless mode is disabled for anti-bot reasons)
- Manual CAPTCHA solving may be required occasionally
- Random delays between requests are built-in to avoid rate limiting
- Some fields may be empty if not available on the listing page

## License

For personal/educational use only. Respect mobile.de's Terms of Service.
