"""
Data cleaning script: raw → processed.

Reads from data/raw/ (JSON or DB), applies cleaning rules,
writes to data/processed/cars_clean.parquet.

Run: python scripts/clean_cars.py
"""
from pathlib import Path
import json
import re

import pandas as pd

# Paths
BASE_DIR = Path(__file__).resolve().parent.parent
RAW_JSON = BASE_DIR / "data" / "raw" / "cars_market_data.json"
RAW_DB = BASE_DIR / "data" / "raw" / "cars_market.db"
OUTPUT_PARQUET = BASE_DIR / "data" / "processed" / "cars_clean.parquet"
OUTPUT_CSV = BASE_DIR / "data" / "processed" / "cars_clean.csv"


def load_raw() -> pd.DataFrame:
    """Load raw data from JSON or DB."""
    if RAW_JSON.exists():
        df = pd.read_json(RAW_JSON)
    elif RAW_DB.exists():
        import sqlite3
        conn = sqlite3.connect(RAW_DB)
        df = pd.read_sql_query("SELECT * FROM cars", conn)
        conn.close()
        # Parse JSON columns
        for col in ["equipment"]:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: json.loads(x) if x else [])
    else:
        raise FileNotFoundError(f"No raw data found. Run the scraper first. Expected: {RAW_JSON} or {RAW_DB}")
    return df


def clean_price(price_str) -> float | None:
    """Extract numeric price from string like '29.990 €' or '€ 1,242'."""
    if pd.isna(price_str) or not str(price_str).strip():
        return None
    s = re.sub(r"[^\d.,]", "", str(price_str))
    s = s.replace(",", ".")
    # Handle German format: 29.990 = 29990
    parts = s.split(".")
    if len(parts) == 2 and len(parts[1]) == 3:  # 29.990
        s = parts[0] + parts[1]
    else:
        s = s.replace(".", "")
    try:
        return float(s) if s else None
    except ValueError:
        return None


def clean_mileage(val) -> float | None:
    """Parse mileage to numeric."""
    if pd.isna(val) or val == "":
        return None
    s = re.sub(r"[^\d]", "", str(val))
    try:
        return float(s) if s else None
    except ValueError:
        return None


def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Apply cleaning rules. Extend this with your own logic."""
    df = df.copy()

    # Convert complex types to JSON strings for parquet compatibility
    for col in ["equipment"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else str(x) if pd.notna(x) else "")

    # Drop duplicates by car_id (keep first)
    if "car_id" in df.columns:
        df = df.drop_duplicates(subset=["car_id"], keep="first")

    # Canonical price (SRP)
    if "price_current_eur" in df.columns:
        df["price_numeric"] = pd.to_numeric(df["price_current_eur"], errors="coerce")
    elif "price" in df.columns:
        # legacy fallback (older JSONs)
        df["price_numeric"] = df["price"].apply(clean_price)

    # Parse mileage
    if "mileage_km" in df.columns:
        df["mileage_numeric"] = df["mileage_km"].apply(clean_mileage)

    # Optional: drop rows with invalid price (e.g. < 1000 or > 500000)
    if "price_numeric" in df.columns:
        df = df[(df["price_numeric"].isna()) | ((df["price_numeric"] >= 1000) & (df["price_numeric"] <= 500000))]

    # Optional: drop columns you don't need (customize as needed)
    # cols_to_drop = ["detail_price_raw"]
    # df = df.drop(columns=[c for c in cols_to_drop if c in df.columns], errors="ignore")

    return df


def main():
    print("Loading raw data...")
    df = load_raw()
    print(f"  Loaded {len(df)} rows")

    print("Cleaning...")
    df_clean = clean(df)
    print(f"  After cleaning: {len(df_clean)} rows")

    # Ensure output dir exists
    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)

    # Save to parquet (primary format for analytics)
    df_clean.to_parquet(OUTPUT_PARQUET, index=False)
    print(f"  Saved to {OUTPUT_PARQUET}")

    # Also save CSV for easy inspection
    df_clean.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"  Saved to {OUTPUT_CSV}")

    print("Done.")


if __name__ == "__main__":
    main()
