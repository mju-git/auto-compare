"""
Export a CSV of "preferred" cars for analysis.

Include a car if:
  - not sold (last_seen_at != 'sold'), AND
  - (mileage_km < 100) OR (is_accident_free == 1) OR (vehicle_condition indicates new/pre-reg/demo/accident-free)

Output:
  data/processed/cars_preferred_not_sold.csv
"""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "raw" / "cars_market.db"
OUTPUT_CSV = BASE_DIR / "data" / "processed" / "cars_preferred_not_sold.csv"


def _parse_km(val: object) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    digits = re.sub(r"[^0-9]", "", s)
    return int(digits) if digits else None


def _is_newish_condition(vc: object) -> bool:
    s = ("" if vc is None else str(vc)).strip().lower()
    if not s:
        return False
    tokens = [
        # English
        "new car",
        "pre-registration",
        "demo",
        "demonstration",
        "accident-free",
        "accident free",
        # German
        "neuwagen",
        "tageszulassung",
        "vorführ",  # Vorführfahrzeug
        "vorfuehr",
        "unfallfrei",
        "unfall frei",
    ]
    return any(t in s for t in tokens)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))
    df = pd.read_sql_query("SELECT * FROM cars", conn)
    conn.close()

    # not sold
    last_seen = df.get("last_seen_at", pd.Series([""] * len(df)))
    not_sold = last_seen.fillna("").astype(str).str.strip().ne("sold")

    km = df.get("mileage_km", pd.Series([None] * len(df))).apply(_parse_km)
    km_lt_100 = km.notna() & (km < 100)

    is_af = df.get("is_accident_free", pd.Series([0] * len(df))).fillna(0).astype(int).eq(1)

    vc = df.get("vehicle_condition", pd.Series([""] * len(df)))
    vc_newish = vc.apply(_is_newish_condition)

    include = not_sold & (km_lt_100 | is_af | vc_newish)

    out = df.loc[include].copy()
    if "mileage_km" in out.columns:
        out["mileage_numeric"] = out["mileage_km"].apply(_parse_km)

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Wrote {len(out)} rows to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

