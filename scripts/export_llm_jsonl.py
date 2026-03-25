"""
Export an LLM-friendly JSONL file from the cleaned dataset.

Output format:
  - one JSON object per line (JSONL)
  - equipment as a real list[str]
  - description as plain text

Default filtering:
  - exclude sold listings (last_seen_at == 'sold')
  - include accident-free cars (is_accident_free == 1)

Run:
  python scripts/export_llm_jsonl.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PARQUET = BASE_DIR / "data" / "processed" / "cars_clean.parquet"
OUTPUT_JSONL = BASE_DIR / "data" / "processed" / "cars_accident_free_clean_for_llm.jsonl"


def _to_equipment_list(val: object) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val if str(x).strip()]
    s = str(val).strip()
    if not s:
        return []
    # In our parquet we store equipment as JSON string for compatibility.
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if str(x).strip()]
    except Exception:
        pass
    return [s]


def main() -> None:
    if not INPUT_PARQUET.exists():
        raise FileNotFoundError(
            f"Missing {INPUT_PARQUET}. Run: python scripts/clean_cars.py"
        )

    df = pd.read_parquet(INPUT_PARQUET)

    # Filter
    if "last_seen_at" in df.columns:
        df = df[df["last_seen_at"].fillna("") != "sold"]
    if "is_accident_free" in df.columns:
        df = df[df["is_accident_free"] == True]  # noqa: E712 (pandas boolean)

    # Keep only useful columns for an LLM
    cols = [
        "car_id",
        "url",
        "brand",
        "model",
        "srp_title",
        "price_current_eur",
        "price_first_eur",
        "price_checked_at",
        "mileage_km",
        "mileage_numeric",
        "first_registration",
        "power_kw",
        "power_hp",
        "fuel_type",
        "transmission",
        "vehicle_condition",
        "price_rating",
        "color",
        "color_manufacturer",
        "interior_design",
        "trim",
        "origin",
        "hu",
        "climatisation",
        "seller_type",
        "seller_rating",
        "ad_online_since",
        "equipment",
        "description",
        "source_search",
        "last_seen_at",
        "created_at",
    ]
    df = df[[c for c in cols if c in df.columns]].copy()

    # Normalize types
    if "equipment" in df.columns:
        df["equipment"] = df["equipment"].apply(_to_equipment_list)
    if "description" in df.columns:
        df["description"] = df["description"].fillna("").astype(str)
    # Ensure datetimes/NaT are JSON serializable
    for col in ("price_checked_at", "created_at", "ad_online_since"):
        if col in df.columns:
            df[col] = df[col].astype("string").fillna("")

    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSONL.open("w", encoding="utf-8") as f:
        for row in df.to_dict(orient="records"):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Wrote {len(df)} rows to {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()

