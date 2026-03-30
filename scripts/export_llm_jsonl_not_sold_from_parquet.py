"""
Export not-sold cars to an LLM-friendly JSONL from cars_clean.parquet.

Output:
  data/processed/cars_not_sold_for_llm_from_parquet.jsonl
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
INPUT_PARQUET = BASE_DIR / "data" / "processed" / "cars_clean.parquet"
OUTPUT_JSONL = BASE_DIR / "data" / "processed" / "cars_not_sold_for_llm_from_parquet.jsonl"


COLS = [
    "car_id",
    "url",
    "brand",
    "model",
    "trim",
    "origin",
    "price_current_eur_int",
    "price_first_eur_int",
    "mileage_numeric",
    "first_registration_month",
    "first_registration_year",
    "power_kw_int",
    "power_hp_int",
    "fuel_type",
    "transmission",
    "vehicle_condition_norm",
    "color",
    "color_manufacturer",
    "interior_design",
    "seller_type",
    "seller_rating_numeric",
    "equipment_list",
    "description_clean",
]


def main() -> None:
    if not INPUT_PARQUET.exists():
        raise FileNotFoundError(f"Missing {INPUT_PARQUET}. Run: python scripts/clean_cars.py")

    df = pd.read_parquet(INPUT_PARQUET)
    if "is_sold" in df.columns:
        df = df[df["is_sold"] == False].copy()  # noqa: E712
    elif "last_seen_at" in df.columns:
        df = df[df["last_seen_at"].fillna("") != "sold"].copy()

    out = df[[c for c in COLS if c in df.columns]].copy()

    # Ensure JSON-safe types
    for c in out.columns:
        if str(out[c].dtype).startswith(("Int", "Float", "boolean")):
            # keep as python scalars / nulls
            pass
        else:
            out[c] = out[c].fillna("").astype(str)

    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSONL.open("w", encoding="utf-8") as f:
        for row in out.to_dict(orient="records"):
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Wrote {len(out)} rows to {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()

