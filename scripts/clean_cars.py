"""
Build the canonical cleaned dataset from raw SQLite/JSON.

Raw source of truth:
  - data/raw/cars_market.db (preferred)
  - data/raw/cars_market_data.json (fallback)

Clean outputs:
  - data/processed/cars_clean.parquet (canonical, analytics-ready)
  - data/processed/cars_clean_meta.json (run metadata + QA summary)

Run:
  python scripts/clean_cars.py
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import sqlite3
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parent.parent
RAW_JSON = BASE_DIR / "data" / "raw" / "cars_market_data.json"
RAW_DB = BASE_DIR / "data" / "raw" / "cars_market.db"
OUTPUT_PARQUET = BASE_DIR / "data" / "processed" / "cars_clean.parquet"
OUTPUT_META = BASE_DIR / "data" / "processed" / "cars_clean_meta.json"


KEEP_COLS = [
    "car_id",
    "url",
    "brand",
    "model",
    "trim",
    "origin",
    "price_current_eur",
    "price_first_eur",
    "mileage_km",
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
    "hu",
    "climatisation",
    "seller_type",
    "seller_rating",
    "equipment",
    "description",
    "last_seen_at",
    "created_at",
]


FEATURE_RULES: dict[str, list[str]] = {
    "has_carplay": ["apple carplay", "carplay"],
    "has_android_auto": ["android auto"],
    "has_leather": ["leder", "leather"],
    "has_partial_leather": ["teilleder", "partial leather"],
    "has_alcantara": ["alcantara"],
    "has_adaptive_cruise": ["abstandstempomat", "adaptive cruise", "acc"],
    "has_parking_sensors": ["einparkhilfe", "parking sensors"],
    "has_rear_camera": ["rückfahrkamera", "rear camera", "backup camera"],
    "has_360_camera": ["360", "surround view", "cam 360"],
    "has_heated_seats": ["sitzheizung", "heated seats"],
    "has_heated_steering_wheel": ["lenkrad heizbar", "heated steering wheel"],
    "has_panorama_roof": ["panorama", "panoramadach", "panoramic roof", "glass roof"],
    "has_head_up_display": ["head-up", "head up", "hud"],
}


def _parse_km(val: object) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    digits = re.sub(r"[^0-9]", "", s)
    return int(digits) if digits else None


def _parse_float(val: object) -> float | None:
    if val is None:
        return None
    s = str(val).strip().replace(",", ".")
    m = re.search(r"\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def _parse_int(val: object) -> int | None:
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    digits = re.sub(r"[^0-9]", "", s)
    return int(digits) if digits else None


def _parse_first_registration(s: object) -> tuple[int | None, int | None]:
    if s is None:
        return (None, None)
    t = str(s).strip()
    if not t:
        return (None, None)
    # expected: MM/YYYY
    m = re.match(r"^\s*(\d{1,2})\s*/\s*(\d{4})\s*$", t)
    if not m:
        return (None, None)
    mm = int(m.group(1))
    yy = int(m.group(2))
    if not (1 <= mm <= 12):
        return (None, None)
    if not (1980 <= yy <= 2100):
        return (None, None)
    return (mm, yy)


def _parse_equipment(val: object) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        out = []
        for x in val:
            s = str(x).strip()
            if s and s not in out:
                out.append(s)
        return out
    s = str(val).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            out = []
            for x in parsed:
                ss = str(x).strip()
                if ss and ss not in out:
                    out.append(ss)
            return out
    except Exception:
        pass
    return [s]


def _condition_norm(val: object) -> str:
    s = ("" if val is None else str(val)).strip().lower()
    if not s:
        return "unknown"
    if "unfallfrei" in s or "accident-free" in s or "accident free" in s:
        return "accident_free"
    if "tageszulassung" in s or "pre-registration" in s or "pre registration" in s:
        return "pre_registration"
    if "vorführ" in s or "vorfuehr" in s or "demo" in s or "demonstration" in s:
        return "demo"
    if "neuwagen" in s or "new car" in s:
        return "new"
    if "gebraucht" in s or "used vehicle" in s or "used" in s:
        return "used"
    return "unknown"


def _feature_flags(equipment_list: list[str]) -> dict[str, bool]:
    blob = " | ".join([e.lower() for e in equipment_list])
    flags = {}
    for col, needles in FEATURE_RULES.items():
        flags[col] = any(n.lower() in blob for n in needles)
    return flags


def load_raw() -> pd.DataFrame:
    if RAW_DB.exists():
        conn = sqlite3.connect(str(RAW_DB))
        df = pd.read_sql_query("SELECT * FROM cars", conn)
        conn.close()
        return df
    if RAW_JSON.exists():
        return pd.read_json(RAW_JSON)
    raise FileNotFoundError(f"Missing {RAW_DB} (or {RAW_JSON}). Run the scraper first.")


def build_clean(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    df = df.copy()

    # Keep a stable subset for analysis (drop scraper-only fields)
    keep = [c for c in KEEP_COLS if c in df.columns]
    df = df[keep].copy()

    # Normalize empties -> NA for consistent parsing
    df = df.replace(r"^\s*$", pd.NA, regex=True)

    # Dedupe by car_id
    if "car_id" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["car_id"], keep="last")
        deduped = before - len(df)
    else:
        deduped = 0

    # Sold flag
    if "last_seen_at" in df.columns:
        df["is_sold"] = df["last_seen_at"].fillna("").astype(str).str.strip().eq("sold")
    else:
        df["is_sold"] = False

    # Numeric conversions
    if "price_current_eur" in df.columns:
        df["price_current_eur_int"] = pd.to_numeric(df["price_current_eur"], errors="coerce").astype("Int64")
    if "price_first_eur" in df.columns:
        df["price_first_eur_int"] = pd.to_numeric(df["price_first_eur"], errors="coerce").astype("Int64")

    if "mileage_km" in df.columns:
        df["mileage_numeric"] = df["mileage_km"].apply(_parse_km).astype("Int64")

    if "seller_rating" in df.columns:
        df["seller_rating_numeric"] = df["seller_rating"].apply(_parse_float).astype("Float64")

    if "power_kw" in df.columns:
        df["power_kw_int"] = df["power_kw"].apply(_parse_int).astype("Int64")
    if "power_hp" in df.columns:
        df["power_hp_int"] = df["power_hp"].apply(_parse_int).astype("Int64")

    # First registration split
    if "first_registration" in df.columns:
        mm_yy = df["first_registration"].apply(_parse_first_registration)
        df["first_registration_month"] = [x[0] for x in mm_yy]
        df["first_registration_year"] = [x[1] for x in mm_yy]
        df["first_registration_month"] = pd.Series(df["first_registration_month"]).astype("Int64")
        df["first_registration_year"] = pd.Series(df["first_registration_year"]).astype("Int64")

    # Vehicle condition normalization
    if "vehicle_condition" in df.columns:
        df["vehicle_condition_norm"] = df["vehicle_condition"].apply(_condition_norm)
    else:
        df["vehicle_condition_norm"] = "unknown"

    # Equipment list + feature flags
    if "equipment" in df.columns:
        df["equipment_list"] = df["equipment"].apply(_parse_equipment)
    else:
        df["equipment_list"] = [[] for _ in range(len(df))]

    flags_df = pd.DataFrame([_feature_flags(x) for x in df["equipment_list"]])
    df = pd.concat([df.reset_index(drop=True), flags_df.reset_index(drop=True)], axis=1)

    # Minimal description cleanup (keep text, normalize whitespace)
    if "description" in df.columns:
        df["description_clean"] = (
            df["description"]
            .fillna("")
            .astype(str)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )

    # QA summary
    qa = {
        "rows_raw": int(len(df)),
        "deduped_by_car_id": int(deduped),
        "sold_count": int(df["is_sold"].sum()),
        "not_sold_count": int((~df["is_sold"]).sum()),
        "missing_price_current_int": int(df.get("price_current_eur_int", pd.Series([pd.NA] * len(df))).isna().sum()),
        "missing_mileage_numeric": int(df.get("mileage_numeric", pd.Series([pd.NA] * len(df))).isna().sum()),
    }

    return df, qa


def main() -> None:
    started = _dt.datetime.now(_dt.UTC).isoformat()
    print("Loading raw data...")
    raw = load_raw()
    print(f"  Loaded {len(raw)} rows")

    print("Cleaning / normalizing...")
    clean_df, qa = build_clean(raw)
    print(f"  Clean rows: {len(clean_df)}")

    OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    clean_df.to_parquet(OUTPUT_PARQUET, index=False)

    meta = {
        "created_at_utc": started,
        "output_parquet": str(OUTPUT_PARQUET),
        "qa": qa,
        "columns": list(clean_df.columns),
    }
    OUTPUT_META.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Saved: {OUTPUT_PARQUET}")
    print(f"Saved: {OUTPUT_META}")
    print("QA:", json.dumps(qa, ensure_ascii=False))


if __name__ == "__main__":
    main()
