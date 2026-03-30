"""
Export a JSONL file for LLM analysis directly from the SQLite DB.

Filter:
  - only not-sold listings (last_seen_at != 'sold')

Output:
  - one JSON object per line
  - equipment parsed to list[str] when possible
  - datetimes coerced to string for JSON safety
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "raw" / "cars_market.db"
OUTPUT_JSONL = BASE_DIR / "data" / "processed" / "cars_not_sold_for_llm.jsonl"


COLS = [
    "car_id",
    "url",
    "brand",
    "model",
    "srp_title",
    "price_current_eur",
    "price_first_eur",
    "price_checked_at",
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


def _parse_equipment(val: object) -> list[str]:
    if val is None:
        return []
    if isinstance(val, list):
        return [str(x) for x in val if str(x).strip()]
    s = str(val).strip()
    if not s:
        return []
    try:
        parsed = json.loads(s)
        if isinstance(parsed, list):
            return [str(x) for x in parsed if str(x).strip()]
    except Exception:
        pass
    return [s]


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    sql = (
        "SELECT " + ", ".join([c for c in COLS if c != "created_at"]) + ", created_at "
        "FROM cars "
        "WHERE COALESCE(TRIM(last_seen_at), '') != 'sold' "
        "ORDER BY created_at"
    )
    rows = [dict(r) for r in conn.execute(sql).fetchall()]
    conn.close()

    for r in rows:
        # normalize
        r["equipment"] = _parse_equipment(r.get("equipment"))
        for k in ("price_checked_at", "created_at", "ad_online_since"):
            if k in r and r[k] is not None:
                r[k] = str(r[k])
            elif k in r:
                r[k] = ""

    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT_JSONL.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    print(f"Wrote {len(rows)} rows to {OUTPUT_JSONL}")


if __name__ == "__main__":
    main()

