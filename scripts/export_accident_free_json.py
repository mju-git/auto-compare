"""
Build data/processed/cars_accident_free.json from the SQLite DB.

Include a row if:
  - last_seen_at is not 'sold', AND
  - (is_accident_free == 1 in the DB) OR (parsed km < 100).

Exclude:
  - Sold listings.
  - Cars with km >= 100 and not marked accident-free.

In the JSON, any row included only because km < 100 gets is_accident_free forced to 1.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "raw" / "cars_market.db"
OUTPUT_JSON = BASE_DIR / "data" / "processed" / "cars_accident_free.json"


def parse_km(raw: object) -> int | None:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    nums = re.sub(r"[^0-9]", "", s)
    return int(nums) if nums else None


def should_include(car: dict) -> bool:
    if (car.get("last_seen_at") or "").strip() == "sold":
        return False
    if car.get("is_accident_free") == 1:
        return True
    km = parse_km(car.get("mileage_km"))
    return km is not None and km < 100


def to_export_row(car: dict) -> dict:
    out = dict(car)
    if out.get("is_accident_free") != 1:
        km = parse_km(out.get("mileage_km"))
        if km is not None and km < 100:
            out["is_accident_free"] = 1
    return out


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    rows = [dict(r) for r in conn.execute("SELECT * FROM cars").fetchall()]
    conn.close()

    included = [to_export_row(r) for r in rows if should_include(r)]

    OUTPUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_JSON.write_text(
        json.dumps(included, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Wrote {len(included)} cars to {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
