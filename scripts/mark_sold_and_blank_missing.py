"""
Mark a subset of listings as sold and replace NULLs with empty strings.

This is intended for legacy/scrape-missed rows where SRP core fields are missing.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "raw" / "cars_market.db"


COND = (
    "price_first_eur IS NULL AND price_current_eur IS NULL "
    "AND (srp_title IS NULL OR TRIM(srp_title)='')"
)


def main() -> None:
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    rows = conn.execute(
        f"SELECT car_id, last_seen_at, url FROM cars WHERE {COND} ORDER BY COALESCE(last_seen_at,'') DESC"
    ).fetchall()

    print(f"Matched {len(rows)} rows.")
    for car_id, last_seen_at, url in rows:
        print(car_id, (last_seen_at or ""), url)

    # Update: mark sold + turn NULLs into empty strings for key columns.
    # (SQLite will happily store '' even in numeric columns; this is OK for your notebook workflow.)
    empty = ""
    conn.executemany(
        """
        UPDATE cars
        SET last_seen_at = 'sold',
            srp_title = COALESCE(srp_title, ?),
            srp_price_raw = COALESCE(srp_price_raw, ?),
            price_first_eur = COALESCE(price_first_eur, ?),
            price_current_eur = COALESCE(price_current_eur, ?),
            price_checked_at = COALESCE(price_checked_at, ?),
            vehicle_condition = COALESCE(vehicle_condition, ?),
            hu = COALESCE(hu, ?),
            trim = COALESCE(trim, ?),
            origin = COALESCE(origin, ?),
            interior_design = COALESCE(interior_design, ?),
            first_registration = COALESCE(first_registration, ?),
            mileage_km = COALESCE(mileage_km, ?),
            fuel_type = COALESCE(fuel_type, ?),
            transmission = COALESCE(transmission, ?),
            seller_rating = COALESCE(seller_rating, ?)
        WHERE car_id = ?
        """,
        [
            (
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                empty,
                car_id,
            )
            for (car_id, _, _) in rows
        ],
    )
    conn.commit()
    conn.close()
    print("Done.")


if __name__ == "__main__":
    main()

