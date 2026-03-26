"""
Backfill missing vehicle_condition in SQLite by visiting detail pages.

This is intended to fix legacy rows where SRP snapshot was missing.

Run:
  python scripts/backfill_vehicle_condition.py
"""

from __future__ import annotations

import re
import sqlite3
import time
from pathlib import Path

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "data" / "raw" / "cars_market.db"


def extract_vehicle_condition_from_detail(driver) -> str:
    # Label-based extraction first (EN+DE), then observed dd.nuAmT fallback.
    for label in ("Vehicle condition", "Fahrzeugzustand"):
        try:
            dd = driver.find_element(By.XPATH, f"//dt[normalize-space(.)='{label}']/following-sibling::dd[1]")
            txt = (dd.text or "").strip()
            if txt:
                return txt
        except Exception:
            pass
    try:
        dd = driver.find_element(By.CSS_SELECTOR, "dd.nuAmT")
        txt = (dd.text or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    return ""


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    rows = conn.execute(
        """
        SELECT car_id, url
        FROM cars
        WHERE vehicle_condition IS NULL OR TRIM(vehicle_condition) = ''
        """
    ).fetchall()
    conn.close()

    if not rows:
        print("No rows to backfill.")
        return

    print(f"Backfilling vehicle_condition for {len(rows)} cars...", flush=True)

    options = uc.ChromeOptions()
    options.add_experimental_option("prefs", {"intl.accept_languages": "en-US,en"})
    driver = uc.Chrome(version_main=145, options=options, headless=False)
    driver.set_page_load_timeout(30)

    updated = 0
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000")
        for i, (car_id, url) in enumerate(rows, start=1):
            print(f"[{i}/{len(rows)}] {car_id} ...", flush=True)
            try:
                driver.get(url)
                # wait for any dd to appear (page loaded)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "dd")))
                time.sleep(1)
                vc = extract_vehicle_condition_from_detail(driver)
                if vc:
                    # Retry update if DB is briefly locked (e.g. notebook open)
                    for attempt in range(6):
                        try:
                            conn.execute(
                                "UPDATE cars SET vehicle_condition = ? WHERE car_id = ? AND (vehicle_condition IS NULL OR TRIM(vehicle_condition) = '')",
                                (vc, car_id),
                            )
                            conn.commit()
                            updated += 1
                            break
                        except sqlite3.OperationalError as e:
                            if "locked" in str(e).lower() and attempt < 5:
                                time.sleep(1.5 * (attempt + 1))
                                continue
                            raise
            except Exception as e:
                print(f"  skip: {e}", flush=True)
                continue
        conn.close()
    finally:
        driver.quit()

    print(f"Updated {updated} rows.", flush=True)


if __name__ == "__main__":
    main()

