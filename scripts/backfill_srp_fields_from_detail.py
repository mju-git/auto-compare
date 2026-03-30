"""
Backfill SRP-canonical fields from detail pages for rows where SRP snapshot is missing.

We only use this for legacy rows where these fields are NULL:
  - srp_title
  - srp_price_raw
  - price_current_eur
  - price_first_eur
  - price_checked_at

Approach:
  - Visit the detail URL
  - Extract the *main* purchase price (not monthly/financing) using stable selectors
  - Parse to integer EUR and write to price_current_eur
  - Set price_first_eur if missing
  - Set srp_price_raw to the raw extracted price text
  - Set srp_title from existing brand+model if possible (fallback to h1 text)

Run:
  python scripts/backfill_srp_fields_from_detail.py
"""

from __future__ import annotations

import datetime as _dt
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


def _parse_eur_price_to_int(raw: str) -> int | None:
    if not raw:
        return None
    t = str(raw).replace("¹", "").replace("\xa0", " ").strip()
    num = re.sub(r"[^0-9,\.]", "", t)
    if not num:
        return None
    if "." in num and "," in num:
        if num.rfind(",") > num.rfind("."):
            num = num.replace(".", "").replace(",", ".")
        else:
            num = num.replace(",", "")
    else:
        if re.match(r"^\d{1,3}\.\d{3}$", num):
            num = num.replace(".", "")
        elif re.match(r"^\d{1,3},\d{3}$", num):
            num = num.replace(",", "")
        else:
            if "," in num and "." not in num:
                num = num.replace(",", "")
    try:
        v = int(float(num))
    except Exception:
        return None
    # Avoid monthly financing amounts like "1.589 €" by using a higher minimum.
    if not (5000 <= v <= 500000):
        return None
    return v


def extract_main_price_text(driver) -> str:
    # Prefer explicit "prime price" on detail page if present
    selectors = [
        "[data-testid='prime-price']",
        "[data-testid='price-block']",
        "[data-testid='main-price-label'] [data-testid='price-label']",
        "[data-testid*='price'] [data-testid='price-label']",
        "div.HBWcC",  # observed main price container on some variants
        ".price-block",
    ]
    for sel in selectors:
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            txt = (el.text or "").strip()
            if txt:
                return txt
        except Exception:
            continue
    # Fallback: known € container patterns
    try:
        el = driver.find_element(By.XPATH, "//div[starts-with(normalize-space(.), '€') and string-length(normalize-space(.)) <= 20]")
        txt = (el.text or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    # Fallback: elements starting with a number and containing €
    try:
        el = driver.find_element(
            By.XPATH,
            "//*[contains(normalize-space(.), '€') and string-length(normalize-space(.)) <= 20 and "
            "translate(substring(normalize-space(.),1,1),'0123456789','') = '']",
        )
        txt = (el.text or "").strip()
        if txt:
            return txt
    except Exception:
        pass
    # Fallback: first short element containing € with a plausible integer
    try:
        for el in driver.find_elements(By.XPATH, "//*[contains(text(),'€')]"):
            txt = (el.text or "").strip()
            if not txt or len(txt) > 40:
                continue
            v = _parse_eur_price_to_int(txt)
            if v is not None:
                return txt
    except Exception:
        pass
    # Last resort: parse candidates from HTML
    try:
        html = driver.page_source or ""
        candidates = re.findall(r"€\s*[0-9][0-9\.,]{3,}", html)
        best_txt = ""
        best_val = None
        for c in candidates[:200]:
            v = _parse_eur_price_to_int(c)
            if v is None:
                continue
            if best_val is None or v > best_val:
                best_val = v
                best_txt = c
        if best_txt:
            return best_txt
    except Exception:
        pass
    return ""


def extract_title_text(driver) -> str:
    for sel in ("[data-testid='prime-title']", "h1[data-testid='ad-title']", "h1"):
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            t = (el.text or "").strip()
            if t:
                return t
        except Exception:
            continue
    try:
        return (driver.title or "").strip()
    except Exception:
        return ""


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(DB_PATH)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA busy_timeout = 30000")
    rows = conn.execute(
        """
        SELECT car_id, url, brand, model
        FROM cars
        WHERE price_current_eur IS NULL
          AND (srp_title IS NULL OR srp_title = '')
          AND COALESCE(TRIM(last_seen_at), '') != 'sold'
        """
    ).fetchall()
    conn.close()

    if not rows:
        print("No rows need SRP backfill.")
        return

    print(f"Backfilling SRP fields from detail pages for {len(rows)} cars...")

    options = uc.ChromeOptions()
    options.add_experimental_option("prefs", {"intl.accept_languages": "en-US,en"})
    driver = uc.Chrome(version_main=145, options=options, headless=False)
    driver.set_page_load_timeout(30)

    now = _dt.datetime.now(_dt.UTC).isoformat()
    updated = 0
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000")
        for i, (car_id, url, brand, model) in enumerate(rows, start=1):
            print(f"[{i}/{len(rows)}] {car_id}", flush=True)
            try:
                driver.get(url)
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                time.sleep(1)

                raw_price = extract_main_price_text(driver)
                price_int = _parse_eur_price_to_int(raw_price)

                if price_int is None:
                    title = ""
                    try:
                        title = (driver.title or "").strip()
                    except Exception:
                        pass
                    marker = ""
                    try:
                        body_txt = (driver.find_element(By.TAG_NAME, "body").text or "")
                        if "no longer available" in body_txt.lower() or "nicht mehr verfügbar" in body_txt.lower():
                            marker = " (looks unavailable)"
                        elif "captcha" in body_txt.lower():
                            marker = " (captcha?)"
                    except Exception:
                        pass
                    if "access denied" in title.lower() or "zugriff verweigert" in title.lower():
                        print("  access denied detected; waiting 90s for manual solve, then retrying...", flush=True)
                        time.sleep(90)
                        try:
                            driver.get(url)
                            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
                            time.sleep(1)
                            raw_price = extract_main_price_text(driver)
                            price_int = _parse_eur_price_to_int(raw_price)
                        except Exception:
                            price_int = None
                        if price_int is None:
                            title2 = ""
                            try:
                                title2 = (driver.title or "").strip()
                            except Exception:
                                pass
                            print(f"  skip: still blocked | title='{title2}'", flush=True)
                            continue
                    else:
                        print(f"  skip: could not parse main price{marker} | title='{title}'", flush=True)
                    continue

                srp_title = ""
                bm = f"{(brand or '').strip()} {(model or '').strip()}".strip()
                if bm and len(bm.split()) >= 2:
                    srp_title = bm
                else:
                    srp_title = extract_title_text(driver)
                    # Keep it short; SRP title is typically "Brand MODEL"
                    parts = [p for p in re.split(r"\s+", (srp_title or "").strip()) if p]
                    if len(parts) >= 2:
                        srp_title = f"{parts[0]} {parts[1]}"
                    else:
                        srp_title = ""

                conn.execute(
                    """
                    UPDATE cars
                    SET srp_title = CASE WHEN srp_title IS NULL OR srp_title = '' THEN ? ELSE srp_title END,
                        srp_price_raw = CASE WHEN srp_price_raw IS NULL OR srp_price_raw = '' THEN ? ELSE srp_price_raw END,
                        price_current_eur = COALESCE(price_current_eur, ?),
                        price_first_eur = CASE WHEN price_first_eur IS NULL THEN ? ELSE price_first_eur END,
                        price_checked_at = COALESCE(price_checked_at, ?)
                    WHERE car_id = ?
                    """,
                    (srp_title, raw_price, price_int, price_int, now, car_id),
                )
                conn.commit()
                updated += 1
            except Exception as e:
                print(f"  skip: {e}", flush=True)
                continue
        conn.close()
    finally:
        driver.quit()

    print(f"Updated {updated} rows.")


if __name__ == "__main__":
    main()

