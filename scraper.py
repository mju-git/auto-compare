"""
mobile.de car data scraper using undetected-chromedriver (Selenium).
Phase 1: Collect all listing URLs from search pagination.
Phase 2: Visit each listing and extract full car data.
"""

import datetime
import hashlib
import json
import os
import random
import re
import sqlite3
import sys
import time
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

# Work around Windows "The handle is invalid" (WinError 6) during time.sleep
# when used with Chrome/driver. Patch time.sleep so it never raises.
_original_sleep = time.sleep
def _safe_sleep(seconds: float) -> None:
    try:
        _original_sleep(seconds)
    except OSError as e:
        if getattr(e, "winerror", None) == 6:  # Windows: handle is invalid
            if seconds > 0.5:
                end = time.perf_counter() + seconds
                while time.perf_counter() < end:
                    try:
                        _original_sleep(0.05)
                    except OSError:
                        pass
        else:
            raise
time.sleep = _safe_sleep

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Configuration ---
BASE_DIR = Path(__file__).resolve().parent
DATA_RAW = BASE_DIR / "data" / "raw"
OUTPUT_FILE = DATA_RAW / "cars_market_data.json"
DB_PATH = DATA_RAW / "cars_market.db"
MIN_DELAY_SEC = 15
MAX_DELAY_SEC = 25
PAGE_LOAD_TIMEOUT = 30
# Hard stop for SRP pagination (prevents infinite loops if a wrong "Next" is clicked).
PHASE1_MAX_PAGES = 40
# Set True to delete existing DB/JSON and re-scrape from scratch. False = skip URLs already in DB.
CLEAR_BEFORE_RUN = False  # Changed to False to accumulate data across multiple searches

# If True: after collecting URLs for this search, remove DB rows that belong to THIS search
# fingerprint but were not in the current result set (likely sold / delisted).
# Safe with multiple searches: only rows tagged with the same search fingerprint are pruned.
PRUNE_NOT_IN_LATEST_SEARCH = True

# Query params ignored when fingerprinting a search URL (change between sessions but same logical search).
_SEARCH_FP_IGNORE_PARAMS = frozenset({"searchId", "refId", "ref", "pageNumber", "fn", "_", "lang"})


def _search_fingerprint(search_url: str) -> str:
    """Stable id for a logical search (same filters, ignoring session-specific query params)."""
    parsed = urlparse(search_url.strip())
    pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k not in _SEARCH_FP_IGNORE_PARAMS
    ]
    pairs.sort()
    canonical = f"{parsed.scheme}://{parsed.netloc}{parsed.path}?{urlencode(pairs)}"
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def _details_url_to_search_url(details_url: str) -> str:
    """Convert a listing/details URL into a search-like URL for fingerprinting.

    We do this because legacy DB rows may not have `source_search` recorded, but
    they still store query parameters that reflect the original search filters.
    """
    parsed = urlparse((details_url or "").strip())
    # Assume mobile.de SRP path. This matches your Phase-1 `search_url`.
    search_path = "/fahrzeuge/search.html"
    return urlunparse((parsed.scheme, parsed.netloc, search_path, "", parsed.query, ""))


def _backfill_source_search_for_legacy_rows() -> int:
    """Fill missing/empty `source_search` for older DB rows so pruning works."""
    if not PRUNE_NOT_IN_LATEST_SEARCH:
        return 0
    if not DB_PATH.exists():
        return 0

    conn = sqlite3.connect(DB_PATH)
    try:
        cols = [c[1] for c in conn.execute("PRAGMA table_info(cars)").fetchall()]
        if "source_search" not in cols:
            return 0

        rows = conn.execute(
            "SELECT car_id, url FROM cars WHERE source_search IS NULL OR source_search = ''"
        ).fetchall()
        if not rows:
            return 0

        updated = 0
        for car_id, url in rows:
            fp = _search_fingerprint(_details_url_to_search_url(url or ""))
            if not fp:
                continue
            conn.execute("UPDATE cars SET source_search = ? WHERE car_id = ?", (fp, car_id))
            updated += 1

        conn.commit()
        return updated
    finally:
        conn.close()


# Selectors (CSS and XPath for Selenium)
SELECTORS = {
    # Phase 1: listing page
    "listing_links": "a[href*='details.html'], a[href*='/fahrzeuge/details'], a[href*='details?id=']",
    "listing_links_fallback": "a[href*='mobile.de'][href*='details'], a[href*='details.html']",
    # SRP card fields (based on your pasted SRP HTML)
    "srp_card": "article[data-testid^='result-listing-'], article.A3G6X",
    "srp_link": "a[data-testid^='result-listing-'][href*='details.html']",
    "srp_brand_model": "h2 span.eO87w",
    "srp_variant": "h2 span.dc_Br",
    "srp_price": "[data-testid='main-price-label'] [data-testid='price-label'], [data-testid='price-label']",
    "srp_price_rating": "[data-testid='main-price-label'] ._u77E, ._u77E",
    # e.g. "Accident-free", "Used vehicle", "New car", "Pre-registration"
    "srp_vehicle_condition": "[data-testid='listing-details-attributes'] strong, [data-testid='listing-details'] strong",
    # "Ad online since <date>" on each listing card (search results)
    "online_since": "[data-testid='online-since']",
    # Next button: XPath (Selenium doesn't support :has-text in CSS)
    "next_button_xpath": [
        "//a[@rel='next']",
        "//a[contains(text(),'Nächste')]",
        "//button[contains(text(),'Nächste')]",
        "//a[contains(text(),'Weiter')]",
        "//button[contains(text(),'Weiter')]",
        "//a[contains(@aria-label,'Nächste')]",
        "//a[contains(@aria-label,'Next')]",
        "//a[contains(@aria-label,'Weiter')]",
        "//a[contains(@aria-label,'Continue')]",
        "//a[contains(@class,'next')]",
        "//button[@aria-label='Weiter']",
        "//button[@aria-label='Continue']",
    ],
    # Phase 2: detail page – technical data rows (label + value per row)
    "technical_data_rows": "div[class*='g-col-6'], div[class*='key-feature'], [class*='key-features'] div, .vehicle-details > div, .cBox-body > div",
    "key_features_section": "div[data-testid='key-features-section'], .key-features, .vehicle-details, .cBox-body",
    "specs_container": "[data-testid='vehicle-details'], .vehicle-details, dl, .cBox-body",
    "equipment_section": "[data-testid='equipment'], .equipment, .ausstattung",
    "equipment_items": "li",
    "description": "[data-testid='seller-comment'], .seller-comment, .description, .vehicle-description",
    # Title & price: prime title/price on mobile.de detail page, with fallbacks
    "title": "[data-testid='prime-title'], h1[data-testid='ad-title'], h1, .vehicle-title",
    "price": "[data-testid='prime-price'], [data-testid='price-block'], .price-block, .h2.u-block",
    "seller_dealer": "[data-testid='dealer-badge'], .dealer-badge",
    "seller_private": "[data-testid='private-seller']",
    # Vehicle description by seller (long text block, not search snippet)
    "vehicle_description": "[data-testid='seller-comment'], [data-testid='vehicle-description'], .seller-comment, .vehicle-description, [class*='VehicleDescription'], [class*='Fahrzeugbeschreibung']",
    # Price rating (mobile.de: button has data-testid="price-evaluation-click" and aria-label e.g. "Good price")
    "price_rating": "[data-testid='price-evaluation-click'], [data-testid='price-rating'], [class*='price-rating'], ._u77E",
}

DB_SCHEMA_VERSION = 3


def _db_has_column(conn: sqlite3.Connection, table: str, col: str) -> bool:
    cols = [c[1] for c in conn.execute(f"PRAGMA table_info({table})").fetchall()]
    return col in cols


def _migrate_db_to_v2() -> None:
    """Migrate cars table to v2 schema (drops title/technical_data/vehicle_id, adds SRP price fields).

    SQLite does not support DROP COLUMN reliably, so we create a new table and copy.
    Old table is kept as `cars_legacy` for rollback.
    """
    if not DB_PATH.exists():
        return

    conn = sqlite3.connect(DB_PATH)
    try:
        # If already migrated, skip
        if _db_has_column(conn, "cars", "price_current_eur") and _db_has_column(conn, "cars", "brand"):
            return

        # If a previous migration left temporary tables behind, clean up
        conn.execute("DROP TABLE IF EXISTS cars_v2")

        conn.execute(
            """
            CREATE TABLE cars_v2 (
                car_id TEXT PRIMARY KEY,
                url TEXT,

                brand TEXT,
                model TEXT,
                srp_title TEXT,
                srp_price_raw TEXT,
                price_first_eur INTEGER,
                price_current_eur INTEGER,
                price_checked_at TEXT,

                detail_price_raw TEXT,

                mileage_km TEXT,
                first_registration TEXT,
                power_hp TEXT,
                power_kw TEXT,
                number_of_owners TEXT,
                fuel_type TEXT,
                transmission TEXT,
                cubic_capacity TEXT,
                is_accident_free INTEGER,
                vehicle_condition TEXT,
                price_rating TEXT,
                color_manufacturer TEXT,
                color TEXT,
                interior_design TEXT,
                trim TEXT,
                origin TEXT,
                hu TEXT,
                climatisation TEXT,
                equipment TEXT,
                description TEXT,
                seller_type TEXT,
                seller_rating TEXT,
                ad_online_since TEXT,
                source_search TEXT,
                last_seen_at TEXT,
                created_at TEXT
            )
            """
        )

        # Copy what we can from legacy schema.
        # We keep legacy `price` as detail_price_raw for debugging.
        conn.execute(
            """
            INSERT INTO cars_v2 (
                car_id, url,
                brand, model, srp_title, srp_price_raw, price_first_eur, price_current_eur, price_checked_at,
                detail_price_raw,
                mileage_km, first_registration, power_hp, power_kw, number_of_owners, fuel_type, transmission,
                cubic_capacity, is_accident_free, vehicle_condition, price_rating, color_manufacturer, color, interior_design,
                trim, origin, hu, climatisation,
                equipment, description, seller_type, seller_rating, ad_online_since,
                source_search, last_seen_at, created_at
            )
            SELECT
                car_id, url,
                NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                price,
                mileage_km, first_registration, power_hp, power_kw, number_of_owners, fuel_type, transmission,
                cubic_capacity, is_accident_free, NULL, price_rating, color_manufacturer, color, interior_design,
                trim, origin, hu, climatisation,
                equipment, description, seller_type, seller_rating, ad_online_since,
                source_search, last_seen_at, created_at
            FROM cars
            """
        )

        # Swap tables
        conn.execute("ALTER TABLE cars RENAME TO cars_legacy")
        conn.execute("ALTER TABLE cars_v2 RENAME TO cars")

        # Basic indexes for lookup/update
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_source_search ON cars(source_search)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_last_seen_at ON cars(last_seen_at)")
        conn.commit()
        print("[DB] Migrated cars table to schema v2 (kept old as cars_legacy).")
    finally:
        conn.close()


def _migrate_db_drop_extraction_sources() -> None:
    """v3 migration: drop `extraction_sources` column from active `cars` table.

    Keeps backups:
      - current `cars` -> `cars_legacy_v2`
      - new table becomes `cars`
    """
    if not DB_PATH.exists():
        return
    conn = sqlite3.connect(DB_PATH)
    try:
        if not _db_has_column(conn, "cars", "extraction_sources"):
            return

        conn.execute("DROP TABLE IF EXISTS cars_v3")
        conn.execute(
            """
            CREATE TABLE cars_v3 (
                car_id TEXT PRIMARY KEY,
                url TEXT,

                brand TEXT,
                model TEXT,
                srp_title TEXT,
                srp_price_raw TEXT,
                price_first_eur INTEGER,
                price_current_eur INTEGER,
                price_checked_at TEXT,

                detail_price_raw TEXT,

                mileage_km TEXT,
                first_registration TEXT,
                power_hp TEXT,
                power_kw TEXT,
                number_of_owners TEXT,
                fuel_type TEXT,
                transmission TEXT,
                cubic_capacity TEXT,
                is_accident_free INTEGER,
                vehicle_condition TEXT,
                price_rating TEXT,
                color_manufacturer TEXT,
                color TEXT,
                interior_design TEXT,
                trim TEXT,
                origin TEXT,
                hu TEXT,
                climatisation TEXT,
                equipment TEXT,
                description TEXT,
                seller_type TEXT,
                seller_rating TEXT,
                ad_online_since TEXT,
                source_search TEXT,
                last_seen_at TEXT,
                created_at TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO cars_v3 (
                car_id, url,
                brand, model, srp_title, srp_price_raw, price_first_eur, price_current_eur, price_checked_at,
                detail_price_raw,
                mileage_km, first_registration, power_hp, power_kw, number_of_owners, fuel_type, transmission,
                cubic_capacity, is_accident_free, vehicle_condition, price_rating, color_manufacturer, color, interior_design,
                trim, origin, hu, climatisation,
                equipment, description, seller_type, seller_rating, ad_online_since,
                source_search, last_seen_at, created_at
            )
            SELECT
                car_id, url,
                brand, model, srp_title, srp_price_raw, price_first_eur, price_current_eur, price_checked_at,
                detail_price_raw,
                mileage_km, first_registration, power_hp, power_kw, number_of_owners, fuel_type, transmission,
                cubic_capacity, is_accident_free, vehicle_condition, price_rating, color_manufacturer, color, interior_design,
                trim, origin, hu, climatisation,
                equipment, description, seller_type, seller_rating, ad_online_since,
                source_search, last_seen_at, created_at
            FROM cars
            """
        )

        # Swap tables
        conn.execute("ALTER TABLE cars RENAME TO cars_legacy_v2")
        conn.execute("ALTER TABLE cars_v3 RENAME TO cars")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_source_search ON cars(source_search)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_cars_last_seen_at ON cars(last_seen_at)")
        conn.commit()
        print("[DB] Migrated cars table to schema v3 (dropped extraction_sources; kept cars_legacy_v2).")
    finally:
        conn.close()


def _parse_eur_price_to_int(raw: str) -> int | None:
    """Parse SRP prices like '€28,990' or '28.990 €' into integer euros."""
    if not raw:
        return None
    t = str(raw).strip()
    # Remove footnote markers like ¹ and whitespace
    t = t.replace("¹", "").replace("\xa0", " ").strip()
    # Keep digits and separators only
    num = re.sub(r"[^0-9,\.]", "", t)
    if not num:
        return None
    # If both separators exist, treat last separator as decimal, others thousands
    if "." in num and "," in num:
        if num.rfind(",") > num.rfind("."):
            num = num.replace(".", "")
            num = num.replace(",", ".")
        else:
            num = num.replace(",", "")
    else:
        # Only one separator type: interpret thousand separators
        if re.match(r"^\d{1,3}\.\d{3}$", num):
            num = num.replace(".", "")
        elif re.match(r"^\d{1,3},\d{3}$", num):
            num = num.replace(",", "")
        else:
            # Decimal commas are unlikely for SRP list prices, but handle safely
            if "," in num and "." not in num:
                num = num.replace(",", "")
    try:
        value = int(float(num))
    except Exception:
        return None
    # sanity check
    if not (1000 <= value <= 500000):
        return None
    return value


def _extract_srp_snapshot_for_link(driver, link_el) -> dict:
    """Extract SRP card snapshot for this listing link."""
    out = {
        "car_id": "",
        "url": "",
        "srp_title": "",
        "brand": "",
        "model": "",
        "srp_price_raw": "",
        "price_current_eur": None,
        "price_rating": "",
        "ad_online_since": "",
        "vehicle_condition": "",
    }
    try:
        href = _get_link_href(driver, link_el)
        out["url"] = href
        out["car_id"] = _extract_car_id_from_url(href)
    except Exception:
        pass
    try:
        card = link_el.find_element(By.XPATH, "./ancestor::article[1]")
    except Exception:
        card = None
    if card is None:
        return out

    # Headline text: brand/model in span.eO87w
    try:
        bm_el = card.find_element(By.CSS_SELECTOR, SELECTORS["srp_brand_model"])
        bm_text = (bm_el.text or "").strip()
        out["srp_title"] = bm_text
        parts = [p for p in re.split(r"\s+", bm_text) if p]
        if len(parts) >= 2:
            out["brand"] = parts[0]
            out["model"] = parts[1]
        elif len(parts) == 1:
            out["brand"] = parts[0]
    except Exception:
        pass

    # Price
    try:
        price_el = card.find_element(By.CSS_SELECTOR, SELECTORS["srp_price"])
        raw = (price_el.text or "").strip()
        out["srp_price_raw"] = raw
        out["price_current_eur"] = _parse_eur_price_to_int(raw)
    except Exception:
        pass

    # Price rating label (e.g. "Good price")
    try:
        pr = card.find_element(By.CSS_SELECTOR, SELECTORS["srp_price_rating"])
        out["price_rating"] = (pr.text or "").strip()
    except Exception:
        pass

    # Online since
    try:
        out["ad_online_since"] = _get_online_since_for_link(driver, link_el)
    except Exception:
        pass
    # Vehicle condition badge ("Accident-free", "Used vehicle", "New car", ...)
    try:
        vc = card.find_element(By.CSS_SELECTOR, SELECTORS["srp_vehicle_condition"])
        out["vehicle_condition"] = (vc.text or "").strip()
    except Exception:
        pass
    return out


def _accident_free_from_rule(vehicle_condition: str, mileage_km: str) -> int | None:
    """Return 1/0 from the rule, or None if we can't decide (e.g. missing mileage + no explicit condition)."""
    cond = (vehicle_condition or "").strip().lower()
    if "accident-free" in cond or "unfallfrei" in cond:
        return 1
    try:
        digits = re.sub(r"[^0-9]", "", str(mileage_km or ""))
        km = int(digits) if digits else None
    except Exception:
        km = None
    if km is None:
        return None
    return 1 if km < 100 else 0


def _init_db() -> None:
    """Create SQLite DB and table if not exist."""
    conn = sqlite3.connect(DB_PATH)
    conn.close()
    # Auto-migrate to v2 once DB exists
    _migrate_db_to_v2()
    # v3 migration (drop extraction_sources)
    _migrate_db_drop_extraction_sources()
    # Additive columns for existing v2 DBs
    conn = sqlite3.connect(DB_PATH)
    try:
        if not _db_has_column(conn, "cars", "vehicle_condition"):
            conn.execute("ALTER TABLE cars ADD COLUMN vehicle_condition TEXT")
            conn.commit()
    finally:
        conn.close()


def _save_car_to_db(car: dict) -> None:
    """Insert or replace one car (by car_id extracted from URL)."""

    # Extract car_id from URL for deduplication
    url = car.get("url", "")
    car_id = _extract_car_id_from_url(url)
    
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        INSERT OR REPLACE INTO cars (
            car_id, url,
            brand, model, srp_title, srp_price_raw, price_first_eur, price_current_eur, price_checked_at,
            detail_price_raw,
            mileage_km, first_registration, power_hp, power_kw,
            number_of_owners, fuel_type, transmission, cubic_capacity, is_accident_free,
            vehicle_condition, price_rating, color_manufacturer, color, interior_design,
            trim, origin, hu, climatisation,
            equipment, description, seller_type, seller_rating, ad_online_since, source_search, last_seen_at, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        car_id,
        url,
        car.get("brand", ""),
        car.get("model", ""),
        car.get("srp_title", ""),
        car.get("srp_price_raw", ""),
        car.get("price_first_eur", None),
        car.get("price_current_eur", None),
        car.get("price_checked_at", ""),
        car.get("detail_price_raw", ""),
        car.get("mileage_km", ""),
        car.get("first_registration", ""),
        car.get("power_hp", ""),
        car.get("power_kw", ""),
        car.get("number_of_owners", ""),
        car.get("fuel_type", ""),
        car.get("transmission", ""),
        car.get("cubic_capacity", ""),
        1 if car.get("is_accident_free") else 0,
        car.get("vehicle_condition", ""),
        car.get("price_rating", ""),
        car.get("color_manufacturer", ""),
        car.get("color", ""),
        car.get("interior_design", ""),
        car.get("trim", ""),
        car.get("origin", ""),
        car.get("hu", ""),
        car.get("climatisation", ""),
        json.dumps(car.get("equipment") or [], ensure_ascii=False),
        car.get("description", ""),
        car.get("seller_type", ""),
        car.get("seller_rating", ""),
        car.get("ad_online_since", ""),
        car.get("source_search", ""),
        car.get("last_seen_at", ""),
        datetime.datetime.utcnow().isoformat(),
    ))
    conn.commit()
    conn.close()


def _extract_car_id_from_url(url: str) -> str:
    """Extract the car ID from a mobile.de URL for deduplication.
    Example: https://suchen.mobile.de/fahrzeuge/details.html?id=450332609&... -> 450332609
    """
    import re
    # Try to find id= parameter
    m = re.search(r'[?&]id=(\d+)', url)
    if m:
        return m.group(1)
    # Try to find id in path like /details/450332609
    m = re.search(r'/details[/.](\d+)', url)
    if m:
        return m.group(1)
    # Fallback to full URL
    return url


def _load_existing_urls_from_db() -> set[str]:
    """Return set of URLs already in DB (for resume / dedupe)."""
    if not DB_PATH.exists():
        return set()
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT url FROM cars").fetchall()
        conn.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _load_existing_car_ids_from_db() -> set[str]:
    """Return set of car IDs already in DB (for better deduplication).
    This is more reliable than URL matching since the same car can have different URL params.
    """
    if not DB_PATH.exists():
        return set()
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("SELECT url FROM cars").fetchall()
        conn.close()
        return {_extract_car_id_from_url(r[0]) for r in rows}
    except Exception:
        return set()


def _clear_db_and_json() -> None:
    """Remove DB file and clear JSON so next run starts fresh."""
    if DB_PATH.exists():
        DB_PATH.unlink()
        print(f"Cleared database: {DB_PATH}")
    if OUTPUT_FILE.exists():
        OUTPUT_FILE.write_text("[]", encoding="utf-8")
        print(f"Cleared JSON: {OUTPUT_FILE}")


def _prune_stale_listings_for_search(fingerprint: str, keep_car_ids: set[str]) -> int:
    """Mark rows as sold that belong to this search fingerprint but are not in keep_car_ids.

    Uses a temp table so large result sets do not hit SQLite's parameter limits.
    Rows with source_search NULL (legacy) are never removed here.
    """
    if not fingerprint or not keep_car_ids:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DROP TABLE IF EXISTS _prune_keep")
        conn.execute("CREATE TEMP TABLE _prune_keep (id TEXT PRIMARY KEY)")
        conn.executemany("INSERT OR IGNORE INTO _prune_keep (id) VALUES (?)", [(cid,) for cid in keep_car_ids])
        cur = conn.execute(
            """
            UPDATE cars
            SET last_seen_at = 'sold'
            WHERE source_search = ?
              AND car_id NOT IN (SELECT id FROM _prune_keep)
            """,
            (fingerprint,),
        )
        deleted = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        return deleted
    finally:
        conn.close()


def _mark_seen_car_ids(keep_car_ids: set[str], seen_at_iso: str) -> int:
    """Set last_seen_at for all car_ids found in current SRP (including skipped ones)."""
    if not keep_car_ids:
        return 0
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DROP TABLE IF EXISTS _seen_keep")
        conn.execute("CREATE TEMP TABLE _seen_keep (id TEXT PRIMARY KEY)")
        conn.executemany("INSERT OR IGNORE INTO _seen_keep (id) VALUES (?)", [(cid,) for cid in keep_car_ids])
        cur = conn.execute(
            """
            UPDATE cars
            SET last_seen_at = ?
            WHERE car_id IN (SELECT id FROM _seen_keep)
            """,
            (seen_at_iso,),
        )
        updated = cur.rowcount if cur.rowcount is not None else 0
        conn.commit()
        return updated
    finally:
        conn.close()


def _full_url(base_url: str, path: str) -> str:
    if path.startswith("http"):
        return path
    parsed = urlparse(base_url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    return urljoin(base, path)


def _find_element_text(driver, by: By, value: str, default: str = "") -> str:
    try:
        el = driver.find_element(by, value)
        return (el.text or "").strip()
    except Exception:
        return default


def _find_first_by_css(driver, selectors: str) -> str:
    """Try each comma-separated CSS selector; return text of first match."""
    for sel in (s.strip() for s in selectors.split(",") if s.strip()):
        try:
            el = driver.find_element(By.CSS_SELECTOR, sel)
            return (el.text or "").strip()
        except Exception:
            continue
    return ""


def _find_all_by_css(driver, selector: str):
    try:
        return driver.find_elements(By.CSS_SELECTOR, selector)
    except Exception:
        return []


def _get_link_href(driver, el) -> str:
    """Robust href extraction for AMP/SPA SRP variants."""
    try:
        href = (el.get_attribute("href") or "").strip()
        if href:
            return href
    except Exception:
        pass
    try:
        href = driver.execute_script("return arguments[0].href || '';", el)
        return (href or "").strip()
    except Exception:
        return ""


def _pagination_element_is_actionable(el) -> bool:
    """Skip disabled / non-interactive pagination controls."""
    try:
        if not el.is_displayed():
            return False
        if not el.is_enabled():
            return False
        if el.get_attribute("aria-disabled") == "true":
            return False
        tag = (el.tag_name or "").lower()
        if tag == "a":
            href = (el.get_attribute("href") or "").strip()
            rel = (el.get_attribute("rel") or "").lower()
            if href in ("", "#") and "next" not in rel:
                return False
        cls = (el.get_attribute("class") or "").lower()
        if "disabled" in cls or "is-disabled" in cls:
            return False
    except Exception:
        return False
    return True


def _find_next_button(driver) -> bool:
    """Click the real search-results 'next' control only.

    Broad selectors like ``aria-label*='Next'`` match carousels/ads and caused
    infinite pagination while listing count stayed flat. We prefer ``rel='next'``,
    explicit pagination testids, and XPath text matches; we avoid generic
    page-wide Next/Weiter buttons.
    """
    try:
        current_url = driver.current_url
        current_url_param = re.search(r"[&?]p=(\d+)", current_url)
        current_page = int(current_url_param.group(1)) if current_url_param else 1

        # 1) rel=next (strong signal for real pagination)
        for css in (
            "nav a[rel='next']",
            "[data-testid*='pagination'] a[rel='next']",
            "a[rel='next']",
        ):
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, css):
                    if not _pagination_element_is_actionable(el):
                        continue
                    print(f"   [Pagination] Clicking rel=next (page ~{current_page})...")
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.3)
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(2)
                    print(f"   [Pagination] URL changed: {driver.current_url != current_url}")
                    return True
            except Exception:
                continue

        # 2) Known XPaths (Nächste / Weiter on SRP)
        for xpath in SELECTORS["next_button_xpath"]:
            try:
                for el in driver.find_elements(By.XPATH, xpath):
                    if not _pagination_element_is_actionable(el):
                        continue
                    print(f"   [Pagination] Clicking next via XPath (page ~{current_page})...")
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.3)
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(2)
                    print(f"   [Pagination] URL changed: {driver.current_url != current_url}")
                    return True
            except Exception:
                continue

        # 3) Strict: pagination testids only (no global Next/Weiter)
        for sel in ("[data-testid='pagination:next']", "[data-testid='pagination-next']"):
            try:
                for el in driver.find_elements(By.CSS_SELECTOR, sel):
                    if not _pagination_element_is_actionable(el):
                        continue
                    print(f"   [Pagination] Clicking {sel!r}...")
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                    time.sleep(0.3)
                    driver.execute_script("arguments[0].click();", el)
                    time.sleep(2)
                    print(f"   [Pagination] URL changed: {driver.current_url != current_url}")
                    return True
            except Exception:
                continue

    except Exception as e:
        print(f"   [Pagination] Error finding next button: {e}")

    print("   [Pagination] No suitable next control found, stopping pagination")
    return False


def _is_detail_url(href: str, base_domain: str) -> bool:
    if not href or "mobile.de" not in href:
        return False
    href_l = href.lower()
    # Standard detail URLs
    if "details" in href_l and ("id=" in href_l or "details.html" in href_l):
        return True
    # Some SRP/AMP variants may omit explicit "details" but keep fahrzeuge + id.
    if "/fahrzeuge/" in href_l and "id=" in href_l:
        return True
    return False


def _page_is_access_denied(driver) -> bool:
    try:
        text = driver.find_element(By.TAG_NAME, "body").text
        return "Access denied" in text or "automated access" in text.lower()
    except Exception:
        return False


def _find_might_also_interest_cutoff_element(driver):
    """Return the DOM element where recommendation cards start.

    mobile.de sometimes appends a "You might also be interested" / recommendation
    section on the last SRP page. We want to avoid scraping those unrelated
    listings.

    **Important:** A naive XPath like ``//*[contains(normalize-space(.), '…')]`` matches
    ``html`` / ``body`` first, because *normalize-space(.)* is the entire subtree text.
    Then every listing link is *contained by* that cutoff and
    ``compareDocumentPosition`` never sets FOLLOWING — Phase 1 collects zero URLs.
    We only accept **short** nodes (section headings), not page roots.
    """
    # Headings / labels are short; html/body are thousands of characters.
    _max_own_text_len = 320

    # Longer phrases first so we anchor on the real section title, not a stray short blurb.
    markers = [
        "Ähnliche Fahrzeuge, die teilweise Deinen Suchkriterien entsprechen",
        "Similar vehicles partially matching your search criteria",
        "Ähnliche Fahrzeuge",
        "Das könnte Sie auch interessieren",
        "Das könnte Sie interessieren",
        "Möglicherweise interessiert",
        "Das könnte Sie auch mögen",
        "Das könnte Ihnen auch gefallen",
        "You might also be interested",
        "You might be interested",
        "You might also like",
        "This might interest you",
        "Similar vehicles partially matching",
        "More like this",
    ]
    for marker in markers:
        if "'" in marker and '"' in marker:
            continue
        lit = f'"{marker}"' if "'" in marker else f"'{marker}'"
        xp = (
            f"//*[contains(normalize-space(.), {lit}) "
            f"and string-length(normalize-space(.)) <= {_max_own_text_len}]"
        )
        try:
            for el in driver.find_elements(By.XPATH, xp):
                try:
                    if el.is_displayed():
                        return el
                except Exception:
                    continue
            els = driver.find_elements(By.XPATH, xp)
            if els:
                return els[0]
        except Exception:
            continue
    return None


def _is_link_before_recommendation_section(driver, link_el, cutoff_el) -> bool:
    """True if link appears in DOM before recommendation cutoff element."""
    if cutoff_el is None:
        return True
    try:
        # If the cutoff contains the link, cutoff is an ancestor (e.g. mis-picked html/body).
        # In that case FOLLOWING is never set — treat as no cutoff so we do not drop all links.
        if driver.execute_script("return arguments[1].contains(arguments[0]);", link_el, cutoff_el):
            return True
        return bool(
            driver.execute_script(
                "return !!(arguments[0].compareDocumentPosition(arguments[1]) & Node.DOCUMENT_POSITION_FOLLOWING);",
                link_el,
                cutoff_el,
            )
        )
    except Exception:
        # Fail-open to avoid dropping real result links if JS check fails.
        return True


def _click_show_more_sections(driver) -> None:
    """
    Click ALL 'show more' / 'Mehr anzeigen' buttons on the page to expand:
    - Technical Data (Technische Daten)
    - Features/Equipment (Ausstattung)
    - Vehicle description (Fahrzeugbeschreibung)
    
    Uses JavaScript clicks as primary method to bypass any overlay/interception issues.
    """
    # First, dismiss any cookie banners or overlays that might block clicks
    try:
        # Common cookie consent / close button patterns
        for selector in [
            "//button[contains(text(), 'Akzeptieren')]",
            "//button[contains(text(), 'Accept')]",
            "//button[contains(text(), 'Alle akzeptieren')]",
            "//button[@aria-label='close']",
            "//button[@aria-label='Close']",
            "//button[contains(@class, 'close')]",
            "//*[@id='onetrust-accept-btn-handler']",
        ]:
            try:
                close_btns = driver.find_elements(By.XPATH, selector)
                for cb in close_btns:
                    if cb.is_displayed():
                        driver.execute_script("arguments[0].click();", cb)
                        time.sleep(0.5)
            except Exception:
                pass
    except Exception:
        pass
    
    # Wait for page to be ready and scroll to load lazy content
    try:
        time.sleep(2)
        
        # Scroll down the page to trigger lazy loading
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 3);")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight * 0.7);")
        time.sleep(0.5)
        driver.execute_script("window.scrollTo(0, 0);")  # Scroll back to top
        time.sleep(1)
    except Exception:
        pass
    
    # Strategy: Use JavaScript to click ALL "Mehr anzeigen" buttons
    # JavaScript clicks bypass any overlay/interception issues
    clicked_count = 0
    
    try:
        # Use JavaScript to find and click all "Mehr anzeigen" buttons
        clicked_count = driver.execute_script("""
            var buttons = document.querySelectorAll('button');
            var clicked = 0;
            for (var i = 0; i < buttons.length; i++) {
                var btn = buttons[i];
                var text = (btn.textContent || '').toLowerCase().trim();
                // Click "Mehr anzeigen" or "Show more" buttons
                if ((text.includes('mehr anzeigen') || text.includes('show more')) && 
                    !text.includes('versicherung') && !text.includes('check24')) {
                    try {
                        btn.scrollIntoView({block: 'center'});
                        btn.click();
                        clicked++;
                    } catch(e) {}
                }
            }
            return clicked;
        """)
        if clicked_count and clicked_count > 0:
            print(f"    [DEBUG] Clicked {clicked_count} 'Mehr anzeigen' buttons via JS")
            time.sleep(2)  # Wait for content to expand
    except Exception as e:
        print(f"    [DEBUG] JS click failed: {e}")
    
    # Second pass: Try again for any new buttons that appeared after expansion
    try:
        time.sleep(1)
        driver.execute_script("""
            var buttons = document.querySelectorAll('button');
            for (var i = 0; i < buttons.length; i++) {
                var btn = buttons[i];
                var text = (btn.textContent || '').toLowerCase().trim();
                if ((text.includes('mehr anzeigen') || text.includes('show more')) && 
                    !text.includes('versicherung') && !text.includes('check24')) {
                    try {
                        btn.scrollIntoView({block: 'center'});
                        btn.click();
                    } catch(e) {}
                }
            }
        """)
        time.sleep(1.5)
    except Exception:
        pass
    
    # Also try data-testid based selectors for equipment (using JS click)
    try:
        for xp in [
            "[data-testid='vip-features-show-more']",
        ]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, xp)
                if el.is_displayed():
                    driver.execute_script("arguments[0].scrollIntoView({block:'center'}); arguments[0].click();", el)
                    time.sleep(1.5)
            except Exception:
                continue
    except Exception:
        pass


def _srp_url_with_page_number(url: str, page: int) -> str:
    """Build SRP URL with pageNumber=N. Preserves duplicate query keys (e.g. multiple ``it=``).

    mobile.de often advances results via JS without updating ``driver.current_url``; using
    explicit ``pageNumber`` is reliable for collecting all listing pages.
    """
    parsed = urlparse(url)
    pairs = [
        (k, v)
        for k, v in parse_qsl(parsed.query, keep_blank_values=True)
        if k.lower() != "pagenumber"
    ]
    pairs.append(("pageNumber", str(page)))
    new_query = urlencode(pairs)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment))


def _get_online_since_for_link(driver, link_el) -> str:
    """Get 'Ad online since <date>' text from the listing card that contains this link."""
    try:
        # First ancestor of the link that contains a [data-testid="online-since"] (the listing card)
        card = link_el.find_element(
            By.XPATH,
            "./ancestor::*[.//*[@data-testid='online-since']][1]"
        )
        el = card.find_element(By.CSS_SELECTOR, "[data-testid='online-since']")
        return (el.text or "").strip()
    except Exception:
        return ""


def _is_main_srp_result_link(link_el) -> bool:
    """True if link belongs to a primary search-result card.

    The recommendation block ("Similar vehicles ...") often lacks the
    `online-since` marker. Requiring this marker helps avoid unrelated cards.
    """
    try:
        link_el.find_element(
            By.XPATH,
            "./ancestor::*[.//*[@data-testid='online-since']][1]"
        )
        return True
    except Exception:
        return False


def _is_recommendation_block_link(link_el) -> bool:
    """True if link is inside known recommendation/similar blocks."""
    rec_markers = [
        "Ähnliche Fahrzeuge, die teilweise Deinen Suchkriterien entsprechen",
        "Ähnliche Fahrzeuge",
        "Similar vehicles partially matching your search criteria",
        "You might also be interested",
        "You might be interested",
        "You might also like",
        "More like this",
        "Das könnte Sie auch interessieren",
        "Das könnte Sie interessieren",
        "Das könnte Ihnen auch gefallen",
    ]
    for marker in rec_markers:
        try:
            link_el.find_element(
                By.XPATH,
                f"./ancestor::*[contains(normalize-space(.), \"{marker}\")][1]"
            )
            return True
        except Exception:
            continue
    return False


def _extract_reported_results_count(driver) -> int | None:
    """Try to read the results count shown at top of SRP."""
    try:
        text = driver.find_element(By.TAG_NAME, "body").text or ""
    except Exception:
        return None
    # English: "17 results", German: "17 Ergebnisse"
    m = re.search(r"\b(\d{1,5})\s+(?:results|Ergebnisse)\b", text, re.I)
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def phase1_collect_urls(driver, search_url: str, base_domain: str) -> tuple[list[str], dict[str, str], dict[str, dict]]:
    """Navigate search result pages and collect all car detail URLs and their 'ad online since' text.
    Returns (list of detail URLs, dict mapping URL -> 'Ad online since <date>' text, srp_snapshot_by_car_id).
    """
    detail_urls: list[str] = []
    url_to_online_since: dict[str, str] = {}
    srp_by_car_id: dict[str, dict] = {}
    seen: set[str] = set()
    page_num = 1
    reported_total = None

    # Remove pageNumber from URL to always start from page 1
    import re as re_module
    clean_url = re_module.sub(r'[&?]pageNumber=\d+', '', search_url)
    if clean_url != search_url:
        print(f"[Phase 1] Note: Removed pageNumber from URL to start from page 1")
        search_url = clean_url
    
    print(f"[Phase 1] Opening search: {search_url}")
    driver.get(search_url)
    print("[Phase 1] If you see a cookie consent or CAPTCHA, handle it now. Waiting 10s for the page to load...")
    time.sleep(10)

    try:
        print(f"[Phase 1] Page loaded: URL={driver.current_url[:80]}... title={driver.title!r}")
    except Exception:
        pass
    reported_total = _extract_reported_results_count(driver)
    if reported_total:
        print(f"[Phase 1] Reported results on page: {reported_total}")

    if _page_is_access_denied(driver):
        print("\n*** ACCESS DENIED: mobile.de has blocked automated access. ***")
        print("For official data access, contact mobile.de: service@team.mobile.de")
        print("Browser will stay open 45s.")
        time.sleep(45)
        return [], {}

    while True:
        if page_num > PHASE1_MAX_PAGES:
            print(f"[Phase 1] Stopping: reached PHASE1_MAX_PAGES ({PHASE1_MAX_PAGES}).")
            break

        count_at_start = len(detail_urls)
        has_recommendation_section = False

        # Scroll so lazy-loaded listing cards and links are in the DOM
        try:
            for _ in range(6):
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(0.35)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.6)
        except Exception:
            pass
        try:
            cutoff_el = _find_might_also_interest_cutoff_element(driver)
            has_recommendation_section = cutoff_el is not None
            links = _find_all_by_css(driver, SELECTORS["listing_links"])
            strict_added = 0
            for el in links:
                if not _is_main_srp_result_link(el):
                    continue
                if not _is_link_before_recommendation_section(driver, el, cutoff_el):
                    continue
                href = _get_link_href(driver, el)
                if not href or not _is_detail_url(href, base_domain):
                    continue
                full = _full_url(base_domain, href)
                if full not in seen:
                    seen.add(full)
                    detail_urls.append(full)
                    strict_added += 1
                    online_since = _get_online_since_for_link(driver, el)
                    if online_since:
                        url_to_online_since[full] = online_since
                    snap = _extract_srp_snapshot_for_link(driver, el)
                    if snap.get("car_id"):
                        snap["url"] = full
                        srp_by_car_id[snap["car_id"]] = snap
            # Some SRP variants do not expose online-since markers on regular cards.
            # If strict filtering found nothing, retry with a looser pass but still
            # keep recommendation section exclusion.
            if strict_added == 0 and links:
                for el in links:
                    if not _is_link_before_recommendation_section(driver, el, cutoff_el):
                        continue
                    href = _get_link_href(driver, el)
                    if not href or not _is_detail_url(href, base_domain):
                        continue
                    full = _full_url(base_domain, href)
                    if full not in seen:
                        seen.add(full)
                        detail_urls.append(full)
                        online_since = _get_online_since_for_link(driver, el)
                        if online_since:
                            url_to_online_since[full] = online_since
                        snap = _extract_srp_snapshot_for_link(driver, el)
                        if snap.get("car_id"):
                            snap["url"] = full
                            srp_by_car_id[snap["car_id"]] = snap
                if page_num == 1:
                    print("[Phase 1] SRP variant without online-since marker detected; used loose link pass.")
            if not links and page_num == 1:
                links_fb = _find_all_by_css(driver, SELECTORS["listing_links_fallback"])
                for el in links_fb:
                    if not _is_link_before_recommendation_section(driver, el, cutoff_el):
                        continue
                    href = _get_link_href(driver, el)
                    if href and _is_detail_url(href, base_domain):
                        full = _full_url(base_domain, href)
                        if full not in seen:
                            seen.add(full)
                            detail_urls.append(full)
                            online_since = _get_online_since_for_link(driver, el)
                            if online_since:
                                url_to_online_since[full] = online_since
                            snap = _extract_srp_snapshot_for_link(driver, el)
                            if snap.get("car_id"):
                                snap["url"] = full
                                srp_by_car_id[snap["car_id"]] = snap
                if detail_urls:
                    print("[Phase 1] Fallback: collected URLs from broader link selector.")
        except Exception as e:
            print(f"[Phase 1] Warning extracting links on page {page_num}: {e}")

        new_this_page = len(detail_urls) - count_at_start
        print(
            f"[Phase 1] Page {page_num}: +{new_this_page} new this page, "
            f"{len(detail_urls)} total listing URLs so far."
        )

        # If top-of-page reports total results, treat it as a hard upper bound.
        # This protects against accidental leakage from recommendation sections.
        if page_num == 1 and reported_total and len(detail_urls) > reported_total:
            detail_urls = detail_urls[:reported_total]
            seen = set(detail_urls)
            url_to_online_since = {u: url_to_online_since[u] for u in detail_urls if u in url_to_online_since}
            new_this_page = len(detail_urls) - count_at_start
            print(
                f"[Phase 1] Trimmed to reported total ({reported_total}) to exclude non-result links."
            )

        # Single-page searches commonly show recommendations below the real results.
        # If page 1 has fewer than one full page of main results and recommendation
        # section is already present, treat it as end-of-results.
        if page_num == 1 and has_recommendation_section and new_this_page < 30:
            print(
                "[Phase 1] Recommendation section detected on page 1 with fewer than 30 results; "
                "treating as single-page search and stopping pagination."
            )
            break
        if page_num == 1 and reported_total and len(detail_urls) >= reported_total:
            print(
                f"[Phase 1] Collected {len(detail_urls)} links, meeting reported total ({reported_total}); "
                "stopping pagination."
            )
            break

        if new_this_page == 0:
            if page_num == 1 and len(detail_urls) == 0:
                print("[Phase 1] No listings on the first page.")
            elif page_num > 1:
                print(
                    f"[Phase 1] No new listings on page {page_num} (end of results or empty page). "
                    "Stopping pagination."
                )
            break

        # Next page: use pageNumber in URL (reliable; clicks often don't change URL on SPA SRP).
        next_page = page_num + 1
        if next_page > PHASE1_MAX_PAGES:
            print(f"[Phase 1] Stopping: next page would exceed PHASE1_MAX_PAGES ({PHASE1_MAX_PAGES}).")
            break
        try:
            base_for_paging = driver.current_url or search_url
            next_url = _srp_url_with_page_number(base_for_paging, next_page)
        except Exception as e:
            print(f"[Phase 1] Could not build next-page URL: {e}. Trying click-based pagination.")
            if not _find_next_button(driver):
                print(f"[Phase 1] No more pages (stopped at page {page_num}).")
                break
            page_num += 1
            time.sleep(1 + random.uniform(0.5, 1.5))
            continue

        print(f"[Phase 1] Loading search page {next_page} (pageNumber={next_page})...")
        driver.get(next_url)
        time.sleep(2.5 + random.uniform(0.5, 1.2))
        page_num = next_page

    return detail_urls, url_to_online_since, srp_by_car_id


def _parse_specs_from_text(text: str) -> dict:
    """Regex fallback for specs from raw text (mileage, power, fuel, etc.)."""
    out = {
        "mileage": "", "first_registration": "", "power_hp": "", "power_kw": "",
        "number_of_owners": "", "fuel_type": "", "transmission": "", "cubic_capacity": "",
    }
    if not text:
        return out
    m = re.search(r"([\d.,]+)\s*km", text, re.I)
    if m:
        out["mileage"] = m.group(1).replace(".", "").replace(",", ".").strip()
    m = re.search(r"(\d{1,2}/\d{4}|\d{4})", text)
    if m:
        out["first_registration"] = m.group(1)
    m = re.search(r"([\d.,]+)\s*PS", text, re.I)
    if m:
        out["power_hp"] = m.group(1).replace(",", ".").strip()
    m = re.search(r"([\d.,]+)\s*kW", text, re.I)
    if m:
        out["power_kw"] = m.group(1).replace(",", ".").strip()
    # Full fuel type including e.g. "Hybrid (Benzin/Elektro)"
    m = re.search(r"Kraftstoff\s*[:\s]*([A-Za-zäöüÄÖÜß\s/()]+?)\s*(?=\n|Getriebe|Hubraum|$)", text, re.I)
    if m:
        out["fuel_type"] = m.group(1).strip()
    m = re.search(r"Getriebe\s*[:\s]*([A-Za-zäöüÄÖÜß\s/]+?)\s*(?=\n|Hubraum|Kraftstoff|$)", text, re.I)
    if m:
        out["transmission"] = m.group(1).strip()
    m = re.search(r"Hubraum\s*[:\s]*([\d.,]+)\s*cm³", text, re.I)
    if m:
        out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".").strip()
    return out


def _extract_from_page_text(driver) -> dict:
    """
    Extract technical data by parsing the visible text on the page.
    This is a fallback method that works when structured elements (DL, data-testid) don't exist.
    
    Looks for German/English labels like:
    - Kilometerstand / Mileage
    - Leistung / Power  
    - Kraftstoffart / Fuel
    - Getriebe / Transmission
    - Erstzulassung / First Registration
    - Fahrzeughalter / Owners
    """
    out = {
        "mileage_km": "",
        "power_hp": "",
        "power_kw": "",
        "fuel_type": "",
        "transmission": "",
        "trim": "",
        "vehicle_id": "",
        "origin": "",
        "cubic_capacity": "",
        "hu": "",
        "climatisation": "",
        "color_manufacturer": "",
        "color": "",
        "interior_design": "",
        "first_registration": "",
        "number_of_owners": "",
    }
    
    try:
        # Get full page text
        body = driver.find_element(By.TAG_NAME, "body")
        page_text = body.text or ""
        
        # Pattern 1: "37.547 km" - mileage
        m = re.search(r"([\d.,]+)\s*km\s*(?:\n|$)", page_text)
        if m:
            out["mileage_km"] = m.group(1).replace(".", "").replace(",", ".")
        
        # Pattern 2: "169 kW (230 PS)" or "169 kW" - power
        m = re.search(r"([\d.,]+)\s*kW\s*\(?\s*([\d.,]+)?\s*(?:PS|hp)?\s*\)?", page_text, re.I)
        if m:
            out["power_kw"] = m.group(1).replace(",", ".")
            if m.group(2):
                out["power_hp"] = m.group(2).replace(",", ".")
        
        # Use line-by-line extraction for label:value pairs where label and value are on separate lines
        lines = page_text.split("\n")
        
        # Build a dict of label -> next line value
        label_to_next = {}
        for i, line in enumerate(lines):
            line_stripped = line.strip().lower()
            if i + 1 < len(lines):
                next_line = lines[i + 1].strip()
                if next_line and len(next_line) < 100:  # Value should be reasonably short
                    label_to_next[line_stripped] = next_line
        
        # Pattern 3: Fuel type - German: "Kraftstoffart", English: "Fuel"
        if "kraftstoffart" in label_to_next:
            out["fuel_type"] = label_to_next["kraftstoffart"]
        elif "fuel" in label_to_next:
            out["fuel_type"] = label_to_next["fuel"]
        elif "fuel type" in label_to_next:
            out["fuel_type"] = label_to_next["fuel type"]
        else:
            # Fallback: look for common fuel types (German and English)
            for fuel in ["Hybrid (petrol/electric)", "Hybrid (Benzin/Elektro)", "Hybrid", "Benzin", "Petrol", "Diesel", "Elektro", "Electric", "Gas"]:
                if fuel in page_text:
                    out["fuel_type"] = fuel
                    break
        
        # Pattern 4: Transmission - German: "Getriebe", English: "Transmission"
        if "getriebe" in label_to_next:
            out["transmission"] = label_to_next["getriebe"]
        elif "transmission" in label_to_next:
            out["transmission"] = label_to_next["transmission"]
        else:
            if "Automatik" in page_text or "Automatic" in page_text:
                out["transmission"] = "Automatic" if "Automatic" in page_text else "Automatik"
            elif "Schaltgetriebe" in page_text or "Manuell" in page_text or "Manual" in page_text:
                out["transmission"] = "Manual" if "Manual" in page_text else "Schaltgetriebe"
        
        # Pattern 5: First registration - German: "Erstzulassung", English: "First Registration"
        if "erstzulassung" in label_to_next:
            val = label_to_next["erstzulassung"]
            m = re.search(r"(\d{1,2}/\d{4})", val)
            if m:
                out["first_registration"] = m.group(1)
            else:
                out["first_registration"] = val
        elif "first registration" in label_to_next:
            val = label_to_next["first registration"]
            m = re.search(r"(\d{1,2}/\d{4})", val)
            if m:
                out["first_registration"] = m.group(1)
            else:
                out["first_registration"] = val
        
        # Pattern 6: Number of owners - German: "Anzahl der Fahrzeughalter" or "Fahrzeughalter"
        for label in ["anzahl der fahrzeughalter", "fahrzeughalter", "vehicle owners", "previous owners", "owners"]:
            if label in label_to_next:
                m = re.search(r"(\d+)", label_to_next[label])
                if m:
                    out["number_of_owners"] = m.group(1)
                break
        
        # Pattern 7: Vehicle ID - German: "Fahrzeugnummer", English: "Vehicle Number"
        m = re.search(r"(?:Fahrzeugnummer|Vehicle Number)[^\d]*(\d+)", page_text, re.I)
        if m:
            out["vehicle_id"] = m.group(1)
        
        # Pattern 8: Origin/Country version - German: "Herkunft", English: "Origin"
        if "herkunft" in label_to_next:
            out["origin"] = label_to_next["herkunft"]
        elif "origin" in label_to_next:
            out["origin"] = label_to_next["origin"]
        else:
            # Fallback: look for any "Ausführung" or "edition" pattern
            # This captures: Deutsche Ausführung, Österreichische Ausführung, EU-Ausführung, etc.
            m = re.search(r"(\w+)\s*(?:Ausführung|edition)", page_text, re.I)
            if m:
                out["origin"] = m.group(0)
        
        # Pattern 9: Cubic capacity - look for "Hubraum" label or pattern
        if "hubraum" in label_to_next:
            m = re.search(r"([\d.,]+)", label_to_next["hubraum"])
            if m:
                out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".")
        elif "cubic capacity" in label_to_next or "displacement" in label_to_next:
            val = label_to_next.get("cubic capacity") or label_to_next.get("displacement", "")
            m = re.search(r"([\d.,]+)", val)
            if m:
                out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".")
        else:
            m = re.search(r"([\d.,]+)\s*(?:ccm|cm³)", page_text, re.I)
            if m:
                out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".")
        
        # Pattern 10: HU (inspection) - "HU neu" or date
        if "hu" in label_to_next:
            out["hu"] = label_to_next["hu"]
        elif "inspection" in label_to_next:
            out["hu"] = label_to_next["inspection"]
        
        # Pattern 11: Climatisation - German: "Klimatisierung", English: "Climatisation"
        if "klimatisierung" in label_to_next:
            out["climatisation"] = label_to_next["klimatisierung"]
        elif "climatisation" in label_to_next:
            out["climatisation"] = label_to_next["climatisation"]
        elif "air conditioning" in label_to_next:
            out["climatisation"] = label_to_next["air conditioning"]
        else:
            # Fallback: look for common climatisation types
            for clim in ["2-Zonen-Klimaautomatik", "Klimaautomatik 2-Zonen", "Klimaautomatik", 
                         "Automatic climatisation", "Klimaanlage", "Air conditioning"]:
                if clim in page_text:
                    out["climatisation"] = clim
                    break
        
        # Pattern 12: Color (Manufacturer) - German: "Farbe (Hersteller)", English: "Colour (Manufacturer)"
        if "farbe (hersteller)" in label_to_next:
            out["color_manufacturer"] = label_to_next["farbe (hersteller)"]
        elif "colour (manufacturer)" in label_to_next:
            out["color_manufacturer"] = label_to_next["colour (manufacturer)"]
        elif "color (manufacturer)" in label_to_next:
            out["color_manufacturer"] = label_to_next["color (manufacturer)"]
        
        # Pattern 13: Color - German: "Farbe", English: "Colour"
        if "farbe" in label_to_next and not out.get("color"):
            out["color"] = label_to_next["farbe"]
        elif "colour" in label_to_next and not out.get("color"):
            out["color"] = label_to_next["colour"]
        elif "color" in label_to_next and not out.get("color"):
            out["color"] = label_to_next["color"]
        
        # Pattern 14: Interior - German: "Innenausstattung", English: "Interior Design"
        if "innenausstattung" in label_to_next:
            out["interior_design"] = label_to_next["innenausstattung"]
        elif "interior design" in label_to_next:
            out["interior_design"] = label_to_next["interior design"]
        elif "interior" in label_to_next:
            out["interior_design"] = label_to_next["interior"]
        
        # Pattern 15: Trim - German: "Ausstattungslinie", English: "Trim line"
        if "ausstattungslinie" in label_to_next:
            out["trim"] = label_to_next["ausstattungslinie"]
        elif "trim line" in label_to_next:
            out["trim"] = label_to_next["trim line"]
        elif "trim" in label_to_next:
            out["trim"] = label_to_next["trim"]
        
        if any(out.values()):
            return out
            
    except Exception:
        pass
    
    return out


def _extract_from_key_features_section(driver) -> dict:
    """
    Extract data from the key features icon-grid section at the top of the detail page.
    This section shows: Kilometerstand, Leistung, Kraftstoffart, Getriebe, Erstzulassung, Fahrzeughalter
    in a grid with icons.
    
    The HTML structure is typically:
    <div class="...">
        <span>Kilometerstand</span>
        <span>40.471 km</span>
    </div>
    """
    out = {
        "mileage_km": "",
        "power_hp": "",
        "power_kw": "",
        "fuel_type": "",
        "transmission": "",
        "first_registration": "",
        "number_of_owners": "",
    }
    
    try:
        # Get the page text from the body
        body = driver.find_element(By.TAG_NAME, "body")
        page_text = body.text or ""
        
        # The key features section typically renders as:
        # Kilometerstand
        # 40.471 km
        # Leistung
        # 169 kW (230 PS)
        # etc.
        
        lines = page_text.split("\n")
        
        # Field mappings: label -> (field_name, parser)
        label_map = {
            "kilometerstand": ("mileage_km", lambda x: re.search(r"([\d.,]+)", x).group(1).replace(".", "").replace(",", ".") if re.search(r"([\d.,]+)", x) else ""),
            "mileage": ("mileage_km", lambda x: re.search(r"([\d.,]+)", x).group(1).replace(".", "").replace(",", ".") if re.search(r"([\d.,]+)", x) else ""),
            "leistung": ("power_kw", lambda x: (re.search(r"([\d.,]+)\s*kW", x, re.I).group(1).replace(",", ".") if re.search(r"([\d.,]+)\s*kW", x, re.I) else "", re.search(r"([\d.,]+)\s*(?:PS|hp)", x, re.I).group(1).replace(",", ".") if re.search(r"([\d.,]+)\s*(?:PS|hp)", x, re.I) else "")),
            "power": ("power_kw", lambda x: (re.search(r"([\d.,]+)\s*kW", x, re.I).group(1).replace(",", ".") if re.search(r"([\d.,]+)\s*kW", x, re.I) else "", re.search(r"([\d.,]+)\s*(?:PS|hp)", x, re.I).group(1).replace(",", ".") if re.search(r"([\d.,]+)\s*(?:PS|hp)", x, re.I) else "")),
            "kraftstoffart": ("fuel_type", lambda x: x.strip()),
            "fuel": ("fuel_type", lambda x: x.strip()),
            "getriebe": ("transmission", lambda x: x.strip()),
            "transmission": ("transmission", lambda x: x.strip()),
            "erstzulassung": ("first_registration", lambda x: re.search(r"(\d{1,2}/\d{4})", x).group(1) if re.search(r"(\d{1,2}/\d{4})", x) else x.strip()),
            "first registration": ("first_registration", lambda x: re.search(r"(\d{1,2}/\d{4})", x).group(1) if re.search(r"(\d{1,2}/\d{4})", x) else x.strip()),
            "fahrzeughalter": ("number_of_owners", lambda x: re.search(r"(\d+)", x).group(1) if re.search(r"(\d+)", x) else ""),
            "previous owners": ("number_of_owners", lambda x: re.search(r"(\d+)", x).group(1) if re.search(r"(\d+)", x) else ""),
            "vehicle owners": ("number_of_owners", lambda x: re.search(r"(\d+)", x).group(1) if re.search(r"(\d+)", x) else ""),
        }
        
        for i, line in enumerate(lines):
            line_lower = line.strip().lower()
            
            for label, (field_name, parser) in label_map.items():
                if line_lower == label and i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and len(next_line) < 100:
                        try:
                            if field_name == "power_kw":
                                # Special handling for power - returns tuple (kw, hp)
                                kw, hp = parser(next_line)
                                if kw:
                                    out["power_kw"] = kw
                                if hp:
                                    out["power_hp"] = hp
                            else:
                                val = parser(next_line)
                                if val:
                                    out[field_name] = val
                        except Exception:
                            pass
                    break
        
        if any(out.values()):
            return out
            
    except Exception:
        pass
    
    return out


def _extract_from_icon_grid(driver) -> dict:
    """
    Extract technical data from icon-grid layout (alternative page layout).
    Some mobile.de pages display specs as icon + value pairs in a grid.
    
    Looks for divs with data-testid attributes like:
    - mileage-item, power-item, fuel-item, transmission-item, etc.
    """
    out = {
        "mileage_km": "",
        "power_hp": "",
        "power_kw": "",
        "fuel_type": "",
        "transmission": "",
        "trim": "",
        "vehicle_id": "",
        "origin": "",
        "cubic_capacity": "",
        "hu": "",
        "climatisation": "",
        "color_manufacturer": "",
        "color": "",
        "interior_design": "",
        "first_registration": "",
        "number_of_owners": "",
    }
    
    try:
        # Find all elements with data-testid attributes (icon grid layout)
        testid_map = {
            "mileage-item": ("mileage_km", lambda x: re.search(r"([\d.,]+)", x).group(1).replace(".", "").replace(",", ".") if re.search(r"([\d.,]+)", x) else ""),
            "power-item": ("power_kw", lambda x: re.search(r"([\d.,]+)\s*kW", x, re.I).group(1).replace(",", ".") if re.search(r"([\d.,]+)\s*kW", x, re.I) else ""),
            "fuel-item": ("fuel_type", lambda x: x),
            "transmission-item": ("transmission", lambda x: x),
            "trimLine-item": ("trim", lambda x: x),
            "sku-item": ("vehicle_id", lambda x: x),
            "countryVersion-item": ("origin", lambda x: x),
            "cubicCapacity-item": ("cubic_capacity", lambda x: re.search(r"([\d.,]+)", x).group(1).replace(".", "").replace(",", ".") if re.search(r"([\d.,]+)", x) else ""),
            "hu-item": ("hu", lambda x: x),
            "climatisation-item": ("climatisation", lambda x: x),
            "manufacturerColorName-item": ("color_manufacturer", lambda x: x),
            "color-item": ("color", lambda x: x),
            "interior-item": ("interior_design", lambda x: x),
            "firstRegistration-item": ("first_registration", lambda x: x),
            "numberOfPreviousOwners-item": ("number_of_owners", lambda x: re.search(r"(\d+)", x).group(1) if re.search(r"(\d+)", x) else ""),
        }
        
        for testid, (field_name, parser) in testid_map.items():
            try:
                # Try to find element by data-testid
                elements = driver.find_elements(By.XPATH, f"//*[@data-testid='{testid}']")
                if not elements:
                    elements = driver.find_elements(By.XPATH, f"//*[@data-testid='{testid}']/following-sibling::*[1]")
                
                for el in elements:
                    text = (el.text or "").strip()
                    if text:
                        parsed = parser(text)
                        if parsed:
                            out[field_name] = parsed
                            break
            except Exception:
                continue
        
        # Special handling for power: extract both kW and PS from single field
        try:
            power_els = driver.find_elements(By.XPATH, "//*[@data-testid='power-item']")
            if power_els:
                power_text = (power_els[0].text or "").strip()
                m_kw = re.search(r"([\d.,]+)\s*kW", power_text, re.I)
                if m_kw:
                    out["power_kw"] = m_kw.group(1).replace(",", ".").strip()
                m_hp = re.search(r"([\d.,]+)\s*(?:PS|hp)", power_text, re.I)
                if m_hp:
                    out["power_hp"] = m_hp.group(1).replace(",", ".").strip()
        except Exception:
            pass
        
        if any(out.values()):
            return out
    
    except Exception:
        pass
    
    return out


def _extract_from_dl(driver) -> dict:
    """
    Extract technical data from the structured <dl> list on the detail page.
    The DL has <dt> labels with data-testid attributes and <dd> values.
    This is the most reliable source for technical data (DL-based layout).
    
    HYBRID APPROACH: This function tries DL first, then falls back to icon grid.
    """
    out = {
        "mileage_km": "",
        "power_hp": "",
        "power_kw": "",
        "fuel_type": "",
        "transmission": "",
        "trim": "",
        "vehicle_id": "",
        "origin": "",
        "cubic_capacity": "",
        "hu": "",
        "climatisation": "",
        "color_manufacturer": "",
        "color": "",
        "interior_design": "",
        "first_registration": "",
        "number_of_owners": "",
    }
    
    try:
        # ATTEMPT 1: Try DL-based layout (structured definition list format)
        # Try multiple class patterns as mobile.de uses minified class names that may change
        dl_elements = []
        for dl_xpath in [
            "//dl[contains(@class, 'm4qzs')]",
            "//dl[contains(@class, 'technical')]",
            "//section[contains(@data-testid, 'technical')]//dl",
            "//h3[contains(text(), 'Technische Daten') or contains(text(), 'Technical data')]/following::dl[1]",
            "//dl[.//dt[@data-testid]]",  # Any DL with data-testid on dt elements
        ]:
            dl_elements = driver.find_elements(By.XPATH, dl_xpath)
            if dl_elements:
                break
        
        if dl_elements:  # Only try DL if we found it
            for dl in dl_elements:
                try:
                    dts = dl.find_elements(By.TAG_NAME, "dt")
                    dds = dl.find_elements(By.TAG_NAME, "dd")
                    
                    for i, dt in enumerate(dts):
                        if i >= len(dds):
                            break
                        
                        try:
                            testid = dt.get_attribute("data-testid") or ""
                            value_text = (dds[i].text or "").strip()
                            
                            if not value_text:
                                continue
                            
                            if testid == "mileage-item":
                                m = re.search(r"([\d.,]+)", value_text)
                                if m:
                                    out["mileage_km"] = m.group(1).replace(".", "").replace(",", ".").strip()
                            elif testid == "power-item":
                                m_kw = re.search(r"([\d.,]+)\s*kW", value_text, re.I)
                                if m_kw:
                                    out["power_kw"] = m_kw.group(1).replace(",", ".").strip()
                                m_hp = re.search(r"([\d.,]+)\s*(?:hp|PS)", value_text, re.I)
                                if m_hp:
                                    out["power_hp"] = m_hp.group(1).replace(",", ".").strip()
                            elif testid == "fuel-item":
                                out["fuel_type"] = value_text
                            elif testid == "transmission-item":
                                out["transmission"] = value_text
                            elif testid == "trimLine-item":
                                out["trim"] = value_text
                            elif testid == "sku-item":
                                out["vehicle_id"] = value_text
                            elif testid == "countryVersion-item":
                                out["origin"] = value_text
                            elif testid == "cubicCapacity-item":
                                m = re.search(r"([\d.,]+)", value_text)
                                if m:
                                    out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".").strip()
                            elif testid == "hu-item":
                                out["hu"] = value_text
                            elif testid == "climatisation-item":
                                out["climatisation"] = value_text
                            elif testid == "manufacturerColorName-item":
                                out["color_manufacturer"] = value_text
                            elif testid == "color-item":
                                out["color"] = value_text
                            elif testid == "interior-item":
                                out["interior_design"] = value_text
                            elif testid == "firstRegistration-item":
                                out["first_registration"] = value_text
                            elif testid == "numberOfPreviousOwners-item":
                                m = re.search(r"(\d+)", value_text)
                                if m:
                                    out["number_of_owners"] = m.group(1)
                        except Exception:
                            continue
                    
                    if any(out.values()):
                        return out  # Success with DL layout
                
                except Exception:
                    continue
    
    except Exception:
        pass
    
    # ATTEMPT 2: Extract from key features section (icon grid at top of page)
    key_features_data = _extract_from_key_features_section(driver)
    if any(key_features_data.values()):
        # Merge with out (key features fills in gaps)
        for key, value in key_features_data.items():
            if value and not out.get(key):
                out[key] = value

    # ATTEMPT 3: Fall back to icon grid layout (data-testid based)
    icon_grid_data = _extract_from_icon_grid(driver)
    if any(icon_grid_data.values()):
        # Merge with out (icon_grid fills in gaps)
        for key, value in icon_grid_data.items():
            if value and not out.get(key):
                out[key] = value

    # ATTEMPT 4: Fall back to text-based extraction (most flexible)
    text_data = _extract_from_page_text(driver)
    if any(text_data.values()):
        # Merge with out (text_data fills in gaps)
        for key, value in text_data.items():
            if value and not out.get(key):
                out[key] = value
    
    return out  # Return whatever we collected


def _parse_specs(driver) -> tuple[dict, bool, dict]:
    """
    Extract technical data from the detail page. Uses two strategies:
    1. Extract from structured DL list (most reliable)
    2. Extract from detailed Technical Data table for additional fields
    Returns (specs_dict, is_accident_free, technical_data_dict).
    """
    # Strategy 1: Get basics from structured DL list
    out = _extract_from_dl(driver)
    out.setdefault("cubic_capacity", "")
    
    is_accident_free = False
    technical_data: dict[str, str] = {}

    # Strategy 2: Extract detailed technical data table
    try:
        for xp in [
            "//h3[contains(text(), 'Technische Daten') or contains(text(), 'Technical data')]/following::div[contains(@class, 'DA8Gd') or contains(@class, 'cANbJ')]",
            "//h3[contains(text(), 'Technische Daten') or contains(text(), 'Technical data')]/ancestor::section//div[contains(@class, 'DA8Gd')]",
        ]:
            try:
                elements = driver.find_elements(By.XPATH, xp)
                for el in elements[:100]:
                    try:
                        text = (el.text or "").strip()
                        if not text or len(text) > 500:
                            continue
                        
                        label = ""
                        value = ""
                        if "\n" in text:
                            parts = text.split("\n", 1)
                            label = parts[0].strip().strip(":")
                            value = parts[1].strip()
                        elif ":" in text:
                            parts = text.split(":", 1)
                            label = parts[0].strip()
                            value = parts[1].strip()
                        
                        if label and value:
                            technical_data[label] = value
                    except Exception:
                        continue
                
                if technical_data:
                    break
            except Exception:
                continue
    except Exception:
        pass

    # Extract specific fields from technical_data that weren't already in top section
    if technical_data:
        for lbl, val in technical_data.items():
            if not val:
                continue
            lbl_lower = lbl.lower()

            # Fill in missing fields from top section if not already set
            # Fuel type (if not already from top)
            if not out.get("fuel_type") and ("kraftstoff" in lbl_lower or "fuel" in lbl_lower):
                out["fuel_type"] = val

            # Number of owners (if not already from top)
            if not out.get("number_of_owners") and ("fahrzeughalter" in lbl_lower or "owners" in lbl_lower or "previous" in lbl_lower):
                m = re.search(r"(\d+)", val)
                if m:
                    out["number_of_owners"] = m.group(1)

            # Transmission (if not already from top)
            if not out.get("transmission") and ("getriebe" in lbl_lower or "transmission" in lbl_lower or "gearbox" in lbl_lower):
                out["transmission"] = val

            # Cubic capacity / Hubraum
            if "hubraum" in lbl_lower or "cubic" in lbl_lower or "displacement" in lbl_lower:
                m = re.search(r"([\d.,]+)", val)
                if m:
                    out["cubic_capacity"] = m.group(1).replace(".", "").replace(",", ".").strip()

            # Mileage (if not already from top)
            if not out.get("mileage") and ("km" in lbl_lower or "mileage" in lbl_lower or "kilometerstand" in lbl_lower):
                m = re.search(r"([\d.,]+)", val)
                if m:
                    out["mileage"] = m.group(1).replace(".", "").replace(",", ".").strip()

            # First registration (if not already from top)
            if not out.get("first_registration") and ("registration" in lbl_lower or "erstzulassung" in lbl_lower or "zulassung" in lbl_lower):
                out["first_registration"] = val

            # Power (if not already from top)
            if not out.get("power_kw") and ("leistung" in lbl_lower or "power" in lbl_lower):
                m_kw = re.search(r"([\d.,]+)\s*kW", val, re.I)
                if m_kw:
                    out["power_kw"] = m_kw.group(1).replace(",", ".").strip()
            if not out.get("power_hp") and ("leistung" in lbl_lower or "power" in lbl_lower):
                m_hp = re.search(r"([\d.,]+)\s*PS", val, re.I)
                if m_hp:
                    out["power_hp"] = m_hp.group(1).replace(",", ".").strip()

            # Accident-free flag
            if "unfallfrei" in (lbl_lower + val.lower()) and "unfallfahrzeug" not in (lbl_lower + val.lower()):
                is_accident_free = True
            if "unfallfahrzeug" in (lbl_lower + val.lower()):
                is_accident_free = False

    return out, is_accident_free, technical_data


def phase2_extract_car(driver, url: str, index: int, total: int) -> dict | None:
    """Visit one detail page and extract car data."""
    print(f"Scraping car {index} of {total}...")
    try:
        driver.get(url)
        time.sleep(3)  # Increased initial wait for page load
    except Exception as e:
        print(f"  Error loading {url}: {e}")
        return None

    # Wait for page to be ready - look for key elements
    try:
        # Wait up to 10 seconds for the price element to appear
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, "//*[contains(text(), '€')]"))
        )
    except Exception:
        time.sleep(2)  # Fallback wait if element not found
    
    # Use final URL (in case of redirects)
    try:
        url = driver.current_url
    except Exception:
        pass

    # Expand Technical Data, Features (Ausstattung), and Vehicle description before reading
    # Try up to 3 times to ensure buttons are clicked
    for expand_attempt in range(3):
        _click_show_more_sections(driver)
        
        # Verify at least one button was clicked by checking for "Weniger anzeigen" (Show less)
        try:
            less_buttons = driver.find_elements(By.XPATH, 
                "//button[contains(normalize-space(.), 'Weniger anzeigen') or contains(normalize-space(.), 'Show less')]"
            )
            if less_buttons:
                break  # Success - at least one section was expanded
        except Exception:
            pass
        
        # If no "Weniger anzeigen" found, try scrolling and waiting more
        if expand_attempt < 2:
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(1)
                driver.execute_script("window.scrollTo(0, 0);")
                time.sleep(1)
            except Exception:
                pass

    # Extract specs from DL (primary source) and technical data (secondary)
    specs, is_accident_free, technical_data = _parse_specs(driver)
    
    # Extract HU and Climatisation from specs (now populated from DL)
    hu = specs.get("hu", "")
    climatisation = specs.get("climatisation", "")
    
    # Accident-free is no longer derived from detail page text/heuristics.
    # Canonical rule is applied at save time using SRP `vehicle_condition` OR mileage<100km.
    is_accident_free = False

    # Detail-page title and price are no longer canonical (SRP is source of truth).
    title = _find_first_by_css(driver, SELECTORS["title"])
    if not title:
        try:
            title = (driver.title or "").strip()
            if title and "|" in title:
                title = title.split("|")[0].strip()
        except Exception:
            pass

    price = ""  # kept only as debug `detail_price_raw`
    try:
        # Strategy 1: Look for data-testid="prime-price" or similar main price
        for sel in [
            "[data-testid='prime-price']",
            "[data-testid='price-block']",
            ".price-block",
        ]:
            try:
                el = driver.find_element(By.CSS_SELECTOR, sel)
                price_text = (el.text or "").strip()
                if price_text:
                    # Extract price value - handles both "€ 26,390" and "28.990 €" formats
                    m = re.search(r"([\d.,]+)\s*€", price_text)  # "28.990 €" format
                    if m:
                        price = m.group(1) + " €"
                        break
                    m = re.search(r"€\s*([\d.,]+)", price_text)  # "€ 26,390" format
                    if m:
                        price = "€ " + m.group(1)
                        break
            except Exception:
                continue
        
        # Strategy 2: Parse from page text - look for price pattern near car title
        if not price:
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text or ""
                # Look for "28.990 €" pattern (number followed by €)
                m = re.search(r"([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)\s*€", body_text)
                if m:
                    value_str = m.group(1).replace(".", "").replace(",", "")
                    try:
                        if 1000 < int(value_str) < 500000:
                            price = m.group(1) + " €"
                    except ValueError:
                        pass
            except Exception:
                pass
        
        # Strategy 3: Search in visible elements for € sign
        if not price:
            for el in driver.find_elements(By.XPATH, "//*[contains(text(), '€')]"):
                text = (el.text or "").strip()
                if text and len(text) < 50:  # Avoid very long text blocks
                    # Try "28.990 €" format first
                    m = re.search(r"([\d]{1,3}(?:\.[\d]{3})*(?:,\d{2})?)\s*€", text)
                    if m:
                        value_str = m.group(1).replace(".", "").replace(",", "")
                        try:
                            if 1000 < int(value_str) < 500000:
                                price = m.group(1) + " €"
                                break
                        except ValueError:
                            continue
                    # Try "€ 26,390" format
                    m = re.search(r"€\s*([\d.,]+)", text)
                    if m:
                        value_str = m.group(1).replace(".", "").replace(",", "")
                        try:
                            if 1000 < int(value_str) < 500000:
                                price = "€ " + m.group(1)
                                break
                        except ValueError:
                            continue
    except Exception:
        pass

    # Features (Ausstattung) – after "show more", collect from ALL columns (both columns of the features grid)
    equipment: list[str] = []
    try:
        # Strategy 1: Look for data-testid="vip-features-list" or similar
        for xp in [
            "[data-testid='vip-features-list']",
            "//section[contains(@data-testid, 'vip-features') or contains(., 'Ausstattung')]",
        ]:
            try:
                if xp.startswith("["):
                    elements = driver.find_elements(By.CSS_SELECTOR, xp)
                else:
                    elements = driver.find_elements(By.XPATH, xp)
                
                for container in elements:
                    # Get all LI items from this container (both columns)
                    items = container.find_elements(By.TAG_NAME, "li")
                    for el in items:
                        t = (el.text or "").strip()
                        if t and len(t) < 200 and t not in equipment:
                            equipment.append(t)
            except Exception:
                continue
        
        # Strategy 2: If still empty, look for equipment section by heading
        if not equipment:
            for el in driver.find_elements(By.XPATH, "//*[contains(normalize-space(.), 'Ausstattung') or contains(normalize-space(.), 'Features')]/ancestor::*[contains(@class, 'vip-features') or contains(@class, 'equipment') or contains(@class, 'features')]//li"):
                t = (el.text or "").strip()
                if t and len(t) < 200 and t not in equipment:
                    equipment.append(t)
        
        # Strategy 3: Last resort - all LI elements on page (after show more clicked)
        if not equipment:
            for el in driver.find_elements(By.TAG_NAME, "li"):
                t = (el.text or "").strip()
                # Only include if looks like a feature (not too long, no links or structure)
                if t and len(t) < 200 and len(t) > 2 and t not in equipment:
                    # Skip if looks like a navigation or list item
                    if not any(skip in t.lower() for skip in ["http", "email", "phone", ">>", "<<", "->", "<-"]):
                        equipment.append(t)
    except Exception:
        pass
    
    # Remove duplicates while preserving order
    equipment = list(dict.fromkeys([e for e in equipment if e]))

    # Vehicle description by seller (Fahrzeugbeschreibung laut Anbieter)
    description = ""
    try:
        # Strategy 1: Find the description section by heading and get all text within it
        for xp in [
            "//h3[contains(text(), 'Fahrzeugbeschreibung') or contains(text(), 'Vehicle description')]",
            "//article[contains(., 'Fahrzeugbeschreibung') or contains(., 'Vehicle description')]",
            "//*[contains(., 'Fahrzeugbeschreibung laut Anbieter') or contains(., 'Vehicle description according')]",
        ]:
            try:
                heading = driver.find_element(By.XPATH, xp)
                # Get the parent container
                container = heading.find_element(By.XPATH, "ancestor::article[1] | ancestor::section[1] | ancestor::div[contains(@class, 'vTKPY') or contains(@class, 'A3G6X')][1]")
                desc_text = container.text or ""
                # Remove the heading itself from the text
                desc_text = desc_text.replace(heading.text, "").strip()
                # Remove "Show more" / "Mehr anzeigen" and similar buttons
                desc_text = re.sub(r"(Show more|Mehr anzeigen|Weniger anzeigen|Show less)\s*", "", desc_text, flags=re.IGNORECASE)
                if len(desc_text) > 100:
                    description = desc_text
                    break
            except Exception:
                continue
        
        # Strategy 2: If not found, look for common description selectors
        if not description:
            for sel in [
                "[data-testid='seller-comment']",
                "[data-testid='vehicle-description']",
                ".seller-comment",
                ".vehicle-description",
                "[class*='Fahrzeugbeschreibung']",
            ]:
                try:
                    el = driver.find_element(By.CSS_SELECTOR, sel)
                    desc_text = (el.text or "").strip()
                    if len(desc_text) > 100:
                        description = desc_text
                        break
                except Exception:
                    continue
        
        # Strategy 3: Look in body text for description-like content
        if not description:
            try:
                body_text = driver.find_element(By.TAG_NAME, "body").text
                # Find text block after "Fahrzeugbeschreibung laut Anbieter"
                idx = body_text.lower().find("fahrzeugbeschreibung")
                if idx > 0:
                    snippet = body_text[idx+len("Fahrzeugbeschreibung"):idx+3000]
                    # Extract meaningful text (skip section headings)
                    lines = [l.strip() for l in snippet.split("\n") if l.strip() and len(l.strip()) > 20]
                    if lines:
                        description = "\n".join(lines[:30])  # First 30 lines
            except Exception:
                pass
    except Exception:
        pass

    # Price rating: mobile.de uses data-testid="price-evaluation-click" with aria-label (e.g. "Good price")
    price_rating = ""
    try:
        el = driver.find_element(By.CSS_SELECTOR, "[data-testid='price-evaluation-click']")
        price_rating = (el.get_attribute("aria-label") or "").strip()
    except Exception:
        pass
    if not price_rating:
        price_rating = _find_first_by_css(driver, SELECTORS["price_rating"])
    if not price_rating:
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            for phrase in [
                "Very good price", "Good price", "Fair price", "Increased price", "High price",
                "Sehr guter Preis", "Guter Preis", "Fairer Preis", "Erhöhter Preis", "Hoher Preis",
            ]:
                if phrase in body_text:
                    price_rating = phrase
                    break
        except Exception:
            pass

    # (removed) detail-page accident heuristics

    seller_type = "Unbekannt"
    seller_rating = ""
    try:
        if driver.find_elements(By.CSS_SELECTOR, SELECTORS["seller_private"]):
            seller_type = "Privatanbieter"
        elif driver.find_elements(By.CSS_SELECTOR, SELECTORS["seller_dealer"]):
            seller_type = "Händler"
    except Exception:
        pass
    if seller_type == "Unbekannt":
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text
            if "Privatanbieter" in body_text:
                seller_type = "Privatanbieter"
            elif "Händler" in body_text:
                seller_type = "Händler"
        except Exception:
            pass
    
    # Extract seller rating/stars
    # Look for patterns like "4.6 Sterne" (German) or "4.6 stars" (English)
    if seller_type == "Händler":
        try:
            body_text = driver.find_element(By.TAG_NAME, "body").text or ""
            
            # Pattern 1: "4.6 Sterne" or "4,6 Sterne" - German format
            rating_match = re.search(r"(\d+(?:[.,]\d+)?)\s*Sterne", body_text, re.IGNORECASE)
            if rating_match:
                seller_rating = rating_match.group(1).replace(",", ".")
            else:
                # Pattern 2: "4.6 stars" - English format
                rating_match = re.search(r"(\d+(?:[.,]\d+)?)\s*stars", body_text, re.IGNORECASE)
                if rating_match:
                    seller_rating = rating_match.group(1).replace(",", ".")
                else:
                    # Pattern 3: "4.5 out of 5" or "4.5/5" or "4,5 von 5"
                    rating_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:out\s+of|\/|\bvon)\s*5", body_text, re.IGNORECASE)
                    if rating_match:
                        seller_rating = rating_match.group(1).replace(",", ".")
        except Exception:
            pass

    # Get color, interior design, vehicle_id, trim, origin from specs (DL extraction first, then fallback to technical_data)
    color_manufacturer = specs.get("color_manufacturer", "")
    color = specs.get("color", "")
    interior_design = specs.get("interior_design", "")
    vehicle_id = specs.get("vehicle_id", "")
    trim = specs.get("trim", "")
    origin = specs.get("origin", "")
    
    # Fallback to technical_data if specs didn't get them
    for lbl, val in (technical_data or {}).items():
        if not val:
            continue
        lbl_lower = lbl.lower()
        if not color_manufacturer and ("color" in lbl_lower and "manufacturer" in lbl_lower or "farbe" in lbl_lower and "hersteller" in lbl_lower):
            color_manufacturer = val
        elif not color and (lbl_lower in ("color", "farbe")):
            color = val
        elif not interior_design and ("interior" in lbl_lower or "innenausstattung" in lbl_lower or "innenausstat" in lbl_lower):
            interior_design = val
        # Fahrzeugnummer / stock / vehicle id
        if not vehicle_id and ("fahrzeugnummer" in lbl_lower or "vehicle id" in lbl_lower or "stock" in lbl_lower):
            vehicle_id = val
        # Baureihe / trim / model line
        if not trim and ("baureihe" in lbl_lower or "trim" in lbl_lower or "model line" in lbl_lower):
            trim = val
        # Herkunft / origin
        if not origin and ("herkunft" in lbl_lower or "origin" in lbl_lower):
            origin = val

    car = {
        "url": url,
        "title": "",  # dropped from DB v2 (kept empty for compatibility)
        "price": price,  # detail price raw (debug only)
        "mileage_km": specs.get("mileage_km") or specs.get("mileage"),
        "first_registration": specs["first_registration"],
        "power_hp": specs["power_hp"],
        "power_kw": specs["power_kw"],
        "number_of_owners": specs["number_of_owners"],
        "fuel_type": specs["fuel_type"],
        "transmission": specs["transmission"],
        "cubic_capacity": specs["cubic_capacity"],
        "is_accident_free": is_accident_free,
        "price_rating": price_rating or "",
        "color_manufacturer": color_manufacturer,
        "color": color,
        "interior_design": interior_design,
        "trim": trim,
        "origin": origin,
        "hu": hu,
        "climatisation": climatisation,
        "equipment": equipment,
        "description": description,
        "seller_type": seller_type,
        "seller_rating": seller_rating,
    }
    # Debug summary so you can see what was collected for each car while scraping
    print(f"Saving Car: {title or url[:60]}...")
    try:
        print(
            f"  URL: {url}\n"
            f"  Detail price (debug): {price} | Mileage: {specs.get('mileage_km') or specs.get('mileage')} | First reg: {specs['first_registration']}\n"
            f"  Power: {specs['power_hp']} PS / {specs['power_kw']} kW | Fuel: {specs['fuel_type']} | Transmission: {specs['transmission']}\n"
            f"  Trim: {trim} | Origin: {origin}\n"
            f"  Owners: {specs['number_of_owners']} | Cubic Capacity: {specs['cubic_capacity']} | Color: {color} / {color_manufacturer}\n"
            f"  Interior: {interior_design} | HU: {hu} | Climatisation: {climatisation}\n"
            f"  Equipment: {len(equipment)} items | Description: {len(description)} chars\n"
            f"  Accident-free: {is_accident_free} | Seller: {seller_type} (rating: {seller_rating})\n"
            f"  Price rating: {price_rating}\n"
        )
    except Exception:
        # Never let debug printing break the scraper
        pass
    return car


def _export_db_to_json() -> None:
    """Write all cars from DB to cars_market_data.json for pandas/notebook."""
    if not DB_PATH.exists():
        return
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT car_id, url,
                   brand, model, srp_title, srp_price_raw, price_first_eur, price_current_eur, price_checked_at,
                   detail_price_raw,
                   mileage_km, first_registration, power_hp, power_kw,
                   number_of_owners, fuel_type, transmission, cubic_capacity, is_accident_free,
                   vehicle_condition, price_rating, color_manufacturer, color, interior_design,
                   trim, origin, hu, climatisation,
                   equipment, description, seller_type, seller_rating, ad_online_since, source_search, last_seen_at
            FROM cars ORDER BY created_at
        """).fetchall()
        conn.close()
        cols = [
            "car_id", "url",
            "brand", "model", "srp_title", "srp_price_raw", "price_first_eur", "price_current_eur", "price_checked_at",
            "detail_price_raw",
            "mileage_km", "first_registration", "power_hp", "power_kw",
                "number_of_owners", "fuel_type", "transmission", "cubic_capacity", "is_accident_free",
                "vehicle_condition", "price_rating", "color_manufacturer", "color", "interior_design",
                "trim", "origin", "hu", "climatisation",
                "equipment", "description", "seller_type", "seller_rating", "ad_online_since", "source_search", "last_seen_at",
        ]
        cars = []
        for r in rows:
            d = dict(zip(cols, r))
            d["is_accident_free"] = bool(d["is_accident_free"])
            try:
                d["equipment"] = json.loads(d["equipment"] or "[]")
            except Exception:
                d["equipment"] = []
            cars.append(d)
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(cars, f, ensure_ascii=False, indent=2)
        print(f"Exported {len(cars)} cars to {OUTPUT_FILE}")
    except Exception as e:
        print(f"Export to JSON failed: {e}")


def run_scraper(search_url: str) -> None:
    if not search_url.strip():
        print("Usage: provide a mobile.de search URL as first argument.")
        sys.exit(1)

    parsed = urlparse(search_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"
    search_fp = _search_fingerprint(search_url)

    if CLEAR_BEFORE_RUN:
        _clear_db_and_json()

    DATA_RAW.mkdir(parents=True, exist_ok=True)
    _init_db()
    before_db_count = len(_load_existing_car_ids_from_db()) if not CLEAR_BEFORE_RUN else 0
    if PRUNE_NOT_IN_LATEST_SEARCH:
        try:
            updated = _backfill_source_search_for_legacy_rows()
            if updated:
                print(f"[Backfill] Filled source_search for {updated} legacy row(s).")
        except Exception as e:
            print(f"[Backfill] source_search backfill failed (continuing anyway): {e}")

    driver = None
    scraped_new = 0
    pruned_count = 0
    run_seen_at = datetime.datetime.utcnow().isoformat()
    try:
        options = uc.ChromeOptions()
        # Force English UI so mobile.de shows "Good price", "Vehicle description", etc.
        options.add_experimental_option("prefs", {"intl.accept_languages": "en-US,en"})
        driver = uc.Chrome(version_main=145, options=options, headless=False)
        driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)

        detail_urls, url_to_online_since, srp_by_car_id = phase1_collect_urls(driver, search_url, base_domain)
        total = len(detail_urls)
        print(f"Found {total} total cars in search results.")

        if total == 0:
            print("No listing URLs found. Browser will stay open 30s.")
            time.sleep(30)
            return

        # Remove listings that disappeared from this search (same fingerprint only).
        keep_ids = {_extract_car_id_from_url(u) for u in detail_urls}
        marked = _mark_seen_car_ids(keep_ids, run_seen_at)
        if marked:
            print(f"Marked {marked} existing listing(s) as seen at {run_seen_at}.")
        if PRUNE_NOT_IN_LATEST_SEARCH:
            pruned_count = _prune_stale_listings_for_search(search_fp, keep_ids)
            if pruned_count:
                print(
                    f"Marked {pruned_count} listing(s) as sold (no longer in this search). "
                    f"Search scope: fingerprint {search_fp}."
                )

        # Reload after prune so we don't skip cars that were just removed from DB.
        existing_car_ids = _load_existing_car_ids_from_db() if not CLEAR_BEFORE_RUN else set()
        to_visit = [u for u in detail_urls if _extract_car_id_from_url(u) not in existing_car_ids]
        skipped = total - len(to_visit)
        if skipped > 0:
            print(f"Skipping {skipped} cars already in database. {len(to_visit)} new cars to scrape.")

        # Always upsert SRP snapshot fields (brand/model/current price) for all seen cars.
        try:
            conn = sqlite3.connect(DB_PATH)
            for cid, snap in (srp_by_car_id or {}).items():
                if not cid:
                    continue
                price_now = snap.get("price_current_eur")
                raw_now = snap.get("srp_price_raw") or ""
                brand = snap.get("brand") or ""
                model = snap.get("model") or ""
                srp_title = snap.get("srp_title") or ""
                price_rating = snap.get("price_rating") or ""
                vehicle_condition = snap.get("vehicle_condition") or ""
                # Read current mileage so we can apply the canonical accident-free rule consistently,
                # even for cars that are skipped in Phase 2.
                row2 = conn.execute(
                    "SELECT mileage_km, is_accident_free FROM cars WHERE car_id = ?",
                    (cid,),
                ).fetchone()
                existing_mileage = row2[0] if row2 else ""
                existing_is_af = row2[1] if row2 else None
                new_is_af = _accident_free_from_rule(vehicle_condition, existing_mileage)
                if new_is_af is None:
                    new_is_af = existing_is_af
                # Get existing first price
                row = conn.execute(
                    "SELECT price_first_eur FROM cars WHERE car_id = ?",
                    (cid,),
                ).fetchone()
                first_price = row[0] if row else None
                conn.execute(
                    """
                    UPDATE cars
                    SET brand = COALESCE(NULLIF(?, ''), brand),
                        model = COALESCE(NULLIF(?, ''), model),
                        srp_title = COALESCE(NULLIF(?, ''), srp_title),
                        srp_price_raw = COALESCE(NULLIF(?, ''), srp_price_raw),
                        price_first_eur = CASE
                            WHEN price_first_eur IS NULL AND ? IS NOT NULL THEN ?
                            ELSE price_first_eur
                        END,
                        price_current_eur = COALESCE(?, price_current_eur),
                        price_checked_at = ?,
                        vehicle_condition = COALESCE(NULLIF(?, ''), vehicle_condition),
                        is_accident_free = COALESCE(?, is_accident_free),
                        price_rating = COALESCE(NULLIF(?, ''), price_rating)
                    WHERE car_id = ?
                    """,
                    (brand, model, srp_title, raw_now, price_now, price_now, price_now, run_seen_at, vehicle_condition, new_is_af, price_rating, cid),
                )
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[SRP] Warning: could not persist SRP snapshot fields: {e}")

        # Optional test limit for safe verification on a few cars
        test_limit = int(os.environ.get("TEST_MAX_CARS", "0") or "0")
        if test_limit and len(to_visit) > test_limit:
            print(f"[TEST] Limiting Phase 2 to first {test_limit} cars (set TEST_MAX_CARS=0 to disable).")
            to_visit = to_visit[:test_limit]

        for i, url in enumerate(to_visit, start=1):
            delay = random.uniform(MIN_DELAY_SEC, MAX_DELAY_SEC)
            if i > 1:
                print(f"Waiting {delay:.1f}s before next request...")
                time.sleep(delay)
            try:
                car = phase2_extract_car(driver, url, i, len(to_visit))
                if car:
                    car["ad_online_since"] = url_to_online_since.get(car.get("url") or url, "")
                    car["last_seen_at"] = run_seen_at
                    # carry SRP canonical fields into the row being saved (brand/model/prices)
                    cid = _extract_car_id_from_url(car.get("url") or url)
                    snap = (srp_by_car_id or {}).get(cid) or {}
                    car["brand"] = snap.get("brand", car.get("brand", ""))
                    car["model"] = snap.get("model", car.get("model", ""))
                    car["srp_title"] = snap.get("srp_title", car.get("srp_title", ""))
                    car["srp_price_raw"] = snap.get("srp_price_raw", car.get("srp_price_raw", ""))
                    car["price_current_eur"] = snap.get("price_current_eur", car.get("price_current_eur", None))
                    car["vehicle_condition"] = snap.get("vehicle_condition", car.get("vehicle_condition", ""))
                    # Apply canonical rule (only source of truth)
                    car["is_accident_free"] = bool(
                        _accident_free_from_rule(car.get("vehicle_condition", ""), car.get("mileage_km", "")) == 1
                    )
                    # price_first_eur is set via the SRP upsert update; keep None here so DB keeps existing
                    car["price_first_eur"] = None
                    car["price_checked_at"] = run_seen_at
                    # keep detail price as debug only
                    car["detail_price_raw"] = car.get("price", "")
                    _save_car_to_db(car)
                    _export_db_to_json()
            except Exception as e:
                print(f"  Skipping car (error): {e}")
                continue

    except KeyboardInterrupt:
        print("\nStopped by user. Saving collected data...")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if driver:
            driver.quit()

    _export_db_to_json()
    n = len(_load_existing_car_ids_from_db())
    if pruned_count:
        print(f"Pruned (this run): {pruned_count}")
    print(
        f"Done. Scraped {scraped_new} new listing(s). Total cars in DB: {n} "
        f"(was {before_db_count} before this run). Exported to {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    url = sys.argv[1] if len(sys.argv) > 1 else ""
    run_scraper(url)
