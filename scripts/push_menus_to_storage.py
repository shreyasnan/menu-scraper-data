#!/usr/bin/env python3
"""
Push scraped menus from SQLite to Firebase Cloud Storage, keyed by Place ID.

The iOS app's MenuDataService fetches menus from
    gs://forkbook-fe65b.firebasestorage.app/menus/{placeId}.json
so we need to produce one JSON file per (resolved) restaurant in the
scraper DB.

Schema v1 — keep in sync with MenuDataService.swift:
    {
      "v": 1,
      "name": "Darbar Indian Cuisine",
      "cuisine": "indian",
      "city": "Palo Alto",
      "dishes": [
        {"n": "Butter Chicken", "d": "...", "p": 22.0}
      ],
      "scrapedAt": "2026-04-18T12:34:56Z",
      "source": "website"
    }

Only restaurants with a resolved `google_place_id` and `place_match_status
== 'matched'` are uploaded. `needs_review` rows are skipped by default —
use --include-review to push them too (useful after you've manually
cleaned up the review list).

Idempotent. Uses each object's MD5 hash to skip uploads when the local
build matches what's already in Storage. Safe to re-run daily.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/firebase-admin-key.json
    python push_menus_to_storage.py --dry-run --limit 5
    python push_menus_to_storage.py --limit 50
    python push_menus_to_storage.py                     # do them all
    python push_menus_to_storage.py --include-review
"""

import argparse
import hashlib
import json
import logging
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from menu_scraper.dish_normalizer import normalize_dish

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("push_menus")

# Match MenuDataService.schemaVersion in the iOS client.
SCHEMA_VERSION = 1

# Default bucket — lines up with GoogleService-Info.plist STORAGE_BUCKET.
DEFAULT_BUCKET = "forkbook-fe65b.firebasestorage.app"

# Max dish-name length before we assume it's actually a description. Real
# dish names are compact — a fancy modifier chain like "Sticky Rice Shao
# Mai with Kurobuta Pork & Mushroom" is ~50 chars; things like "A light
# and delicious steamed beef broth with tender slices..." are 80+.
MAX_DISH_NAME_LEN = 60

# Prose-marker prefixes we see on descriptions getting misclassified as
# names. Lowercased, match at word boundary. Keep this tight — real dish
# names like "The Reuben" or "A5 Wagyu" should still pass.
DESCRIPTION_PREFIXES = (
    "a light ", "a rich ", "a warm ", "a fresh ", "a delicate ",
    "our signature ", "our famous ", "our classic ",
    "made with ", "served with ", "topped with ", "featuring ",
    "crafted with ", "prepared with ",
)

# Category-header words that occasionally leak into menu_items. These
# appear in ALL CAPS in the source HTML (section dividers), and the
# scraper sometimes pulls them into the name column. Compared
# case-insensitively after a caps check.
SECTION_HEADER_WORDS = {
    "wontons", "greens", "appetizers", "starters", "sides", "salads",
    "soups", "desserts", "drinks", "beverages", "beers", "wines", "cocktails",
    "mains", "entrees", "entrées", "noodles", "rice", "dumplings", "specials",
    "breakfast", "lunch", "dinner", "brunch", "kids menu", "for the table",
}


def is_dishlike(name: str) -> bool:
    """Cheap heuristic: does this look like a real dish name, or scraper noise?

    Returns True to keep, False to drop. Intentionally conservative — we'd
    rather drop an edge-case real dish than ship descriptions and section
    headers as chips in the iOS UI.
    """
    if not name:
        return False
    s = name.strip()
    if len(s) > MAX_DISH_NAME_LEN:
        return False
    low = s.lower()
    # Section headers: short ALL-CAPS strings matching a known header word.
    if s == s.upper() and s.lower() in SECTION_HEADER_WORDS:
        return False
    # Description prose: starts with a known prose marker.
    for prefix in DESCRIPTION_PREFIXES:
        if low.startswith(prefix):
            return False
    # 3+ commas strongly suggests a descriptive sentence, not a dish name.
    # ("Served with jasmine rice, bok choy, and scallions")
    if s.count(",") >= 3:
        return False
    return True

_HERE = Path(__file__).resolve().parent
DEFAULT_DB = _HERE.parent / "bay_area_menus.db"
DEFAULT_SERVICE_ACCOUNT = _HERE.parent / "firebase-admin-key.json"


def build_menu_payload(
    row: sqlite3.Row, dishes: list[sqlite3.Row]
) -> tuple[dict, int]:
    """Assemble the v1 JSON for one restaurant.

    Ordering: priced dishes first (price desc), then priceless dishes in
    the order they were scraped. This matters because ~67% of scraped
    items have no price — if we sort purely by price, priceless blurbs
    ("1 to 4 guests", catering copy, section headers) end up at the top
    by default. The iOS client's `topDishes(limit:)` does `prefix(n)`, so
    the top items need to be actual main-course candidates.
    """
    priced = [d for d in dishes if d["price"] is not None and d["price"] > 0]
    priced.sort(key=lambda d: d["price"], reverse=True)
    priceless = [d for d in dishes if not (d["price"] is not None and d["price"] > 0)]
    ordered = priced + priceless

    # Dedupe on (cleaned name, price). The scraper sometimes inserts
    # the same dish twice; the iOS UI shouldn't show the same dish
    # twice. Keep "Salad $8" and "Salad $12" distinct since that's a
    # real menu choice.
    seen: set[tuple[str, float]] = set()
    dish_payload = []
    dropped_noise = 0
    for d in ordered:
        raw_name = (d["name"] or "").strip()
        if not raw_name:
            continue
        # Skip scraper noise (section headers, contact info, prose) on
        # the RAW name — the noise filter is tuned for unprocessed
        # scraper output.
        if not is_dishlike(raw_name):
            dropped_noise += 1
            continue

        price = d["price"]
        price_val = float(price) if (price is not None and price > 0) else 0.0

        # Normalize: split fused name+description, title-case lowercase
        # menus (CSS text-transform artifacts). See
        # menu_scraper/dish_normalizer.py for the heuristics.
        # Conservative — clean inputs pass through unchanged.
        item = normalize_dish(
            raw_name,
            d["description"],
            price_val if price_val > 0 else None,
        )

        # Dedupe on the CLEANED name so post-normalization duplicates
        # collapse (e.g. two raw inputs that title-case to the same
        # canonical name).
        dedupe_key = (item["n"].lower(), price_val)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)

        dish_payload.append(item)

    scraped_at = row["scraped_at"]
    if scraped_at:
        # SQLite stores as 'YYYY-MM-DD HH:MM:SS'. Normalize to ISO8601 Z.
        try:
            dt = datetime.fromisoformat(scraped_at.replace(" ", "T"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            scraped_at_iso = dt.isoformat().replace("+00:00", "Z")
        except ValueError:
            scraped_at_iso = None
    else:
        scraped_at_iso = None

    payload: dict = {
        "v": SCHEMA_VERSION,
        "name": row["name"] or "",
        "dishes": dish_payload,
    }
    if row["cuisine"]:
        payload["cuisine"] = row["cuisine"]
    if row["city"]:
        payload["city"] = row["city"]
    if scraped_at_iso:
        payload["scrapedAt"] = scraped_at_iso
    # `source` — we don't track per-restaurant source in this schema, but
    # leave a slot in case we add a `source` column later.
    return payload, dropped_noise


def canonical_json(payload: dict) -> bytes:
    """Deterministic serialization so the MD5 check in upload is stable."""
    return json.dumps(
        payload,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def fetch_restaurants(
    conn: sqlite3.Connection,
    include_review: bool,
    limit: Optional[int],
) -> list[sqlite3.Row]:
    # Only push rows we're confident about, and that actually have dishes.
    statuses = ("matched",) if not include_review else ("matched", "needs_review")
    placeholders = ",".join(["?"] * len(statuses))
    query = f"""
        SELECT r.id, r.name, r.cuisine, r.city, r.google_place_id,
               r.scraped_at, r.place_match_confidence, r.place_match_status
        FROM restaurants r
        WHERE r.google_place_id IS NOT NULL
          AND r.place_match_status IN ({placeholders})
          AND EXISTS (SELECT 1 FROM menu_items m WHERE m.restaurant_id = r.id)
        ORDER BY r.id
    """
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return conn.execute(query, statuses).fetchall()


def fetch_dishes(conn: sqlite3.Connection, restaurant_id: int) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT name, description, price, price_text, category
        FROM menu_items
        WHERE restaurant_id = ?
        """,
        (restaurant_id,),
    ).fetchall()


def upload_one(
    bucket,
    place_id: str,
    payload_bytes: bytes,
    dry_run: bool,
) -> str:
    """Upload one menu JSON. Returns a status tag: 'uploaded' / 'unchanged' / 'dry'."""
    if dry_run:
        return "dry"
    blob = bucket.blob(f"menus/{place_id}.json")

    # Skip if remote object already matches byte-for-byte.
    new_md5 = hashlib.md5(payload_bytes).hexdigest()
    try:
        blob.reload()
        # google-cloud-storage exposes the server MD5 as hex via md5_hash (b64)
        # but the content_md5 property is not always populated — easier to
        # compare sizes + md5_hash converted from b64.
        import base64
        if blob.md5_hash:
            remote_md5 = base64.b64decode(blob.md5_hash).hex()
            if remote_md5 == new_md5 and blob.size == len(payload_bytes):
                return "unchanged"
    except Exception:
        # NotFound etc. — fall through to upload.
        pass

    blob.cache_control = "public, max-age=3600"
    blob.content_type = "application/json; charset=utf-8"
    blob.upload_from_string(payload_bytes, content_type="application/json; charset=utf-8")
    return "uploaded"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path")
    ap.add_argument(
        "--service-account", default=str(DEFAULT_SERVICE_ACCOUNT),
        help="Firebase admin service-account JSON (overrides GOOGLE_APPLICATION_CREDENTIALS)",
    )
    ap.add_argument("--bucket", default=DEFAULT_BUCKET, help="Cloud Storage bucket name")
    ap.add_argument("--limit", type=int, default=None, help="Max rows to process")
    ap.add_argument("--dry-run", action="store_true", help="Build payloads but do not upload")
    ap.add_argument(
        "--include-review", action="store_true",
        help="Also upload needs_review matches (normally skipped)",
    )
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("DB not found: %s", db_path)
        return 2

    if args.service_account and Path(args.service_account).exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.service_account)

    # Lazy import so --dry-run still works with no cloud creds installed.
    if not args.dry_run:
        try:
            from google.cloud import storage
        except ImportError:
            logger.error(
                "google-cloud-storage not installed. "
                "Run: pip install -r requirements.txt"
            )
            return 2
        client = storage.Client()
        bucket = client.bucket(args.bucket)
    else:
        bucket = None

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    rows = fetch_restaurants(
        conn, include_review=args.include_review, limit=args.limit
    )
    logger.info("Found %d restaurants to push", len(rows))

    uploaded = unchanged = empty = failed = 0
    total_noise_dropped = 0
    for row in rows:
        place_id = row["google_place_id"]
        dishes = fetch_dishes(conn, row["id"])
        payload, dropped_noise = build_menu_payload(row, dishes)
        total_noise_dropped += dropped_noise
        if not payload["dishes"]:
            logger.info("SKIP %s %s — no dishes (dropped %d noise)",
                        place_id, row["name"], dropped_noise)
            empty += 1
            continue
        payload_bytes = canonical_json(payload)

        try:
            status = upload_one(
                bucket, place_id, payload_bytes, dry_run=args.dry_run
            )
        except Exception as e:
            logger.warning("FAIL %s %s: %s", place_id, row["name"], e)
            failed += 1
            continue

        size_kb = len(payload_bytes) / 1024.0
        tag = {
            "uploaded": "PUT ",
            "unchanged": "SAME",
            "dry": "DRY ",
        }.get(status, "??? ")
        noise_tag = f" [-{dropped_noise} noise]" if dropped_noise else ""
        logger.info(
            "%s %s %s (%d dishes, %.1f KB)%s",
            tag, place_id, row["name"], len(payload["dishes"]), size_kb, noise_tag,
        )
        if status == "uploaded":
            uploaded += 1
        elif status == "unchanged":
            unchanged += 1

    logger.info(
        "Done. uploaded=%d unchanged=%d no-dishes=%d failed=%d "
        "noise-dropped=%d (total=%d)",
        uploaded, unchanged, empty, failed, total_noise_dropped, len(rows),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
