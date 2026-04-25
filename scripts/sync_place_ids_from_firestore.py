#!/usr/bin/env python3
"""
Pull resolved Google Place IDs from the iOS app's Firestore into the
scraper's SQLite DB.

The iOS app maintains the canonical place_id for each restaurant (set
when a user saves a restaurant via the Places API on the device, or
backfilled by `backfill_firestore_place_ids.py`). The scraper's
SQLite needs those same place_ids to key its menu uploads to
Firebase Storage. This script copies them across.

Free — no Google Places API call. Reads only.

Match strategy:
    1. Exact match on (lower(name), lower(city)) — most reliable.
    2. Fallback: lower(name) only when there's exactly ONE candidate
       in the scraper DB. Avoids cross-restaurant collisions.

Usage:
    export GOOGLE_APPLICATION_CREDENTIALS=/path/to/firebase-admin-key.json
    python sync_place_ids_from_firestore.py            # apply
    python sync_place_ids_from_firestore.py --dry-run  # report only
    python sync_place_ids_from_firestore.py --db PATH  # override DB

Intended to run as a step in the daily scheduled task, before
push_menus_to_storage.py — that way every daily run picks up the
latest place_ids any iOS user has added.
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("sync_place_ids")

_HERE = Path(__file__).resolve().parent
_DEFAULT_DB = _HERE.parent / "bay_area_menus.db"
_DEFAULT_KEY = _HERE.parent / "firebase-admin-key.json"


def _resolve_db_path(cli_path: str | None) -> str:
    if cli_path:
        return cli_path
    if os.environ.get("MENU_DB"):
        return os.environ["MENU_DB"]
    return str(_DEFAULT_DB)


def _city_from_address(address: str) -> str:
    """Extract a city-ish segment from a comma-separated address.
    Tolerant: just grabs the part likely to be the city."""
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",") if p.strip()]
    if len(parts) < 2:
        return ""
    # Address shape is usually "Street, City, State ZIP, Country" or
    # "Street, City, State ZIP". The city is the second segment.
    return parts[1]


def _ensure_schema(con: sqlite3.Connection) -> None:
    """Idempotent column adds — required if init_db migration didn't
    run first. Mirrors the migration list in batch_chunk.py."""
    cur = con.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(restaurants)").fetchall()}
    for col, sql in [
        ("google_place_id",        "ALTER TABLE restaurants ADD COLUMN google_place_id TEXT"),
        ("place_matched_name",     "ALTER TABLE restaurants ADD COLUMN place_matched_name TEXT"),
        ("place_match_confidence", "ALTER TABLE restaurants ADD COLUMN place_match_confidence REAL"),
        ("place_match_status",     "ALTER TABLE restaurants ADD COLUMN place_match_status TEXT"),
        ("place_resolved_at",      "ALTER TABLE restaurants ADD COLUMN place_resolved_at TEXT"),
    ]:
        if col not in existing:
            logger.info(f"Migration: adding restaurants.{col}")
            cur.execute(sql)
    con.commit()


def _load_firestore_resolutions(service_account_path: str) -> list[dict]:
    """Pull every Firestore restaurant doc that has a googlePlaceId.
    Returns a normalized list of {name, city, place_id, matched_name,
    confidence, status, resolved_at}."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        logger.error("firebase-admin not installed. Run: pip install firebase-admin")
        sys.exit(2)

    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)
    db = firestore.client()

    out = []
    docs = db.collection_group("restaurants").stream()
    for doc in docs:
        data = doc.to_dict() or {}
        place_id = data.get("googlePlaceId")
        if not place_id:
            continue
        name = (data.get("name") or "").strip()
        if not name:
            continue
        city = _city_from_address(data.get("address") or "")
        out.append({
            "name": name,
            "city": city,
            "place_id": place_id,
            "matched_name": data.get("placeMatchedName") or name,
            "confidence": data.get("placeMatchConfidence"),
            "status": data.get("placeMatchStatus") or "matched",
            "resolved_at": data.get("placeResolvedAt"),
        })
    return out


def _find_scraper_match(
    cur: sqlite3.Cursor, name: str, city: str
) -> int | None:
    """Locate the scraper DB row for a Firestore restaurant. Prefer
    exact (name, city) match; fall back to name-only when unambiguous."""
    name_l = name.lower()
    city_l = (city or "").lower()

    # 1. Exact (name, city)
    if city_l:
        rows = cur.execute(
            "SELECT id FROM restaurants WHERE LOWER(name) = ? AND LOWER(city) = ?",
            (name_l, city_l),
        ).fetchall()
        if len(rows) == 1:
            return rows[0][0]

    # 2. Name-only when there's exactly ONE scraper restaurant by that name
    rows = cur.execute(
        "SELECT id FROM restaurants WHERE LOWER(name) = ?",
        (name_l,),
    ).fetchall()
    if len(rows) == 1:
        return rows[0][0]

    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=None)
    parser.add_argument("--service-account", default=str(_DEFAULT_KEY),
                        help="Firebase admin service-account JSON")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--include-review", action="store_true",
                        help="Also sync Firestore docs flagged needs_review")
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db)
    if not Path(db_path).exists():
        logger.error(f"DB not found: {db_path}")
        return 1

    if args.service_account and Path(args.service_account).exists():
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", args.service_account)

    con = sqlite3.connect(db_path)
    _ensure_schema(con)
    cur = con.cursor()

    logger.info("Fetching place_id resolutions from Firestore…")
    fs_rows = _load_firestore_resolutions(args.service_account)
    logger.info(f"Firestore resolutions with googlePlaceId: {len(fs_rows):,}")

    matched = updated = skipped_no_match = skipped_already = 0
    for row in fs_rows:
        if not args.include_review and row["status"] not in ("matched", None):
            # Skip needs_review unless the user opts in
            continue

        rid = _find_scraper_match(cur, row["name"], row["city"])
        if rid is None:
            skipped_no_match += 1
            continue
        matched += 1

        # Don't overwrite an existing scraper-side resolution unless
        # ours is stronger or theirs is missing.
        existing_pid = cur.execute(
            "SELECT google_place_id FROM restaurants WHERE id = ?", (rid,)
        ).fetchone()[0]
        if existing_pid == row["place_id"]:
            skipped_already += 1
            continue

        if args.dry_run:
            logger.info(
                f"[dry] would update rid={rid} '{row['name']}' "
                f"({row['city']}) → {row['place_id'][:30]}…"
            )
            continue

        cur.execute(
            """
            UPDATE restaurants
            SET google_place_id = ?,
                place_matched_name = ?,
                place_match_confidence = ?,
                place_match_status = ?,
                place_resolved_at = COALESCE(?, datetime('now'))
            WHERE id = ?
            """,
            (
                row["place_id"], row["matched_name"], row["confidence"],
                row["status"], row["resolved_at"], rid,
            ),
        )
        updated += 1

    if not args.dry_run:
        con.commit()

    logger.info(
        f"Done. matched={matched:,} updated={updated:,} "
        f"already_in_sync={skipped_already:,} no_scraper_match={skipped_no_match:,}"
    )
    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
