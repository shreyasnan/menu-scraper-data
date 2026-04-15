#!/usr/bin/env python3
"""
Seed restaurants into the DB from an OSM Overpass extraction.

Usage:
    python seed_from_osm.py <restaurants.json> [--db /tmp/menus.db] [--source osm]

Expects JSON in the shape produced by extract_osm.py:
    [{"name": "...", "website": "...", "cuisine": "...", "city": "..."}, ...]

Inserts with scrape_status='pending'. Idempotent — the restaurants table
uses UNIQUE(name, address), so reruns only add new rows.
"""

import argparse
import json
import os
import sqlite3
import sys


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("input", help="JSON file of {name, website, cuisine, city} dicts")
    ap.add_argument("--db", default=os.environ.get("MENU_DB", "/tmp/menus.db"))
    ap.add_argument("--source", default="osm")
    args = ap.parse_args()

    with open(args.input) as f:
        rows = json.load(f)

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA journal_mode=WAL")

    # Make sure batch columns exist
    cols = {r[1] for r in conn.execute("PRAGMA table_info(restaurants)")}
    if "scrape_status" not in cols:
        conn.execute("ALTER TABLE restaurants ADD COLUMN scrape_status TEXT DEFAULT 'pending'")
    if "last_scraped_at" not in cols:
        conn.execute("ALTER TABLE restaurants ADD COLUMN last_scraped_at TEXT")

    inserted = 0
    skipped = 0
    for r in rows:
        name = r.get("name")
        website = r.get("website")
        if not name or not website:
            skipped += 1
            continue
        # Use the city as a pseudo-address so UNIQUE(name, address) lets us
        # differentiate same-named chains across cities.
        address = r.get("city") or None
        try:
            conn.execute(
                """
                INSERT INTO restaurants (name, address, website, cuisine_type, source, source_url, scrape_status)
                VALUES (?, ?, ?, ?, ?, ?, 'pending')
                ON CONFLICT(name, address) DO NOTHING
                """,
                (name, address, website, r.get("cuisine") or None, args.source, website),
            )
            if conn.total_changes > inserted:
                inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
    conn.commit()
    conn.close()

    print(f"Inserted {inserted} new restaurants, skipped {skipped}.")
    print(f"Next: python batch_chunk.py 30 (in a loop)")


if __name__ == "__main__":
    main()
