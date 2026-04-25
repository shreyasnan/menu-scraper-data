#!/usr/bin/env python3
"""
Resolve scraped restaurants to Google Place IDs.

Reads GOOGLE_PLACES_API_KEY from the environment. For each unresolved
restaurant in the menu DB, calls Places "Find Place from Text" with
(name, city, CA) and writes the best match back.

Usage:
    export GOOGLE_PLACES_API_KEY=AIza...
    python resolve_places.py --limit 50          # quick sample run
    python resolve_places.py --only-useful       # only the ~639 "useful" restaurants
    python resolve_places.py --has-items         # any restaurant with items_found > 0
    python resolve_places.py                     # resolve everything unresolved

Flags:
    --limit N         stop after resolving N restaurants (default: all)
    --only-useful     skip restaurants that don't meet the quality bar
                      (>=10 items AND >=5 with price or rich description)
    --has-items       weaker bar: only skip restaurants with items_found = 0.
                      Useful for catching menus that didn't clear the rich
                      quality bar (e.g. name+description fused so description
                      is NULL). Ignored when --only-useful is set.
    --dry-run         log what we'd write but don't touch the DB
    --db PATH         path to SQLite DB (default: ../bay_area_menus.db
                      or $MENU_DB)
    --redo-review     also re-query restaurants previously flagged needs_review
"""

import argparse
import logging
import os
import sqlite3
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from menu_scraper.places_resolver import PlacesResolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("resolve_places")


def _resolve_db_path(cli_path: str | None) -> str:
    if cli_path:
        return cli_path
    if os.environ.get("MENU_DB"):
        return os.environ["MENU_DB"]
    return str(_HERE.parent / "bay_area_menus.db")


def _ensure_schema(con: sqlite3.Connection) -> None:
    """Add google_place_id + place_match_confidence + place_match_status
    columns if they don't exist. Safe to re-run.
    """
    cur = con.cursor()
    existing = {row[1] for row in cur.execute("PRAGMA table_info(restaurants)").fetchall()}
    migrations = [
        ("google_place_id",        "ALTER TABLE restaurants ADD COLUMN google_place_id TEXT"),
        ("place_matched_name",     "ALTER TABLE restaurants ADD COLUMN place_matched_name TEXT"),
        ("place_match_confidence", "ALTER TABLE restaurants ADD COLUMN place_match_confidence REAL"),
        ("place_match_status",     "ALTER TABLE restaurants ADD COLUMN place_match_status TEXT"),
        ("place_resolved_at",      "ALTER TABLE restaurants ADD COLUMN place_resolved_at TEXT"),
    ]
    for col, sql in migrations:
        if col not in existing:
            logger.info(f"Migration: adding restaurants.{col}")
            cur.execute(sql)
    con.commit()


def _candidate_sql(
    only_useful: bool,
    redo_review: bool,
    has_items: bool,
    cuisines: list[str] | None = None,
) -> tuple[str, list]:
    """SQL for restaurants that still need resolution. Returns (query, params)."""
    conds = [
        "(r.place_match_status IS NULL OR r.place_match_status = 'no_match'",
    ]
    if redo_review:
        conds[0] += " OR r.place_match_status = 'needs_review'"
    conds[0] += ")"
    conds.append("r.name IS NOT NULL AND r.name != ''")

    params: list = []
    if cuisines:
        # Match if `cuisine` LIKE any of the patterns (case-insensitive
        # via LOWER). OSM cuisines are often semi-colon separated lists
        # like "italian;pizza" so we use substring matching.
        like_clauses = " OR ".join(["LOWER(r.cuisine) LIKE ?"] * len(cuisines))
        conds.append(f"({like_clauses})")
        params.extend(f"%{c.lower()}%" for c in cuisines)

    if only_useful:
        quality_join = """
        JOIN (
            SELECT restaurant_id
            FROM menu_items
            GROUP BY restaurant_id
            HAVING COUNT(*) >= 10
               AND SUM(CASE WHEN price IS NOT NULL OR
                               (description IS NOT NULL AND LENGTH(description) > 10)
                            THEN 1 ELSE 0 END) >= 5
        ) q ON q.restaurant_id = r.id
        """
    else:
        quality_join = ""
        if has_items:
            conds.append("r.items_found > 0")

    sql = f"""
        SELECT r.id, r.name, r.city
        FROM restaurants r
        {quality_join}
        WHERE {' AND '.join(conds)}
        ORDER BY r.id
    """
    return sql, params


def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--only-useful", action="store_true")
    parser.add_argument("--has-items", action="store_true",
                        help="Weaker filter than --only-useful: items_found > 0")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--db", default=None)
    parser.add_argument("--redo-review", action="store_true")
    parser.add_argument("--cuisines", default=None,
                        help="Comma-separated cuisine substrings to filter by "
                             "(e.g. 'indian,italian,mexican,pizza'). Matches "
                             "OSM cuisine tags case-insensitively, including "
                             "semicolon-separated lists like 'italian;pizza'.")
    args = parser.parse_args()
    cuisines = [c.strip() for c in args.cuisines.split(",")] if args.cuisines else None

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("ERROR: set GOOGLE_PLACES_API_KEY in the environment.", file=sys.stderr)
        sys.exit(1)

    db_path = _resolve_db_path(args.db)
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(db_path)
    _ensure_schema(con)
    cur = con.cursor()

    sql, params = _candidate_sql(
        args.only_useful, args.redo_review, args.has_items, cuisines=cuisines
    )
    rows = cur.execute(sql, params).fetchall()
    if args.limit:
        rows = rows[: args.limit]

    logger.info(
        f"DB: {db_path} | to resolve: {len(rows):,} restaurants "
        f"{'(only useful)' if args.only_useful else '(all)'} "
        f"{'[dry-run]' if args.dry_run else ''}"
    )
    if not rows:
        logger.info("Nothing to do.")
        return

    resolver = PlacesResolver(api_key=api_key)

    stats = {"matched": 0, "needs_review": 0, "no_match": 0, "errors": 0}
    updates = []  # (place_id, matched_name, confidence, status, rid)

    for i, (rid, name, city) in enumerate(rows, start=1):
        try:
            match = resolver.resolve(name=name, city=city)
        except Exception as e:
            logger.error(f"[{i}/{len(rows)}] {name!r} ({city}): resolver error: {e}")
            stats["errors"] += 1
            continue

        if match is None:
            stats["no_match"] += 1
            updates.append((None, None, None, "no_match", rid))
            logger.info(f"[{i}/{len(rows)}] {name!r} ({city}): no match")
            continue

        status = match["status"]
        stats[status] += 1
        updates.append((
            match["place_id"],
            match["matched_name"],
            match["confidence"],
            status,
            rid,
        ))
        logger.info(
            f"[{i}/{len(rows)}] {name!r} ({city}) → "
            f"{match['matched_name']!r} "
            f"(conf={match['confidence']:.2f}, {status})"
        )

        # Commit every 25 so a crash mid-run doesn't lose progress.
        if not args.dry_run and len(updates) >= 25:
            _flush(cur, updates)
            con.commit()
            updates = []

    if not args.dry_run and updates:
        _flush(cur, updates)
        con.commit()

    logger.info(
        f"Done. matched={stats['matched']} "
        f"needs_review={stats['needs_review']} "
        f"no_match={stats['no_match']} "
        f"errors={stats['errors']}"
    )
    con.close()


def _flush(cur: sqlite3.Cursor, updates: list[tuple]) -> None:
    cur.executemany(
        """
        UPDATE restaurants
        SET google_place_id = ?,
            place_matched_name = ?,
            place_match_confidence = ?,
            place_match_status = ?,
            place_resolved_at = datetime('now')
        WHERE id = ?
        """,
        updates,
    )


if __name__ == "__main__":
    main()
