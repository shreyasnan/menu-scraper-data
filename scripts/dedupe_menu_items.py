#!/usr/bin/env python3
"""
Collapse duplicate rows in menu_items keyed by (restaurant_id, name).

History: `scrape_one` in batch_chunk.py used to INSERT new rows on
every re-scrape without deleting the old ones. That bug has been
fixed (DELETE-before-INSERT), but the DB has accumulated ~17k
duplicate (restaurant_id, name) pairs from months of prior runs.
This script cleans them up.

Dedup rule: within each (restaurant_id, name) group, keep the row
with the MOST data — prefer non-null price, then non-null rich
description (>10 chars), then the oldest id. The others are
deleted.

Safe to run repeatedly. If there's nothing to dedupe it reports 0
and exits.

Usage:
    python dedupe_menu_items.py              # dry-run
    python dedupe_menu_items.py --apply      # actually delete
    python dedupe_menu_items.py --db PATH    # override DB location
                                             # (default: $MENU_DB or ../bay_area_menus.db)

Intended to run as a step in the daily scheduled task, right after
batch_chunk.py finishes and before push_menus_to_storage.py. That
way Storage always receives cleaned data.
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
logger = logging.getLogger("dedupe_menu_items")

_HERE = Path(__file__).resolve().parent
_DEFAULT_DB = _HERE.parent / "bay_area_menus.db"


def _resolve_db_path(cli_path: str | None) -> str:
    if cli_path:
        return cli_path
    if os.environ.get("MENU_DB"):
        return os.environ["MENU_DB"]
    return str(_DEFAULT_DB)


def dedupe(db_path: str, apply: bool) -> tuple[int, int]:
    """Return (groups_processed, rows_deleted)."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    groups = cur.execute(
        """
        SELECT restaurant_id, name, COUNT(*) AS n
        FROM menu_items
        WHERE name IS NOT NULL AND name != ''
        GROUP BY restaurant_id, name
        HAVING n > 1
        """
    ).fetchall()

    if not groups:
        logger.info("No duplicates. DB is clean.")
        con.close()
        return 0, 0

    total_rows = sum(n for _, _, n in groups)
    logger.info(
        f"{len(groups):,} duplicate (restaurant_id, name) groups "
        f"covering {total_rows:,} rows; "
        f"will keep {len(groups):,} and delete {total_rows - len(groups):,}."
    )

    deleted = 0
    # Keep the "richest" row per group. SQLite booleans are 0/1,
    # so DESC puts non-null first.
    for rid, name, _ in groups:
        rows = cur.execute(
            """
            SELECT id FROM menu_items
            WHERE restaurant_id = ? AND name = ?
            ORDER BY
                (price IS NOT NULL) DESC,
                (description IS NOT NULL AND LENGTH(description) > 10) DESC,
                id ASC
            """,
            (rid, name),
        ).fetchall()
        delete_ids = [r[0] for r in rows[1:]]
        if not delete_ids:
            continue
        if apply:
            cur.executemany(
                "DELETE FROM menu_items WHERE id = ?",
                [(did,) for did in delete_ids],
            )
        deleted += len(delete_ids)

    if apply:
        con.commit()
        logger.info(f"Deleted {deleted:,} duplicate rows.")
    else:
        logger.info(
            f"Would delete {deleted:,} duplicate rows "
            f"(dry-run; re-run with --apply to commit)."
        )

    # Report remaining counts for sanity.
    remaining = cur.execute("SELECT COUNT(*) FROM menu_items").fetchone()[0]
    logger.info(f"menu_items row count: {remaining:,}")
    con.close()
    return len(groups), deleted


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=None,
                        help="Path to SQLite DB (default: $MENU_DB or ../bay_area_menus.db)")
    parser.add_argument("--apply", action="store_true",
                        help="Actually delete rows (default: dry-run)")
    args = parser.parse_args()

    db_path = _resolve_db_path(args.db)
    if not Path(db_path).exists():
        logger.error(f"DB not found at {db_path}")
        return 1

    dedupe(db_path, apply=args.apply)
    return 0


if __name__ == "__main__":
    sys.exit(main())
