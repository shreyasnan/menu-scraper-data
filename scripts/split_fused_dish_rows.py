#!/usr/bin/env python3
"""
Split fused dish-name + description rows in menu_items.

Many menus were scraped with the dish name and the first descriptive
sentence concatenated into `name`, leaving `description` NULL. This
script detects those rows and splits them at the
lowercase/digit/punct → Capital+lowercase boundary.

Default mode is dry-run: reports to reports/fused_name_splits_dryrun.tsv
with a proposed action per row. Use --apply to write the splits back
to the DB in a single transaction (after taking a .bak snapshot).

Actions emitted:
    SPLIT           — clean split, will rewrite name + set description
    SKIP_NO_BOUNDARY — no clear split point found; leave row as-is
    SKIP_JUNK       — row is clearly not a menu item (JS code,
                      navigation bar, TOC with dots, etc.)
    FLAG_CATEGORY   — split would leave name looking like a menu
                      category ("Seafood Dishes", "Featured Items",
                      etc.) — skipped by default; review in TSV

Usage:
    python split_fused_dish_rows.py                # dry-run, write TSV
    python split_fused_dish_rows.py --apply        # dry-run + apply SPLITs
    python split_fused_dish_rows.py --db PATH      # override DB path
    python split_fused_dish_rows.py --min-len 80   # lower bound on name
"""

from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import shutil
import sqlite3
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_DATA_ROOT = _HERE.parent
_DEFAULT_DB = _DATA_ROOT / "bay_area_menus.db"
_DEFAULT_TSV = _DATA_ROOT / "reports" / "fused_name_splits_dryrun.tsv"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("split_fused_dish_rows")

# Primary split boundary: any lowercase letter, digit, ), ], ., *, or
# bullet star, followed directly by a Capital + lowercase pair with no
# whitespace between. Non-greedy on the name side so we grab the FIRST
# boundary, which is where the fusion happened.
SPLIT_RE = re.compile(
    r"^(?P<name>.{8,80}?[a-z0-9\)\]\.\*\u2605\u2606])(?P<desc>[A-Z][a-z][^\n]{15,})$"
)

# Junk signals — rows that clearly aren't a menu item.
JUNK_PATTERNS = [
    re.compile(r"\(function\s*\(html\)"),          # inline JS
    re.compile(r"documentElement|className"),       # inline JS
    re.compile(r"https?://"),                       # URL junk
    re.compile(r"^\s*\d+\.\s.*\.{5,}\s*$"),         # TOC "12. Foo ........"
    re.compile(r"\|"),                              # "Menu A | Menu B | ..."
]

# Tokens that, when they appear as the tail of the split `name`, suggest
# the "name" is actually a category header that got fused with the next
# dish+description. These are flagged for review, not auto-split.
CATEGORY_TAIL = re.compile(
    r"(?i)(?:^|\W)(?:"
    r"featured items?|popular items?|signature items?|best sellers?|"
    r"seafood dishes|beef dishes|chicken dishes|pork dishes|"
    r"vegetarian dishes|vegan dishes|side dishes|main dishes|"
    r"lunch specials?|dinner specials?|daily specials?|chef'?s specials?|"
    r"char broiled burgers|house burgers|classic burgers|"
    r"sweet\s*&\s*savory crepes|savory crepes|sweet crepes|"
    r"house specialties|house favorites|chef'?s favorites|"
    r"appetizers?|starters?|entr[ée]es?|mains?|sides?|desserts?|"
    r"beverages?|drinks?|cocktails?|sandwiches|salads|soups|"
    r"specialty rolls?|signature rolls?|classic rolls?"
    r")\s*$"
)

# Section-label prefix: a split-name that STARTS with an all-caps or
# title-case section label and is then followed immediately by a new
# token (capital, digit, or emoji/symbol). Produces bad names like
# "APPETIZERToong Gyen Yung" or "SoupsTom Yum" — flag for review
# instead of splitting. Case-insensitive across the labels.
CATEGORY_PREFIX = re.compile(
    r"^(?:menu|appetizers?|starters?|entr[eé]es?|mains?|sides?|"
    r"desserts?|beverages?|drinks?|specials?|salads?|soups?|"
    r"sandwiches|burgers|pizzas?|rolls?|breakfast|brunch|lunch|"
    r"dinner|kids?|bar|wine|beer|catering|happy\s*hour|"
    r"featured|popular|best\s*sellers?|signature|house\s*favorites?|"
    r"house\s*specialties|chef'?s\s*(?:specials?|favorites?)|"
    r"rewards?|home|pickup|delivery|online|order)"
    r"(?=[^a-z\s])",  # must be followed by non-lowercase, non-space
    re.IGNORECASE,
)


def _scan(
    rows: list[tuple[int, int, str]],
) -> tuple[list[dict], dict[str, int]]:
    out: list[dict] = []
    stats = {
        "SPLIT": 0,
        "SKIP_NO_BOUNDARY": 0,
        "SKIP_JUNK": 0,
        "FLAG_CATEGORY": 0,
    }
    for mid, rid, full_name in rows:
        if any(p.search(full_name) for p in JUNK_PATTERNS):
            stats["SKIP_JUNK"] += 1
            out.append({
                "menu_item_id": mid,
                "restaurant_id": rid,
                "action": "SKIP_JUNK",
                "orig_len": len(full_name),
                "new_name": "",
                "new_desc": "",
                "preview": full_name[:120],
            })
            continue

        m = SPLIT_RE.match(full_name)
        if not m:
            stats["SKIP_NO_BOUNDARY"] += 1
            out.append({
                "menu_item_id": mid,
                "restaurant_id": rid,
                "action": "SKIP_NO_BOUNDARY",
                "orig_len": len(full_name),
                "new_name": "",
                "new_desc": "",
                "preview": full_name[:120],
            })
            continue

        name = m.group("name").strip()
        desc = m.group("desc").strip()

        if CATEGORY_TAIL.search(name) or CATEGORY_PREFIX.match(name):
            stats["FLAG_CATEGORY"] += 1
            out.append({
                "menu_item_id": mid,
                "restaurant_id": rid,
                "action": "FLAG_CATEGORY",
                "orig_len": len(full_name),
                "new_name": name,
                "new_desc": desc,
                "preview": full_name[:120],
            })
            continue

        stats["SPLIT"] += 1
        out.append({
            "menu_item_id": mid,
            "restaurant_id": rid,
            "action": "SPLIT",
            "orig_len": len(full_name),
            "new_name": name,
            "new_desc": desc,
            "preview": full_name[:120],
        })
    return out, stats


def _write_tsv(path: Path, results: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "menu_item_id", "restaurant_id", "action", "orig_len",
        "new_name", "new_desc", "preview",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, delimiter="\t")
        w.writeheader()
        for r in results:
            w.writerow(r)


def _apply(con: sqlite3.Connection, results: list[dict]) -> int:
    updates = [
        (r["new_name"], r["new_desc"], r["menu_item_id"])
        for r in results
        if r["action"] == "SPLIT"
    ]
    if not updates:
        return 0
    cur = con.cursor()
    cur.executemany(
        "UPDATE menu_items SET name = ?, description = ? WHERE id = ?",
        updates,
    )
    con.commit()
    return len(updates)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--db", default=str(_DEFAULT_DB))
    parser.add_argument("--tsv", default=str(_DEFAULT_TSV))
    parser.add_argument("--min-len", type=int, default=80,
                        help="Only scan rows where length(name) > this. "
                             "Default: 80.")
    parser.add_argument("--apply", action="store_true",
                        help="Write SPLIT updates to DB after .bak snapshot.")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error(f"DB not found at {db_path}")
        return 1

    con = sqlite3.connect(str(db_path))
    cur = con.cursor()

    rows = cur.execute(
        "SELECT id, restaurant_id, name FROM menu_items "
        "WHERE length(name) > ? AND description IS NULL "
        "ORDER BY id",
        (args.min_len,),
    ).fetchall()
    logger.info(f"DB: {db_path}  candidates: {len(rows):,}")

    results, stats = _scan(rows)
    tsv_path = Path(args.tsv)
    _write_tsv(tsv_path, results)
    logger.info(f"Dry-run TSV written: {tsv_path}")
    logger.info(
        "Proposed actions: "
        f"SPLIT={stats['SPLIT']:,}  "
        f"FLAG_CATEGORY={stats['FLAG_CATEGORY']:,}  "
        f"SKIP_NO_BOUNDARY={stats['SKIP_NO_BOUNDARY']:,}  "
        f"SKIP_JUNK={stats['SKIP_JUNK']:,}"
    )

    if args.apply:
        bak = db_path.with_suffix(db_path.suffix + ".bak")
        if bak.exists():
            logger.warning(f"Existing backup at {bak} — leaving it in place")
        else:
            shutil.copy2(db_path, bak)
            logger.info(f"Snapshot: {bak}")
        n = _apply(con, results)
        logger.info(f"Applied {n:,} SPLIT updates.")
    else:
        logger.info("Dry-run only. Re-run with --apply to write SPLIT rows.")

    con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
