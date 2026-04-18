#!/usr/bin/env python3
"""
Cleanup pass for bay_area_menus.db.

Two passes:
  A. DELETE items that are clearly nav/UI/promo junk (exact-match nav patterns,
     items containing year stamps, items in obviously-non-menu categories).
  B. CLEAN surviving items in place — strip HTML residue, markdown noise,
     normalize whitespace.

Then re-audits "good restaurants" using two bars:
  strict:  items with price + meaningful description
  relaxed: ≥10 items, ≥5 of which have price OR description >10 chars

Rules live in `menu_scraper/junk_filter.py` so this script and the live
website scraper share one source of truth for what counts as junk.

Usage:
    python cleanup_menu_db.py                       # uses ../bay_area_menus.db
    python cleanup_menu_db.py /path/to/another.db
    MENU_DB=/some/path python cleanup_menu_db.py
"""

import os
import sqlite3
import sys
from pathlib import Path

# Make the menu_scraper package importable when this script is run directly
# from the scripts/ directory.
_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from menu_scraper.junk_filter import is_junk_name, clean_text


def _resolve_db_path() -> str:
    if len(sys.argv) > 1:
        return sys.argv[1]
    if os.environ.get("MENU_DB"):
        return os.environ["MENU_DB"]
    # Default: sibling of scripts/ in the repo layout
    return str(_HERE.parent / "bay_area_menus.db")


def main():
    db_path = _resolve_db_path()
    if not os.path.exists(db_path):
        print(f"ERROR: DB not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    con = sqlite3.connect(db_path)
    cur = con.cursor()

    n_items_before = cur.execute("SELECT COUNT(*) FROM menu_items").fetchone()[0]
    n_rest_before = cur.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0]
    print(f"DB: {db_path}")
    print(f"Before: {n_items_before:,} menu items / {n_rest_before:,} restaurants")

    # PASS A — drop junk by name
    to_delete = []
    for item_id, name in cur.execute("SELECT id, name FROM menu_items"):
        if is_junk_name(name or ""):
            to_delete.append(item_id)
    if to_delete:
        for i in range(0, len(to_delete), 500):
            chunk = to_delete[i:i+500]
            cur.execute(
                f"DELETE FROM menu_items WHERE id IN ({','.join('?'*len(chunk))})",
                chunk,
            )
    con.commit()
    print(f"Pass A — deleted {len(to_delete):,} junk items.")

    # PASS B — clean HTML/whitespace on surviving items; drop those whose
    # name becomes empty after stripping (NOT NULL constraint on `name`).
    updates = []
    delete_after_clean = []
    for item_id, name, category, description in cur.execute(
        "SELECT id, name, category, description FROM menu_items"
    ):
        new_name = clean_text(name)
        new_category = clean_text(category)
        new_description = clean_text(description)
        if not new_name:
            delete_after_clean.append(item_id)
            continue
        if (new_name, new_category, new_description) != (name, category, description):
            updates.append((new_name, new_category, new_description, item_id))
    if updates:
        cur.executemany(
            "UPDATE menu_items SET name=?, category=?, description=? WHERE id=?",
            updates,
        )
    if delete_after_clean:
        for i in range(0, len(delete_after_clean), 500):
            chunk = delete_after_clean[i:i+500]
            cur.execute(
                f"DELETE FROM menu_items WHERE id IN ({','.join('?'*len(chunk))})",
                chunk,
            )
    con.commit()
    print(f"Pass B — cleaned (HTML/whitespace) {len(updates):,} items, "
          f"deleted {len(delete_after_clean):,} that became empty after cleaning.")

    n_items_after = cur.execute("SELECT COUNT(*) FROM menu_items").fetchone()[0]
    print(f"\nAfter:  {n_items_after:,} menu items "
          f"(-{n_items_before - n_items_after:,})")

    print("\n=== STRICT BAR (price + description >10) ===")
    n = cur.execute("""
        SELECT COUNT(DISTINCT mi.restaurant_id)
        FROM menu_items mi
        WHERE mi.price IS NOT NULL
          AND mi.description IS NOT NULL AND LENGTH(mi.description) > 10
    """).fetchone()[0]
    print(f"  {n} restaurants meet the strict bar")

    print("\n=== RELAXED BAR (≥10 items, ≥5 of which have price OR description) ===")
    n_relaxed = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT restaurant_id,
                   COUNT(*) AS total,
                   SUM(CASE WHEN price IS NOT NULL OR (description IS NOT NULL AND LENGTH(description) > 10) THEN 1 ELSE 0 END) AS rich
            FROM menu_items
            GROUP BY restaurant_id
        ) WHERE total >= 10 AND rich >= 5
    """).fetchone()[0]
    print(f"  {n_relaxed} restaurants meet the relaxed bar")

    print("\n=== EVEN MORE RELAXED (≥15 items) ===")
    n_15 = cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT restaurant_id, COUNT(*) AS total
            FROM menu_items GROUP BY restaurant_id
        ) WHERE total >= 15
    """).fetchone()[0]
    print(f"  {n_15} restaurants have ≥15 items (regardless of price/description)")

    con.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
