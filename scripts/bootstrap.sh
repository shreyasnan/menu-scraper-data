#!/usr/bin/env bash
# Bootstrap a working environment for the menu scraper.
# Usage: ./bootstrap.sh [db_path]
#
# Installs Python deps + Chromium, initializes the SQLite DB, exports MENU_DB
# for the current shell. Idempotent.

set -e

DB_PATH="${1:-/tmp/menus.db}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "== installing python deps =="
pip install -r "$SCRIPT_DIR/requirements.txt" --break-system-packages --quiet

echo "== installing Chromium for Playwright =="
python -m playwright install chromium

echo "== initializing DB at $DB_PATH =="
cd "$SCRIPT_DIR"
python -c "
from menu_scraper.database import init_db
init_db('$DB_PATH')
print('schema ready')
"

# Add scrape_status / last_scraped_at columns if they don't exist (batch_chunk.py needs these)
python -c "
import sqlite3
c = sqlite3.connect('$DB_PATH')
cols = {r[1] for r in c.execute('PRAGMA table_info(restaurants)')}
if 'scrape_status' not in cols:
    c.execute(\"ALTER TABLE restaurants ADD COLUMN scrape_status TEXT DEFAULT 'pending'\")
if 'last_scraped_at' not in cols:
    c.execute('ALTER TABLE restaurants ADD COLUMN last_scraped_at TEXT')
c.commit()
print('batch columns ready')
"

echo ""
echo "Done. Next steps:"
echo "  export MENU_DB=$DB_PATH"
echo "  export MENU_SEED_JSON=/tmp/restaurants_seed.json"
echo "  # then either seed restaurants via discovery (see references/discovery.md)"
echo "  # or insert your own URLs and run:  python batch_chunk.py 30"
