# menu-scraper-data

Persistent state for the Bay Area restaurant menu scraper, accessed by a daily Cowork scheduled task.

## Layout

- `bay_area_menus.db` — SQLite database. Schema in `scripts/menu_scraper/database.py`. Tables: `restaurants`, `menu_items`, `menu_categories`, `scrape_logs`. The `restaurants` table also has `scrape_status` and `last_scraped_at` columns added by the batch runner.
- `scripts/` — the scraper package. Self-contained so the scheduled task can `git clone` and run without external dependencies beyond `pip install`.
- `restaurants_seed.json` (optional) — the discovered URL list, used by `seed_from_osm.py` to enqueue new restaurants.

## Daily run flow

The `menu-scraper-daily` Cowork scheduled task does:

1. `git clone` this repo into the sandbox using a PAT
2. `pip install -r scripts/requirements.txt --break-system-packages` and `python -m playwright install chromium`
3. Set `MENU_DB=$PWD/bay_area_menus.db`, `cd scripts`
4. (Sundays only) Run OSM Overpass discovery and `seed_from_osm.py` to enqueue new restaurants
5. Re-enqueue the 200 stalest successful rows (set `scrape_status='pending'`)
6. Loop `python batch_chunk.py 30` until the queue drains
7. Push per-restaurant menus to Firebase Storage so the ForkBook iOS app can read them:
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="$PWD/../firebase-admin-key.json"
   python push_menus_to_storage.py
   ```
   Uploads `menus/{placeId}.json` for every restaurant with `place_match_status = 'matched'`. Idempotent (MD5-skips unchanged blobs), so safe to re-run daily. Read by `MenuDataService.swift` in the iOS app.
8. `git add bay_area_menus.db && git commit && git push`

## Stats (last update committed in git history)

Check `git log --stat bay_area_menus.db` for size deltas over time. To peek at counts:

```bash
sqlite3 bay_area_menus.db "
  SELECT
    COUNT(*) AS total,
    SUM(scrape_status='success') AS success,
    SUM(scrape_status='pending') AS pending,
    SUM(scrape_status='failed') AS failed,
    MAX(last_scraped_at) AS latest_scrape
  FROM restaurants;
"
```

## Manual operation

If you want to refresh the DB outside the scheduled task:

```bash
git clone https://github.com/shreyasnan/menu-scraper-data.git
cd menu-scraper-data
pip install -r scripts/requirements.txt --break-system-packages
python -m playwright install chromium
export MENU_DB=$PWD/bay_area_menus.db
cd scripts
while python batch_chunk.py 30; do :; done
cd ..
git add bay_area_menus.db
git commit -m "manual refresh"
git push
```
