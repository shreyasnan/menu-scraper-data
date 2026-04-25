#!/usr/bin/env python3
"""
Chunked batch scraper — processes N restaurants per invocation so it can
be called repeatedly within a shell timeout. Tracks state in SQLite.
"""

import asyncio
import json
import logging
import os
import re
import sqlite3
import sys
import time
from typing import Optional

from playwright.async_api import async_playwright

from menu_scraper.jsonld import parse_menu_from_ld_blocks

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
logger = logging.getLogger(__name__)

# Configurable via env vars. Defaults are safe sandbox paths.
DB_PATH = os.environ.get("MENU_DB", "/tmp/menus.db")
JSON_PATH = os.environ.get("MENU_SEED_JSON", "/tmp/restaurants_seed.json")
CONCURRENT = int(os.environ.get("MENU_CONCURRENT", "8"))
PAGE_TIMEOUT = 12_000
SETTLE_MS = 1_500

PRICE_RE = re.compile(r'[\$£€]\s*\d+(?:[.,]\d{1,2})?')

SKIP_PHRASES = {
    "skip","cookie","navigation","order now","order online","find a location",
    "find your location","franchise","our story","gift card","join","connect",
    "download","follow us","sign in","sign up","log in","privacy","terms",
    "copyright","all rights","careers","contact us","about us","subscribe",
    "newsletter","blog","press","investor","product images","items may vary",
    "check the nutritional","powered by","reserv","book a table",
}

CATEGORY_KEYWORDS = {
    "appetizer","starter","entree","entre","main","pizza","pasta","salad",
    "soup","sandwich","burger","taco","wrap","bowl","side","dessert","drink",
    "beverage","breakfast","lunch","dinner","brunch","special","seafood",
    "chicken","beef","pork","vegetarian","kids","combo","platter","shareables",
    "wings","sushi","roll","nigiri","sashimi","ramen","noodle","curry",
    "tandoori","biryani","dim sum","dumpling","burrito","quesadilla","nacho",
    "enchilada","small plate","large plate","from the grill","from the oven",
    "starters","mains","entrees","sides","sweets",
}


def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS restaurants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            city TEXT, cuisine TEXT, website TEXT, phone TEXT,
            latitude REAL, longitude REAL, osm_id TEXT UNIQUE,
            scrape_status TEXT DEFAULT 'pending',
            items_found INTEGER DEFAULT 0,
            error_message TEXT, scrape_duration REAL, scraped_at TEXT
        );
        CREATE TABLE IF NOT EXISTS menu_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER NOT NULL,
            category TEXT, name TEXT NOT NULL, description TEXT,
            price REAL, price_text TEXT, dietary_tags TEXT,
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id)
        );
        CREATE INDEX IF NOT EXISTS idx_items_rest ON menu_items(restaurant_id);
        CREATE INDEX IF NOT EXISTS idx_items_name ON menu_items(name);
    """)

    # Migrations: idempotent column adds for fields owned by other
    # scripts (resolve_places.py, push_menus_to_storage.py,
    # backfill_firestore_place_ids.py). Keeping them here means the
    # daily task always boots into a known-good schema even if the
    # repo gets rolled back to a commit that predates these columns.
    existing = {row[1] for row in conn.execute("PRAGMA table_info(restaurants)").fetchall()}
    migrations = [
        ("address",                "ALTER TABLE restaurants ADD COLUMN address TEXT"),
        ("google_place_id",        "ALTER TABLE restaurants ADD COLUMN google_place_id TEXT"),
        ("place_matched_name",     "ALTER TABLE restaurants ADD COLUMN place_matched_name TEXT"),
        ("place_match_confidence", "ALTER TABLE restaurants ADD COLUMN place_match_confidence REAL"),
        ("place_match_status",     "ALTER TABLE restaurants ADD COLUMN place_match_status TEXT"),
        ("place_resolved_at",      "ALTER TABLE restaurants ADD COLUMN place_resolved_at TEXT"),
        ("last_scraped_at",        "ALTER TABLE restaurants ADD COLUMN last_scraped_at TEXT"),
    ]
    for col, sql in migrations:
        if col not in existing:
            logger.info(f"Schema migration: adding restaurants.{col}")
            conn.execute(sql)
    conn.commit()
    return conn


def load_json_if_needed(conn):
    count = conn.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0]
    if count > 0:
        return count
    with open(JSON_PATH) as f:
        data = json.load(f)
    if isinstance(data, dict):
        data = data.get("restaurants", [])
    for i, r in enumerate(data):
        name = r.get("name")
        website = r.get("website", "")
        if not name or not website:
            continue
        if not website.startswith("http"):
            website = "https://" + website
        try:
            conn.execute("""
                INSERT OR IGNORE INTO restaurants (name, city, cuisine, website, phone, osm_id)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (name, r.get("city",""), r.get("cuisine",""), website, r.get("phone",""), f"json_{i}"))
        except Exception:
            pass
    conn.commit()
    return conn.execute("SELECT COUNT(*) FROM restaurants").fetchone()[0]


def parse_price(text):
    if not text:
        return None
    m = re.search(r'[\$£€]?\s*(\d+(?:[.,]\d{1,2})?)', text.strip())
    if m:
        try: return float(m.group(1).replace(',','.'))
        except: pass
    return None


def extract_items_from_text(text):
    lines = [l.strip() for l in text.split("\n") if l.strip() and len(l.strip()) > 2]
    items = []
    current_category = None
    for i, line in enumerate(lines):
        ll = line.lower()
        if any(p in ll for p in SKIP_PHRASES): continue
        is_cat = False
        if line.isupper() and 3 < len(line) < 50:
            if any(kw in ll for kw in CATEGORY_KEYWORDS) or len(line.split()) >= 2:
                is_cat = True
        elif len(line) < 40 and any(kw in ll for kw in CATEGORY_KEYWORDS):
            is_cat = True
        if is_cat:
            current_category = line.title() if line.isupper() else line
            continue
        if 3 < len(line) < 80 and current_category and not line.startswith("http"):
            desc = None
            if i+1 < len(lines):
                nl = lines[i+1]
                if len(nl) > 25 and not nl.isupper() and not any(p in nl.lower() for p in SKIP_PHRASES):
                    desc = nl[:200]
            pm = PRICE_RE.search(line)
            pt = pm.group() if pm else None
            name = line[:pm.start()].strip().rstrip("-–—·.") if pm else line
            if name and 2 < len(name) < 80:
                items.append({"category":current_category,"name":name,"description":desc,
                              "price":parse_price(pt),"price_text":pt})
    return items[:150]


async def try_jsonld(page):
    """Grab every JSON-LD <script> body from the current DOM and hand
    them to the Python parser. Returns {items, follow_url}. Items is
    always a list (possibly empty); follow_url is a string when the
    page's Restaurant.hasMenu points to a separate menu page we should
    navigate to and re-extract from."""
    try:
        ld_strings = await page.evaluate("""() => {
            const scripts = document.querySelectorAll('script[type="application/ld+json"]');
            return Array.from(scripts).map(s => s.textContent || '');
        }""")
    except Exception:
        return {"items": [], "follow_url": None}

    if not ld_strings:
        return {"items": [], "follow_url": None}

    try:
        return parse_menu_from_ld_blocks(ld_strings)
    except Exception:
        return {"items": [], "follow_url": None}


async def extract_structured(page):
    # JSON-LD (schema.org Menu / Restaurant.hasMenu → inline items)
    ld = await try_jsonld(page)
    if ld["items"]:
        return ld["items"]

    # CSS patterns
    try:
        css = await page.evaluate("""() => {
            const r=[];
            const ss=['.menu-section','[class*="menu-section"]','[class*="menuSection"]','[class*="menu-category"]'];
            let secs=[];
            for(const s of ss){secs=document.querySelectorAll(s);if(secs.length)break;}
            for(const sec of secs){
                const h=sec.querySelector('h1,h2,h3,h4,[class*="title"]');
                const cat=h?h.textContent.trim():'';
                const is=['.menu-item','[class*="menu-item"]','[class*="menuItem"]'];
                let items=[];
                for(const s of is){items=sec.querySelectorAll(s);if(items.length)break;}
                for(const el of items){
                    const n=el.querySelector('[class*="name"],[class*="title"],h3,h4,strong');
                    const d=el.querySelector('[class*="desc"],p');
                    const p=el.querySelector('[class*="price"]');
                    if(n)r.push({name:n.textContent.trim(),description:d?d.textContent.trim():'',price_text:p?p.textContent.trim():'',category:cat,price:null});
                }
            }
            return r;
        }""")
        if css:
            for i in css:
                if i.get("price_text"): i["price"] = parse_price(i["price_text"])
            return [i for i in css if i.get("name")]
    except: pass

    # Price patterns in DOM
    try:
        pp = await page.evaluate("""() => {
            const re=/[\$£€]\s*\d+(?:[.,]\d{1,2})?/;
            const r=[],seen=new Set();
            const w=document.createTreeWalker(document.body,NodeFilter.SHOW_ELEMENT);
            let n;
            while(n=w.nextNode()){
                const t=n.textContent.trim();
                if(!re.test(t)||t.length>500)continue;
                let c=n;
                for(let i=0;i<5;i++){if(!c.parentElement)break;c=c.parentElement;const ct=c.textContent.trim();if(ct.length>30&&ct.length<500)break;}
                const ct=c.textContent.trim();
                if(seen.has(ct)||ct.length<5)continue;
                seen.add(ct);
                const m=ct.match(re);
                if(m){const idx=ct.indexOf(m[0]);const name=ct.slice(0,idx).trim().split('\\n')[0].trim();
                    if(name.length>1&&name.length<200)r.push({name,description:'',price_text:m[0],category:'',price:null});}
            }
            return r.slice(0,200);
        }""")
        if pp:
            for i in pp:
                if i.get("price_text"): i["price"] = parse_price(i["price_text"])
            return [i for i in pp if i.get("name")]
    except: pass
    return []


async def scrape_one(context, rid, name, website, sem):
    """Scrape one restaurant. Each task opens its own SQLite connection
    for the DB write block at the end — sharing a single connection
    across 8 concurrent async tasks under WAL mode + AUTOINCREMENT
    interleaves writes badly enough to corrupt the btree (rowids out
    of order, page init failures). Per-task connections serialize on
    SQLite's file-level write lock instead, which is correct."""
    async with sem:
        start = time.time()
        items = []
        error = None
        try:
            page = await context.new_page()
            try:
                await page.goto(website, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                await page.wait_for_timeout(SETTLE_MS)
                title = await page.title()
                if any(x in (title or "").lower() for x in ["404","not found","error","access denied","forbidden"]):
                    error = f"Bad page: {title[:80]}"
                else:
                    # 1. Peek at JSON-LD on the landing page first — it may
                    #    have inline items OR a hasMenu URL that points us
                    #    directly at the real menu page.
                    ld = await try_jsonld(page)
                    items = ld["items"]
                    menu_url = ld["follow_url"]

                    # 2. If JSON-LD didn't give us a URL, fall back to a
                    #    text-match on anchors (existing heuristic).
                    if not items and not menu_url:
                        menu_url = await page.evaluate("""()=>{
                            const a=Array.from(document.querySelectorAll('a'));
                            const m=a.find(x=>{const t=(x.textContent||'').toLowerCase().trim();const h=(x.href||'').toLowerCase();
                                return(t==='menu'||t==='our menu'||t==='food menu'||t==='food & drink'||t==='view menu'||h.includes('/menu'))&&x.href&&x.href.startsWith('http');});
                            return m?m.href:null;
                        }""")

                    # 3. Navigate to the menu page if different, then
                    #    re-run the full extractor stack there.
                    if (not items) and menu_url and menu_url != page.url:
                        try:
                            await page.goto(menu_url, wait_until="domcontentloaded", timeout=PAGE_TIMEOUT)
                            await page.wait_for_timeout(SETTLE_MS)
                        except: pass
                        items = await extract_structured(page)
                    elif not items:
                        items = await extract_structured(page)

                    # 4. Final fallback: pure text-heuristic on body.
                    if not items:
                        try:
                            text = await page.inner_text("body")
                            items = extract_items_from_text(text)
                        except: pass
            except Exception as e:
                error = str(e)[:200]
            finally:
                await page.close()
        except Exception as e:
            error = str(e)[:200]

        dur = time.time() - start
        status = "success" if items else ("failed" if error else "no_items")

        # Per-task connection. Closes after commit. busy_timeout makes
        # us wait up to 10s for the write lock instead of erroring out
        # under contention.
        local_conn = sqlite3.connect(DB_PATH)
        try:
            local_conn.execute("PRAGMA busy_timeout=10000")
            local_conn.execute("BEGIN IMMEDIATE")
            local_conn.execute(
                "UPDATE restaurants SET scrape_status=?,items_found=?,error_message=?,scrape_duration=?,scraped_at=datetime('now') WHERE id=?",
                (status, len(items), error, round(dur, 2), rid),
            )
            # Clear prior items for this restaurant — a re-scrape
            # should replace, not accumulate. (Earlier bug: repeated
            # scrapes doubled up menu_items rows.)
            local_conn.execute("DELETE FROM menu_items WHERE restaurant_id=?", (rid,))
            if items:
                local_conn.executemany(
                    "INSERT INTO menu_items(restaurant_id,category,name,description,price,price_text,dietary_tags) VALUES (?,?,?,?,?,?,?)",
                    [(rid, i.get("category", ""), i["name"], i.get("description", ""), i.get("price"), i.get("price_text", ""), "") for i in items],
                )
            local_conn.commit()
        except Exception as e:
            logger.error(f"DB err {name}: {e}")
            try:
                local_conn.rollback()
            except Exception:
                pass
        finally:
            local_conn.close()
        return status, len(items)


async def run_chunk(chunk_size=40):
    conn = init_db()
    total = load_json_if_needed(conn)

    rows = conn.execute("SELECT id,name,website FROM restaurants WHERE scrape_status='pending' ORDER BY id LIMIT ?",(chunk_size,)).fetchall()
    if not rows:
        # Print summary
        stats = conn.execute("SELECT scrape_status,COUNT(*),COALESCE(SUM(items_found),0) FROM restaurants GROUP BY scrape_status").fetchall()
        total_items = conn.execute("SELECT COUNT(*) FROM menu_items").fetchone()[0]
        print(f"ALL DONE. No pending restaurants.")
        for s in stats:
            print(f"  {s[0]}: {s[1]} restaurants, {s[2]} items")
        print(f"  Total menu items in DB: {total_items}")
        conn.close()
        return 0

    pending = conn.execute("SELECT COUNT(*) FROM restaurants WHERE scrape_status='pending'").fetchone()[0]
    done = total - pending
    logger.info(f"Processing {len(rows)} restaurants (done: {done}, pending: {pending}, total: {total})")

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=True)
    ctx = await browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36",
        viewport={"width":1440,"height":900},
    )
    sem = asyncio.Semaphore(CONCURRENT)

    # Close the chunk-level connection BEFORE dispatching scrape tasks.
    # The chunk-level connection was only needed for the row selection
    # above; each scrape_one task opens its own connection for writes
    # so concurrent tasks can't corrupt each other under WAL.
    conn.close()

    ok = 0; fail = 0; item_count = 0
    tasks = [scrape_one(ctx, r[0], r[1], r[2], sem) for r in rows]
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        if isinstance(r, Exception):
            fail += 1; continue
        if r[0] == "success":
            ok += 1; item_count += r[1]
        else:
            fail += 1

    await ctx.close()
    await browser.close()

    # Re-open a fresh connection for the post-scrape stats — the
    # chunk-level connection was closed before dispatch to avoid
    # collision with per-task writers.
    stats_conn = sqlite3.connect(DB_PATH)
    total_items = stats_conn.execute("SELECT COUNT(*) FROM menu_items").fetchone()[0]
    success_count = stats_conn.execute("SELECT COUNT(*) FROM restaurants WHERE scrape_status='success'").fetchone()[0]
    remaining = stats_conn.execute("SELECT COUNT(*) FROM restaurants WHERE scrape_status='pending'").fetchone()[0]
    stats_conn.close()

    logger.info(f"Chunk done: {ok} OK, {fail} fail, {item_count} new items")
    logger.info(f"Cumulative: {success_count} successful restaurants, {total_items} total menu items, {remaining} remaining")
    return remaining


if __name__ == "__main__":
    chunk = int(sys.argv[1]) if len(sys.argv) > 1 else 40
    remaining = asyncio.run(run_chunk(chunk))
    sys.exit(0 if remaining == 0 else 1)
