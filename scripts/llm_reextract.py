#!/usr/bin/env python3
"""
LLM-based menu re-extraction — prototype.

Fetches a restaurant's menu page, sends the rendered text through Claude
Haiku 4.5 with a strict schema + few-shot examples, and compares the
output against what's currently in SQLite. Optional `--apply` replaces
the restaurant's menu_items rows with the LLM output.

Built to answer: "is LLM extraction materially better than the regex
scraper at filtering section headers and description prose out of the
name column?" — without committing to rewriting the whole pipeline.

Usage:
    export ANTHROPIC_API_KEY=sk-ant-...
    python llm_reextract.py --restaurant-id 2253           # Din Tai Fung
    python llm_reextract.py --url https://dintaifungusa.com/
    python llm_reextract.py --restaurant-id 2253 --apply   # write to DB

Output:
    - Before/after sample (first N items from DB vs. LLM)
    - Total counts and dropped-noise estimate
    - Optional JSON dump of the LLM output to a file for inspection

Cost at Haiku 4.5 pricing (~$1/MTok in, ~$5/MTok out, rough): a typical
restaurant page is 10–30k input tokens + ~1–2k output, so roughly
$0.02–$0.05 per restaurant. Cheap enough to run the whole DB if the
quality checks out.
"""

import argparse
import asyncio
import json
import logging
import os
import sqlite3
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("llm_reextract")

_HERE = Path(__file__).resolve().parent
DEFAULT_DB = _HERE.parent / "bay_area_menus.db"

# Haiku 4.5 — good enough for extraction, ~7x cheaper than Sonnet.
# If you want to compare, swap to "claude-sonnet-4-6" and re-run.
LLM_MODEL = "claude-haiku-4-5-20251001"

# How much rendered page text we send. Most restaurant menu pages come in
# well under this; we truncate at the end rather than the middle so
# section-header noise at the top of the page is preserved (the LLM needs
# to see it to correctly categorize dishes).
MAX_TEXT_CHARS = 80_000

# Cap on items we expect per page. Well-structured menus rarely exceed
# this — if the LLM returns more, something is almost certainly wrong.
MAX_ITEMS = 250

# Output-token budget. A packed menu with descriptions runs ~80 tokens
# per item (name + desc + category), so 16K handles ~200 items
# comfortably. Haiku 4.5 supports up to 64K — if you hit this cap on a
# huge menu, pass --max-output 32768 or higher.
DEFAULT_MAX_OUTPUT = 16384


@dataclass
class LLMItem:
    """What we want back from the LLM — aligned with menu_items columns."""
    name: str
    description: Optional[str] = None
    price: Optional[float] = None
    category: Optional[str] = None

    def to_db_row(self, restaurant_id: int, category_id: Optional[int]) -> dict:
        return {
            "restaurant_id": restaurant_id,
            "category_id": category_id,
            "name": self.name,
            "description": self.description,
            "price": self.price,
            "price_text": f"${self.price:.2f}" if self.price else None,
            "image_url": None,
            "dietary_tags": [],
            "is_available": 1,
        }


# The prompt is the whole game here. Three things matter:
#   1. Tell the model what a "dish" is and is NOT (section headers,
#      descriptions, marketing copy).
#   2. Show it the exact JSON shape we want — no markdown fences.
#   3. Give it a few-shot showing realistic noise it should filter out.
#
# We use a system prompt so the instruction-following is stronger, and
# put the page text in the user message.
SYSTEM_PROMPT = """You extract structured menu data from restaurant web pages.

Your job is to identify *orderable menu items* — the things a diner would actually
order. You must EXCLUDE:
  - Section headers (e.g., "WONTONS", "Appetizers", "From the Wok", "Signature Cocktails")
  - Descriptive prose that belongs with a dish, not as its own item (e.g., "A light
    and delicious steamed beef broth with tender slices of beef short ribs.")
  - Marketing copy, navigation text, delivery/ordering CTAs, footer content,
    location names, hours, gift card promotions.
  - Duplicates (if the same dish appears twice, keep one).

Each item you return must have a concise, diner-recognizable `name` (typically
1–6 words, at most ~50 characters). The description — if present — goes in
the `description` field, NOT the name. Keep descriptions tight — ideally
under ~140 characters. Summarize long marketing copy rather than quoting it
verbatim; we only need enough for a diner to recognize what the dish is.

Preserve the original capitalization of dish names as they appear on the menu
(don't aggressively title-case if the menu uses sentence case).

If a price is ambiguous (range, multiple sizes), return the lowest numeric price
and note the rest in the description. Skip items that have no price AND no
discernible description — they're usually section headers.

Return ONLY a JSON object with this exact shape, no commentary, no code fences:

{
  "items": [
    {
      "name": "Xiao Long Bao",
      "description": "Steamed pork soup dumplings",
      "price": 13.50,
      "category": "Dumplings"
    }
  ]
}

If the page has no extractable menu items (e.g., it's a homepage or landing
page), return {"items": []}."""


FEW_SHOT_USER = """Example page excerpt:

DUMPLINGS

Xiao Long Bao
Our signature soup dumplings filled with Kurobuta pork.
$13.50

Shrimp & Pork Wontons
$11.75

GREENS

String Beans with Garlic
$10.50

Signature Cocktails

Pear Lychee Martini - 2oz
Grey Goose La Poire Vodka, St-Germain, fresh lemon juice, and lychee fruit.
$16"""

FEW_SHOT_ASSISTANT = """{
  "items": [
    {
      "name": "Xiao Long Bao",
      "description": "Our signature soup dumplings filled with Kurobuta pork.",
      "price": 13.50,
      "category": "Dumplings"
    },
    {
      "name": "Shrimp & Pork Wontons",
      "description": null,
      "price": 11.75,
      "category": "Dumplings"
    },
    {
      "name": "String Beans with Garlic",
      "description": null,
      "price": 10.50,
      "category": "Greens"
    },
    {
      "name": "Pear Lychee Martini - 2oz",
      "description": "Grey Goose La Poire Vodka, St-Germain, fresh lemon juice, and lychee fruit.",
      "price": 16.00,
      "category": "Signature Cocktails"
    }
  ]
}"""


async def fetch_page_text(url: str, timeout_s: int = 30) -> tuple[str, str]:
    """Return (page_title, rendered_body_text). Uses Playwright because most
    modern restaurant sites are JS-rendered (Din Tai Fung included)."""
    from playwright.async_api import async_playwright

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        try:
            page = await browser.new_page(
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                )
            )
            await page.goto(url, wait_until="networkidle", timeout=timeout_s * 1000)
            await page.wait_for_timeout(2500)  # let JS-rendered menus settle

            # Try to navigate to a dedicated /menu page if we're on the homepage.
            menu_link = await page.evaluate("""
                () => {
                    const links = Array.from(document.querySelectorAll('a'));
                    const menuLink = links.find(a => {
                        const text = (a.textContent || '').toLowerCase().trim();
                        const href = (a.href || '').toLowerCase();
                        return (text === 'menu' || text === 'our menu'
                                || text === 'food menu' || text === 'view menu'
                                || href.endsWith('/menu') || href.includes('/menu/'));
                    });
                    return menuLink ? menuLink.href : null;
                }
            """)
            if menu_link and menu_link != page.url:
                try:
                    await page.goto(menu_link, wait_until="networkidle",
                                    timeout=timeout_s * 1000)
                    await page.wait_for_timeout(1500)
                except Exception as e:
                    logger.debug("menu-link navigation failed: %s", e)

            title = await page.title()
            body = await page.inner_text("body")
            return title, body
        finally:
            await browser.close()


def call_llm(page_title: str, url: str, page_text: str,
             max_output: int = DEFAULT_MAX_OUTPUT) -> list[LLMItem]:
    """Send the page text to Claude Haiku and parse items from the response."""
    try:
        import anthropic
    except ImportError:
        logger.error("anthropic package not installed. Run: pip install -r requirements.txt")
        sys.exit(2)

    if not os.environ.get("ANTHROPIC_API_KEY"):
        logger.error("ANTHROPIC_API_KEY not set")
        sys.exit(2)

    if len(page_text) > MAX_TEXT_CHARS:
        logger.info("truncating page text from %d to %d chars",
                    len(page_text), MAX_TEXT_CHARS)
        page_text = page_text[:MAX_TEXT_CHARS]

    client = anthropic.Anthropic()

    user_message = (
        f"Restaurant page: {url}\n"
        f"Page title: {page_title}\n\n"
        f"Extract the menu items from this rendered page text:\n\n"
        f"---\n{page_text}\n---"
    )

    logger.info("calling %s (%d input chars, max_output=%d)...",
                LLM_MODEL, len(page_text), max_output)

    # Stream so the user sees tokens arrive in real time — Haiku outputs
    # ~100-200 tokens/sec so a 10k-token extraction otherwise looks frozen
    # for a minute. We still collect the full text for JSON parsing below.
    print("── streaming from Haiku ──", file=sys.stderr, flush=True)
    chunks: list[str] = []
    with client.messages.stream(
        model=LLM_MODEL,
        max_tokens=max_output,
        system=SYSTEM_PROMPT,
        messages=[
            {"role": "user", "content": FEW_SHOT_USER},
            {"role": "assistant", "content": FEW_SHOT_ASSISTANT},
            {"role": "user", "content": user_message},
        ],
    ) as stream:
        for text in stream.text_stream:
            chunks.append(text)
            sys.stderr.write(text)
            sys.stderr.flush()
        final_message = stream.get_final_message()
    sys.stderr.write("\n── end stream ──\n")
    sys.stderr.flush()

    usage = final_message.usage
    stop = final_message.stop_reason
    logger.info("usage: input=%d output=%d stop_reason=%s",
                usage.input_tokens, usage.output_tokens, stop)

    # If we hit max_tokens the JSON is almost certainly truncated — fail
    # loudly with actionable advice rather than a confusing parse error.
    if stop == "max_tokens":
        logger.error(
            "LLM hit max_tokens=%d — output truncated mid-response. "
            "Re-run with --max-output %d (or higher).",
            max_output, min(max_output * 2, 64000),
        )
        sys.exit(3)

    raw = "".join(chunks).strip()
    # Defensive: strip code fences if the model adds them despite instructions.
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        logger.error("LLM returned invalid JSON: %s", e)
        logger.error("First 500 chars of response: %s", raw[:500])
        sys.exit(3)

    items_raw = data.get("items") or []
    if not isinstance(items_raw, list):
        logger.error("Expected items to be a list, got %s", type(items_raw))
        sys.exit(3)

    items: list[LLMItem] = []
    for it in items_raw[:MAX_ITEMS]:
        if not isinstance(it, dict):
            continue
        name = (it.get("name") or "").strip()
        if not name:
            continue
        price = it.get("price")
        try:
            price = float(price) if price is not None else None
        except (TypeError, ValueError):
            price = None
        desc = (it.get("description") or "").strip() or None
        category = (it.get("category") or "").strip() or None
        items.append(LLMItem(name=name, description=desc, price=price, category=category))

    return items


def load_restaurant(conn: sqlite3.Connection, restaurant_id: int) -> sqlite3.Row:
    row = conn.execute(
        "SELECT id, name, website, google_place_id FROM restaurants WHERE id = ?",
        (restaurant_id,),
    ).fetchone()
    if not row:
        logger.error("restaurant id=%d not found", restaurant_id)
        sys.exit(2)
    if not row["website"]:
        logger.error("restaurant id=%d has no website URL", restaurant_id)
        sys.exit(2)
    return row


def load_current_items(conn: sqlite3.Connection, restaurant_id: int) -> list[dict]:
    rows = conn.execute(
        "SELECT name, description, price FROM menu_items WHERE restaurant_id = ? ORDER BY id",
        (restaurant_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def print_comparison(old: list[dict], new: list[LLMItem], preview: int = 20) -> None:
    """Side-by-side-ish preview so humans can sanity-check."""
    print()
    print("=" * 72)
    print(f"  OLD (regex scrape): {len(old)} items")
    print(f"  NEW (LLM re-extract): {len(new)} items")
    print("=" * 72)

    def fmt(name: str, price) -> str:
        p = f"${price:.2f}" if price not in (None, 0, 0.0) else "—"
        return f"{p:>8}  {name}"

    print(f"\n── OLD (first {preview}) ──")
    for row in old[:preview]:
        print(fmt(row["name"] or "", row["price"]))
    if len(old) > preview:
        print(f"  ... and {len(old) - preview} more")

    print(f"\n── NEW (first {preview}) ──")
    for item in new[:preview]:
        print(fmt(item.name, item.price))
        if item.description:
            desc = item.description[:80] + ("..." if len(item.description) > 80 else "")
            print(f"{'':>10}└─ {desc}")
    if len(new) > preview:
        print(f"  ... and {len(new) - preview} more")

    # Quick noise heuristic so we can call out the win.
    def looks_noisy(name: str) -> bool:
        n = (name or "").strip()
        if not n:
            return True
        if len(n) > 60:
            return True
        if n == n.upper() and len(n) < 20:
            return True
        low = n.lower()
        for prefix in ("a light ", "a rich ", "our signature ", "our house-made ",
                       "our famous ", "made with ", "served with "):
            if low.startswith(prefix):
                return True
        if n.count(",") >= 3:
            return True
        return False

    old_noisy = sum(1 for r in old if looks_noisy(r["name"]))
    new_noisy = sum(1 for i in new if looks_noisy(i.name))
    print()
    print(f"Noise estimate (same heuristic both sides):")
    print(f"  OLD: {old_noisy}/{len(old)} ({100*old_noisy/max(len(old),1):.0f}%)")
    print(f"  NEW: {new_noisy}/{len(new)} ({100*new_noisy/max(len(new),1):.0f}%)")
    print()


def apply_to_db(conn: sqlite3.Connection, restaurant_id: int,
                new_items: list[LLMItem]) -> None:
    """Replace the restaurant's menu_items and rebuild categories.

    Not atomic — menu_scraper.database.insert_menu_item commits after every
    row, so a true transaction wrapper doesn't help. Acceptable for a
    prototype since a failed re-extract can just be re-run. The whole row
    set is rewritten each call.
    """
    from menu_scraper.database import upsert_category, insert_menu_item

    # Unique categories in insertion order.
    category_order: list[str] = []
    seen_cats: set[str] = set()
    for it in new_items:
        c = it.category
        if c and c not in seen_cats:
            seen_cats.add(c)
            category_order.append(c)

    conn.execute("DELETE FROM menu_items WHERE restaurant_id = ?", (restaurant_id,))
    conn.execute("DELETE FROM menu_categories WHERE restaurant_id = ?", (restaurant_id,))
    conn.commit()

    cat_ids: dict[str, int] = {}
    for i, cat_name in enumerate(category_order):
        cat_ids[cat_name] = upsert_category(conn, restaurant_id, cat_name, sort_order=i)

    for it in new_items:
        insert_menu_item(conn, it.to_db_row(restaurant_id, cat_ids.get(it.category)))

    conn.execute(
        "UPDATE restaurants SET items_found = ? WHERE id = ?",
        (len(new_items), restaurant_id),
    )
    conn.commit()
    logger.info("wrote %d items for restaurant_id=%d", len(new_items), restaurant_id)


async def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--restaurant-id", type=int, help="Restaurant row ID in the scraper DB")
    src.add_argument("--url", type=str, help="Menu page URL to fetch directly")
    ap.add_argument("--db", default=str(DEFAULT_DB), help="SQLite DB path")
    ap.add_argument("--apply", action="store_true",
                    help="Replace the restaurant's menu_items with the LLM output")
    ap.add_argument("--dump-json", type=str, default=None,
                    help="Also write the LLM output to this JSON file")
    ap.add_argument("--preview", type=int, default=20,
                    help="How many items to preview on each side")
    ap.add_argument("--max-output", type=int, default=DEFAULT_MAX_OUTPUT,
                    help="LLM max output tokens (default %(default)s; bump if "
                         "large menus get truncated)")
    args = ap.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        logger.error("DB not found: %s", db_path)
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row

    # Make the menu_scraper package importable when using --apply.
    sys.path.insert(0, str(_HERE))

    if args.restaurant_id is not None:
        row = load_restaurant(conn, args.restaurant_id)
        url = row["website"]
        logger.info("restaurant id=%d name=%r url=%s", row["id"], row["name"], url)
        current = load_current_items(conn, row["id"])
    else:
        url = args.url
        row = None
        current = []

    title, body = await fetch_page_text(url)
    logger.info("fetched page: title=%r (%d chars body)", title, len(body))

    new_items = call_llm(title, url, body, max_output=args.max_output)
    logger.info("LLM returned %d items", len(new_items))

    print_comparison(current, new_items, preview=args.preview)

    if args.dump_json:
        out = {
            "url": url,
            "restaurant_name": row["name"] if row else None,
            "items": [asdict(it) for it in new_items],
        }
        Path(args.dump_json).write_text(json.dumps(out, indent=2, ensure_ascii=False))
        logger.info("wrote %s", args.dump_json)

    if args.apply:
        if row is None:
            logger.error("--apply requires --restaurant-id (need a DB row to target)")
            return 2
        apply_to_db(conn, row["id"], new_items)
        print(f"✓ Applied {len(new_items)} items to restaurant_id={row['id']}.")
        print(f"  Next: re-run push_menus_to_storage.py to refresh Cloud Storage.")

    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
