"""
Schema.org JSON-LD menu parser.

Real restaurant sites embed menu data in a variety of shapes. The
parser normalizes them into a common `[{name, description, price,
price_text, category}, ...]` list.

Supported shapes:
    - Top-level array of LD objects
    - Top-level @graph array
    - Restaurant → hasMenu → Menu → hasMenuSection[*] → hasMenuItem[*]
    - Restaurant → hasMenu → "https://.../menu" (URL — returned as a
      `follow_url` hint for the scraper to re-navigate)
    - Menu at top level with hasMenuSection
    - MenuSection at top level with hasMenuItem
    - MenuItem at top level or nested anywhere

Prices:
    - offers.price (string or number)
    - offers.priceCurrency (prepended to price_text)
    - offers as array → first entry
    - price directly on MenuItem (non-standard but seen in the wild)

Everything is best-effort; malformed blocks are ignored without
raising so the scraper can fall through to HTML heuristics.
"""

from __future__ import annotations

import json
import re
from typing import Iterable

# Types whose `name` field we treat as a category label.
_SECTION_TYPES = {"MenuSection"}
# Types we treat as individual items.
_ITEM_TYPES = {"MenuItem"}

_PRICE_RE = re.compile(r"[\$£€]?\s*(\d+(?:[.,]\d{1,2})?)")


def parse_menu_from_ld_blocks(ld_strings: Iterable[str]) -> dict:
    """
    Parse a collection of JSON-LD <script> bodies into menu items.

    Returns:
        {
            "items": [{name, description, price, price_text, category}, ...],
            "follow_url": Optional[str]   # if Restaurant.hasMenu pointed
                                          # to a separate menu page URL,
                                          # the scraper should navigate
                                          # there and retry.
        }
    """
    items: list[dict] = []
    follow_url: str | None = None

    for raw in ld_strings:
        if not raw or not raw.strip():
            continue
        try:
            doc = json.loads(raw)
        except Exception:
            continue

        for node in _iter_nodes(doc):
            t = _type_of(node)
            if not t:
                continue
            if "Restaurant" in t or "FoodEstablishment" in t or "LocalBusiness" in t:
                menu = node.get("hasMenu")
                # URL form — the menu is on a different page.
                if isinstance(menu, str) and menu.startswith("http"):
                    follow_url = follow_url or menu
                elif isinstance(menu, dict):
                    items.extend(_items_from_menu(menu))
                elif isinstance(menu, list):
                    for m in menu:
                        if isinstance(m, str) and m.startswith("http"):
                            follow_url = follow_url or m
                        elif isinstance(m, dict):
                            items.extend(_items_from_menu(m))
            elif "Menu" in t and "MenuSection" not in t and "MenuItem" not in t:
                items.extend(_items_from_menu(node))
            elif t & _SECTION_TYPES:
                items.extend(_items_from_section(node, parent_category=None))
            elif t & _ITEM_TYPES:
                mi = _item_from_menuitem(node, category=None)
                if mi:
                    items.append(mi)

    # Dedupe by (name, category) — some sites duplicate entries across
    # @graph and a top-level Menu node.
    seen: set[tuple[str, str]] = set()
    deduped: list[dict] = []
    for it in items:
        key = (it["name"].strip().lower(), (it.get("category") or "").strip().lower())
        if key in seen:
            continue
        seen.add(key)
        deduped.append(it)

    return {"items": deduped, "follow_url": follow_url}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _iter_nodes(doc) -> Iterable[dict]:
    """Yield every dict-like JSON-LD node inside `doc`, walking
    @graph arrays and plain arrays. Skips non-dict values."""
    stack = [doc]
    while stack:
        cur = stack.pop()
        if isinstance(cur, dict):
            if "@graph" in cur and isinstance(cur["@graph"], list):
                stack.extend(cur["@graph"])
            yield cur
        elif isinstance(cur, list):
            stack.extend(cur)


def _type_of(node: dict) -> set[str]:
    """Return the @type(s) of a node as a set of strings. @type may
    be a string or a list. Empty set if missing."""
    t = node.get("@type")
    if not t:
        return set()
    if isinstance(t, str):
        return {t}
    if isinstance(t, list):
        return {x for x in t if isinstance(x, str)}
    return set()


def _items_from_menu(menu_node: dict) -> list[dict]:
    """Extract items from a `Menu` object. Prefers hasMenuSection when
    present; falls back to hasMenuItem on the Menu itself."""
    out: list[dict] = []
    sections = menu_node.get("hasMenuSection")
    if sections:
        if isinstance(sections, dict):
            sections = [sections]
        for sec in sections:
            if isinstance(sec, dict):
                out.extend(_items_from_section(sec, parent_category=None))
    # Some sites put items directly on the Menu node without sections.
    direct = menu_node.get("hasMenuItem")
    if direct:
        if isinstance(direct, dict):
            direct = [direct]
        for mi in direct:
            if isinstance(mi, dict):
                item = _item_from_menuitem(mi, category=None)
                if item:
                    out.append(item)
    return out


def _items_from_section(section: dict, parent_category: str | None) -> list[dict]:
    """Extract items from a `MenuSection`. Recurses into nested
    sub-sections (hasMenuSection on a section is legal per schema.org)."""
    out: list[dict] = []
    cat = section.get("name") or parent_category or ""
    # Direct items
    direct = section.get("hasMenuItem")
    if direct:
        if isinstance(direct, dict):
            direct = [direct]
        for mi in direct:
            if isinstance(mi, dict):
                item = _item_from_menuitem(mi, category=cat)
                if item:
                    out.append(item)
    # Nested sub-sections
    sub = section.get("hasMenuSection")
    if sub:
        if isinstance(sub, dict):
            sub = [sub]
        for s in sub:
            if isinstance(s, dict):
                out.extend(_items_from_section(s, parent_category=cat))
    return out


def _item_from_menuitem(mi: dict, category: str | None) -> dict | None:
    """Build a normalized item dict from a MenuItem node, or None
    when the node lacks a usable name."""
    name = mi.get("name")
    if not name or not isinstance(name, str):
        return None
    name = name.strip()
    if not name:
        return None

    desc = mi.get("description") or ""
    if isinstance(desc, list):
        desc = " ".join(str(d) for d in desc if d)
    desc = str(desc).strip()

    price, price_text = _extract_price(mi)

    return {
        "name": name[:120],
        "description": desc[:500],
        "price": price,
        "price_text": price_text,
        "category": (category or "").strip()[:60],
    }


def _extract_price(mi: dict) -> tuple[float | None, str]:
    """Pull price from any of the common shapes: offers.price,
    offers[*].price, mi.price. Returns (float_or_none, display_text)."""
    offers = mi.get("offers")
    candidates: list = []
    if isinstance(offers, dict):
        candidates.append(offers)
    elif isinstance(offers, list):
        candidates.extend(o for o in offers if isinstance(o, dict))

    for off in candidates:
        p = off.get("price")
        if p in (None, ""):
            continue
        num = _coerce_price(p)
        if num is None:
            continue
        cur = off.get("priceCurrency") or ""
        symbol = {"USD": "$", "GBP": "£", "EUR": "€"}.get(cur, "$")
        return num, f"{symbol}{num:.2f}" if num == int(num) is False else f"{symbol}{num:g}"

    # Bare price on MenuItem (non-standard but seen on e.g. Toast)
    p = mi.get("price")
    if p not in (None, ""):
        num = _coerce_price(p)
        if num is not None:
            return num, f"${num:g}"

    return None, ""


def _coerce_price(v) -> float | None:
    """Convert a price value (str, int, float) to float. Returns
    None for unparseable values."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = _PRICE_RE.search(v.strip())
        if m:
            try:
                return float(m.group(1).replace(",", "."))
            except ValueError:
                return None
    return None
