"""
Clean up dish names produced by the heuristic HTML scraper before
they reach Firebase Storage.

Two failure modes we see on real menus:

1. **Fused name + description.** The scraper concatenates the styled
   dish name with its description without preserving the boundary,
   producing strings like:
       "shizen shiitakestuffed with shredded tofu, matcha salt"
       "shishitoJapanese sweet and spicy peppers, tare"
       "philadelphia rollsmoked tofu, avocado, vegan cream cheese"

2. **Lowercased dish names.** Many restaurant menus use CSS
   `text-transform: lowercase` for stylized rendering. The scraper
   reads `textContent` (rendered text) so we lose proper casing.

This module fixes both at PUSH time — no re-scrape needed. The
underlying scraper bug should also be fixed eventually, but this
gets the iOS app readable data immediately.

Conservative by design — when the heuristic isn't confident, it
leaves the input alone rather than mangling it.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Splitting heuristics
# ---------------------------------------------------------------------------

# lowercase|digit|punct → Capital + lowercase boundary. Same logic as
# split_fused_dish_rows.py — catches "shishitoJapanese" → "shishito" |
# "Japanese ...". Lookbehind requires a real letter so we don't trigger
# inside a normal multi-word name.
_LOWER_TO_CAP = re.compile(r"(?<=[a-z0-9\)\]\.])(?=[A-Z][a-z])")

# Verb-with-preposition starters that almost always begin a menu
# description ("stuffed with…", "served on…", "drizzled in…"). Lookbehind
# requires lowercase directly before the verb — so the verb is glued to
# the prior word with no space, which is the bug we're fixing. Properly
# spaced phrases like "Beef Served with Pickles" won't trigger.
_VERB_GLUE = re.compile(
    r"(?<=[a-z])"
    r"(?="
    r"stuffed|served|topped|prepared|made|marinated|cooked|fried|grilled|"
    r"baked|roasted|saut[eé]ed|seared|smoked|dressed|wrapped|filled|tossed|"
    r"drizzled|glazed|braised|poached|steamed|charred|coated|crusted|"
    r"layered|finished|garnished|infused|seasoned|simmered"
    r")",
)


def split_fused_name(name: str) -> tuple[str, str | None]:
    """If `name` looks fused, return (clean_name, extracted_desc).
    Otherwise return (name, None). Conservative — short names without
    a detectable boundary stay as-is, but the post-match length checks
    (head >= 4 chars, tail >= 12 chars) prevent over-aggressive splits
    on legitimately short dish names."""
    s = (name or "").strip()
    # Was 30 — too high; missed cases like "BeersNorth Coast Ale" (19)
    # and "salmonseasoned with miso" (22). Post-match length checks
    # below are the real safety net.
    if len(s) < 16:
        return s, None

    # Strategy 1: lowercase → Capital boundary.
    # Don't use a `pos` start — the post-match length check enforces
    # head >= 4 chars, and a `pos=8` start was hiding valid early
    # matches (e.g. "salmonseasoned" where head="salmon" length 6).
    m = _LOWER_TO_CAP.search(s)
    if m:
        head, tail = s[: m.start()].rstrip(), s[m.start():].strip()
        if 4 <= len(head) <= 80 and len(tail) >= 12:
            return head, tail

    # Strategy 2: verb+preposition glued to previous word.
    m = _VERB_GLUE.search(s)
    if m:
        head, tail = s[: m.start()].rstrip(), s[m.start():].strip()
        if 4 <= len(head) <= 80 and len(tail) >= 12:
            return head, tail

    return s, None


# ---------------------------------------------------------------------------
# Title-casing
# ---------------------------------------------------------------------------

# Stop-words that stay lowercase mid-name (NOT at the start).
_LOWER_STOPS = {
    "a", "an", "and", "as", "at", "but", "by", "en", "for", "if", "in",
    "of", "on", "or", "the", "to", "via", "vs", "with",
}


def title_case_dish(name: str) -> str:
    """Title-case a dish name when it's mostly lowercase. Preserves
    common stop-words mid-name and short symbols. Won't change names
    that already have meaningful capitalization."""
    if not name:
        return name
    letters = [c for c in name if c.isalpha()]
    if not letters:
        return name
    lower_pct = sum(1 for c in letters if c.islower()) / len(letters)
    # Only fire when overwhelmingly lowercase. Mixed case is left alone
    # so we don't fight a menu that's already cased deliberately.
    if lower_pct < 0.85:
        return name

    out: list[str] = []
    for i, w in enumerate(name.split()):
        if not w:
            continue
        if w in {"&", "+", "/"}:
            out.append(w)
            continue
        # Preserve words whose alpha letters are all uppercase
        # (acronyms, dietary tags like "(D)", "(GF)", "BBQ", "DJ").
        alpha = "".join(c for c in w if c.isalpha())
        if alpha and alpha == alpha.upper():
            out.append(w)
            continue
        wl = w.lower()
        if i > 0 and wl in _LOWER_STOPS:
            out.append(wl)
        else:
            # Title-case the leading alpha char; lowercase the rest.
            # Punctuation passes through unchanged because we only
            # touch the first alpha position.
            out.append(w[:1].upper() + w[1:].lower())
    return " ".join(out)


# ---------------------------------------------------------------------------
# ALL-CAPS prefix stripping
# ---------------------------------------------------------------------------
#
# Aurum's menu in the wild produces things like:
#   "SALLMON-ELLA-FREE - tandoori salmon (D)"
#   "SPICE BOY - Goat Biryani (D)"
#   "BASSES - Sea Bass Curry"
# where the all-caps part is a dish-section pun/codeword and the real
# dish name lives after the dash. Strip the all-caps lead when we see
# that pattern.

_CAPS_PREFIX = re.compile(
    # Head: at least one all-caps token, optionally including digits,
    # spaces, hyphens, apostrophes, periods (for "I'M", "G.O.A.T", etc.)
    # Separator: optional leading whitespace, dash, required trailing
    # whitespace — so it matches both "SPICE BOY - Goat" and "SHEESH- lamb".
    r"^([A-Z][A-Z0-9 \-\.\']+?)\s*-\s+(.+)$"
)


def strip_caps_prefix(name: str) -> str:
    """If `name` is `'ALL CAPS WORDS - real dish'`, return just the
    real-dish part. Otherwise return name unchanged."""
    m = _CAPS_PREFIX.match(name.strip())
    if not m:
        return name
    head, tail = m.group(1).strip(), m.group(2).strip()
    # Sanity: the head must be ALL-CAPS (its alpha chars all uppercase)
    # and the tail must be at least 4 chars and contain a lowercase
    # letter (so we don't strip "BBQ - 12 OZ" type cases).
    head_alpha = "".join(c for c in head if c.isalpha())
    if not head_alpha or head_alpha != head_alpha.upper():
        return name
    if len(tail) < 4 or not any(c.islower() for c in tail):
        return name
    return tail


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def normalize_dish(
    name: str,
    description: str | None,
    price: float | None,
) -> dict:
    """Take a scraped (name, description, price) and return a cleaned
    `{n, d?, p?}` payload ready to ship to Storage.

    - Strips ALL-CAPS section-name prefixes ("SPICE BOY - Goat Biryani"
      → "Goat Biryani").
    - Splits a fused name when detected.
    - Title-cases mostly-lowercase names.
    - Preserves an existing description; falls back to the extracted
      tail from a split if no description was given.
    """
    name = strip_caps_prefix(name)
    head, tail = split_fused_name(name)
    head = title_case_dish(head)

    desc = (description or "").strip()
    if not desc and tail:
        desc = tail
    # If the original description looks like the tail we just extracted
    # (rare — usually the original is None for fused rows), don't
    # duplicate it.
    if desc and tail and desc.lower() == tail.lower():
        desc = tail

    item: dict = {"n": head}
    if desc:
        item["d"] = desc
    if price is not None and price > 0:
        item["p"] = float(price)
    return item
