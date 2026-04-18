"""
Shared junk-name filter for menu items.

Used in two places:
  1. `scrapers/website.py` — applied at scrape-time so navigation/UI/social
     phrases never enter the DB.
  2. `scripts/cleanup_menu_db.py` — applied as a one-shot cleanup pass over
     existing rows in the live DB.

Keeping the rules in one place ensures the cleanup script and the live
scraper always agree on what counts as "junk."
"""

import re
from typing import Optional


# Exact-match (case-insensitive) navigational/UI/social/legal phrases.
# Items whose stripped name matches any of these are dropped entirely.
NAV_JUNK = {
    "menu", "main menu", "open menu", "close menu", "view menu", "view item",
    "view more", "view all", "see more", "see all", "view", "show more",
    "order", "order now", "order online", "order ahead", "shop now", "buy now",
    "click here", "learn more", "read more", "more info", "details",
    "next", "previous", "prev", "back", "back to top", "skip to content",
    "skip to main content", "continue", "submit", "search", "filter", "sort by",
    "select", "select location", "choose location", "find a location",
    "decline all", "decline", "accept all", "accept", "got it", "ok", "close",
    "log in", "login", "sign in", "sign up", "signup", "register", "subscribe",
    "newsletter", "your cart", "cart", "checkout", "view cart",
    "delivery", "pickup", "takeout", "to go", "dine in",
    "reservation", "reservations", "book a table", "book now", "make reservation",
    "follow us", "find us", "contact", "contact us", "about", "about us",
    "home", "our story", "story", "team", "press", "press kit", "media",
    "blog", "news", "events", "gallery", "photos",
    "careers", "jobs", "employment", "join our team", "work with us",
    "gift card", "gift cards", "purchase gift card", "buy gift card",
    "catering", "catering menu", "private events", "events space",
    "locations", "all locations", "find a restaurant", "store locator",
    "faq", "faqs", "help", "support", "customer service",
    "facebook", "instagram", "twitter", "tiktok", "youtube", "linkedin", "yelp",
    "powered by", "all rights reserved", "privacy policy", "terms of service",
    "terms", "terms & conditions", "terms and conditions", "cookie policy",
    "cookies", "accessibility", "do not sell", "ada compliance",
    "item", "item 1", "item 2", "item 3", "item 4", "item 5",
}

# Substring patterns — if these appear anywhere in the item name (case-insensitive),
# the item is dropped.
NAV_JUNK_SUBSTRINGS = [
    "all rights reserved",
    "wine spectator award",
    "restaurant week",
    "click to order",
    "get directions",
    "hours of operation",
]

# A bare year token (2010-2029) in an "item name" is almost always a copyright
# stamp or a promotional date, not a dish name.
YEAR_PATTERN = re.compile(r"\b(20[12]\d)\b")

# HTML residue + markdown noise we strip before storage. Surviving names that
# become empty after stripping are dropped.
HTML_TAG_PATTERN = re.compile(r"<[^>]+>")
HTML_ENTITY_PATTERN = re.compile(r"&[a-zA-Z#0-9]+;")
MARKDOWN_NOISE = re.compile(r"#{2,}|\\u200[bcd]|\u200B")


def is_junk_name(name: Optional[str]) -> bool:
    """Return True if `name` is navigation/UI/social junk, not a real dish."""
    if not name:
        return True
    norm = name.strip().lower()
    if norm in NAV_JUNK:
        return True
    for sub in NAV_JUNK_SUBSTRINGS:
        if sub in norm:
            return True
    if YEAR_PATTERN.search(name):
        return True
    if len(norm) <= 1:
        return True
    return False


def clean_text(s: Optional[str]) -> Optional[str]:
    """Strip HTML tags, entities, markdown noise, and collapse whitespace.

    Returns None for empty / all-whitespace results so callers can treat
    "name became empty after cleaning" as a deletion signal.
    """
    if s is None:
        return None
    s = HTML_TAG_PATTERN.sub("", s)
    s = HTML_ENTITY_PATTERN.sub(" ", s)
    s = MARKDOWN_NOISE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s if s else None
