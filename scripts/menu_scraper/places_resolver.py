"""
Resolve scraped restaurants to Google Place IDs.

This is the critical join between the menu DB (identified by website/osm_id)
and ForkBook's world (identified by Google Place ID). Without it, the iOS
app has no way to look up menus for a place the user just picked.

Uses Places API "Find Place from Text" — $17 per 1k calls, and for our
639-useful-restaurant MVP the full run is under $15.

Usage (as a library):
    from menu_scraper.places_resolver import PlacesResolver
    resolver = PlacesResolver(api_key=os.environ["GOOGLE_PLACES_API_KEY"])
    match = resolver.resolve(name="Tikka Masala House", city="Palo Alto")
    # → {"place_id": "ChIJ...", "matched_name": "Tikka Masala", "confidence": 0.93}
    # or None if no plausible match

Confidence threshold:
    >= 0.85  → accept as match
    >= 0.60  → store but flag as "needs_review"
    <  0.60  → no match
"""

import re
import logging
import time
import unicodedata
from difflib import SequenceMatcher
from typing import Optional, Any
from urllib.parse import urlencode
from urllib.request import urlopen, Request
import json

logger = logging.getLogger(__name__)

FIND_PLACE_URL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

# Filler words stripped before name comparison. A restaurant's marketing name
# often wraps the real name in these ("The", "Restaurant", "Kitchen & Bar",
# "- Indian Restaurant and Catering", "Coffee Co."), which hurts fuzzy matching
# if left in.
_STOPWORDS = {
    # Generic framing
    "the", "a", "an", "and", "&",
    # Establishment-type words
    "restaurant", "restaurants", "cafe", "café", "coffee", "bar", "pub",
    "tavern", "lounge", "kitchen", "grill", "eatery", "bistro", "diner",
    "place", "house", "bakery", "deli", "company", "co",
    # Descriptor words Google appends inconsistently — "Varam Indian Cuisine"
    # on our side vs just "Varam" on Google, "Reveille Coffee" vs "Reveille
    # Coffee Co." — stripping these avoids false-negative rejections below
    # the review threshold.
    "cuisine", "food", "foods", "eats", "dining", "fine",
    "cucina", "trattoria", "osteria",
    # Cuisine-type suffixes (people drop these casually — "Pancho Villa Taqueria"
    # vs "Pancho Villa" on Google are the same place)
    "taqueria", "pizzeria", "sushi", "pizza", "tacos", "ramen", "noodle",
    "noodles", "bbq", "barbecue", "steakhouse",
    # Cuisine adjectives — Google often appends these ("Benihana Japanese
    # Steakhouse", "Tabla Indian Restaurant") and they hurt name matching
    # when the user stored only the short name.
    "indian", "japanese", "italian", "mexican", "chinese", "thai",
    "vietnamese", "korean", "french", "mediterranean", "american",
    "greek", "turkish", "spanish", "ethiopian", "peruvian",
    # Catering / marketing suffixes
    "catering", "co.", "inc", "llc",
}

_PUNCT = re.compile(r"[^\w\s]")
_WS = re.compile(r"\s+")


def _fold_accents(s: str) -> str:
    """Strip diacritics so 'Réveille' == 'Reveille' after normalization."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", s)
        if not unicodedata.combining(c)
    )


def _normalize_for_comparison(name: str, extra_stopwords: Optional[set] = None) -> str:
    """Lower, fold accents, strip punctuation, drop filler words,
    collapse whitespace.
    """
    if not name:
        return ""
    s = _fold_accents(name).lower()
    s = _PUNCT.sub(" ", s)
    stops = _STOPWORDS if extra_stopwords is None else (_STOPWORDS | extra_stopwords)
    tokens = [t for t in s.split() if t and t not in stops]
    return _WS.sub(" ", " ".join(tokens)).strip()


def _city_tokens(city: Optional[str]) -> set:
    """Tokenized, lowered, accent-folded set of city words.

    Used to strip 'San Mateo' suffix from 'Tabla Indian Restaurant - San
    Mateo' before comparison. If we searched by city=San Mateo, any
    '- San Mateo' in the Google name is just a disambiguator Google added
    and shouldn't count as a name difference.
    """
    if not city:
        return set()
    s = _fold_accents(city).lower()
    s = _PUNCT.sub(" ", s)
    return {t for t in s.split() if t}


def _name_similarity(a: str, b: str, city: Optional[str] = None) -> float:
    """Return [0, 1] — 1.0 is identical after normalization.

    Does NOT auto-boost for substring containment — extra tokens in the
    Google name are often branch/location identifiers ("Amber India
    Milpitas" is a different place than Amber India in Fremont), and we
    want the needs_review flag to surface those rather than auto-accepting.

    If `city` is provided, its tokens are treated as stopwords on both
    sides. This handles the very common pattern where Google adds the
    city as a suffix ("Benihana - Burlingame", "Tabla Indian Restaurant -
    San Mateo") for disambiguation — we already know the city because we
    searched with it, so it's not new information.
    """
    city_stops = _city_tokens(city)
    na = _normalize_for_comparison(a, extra_stopwords=city_stops)
    nb = _normalize_for_comparison(b, extra_stopwords=city_stops)
    if not na or not nb:
        return 0.0
    if na == nb:
        return 1.0
    return SequenceMatcher(None, na, nb).ratio()


class PlacesResolver:
    """Thin wrapper around Google Places 'Find Place from Text'."""

    def __init__(
        self,
        api_key: str,
        accept_threshold: float = 0.85,
        review_threshold: float = 0.60,
        request_delay_sec: float = 0.1,
        timeout_sec: float = 10.0,
    ):
        if not api_key:
            raise ValueError("api_key is required")
        self.api_key = api_key
        self.accept_threshold = accept_threshold
        self.review_threshold = review_threshold
        self.request_delay_sec = request_delay_sec
        self.timeout_sec = timeout_sec
        self._last_call_ts: float = 0.0

    def resolve(
        self,
        name: str,
        city: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None,
    ) -> Optional[dict[str, Any]]:
        """Resolve (name, city, lat/lng) → best place match, or None.

        Returns a dict with keys: place_id, matched_name, confidence, status
            status ∈ {"matched", "needs_review"}

        lat/lng, when provided, are used as a Places API locationbias —
        this is far more reliable than the city string, especially when the
        city was parsed incorrectly from a messy address (e.g. suite
        numbers, shopping-center names, or MapKit UI artifacts).
        """
        if not name:
            return None

        # Build the query: name + city + CA so Places doesn't return a
        # same-name restaurant on the other coast.
        query_parts = [name]
        if city:
            query_parts.append(city)
        query_parts.append("CA")
        query = ", ".join(query_parts)

        # Bias to lat/lng when we have it — 2km radius keeps us inside the
        # neighborhood without being so tight we miss a legit match a couple
        # of blocks away.
        locationbias = None
        if lat is not None and lng is not None:
            locationbias = f"circle:2000@{lat},{lng}"

        candidates = self._find_place(query, locationbias=locationbias)
        if not candidates:
            return None

        # Score each candidate against our input name, pick best.
        best = None
        best_score = -1.0
        for cand in candidates:
            cand_name = cand.get("name", "")
            score = _name_similarity(name, cand_name, city=city)
            if score > best_score:
                best = cand
                best_score = score

        if best_score < self.review_threshold:
            # Log the candidates we saw so a low-confidence rejection isn't
            # opaque — we need to be able to tell a "Google returned junk"
            # miss from a "user's stored name is unrecognizable" miss.
            preview = ", ".join(
                f"{c.get('name', '')!r}({_name_similarity(name, c.get('name', ''), city=city):.2f})"
                for c in candidates[:3]
            )
            logger.debug(f"resolve('{name}', city={city}) → rejected. Top: {preview}")
            return None

        return {
            "place_id": best.get("place_id"),
            "matched_name": best.get("name"),
            "formatted_address": best.get("formatted_address"),
            "confidence": round(best_score, 3),
            "status": "matched" if best_score >= self.accept_threshold else "needs_review",
        }

    def _find_place(
        self,
        query: str,
        locationbias: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Call Places 'Find Place from Text' and return the candidates."""
        # Simple rate limiter — keeps us well under the 600 RPM default quota.
        elapsed = time.time() - self._last_call_ts
        if elapsed < self.request_delay_sec:
            time.sleep(self.request_delay_sec - elapsed)

        params = {
            "input": query,
            "inputtype": "textquery",
            # Basic Data fields — free alongside the base Find Place call.
            "fields": "place_id,name,formatted_address,geometry",
            "key": self.api_key,
        }
        if locationbias:
            params["locationbias"] = locationbias
        url = f"{FIND_PLACE_URL}?{urlencode(params)}"

        for attempt in range(3):
            try:
                req = Request(url, headers={"User-Agent": "menu-scraper/1.0"})
                with urlopen(req, timeout=self.timeout_sec) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                self._last_call_ts = time.time()
                break
            except Exception as e:
                logger.warning(f"Places API error (attempt {attempt+1}/3) for '{query}': {e}")
                time.sleep(1 + attempt * 2)
        else:
            return []

        status = data.get("status")
        if status == "ZERO_RESULTS":
            return []
        if status != "OK":
            # OVER_QUERY_LIMIT, REQUEST_DENIED, INVALID_REQUEST, etc.
            logger.warning(
                f"Places API returned status={status} for '{query}': "
                f"{data.get('error_message', '')}"
            )
            return []

        return data.get("candidates", [])
