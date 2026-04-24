#!/usr/bin/env python3
"""
Backfill google_place_id on ForkBook's Firestore restaurants.

The iOS app keys menu lookups by Google Place ID, but older Restaurant docs
predate that field — the Apple MapKit picker doesn't provide Place IDs, so
we have to resolve them after the fact.

For every doc in `circles/{circleId}/restaurants/{restaurantId}` that is
missing `googlePlaceId`, this script:
  1. Reads (name, address, latitude, longitude)
  2. Calls Google Places "Find Place from Text" via the shared
     PlacesResolver, using (name, city, CA)
  3. Writes `googlePlaceId` back to the doc (and a few diagnostic fields
     so we can tell confident matches from reviewable ones)

Safety:
  - Idempotent — skips docs that already have googlePlaceId
  - --dry-run logs what it would do but doesn't write
  - --limit N caps the number of docs processed per run
  - Logs every decision so you can eyeball low-confidence ones

Usage:
    export GOOGLE_PLACES_API_KEY=AIza...
    # service account key lives next to this repo; override with --service-account
    python backfill_firestore_place_ids.py --dry-run --limit 10
    python backfill_firestore_place_ids.py --limit 50
    python backfill_firestore_place_ids.py                       # do them all
    python backfill_firestore_place_ids.py --redo-review         # also re-run low-confidence
"""

import argparse
import logging
import os
import re
import sys
from pathlib import Path

_HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(_HERE))

from menu_scraper.places_resolver import PlacesResolver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger("backfill_place_ids")


# ---------------------------------------------------------------------------
# Swift Restaurant.city logic, ported to Python so we build the same
# query the iOS app would build.
# ---------------------------------------------------------------------------

_COUNTRY_SUFFIXES = {"USA", "US", "United States", "UK", "Canada"}

# Junk parts that sometimes appear mid-address — suite/unit designators,
# or MapKit autocomplete UI artifacts that leaked into saved addresses.
_ADDRESS_JUNK_PATTERNS = [
    re.compile(r"^\s*(unit|suite|ste|apt|#)\b", re.I),
    re.compile(r"^\s*search\s+nearby\s*$", re.I),
    re.compile(r"^\s*directions\s*$", re.I),
]


def _is_junk_part(part: str) -> bool:
    return any(p.match(part) for p in _ADDRESS_JUNK_PATTERNS)


def _city_from_address(address: str) -> str:
    if not address:
        return ""
    parts = [p.strip() for p in address.split(",") if p.strip()]
    # Drop any parts that are clearly unit/suite designators or MapKit
    # UI text ("Search Nearby", "Directions") that got saved by accident.
    parts = [p for p in parts if not _is_junk_part(p)]
    if not parts:
        return ""
    if parts[-1] in _COUNTRY_SUFFIXES:
        parts = parts[:-1]
    if parts:
        last = parts[-1]
        tokens = last.split()
        state_like = (
            len(tokens) >= 1
            and len(tokens[0]) == 2
            and tokens[0].isupper()
            and tokens[0].isalpha()
        )
        starts_with_digit = bool(re.match(r"^\d", last))
        if (state_like or starts_with_digit) and len(parts) >= 2:
            parts = parts[:-1]
    if len(parts) >= 2 and re.match(r"^\d", parts[0]):
        return parts[1]
    return parts[-1] if parts else ""


# ---------------------------------------------------------------------------
# Firestore setup
# ---------------------------------------------------------------------------

def _init_firestore(service_account_path: str):
    """Lazy-import so the rest of the script can explain install errors nicely."""
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except ImportError:
        print(
            "ERROR: firebase-admin is not installed.\n"
            "Install it with: pip install firebase-admin",
            file=sys.stderr,
        )
        sys.exit(1)

    if not os.path.exists(service_account_path):
        print(
            f"ERROR: service account key not found at {service_account_path}\n"
            "Download one from:\n"
            "  https://console.firebase.google.com/project/forkbook-fe65b/settings/serviceaccounts/adminsdk",
            file=sys.stderr,
        )
        sys.exit(1)

    # Only initialize once per process
    if not firebase_admin._apps:
        cred = credentials.Certificate(service_account_path)
        firebase_admin.initialize_app(cred)
    return firestore.client()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--dry-run", action="store_true",
                        help="log what we'd write but don't touch Firestore")
    parser.add_argument("--limit", type=int, default=None,
                        help="stop after this many docs (default: all)")
    parser.add_argument("--redo-review", action="store_true",
                        help="also re-run docs flagged needs_review from a prior run")
    parser.add_argument("--service-account",
                        default=str(_HERE.parent / "firebase-admin-key.json"),
                        help="path to Firebase Admin SDK JSON")
    args = parser.parse_args()

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY")
    if not api_key:
        print("ERROR: set GOOGLE_PLACES_API_KEY in the environment.", file=sys.stderr)
        sys.exit(1)

    db = _init_firestore(args.service_account)
    resolver = PlacesResolver(api_key=api_key)

    # collection_group lets us hit every `restaurants` subcollection across
    # every circle in one query.
    logger.info("Fetching restaurants across all circles…")
    docs = list(db.collection_group("restaurants").stream())
    logger.info(f"Found {len(docs):,} restaurant docs total")

    candidates = []
    for doc in docs:
        data = doc.to_dict() or {}
        existing = data.get("googlePlaceId")
        status = data.get("placeMatchStatus")

        if existing:
            # Already resolved. Skip unless we're re-running review cases.
            if args.redo_review and status == "needs_review":
                candidates.append((doc, data))
            continue
        candidates.append((doc, data))

    if args.limit:
        candidates = candidates[: args.limit]

    logger.info(
        f"To resolve: {len(candidates):,} "
        f"{'[dry-run]' if args.dry_run else ''}"
    )
    if not candidates:
        logger.info("Nothing to do.")
        return

    stats = {"matched": 0, "needs_review": 0, "no_match": 0, "errors": 0, "skipped": 0}

    for i, (doc, data) in enumerate(candidates, start=1):
        name = (data.get("name") or "").strip()
        address = data.get("address") or ""
        if not name:
            stats["skipped"] += 1
            logger.info(f"[{i}/{len(candidates)}] doc {doc.reference.path}: no name, skipping")
            continue

        city = _city_from_address(address)
        lat = data.get("latitude")
        lng = data.get("longitude")
        # Firestore sometimes stores numbers that come back as strings —
        # coerce defensively so a malformed doc doesn't poison the query.
        try:
            lat = float(lat) if lat is not None else None
            lng = float(lng) if lng is not None else None
        except (TypeError, ValueError):
            lat = lng = None

        try:
            match = resolver.resolve(name=name, city=city, lat=lat, lng=lng)
        except Exception as e:
            stats["errors"] += 1
            logger.error(f"[{i}/{len(candidates)}] {name!r} ({city}): resolver error: {e}")
            continue

        if match is None:
            stats["no_match"] += 1
            logger.info(f"[{i}/{len(candidates)}] {name!r} ({city}): no match")
            if not args.dry_run:
                # Record that we tried, so we don't re-run until --redo-review.
                doc.reference.update({
                    "placeMatchStatus": "no_match",
                    "placeResolvedAt": _now_iso(),
                })
            continue

        status = match["status"]
        stats[status] += 1
        bias = "geo" if (lat is not None and lng is not None) else "no-geo"
        logger.info(
            f"[{i}/{len(candidates)}] {name!r} ({city}, {bias}) → "
            f"{match['matched_name']!r} "
            f"(conf={match['confidence']:.2f}, {status})"
        )

        if args.dry_run:
            continue

        update = {
            "googlePlaceId": match["place_id"],
            "placeMatchedName": match["matched_name"],
            "placeMatchConfidence": match["confidence"],
            "placeMatchStatus": status,
            "placeResolvedAt": _now_iso(),
        }
        doc.reference.update(update)

    logger.info(
        f"Done. matched={stats['matched']} "
        f"needs_review={stats['needs_review']} "
        f"no_match={stats['no_match']} "
        f"errors={stats['errors']} "
        f"skipped={stats['skipped']}"
    )


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


if __name__ == "__main__":
    main()
