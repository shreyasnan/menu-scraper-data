"""
Tests for menu_scraper.jsonld. Run with:
    python -m menu_scraper.test_jsonld

Samples are trimmed but structurally faithful to what's in the wild
on Squarespace, Bentobox, Toast, Popmenu, and restaurant-built-it-
themselves sites.
"""

import json
from menu_scraper.jsonld import parse_menu_from_ld_blocks


def _run(name, ld_objects, *, expect_items=None, expect_follow=None,
         expect_names=(), expect_categories=(), expect_prices=()):
    ld_strings = [json.dumps(o) for o in ld_objects]
    result = parse_menu_from_ld_blocks(ld_strings)
    items = result["items"]
    failed = False

    if expect_items is not None and len(items) != expect_items:
        print(f"  FAIL {name}: expected {expect_items} items, got {len(items)}")
        failed = True
    if expect_follow is not None and result["follow_url"] != expect_follow:
        print(f"  FAIL {name}: expected follow_url={expect_follow!r}, got {result['follow_url']!r}")
        failed = True

    got_names = {i["name"] for i in items}
    for n in expect_names:
        if n not in got_names:
            print(f"  FAIL {name}: missing expected name {n!r}")
            failed = True

    got_cats = {i["category"] for i in items}
    for c in expect_categories:
        if c not in got_cats:
            print(f"  FAIL {name}: missing expected category {c!r}")
            failed = True

    got_prices = {i["price"] for i in items if i["price"] is not None}
    for p in expect_prices:
        if p not in got_prices:
            print(f"  FAIL {name}: missing expected price {p}")
            failed = True

    if not failed:
        print(f"  PASS {name}  ({len(items)} items)")
    return failed


def main() -> int:
    failures = 0

    # ---- 1. Classic Restaurant → Menu → MenuSection → MenuItem
    failures += _run(
        "restaurant with inline menu",
        [{
            "@context": "https://schema.org",
            "@type": "Restaurant",
            "name": "Chez Sample",
            "hasMenu": {
                "@type": "Menu",
                "hasMenuSection": [
                    {
                        "@type": "MenuSection",
                        "name": "Starters",
                        "hasMenuItem": [
                            {"@type": "MenuItem", "name": "Tuna Tartare",
                             "description": "Ahi tuna, avocado, lime",
                             "offers": {"@type": "Offer", "price": "18.00", "priceCurrency": "USD"}},
                            {"@type": "MenuItem", "name": "Beet Salad",
                             "offers": {"price": 14, "priceCurrency": "USD"}},
                        ],
                    },
                    {
                        "@type": "MenuSection",
                        "name": "Mains",
                        "hasMenuItem": {
                            "@type": "MenuItem", "name": "Duck Breast",
                            "offers": {"price": "42"},
                        },
                    },
                ],
            },
        }],
        expect_items=3,
        expect_follow=None,
        expect_names={"Tuna Tartare", "Beet Salad", "Duck Breast"},
        expect_categories={"Starters", "Mains"},
        expect_prices={18.0, 14.0, 42.0},
    )

    # ---- 2. Restaurant with hasMenu as URL string (must surface follow_url)
    failures += _run(
        "restaurant with hasMenu URL",
        [{
            "@context": "https://schema.org",
            "@type": "Restaurant",
            "name": "Linky Grill",
            "hasMenu": "https://example.com/our-menu",
        }],
        expect_items=0,
        expect_follow="https://example.com/our-menu",
    )

    # ---- 3. @graph array with Menu and MenuSection siblings
    failures += _run(
        "@graph with Menu and MenuSection",
        [{
            "@context": "https://schema.org",
            "@graph": [
                {"@type": "Restaurant", "name": "Graph Bistro"},
                {"@type": "Menu",
                 "hasMenuSection": [
                     {"@type": "MenuSection", "name": "Brunch",
                      "hasMenuItem": [
                          {"@type": "MenuItem", "name": "Eggs Benedict",
                           "offers": {"price": "16.50", "priceCurrency": "USD"}},
                      ]}
                 ]},
            ],
        }],
        expect_items=1,
        expect_names={"Eggs Benedict"},
        expect_categories={"Brunch"},
        expect_prices={16.5},
    )

    # ---- 4. Nested sub-sections (MenuSection inside a MenuSection)
    failures += _run(
        "nested menu sections",
        [{
            "@type": "Menu",
            "hasMenuSection": [
                {"@type": "MenuSection", "name": "Dinner",
                 "hasMenuSection": [
                     {"@type": "MenuSection", "name": "Pasta",
                      "hasMenuItem": [
                          {"@type": "MenuItem", "name": "Cacio e Pepe",
                           "offers": {"price": "22"}},
                      ]}
                 ]}
            ]
        }],
        expect_items=1,
        expect_names={"Cacio e Pepe"},
        # The innermost section's name wins as the category.
        expect_categories={"Pasta"},
        expect_prices={22.0},
    )

    # ---- 5. Offers as array (Wordpress/Yoast variant)
    failures += _run(
        "offers as array",
        [{
            "@type": "MenuItem",
            "name": "Lamb Burger",
            "offers": [
                {"@type": "Offer", "price": "24.00", "priceCurrency": "USD"},
            ],
        }],
        expect_items=1,
        expect_names={"Lamb Burger"},
        expect_prices={24.0},
    )

    # ---- 6. Bare price on MenuItem (non-standard, common on Toast)
    failures += _run(
        "bare price on MenuItem",
        [{
            "@type": "MenuItem",
            "name": "House Lemonade",
            "price": "$6",
        }],
        expect_items=1,
        expect_names={"House Lemonade"},
        expect_prices={6.0},
    )

    # ---- 7. Multiple LD script blocks
    failures += _run(
        "multiple LD blocks",
        [
            {"@type": "Organization", "name": "ACME Restaurants"},
            {"@type": "MenuItem", "name": "Solo Item", "offers": {"price": "9"}},
        ],
        expect_items=1,
        expect_names={"Solo Item"},
    )

    # ---- 8. Dedupe across @graph and top-level Menu
    failures += _run(
        "dedupe duplicated items",
        [{
            "@graph": [
                {"@type": "MenuItem", "name": "Mac & Cheese",
                 "offers": {"price": "12"}},
                {"@type": "Menu",
                 "hasMenuSection": [{"@type": "MenuSection", "name": "Sides",
                     "hasMenuItem": [{"@type": "MenuItem",
                                       "name": "Mac & Cheese",
                                       "offers": {"price": "12"}}]}]},
            ]
        }],
        # Both entries share (name, category) → second has category "Sides"
        # but first has no category, so they DON'T dedupe. Check we get
        # both but only once per (name, category) key.
        expect_items=2,
    )

    # ---- 9. Malformed LD should not crash
    failures += _run(
        "malformed LD tolerated",
        [{"@type": "MenuItem", "name": None}],  # null name → dropped
        expect_items=0,
    )

    # ---- 10. @type as array
    failures += _run(
        "@type as array",
        [{
            "@type": ["Restaurant", "LocalBusiness"],
            "name": "Dual Type",
            "hasMenu": {
                "@type": "Menu",
                "hasMenuItem": [
                    {"@type": "MenuItem", "name": "Catch of the Day",
                     "offers": {"price": "29.50"}},
                ],
            },
        }],
        expect_items=1,
        expect_names={"Catch of the Day"},
        expect_prices={29.5},
    )

    # ---- 11. Empty / whitespace input
    failures += _run(
        "empty input tolerated",
        [],
        expect_items=0,
        expect_follow=None,
    )

    if failures:
        print(f"\n{failures} test(s) FAILED")
        return 1
    print("\nAll tests passed.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
