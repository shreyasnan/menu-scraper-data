"""
Tests for dish_normalizer. Run with:
    python -m menu_scraper.test_dish_normalizer

Cases drawn from real Shizen / SF Bay Area scraper output that
exposed the fused-name-and-lowercased-name bug.
"""

from menu_scraper.dish_normalizer import (
    normalize_dish, split_fused_name, title_case_dish, strip_caps_prefix
)


def _check(label: str, got, expected) -> bool:
    if got == expected:
        print(f"  PASS {label}")
        return False
    print(f"  FAIL {label}\n        got      = {got!r}\n        expected = {expected!r}")
    return True


def main() -> int:
    failures = 0

    print("\n=== split_fused_name ===")
    # 1. Lowercase → Capital boundary (clean fix)
    failures += _check(
        "shishitoJapanese boundary",
        split_fused_name("shishitoJapanese sweet and spicy peppers, tare"),
        ("shishito", "Japanese sweet and spicy peppers, tare"),
    )
    failures += _check(
        "bean curd katsuPanko boundary",
        split_fused_name("bean curd katsuPanko bean curd, mirin, house-made katsu"),
        ("bean curd katsu", "Panko bean curd, mirin, house-made katsu"),
    )

    # 2. Verb-glue boundary (the 'stuffed with' pattern)
    failures += _check(
        "shizen shiitakestuffed with…",
        split_fused_name("shizen shiitakestuffed with shredded tofu, matcha salt"),
        ("shizen shiitake", "stuffed with shredded tofu, matcha salt"),
    )
    failures += _check(
        "salmonseasoned with miso",
        split_fused_name("salmonseasoned with white miso and citrus"),
        ("salmon", "seasoned with white miso and citrus"),
    )

    # 3. Properly-spaced names should NOT split (false-positive guard)
    failures += _check(
        "Beef Served with Pickles (clean — no split)",
        split_fused_name("Beef Served with Pickles and aged soy"),
        ("Beef Served with Pickles and aged soy", None),
    )
    failures += _check(
        "Short clean name (no split)",
        split_fused_name("Margherita Pizza"),
        ("Margherita Pizza", None),
    )

    # 4. Fused but no detectable boundary — leave as-is
    failures += _check(
        "agedashi tofudaikon — no clean boundary, leave alone",
        split_fused_name("agedashi tofudaikon, scallions, light shoyu"),
        ("agedashi tofudaikon, scallions, light shoyu", None),
    )

    print("\n=== title_case_dish ===")
    failures += _check(
        "all-lowercase → title case",
        title_case_dish("shizen shiitake"),
        "Shizen Shiitake",
    )
    failures += _check(
        "preserve stop-words mid-name",
        title_case_dish("chicken with miso and rice"),
        "Chicken with Miso and Rice",
    )
    failures += _check(
        "keep & symbol",
        title_case_dish("burger & fries"),
        "Burger & Fries",
    )
    failures += _check(
        "already mixed case → leave alone",
        title_case_dish("BBQ Pulled Pork"),
        "BBQ Pulled Pork",
    )
    failures += _check(
        "Pre-cased name → leave alone",
        title_case_dish("Spicy Garlic Miso"),
        "Spicy Garlic Miso",
    )
    # "Spicy garlic miso" — letters lowercase % is high but not 85+%.
    # Letters: S,p,i,c,y,g,a,r,l,i,c,m,i,s,o = 15 letters. Lowercase = 14.
    # 14/15 = 93%, so it WOULD title-case. That's fine — we want
    # consistent casing across the menu.
    failures += _check(
        "mostly-lower with one cap → title-case",
        title_case_dish("Spicy garlic miso"),
        "Spicy Garlic Miso",
    )

    print("\n=== strip_caps_prefix ===")
    failures += _check(
        "strip 'SPICE BOY - Goat Biryani'",
        strip_caps_prefix("SPICE BOY - Goat Biryani"),
        "Goat Biryani",
    )
    failures += _check(
        "strip 'SALLMON-ELLA-FREE - tandoori salmon (D)'",
        strip_caps_prefix("SALLMON-ELLA-FREE - tandoori salmon (D)"),
        "tandoori salmon (D)",
    )
    failures += _check(
        "strip 'BASSES - Sea Bass Curry'",
        strip_caps_prefix("BASSES - Sea Bass Curry"),
        "Sea Bass Curry",
    )
    failures += _check(
        "leave clean dish unchanged",
        strip_caps_prefix("Margherita Pizza"),
        "Margherita Pizza",
    )
    failures += _check(
        "leave 'BBQ - 12 oz' alone (tail too short / no lowercase)",
        strip_caps_prefix("BBQ - 12 OZ"),
        "BBQ - 12 OZ",
    )
    # Round 2: no-space dash variant
    failures += _check(
        "strip 'SHEESH- lamb Seekh Kebab' (no leading space)",
        strip_caps_prefix("SHEESH- lamb Seekh Kebab"),
        "lamb Seekh Kebab",
    )
    failures += _check(
        "strip 'CRABOOM- Crab Cake'",
        strip_caps_prefix("CRABOOM- Crab Cake"),
        "Crab Cake",
    )
    # Apostrophe + period in head
    failures += _check(
        "strip \"I'M NOT PASTA- spinach paneer lasagna (D)\"",
        strip_caps_prefix("I'M NOT PASTA- spinach paneer lasagna (D)"),
        "spinach paneer lasagna (D)",
    )
    failures += _check(
        "strip 'G.O.A.T-  Kholapuri Goat Curry (D)' (period in head, double space)",
        strip_caps_prefix("G.O.A.T-  Kholapuri Goat Curry (D)"),
        "Kholapuri Goat Curry (D)",
    )

    print("\n=== normalize_dish (end-to-end) ===")
    failures += _check(
        "fused + lowercase Shizen item → fully cleaned",
        normalize_dish(
            "shizen shiitakestuffed with shredded tofu, matcha salt", None, None
        ),
        {"n": "Shizen Shiitake", "d": "stuffed with shredded tofu, matcha salt"},
    )
    failures += _check(
        "split via lowerCap, with price",
        normalize_dish(
            "bean curd katsuPanko bean curd, mirin, house-made katsu", None, 14.0
        ),
        {
            "n": "Bean Curd Katsu",
            "d": "Panko bean curd, mirin, house-made katsu",
            "p": 14.0,
        },
    )
    failures += _check(
        "already-clean name preserved",
        normalize_dish("Margherita Pizza", "San Marzano tomatoes, fresh basil", 18.0),
        {
            "n": "Margherita Pizza",
            "d": "San Marzano tomatoes, fresh basil",
            "p": 18.0,
        },
    )
    failures += _check(
        "no description, no price",
        normalize_dish("gyoza", None, None),
        {"n": "Gyoza"},
    )
    failures += _check(
        "lowercase 'Spicy garlic miso bold & spicy'",
        normalize_dish("Spicy garlic miso bold & spicy", None, None),
        {"n": "Spicy Garlic Miso Bold & Spicy"},
    )
    failures += _check(
        "preserve existing description over extracted tail",
        normalize_dish(
            "shizen shiitakestuffed with shredded tofu",
            "Custom note from waiter",
            None,
        ),
        {"n": "Shizen Shiitake", "d": "Custom note from waiter"},
    )
    failures += _check(
        "Aurum: SPICE BOY - Goat Biryani → cleaned",
        normalize_dish("SPICE BOY - Goat Biryani (D)", None, None),
        {"n": "Goat Biryani (D)"},
    )
    failures += _check(
        "Aurum: SALLMON-ELLA-FREE - tandoori salmon (D) → cleaned",
        normalize_dish("SALLMON-ELLA-FREE - tandoori salmon (D)", None, None),
        {"n": "Tandoori Salmon (D)"},
    )

    print()
    if failures:
        print(f"{failures} test(s) FAILED")
        return 1
    print("All tests passed.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
