"""
Tests for dish_normalizer. Run with:
    python -m menu_scraper.test_dish_normalizer

Cases drawn from real Shizen / SF Bay Area scraper output that
exposed the fused-name-and-lowercased-name bug.
"""

from menu_scraper.dish_normalizer import normalize_dish, split_fused_name, title_case_dish


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

    print()
    if failures:
        print(f"{failures} test(s) FAILED")
        return 1
    print("All tests passed.")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
