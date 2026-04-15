#!/usr/bin/env python3
"""
CLI interface for the restaurant menu scraper agent.

Usage:
    # Scrape a specific restaurant URL
    python -m menu_scraper scrape https://somerestaurant.com/menu

    # Search for a restaurant by name
    python -m menu_scraper search "Pizzeria Delfina" --location "San Francisco, CA"

    # Search using only specific sources
    python -m menu_scraper search "Shake Shack" --sources yelp google

    # List all scraped restaurants
    python -m menu_scraper list

    # View a restaurant's full menu
    python -m menu_scraper menu 1

    # Search stored menu items
    python -m menu_scraper find "burger"

    # Export a restaurant's menu to JSON
    python -m menu_scraper export 1 --output menu.json
"""

import argparse
import asyncio
import json
import logging
import sys

from .agent import MenuScraperAgent, AgentConfig


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="menu_scraper",
        description="Restaurant menu scraper agent — scrape menus from websites, Yelp, and Google",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    parser.add_argument("--db", default=None, help="Path to SQLite database (default: ./menus.db)")
    parser.add_argument("--no-headless", action="store_true", help="Show browser window")
    parser.add_argument("--timeout", type=int, default=30, help="Page load timeout in seconds")
    parser.add_argument("--use-llm", action="store_true", help="Enable LLM fallback for extraction")

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # scrape
    p_scrape = subparsers.add_parser("scrape", help="Scrape a specific URL")
    p_scrape.add_argument("url", help="Restaurant or menu page URL")
    p_scrape.add_argument("--source", choices=["website", "yelp", "google"], help="Force source type")

    # search
    p_search = subparsers.add_parser("search", help="Search for a restaurant by name")
    p_search.add_argument("name", help="Restaurant name")
    p_search.add_argument("--location", "-l", default="", help="City or address")
    p_search.add_argument("--sources", nargs="+", choices=["website", "yelp", "google"],
                          default=None, help="Sources to search")

    # list
    subparsers.add_parser("list", help="List all scraped restaurants")

    # menu
    p_menu = subparsers.add_parser("menu", help="View a restaurant's menu")
    p_menu.add_argument("restaurant_id", type=int, help="Restaurant ID")

    # find
    p_find = subparsers.add_parser("find", help="Search stored menu items")
    p_find.add_argument("query", help="Search keyword")

    # export
    p_export = subparsers.add_parser("export", help="Export a menu to JSON")
    p_export.add_argument("restaurant_id", type=int, help="Restaurant ID")
    p_export.add_argument("--output", "-o", default=None, help="Output file (default: stdout)")

    return parser


async def cmd_scrape(agent: MenuScraperAgent, args):
    result = await agent.scrape_url(args.url, source=args.source)
    print_result(result)


async def cmd_search(agent: MenuScraperAgent, args):
    results = await agent.search(args.name, args.location, sources=args.sources)
    if not results:
        print("No results found.")
        return
    for r in results:
        print_result(r)
        print()


def cmd_list(agent: MenuScraperAgent):
    restaurants = agent.list_restaurants()
    if not restaurants:
        print("No restaurants in database yet. Try 'scrape' or 'search' first.")
        return
    print(f"{'ID':<5} {'Name':<35} {'Source':<10} {'Items':<7} {'Rating':<7} {'Cuisine'}")
    print("-" * 90)
    for r in restaurants:
        print(f"{r['id']:<5} {r['name'][:34]:<35} {r['source']:<10} {r['item_count']:<7} "
              f"{r['rating'] or '-':<7} {r['cuisine_type'] or '-'}")


def cmd_menu(agent: MenuScraperAgent, args):
    menu = agent.get_menu(args.restaurant_id)
    if not menu:
        print(f"Restaurant {args.restaurant_id} not found.")
        return

    print(f"\n{'=' * 60}")
    print(f"  {menu['name']}")
    if menu.get("cuisine_type"):
        print(f"  {menu['cuisine_type']}")
    if menu.get("address"):
        print(f"  {menu['address']}")
    if menu.get("rating"):
        print(f"  Rating: {menu['rating']}/5")
    print(f"{'=' * 60}\n")

    for cat in menu.get("categories", []):
        print(f"  --- {cat['name']} ---")
        for item in cat.get("items", []):
            price = item.get("price_text") or (f"${item['price']:.2f}" if item.get("price") else "")
            tags = ""
            if item.get("dietary_tags"):
                tag_list = item["dietary_tags"]
                if isinstance(tag_list, str):
                    import json as j
                    try:
                        tag_list = j.loads(tag_list)
                    except Exception:
                        tag_list = []
                if tag_list:
                    tags = f"  [{', '.join(tag_list)}]"
            print(f"    {item['name']:<40} {price:>10}{tags}")
            if item.get("description"):
                print(f"      {item['description'][:70]}")
        print()


def cmd_find(agent: MenuScraperAgent, args):
    results = agent.search_items(args.query)
    if not results:
        print(f"No menu items matching '{args.query}'.")
        return
    print(f"Found {len(results)} item(s):\n")
    for item in results:
        price = item.get("price_text") or (f"${item['price']:.2f}" if item.get("price") else "N/A")
        print(f"  {item['name']:<35} {price:>10}  @ {item['restaurant_name']}")
        if item.get("description"):
            print(f"    {item['description'][:70]}")


def cmd_export(agent: MenuScraperAgent, args):
    menu = agent.get_menu(args.restaurant_id)
    if not menu:
        print(f"Restaurant {args.restaurant_id} not found.", file=sys.stderr)
        sys.exit(1)

    output = json.dumps(menu, indent=2, default=str)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Exported to {args.output}")
    else:
        print(output)


def print_result(result: dict):
    status_icon = {"success": "OK", "partial": "PARTIAL", "failed": "FAIL"}.get(result.get("status"), "?")
    print(f"[{status_icon}] {result.get('restaurant_name', 'Unknown')} "
          f"({result.get('source', '?')}) — "
          f"{result.get('items_found', 0)} items, "
          f"{result.get('categories_found', 0)} categories "
          f"({result.get('duration_seconds', 0):.1f}s)")
    if result.get("restaurant_id"):
        print(f"  Restaurant ID: {result['restaurant_id']}")
    if result.get("error"):
        print(f"  Error: {result['error']}")


async def async_main():
    parser = build_parser()
    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    setup_logging(args.verbose)

    config = AgentConfig(
        headless=not args.no_headless,
        timeout=args.timeout,
        use_llm=args.use_llm,
        db_path=args.db,
    )
    agent = MenuScraperAgent(config)

    try:
        if args.command == "scrape":
            await cmd_scrape(agent, args)
        elif args.command == "search":
            await cmd_search(agent, args)
        elif args.command == "list":
            cmd_list(agent)
        elif args.command == "menu":
            cmd_menu(agent, args)
        elif args.command == "find":
            cmd_find(agent, args)
        elif args.command == "export":
            cmd_export(agent, args)
    finally:
        await agent.close()


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
