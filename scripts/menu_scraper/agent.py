"""
Menu Scraper Agent — orchestrates scraping from multiple sources,
deduplicates results, and stores everything in SQLite.
"""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Optional

from .database import (
    init_db, get_connection, upsert_restaurant, upsert_category,
    insert_menu_item, log_scrape, get_restaurant_menu, search_items,
)
from .scrapers.base import ScrapeResult, MenuItem
from .scrapers.website import WebsiteScraper
from .scrapers.yelp import YelpScraper
from .scrapers.google import GoogleScraper

logger = logging.getLogger(__name__)


@dataclass
class AgentConfig:
    """Configuration for the scraper agent."""
    headless: bool = True
    timeout: int = 30
    use_llm: bool = False           # use LLM fallback for unstructured pages
    sources: list = None             # which sources to use: ['website', 'yelp', 'google']
    db_path: Optional[str] = None

    def __post_init__(self):
        if self.sources is None:
            self.sources = ["website", "yelp", "google"]


class MenuScraperAgent:
    """
    High-level agent that coordinates scraping across multiple sources.

    Usage:
        agent = MenuScraperAgent()

        # Scrape by URL
        result = await agent.scrape_url("https://somerestaurant.com/menu")

        # Search by name + location
        result = await agent.search("Pizzeria Delfina", "San Francisco, CA")

        # Query the database
        menu = agent.get_menu(restaurant_id=1)
        results = agent.search_items("burger")

        await agent.close()
    """

    def __init__(self, config: Optional[AgentConfig] = None):
        self.config = config or AgentConfig()
        self._scrapers = {}
        self._init_scrapers()
        init_db(self.config.db_path)

    def _init_scrapers(self):
        """Initialize the enabled scrapers."""
        if "website" in self.config.sources:
            self._scrapers["website"] = WebsiteScraper(
                headless=self.config.headless,
                timeout=self.config.timeout,
                use_llm=self.config.use_llm,
            )
        if "yelp" in self.config.sources:
            self._scrapers["yelp"] = YelpScraper(
                headless=self.config.headless,
                timeout=self.config.timeout,
            )
        if "google" in self.config.sources:
            self._scrapers["google"] = GoogleScraper(
                headless=self.config.headless,
                timeout=self.config.timeout,
            )

    async def close(self):
        """Close all scraper browser instances."""
        for scraper in self._scrapers.values():
            await scraper.close()

    async def scrape_url(self, url: str, source: str = None) -> dict:
        """
        Scrape a specific URL. Auto-detects source if not provided.

        Returns a dict with restaurant_id and a summary.
        """
        if source is None:
            source = self._detect_source(url)

        scraper = self._scrapers.get(source)
        if not scraper:
            return {"error": f"No scraper available for source '{source}'"}

        logger.info(f"Scraping {url} with {source} scraper...")
        result = await scraper.scrape(url)
        return self._store_result(result)

    async def search(self, restaurant_name: str, location: str = "",
                     sources: list = None) -> list[dict]:
        """
        Search for a restaurant across enabled sources and scrape menus.

        Returns a list of dicts with restaurant_id and summary for each source.
        """
        sources = sources or self.config.sources
        all_results = []

        tasks = []
        for source_name in sources:
            scraper = self._scrapers.get(source_name)
            if scraper and source_name != "website":  # website scraper doesn't support search
                tasks.append(self._search_source(scraper, restaurant_name, location))

        if tasks:
            gathered = await asyncio.gather(*tasks, return_exceptions=True)
            for result_list in gathered:
                if isinstance(result_list, Exception):
                    logger.error(f"Search task failed: {result_list}")
                    continue
                for scrape_result in result_list:
                    stored = self._store_result(scrape_result)
                    all_results.append(stored)

        return all_results

    async def _search_source(self, scraper, name: str, location: str) -> list[ScrapeResult]:
        """Run a search on a single scraper."""
        try:
            return await scraper.search(name, location)
        except Exception as e:
            logger.error(f"Search failed on {scraper.source_name}: {e}")
            return []

    def _store_result(self, result: ScrapeResult) -> dict:
        """Store a ScrapeResult in the database. Returns summary dict."""
        conn = get_connection(self.config.db_path)
        summary = {
            "restaurant_name": result.restaurant_name,
            "source": result.source,
            "source_url": result.source_url,
            "status": result.status,
            "items_found": len(result.items),
            "categories_found": len(result.categories),
            "duration_seconds": round(result.duration_seconds, 2),
        }

        if result.status == "failed" and not result.restaurant_name:
            summary["error"] = result.error_message
            log_scrape(conn, None, result.source, result.source_url,
                       result.status, 0, result.error_message, result.duration_seconds)
            conn.close()
            return summary

        # Upsert restaurant
        restaurant_id = upsert_restaurant(conn, {
            "name": result.restaurant_name,
            "address": result.address,
            "phone": result.phone,
            "website": result.website or result.source_url,
            "cuisine_type": result.cuisine_type,
            "source": result.source,
            "source_url": result.source_url,
            "latitude": result.latitude,
            "longitude": result.longitude,
            "rating": result.rating,
        })
        summary["restaurant_id"] = restaurant_id

        # Clear old items for this restaurant before inserting fresh ones
        conn.execute("DELETE FROM menu_items WHERE restaurant_id = ?", (restaurant_id,))
        conn.execute("DELETE FROM menu_categories WHERE restaurant_id = ?", (restaurant_id,))
        conn.commit()

        # Insert categories and items
        category_ids = {}
        for i, cat_name in enumerate(result.categories):
            cat_id = upsert_category(conn, restaurant_id, cat_name, sort_order=i)
            category_ids[cat_name] = cat_id

        for item in result.items:
            insert_menu_item(conn, {
                "restaurant_id": restaurant_id,
                "category_id": category_ids.get(item.category),
                "name": item.name,
                "description": item.description,
                "price": item.price,
                "price_text": item.price_text,
                "image_url": item.image_url,
                "dietary_tags": item.dietary_tags,
                "is_available": 1 if item.is_available else 0,
            })

        # Log the scrape
        log_scrape(conn, restaurant_id, result.source, result.source_url,
                   result.status, len(result.items), result.error_message, result.duration_seconds)

        conn.close()
        logger.info(f"Stored {len(result.items)} items for '{result.restaurant_name}' (id={restaurant_id})")
        return summary

    def get_menu(self, restaurant_id: int) -> Optional[dict]:
        """Retrieve a full restaurant menu from the database."""
        conn = get_connection(self.config.db_path)
        menu = get_restaurant_menu(conn, restaurant_id)
        conn.close()
        return menu

    def search_items(self, query: str) -> list[dict]:
        """Search stored menu items by keyword."""
        conn = get_connection(self.config.db_path)
        results = search_items(conn, query)
        conn.close()
        return results

    def list_restaurants(self) -> list[dict]:
        """List all restaurants in the database."""
        conn = get_connection(self.config.db_path)
        rows = conn.execute("""
            SELECT r.*, COUNT(mi.id) as item_count
            FROM restaurants r
            LEFT JOIN menu_items mi ON r.id = mi.restaurant_id
            GROUP BY r.id
            ORDER BY r.name
        """).fetchall()
        conn.close()
        return [dict(r) for r in rows]

    @staticmethod
    def _detect_source(url: str) -> str:
        """Auto-detect the source type from a URL."""
        url_lower = url.lower()
        if "yelp.com" in url_lower:
            return "yelp"
        elif "google.com" in url_lower or "goo.gl" in url_lower:
            return "google"
        elif "doordash.com" in url_lower:
            return "website"  # treat as generic for now
        elif "ubereats.com" in url_lower:
            return "website"
        elif "grubhub.com" in url_lower:
            return "website"
        else:
            return "website"
