"""
Google scraper — searches for restaurant menus via Google and extracts
data from Google's knowledge panel or follows links to menu pages.
"""

import asyncio
import re
import time
import logging
from typing import Optional
from urllib.parse import quote_plus, urlparse

from playwright.async_api import async_playwright, Browser

from .base import BaseScraper, ScrapeResult, MenuItem

logger = logging.getLogger(__name__)


class GoogleScraper(BaseScraper):
    """Uses Google Search to find restaurant menus and scrape them."""

    source_name = "google"

    def __init__(self, headless: bool = True, timeout: int = 30):
        super().__init__(headless=headless, timeout=timeout)
        self._browser: Optional[Browser] = None

    async def _get_browser(self):
        if not self._browser:
            pw = await async_playwright().start()
            self._browser = await pw.chromium.launch(headless=self.headless)
        return self._browser

    async def close(self):
        if self._browser:
            await self._browser.close()
            self._browser = None

    async def scrape(self, url: str) -> ScrapeResult:
        """Not the primary interface — use search() instead."""
        # If given a Google Maps or search URL, extract the query
        if "google.com" in url:
            return ScrapeResult(
                restaurant_name="",
                source=self.source_name,
                source_url=url,
                status="failed",
                error_message="Use search() with a restaurant name instead of scrape() with a Google URL"
            )
        # Otherwise delegate to website scraper
        from .website import WebsiteScraper
        ws = WebsiteScraper(headless=self.headless, timeout=self.timeout)
        result = await ws.scrape(url)
        await ws.close()
        result.source = self.source_name
        return result

    async def search(self, restaurant_name: str, location: str = "") -> list[ScrapeResult]:
        """Search Google for a restaurant menu and extract data."""
        start = time.time()
        query = f"{restaurant_name} {location} menu".strip()
        search_url = f"https://www.google.com/search?q={quote_plus(query)}"

        result = ScrapeResult(
            restaurant_name=restaurant_name,
            source=self.source_name,
            source_url=search_url,
        )

        try:
            browser = await self._get_browser()
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )

            await page.goto(search_url, wait_until="networkidle", timeout=self.timeout * 1000)
            await page.wait_for_timeout(2000)

            # Try to extract from Google's knowledge panel (right sidebar)
            panel_data = await self._extract_knowledge_panel(page)
            if panel_data:
                result.restaurant_name = panel_data.get("name", restaurant_name)
                result.address = panel_data.get("address")
                result.phone = panel_data.get("phone")
                result.rating = panel_data.get("rating")
                result.cuisine_type = panel_data.get("cuisine_type")
                result.website = panel_data.get("website")

            # Try to find menu items in Google's "Popular dishes" or "Menu" section
            google_items = await self._extract_google_menu_items(page)
            if google_items:
                result.items = google_items
                result.categories = list(set(i.category for i in google_items if i.category))
                result.status = "success"
            else:
                # Try to find a menu link and follow it
                menu_link = await self._find_menu_link(page)
                if menu_link:
                    from .website import WebsiteScraper
                    ws = WebsiteScraper(headless=self.headless, timeout=self.timeout)
                    linked_result = await ws.scrape(menu_link)
                    await ws.close()
                    if linked_result.items:
                        result.items = linked_result.items
                        result.categories = linked_result.categories
                        result.website = menu_link
                        result.status = "success"
                    else:
                        result.status = "partial"
                        result.error_message = f"Found menu page ({menu_link}) but couldn't extract items"
                else:
                    result.status = "partial"
                    result.error_message = "Found restaurant info but no menu items"

            await page.close()

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            logger.error(f"Google scrape failed for '{restaurant_name}': {e}")

        result.duration_seconds = time.time() - start
        return [result] if result.items or result.restaurant_name else []

    async def _extract_knowledge_panel(self, page) -> dict:
        """Extract restaurant info from Google's knowledge panel."""
        try:
            data = await page.evaluate("""
                () => {
                    const result = {};

                    // Restaurant name
                    const nameEl = document.querySelector('[data-attrid="title"], .kp-header [role="heading"]');
                    if (nameEl) result.name = nameEl.textContent.trim();

                    // Address
                    const addrEl = document.querySelector('[data-attrid*="address"], [class*="address"]');
                    if (addrEl) result.address = addrEl.textContent.replace(/^Address:?\s*/i, '').trim();

                    // Phone
                    const phoneEl = document.querySelector('[data-attrid*="phone"], a[href^="tel:"]');
                    if (phoneEl) result.phone = phoneEl.textContent.replace(/^Phone:?\s*/i, '').trim();

                    // Rating
                    const ratingEl = document.querySelector('[class*="rating"], .Aq14fc');
                    if (ratingEl) {
                        const match = ratingEl.textContent.match(/([\d.]+)/);
                        if (match) result.rating = parseFloat(match[1]);
                    }

                    // Cuisine type
                    const typeEl = document.querySelector('[data-attrid*="category"], .YhemCb');
                    if (typeEl) result.cuisine_type = typeEl.textContent.trim();

                    // Website
                    const webEl = document.querySelector('[data-attrid*="website"] a, a[class*="website"]');
                    if (webEl) result.website = webEl.href;

                    return result;
                }
            """)
            return data if data.get("name") else {}
        except Exception:
            return {}

    async def _extract_google_menu_items(self, page) -> list[MenuItem]:
        """Extract menu items from Google's 'Popular dishes' or inline menu."""
        items = []
        try:
            raw_items = await page.evaluate("""
                () => {
                    const results = [];

                    // Google "Popular dishes" carousel
                    const dishes = document.querySelectorAll('[data-attrid*="menu"] [role="listitem"], [data-attrid*="dishes"] [role="listitem"]');
                    for (const dish of dishes) {
                        const nameEl = dish.querySelector('[class*="name"], [role="heading"], span');
                        const priceEl = dish.querySelector('[class*="price"]');
                        const imgEl = dish.querySelector('img');
                        if (nameEl) {
                            results.push({
                                name: nameEl.textContent.trim(),
                                price_text: priceEl ? priceEl.textContent.trim() : '',
                                image_url: imgEl ? imgEl.src : '',
                                category: 'Popular Dishes',
                            });
                        }
                    }

                    // Google "Menu" tab items (if the menu tab is showing)
                    if (results.length === 0) {
                        const menuItems = document.querySelectorAll('[data-attrid*="menu_item"], [class*="menu-item"]');
                        for (const mi of menuItems) {
                            const text = mi.textContent.trim();
                            const priceMatch = text.match(/[\$]\s*(\d+(?:\.\d{1,2})?)/);
                            if (text.length > 2 && text.length < 300) {
                                results.push({
                                    name: priceMatch ? text.slice(0, text.indexOf(priceMatch[0])).trim() : text,
                                    price_text: priceMatch ? priceMatch[0] : '',
                                    image_url: '',
                                    category: 'Menu',
                                });
                            }
                        }
                    }

                    return results;
                }
            """)

            for raw in raw_items:
                name = self.clean_text(raw.get("name", ""))
                if not name or len(name) < 2:
                    continue
                items.append(MenuItem(
                    name=name,
                    price=self.parse_price(raw.get("price_text", "")),
                    price_text=raw.get("price_text") or None,
                    category=raw.get("category") or None,
                    image_url=raw.get("image_url") or None,
                    dietary_tags=self.detect_dietary_tags(name),
                ))
        except Exception as e:
            logger.debug(f"Google menu item extraction failed: {e}")

        return items

    async def _find_menu_link(self, page) -> Optional[str]:
        """Find a menu URL from Google search results."""
        try:
            link = await page.evaluate("""
                () => {
                    // Look for "Menu" link in knowledge panel
                    const menuLinks = Array.from(document.querySelectorAll('a')).filter(a => {
                        const text = (a.textContent || '').toLowerCase().trim();
                        const href = (a.href || '').toLowerCase();
                        return (text.includes('menu') || text.includes('view menu')
                                || href.includes('menu'))
                               && !href.includes('google.com')
                               && !href.includes('yelp.com/search')
                               && a.href.startsWith('http');
                    });
                    return menuLinks.length > 0 ? menuLinks[0].href : null;
                }
            """)
            return link
        except Exception:
            return None
