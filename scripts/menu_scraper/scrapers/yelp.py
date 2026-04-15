"""
Yelp scraper — extracts menu data from Yelp restaurant pages.

Uses Playwright to render the Yelp page (which is heavily JS-dependent)
and extracts menu items from the "Full Menu" section.
"""

import asyncio
import re
import time
import logging
from typing import Optional
from urllib.parse import quote_plus

from playwright.async_api import async_playwright, Browser

from .base import BaseScraper, ScrapeResult, MenuItem

logger = logging.getLogger(__name__)

YELP_BASE = "https://www.yelp.com"


class YelpScraper(BaseScraper):
    """Scrapes restaurant menus from Yelp."""

    source_name = "yelp"

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
        """Scrape menu from a Yelp restaurant page URL."""
        start = time.time()
        result = ScrapeResult(restaurant_name="", source=self.source_name, source_url=url)

        try:
            browser = await self._get_browser()
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )

            # Navigate to the menu tab if not already there
            menu_url = url.rstrip("/")
            if not menu_url.endswith("/menu"):
                menu_url += "/menu"

            await page.goto(menu_url, wait_until="networkidle", timeout=self.timeout * 1000)
            await page.wait_for_timeout(2000)

            # Extract restaurant info
            result.restaurant_name = await page.evaluate("""
                () => {
                    const h1 = document.querySelector('h1');
                    return h1 ? h1.textContent.trim() : '';
                }
            """)

            result.address = await page.evaluate("""
                () => {
                    const addr = document.querySelector('[class*="businessAddress"], address, [data-testid*="address"]');
                    return addr ? addr.textContent.trim() : '';
                }
            """)

            result.phone = await page.evaluate("""
                () => {
                    const phone = document.querySelector('[class*="phone"], a[href^="tel:"]');
                    return phone ? phone.textContent.trim() : '';
                }
            """)

            # Extract cuisine type from breadcrumbs or categories
            result.cuisine_type = await page.evaluate("""
                () => {
                    const cats = document.querySelectorAll('[class*="category"] a, [class*="breadcrumb"] a');
                    const types = Array.from(cats).map(a => a.textContent.trim()).filter(t => t.length < 30);
                    return types.slice(0, 3).join(', ');
                }
            """)

            # Extract rating
            rating_text = await page.evaluate("""
                () => {
                    const rating = document.querySelector('[class*="rating"], [aria-label*="star"]');
                    if (!rating) return '';
                    const label = rating.getAttribute('aria-label') || rating.textContent || '';
                    const match = label.match(/([\d.]+)/);
                    return match ? match[1] : '';
                }
            """)
            if rating_text:
                try:
                    result.rating = float(rating_text)
                except ValueError:
                    pass

            # Extract menu items
            items = await self._extract_yelp_menu(page)
            result.items = items
            result.categories = list(set(item.category for item in items if item.category))
            result.status = "success" if items else "partial"
            if not items:
                result.error_message = "No menu items found on Yelp page"

            await page.close()

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            logger.error(f"Yelp scrape failed for {url}: {e}")

        result.duration_seconds = time.time() - start
        return result

    async def search(self, restaurant_name: str, location: str = "") -> list[ScrapeResult]:
        """Search Yelp for a restaurant and scrape its menu."""
        results = []
        try:
            browser = await self._get_browser()
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )

            query = quote_plus(restaurant_name)
            loc = quote_plus(location) if location else ""
            search_url = f"{YELP_BASE}/search?find_desc={query}&find_loc={loc}"

            await page.goto(search_url, wait_until="networkidle", timeout=self.timeout * 1000)
            await page.wait_for_timeout(2000)

            # Get top search result links
            links = await page.evaluate("""
                () => {
                    const results = document.querySelectorAll('[class*="searchResult"] a[href*="/biz/"], [data-testid*="result"] a[href*="/biz/"]');
                    const seen = new Set();
                    const links = [];
                    for (const a of results) {
                        const href = a.href.split('?')[0];
                        if (href.includes('/biz/') && !seen.has(href)) {
                            seen.add(href);
                            links.push(href);
                        }
                    }
                    return links.slice(0, 3);
                }
            """)

            await page.close()

            # Scrape the top result
            for link in links[:1]:  # just the top match for now
                r = await self.scrape(link)
                if r.items:
                    results.append(r)
                    break

        except Exception as e:
            logger.error(f"Yelp search failed for '{restaurant_name}': {e}")

        return results

    async def _extract_yelp_menu(self, page) -> list[MenuItem]:
        """Extract menu items from a Yelp menu page."""
        items = []
        try:
            raw_items = await page.evaluate("""
                () => {
                    const results = [];
                    let currentCategory = '';

                    // Yelp menu sections have headings followed by item lists
                    // Try multiple selector strategies

                    // Strategy 1: Look for menu section containers
                    const sections = document.querySelectorAll(
                        'section[class*="menu"], div[class*="menu-section"], [class*="MenuSection"]'
                    );

                    if (sections.length > 0) {
                        for (const section of sections) {
                            const heading = section.querySelector('h2, h3, h4, [class*="heading"], [class*="title"]');
                            if (heading) currentCategory = heading.textContent.trim();

                            const menuItems = section.querySelectorAll(
                                '[class*="menuItem"], [class*="MenuItem"], [class*="menu-item"]'
                            );
                            for (const mi of menuItems) {
                                const nameEl = mi.querySelector('h4, h3, [class*="name"], [class*="title"], strong');
                                const descEl = mi.querySelector('p, [class*="desc"], [class*="description"]');
                                const priceEl = mi.querySelector('[class*="price"], [class*="Price"]');
                                const imgEl = mi.querySelector('img');

                                if (nameEl) {
                                    results.push({
                                        name: nameEl.textContent.trim(),
                                        description: descEl ? descEl.textContent.trim() : '',
                                        price_text: priceEl ? priceEl.textContent.trim() : '',
                                        category: currentCategory,
                                        image_url: imgEl ? imgEl.src : '',
                                    });
                                }
                            }
                        }
                    }

                    // Strategy 2: Generic extraction — look for repeated patterns with prices
                    if (results.length === 0) {
                        const allElements = document.querySelectorAll('li, tr, [role="listitem"]');
                        for (const el of allElements) {
                            const text = el.textContent.trim();
                            const priceMatch = text.match(/[\$]\s*(\d+(?:\.\d{1,2})?)/);
                            if (priceMatch && text.length < 500 && text.length > 5) {
                                const pIdx = text.indexOf(priceMatch[0]);
                                const name = text.slice(0, pIdx).trim().split('\\n')[0];
                                if (name.length > 1 && name.length < 150) {
                                    results.push({
                                        name: name,
                                        description: '',
                                        price_text: priceMatch[0],
                                        category: '',
                                        image_url: '',
                                    });
                                }
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
                desc = self.clean_text(raw.get("description", ""))
                items.append(MenuItem(
                    name=name,
                    description=desc if desc else None,
                    price=self.parse_price(raw.get("price_text", "")),
                    price_text=raw.get("price_text") or None,
                    category=raw.get("category") or None,
                    image_url=raw.get("image_url") or None,
                    dietary_tags=self.detect_dietary_tags(f"{name} {desc}"),
                ))

        except Exception as e:
            logger.debug(f"Yelp menu extraction failed: {e}")

        return items
