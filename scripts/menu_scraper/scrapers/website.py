"""
Generic website scraper using Playwright for JavaScript-rendered pages.

This scraper uses heuristics + optional LLM extraction to parse menus
from arbitrary restaurant websites. It works in two passes:

1. Structural pass: looks for common menu HTML patterns (tables, definition
   lists, repeated item+price divs).
2. LLM pass (optional): sends the cleaned page text to an LLM to extract
   structured menu data when heuristics fail.
"""

import asyncio
import re
import time
import logging
from typing import Optional
from urllib.parse import urljoin, urlparse

from playwright.async_api import async_playwright, Page, Browser

from .base import BaseScraper, ScrapeResult, MenuItem

logger = logging.getLogger(__name__)

# CSS selectors commonly used by restaurant websites and menu plugins
MENU_SELECTORS = [
    # Common menu page identifiers
    '[class*="menu"]', '[id*="menu"]',
    '[class*="food"]', '[id*="food"]',
    '[class*="dish"]', '[class*="entree"]',
    # Common CMS menu plugins
    '.restaurant-menu', '.menu-list', '.menu-section',
    '.menu-item', '.food-menu', '.price-list',
    # Structured data
    '[itemtype*="MenuItem"]', '[itemtype*="Menu"]',
]

# Patterns for identifying menu item + price pairs
PRICE_PATTERN = re.compile(
    r'[\$£€]\s*\d+(?:[.,]\d{1,2})?'
    r'|\d+(?:[.,]\d{1,2})?\s*(?:USD|EUR|GBP|\$|£|€)',
    re.IGNORECASE
)


class WebsiteScraper(BaseScraper):
    """Scrapes menus directly from restaurant websites using Playwright."""

    source_name = "website"

    def __init__(self, headless: bool = True, timeout: int = 30, use_llm: bool = False):
        super().__init__(headless=headless, timeout=timeout)
        self.use_llm = use_llm
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
        """Scrape menu items from a direct restaurant website URL."""
        start = time.time()
        result = ScrapeResult(
            restaurant_name="",
            source=self.source_name,
            source_url=url,
        )

        try:
            browser = await self._get_browser()
            page = await browser.new_page(
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"
            )
            await page.goto(url, wait_until="networkidle", timeout=self.timeout * 1000)
            await page.wait_for_timeout(2000)  # extra settle time for JS-rendered content

            # Extract restaurant name from page title or meta
            result.restaurant_name = await self._extract_restaurant_name(page, url)

            # Try to find and click a "Menu" link if we're on the homepage
            menu_page = await self._navigate_to_menu_page(page, url)

            # Extract menu items
            items = await self._extract_menu_items(menu_page)

            if not items and self.use_llm:
                # Fallback: extract page text and use LLM
                page_text = await menu_page.inner_text("body")
                items = await self._llm_extract(page_text, url)

            result.items = items
            result.categories = list(set(item.category for item in items if item.category))
            result.status = "success" if items else "partial"
            if not items:
                result.error_message = "No menu items found — site may need custom parsing rules"

            await page.close()

        except Exception as e:
            result.status = "failed"
            result.error_message = str(e)
            logger.error(f"Failed to scrape {url}: {e}")

        result.duration_seconds = time.time() - start
        return result

    async def search(self, restaurant_name: str, location: str = "") -> list[ScrapeResult]:
        """Not applicable for direct website scraper — use GoogleScraper for search."""
        logger.warning("WebsiteScraper.search() is a no-op. Use GoogleScraper for search-based scraping.")
        return []

    async def _extract_restaurant_name(self, page: Page, url: str) -> str:
        """Try to get the restaurant name from the page."""
        # Try OG site_name first (most reliable — it's usually just the brand)
        og_name = await page.evaluate("""
            () => document.querySelector('meta[property="og:site_name"]')?.content || ''
        """)
        if og_name and len(og_name) < 60:
            return self.clean_text(og_name)

        # Try <title> with aggressive cleaning
        title = await page.title()
        if title:
            # Strip common suffixes / separators
            for sep in [" - ", " | ", " — ", " · ", " :: ", " : "]:
                if sep in title:
                    parts = title.split(sep)
                    # Usually the restaurant name is the first or shortest meaningful part
                    candidates = [p.strip() for p in parts if len(p.strip()) > 2]
                    # Remove generic words
                    GENERIC = {"menu", "home", "official", "order", "online", "delivery",
                               "restaurant", "our menu", "view menu", "food"}
                    candidates = [c for c in candidates if c.lower() not in GENERIC] or candidates
                    if candidates:
                        title = min(candidates, key=len) if len(candidates) > 1 else candidates[0]
                        break
            if len(title) < 60:
                return self.clean_text(title)

        # Try OG title
        og_title = await page.evaluate("""
            () => document.querySelector('meta[property="og:title"]')?.content || ''
        """)
        if og_title and len(og_title) < 60:
            return self.clean_text(og_title)

        # Fallback to domain
        return urlparse(url).hostname.replace("www.", "").split(".")[0].title()

    async def _navigate_to_menu_page(self, page: Page, base_url: str) -> Page:
        """If we're on a homepage, try to find and navigate to the menu page."""
        menu_link = await page.evaluate("""
            () => {
                const links = Array.from(document.querySelectorAll('a'));
                const menuLink = links.find(a => {
                    const text = (a.textContent || '').toLowerCase().trim();
                    const href = (a.href || '').toLowerCase();
                    return (text === 'menu' || text === 'our menu' || text === 'food menu'
                            || text === 'view menu' || text === 'see menu'
                            || href.includes('/menu'));
                });
                return menuLink ? menuLink.href : null;
            }
        """)
        if menu_link and menu_link != page.url:
            try:
                await page.goto(menu_link, wait_until="networkidle", timeout=self.timeout * 1000)
                await page.wait_for_timeout(1500)
            except Exception:
                pass  # stay on current page
        return page

    async def _extract_menu_items(self, page: Page) -> list[MenuItem]:
        """Extract menu items using structural heuristics."""
        items = []

        # Strategy 1: Look for structured data (JSON-LD)
        json_ld_items = await self._extract_from_json_ld(page)
        if json_ld_items:
            return json_ld_items

        # Strategy 2: Look for common menu section patterns
        section_items = await self._extract_from_menu_sections(page)
        if section_items:
            return section_items

        # Strategy 3: Pattern-match repeated elements with prices
        pattern_items = await self._extract_by_price_pattern(page)
        if pattern_items:
            return pattern_items

        # Strategy 4: Text-based extraction — parse visible page text
        text_items = await self._extract_from_page_text(page)
        if text_items:
            return text_items

        return items

    async def _extract_from_json_ld(self, page: Page) -> list[MenuItem]:
        """Extract from JSON-LD structured data if present."""
        items = []
        try:
            json_ld_data = await page.evaluate("""
                () => {
                    const scripts = document.querySelectorAll('script[type="application/ld+json"]');
                    return Array.from(scripts).map(s => {
                        try { return JSON.parse(s.textContent); }
                        catch { return null; }
                    }).filter(Boolean);
                }
            """)
            for data in json_ld_data:
                if isinstance(data, list):
                    for entry in data:
                        items.extend(self._parse_json_ld_entry(entry))
                else:
                    items.extend(self._parse_json_ld_entry(data))
        except Exception:
            pass
        return items

    def _parse_json_ld_entry(self, data: dict) -> list[MenuItem]:
        """Parse a single JSON-LD entry for menu items."""
        items = []
        if not isinstance(data, dict):
            return items

        schema_type = data.get("@type", "")

        if schema_type == "Menu":
            for section in data.get("hasMenuSection", []):
                cat_name = section.get("name", "")
                for item_data in section.get("hasMenuItem", []):
                    item = self._json_ld_to_menu_item(item_data, cat_name)
                    if item:
                        items.append(item)

        elif schema_type == "Restaurant":
            menu = data.get("hasMenu", {})
            if isinstance(menu, dict):
                items.extend(self._parse_json_ld_entry(menu))

        elif schema_type == "MenuItem":
            item = self._json_ld_to_menu_item(data)
            if item:
                items.append(item)

        return items

    def _json_ld_to_menu_item(self, data: dict, category: str = None) -> Optional[MenuItem]:
        """Convert a JSON-LD MenuItem to our MenuItem."""
        name = data.get("name")
        if not name:
            return None

        price = None
        price_text = None
        offers = data.get("offers", {})
        if isinstance(offers, dict):
            price = offers.get("price")
            price_text = offers.get("priceCurrency", "$") + str(price) if price else None
        elif isinstance(offers, list) and offers:
            price = offers[0].get("price")

        return MenuItem(
            name=self.clean_text(name),
            description=self.clean_text(data.get("description", "")),
            price=float(price) if price else None,
            price_text=price_text,
            category=category,
            image_url=data.get("image"),
            dietary_tags=self.detect_dietary_tags(f"{name} {data.get('description', '')}"),
        )

    async def _extract_from_menu_sections(self, page: Page) -> list[MenuItem]:
        """Extract items from common menu section HTML patterns."""
        items = []
        try:
            raw_items = await page.evaluate("""
                () => {
                    const results = [];
                    // Find menu sections by common class patterns
                    const sectionSelectors = [
                        '.menu-section', '.menu-category', '.menu-group',
                        '[class*="menu-section"]', '[class*="menuSection"]',
                        '[class*="menu-category"]', '[class*="menuCategory"]',
                    ];

                    let sections = [];
                    for (const sel of sectionSelectors) {
                        sections = document.querySelectorAll(sel);
                        if (sections.length > 0) break;
                    }

                    if (sections.length === 0) return results;

                    for (const section of sections) {
                        // Try to get category name from first heading
                        const heading = section.querySelector('h1, h2, h3, h4, h5, h6, [class*="title"], [class*="heading"]');
                        const categoryName = heading ? heading.textContent.trim() : '';

                        // Find items within this section
                        const itemSelectors = [
                            '.menu-item', '[class*="menu-item"]', '[class*="menuItem"]',
                            '.item', '.dish', '[class*="dish"]',
                        ];
                        let itemEls = [];
                        for (const sel of itemSelectors) {
                            itemEls = section.querySelectorAll(sel);
                            if (itemEls.length > 0) break;
                        }

                        for (const el of itemEls) {
                            const nameEl = el.querySelector(
                                '[class*="name"], [class*="title"], h3, h4, h5, strong, b'
                            );
                            const descEl = el.querySelector(
                                '[class*="desc"], [class*="description"], p, .description'
                            );
                            const priceEl = el.querySelector(
                                '[class*="price"], .price, .cost, .amount'
                            );
                            const imgEl = el.querySelector('img');

                            if (nameEl) {
                                results.push({
                                    name: nameEl.textContent.trim(),
                                    description: descEl ? descEl.textContent.trim() : '',
                                    price_text: priceEl ? priceEl.textContent.trim() : '',
                                    category: categoryName,
                                    image_url: imgEl ? imgEl.src : '',
                                });
                            }
                        }
                    }
                    return results;
                }
            """)

            for raw in raw_items:
                if not raw.get("name"):
                    continue
                name = self.clean_text(raw["name"])
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
            logger.debug(f"Menu section extraction failed: {e}")

        return items

    async def _extract_by_price_pattern(self, page: Page) -> list[MenuItem]:
        """Last-resort: find elements that contain price-like text and back up to find item names."""
        items = []
        try:
            raw_items = await page.evaluate("""
                () => {
                    const priceRegex = /[\$£€]\s*\d+(?:[.,]\d{1,2})?|\d+(?:[.,]\d{1,2})?\s*(?:USD|EUR|GBP)/i;
                    const results = [];
                    const seen = new Set();

                    // Get all text nodes that look like prices
                    const walker = document.createTreeWalker(
                        document.body, NodeFilter.SHOW_ELEMENT, null
                    );
                    let node;
                    while (node = walker.nextNode()) {
                        const text = node.textContent.trim();
                        if (!priceRegex.test(text)) continue;
                        if (text.length > 500) continue; // skip large containers

                        // Walk up to find a reasonable "item container"
                        let container = node;
                        for (let i = 0; i < 5; i++) {
                            if (!container.parentElement) break;
                            container = container.parentElement;
                            const ct = container.textContent.trim();
                            if (ct.length > 30 && ct.length < 500) break;
                        }

                        const containerText = container.textContent.trim();
                        if (seen.has(containerText) || containerText.length < 5) continue;
                        seen.add(containerText);

                        // Try to split into name and price
                        const priceMatch = containerText.match(priceRegex);
                        if (priceMatch) {
                            const priceIdx = containerText.indexOf(priceMatch[0]);
                            const name = containerText.slice(0, priceIdx).trim();
                            const rest = containerText.slice(priceIdx + priceMatch[0].length).trim();

                            if (name.length > 1 && name.length < 200) {
                                results.push({
                                    name: name.split('\\n')[0].trim(),
                                    description: rest.length > 5 && rest.length < 300 ? rest : '',
                                    price_text: priceMatch[0],
                                });
                            }
                        }
                    }
                    return results.slice(0, 200);  // cap at 200
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
                    dietary_tags=self.detect_dietary_tags(f"{name} {desc}"),
                ))
        except Exception as e:
            logger.debug(f"Price pattern extraction failed: {e}")

        return items

    async def _extract_from_page_text(self, page: Page) -> list[MenuItem]:
        """
        Strategy 4: Parse visible page text to find menu items.
        Works well on sites where items are listed as heading+description blocks
        without standard CSS class conventions or visible prices.
        """
        items = []
        try:
            page_text = await page.inner_text("body")
            lines = [l.strip() for l in page_text.split("\n") if l.strip() and len(l.strip()) > 2]

            # Navigation / boilerplate phrases to skip
            SKIP_PHRASES = {
                "skip", "cookie", "navigation", "order now", "order online",
                "find a location", "find your location", "franchise", "our story",
                "gift card", "join", "connect", "download", "follow us", "sign in",
                "sign up", "log in", "privacy", "terms", "copyright",
                "all rights", "careers", "contact us", "about us",
                "subscribe", "newsletter", "blog", "press", "investor",
                "product images", "items may vary", "catering", "our company",
                "check the nutritionals",
            }

            # Known category keywords — lines containing these are likely section headings
            CATEGORY_KEYWORDS = {
                "appetizer", "starter", "entree", "entre", "main", "pizza",
                "pasta", "salad", "soup", "sandwich", "burger", "taco",
                "wrap", "bowl", "side", "dessert", "drink", "beverage",
                "breakfast", "lunch", "dinner", "brunch", "special",
                "seafood", "chicken", "beef", "pork", "vegetarian",
                "kids", "combo", "platter", "shareables", "wings",
                "korean fried", "fried chicken", "noodle", "rice dish",
                "korean dish", "bread", "sauce", "dip",
            }

            current_category = None
            i = 0
            while i < len(lines):
                line = lines[i]
                line_lower = line.lower()

                # Skip boilerplate
                if any(phrase in line_lower for phrase in SKIP_PHRASES):
                    i += 1
                    continue

                # Detect category headings:
                # - ALL CAPS and looks like a food category section name
                # - Short title-case with a known category keyword
                is_category = False
                if line.isupper() and 3 < len(line) < 50:
                    # ALL CAPS lines are usually headings — but verify they're plausible
                    # categories and not just single food items in caps
                    if any(kw in line_lower for kw in CATEGORY_KEYWORDS) or len(line.split()) >= 2:
                        is_category = True
                elif (len(line) < 40 and any(kw in line_lower for kw in CATEGORY_KEYWORDS)):
                    is_category = True

                if is_category:
                    candidate = line.title() if line.isupper() else line
                    if candidate not in ("Menu", "Items", "Order"):
                        current_category = candidate
                    i += 1
                    continue

                # Detect menu items: moderate-length lines that look like food names
                # followed optionally by a longer description line
                if (3 < len(line) < 80
                        and not line.startswith("http")
                        and not line.startswith("©")
                        and current_category):
                    # Check if next line is a description (longer, not a heading)
                    description = None
                    if i + 1 < len(lines):
                        next_line = lines[i + 1]
                        if (len(next_line) > 25 and not next_line.isupper()
                                and not any(p in next_line.lower() for p in SKIP_PHRASES)):
                            description = next_line[:200]
                            i += 1  # consume the description line

                    # Extract price if embedded in the line
                    price = self.parse_price(line)
                    price_text = None
                    name = line
                    price_match = PRICE_PATTERN.search(line)
                    if price_match:
                        price_text = price_match.group()
                        name = line[:price_match.start()].strip().rstrip("-–—·.")

                    if name and len(name) > 2:
                        items.append(MenuItem(
                            name=self.clean_text(name),
                            description=self.clean_text(description) if description else None,
                            price=price,
                            price_text=price_text,
                            category=current_category,
                            dietary_tags=self.detect_dietary_tags(
                                f"{name} {description or ''}"
                            ),
                        ))
                i += 1

            # Filter: if we got too many items (>100) it's probably noise
            if len(items) > 100:
                items = items[:100]

        except Exception as e:
            logger.debug(f"Text-based extraction failed: {e}")

        return items

    async def _llm_extract(self, page_text: str, url: str) -> list[MenuItem]:
        """
        Optional: use an LLM to extract structured menu data from raw page text.
        Requires OPENAI_API_KEY or ANTHROPIC_API_KEY in environment.
        """
        # Truncate to avoid token limits
        text = page_text[:8000]
        try:
            import anthropic
            client = anthropic.Anthropic()
            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=4096,
                messages=[{
                    "role": "user",
                    "content": f"""Extract all menu items from this restaurant page text.
Return a JSON array where each item has: name, description, price (number or null), category, dietary_tags (array).
Only return the JSON array, nothing else.

Page text:
{text}"""
                }]
            )
            import json
            content = response.content[0].text.strip()
            # Strip markdown code fences if present
            if content.startswith("```"):
                content = content.split("\n", 1)[1].rsplit("```", 1)[0]
            data = json.loads(content)
            return [
                MenuItem(
                    name=self.clean_text(item.get("name", "")),
                    description=self.clean_text(item.get("description", "")),
                    price=item.get("price"),
                    price_text=f"${item['price']}" if item.get("price") else None,
                    category=item.get("category"),
                    dietary_tags=item.get("dietary_tags", []),
                )
                for item in data if item.get("name")
            ]
        except ImportError:
            logger.info("anthropic package not installed — skipping LLM extraction")
        except Exception as e:
            logger.warning(f"LLM extraction failed: {e}")
        return []
