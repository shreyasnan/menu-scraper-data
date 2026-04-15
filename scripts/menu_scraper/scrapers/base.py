"""
Base scraper class that all source-specific scrapers inherit from.
"""

import re
import time
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

logger = logging.getLogger(__name__)


@dataclass
class MenuItem:
    """A single menu item."""
    name: str
    description: Optional[str] = None
    price: Optional[float] = None
    price_text: Optional[str] = None
    category: Optional[str] = None
    image_url: Optional[str] = None
    dietary_tags: list = field(default_factory=list)
    is_available: bool = True


@dataclass
class ScrapeResult:
    """Result of a scrape operation."""
    restaurant_name: str
    source: str
    source_url: str
    address: Optional[str] = None
    phone: Optional[str] = None
    website: Optional[str] = None
    cuisine_type: Optional[str] = None
    rating: Optional[float] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    items: list = field(default_factory=list)       # list of MenuItem
    categories: list = field(default_factory=list)   # list of category names found
    status: str = "success"
    error_message: Optional[str] = None
    duration_seconds: float = 0.0


class BaseScraper(ABC):
    """Abstract base class for menu scrapers."""

    source_name: str = "unknown"

    def __init__(self, headless: bool = True, timeout: int = 30):
        self.headless = headless
        self.timeout = timeout
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")

    @abstractmethod
    async def scrape(self, url_or_query: str) -> ScrapeResult:
        """Scrape menu data from the given URL or search query."""
        ...

    @abstractmethod
    async def search(self, restaurant_name: str, location: str = "") -> list[ScrapeResult]:
        """Search for a restaurant and scrape its menu."""
        ...

    @staticmethod
    def parse_price(text: str) -> Optional[float]:
        """Extract a numeric price from a string like '$12.99' or '12.99 USD'."""
        if not text:
            return None
        match = re.search(r'[\$£€]?\s*(\d+(?:[.,]\d{1,2})?)', text.strip())
        if match:
            price_str = match.group(1).replace(',', '.')
            try:
                return float(price_str)
            except ValueError:
                return None
        return None

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean whitespace and special characters from text."""
        if not text:
            return ""
        return re.sub(r'\s+', ' ', text.strip())

    @staticmethod
    def detect_dietary_tags(text: str) -> list[str]:
        """Detect common dietary tags from item name/description."""
        if not text:
            return []
        text_lower = text.lower()
        tags = []
        patterns = {
            "vegetarian": r'\b(vegetarian|veggie|veg)\b',
            "vegan": r'\bvegan\b',
            "gluten-free": r'\b(gluten[\s-]?free|gf)\b',
            "spicy": r'\b(spicy|hot|🌶)\b',
            "contains-nuts": r'\b(nuts?|peanut|almond|walnut|cashew)\b',
            "dairy-free": r'\bdairy[\s-]?free\b',
            "halal": r'\bhalal\b',
            "kosher": r'\bkosher\b',
            "organic": r'\borganic\b',
        }
        for tag, pattern in patterns.items():
            if re.search(pattern, text_lower):
                tags.append(tag)
        return tags
