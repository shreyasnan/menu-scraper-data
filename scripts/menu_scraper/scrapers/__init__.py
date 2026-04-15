"""Scraper modules for different restaurant data sources."""

from .base import BaseScraper, ScrapeResult
from .website import WebsiteScraper
from .yelp import YelpScraper
from .google import GoogleScraper

__all__ = ["BaseScraper", "ScrapeResult", "WebsiteScraper", "YelpScraper", "GoogleScraper"]
