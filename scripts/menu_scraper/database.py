"""
SQLite database layer for storing scraped restaurant menu data.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional


DB_PATH = Path(__file__).parent / "menus.db"


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    """Get a database connection with row factory enabled."""
    conn = sqlite3.connect(db_path or str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: Optional[str] = None):
    """Initialize the database schema."""
    conn = get_connection(db_path)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS restaurants (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            address TEXT,
            phone TEXT,
            website TEXT,
            cuisine_type TEXT,
            source TEXT NOT NULL,          -- 'website', 'yelp', 'doordash', 'google', etc.
            source_url TEXT,
            latitude REAL,
            longitude REAL,
            rating REAL,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            UNIQUE(name, address)
        );

        CREATE TABLE IF NOT EXISTS menu_categories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            description TEXT,
            sort_order INTEGER DEFAULT 0,
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE,
            UNIQUE(restaurant_id, name)
        );

        CREATE TABLE IF NOT EXISTS menu_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER NOT NULL,
            category_id INTEGER,
            name TEXT NOT NULL,
            description TEXT,
            price REAL,
            price_text TEXT,               -- original price string, e.g. "$12.99" or "Market Price"
            image_url TEXT,
            dietary_tags TEXT,             -- JSON array: ["vegetarian", "gluten-free", ...]
            is_available INTEGER DEFAULT 1,
            created_at TEXT DEFAULT (datetime('now')),
            updated_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE CASCADE,
            FOREIGN KEY (category_id) REFERENCES menu_categories(id) ON DELETE SET NULL
        );

        CREATE TABLE IF NOT EXISTS scrape_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            restaurant_id INTEGER,
            source TEXT NOT NULL,
            source_url TEXT,
            status TEXT NOT NULL,          -- 'success', 'partial', 'failed'
            items_found INTEGER DEFAULT 0,
            error_message TEXT,
            duration_seconds REAL,
            scraped_at TEXT DEFAULT (datetime('now')),
            FOREIGN KEY (restaurant_id) REFERENCES restaurants(id) ON DELETE SET NULL
        );

        CREATE INDEX IF NOT EXISTS idx_menu_items_restaurant ON menu_items(restaurant_id);
        CREATE INDEX IF NOT EXISTS idx_menu_items_category ON menu_items(category_id);
        CREATE INDEX IF NOT EXISTS idx_menu_categories_restaurant ON menu_categories(restaurant_id);
        CREATE INDEX IF NOT EXISTS idx_scrape_logs_restaurant ON scrape_logs(restaurant_id);
    """)
    conn.commit()
    conn.close()


def upsert_restaurant(conn: sqlite3.Connection, data: dict) -> int:
    """Insert or update a restaurant. Returns the restaurant ID."""
    cursor = conn.execute("""
        INSERT INTO restaurants (name, address, phone, website, cuisine_type, source, source_url, latitude, longitude, rating)
        VALUES (:name, :address, :phone, :website, :cuisine_type, :source, :source_url, :latitude, :longitude, :rating)
        ON CONFLICT(name, address) DO UPDATE SET
            phone = COALESCE(excluded.phone, restaurants.phone),
            website = COALESCE(excluded.website, restaurants.website),
            cuisine_type = COALESCE(excluded.cuisine_type, restaurants.cuisine_type),
            source = excluded.source,
            source_url = COALESCE(excluded.source_url, restaurants.source_url),
            latitude = COALESCE(excluded.latitude, restaurants.latitude),
            longitude = COALESCE(excluded.longitude, restaurants.longitude),
            rating = COALESCE(excluded.rating, restaurants.rating),
            updated_at = datetime('now')
    """, {
        "name": data.get("name", "Unknown"),
        "address": data.get("address"),
        "phone": data.get("phone"),
        "website": data.get("website"),
        "cuisine_type": data.get("cuisine_type"),
        "source": data.get("source", "unknown"),
        "source_url": data.get("source_url"),
        "latitude": data.get("latitude"),
        "longitude": data.get("longitude"),
        "rating": data.get("rating"),
    })
    conn.commit()
    return cursor.lastrowid or conn.execute(
        "SELECT id FROM restaurants WHERE name = ? AND address IS ?",
        (data["name"], data.get("address"))
    ).fetchone()["id"]


def upsert_category(conn: sqlite3.Connection, restaurant_id: int, name: str, description: str = None, sort_order: int = 0) -> int:
    """Insert or update a menu category. Returns the category ID."""
    cursor = conn.execute("""
        INSERT INTO menu_categories (restaurant_id, name, description, sort_order)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(restaurant_id, name) DO UPDATE SET
            description = COALESCE(excluded.description, menu_categories.description),
            sort_order = excluded.sort_order
    """, (restaurant_id, name, description, sort_order))
    conn.commit()
    return cursor.lastrowid or conn.execute(
        "SELECT id FROM menu_categories WHERE restaurant_id = ? AND name = ?",
        (restaurant_id, name)
    ).fetchone()["id"]


def insert_menu_item(conn: sqlite3.Connection, item: dict) -> int:
    """Insert a menu item. Returns the item ID."""
    dietary_tags = item.get("dietary_tags")
    if isinstance(dietary_tags, list):
        dietary_tags = json.dumps(dietary_tags)

    cursor = conn.execute("""
        INSERT INTO menu_items (restaurant_id, category_id, name, description, price, price_text, image_url, dietary_tags, is_available)
        VALUES (:restaurant_id, :category_id, :name, :description, :price, :price_text, :image_url, :dietary_tags, :is_available)
    """, {
        "restaurant_id": item["restaurant_id"],
        "category_id": item.get("category_id"),
        "name": item["name"],
        "description": item.get("description"),
        "price": item.get("price"),
        "price_text": item.get("price_text"),
        "image_url": item.get("image_url"),
        "dietary_tags": dietary_tags,
        "is_available": item.get("is_available", 1),
    })
    conn.commit()
    return cursor.lastrowid


def log_scrape(conn: sqlite3.Connection, restaurant_id: int, source: str, source_url: str,
               status: str, items_found: int = 0, error_message: str = None, duration: float = None):
    """Log a scrape attempt."""
    conn.execute("""
        INSERT INTO scrape_logs (restaurant_id, source, source_url, status, items_found, error_message, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (restaurant_id, source, source_url, status, items_found, error_message, duration))
    conn.commit()


def get_restaurant_menu(conn: sqlite3.Connection, restaurant_id: int) -> dict:
    """Get a full restaurant menu as a structured dict."""
    restaurant = conn.execute("SELECT * FROM restaurants WHERE id = ?", (restaurant_id,)).fetchone()
    if not restaurant:
        return None

    categories = conn.execute(
        "SELECT * FROM menu_categories WHERE restaurant_id = ? ORDER BY sort_order", (restaurant_id,)
    ).fetchall()

    items = conn.execute(
        "SELECT * FROM menu_items WHERE restaurant_id = ? ORDER BY category_id, name", (restaurant_id,)
    ).fetchall()

    result = dict(restaurant)
    result["categories"] = []
    cat_map = {}

    for cat in categories:
        cat_dict = dict(cat)
        cat_dict["items"] = []
        cat_map[cat["id"]] = cat_dict
        result["categories"].append(cat_dict)

    uncategorized = {"id": None, "name": "Uncategorized", "items": []}
    for item in items:
        item_dict = dict(item)
        if item_dict.get("dietary_tags"):
            try:
                item_dict["dietary_tags"] = json.loads(item_dict["dietary_tags"])
            except json.JSONDecodeError:
                pass
        if item["category_id"] and item["category_id"] in cat_map:
            cat_map[item["category_id"]]["items"].append(item_dict)
        else:
            uncategorized["items"].append(item_dict)

    if uncategorized["items"]:
        result["categories"].append(uncategorized)

    return result


def search_items(conn: sqlite3.Connection, query: str) -> list:
    """Search menu items by name or description."""
    rows = conn.execute("""
        SELECT mi.*, r.name as restaurant_name, mc.name as category_name
        FROM menu_items mi
        JOIN restaurants r ON mi.restaurant_id = r.id
        LEFT JOIN menu_categories mc ON mi.category_id = mc.id
        WHERE mi.name LIKE ? OR mi.description LIKE ?
        ORDER BY r.name, mi.name
    """, (f"%{query}%", f"%{query}%")).fetchall()
    return [dict(r) for r in rows]
