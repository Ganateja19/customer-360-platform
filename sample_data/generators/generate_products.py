"""
Product Catalog Generator
==========================
Generates realistic product catalog CSV.

Usage:
    python generate_products.py --count 200 --output ../output/products.csv
"""

import argparse
import csv
import os
import random
import logging
from datetime import datetime

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

CATEGORIES = {
    "electronics": {
        "subcategories": ["smartphones", "laptops", "headphones", "tablets", "cameras", "smart_home"],
        "price_range": (29.99, 1999.99),
        "weight_range": (0.1, 5.0),
    },
    "clothing": {
        "subcategories": ["mens_shirts", "womens_dresses", "shoes", "outerwear", "accessories"],
        "price_range": (14.99, 299.99),
        "weight_range": (0.1, 2.0),
    },
    "home_garden": {
        "subcategories": ["furniture", "kitchen", "bedding", "decor", "garden_tools"],
        "price_range": (9.99, 999.99),
        "weight_range": (0.5, 30.0),
    },
    "sports": {
        "subcategories": ["fitness", "outdoor", "team_sports", "water_sports", "yoga"],
        "price_range": (9.99, 499.99),
        "weight_range": (0.2, 15.0),
    },
    "books_media": {
        "subcategories": ["fiction", "non_fiction", "textbooks", "audiobooks", "ebooks"],
        "price_range": (4.99, 79.99),
        "weight_range": (0.1, 2.0),
    },
}

BRANDS = [
    "TechVibe", "UrbanPulse", "AquaFlow", "EcoStar", "NovaPeak",
    "BlueHorizon", "GreenLeaf", "SwiftEdge", "PrimeCraft", "SunRise",
    "CloudNine", "IronClad", "FreshStart", "ZenCore", "BoldStep"
]


def generate_products(count: int, output_path: str) -> None:
    """Generate realistic product catalog records."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        "product_id", "product_name", "category", "subcategory", "brand",
        "price", "cost", "weight_kg", "is_active", "created_date"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, count + 1):
            category = random.choice(list(CATEGORIES.keys()))
            cat_info = CATEGORIES[category]
            subcategory = random.choice(cat_info["subcategories"])
            price = round(random.uniform(*cat_info["price_range"]), 2)
            margin = random.uniform(0.25, 0.65)

            writer.writerow({
                "product_id": f"PROD-{i:04d}",
                "product_name": f"{random.choice(BRANDS)} {fake.word().title()} {subcategory.replace('_', ' ').title()}",
                "category": category,
                "subcategory": subcategory,
                "brand": random.choice(BRANDS),
                "price": price,
                "cost": round(price * (1 - margin), 2),
                "weight_kg": round(random.uniform(*cat_info["weight_range"]), 2),
                "is_active": random.choices([True, False], weights=[90, 10])[0],
                "created_date": fake.date_between(start_date="-3y", end_date="today").strftime("%Y-%m-%d"),
            })

    logger.info("Generated %d products → %s", count, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate product data")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--output", default=os.path.join(
        os.path.dirname(__file__), "..", "output", "products.csv"))
    args = parser.parse_args()
    generate_products(args.count, args.output)
