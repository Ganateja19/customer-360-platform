"""
Clickstream Data Generator
============================
Generates realistic clickstream events for local testing.

Usage:
    python generate_clickstream.py --count 100000 --output ../output/clickstream.json
"""

import argparse
import json
import os
import random
import uuid
import logging
from datetime import datetime, timedelta, timezone

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist_add"]
EVENT_WEIGHTS = [40, 20, 5, 10, 20, 5]
PAGES = ["/home", "/products", "/products/electronics", "/products/clothing",
         "/cart", "/checkout", "/account", "/search", "/deals"]
CHANNELS = ["web", "mobile_app", "tablet"]
DEVICES = ["desktop", "iphone", "android", "ipad", "macbook"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast"]
SEARCH_TERMS = ["wireless headphones", "running shoes", "laptop stand", "yoga mat",
                "coffee maker", "bluetooth speaker", "phone case", "smart watch"]


def generate_clickstream(count: int, num_customers: int, output_path: str) -> None:
    """Generate clickstream events as newline-delimited JSON."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    customer_ids = [f"CUST-{i:06d}" for i in range(1, num_customers + 1)]
    sessions = {}

    with open(output_path, "w", encoding="utf-8") as f:
        for i in range(count):
            cust = random.choice(customer_ids)
            if cust not in sessions or random.random() < 0.03:
                sessions[cust] = str(uuid.uuid4())

            event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
            ts = fake.date_time_between(start_date="-30d", end_date="now", tzinfo=timezone.utc)

            event = {
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "event_timestamp": ts.isoformat(),
                "customer_id": cust,
                "session_id": sessions[cust],
                "channel": random.choice(CHANNELS),
                "device": random.choice(DEVICES),
                "browser": random.choice(BROWSERS),
                "region": random.choice(REGIONS),
            }

            if event_type == "page_view":
                event["page_url"] = random.choice(PAGES)
            elif event_type in ("add_to_cart", "remove_from_cart"):
                event["product_id"] = f"PROD-{random.randint(1, 200):04d}"
                event["quantity"] = random.randint(1, 3)
                event["unit_price"] = round(random.uniform(9.99, 499.99), 2)
            elif event_type == "purchase":
                event["order_total"] = round(random.uniform(15.0, 1500.0), 2)
                event["payment_method"] = random.choice(["credit_card", "debit_card", "paypal"])
            elif event_type == "search":
                event["search_term"] = random.choice(SEARCH_TERMS)
                event["results_count"] = random.randint(0, 500)

            f.write(json.dumps(event) + "\n")

            if (i + 1) % 25000 == 0:
                logger.info("Generated %d / %d events", i + 1, count)

    logger.info("Generated %d clickstream events → %s", count, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate clickstream data")
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--output", default=os.path.join(
        os.path.dirname(__file__), "..", "output", "clickstream.json"))
    args = parser.parse_args()
    generate_clickstream(args.count, args.customers, args.output)
