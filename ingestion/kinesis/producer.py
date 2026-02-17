"""
Kinesis Clickstream Event Producer
===================================
Simulates real-time customer clickstream and transaction events,
publishing them to an Amazon Kinesis Data Stream.

Usage:
    python producer.py --stream-name c360-clickstream-events --rate 10 --duration 60
"""

import json
import time
import uuid
import random
import argparse
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List

import boto3
from botocore.exceptions import ClientError

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ── Event Templates ──────────────────────────────────────────

EVENT_TYPES = ["page_view", "add_to_cart", "remove_from_cart", "purchase", "search", "wishlist_add"]

PAGES = [
    "/home", "/products", "/products/electronics", "/products/clothing",
    "/products/home-garden", "/cart", "/checkout", "/account",
    "/search", "/deals", "/product-detail"
]

SEARCH_TERMS = [
    "wireless headphones", "running shoes", "laptop stand", "yoga mat",
    "coffee maker", "bluetooth speaker", "desk lamp", "water bottle",
    "backpack", "phone case", "smart watch", "air purifier"
]

PRODUCT_IDS = [f"PROD-{i:04d}" for i in range(1, 201)]
CUSTOMER_IDS = [f"CUST-{i:06d}" for i in range(1, 5001)]
SESSION_POOL: Dict[str, str] = {}  # customer_id → session_id

CHANNELS = ["web", "mobile_app", "tablet"]
DEVICES = ["desktop", "iphone", "android", "ipad", "macbook"]
BROWSERS = ["chrome", "safari", "firefox", "edge"]
REGIONS = ["us-east", "us-west", "eu-west", "ap-southeast", "sa-east"]


def _get_session(customer_id: str) -> str:
    """Return or create a session for a customer (sessions expire randomly)."""
    if customer_id not in SESSION_POOL or random.random() < 0.05:
        SESSION_POOL[customer_id] = str(uuid.uuid4())
    return SESSION_POOL[customer_id]


def generate_event() -> Dict[str, Any]:
    """Generate a single realistic clickstream / transaction event."""
    customer_id = random.choice(CUSTOMER_IDS)
    event_type = random.choices(
        EVENT_TYPES,
        weights=[40, 20, 5, 10, 20, 5],  # realistic distribution
        k=1
    )[0]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "customer_id": customer_id,
        "session_id": _get_session(customer_id),
        "channel": random.choice(CHANNELS),
        "device": random.choice(DEVICES),
        "browser": random.choice(BROWSERS),
        "region": random.choice(REGIONS),
        "ip_address": f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
    }

    # Event-specific payloads
    if event_type == "page_view":
        event["page_url"] = random.choice(PAGES)
        event["referrer"] = random.choice(["google", "direct", "facebook", "email", "instagram", ""])
        event["time_on_page_seconds"] = random.randint(3, 300)

    elif event_type in ("add_to_cart", "remove_from_cart", "wishlist_add"):
        event["product_id"] = random.choice(PRODUCT_IDS)
        event["quantity"] = random.randint(1, 5)
        event["unit_price"] = round(random.uniform(9.99, 499.99), 2)

    elif event_type == "purchase":
        num_items = random.randint(1, 6)
        items = []
        total = 0.0
        for _ in range(num_items):
            price = round(random.uniform(9.99, 499.99), 2)
            qty = random.randint(1, 3)
            items.append({
                "product_id": random.choice(PRODUCT_IDS),
                "quantity": qty,
                "unit_price": price,
                "subtotal": round(price * qty, 2)
            })
            total += price * qty
        event["items"] = items
        event["order_total"] = round(total, 2)
        event["payment_method"] = random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"])
        event["shipping_method"] = random.choice(["standard", "express", "next_day", "pickup"])

    elif event_type == "search":
        event["search_term"] = random.choice(SEARCH_TERMS)
        event["results_count"] = random.randint(0, 500)
        event["results_clicked"] = random.randint(0, min(10, event["results_count"]))

    return event


def publish_events(
    stream_name: str,
    region: str,
    rate: int,
    duration: int
) -> None:
    """
    Publish events to Kinesis at a given rate (events/sec) for a duration (seconds).

    Parameters
    ----------
    stream_name : str
        Kinesis Data Stream name.
    region : str
        AWS region.
    rate : int
        Target events per second.
    duration : int
        Total seconds to produce events.
    """
    kinesis = boto3.client("kinesis", region_name=region)
    total_sent = 0
    total_errors = 0
    start = time.time()

    logger.info(
        "Starting producer → stream=%s  rate=%d/s  duration=%ds",
        stream_name, rate, duration
    )

    while time.time() - start < duration:
        batch_start = time.time()

        # Build batch (Kinesis PutRecords supports up to 500)
        batch_size = min(rate, 500)
        records: List[Dict[str, Any]] = []
        for _ in range(batch_size):
            event = generate_event()
            records.append({
                "Data": json.dumps(event).encode("utf-8"),
                "PartitionKey": event["customer_id"]  # partition by customer
            })

        try:
            response = kinesis.put_records(
                StreamName=stream_name,
                Records=records
            )
            failed = response.get("FailedRecordCount", 0)
            total_sent += batch_size - failed
            total_errors += failed

            if failed:
                logger.warning("Batch had %d failed records", failed)
            else:
                logger.info("Sent %d events  (total: %d)", batch_size, total_sent)

        except ClientError as e:
            logger.error("Kinesis PutRecords error: %s", e)
            total_errors += batch_size

        # Throttle to maintain target rate
        elapsed = time.time() - batch_start
        sleep_time = max(0, 1.0 - elapsed)
        if sleep_time > 0:
            time.sleep(sleep_time)

    logger.info(
        "Producer finished — sent=%d  errors=%d  elapsed=%.1fs",
        total_sent, total_errors, time.time() - start
    )


# ── CLI ──────────────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kinesis clickstream event producer")
    parser.add_argument("--stream-name", default="c360-clickstream-events", help="Kinesis stream name")
    parser.add_argument("--region", default="us-east-1", help="AWS region")
    parser.add_argument("--rate", type=int, default=10, help="Events per second")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    args = parser.parse_args()

    publish_events(args.stream_name, args.region, args.rate, args.duration)
