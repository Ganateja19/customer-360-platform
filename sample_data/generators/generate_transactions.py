"""
Transaction Data Generator
============================
Generates realistic transaction records with proper foreign keys.

Usage:
    python generate_transactions.py --count 50000 --output ../output/transactions.csv
"""

import argparse
import csv
import os
import random
import uuid
import logging
from datetime import datetime, timedelta

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "apple_pay", "google_pay"]
PAYMENT_WEIGHTS = [40, 25, 15, 12, 8]
CHANNELS = ["web", "mobile_app", "tablet", "in_store", "phone"]
CHANNEL_WEIGHTS = [35, 30, 10, 20, 5]


def generate_transactions(
    count: int,
    num_customers: int,
    num_products: int,
    output_path: str
) -> None:
    """Generate realistic transaction records."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    customer_ids = [f"CUST-{i:06d}" for i in range(1, num_customers + 1)]
    product_ids = [f"PROD-{i:04d}" for i in range(1, num_products + 1)]

    # Simulate product price lookup
    product_prices = {pid: round(random.uniform(9.99, 499.99), 2) for pid in product_ids}

    fieldnames = [
        "transaction_id", "customer_id", "product_id", "transaction_date",
        "quantity", "unit_price", "discount_amount", "total_amount",
        "payment_method", "channel", "store_id"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(count):
            customer_id = random.choice(customer_ids)
            product_id = random.choice(product_ids)
            unit_price = product_prices[product_id]
            quantity = random.choices(
                [1, 2, 3, 4, 5],
                weights=[50, 25, 12, 8, 5],
                k=1
            )[0]

            # Random discount (30% chance)
            if random.random() < 0.3:
                discount_pct = random.choice([5, 10, 15, 20, 25])
                discount = round(unit_price * quantity * discount_pct / 100, 2)
            else:
                discount = 0.0

            total = round(unit_price * quantity - discount, 2)
            channel = random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0]

            tx_date = fake.date_time_between(
                start_date="-1y", end_date="now"
            )

            writer.writerow({
                "transaction_id": str(uuid.uuid4()),
                "customer_id": customer_id,
                "product_id": product_id,
                "transaction_date": tx_date.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity": quantity,
                "unit_price": unit_price,
                "discount_amount": discount,
                "total_amount": total,
                "payment_method": random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0],
                "channel": channel,
                "store_id": f"STORE-{random.randint(1,50):03d}" if channel == "in_store" else ""
            })

            if (i + 1) % 10000 == 0:
                logger.info("Generated %d / %d transactions", i + 1, count)

    logger.info("Generated %d transactions → %s", count, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate transaction data")
    parser.add_argument("--count", type=int, default=50000)
    parser.add_argument("--customers", type=int, default=5000)
    parser.add_argument("--products", type=int, default=200)
    parser.add_argument("--output", default=os.path.join(
        os.path.dirname(__file__), "..", "output", "transactions.csv"))
    args = parser.parse_args()
    generate_transactions(args.count, args.customers, args.products, args.output)
