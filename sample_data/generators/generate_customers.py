"""
Customer Data Generator
========================
Generates realistic customer master data CSV using Faker.

Usage:
    python generate_customers.py --count 5000 --output ../output/customers.csv
"""

import argparse
import csv
import os
import random
import logging
from datetime import datetime, timedelta

from faker import Faker

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

fake = Faker()
Faker.seed(42)
random.seed(42)

SEGMENTS = ["premium", "standard", "basic"]
SEGMENT_WEIGHTS = [15, 55, 30]


def generate_customers(count: int, output_path: str) -> None:
    """Generate realistic customer records."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    fieldnames = [
        "customer_id", "first_name", "last_name", "email", "phone",
        "date_of_birth", "gender", "address_street", "address_city",
        "address_state", "address_zip", "address_country",
        "registration_date", "customer_segment", "lifetime_value"
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for i in range(1, count + 1):
            segment = random.choices(SEGMENTS, weights=SEGMENT_WEIGHTS, k=1)[0]
            ltv_ranges = {"premium": (2000, 50000), "standard": (200, 3000), "basic": (10, 500)}
            ltv_min, ltv_max = ltv_ranges[segment]

            reg_date = fake.date_between(start_date="-5y", end_date="today")
            dob = fake.date_of_birth(minimum_age=18, maximum_age=85)

            writer.writerow({
                "customer_id": f"CUST-{i:06d}",
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "date_of_birth": dob.strftime("%Y-%m-%d"),
                "gender": random.choice(["M", "F", "Other", "Prefer not to say"]),
                "address_street": fake.street_address(),
                "address_city": fake.city(),
                "address_state": fake.state_abbr(),
                "address_zip": fake.zipcode(),
                "address_country": "US",
                "registration_date": reg_date.strftime("%Y-%m-%d"),
                "customer_segment": segment,
                "lifetime_value": round(random.uniform(ltv_min, ltv_max), 2)
            })

            if i % 1000 == 0:
                logger.info("Generated %d / %d customers", i, count)

    logger.info("Generated %d customers → %s", count, output_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate customer data")
    parser.add_argument("--count", type=int, default=5000)
    parser.add_argument("--output", default=os.path.join(
        os.path.dirname(__file__), "..", "output", "customers.csv"))
    args = parser.parse_args()
    generate_customers(args.count, args.output)
