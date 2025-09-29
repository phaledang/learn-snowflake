"""Seed a richer synthetic dataset into Snowflake `sample_data` table for Lab05.

Features of generated data:
- Adjustable row volume (default 2,500)
- Multiple product categories & dynamic price bands
- Regional + seasonal variation (monthly trend shaping)
- Repeat customers with variable order counts (power-law distribution)
- Introduces occasional high-value outliers
- Deterministic option via --seed

Usage (after activating lab05 venv and configuring .env credentials):

    python python/seed_sample_data.py --rows 5000 --year 2024 --seed 42

Data Model Columns:
  customer_id        INT
  customer_name      STRING
  email              STRING
  region             STRING
  product_category   STRING
  purchase_date      TIMESTAMP_NTZ
  amount             NUMBER(10,2)
  channel            STRING (web, mobile, store, partner)
  payment_type       STRING (card, ach, wallet, cash)
  quantity           INT
  discount_pct       FLOAT
  returned_flag      BOOLEAN

Creates table `sample_data` (replacing if exists) then bulk inserts via pandas to_sql.
"""
from __future__ import annotations

import argparse
import math
import os
import random
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from pandas_integration import SnowflakeDataAnalyzer

# ----------------------------- Configuration -------------------------------- #
PRODUCT_CATEGORIES = {
    "Electronics": (80, 600),
    "Books": (8, 45),
    "Home": (15, 180),
    "Sports": (10, 220),
    "Clothing": (12, 150),
    "Grocery": (3, 60),
    "Toys": (5, 90),
    "Health": (6, 130),
}
REGIONS = ["North", "South", "East", "West", "Central"]
CHANNELS = ["web", "mobile", "store", "partner"]
PAYMENT_TYPES = ["card", "ach", "wallet", "cash"]

FIRST_NAMES = [
    "Ava", "Liam", "Emma", "Noah", "Olivia", "Ethan", "Sophia", "Mason",
    "Isabella", "Logan", "Mia", "Lucas", "Charlotte", "Amelia", "Harper",
]
LAST_NAMES = [
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller",
    "Davis", "Wilson", "Taylor", "Anderson", "Thomas", "Jackson", "White",
]
EMAIL_DOMAINS = ["example.com", "mail.com", "sample.org", "demo.io"]

# Seasonal multipliers by month (simulate peaks in Nov/Dec, slump in Feb)
MONTH_SEASONALITY = {
    1: 0.95,  2: 0.80, 3: 0.90, 4: 1.00, 5: 1.05, 6: 1.10,
    7: 1.08, 8: 1.02, 9: 0.97, 10: 1.15, 11: 1.40, 12: 1.55,
}

# Region revenue weighting (simulate distribution skew)
REGION_WEIGHTS = {
    "North": 1.10,
    "South": 0.95,
    "East": 1.20,
    "West": 1.00,
    "Central": 0.85,
}

# Category base popularity weights
CATEGORY_WEIGHTS = {
    "Electronics": 1.30,
    "Books": 0.90,
    "Home": 1.00,
    "Sports": 0.95,
    "Clothing": 1.05,
    "Grocery": 1.15,
    "Toys": 0.80,
    "Health": 0.88,
}

# Probability a row will be a high-value outlier multiplier on amount
OUTLIER_PROB = 0.01
OUTLIER_MULTIPLIER_RANGE = (3.0, 6.5)

RETURN_RATE = 0.04  # 4% of orders flagged as returned
DISCOUNT_PROB = 0.30

# --------------------------- Helper Functions -------------------------------- #

def _rng_choice_weighted(items, weights):
    total = sum(weights)
    r = random.uniform(0, total)
    upto = 0
    for item, w in zip(items, weights):
        if upto + w >= r:
            return item
        upto += w
    return items[-1]


def generate_customers(n_customers: int) -> pd.DataFrame:
    # Use a power-law like distribution for purchase propensity
    ids = np.arange(1, n_customers + 1)
    propensity = np.random.zipf(2.0, size=n_customers).astype(float)
    propensity = propensity / propensity.max()

    names = [
        f"{random.choice(FIRST_NAMES)} {random.choice(LAST_NAMES)}"
        for _ in ids
    ]
    emails = [
        name.lower().replace(" ", ".") + f"@{random.choice(EMAIL_DOMAINS)}"
        for name in names
    ]

    df = pd.DataFrame(
        {
            "customer_id": ids,
            "customer_name": names,
            "email": emails,
            "purchase_propensity": propensity,
        }
    )
    return df


def generate_orders(
    customers: pd.DataFrame,
    year: int,
    total_rows: int,
) -> pd.DataFrame:
    rows = []
    # Precompute weights arrays
    category_list = list(PRODUCT_CATEGORIES.keys())
    category_weights = [CATEGORY_WEIGHTS[c] for c in category_list]
    region_weights = [REGION_WEIGHTS[r] for r in REGIONS]

    # Normalize weights for random.choices usage
    category_weights_norm = np.array(category_weights) / sum(category_weights)
    region_weights_norm = np.array(region_weights) / sum(region_weights)

    # Use propensity as another sampling distribution for customers
    cust_ids = customers["customer_id"].values
    cust_weights = customers["purchase_propensity"].values
    cust_weights = cust_weights / cust_weights.sum()

    for _ in range(total_rows):
        cust_id = np.random.choice(cust_ids, p=cust_weights)
        region = np.random.choice(REGIONS, p=region_weights_norm)
        category = np.random.choice(category_list, p=category_weights_norm)

        base_low, base_high = PRODUCT_CATEGORIES[category]
        amount = random.uniform(base_low, base_high)

        # Seasonal & regional adjustment
        month = random.randint(1, 12)
        season_factor = MONTH_SEASONALITY[month]
        region_factor = REGION_WEIGHTS[region]
        amount *= season_factor * region_factor

        # Potential discount
        discount_pct = 0.0
        if random.random() < DISCOUNT_PROB:
            discount_pct = random.choice([0.05, 0.10, 0.15, 0.20])
            amount *= (1 - discount_pct)

        # Outlier bump
        if random.random() < OUTLIER_PROB:
            amount *= random.uniform(*OUTLIER_MULTIPLIER_RANGE)

        quantity = np.random.choice([1, 1, 1, 2, 2, 3, 5])  # Weighted small basket
        amount = round(amount * quantity, 2)

        payment_type = random.choice(PAYMENT_TYPES)
        channel = random.choice(CHANNELS)
        returned_flag = random.random() < RETURN_RATE

        # Random day & time in month
        day = random.randint(1, 28)  # avoid month-end complexity
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        purchase_date = datetime(year, month, day, hour, minute)

        rows.append(
            (
                cust_id,
                region,
                category,
                purchase_date,
                amount,
                channel,
                payment_type,
                quantity,
                discount_pct,
                returned_flag,
            )
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "customer_id",
            "region",
            "product_category",
            "purchase_date",
            "amount",
            "channel",
            "payment_type",
            "quantity",
            "discount_pct",
            "returned_flag",
        ],
    )
    return df


def assemble_dataset(
    total_rows: int, year: int, seed: int | None = None, est_customers: int | None = None
) -> pd.DataFrame:
    if seed is not None:
        random.seed(seed)
        np.random.seed(seed)

    if est_customers is None:
        # Heuristic: ~1 customer per 6 orders, clamp to minimum 50
        est_customers = max(50, total_rows // 6)

    customers = generate_customers(est_customers)
    orders = generate_orders(customers, year, total_rows)

    # Join to add names/emails
    df = orders.merge(customers, on="customer_id", how="left")
    # Reorder columns
    df = df[
        [
            "customer_id",
            "customer_name",
            "email",
            "region",
            "product_category",
            "purchase_date",
            "amount",
            "channel",
            "payment_type",
            "quantity",
            "discount_pct",
            "returned_flag",
        ]
    ]
    return df


# ------------------------------ Snowflake I/O ------------------------------- #

def seed_snowflake(total_rows: int, year: int, seed: int | None):
    analyzer = SnowflakeDataAnalyzer()

    print(f"🔧 Generating synthetic dataset: rows={total_rows} year={year} seed={seed}")
    df = assemble_dataset(total_rows=total_rows, year=year, seed=seed)

    print("📊 Preview of generated data:")
    print(df.head())
    print("\nStats:")
    print(
        df.groupby("product_category")["amount"].agg(["count", "sum", "mean"]).round(2).head()
    )

    # Create / replace table with explicit schema first for clarity & types
    create_sql = """
    CREATE OR REPLACE TABLE sample_data (
        customer_id        INT,
        customer_name      STRING,
        email              STRING,
        region             STRING,
        product_category   STRING,
        purchase_date      TIMESTAMP_NTZ,
        amount             NUMBER(10,2),
        channel            STRING,
        payment_type       STRING,
        quantity           INT,
        discount_pct       FLOAT,
        returned_flag      BOOLEAN
    );
    """
    analyzer.query_to_dataframe(create_sql)  # executes DDL (returns None)

    # Write data
    analyzer.dataframe_to_snowflake(df, "sample_data", if_exists="append")

    # Simple verification
    verify_df = analyzer.query_to_dataframe(
        "SELECT COUNT(*) AS row_count, SUM(amount) AS total_revenue FROM sample_data"
    )
    if verify_df is not None:
        print("\n✅ Load complete:")
        print(verify_df)

    analyzer.close()


# --------------------------------- CLI -------------------------------------- #

def parse_args():
    parser = argparse.ArgumentParser(
        description="Seed richer synthetic data into Snowflake sample_data table"
    )
    parser.add_argument("--rows", type=int, default=2500, help="Number of rows to generate")
    parser.add_argument("--year", type=int, default=datetime.now().year, help="Target year")
    parser.add_argument("--seed", type=int, default=None, help="Random seed for reproducibility")
    parser.add_argument(
        "--customers", type=int, default=None, help="Override number of distinct customers"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    seed_snowflake(total_rows=args.rows, year=args.year, seed=args.seed)


if __name__ == "__main__":
    main()
