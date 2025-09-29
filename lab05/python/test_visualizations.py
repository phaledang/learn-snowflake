"""Lightweight test harness for advanced_analysis visualization logic.

This avoids hitting Snowflake by constructing an in-memory synthetic DataFrame
with similar columns. Designed for quick sanity verification that plotting
and CSV export pathways do not raise exceptions.

Run (from lab05 directory with venv active):

    python python/test_visualizations.py --show

"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta
import numpy as np
import pandas as pd

from advanced_analysis import create_visualizations, export_aggregates


def build_synthetic_df(rows: int = 250):
    rng = np.random.default_rng(123)
    base_date = datetime(2024, 1, 1)
    dates = [base_date + timedelta(days=int(x)) for x in rng.integers(0, 365, size=rows)]

    df = pd.DataFrame({
        'customer_id': rng.integers(1, 60, size=rows),
        'customer_name': [f"Cust_{i}" for i in rng.integers(1, 500, size=rows)],
        'email': [f"cust{i}@example.com" for i in rng.integers(1, 500, size=rows)],
        'region': rng.choice(['North', 'South', 'East', 'West', 'Central'], size=rows),
        'product_category': rng.choice(['Electronics', 'Books', 'Home', 'Sports', 'Clothing'], size=rows),
        'purchase_date': dates,
        'amount': rng.uniform(5, 800, size=rows).round(2),
        'channel': rng.choice(['web', 'mobile', 'store', 'partner'], size=rows),
        'payment_type': rng.choice(['card', 'ach', 'wallet', 'cash'], size=rows),
        'quantity': rng.integers(1, 5, size=rows),
        'discount_pct': rng.choice([0, 0, 0, 0.05, 0.10, 0.15], size=rows),
        'returned_flag': rng.choice([False, False, False, True], size=rows)
    })
    return df


def main():
    parser = argparse.ArgumentParser(description='Test visualization & export logic')
    parser.add_argument('--rows', type=int, default=250)
    parser.add_argument('--no-region', action='store_true')
    parser.add_argument('--no-hist', action='store_true')
    parser.add_argument('--no-pie', action='store_true')
    parser.add_argument('--no-box', action='store_true')
    parser.add_argument('--export-csv', action='store_true')
    parser.add_argument('--show', action='store_true', help='Display plots interactively (if backend available)')
    args = parser.parse_args()

    df = build_synthetic_df(args.rows)
    print(f"Synthetic DF shape: {df.shape}")

    create_visualizations(
        df,
        disable_region=args.no_region,
        disable_hist=args.no_hist,
        disable_pie=args.no_pie,
        disable_box=args.no_box,
        out_file='test_sales_analysis.png'
    )

    if args.export_csv:
        export_aggregates(df, 'test_sales_summary')

    if args.show:
        print('NOTE: create_visualizations already attempted to show plot if possible.')


if __name__ == '__main__':
    main()
