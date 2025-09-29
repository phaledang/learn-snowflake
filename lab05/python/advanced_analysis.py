"""Advanced sales data analysis and visualization for Lab05.

Provides comprehensive aggregation
and plotting routines using the shared SnowflakeDataAnalyzer.
"""
from __future__ import annotations

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Local import (kept relative to lab05/python)
from pandas_integration import SnowflakeDataAnalyzer


def analyze_sales_data():
    """Comprehensive sales data analysis.

    Pulls enriched sales dataset from Snowflake and performs:
      - Revenue by region
      - Category statistics
      - Monthly trend aggregation
      - Top customer analysis
      - Visualization generation (saved to sales_analysis.png)
    """
    analyzer = SnowflakeDataAnalyzer()

    # Query includes temporal breakdown fields for easier slicing
    sales_df = analyzer.query_to_dataframe(
        """
        SELECT 
            s.*,
            EXTRACT(MONTH FROM purchase_date) AS purchase_month,
            EXTRACT(YEAR  FROM purchase_date) AS purchase_year,
            EXTRACT(DOW   FROM purchase_date) AS day_of_week
        FROM sample_data s
        ORDER BY purchase_date
        """
    )

    if sales_df is None:
        print("❌ Failed to retrieve data")
        analyzer.close()
        return

    print("📈 SALES DATA ANALYSIS")
    print("=" * 50)

    # --- Revenue by Region ---
    revenue_by_region = (
        sales_df.groupby("region")["amount"].agg(["count", "sum", "mean", "std"]).round(2)
    )
    print("\n💰 Revenue by Region:")
    print(revenue_by_region)

    # --- Category Stats ---
    category_stats = (
        sales_df.groupby("product_category").agg({
            "amount": ["count", "sum", "mean"],
            "customer_id": "nunique",
        }).round(2)
    )
    print("\n📦 Category Statistics:")
    print(category_stats)

    # --- Monthly Sales Trends ---
    monthly_sales = (
        sales_df.groupby(["purchase_year", "purchase_month"])["amount"].sum()
    )
    print("\n📅 Monthly Sales Trends:")
    print(monthly_sales)

    # --- Customer Insights ---
    customer_stats = (
        sales_df.groupby("customer_id").agg({
            "amount": ["count", "sum", "mean"],
            "product_category": lambda x: len(x.unique()),
        }).round(2)
    )
    customer_stats.columns = [
        "order_count",
        "total_spent",
        "avg_order",
        "categories_purchased",
    ]
    print("\n👥 Top Customers:")
    print(customer_stats.sort_values("total_spent", ascending=False).head())

    create_visualizations(sales_df)

    analyzer.close()


def create_visualizations(df: pd.DataFrame):
    """Create and persist data visualizations.

    Generates:
      1. Revenue by region bar chart
      2. Purchase amount distribution histogram
      3. Sales by category pie chart
      4. Amount distribution by category boxplot

    Saves figure to sales_analysis.png and displays interactively.
    """
    plt.style.use("seaborn-v0_8")
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))

    # 1. Revenue by region
    (df.groupby("region")["amount"].sum().plot(
        kind="bar", ax=axes[0, 0], title="Revenue by Region")
    )
    axes[0, 0].set_ylabel("Revenue ($)")

    # 2. Amount distribution
    df["amount"].hist(bins=20, ax=axes[0, 1])
    axes[0, 1].set_title("Purchase Amount Distribution")
    axes[0, 1].set_xlabel("Amount ($)")
    axes[0, 1].set_ylabel("Frequency")

    # 3. Category breakdown (pie)
    df["product_category"].value_counts().plot(
        kind="pie", ax=axes[1, 0], title="Sales by Category")

    # 4. Amount by category boxplot
    df.boxplot(column="amount", by="product_category", ax=axes[1, 1])
    axes[1, 1].set_title("Amount Distribution by Category")
    axes[1, 1].set_xlabel("Product Category")

    plt.tight_layout()
    plt.savefig("sales_analysis.png", dpi=300, bbox_inches="tight")
    try:
        plt.show()
    except Exception:
        # In headless environments just close
        plt.close(fig)


if __name__ == "__main__":
    analyze_sales_data()
