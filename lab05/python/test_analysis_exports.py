"""Pytest tests for advanced_analysis export & persistence logic.

These tests focus on offline parts (CSV export & plotting) using a synthetic
DataFrame so they don't require a live Snowflake connection. Persistence to
Snowflake is skipped unless SNOWFLAKE_CONNECTION_STRING (or equivalent vars)
are present.
"""
from pathlib import Path
import os
import pandas as pd
import pytest

# Import target functions
from advanced_analysis import export_aggregates, create_visualizations

DATA_ROWS = 300

@pytest.fixture
def synthetic_df():
    import numpy as np
    rng = np.random.default_rng(123)
    regions = ['NORTH','SOUTH','EAST','WEST']
    categories = ['A','B','C','D']
    df = pd.DataFrame({
        'region': rng.choice(regions, DATA_ROWS),
        'product_category': rng.choice(categories, DATA_ROWS),
        'amount': rng.normal(120, 35, DATA_ROWS).round(2),
        'purchase_date': pd.date_range('2024-01-01', periods=DATA_ROWS, freq='D'),
        'customer_id': rng.integers(1, 50, DATA_ROWS)
    })
    return df


def test_export_aggregates_creates_csvs(tmp_path, synthetic_df, monkeypatch):
    # Switch working dir to temp so created files land there
    monkeypatch.chdir(tmp_path)
    prefix = 'test_summary'
    export_aggregates(synthetic_df, prefix)
    for suffix in ['region','category','monthly']:
        path = tmp_path / f"{prefix}_{suffix}.csv"
        assert path.exists(), f"Expected {path} to exist"
        df = pd.read_csv(path)
        assert not df.empty, f"{path} should not be empty"


def test_create_visualizations_generates_file(tmp_path, synthetic_df, monkeypatch):
    monkeypatch.chdir(tmp_path)
    out_file = 'viz.png'
    create_visualizations(
        synthetic_df,
        disable_region=False,
        disable_hist=False,
        disable_pie=False,
        disable_box=False,
        out_file=out_file,
        out_format=None,
    )
    assert Path(out_file).exists(), "Visualization output file not created"


@pytest.mark.skipif(
    not os.getenv('SNOWFLAKE_CONNECTION_STRING') and not os.getenv('SNOWFLAKE_ACCOUNT'),
    reason="Snowflake environment variables not set"
)
def test_persist_with_live_snowflake(monkeypatch, synthetic_df):
    # Lazy import to avoid connector overhead when skipped
    from advanced_analysis import persist_aggregates_to_snowflake, SnowflakeDataAnalyzer
    analyzer = SnowflakeDataAnalyzer()
    try:
        persist_aggregates_to_snowflake(synthetic_df, analyzer, prefix='TESTPY_')
        # Simple smoke: attempt a query to one table (case-insensitive)
        res = analyzer.query_to_dataframe("SELECT COUNT(*) AS C FROM TESTPY_REGION_SUMMARY")
        assert res is not None and res.iloc[0]['C'] > 0
    finally:
        analyzer.close()
