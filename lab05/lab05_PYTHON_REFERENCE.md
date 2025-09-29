# Lab 05 Python Integration Reference

Comprehensive architectural & operational overview of the Python ↔ Snowflake integration layer used in Lab 05.

## 1. Objectives Recap
- Reliable connection handling (env vars OR connection string)
- Pandas + SQLAlchemy integration without warnings
- Reproducible virtual environment & dependencies (Python 3.13 safe)
- Rich synthetic dataset generation for repeatable analysis
- Modular analytics + visualization with safe fallbacks
- Extensible ETL + scheduling foundation
- Testability without hitting Snowflake (synthetic harness)

## 2. Key Modules & Their Roles
| File | Role |
|------|------|
| `python/snowflake_connection.py` | Normalizes connection config & returns engine/connector |
| `python/pandas_integration.py` | Defines `SnowflakeDataAnalyzer` wrapper around SQLAlchemy engine |
| `python/basic_connection.py` | Simple connectivity smoke test |
| `python/sqlalchemy_integration.py` | (Lab evolution artifact) earlier integration demo |
| `python/advanced_analysis.py` | Advanced aggregation + visualization (now with CLI & exports) |
| `python/seed_sample_data.py` | Rich synthetic dataset generator (seasonality, categories, returns) |
| `python/etl_pipeline.py` | Reusable ETL orchestration (extract→transform→validate→load) |
| `python/scheduled_pipeline.py` | Time‑based pipeline orchestration via `schedule` library |
| `python/test_visualizations.py` | Snowflake‑free local plotting & export test harness |

## 3. Connection Strategy
Priority order when building a connection:
1. `SNOWFLAKE_CONNECTION_STRING` (highest fidelity, includes role & warehouse)
2. Individual variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, `SNOWFLAKE_ROLE`
3. Account normalization attempts (with/without region / domain suffix)

Error mitigation patterns:
- URL‐encoding of password (`quote_plus`)
- Defensive logging of effective configuration (excluding password)
- Graceful fallbacks if `role` or `warehouse` absent

## 4. Data Model (Synthetic `sample_data` Table)
| Column | Type | Notes |
|--------|------|-------|
| customer_id | INT | Reused across multiple orders (power‑law distribution) |
| customer_name | STRING | Generated from name pools |
| email | STRING | Domain randomized |
| region | STRING | Weighted geography distribution |
| product_category | STRING | Weighted category popularity |
| purchase_date | TIMESTAMP_NTZ | Includes randomized time-of-day |
| amount | NUMBER(10,2) | Base price * modifiers * quantity; supports outliers |
| channel | STRING | web/mobile/store/partner |
| payment_type | STRING | card/ach/wallet/cash |
| quantity | INT | Small basket size distribution |
| discount_pct | FLOAT | Optional discount application (0–0.20) |
| returned_flag | BOOLEAN | Simulated return rate (~4%) |

Generation features:
- Seasonal month multipliers (Nov/Dec retail uplift)
- Region revenue weights
- Category price ranges & popularity
- Outlier high-value transactions (1%)
- Deterministic reproducibility via `--seed`

## 5. Analytics Pipeline Overview
Sequence for `advanced_analysis.py`:
```
Snowflake → pandas DataFrame → grouped aggregates (region/category/month/customer)
         → optional CSV exports → visualization (multi-panel) → optional persisted tables (future)
```
Safeguards:
- Numeric coercion for `amount`
- Per-plot disable flags (CLI)
- Protected plotting blocks (no single failure halts run)

## 6. CLI Flags (Advanced Analysis)
| Flag | Purpose |
|------|---------|
| `--no-region` | Skip region revenue bar chart |
| `--no-hist` | Skip amount distribution histogram |
| `--no-pie` | Skip category pie chart |
| `--no-box` | Skip amount-by-category boxplot |
| `--out-file NAME` | Base output image filename (default `sales_analysis.png`) |
| `--format svg` | Force image format override (e.g. png, svg, pdf) |
| `--export-csv` | Write region/category/month summary CSVs |
| `--export-prefix PREFIX` | CSV filename prefix (default `sales_summary`) |

Example:
```bash
python python/advanced_analysis.py --export-csv --format svg --out-file analysis --no-pie
```

## 7. Synthetic Seeding Usage
```bash
# Generate 10k rows for 2024 reproducibly
python python/seed_sample_data.py --rows 10000 --year 2024 --seed 42
```
Heuristic for customer count: ~1 distinct customer per 6 orders (min 50). Override by adding `--customers N` (future extension).

## 8. ETL Pipeline Extension Points
Hook points (`etl_pipeline.py`):
| Method | Extend To |
|--------|-----------|
| `extract_data` | Parameterize source filtering (date windows, incremental boundary) |
| `transform_data` | Add enrichment lookups / derived KPIs |
| `validate_data` | Add schema contracts, anomaly detection stats |
| `load_data` | Switch to staged bulk load (PUT + COPY) for very large volumes |
| `log_pipeline_run` | Persist to Snowflake audit table |

## 9. Scheduling Strategy
`schedule` library used for conceptual clarity (not production grade). Recommended production alternatives:
- Airflow / MWAA / Astronomer
- Dagster
- Prefect
- Snowflake Tasks + Streams (for intra‑Snowflake orchestration)

## 10. Testing Approach
| Test Vector | Mechanism |
|-------------|-----------|
| Visualizations | `test_visualizations.py` synthetic DataFrame |
| Engine connectivity | `basic_connection.py` & `sqlalchemy_integration.py` runs |
| Pandas round-trip | `pandas_integration.analyze_sample_data()` + writeback |
| Future (pytest) | Add assertions on aggregate shapes & file creation |

Suggested pytest pattern (future):
```python
def test_region_summary(tmp_path):
    from advanced_analysis import export_aggregates
    import pandas as pd
    df = pd.DataFrame({
        'region':['North','South'], 'amount':[100,200],
        'product_category':['Electronics','Books'],
        'purchase_date': pd.to_datetime(['2024-01-01','2024-01-02'])
    })
    export_aggregates(df, prefix=tmp_path / 'out')
    assert (tmp_path / 'out_region.csv').exists()
```

## 11. Performance Notes
- For < ~200k rows: `pandas` + `to_sql(method='multi')` is sufficient.
- For larger volumes:
  - Use Snowflake internal staging + `COPY INTO` (bulk load)
  - Consider partitioned generation & chunked writes
  - Replace per-row Python loops with vectorized NumPy ops (already applied in generation)

## 12. Security & Secrets
| Concern | Mitigation |
|---------|------------|
| Credentials leakage | `.env` kept out of VCS (ensure `.gitignore`) |
| Password special chars | URL encoded in connection URL |
| Role-based access | Support for `role` query param in connection string |
| Data masking (PII) | Introduced in Lab 06 (tag + masking policy) – can reuse concept here |

## 13. Future Enhancements
| Idea | Benefit |
|------|---------|
| Persist aggregates to Snowflake summary tables | Downstream BI consumption |
| Add anomaly detection (z-score / IQR) for outliers | Data quality insight |
| Introduce incremental load watermarking | Efficient daily pipelines |
| Add pytest & CI workflow | Automated regression detection |
| Provide Streamlit dashboard variant | Richer interactive exploration |
| CLI command grouping (e.g., `--quick`) | Fast developer feedback |

## 14. Quick Reference Commands
```bash
# Seed data
python python/seed_sample_data.py --rows 5000 --year 2024 --seed 7

# Run full analysis with exports
python python/advanced_analysis.py --export-csv

# Minimal plots only histogram + region
python python/advanced_analysis.py --no-pie --no-box

# Test harness (no Snowflake)
python python/test_visualizations.py --rows 300 --export-csv
```

## 15. Troubleshooting Matrix
| Symptom | Likely Cause | Action |
|---------|--------------|--------|
| Empty aggregates | Source table missing / no rows | Re-run seed script |
| Plot skipped warnings | Column missing or non-numeric | Inspect DataFrame head & dtypes |
| Connection failure | Account format mismatch | Verify `account` vs full domain; use connection string |
| Slow insert | Very large dataset via `to_sql` | Switch to staged bulk load |
| CSV exports missing | No write permissions | Check working directory / run path |

---
**End of Reference**
