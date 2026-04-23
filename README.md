# Walmart Retail Data Pipeline

An end-to-end ETL pipeline built on Walmart's 10K sales dataset.
Extracts from two source types (SQL + Parquet), applies transformations,
and loads the final output into Parquet files.

---

## Project Structure

```
walmart_pipeline/
├── data/
│   ├── raw/                    # Source data (SQLite DB + Parquet file)
│   └── processed/              # Output Parquet files
├── extract.py              # Extraction logic
├── transform.py            # Cleaning, enrichment, merging
├── load.py                 # Loading to Parquet
├── pipeline.py                 # Main ETL entry point
├── setup_sources.py            # One-time data setup script
└── requirements.txt
```

---

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Get the dataset

**Option A – Kaggle API**

Make sure your Kaggle API token is at `~/.kaggle/kaggle.json`.
Then run:

```bash
python setup_sources.py --download
```

**Option B – Manual download**

1. Go to https://www.kaggle.com/datasets/najir0123/walmart-10k-sales-datasets
2. Download `Walmart.csv`
3. Place it at `data/raw/Walmart.csv`
4. Then run:

```bash
python setup_sources.py
```

This creates two source files:

- `data/raw/walmart_sales.db` — SQLite database with all transactions
- `data/raw/store_features.parquet` — Aggregated store-level features

---

## Run the pipeline

```bash
python pipeline.py
```

---

## Pipeline Stages

### Extract

- `extract_from_sql()` — Queries all transaction records from the SQLite DB using SQLAlchemy
- `extract_from_parquet()` — Reads the store features Parquet file into a DataFrame

### Transform

- **Clean**: Type-cast date/time columns, handle nulls, deduplicate on invoice_id
- **Enrich**: Add derived columns - hour, day_of_week, month, week_of_year, revenue_before_tax, time_of_day bucket
- **Merge**: Join transactions with store features on branch + city + catogery

### Load

- `all_transactions_enriched.parquet` - Full enriched dataset (Snappy compressed)
- `branch_summary.parquet` - Pre-aggregated mart grouped by branch, city, catogery

---

## Dataset source

Kaggle: https://www.kaggle.com/datasets/najir0123/walmart-10k-sales-datasets
