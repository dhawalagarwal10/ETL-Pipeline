"""
setup_sources.py

Run this once before the pipeline. It:
  1. Downloads the Walmart 10K dataset from Kaggle
  2. Loads the CSV into a local SQLite database (simulating a SQL data source)
  3. Saves the store-level feature data as a Parquet file (simulating a file-based source)

Requirements:
  - Kaggle account + API token saved at ~/.kaggle/kaggle.json
  - OR manually download from https://www.kaggle.com/datasets/najir0123/walmart-10k-sales-datasets
    and place Walmart.csv inside data/raw/
"""

import os
import pandas as pd
from sqlalchemy import create_engine

RAW_DIR = "data/raw"
DB_PATH = "data/raw/walmart_sales.db"
PARQUET_PATH = "data/raw/store_features.parquet"
CSV_PATH = os.path.join(RAW_DIR, "Walmart.csv")


def download_from_kaggle():
    print("Downloading dataset from Kaggle...")
    os.makedirs(RAW_DIR, exist_ok=True)
    os.system(
        f"kaggle datasets download -d najir0123/walmart-10k-sales-datasets "
        f"--unzip -p {RAW_DIR}"
    )
    print("Download complete.")


def load_csv() -> pd.DataFrame:
    if not os.path.exists(r"C:\\Users\\dhawa\\OneDrive\\Desktop\\Walmart.csv"):
        raise FileNotFoundError(
            f"CSV not found at {CSV_PATH}. "
            "Run with --download flag or place Walmart.csv in data/raw/ manually."
        )
    df = pd.read_csv(r"C:\Users\dhawa\OneDrive\Desktop\Walmart.csv")
    print(f"Loaded {len(df)} rows from CSV.")
    return df


def create_sql_source(df: pd.DataFrame):
    """
    Loads transaction-level data into SQLite.
    This represents the transactional database source in the pipeline.
    Columns: invoice_id, branch, city, customer_type, gender,
             product_line, unit_price, quantity, tax_5pct,
             total, date, time, payment, cogs, gross_income, rating
    """
    # Rename columns to snake_case for SQL friendliness
    df_sql = df.copy()
    df_sql.columns = (
        df_sql.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("%", "pct")
        .str.replace("/", "_")
    )

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df_sql.to_sql("sales_transactions", engine, if_exists="replace", index=False)
    print(f"SQLite DB created at {DB_PATH} with {len(df_sql)} rows in 'sales_transactions'.")


def create_parquet_source(df: pd.DataFrame):
    df_clean = df.copy()

    df_clean.columns = (
        df_clean.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("%", "pct")
        .str.replace("/", "_")
    )

    df_clean["unit_price"] = (
        df_clean["unit_price"]
        .astype(str)
        .str.replace("$", "", regex=False)
        .astype(float)
    )

    store_features = (
        df_clean
        .groupby(["branch", "city", "category"])
        .agg(
            avg_unit_price=("unit_price", "mean"),
            avg_quantity=("quantity", "mean"),
            avg_rating=("rating", "mean"),
            transaction_count=("invoice_id", "count"),
        )
        .reset_index()
        .round(2)
    )

    os.makedirs(os.path.dirname(PARQUET_PATH), exist_ok=True)

    store_features.to_parquet(PARQUET_PATH, index=False)

    print(
        f"Parquet store features saved at {PARQUET_PATH} "
        f"with {len(store_features)} rows."
    )


if __name__ == "__main__":
    import sys
    if "--download" in sys.argv:
        download_from_kaggle()

    df = load_csv()
    create_sql_source(df)
    create_parquet_source(df)
    print("\nSetup complete. Both sources are ready. Run pipeline.py next.")
