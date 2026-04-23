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
        )
    df = pd.read_csv(r"C:\Users\dhawa\OneDrive\Desktop\Walmart.csv")
    print(f"Loaded {len(df)} rows from CSV.")
    return df


def create_sql_source(df: pd.DataFrame):
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
