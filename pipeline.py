import time
from extract import extract_from_sql, extract_from_parquet
from transform import run_transformations
from load import run_load

DB_PATH = "data/raw/walmart_sales.db"
PARQUET_SOURCE_PATH = "data/raw/store_features.parquet"
OUTPUT_DIR = "data/processed"


def run_pipeline():
    start = time.time()
    print("=" * 50)
    print("  Walmart Retail Data Pipeline")
    print("=" * 50)

    # EXTRACT
    print("\n--- Starting Extraction ---")
    df_transactions = extract_from_sql(DB_PATH)
    df_store_features = extract_from_parquet(PARQUET_SOURCE_PATH)
    print("--- Extraction Complete ---")

    # TRANSFORM
    df_final = run_transformations(df_transactions, df_store_features)

    # LOAD
    run_load(df_final, OUTPUT_DIR)

    elapsed = round(time.time() - start, 2)
    print(f"Pipeline finished in {elapsed}s.")
    print("=" * 50)


if __name__ == "__main__":
    run_pipeline()
