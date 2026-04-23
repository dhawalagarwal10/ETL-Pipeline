import pandas as pd
from sqlalchemy import create_engine, text


def extract_from_sql(db_path: str) -> pd.DataFrame:

    engine = create_engine(f"sqlite:///{db_path}")

    query = text("""
        SELECT
            invoice_id,
            branch,
            city,
            category,
            unit_price,
            quantity,
            date,
            time,
            payment_method,
            rating,
            profit_margin
        FROM sales_transactions
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    print(f"[EXTRACT] SQL source: {len(df)} rows extracted from 'sales_transactions'.")
    return df


def extract_from_parquet(parquet_path: str) -> pd.DataFrame:
    df = pd.read_parquet(parquet_path)
    print(f"[EXTRACT] Parquet source: {len(df)} rows extracted from store features file.")
    return df
