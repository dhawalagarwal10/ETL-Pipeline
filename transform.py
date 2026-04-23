import pandas as pd


def clean_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Parse date
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Parse time
    df["time"] = pd.to_datetime(
        df["time"].astype(str),
        format="%H:%M:%S",
        errors="coerce"
)

    # Remove dollar sign from unit_price
    df["unit_price"] = (
        df["unit_price"]
        .astype(str)
        .str.replace("$", "", regex=False)
    )

    numeric_cols = [
        "unit_price",
        "quantity",
        "rating",
        "profit_margin"
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Create total revenue column
    df["total"] = df["unit_price"] * df["quantity"]

    # Null handling
    df = df.dropna(subset=["invoice_id", "branch", "date", "total"])

    # Fill missing rating
    df["rating"] = df["rating"].fillna(df["rating"].median())

    # Deduplicate
    df = df.drop_duplicates(subset=["invoice_id"])

    # Strip text columns
    str_cols = df.select_dtypes(include="object").columns

    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    print(f"[TRANSFORM] Transactions cleaned: {len(df)} rows.")
    return df


def enrich_transactions(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["hour"] = df["time"].dt.hour
    df["day_of_week"] = df["date"].dt.day_name()
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["week_of_year"] = df["date"].dt.isocalendar().week.astype(int)

    df["revenue_before_tax"] = df["unit_price"] * df["quantity"]

    df["time_of_day"] = pd.cut(
        df["hour"],
        bins=[0, 12, 17, 21, 24],
        labels=["Morning", "Afternoon", "Evening", "Night"],
        right=False
    )

    print(f"[TRANSFORM] Transactions enriched with {len(df.columns)} columns.")
    return df


def merge_sources(df_transactions: pd.DataFrame, df_store_features: pd.DataFrame) -> pd.DataFrame:

    df_merged = df_transactions.merge(
        df_store_features,
        on=["branch", "city", "category"],
        how="left",
        suffixes=("", "_store")
    )

    df_merged["above_avg_rating"] = (
        df_merged["rating"] > df_merged["avg_rating"]
    )

    print(
        f"[TRANSFORM] Merged sources: {len(df_merged)} rows, "
        f"{len(df_merged.columns)} columns."
    )

    return df_merged


def run_transformations(df_transactions: pd.DataFrame, df_store_features: pd.DataFrame) -> pd.DataFrame:

    print("\n--- Starting Transformations ---")
    df = clean_transactions(df_transactions)
    df = enrich_transactions(df)
    df = merge_sources(df, df_store_features)
    print("--- Transformations Complete ---\n")
    return df
