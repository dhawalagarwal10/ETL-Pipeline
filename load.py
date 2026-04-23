import os
import pandas as pd


def load_full_dataset(df: pd.DataFrame, output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "all_transactions_enriched.parquet")

    df.to_parquet(output_path, index=False, compression="snappy")

    size_kb = os.path.getsize(output_path) / 1024
    print(f"[LOAD] Full dataset written to {output_path} ({size_kb:.1f} KB, {len(df)} rows).")


def load_branch_summary(df: pd.DataFrame, output_dir: str):
    summary = (
    df.groupby(["branch", "city", "category"])
    .agg(
        total_revenue=("total", "sum"),
        total_transactions=("invoice_id", "count"),
        avg_transaction_value=("total", "mean"),
        avg_customer_rating=("rating", "mean"),
        avg_profit_margin=("profit_margin", "mean")
    )
    .reset_index()
    .round(2)
    .sort_values("total_revenue", ascending=False)
)

    output_path = os.path.join(output_dir, "branch_summary.parquet")
    summary.to_parquet(output_path, index=False, compression="snappy")

    size_kb = os.path.getsize(output_path) / 1024
    print(f"[LOAD] Branch summary written to {output_path} ({size_kb:.1f} KB, {len(summary)} rows).")


def run_load(df: pd.DataFrame, output_dir: str
    print("\n--- Starting Load ---")
    load_full_dataset(df, output_dir)
    load_branch_summary(df, output_dir)
    print("--- Load Complete ---\n")
