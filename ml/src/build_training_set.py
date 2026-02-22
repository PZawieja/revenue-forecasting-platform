"""
Export the dbt-built ml_dataset__renewal for a company to Parquet.
Deterministic sort for reproducibility.
"""

import argparse
from pathlib import Path

from ml.src.io_duckdb import read_sql, write_parquet
from ml.src.utils import get_repo_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Build training set from ml_dataset__renewal")
    parser.add_argument("--company-id", required=True, help="Filter by company_id")
    parser.add_argument(
        "--output-path",
        type=Path,
        default=None,
        help="Output Parquet path (default: ml/outputs/training/renewal_dataset.parquet)",
    )
    args = parser.parse_args()

    out = args.output_path
    if out is None:
        out = get_repo_root() / "ml" / "outputs" / "training" / "renewal_dataset.parquet"

    query = f"""
        SELECT *
        FROM ml_dataset__renewal
        WHERE company_id = '{args.company_id.replace("'", "''")}'
        ORDER BY company_id, customer_id, renewal_month
    """
    df = read_sql(query)
    write_parquet(df, out)
    print(f"Wrote {len(df)} rows to {out}")


if __name__ == "__main__":
    main()
