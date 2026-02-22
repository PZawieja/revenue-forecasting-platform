"""
Export key marts and ML outputs to docs/artifacts/ as CSVs for sharing without running Streamlit.
Run from repo root: PYTHONPATH=. python -m forecasting.src.export_artifacts [--duckdb-path PATH] [--out-dir PATH]
"""

import argparse
import sys
from pathlib import Path

from forecasting.src.io_duckdb import get_warehouse_dir, read_table


def _repo_root() -> Path:
    # forecasting/src/export_artifacts.py -> src -> forecasting -> repo root
    return Path(__file__).resolve().parent.parent.parent


def _out_dir(repo_root: Path, given: Path | None) -> Path:
    if given is not None:
        return given.resolve()
    return repo_root / "docs" / "artifacts"


def _duckdb_path(given: str | None) -> Path:
    if given:
        return Path(given).resolve()
    return get_warehouse_dir(_repo_root()) / "revenue_forecasting.duckdb"


def export_artifacts(duckdb_path: Path, out_dir: Path, warehouse_dir: Path | None = None) -> None:
    import pandas as pd

    if warehouse_dir is None:
        warehouse_dir = duckdb_path.parent

    if not duckdb_path.exists():
        print(f"DuckDB not found: {duckdb_path}", file=sys.stderr)
        sys.exit(1)

    out_dir.mkdir(parents=True, exist_ok=True)

    # 1) mart_executive_forecast_summary — latest 12 months, scenario=base
    try:
        df = read_table(
            """
            SELECT * FROM main.mart_executive_forecast_summary
            WHERE scenario = 'base'
            ORDER BY month DESC
            LIMIT 12
            """,
            warehouse_dir=warehouse_dir,
        )
        if not df.empty:
            df = df.sort_values("month").reset_index(drop=True)
            df.to_csv(out_dir / "mart_executive_forecast_summary.csv", index=False)
            print("Exported mart_executive_forecast_summary.csv")
    except Exception as e:
        print(f"Skip mart_executive_forecast_summary: {e}", file=sys.stderr)

    # 2) mart_arr_waterfall_monthly — latest 6 months, scenario=base, segment=All or aggregate
    try:
        df = read_table(
            """
            SELECT * FROM main.mart_arr_waterfall_monthly
            WHERE scenario = 'base'
            ORDER BY month DESC
            LIMIT 6
            """,
            warehouse_dir=warehouse_dir,
        )
        if not df.empty:
            df = df.sort_values("month").reset_index(drop=True)
            df.to_csv(out_dir / "mart_arr_waterfall_monthly.csv", index=False)
            print("Exported mart_arr_waterfall_monthly.csv")
    except Exception as e:
        print(f"Skip mart_arr_waterfall_monthly: {e}", file=sys.stderr)

    # 3) mart_churn_risk_watchlist — latest month, top 20
    try:
        df = read_table(
            """
            SELECT * FROM main.mart_churn_risk_watchlist
            WHERE month = (SELECT max(month) FROM main.mart_churn_risk_watchlist)
            ORDER BY COALESCE(p_renew, 0) ASC
            LIMIT 20
            """,
            warehouse_dir=warehouse_dir,
        )
        if not df.empty:
            df.to_csv(out_dir / "mart_churn_risk_watchlist.csv", index=False)
            print("Exported mart_churn_risk_watchlist.csv")
    except Exception as e:
        print(f"Skip mart_churn_risk_watchlist: {e}", file=sys.stderr)

    # 4) Backtest metrics — latest 6 cutoff months (renewal + pipeline)
    for name, table in [
        ("renewal_backtest_metrics", "main.ml_renewal_backtest_metrics"),
        ("pipeline_backtest_metrics", "main.ml_pipeline_backtest_metrics"),
    ]:
        try:
            df = read_table(
                f"""
                SELECT * FROM {table}
                WHERE cutoff_month IN (
                    SELECT cutoff_month FROM (SELECT DISTINCT cutoff_month AS cutoff_month FROM {table}) AS t
                    ORDER BY cutoff_month DESC LIMIT 6
                )
                ORDER BY cutoff_month, model_name, segment
                """,
                warehouse_dir=warehouse_dir,
            )
            if not df.empty:
                df.to_csv(out_dir / f"{name}.csv", index=False)
                print(f"Exported {name}.csv")
        except Exception as e:
            print(f"Skip {name}: {e}", file=sys.stderr)

    # 5) ml_model_selection
    try:
        df = read_table("SELECT * FROM main.ml_model_selection ORDER BY dataset", warehouse_dir=warehouse_dir)
        if not df.empty:
            df.to_csv(out_dir / "ml_model_selection.csv", index=False)
            print("Exported ml_model_selection.csv")
    except Exception as e:
        print(f"Skip ml_model_selection: {e}", file=sys.stderr)

    # 6) ml_calibration_bins — latest cutoff for renewals + pipeline, preferred models only
    try:
        sel = read_table("SELECT dataset, preferred_model FROM main.ml_model_selection", warehouse_dir=warehouse_dir)
        if sel.empty:
            # Fallback: export latest cutoff per dataset for both models
            df = read_table(
                """
                SELECT * FROM main.ml_calibration_bins
                WHERE (dataset, cutoff_month) IN (
                    SELECT dataset, max(cutoff_month) FROM main.ml_calibration_bins GROUP BY dataset
                )
                ORDER BY dataset, model_name, bin_id
                """,
                warehouse_dir=warehouse_dir,
            )
        else:
            parts = []
            for _, row in sel.iterrows():
                d, m = str(row["dataset"]), str(row["preferred_model"])
                part = read_table(
                    f"""
                    SELECT * FROM main.ml_calibration_bins
                    WHERE dataset = '{d}' AND model_name = '{m}'
                      AND cutoff_month = (SELECT max(cutoff_month) FROM main.ml_calibration_bins WHERE dataset = '{d}' AND model_name = '{m}')
                    ORDER BY bin_id
                    """,
                    warehouse_dir=warehouse_dir,
                )
                if not part.empty:
                    parts.append(part)
            df = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
        if not df.empty:
            df.to_csv(out_dir / "ml_calibration_bins.csv", index=False)
            print("Exported ml_calibration_bins.csv")
    except Exception as e:
        print(f"Skip ml_calibration_bins: {e}", file=sys.stderr)

    print("Export done.", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser(description="Export key marts/ML tables to docs/artifacts/ as CSVs.")
    parser.add_argument(
        "--duckdb-path",
        default=None,
        help="Path to revenue_forecasting.duckdb (default: ./warehouse/revenue_forecasting.duckdb)",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        type=Path,
        help="Output directory (default: docs/artifacts)",
    )
    args = parser.parse_args()

    repo = _repo_root()
    db_path = _duckdb_path(args.duckdb_path)
    out = _out_dir(repo, args.out_dir)
    export_artifacts(db_path, out, warehouse_dir=db_path.parent)


if __name__ == "__main__":
    main()
