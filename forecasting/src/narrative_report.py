"""
Narrative revenue intelligence report generator.
Reads from DuckDB marts and writes a single Markdown report.
No external APIs or LLM calls; deterministic output.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Optional

import pandas as pd

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

# Defaults for CLI
DEFAULT_DUCKDB_PATH = "./warehouse/revenue_forecasting.duckdb"
DEFAULT_SCENARIO = "base"
DEFAULT_SEGMENT = "All"
DEFAULT_MONTHS = 6
DEFAULT_OUTPUT = "./docs/reports/revenue_intelligence_report.md"


def _connect(duckdb_path: str):
    if duckdb is None:
        raise RuntimeError("duckdb is required; install with pip install duckdb")
    path = Path(duckdb_path).resolve()
    if not path.exists():
        raise FileNotFoundError(f"DuckDB file not found: {path}")
    return duckdb.connect(str(path), read_only=True)


def _table_exists(conn, schema: str, table: str) -> bool:
    q = """
    SELECT 1 FROM information_schema.tables
    WHERE table_schema = ? AND table_name = ?
    LIMIT 1
    """
    return len(conn.execute(q, [schema, table]).fetchall()) > 0


def _run(conn, sql: str, params: Optional[dict[str, Any]] = None) -> pd.DataFrame:
    if params:
        return conn.execute(sql, list(params.values())).fetchdf()
    return conn.execute(sql).fetchdf()


def _run_safe(conn, sql: str, params: Optional[dict[str, Any]] = None) -> Optional[pd.DataFrame]:
    try:
        df = _run(conn, sql, params)
        return df if df is not None and len(df) > 0 else None
    except Exception:
        return None


def _get_available_months(conn, scenario: str, source: str) -> list[str]:
    """Last N months from executive summary or forecast fact; descending order."""
    if source == "exec":
        sql = """
        SELECT DISTINCT month FROM main.mart_executive_forecast_summary
        WHERE scenario = ?
        ORDER BY month DESC
        """
    else:
        sql = """
        SELECT DISTINCT month FROM main.fct_revenue_forecast_with_intervals
        WHERE scenario = ?
        ORDER BY month DESC
        """
    try:
        df = conn.execute(sql, [scenario]).fetchdf()
    except Exception:
        try:
            sql_fallback = """
            SELECT DISTINCT month FROM main.fct_revenue_forecast_monthly
            WHERE scenario = ?
            ORDER BY month DESC
            """
            df = conn.execute(sql_fallback, [scenario]).fetchdf()
        except Exception:
            return []
    if df is None or df.empty:
        return []
    months = df["month"].astype(str).tolist()
    return months


def _select_last_n_months(months: list[str], n: int) -> list[str]:
    """Return last n months in ascending order (oldest first) for display."""
    if not months or n <= 0:
        return []
    take = months[: min(n, len(months))]
    return list(reversed(take))


def _forecast_vs_actual(
    conn, scenario: str, segment: str, months: list[str]
) -> tuple[Optional[pd.DataFrame], str]:
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [scenario] + ([segment] if segment and segment != "All" else [])
    placeholders = ", ".join(["?" for _ in months])
    if not months:
        return None, "Not available (run dbt build and ensure forecast/actual data)."
    # Prefer fct_revenue_forecast_with_intervals
    sql_intervals = f"""
    SELECT
        month,
        sum(forecast_mrr_total) AS forecast_mrr,
        sum(actual_mrr) AS actual_mrr,
        sum(forecast_lower) AS forecast_lower,
        sum(forecast_upper) AS forecast_upper
    FROM main.fct_revenue_forecast_with_intervals
    WHERE scenario = ? {seg_filter} AND month IN ({placeholders})
    GROUP BY month
    ORDER BY month
    """
    try:
        df = conn.execute(
            sql_intervals, [scenario] + ([] if segment == "All" else [segment]) + months
        ).fetchdf()
    except Exception:
        df = None
    if df is None or df.empty:
        sql_fallback = f"""
        SELECT
            month,
            sum(forecast_mrr_total) AS forecast_mrr,
            sum(actual_mrr) AS actual_mrr,
            cast(null as double) AS forecast_lower,
            cast(null as double) AS forecast_upper
        FROM main.fct_revenue_forecast_monthly
        WHERE scenario = ? {seg_filter} AND month IN ({placeholders})
        GROUP BY month
        ORDER BY month
        """
        try:
            df = conn.execute(
                sql_fallback, [scenario] + ([] if segment == "All" else [segment]) + months
            ).fetchdf()
        except Exception:
            return None, "Not available (run dbt build and fct_revenue_forecast_with_intervals or fct_revenue_forecast_monthly)."
    if df is None or df.empty:
        return None, "Not available (run dbt build)."
    df["month"] = df["month"].astype(str)
    df["error"] = df["forecast_mrr"] - df["actual_mrr"]
    df["ape"] = (df["error"].abs() / df["actual_mrr"].replace(0, float("nan")) * 100).round(2)
    return df, ""


def _exec_summary(conn, scenario: str, month: str) -> tuple[Optional[dict], str]:
    sql = """
    SELECT
        max(month) AS month,
        sum(total_forecast_revenue) AS total_forecast_revenue,
        sum(total_actual_revenue) AS total_actual_revenue,
        avg(revenue_growth_mom) AS revenue_growth_mom,
        avg(avg_confidence_score) AS avg_confidence_score
    FROM main.mart_executive_forecast_summary
    WHERE scenario = ? AND month = ?
    GROUP BY scenario
    """
    try:
        df = conn.execute(sql, [scenario, month]).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_executive_forecast_summary)."
    if df is None or df.empty:
        return None, "Not available (run dbt build: mart_executive_forecast_summary)."
    row = df.iloc[0]
    return {
        "month": str(row["month"]),
        "total_forecast_revenue": float(row["total_forecast_revenue"] or 0),
        "total_actual_revenue": float(row["total_actual_revenue"] or 0),
        "revenue_growth_mom": float(row["revenue_growth_mom"] or 0),
        "avg_confidence_score": float(row["avg_confidence_score"] or 0),
    }, ""


def _confidence(conn, scenario: str, month: str) -> tuple[Optional[float], str]:
    if not _table_exists(conn, "main", "int_forecast_confidence"):
        return None, "Not available (run dbt build: int_forecast_confidence)."
    sql = """
    SELECT avg(confidence_score_0_100) AS score
    FROM main.int_forecast_confidence
    WHERE scenario = ? AND month = ?
    """
    try:
        df = conn.execute(sql, [scenario, month]).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: int_forecast_confidence)."
    if df is None or df.empty or pd.isna(df.iloc[0]["score"]):
        return None, ""
    return float(df.iloc[0]["score"]), ""


def _arr_waterfall(
    conn, scenario: str, segment: str, month: str
) -> tuple[Optional[pd.DataFrame], str]:
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [month, scenario] + ([segment] if segment and segment != "All" else [])
    sql = f"""
    SELECT
        month,
        sum(starting_arr) AS starting_arr,
        sum(new_arr) AS new_arr,
        sum(expansion_arr) AS expansion_arr,
        sum(contraction_arr) AS contraction_arr,
        sum(churn_arr) AS churn_arr,
        sum(ending_arr) AS ending_arr,
        (sum(starting_arr) + sum(expansion_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS nrr,
        (sum(starting_arr) - sum(contraction_arr) - sum(churn_arr)) / nullif(sum(starting_arr), 0) AS grr
    FROM main.mart_arr_waterfall_monthly
    WHERE month = ? AND scenario = ? {seg_filter}
    GROUP BY month, scenario
    """
    try:
        df = conn.execute(sql, params).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_arr_waterfall_monthly)."
    if df is None or df.empty:
        return None, "Not available (run dbt build: mart_arr_waterfall_monthly)."
    return df, ""


def _explainability(
    conn, scenario: str, segment: str, month: str
) -> tuple[Optional[pd.DataFrame], str]:
    if not _table_exists(conn, "main", "mart_forecast_explainability_monthly"):
        return None, "Not available (run dbt build: mart_forecast_explainability_monthly)."
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [month, scenario] + ([segment] if segment and segment != "All" else [])
    sql = f"""
    SELECT
        sum(renewal_driver_delta) AS renewal_driver_delta,
        sum(pipeline_driver_delta) AS pipeline_driver_delta,
        sum(expansion_driver_delta) AS expansion_driver_delta,
        sum(residual_delta) AS residual_delta,
        any_value(top_driver) AS top_driver,
        avg(top_driver_share_pct) AS top_driver_share_pct
    FROM main.mart_forecast_explainability_monthly
    WHERE month = ? AND scenario = ? {seg_filter}
    """
    try:
        df = conn.execute(sql, params).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_forecast_explainability_monthly)."
    if df is None or df.empty:
        return None, ""
    return df, ""


def _churn_risk_watchlist(
    conn, segment: str, month: str, limit: int = 10
) -> tuple[Optional[pd.DataFrame], str]:
    if not _table_exists(conn, "main", "mart_churn_risk_watchlist"):
        return None, "Not available (run dbt build: mart_churn_risk_watchlist)."
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [month] + ([segment] if segment and segment != "All" else []) + [limit]
    sql = f"""
    SELECT
        coalesce(c.customer_name, w.customer_id::varchar) AS customer_name,
        w.current_arr AS arr,
        w.months_to_renewal,
        w.p_renew,
        w.health_score_1_10 AS health,
        w.risk_reason AS reason
    FROM main.mart_churn_risk_watchlist w
    LEFT JOIN main.dim_customer c ON c.company_id = w.company_id AND c.customer_id = w.customer_id
    WHERE w.month = ? {seg_filter}
    ORDER BY w.risk_rank
    LIMIT ?
    """
    try:
        df = conn.execute(sql, params).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_churn_risk_watchlist)."
    if df is None or df.empty:
        return df, ""
    return df, ""


def _top_arr_movers(
    conn, segment: str, month: str, limit: int = 5
) -> tuple[Optional[pd.DataFrame], str]:
    if not _table_exists(conn, "main", "mart_top_arr_movers"):
        return None, "Not available (run dbt build: mart_top_arr_movers)."
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [month] + ([segment] if segment and segment != "All" else []) + [limit]
    sql = f"""
    SELECT customer_name, arr_delta, bridge_category AS category
    FROM main.mart_top_arr_movers
    WHERE month = ? {seg_filter}
    ORDER BY rank
    LIMIT ?
    """
    try:
        df = conn.execute(sql, params).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_top_arr_movers)."
    return df, ""


def _coverage_metrics(conn, scenario: str, segment: str, month: str) -> tuple[Optional[dict], str]:
    if not _table_exists(conn, "main", "mart_forecast_coverage_metrics"):
        return None, "Not available (run dbt build: mart_forecast_coverage_metrics)."
    seg_filter = "AND segment = ?" if segment and segment != "All" else ""
    params = [month, scenario] + ([segment] if segment and segment != "All" else [])
    sql = f"""
    SELECT
        avg(pipeline_coverage_ratio) AS pipeline_coverage_ratio,
        avg(renewal_coverage_ratio) AS renewal_coverage_ratio,
        avg(concentration_ratio_top5) AS concentration_ratio_top5
    FROM main.mart_forecast_coverage_metrics
    WHERE month = ? AND scenario = ? {seg_filter}
    """
    try:
        df = conn.execute(sql, params).fetchdf()
    except Exception:
        return None, "Not available (run dbt build: mart_forecast_coverage_metrics)."
    if df is None or df.empty:
        return None, ""
    row = df.iloc[0]
    return {
        "pipeline_coverage_ratio": float(row["pipeline_coverage_ratio"] or 0),
        "renewal_coverage_ratio": float(row["renewal_coverage_ratio"] or 0),
        "concentration_ratio_top5": float(row["concentration_ratio_top5"] or 0),
    }, ""


def _model_selection(conn) -> tuple[Optional[pd.DataFrame], str]:
    if not _table_exists(conn, "main", "ml_model_selection"):
        return None, "Not available (run ML publish step: ml_model_selection)."
    try:
        df = conn.execute("SELECT * FROM main.ml_model_selection ORDER BY dataset").fetchdf()
    except Exception:
        return None, "Not available (run ML publish step: ml_model_selection)."
    return df, ""


def _backtest_metrics(conn, dataset: str) -> tuple[Optional[pd.DataFrame], str]:
    table = "main.ml_renewal_backtest_metrics" if dataset == "renewals" else "main.ml_pipeline_backtest_metrics"
    if not _table_exists(conn, "main", table.split(".")[-1]):
        return None, f"Not available (run ML backtest: {table})."
    sql = f"""
    SELECT * FROM {table}
    WHERE cutoff_month = (SELECT max(cutoff_month) FROM {table})
    ORDER BY model_name, segment
    """
    try:
        df = conn.execute(sql).fetchdf()
    except Exception:
        return None, ""
    return df, ""


def _drift_months(conn, scenario: str, segment: str) -> tuple[Optional[pd.DataFrame], str]:
    for tbl in ("mart_forecast_drift", "int_forecast_drift"):
        schema, name = "main", tbl
        if not _table_exists(conn, schema, name):
            continue
        seg_filter = "AND segment = ?" if segment and segment != "All" else ""
        params = [scenario] + ([segment] if segment and segment != "All" else [])
        sql = f"""
        SELECT DISTINCT month
        FROM main.{name}
        WHERE scenario = ? {seg_filter} AND drift_flag = true
        ORDER BY month
        """
        try:
            df = conn.execute(sql, params).fetchdf()
        except Exception:
            continue
        if df is not None and not df.empty:
            return df, ""
    return None, "Not available (run dbt build: int_forecast_drift; requires prior snapshot)."


def _largest_waterfall_category(row) -> str:
    if row is None or row.empty:
        return "—"
    r = row.iloc[0]
    start = float(r.get("starting_arr") or 0)
    new = float(r.get("new_arr") or 0)
    exp = float(r.get("expansion_arr") or 0)
    cont = float(r.get("contraction_arr") or 0)
    churn = float(r.get("churn_arr") or 0)
    best = max(
        [("New", new), ("Expansion", exp), ("Contraction", cont), ("Churn", churn)],
        key=lambda x: abs(x[1]),
    )
    return f"{best[0]} ({best[1]:,.0f})"


def _format_table(df: pd.DataFrame, columns: Optional[list[str]] = None) -> str:
    if df is None or df.empty:
        return ""
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object" and hasattr(df[c].dtype, "categories"):
            df[c] = df[c].astype(str)
        elif pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].round(2)
    df = df.astype(str).fillna("—")
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    try:
        return df.to_markdown(index=False)
    except AttributeError:
        # Fallback for pandas < 1.0: pipe table
        headers = "| " + " | ".join(df.columns) + " |"
        sep = "| " + " | ".join("---" for _ in df.columns) + " |"
        rows = [
            "| " + " | ".join(str(v) for v in row) + " |"
            for _, row in df.iterrows()
        ]
        return "\n".join([headers, sep] + rows)


def _build_report(
    conn,
    scenario: str,
    segment: str,
    months: int,
    output_path: Path,
) -> None:
    available = _get_available_months(conn, scenario, "exec")
    if not available:
        available = _get_available_months(conn, scenario, "fct")
    if not available:
        report = (
            "# Revenue Intelligence Report\n\n"
            "No forecast data found. Run `dbt build` (and ensure mart_executive_forecast_summary or fct_revenue_forecast* exists).\n"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(report, encoding="utf-8")
        return

    latest_month = available[0]
    selected_months = _select_last_n_months(available, months)

    # Gather all data
    exec_data, exec_note = _exec_summary(conn, scenario, latest_month)
    conf_score, _ = _confidence(conn, scenario, latest_month)
    fva_df, fva_note = _forecast_vs_actual(conn, scenario, segment, selected_months)
    waterfall_df, wf_note = _arr_waterfall(conn, scenario, segment, latest_month)
    explain_df, exp_note = _explainability(conn, scenario, segment, latest_month)
    churn_df, churn_note = _churn_risk_watchlist(conn, segment, latest_month, 10)
    movers_df, movers_note = _top_arr_movers(conn, segment, latest_month, 5)
    coverage_dict, cov_note = _coverage_metrics(conn, scenario, segment, latest_month)
    model_sel_df, model_sel_note = _model_selection(conn)
    renewal_bt, _ = _backtest_metrics(conn, "renewals")
    pipeline_bt, _ = _backtest_metrics(conn, "pipeline")
    drift_df, drift_note = _drift_months(conn, scenario, segment)

    # Executive summary bullets
    bullet_forecast_actual = "—"
    if exec_data:
        f = exec_data["total_forecast_revenue"]
        a = exec_data["total_actual_revenue"]
        bullet_forecast_actual = f"Forecast ${f:,.0f}; Actual ${a:,.0f}."
    bullet_mom = "—"
    if exec_data and exec_data.get("revenue_growth_mom") is not None:
        bullet_mom = f"MoM growth: {exec_data['revenue_growth_mom']:.1%}."
    bullet_confidence = "—"
    if conf_score is not None:
        bullet_confidence = f"Confidence score: {conf_score:.0f}/100."
    bullet_waterfall = _largest_waterfall_category(waterfall_df) if waterfall_df is not None else "—"
    bullet_risk = "—"
    if churn_df is not None and not churn_df.empty:
        low = churn_df[churn_df["p_renew"].astype(float) < 0.7] if "p_renew" in churn_df.columns else churn_df
        n_low = len(low)
        bullet_risk = f"{n_low} renewal(s) in watchlist with low p_renew in latest month."

    lines = [
        f"# Revenue Intelligence Report — {latest_month} — Scenario: {scenario} — Segment: {segment}",
        "",
        "## Executive Summary",
        "",
        f"- **Forecast vs actual:** {bullet_forecast_actual}",
        f"- **MoM change:** {bullet_mom}",
        f"- **Confidence:** {bullet_confidence}",
        f"- **Largest ARR movement category:** {bullet_waterfall}",
        f"- **Top risk headline:** {bullet_risk}",
        "",
    ]

    # Forecast vs Actual
    lines.append("## Forecast vs Actual")
    lines.append("")
    if fva_df is not None and not fva_df.empty:
        cols = ["month", "actual_mrr", "forecast_mrr", "error", "ape"]
        if "forecast_lower" in fva_df.columns and fva_df["forecast_lower"].notna().any():
            cols.extend(["forecast_lower", "forecast_upper"])
        lines.append(_format_table(fva_df, cols))
    else:
        lines.append(fva_note or "No data.")
    lines.append("")

    # ARR Waterfall
    lines.append("## ARR Waterfall (latest month)")
    lines.append("")
    if waterfall_df is not None and not waterfall_df.empty:
        lines.append(_format_table(waterfall_df))
    else:
        lines.append(wf_note or "No data.")
    lines.append("")

    # Drivers of change
    lines.append("## Drivers of change (latest month)")
    lines.append("")
    if explain_df is not None and not explain_df.empty:
        r = explain_df.iloc[0]
        lines.append(
            f"- renewal_driver_delta: {float(r.get('renewal_driver_delta', 0) or 0):,.2f}\n"
            f"- pipeline_driver_delta: {float(r.get('pipeline_driver_delta', 0) or 0):,.2f}\n"
            f"- expansion_driver_delta: {float(r.get('expansion_driver_delta', 0) or 0):,.2f}\n"
            f"- residual: {float(r.get('residual_delta', 0) or 0):,.2f}\n"
            f"- top_driver: {r.get('top_driver', '—')}, share: {float(r.get('top_driver_share_pct', 0) or 0):.0%}"
        )
    else:
        lines.append(exp_note or "No data.")
    lines.append("")

    # Risk Radar
    lines.append("## Risk Radar")
    lines.append("")
    lines.append("### Top 10 churn risks")
    lines.append("")
    if churn_df is not None and not churn_df.empty:
        lines.append(_format_table(churn_df))
    else:
        lines.append(churn_note or "No data.")
    lines.append("")
    lines.append("### Top 5 ARR movers")
    lines.append("")
    if movers_df is not None and not movers_df.empty:
        lines.append(_format_table(movers_df))
    else:
        lines.append(movers_note or "No data.")
    lines.append("")

    # Coverage metrics
    lines.append("## Coverage metrics (latest month)")
    lines.append("")
    if coverage_dict:
        lines.append(
            f"- pipeline_coverage_ratio: {coverage_dict['pipeline_coverage_ratio']:.2f}\n"
            f"- renewal_coverage_ratio: {coverage_dict['renewal_coverage_ratio']:.2f}\n"
            f"- concentration_ratio_top5: {coverage_dict['concentration_ratio_top5']:.2f}"
        )
    else:
        lines.append(cov_note or "No data.")
    lines.append("")

    # Model status
    lines.append("## Model status")
    lines.append("")
    if model_sel_df is not None and not model_sel_df.empty:
        lines.append("### Selected models")
        lines.append("")
        lines.append(_format_table(model_sel_df))
        lines.append("")
    else:
        lines.append(model_sel_note)
        lines.append("")
    lines.append("### Latest backtest metrics")
    lines.append("")
    for name, bt in [("Renewals", renewal_bt), ("Pipeline", pipeline_bt)]:
        if bt is not None and not bt.empty:
            lines.append(f"**{name}**")
            lines.append("")
            lines.append(_format_table(bt))
            lines.append("")
    if (renewal_bt is None or renewal_bt.empty) and (pipeline_bt is None or pipeline_bt.empty):
        lines.append("No backtest metrics (run ML backtest: ml_*_backtest_metrics).")
        lines.append("")
    # Calibration interpretation
    calibration_note = "Calibration: not assessed (no metrics)."
    if renewal_bt is not None and not renewal_bt.empty and "brier" in renewal_bt.columns:
        brier = renewal_bt["brier"].astype(float).mean()
        calibration_note = "Calibration: good." if brier <= 0.25 else "Calibration: needs improvement (elevated Brier score)."
    lines.append(calibration_note)
    lines.append("")

    # Drift monitoring
    lines.append("## Drift monitoring")
    lines.append("")
    if drift_df is not None and not drift_df.empty:
        months_drift = drift_df["month"].astype(str).tolist()
        lines.append("Future months with drift_flag=true: " + ", ".join(months_drift))
    else:
        lines.append(drift_note or "No drift data.")
    lines.append("")

    # Actions
    lines.append("## Actions")
    lines.append("")
    actions = []
    if churn_df is not None and len(churn_df) > 0:
        actions.append("Focus CSM outreach on top churn risks in the watchlist.")
    if coverage_dict is not None:
        if coverage_dict.get("pipeline_coverage_ratio", 1) < 1.0:
            actions.append("Pipeline coverage below 1.0 implies new business may not fully cover plan; review pipeline and conversion assumptions.")
        if coverage_dict.get("concentration_ratio_top5", 0) > 0.5:
            actions.append("High concentration in top 5 customers; confidence and forecast are sensitive to a few accounts.")
    if conf_score is not None and conf_score < 70:
        actions.append("Confidence score below 70; consider improving data coverage or model calibration.")
    while len(actions) < 3:
        actions.append("Review forecast vs actual and adjust planning assumptions as needed.")
        if len(actions) >= 3:
            break
    for a in actions[:3]:
        lines.append(f"- {a}")
    lines.append("")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")


def _main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate narrative revenue intelligence report from DuckDB."
    )
    parser.add_argument(
        "--duckdb-path",
        default=DEFAULT_DUCKDB_PATH,
        help="Path to revenue_forecasting.duckdb",
    )
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO, help="Forecast scenario")
    parser.add_argument("--segment", default=DEFAULT_SEGMENT, help="Segment filter (e.g. All)")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="Last N months for forecast vs actual")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output Markdown path")
    args = parser.parse_args()

    try:
        conn = _connect(args.duckdb_path)
        try:
            _build_report(
                conn,
                scenario=args.scenario,
                segment=args.segment,
                months=args.months,
                output_path=Path(args.output),
            )
        finally:
            conn.close()
        print(f"Report written to {args.output}", file=sys.stderr)
        return 0
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_main())
