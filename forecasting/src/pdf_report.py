"""
Investor/Board-ready PDF export of revenue intelligence report.
Reads from DuckDB (same sources as narrative_report), produces a multi-page PDF
with executive summary, forecast vs actual chart/table, ARR waterfall, risks, and model governance.
Uses ReportLab for PDF and matplotlib for charts. No external services.
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pandas as pd

# Reuse data loading from narrative_report to keep one source of truth
try:
    from . import narrative_report as nr
except ImportError:
    import narrative_report as nr  # noqa: F401

try:
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import inch
    from reportlab.platypus import Image, PageBreak, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle
except ImportError as e:
    raise ImportError("reportlab is required for PDF export; install with pip install reportlab") from e

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError as e:
    raise ImportError("matplotlib is required for PDF charts; install with pip install matplotlib") from e

try:
    import duckdb
except ImportError:
    duckdb = None  # type: ignore

DEFAULT_DUCKDB_PATH = "./warehouse/revenue_forecasting.duckdb"
DEFAULT_SCENARIO = "base"
DEFAULT_SEGMENT = "All"
DEFAULT_MONTHS = 6
DEFAULT_OUTPUT = "./docs/reports/revenue_intelligence_report.pdf"


def _gather_data(conn, scenario: str, segment: str, months: int):
    """Gather all report data using narrative_report helpers. Returns (latest_month, selected_months, data_dict)."""
    available = nr._get_available_months(conn, scenario, "exec")
    if not available:
        available = nr._get_available_months(conn, scenario, "fct")
    if not available:
        return None, [], {}

    latest_month = available[0]
    selected_months = nr._select_last_n_months(available, months)

    exec_data, _ = nr._exec_summary(conn, scenario, latest_month)
    conf_score, _ = nr._confidence(conn, scenario, latest_month)
    fva_df, fva_note = nr._forecast_vs_actual(conn, scenario, segment, selected_months)
    waterfall_df, wf_note = nr._arr_waterfall(conn, scenario, segment, latest_month)
    churn_df, churn_note = nr._churn_risk_watchlist(conn, segment, latest_month, 10)
    movers_df, movers_note = nr._top_arr_movers(conn, segment, latest_month, 5)
    coverage_dict, cov_note = nr._coverage_metrics(conn, scenario, segment, latest_month)
    model_sel_df, model_sel_note = nr._model_selection(conn)
    renewal_bt, _ = nr._backtest_metrics(conn, "renewals")
    pipeline_bt, _ = nr._backtest_metrics(conn, "pipeline")
    drift_df, drift_note = nr._drift_months(conn, scenario, segment)

    bullets = _exec_bullets(exec_data, conf_score, waterfall_df, churn_df, nr._largest_waterfall_category)
    actions = _action_bullets(churn_df, coverage_dict, conf_score)

    return latest_month, selected_months, {
        "exec_data": exec_data,
        "conf_score": conf_score,
        "fva_df": fva_df,
        "fva_note": fva_note,
        "waterfall_df": waterfall_df,
        "wf_note": wf_note,
        "churn_df": churn_df,
        "churn_note": churn_note,
        "movers_df": movers_df,
        "movers_note": movers_note,
        "coverage_dict": coverage_dict,
        "cov_note": cov_note,
        "model_sel_df": model_sel_df,
        "model_sel_note": model_sel_note,
        "renewal_bt": renewal_bt,
        "pipeline_bt": pipeline_bt,
        "drift_df": drift_df,
        "drift_note": drift_note,
        "bullets": bullets,
        "actions": actions,
    }


def _exec_bullets(exec_data, conf_score, waterfall_df, churn_df, largest_cat_fn):
    b1 = "—"
    if exec_data:
        f = exec_data["total_forecast_revenue"]
        a = exec_data["total_actual_revenue"]
        b1 = f"Forecast ${f:,.0f}; Actual ${a:,.0f}."
    b2 = "—"
    if exec_data and exec_data.get("revenue_growth_mom") is not None:
        b2 = f"MoM growth: {exec_data['revenue_growth_mom']:.1%}."
    b3 = "—"
    if conf_score is not None:
        b3 = f"Confidence score: {conf_score:.0f}/100."
    b4 = largest_cat_fn(waterfall_df) if waterfall_df is not None else "—"
    b5 = "—"
    if churn_df is not None and not churn_df.empty and "p_renew" in churn_df.columns:
        low = churn_df[churn_df["p_renew"].astype(float) < 0.7]
        b5 = f"{len(low)} renewal(s) in watchlist with low p_renew in latest month."
    return [b1, b2, b3, b4, b5]


def _action_bullets(churn_df, coverage_dict, conf_score):
    actions = []
    if churn_df is not None and len(churn_df) > 0:
        actions.append("Focus CSM outreach on top churn risks in the watchlist.")
    if coverage_dict is not None:
        if coverage_dict.get("pipeline_coverage_ratio", 1) < 1.0:
            actions.append("Pipeline coverage below 1.0; review pipeline and conversion assumptions.")
        if coverage_dict.get("concentration_ratio_top5", 0) > 0.5:
            actions.append("High concentration in top 5; forecast sensitive to a few accounts.")
    if conf_score is not None and conf_score < 70:
        actions.append("Confidence below 70; consider improving data coverage or model calibration.")
    while len(actions) < 3:
        actions.append("Review forecast vs actual and adjust planning assumptions as needed.")
        if len(actions) >= 3:
            break
    return actions[:3]


def _df_to_table_data(df: pd.DataFrame, columns: Optional[list[str]] = None) -> list[list[str]]:
    """DataFrame to list of lists for ReportLab Table; deterministic order."""
    if df is None or df.empty:
        return []
    df = df.copy()
    for c in df.columns:
        if pd.api.types.is_numeric_dtype(df[c]):
            df[c] = df[c].round(2)
    df = df.astype(str).fillna("—")
    if columns:
        df = df[[c for c in columns if c in df.columns]]
    return [df.columns.tolist()] + df.values.tolist()


def _draw_forecast_chart(fva_df: pd.DataFrame, out_path: Path) -> None:
    """Line chart: actual_mrr vs forecast_mrr; optional lower/upper bands. Light background."""
    if fva_df is None or fva_df.empty or "month" not in fva_df.columns:
        return
    fig, ax = plt.subplots(figsize=(6, 2.5), facecolor="white")
    ax.set_facecolor("white")
    months = fva_df["month"].astype(str).tolist()
    x = list(range(len(months)))
    actual = fva_df["actual_mrr"].astype(float).tolist()
    forecast = fva_df["forecast_mrr"].astype(float).tolist()
    ax.plot(x, actual, "o-", color="black", label="Actual MRR", linewidth=1.5)
    ax.plot(x, forecast, "s--", color="gray", label="Forecast MRR", linewidth=1)
    if "forecast_lower" in fva_df.columns and fva_df["forecast_lower"].notna().any():
        lower = fva_df["forecast_lower"].astype(float).tolist()
        upper = fva_df["forecast_upper"].astype(float).tolist()
        ax.fill_between(x, lower, upper, color="lightgray", alpha=0.5, label="Interval")
    ax.set_xticks(x)
    ax.set_xticklabels(months, rotation=45, ha="right")
    ax.set_ylabel("MRR")
    ax.legend(loc="best", fontsize=8)
    ax.grid(True, linestyle=":", alpha=0.7)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _draw_waterfall_chart(waterfall_df: pd.DataFrame, out_path: Path) -> None:
    """Simple bar chart of ARR components (starting, new, expansion, contraction, churn, ending)."""
    if waterfall_df is None or waterfall_df.empty:
        return
    row = waterfall_df.iloc[0]
    labels = ["Starting", "New", "Expansion", "Contraction", "Churn", "Ending"]
    vals = [
        float(row.get("starting_arr") or 0),
        float(row.get("new_arr") or 0),
        float(row.get("expansion_arr") or 0),
        -float(row.get("contraction_arr") or 0),
        -float(row.get("churn_arr") or 0),
        float(row.get("ending_arr") or 0),
    ]
    fig, ax = plt.subplots(figsize=(6, 2.5), facecolor="white")
    ax.set_facecolor("white")
    colors_bar = ["#1f77b4", "#2ca02c", "#2ca02c", "#d62728", "#d62728", "#1f77b4"]
    bars = ax.bar(labels, vals, color=colors_bar, edgecolor="black", linewidth=0.5)
    ax.axhline(0, color="black", linewidth=0.5)
    ax.set_ylabel("ARR")
    ax.tick_params(axis="x", rotation=45)
    fig.tight_layout()
    fig.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close(fig)


def _table_style():
    return TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#e0e0e0")),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
        ("ALIGN", (0, 0), (-1, -1), "CENTER"),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 8),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 6),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
    ])


def build_pdf(
    conn,
    scenario: str,
    segment: str,
    months: int,
    output_path: Path,
    generated_at: Optional[datetime] = None,
) -> None:
    latest_month, selected_months, data = _gather_data(conn, scenario, segment, months)
    if latest_month is None:
        # No data: write a single-page PDF with message
        doc = SimpleDocTemplate(
            str(output_path),
            pagesize=letter,
            rightMargin=0.75 * inch,
            leftMargin=0.75 * inch,
            topMargin=0.75 * inch,
            bottomMargin=0.75 * inch,
        )
        styles = getSampleStyleSheet()
        story = [
            Paragraph("Revenue Intelligence Report", styles["Title"]),
            Spacer(1, 0.25 * inch),
            Paragraph("No forecast data found.", styles["Normal"]),
            Paragraph("Section unavailable—run dbt build (and ensure mart_executive_forecast_summary or fct_revenue_forecast* exists).", styles["Normal"]),
        ]
        doc.build(story)
        return

    if generated_at is None:
        generated_at = datetime.now(timezone.utc)
    gen_str = generated_at.strftime("%Y-%m-%d %H:%M UTC")

    doc = SimpleDocTemplate(
        str(output_path),
        pagesize=letter,
        rightMargin=0.75 * inch,
        leftMargin=0.75 * inch,
        topMargin=0.75 * inch,
        bottomMargin=0.75 * inch,
    )
    styles = getSampleStyleSheet()
    story = []

    # ----- Page 1: Executive Summary -----
    story.append(Paragraph("Revenue Intelligence Report", styles["Title"]))
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph(
        f"Latest month: {latest_month}  |  Scenario: {scenario}  |  Segment: {segment}  |  Generated: {gen_str}",
        styles["Normal"],
    ))
    story.append(Spacer(1, 0.25 * inch))
    story.append(Paragraph("Executive Summary", styles["Heading2"]))
    story.append(Spacer(1, 0.1 * inch))
    for b in data["bullets"]:
        story.append(Paragraph(f"• {b}", styles["Normal"]))
    story.append(PageBreak())

    # ----- Page 2: Forecast vs Actual -----
    story.append(Paragraph("Forecast vs Actual", styles["Heading2"]))
    story.append(Spacer(1, 0.15 * inch))
    fva_df = data["fva_df"]
    if fva_df is not None and not fva_df.empty:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            tmp_chart = Path(f.name)
        try:
            _draw_forecast_chart(fva_df, tmp_chart)
            story.append(Image(str(tmp_chart), width=5.5 * inch, height=2.2 * inch))
            tmp_chart.unlink(missing_ok=True)
        except Exception:
            tmp_chart.unlink(missing_ok=True)
        story.append(Spacer(1, 0.15 * inch))
        cols = ["month", "actual_mrr", "forecast_mrr", "error", "ape"]
        if "forecast_lower" in fva_df.columns and fva_df["forecast_lower"].notna().any():
            cols.extend(["forecast_lower", "forecast_upper"])
        tdata = _df_to_table_data(fva_df, cols)
        if tdata:
            t = Table(tdata, colWidths=[1.0 * inch] + [0.9 * inch] * (len(tdata[0]) - 1))
            t.setStyle(_table_style())
            story.append(t)
    else:
        story.append(Paragraph(f"Section unavailable—run dbt build. ({data.get('fva_note', '')})", styles["Normal"]))
    story.append(PageBreak())

    # ----- Page 3: ARR Waterfall -----
    story.append(Paragraph("ARR Waterfall (latest month)", styles["Heading2"]))
    story.append(Spacer(1, 0.15 * inch))
    wf_df = data["waterfall_df"]
    if wf_df is not None and not wf_df.empty:
        with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
            tmp_chart = Path(f.name)
        try:
            _draw_waterfall_chart(wf_df, tmp_chart)
            story.append(Image(str(tmp_chart), width=5.5 * inch, height=2.2 * inch))
            tmp_chart.unlink(missing_ok=True)
        except Exception:
            tmp_chart.unlink(missing_ok=True)
        story.append(Spacer(1, 0.15 * inch))
        tdata = _df_to_table_data(wf_df)
        if tdata:
            t = Table(tdata, colWidths=[0.85 * inch] * len(tdata[0]))
            t.setStyle(_table_style())
            story.append(t)
    else:
        story.append(Paragraph(f"Section unavailable—run dbt build: mart_arr_waterfall_monthly. ({data.get('wf_note', '')})", styles["Normal"]))
    story.append(PageBreak())

    # ----- Page 4: Risks & Actions -----
    story.append(Paragraph("Risks & Actions", styles["Heading2"]))
    story.append(Spacer(1, 0.1 * inch))
    story.append(Paragraph("<b>Top 10 churn risks</b>", styles["Normal"]))
    churn_df = data["churn_df"]
    if churn_df is not None and not churn_df.empty:
        tdata = _df_to_table_data(churn_df)
        if tdata:
            ncol = len(tdata[0])
            t = Table(tdata, colWidths=[5.5 * inch / max(ncol, 1)] * ncol)
            t.setStyle(_table_style())
            story.append(t)
    else:
        story.append(Paragraph(f"Section unavailable—run dbt build: mart_churn_risk_watchlist. ({data.get('churn_note', '')})", styles["Normal"]))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph("<b>Top 5 ARR movers</b>", styles["Normal"]))
    movers_df = data["movers_df"]
    if movers_df is not None and not movers_df.empty:
        tdata = _df_to_table_data(movers_df)
        if tdata:
            ncol = len(tdata[0])
            t = Table(tdata, colWidths=[5.5 * inch / max(ncol, 1)] * ncol)
            t.setStyle(_table_style())
            story.append(t)
    else:
        story.append(Paragraph(f"Section unavailable—run dbt build: mart_top_arr_movers. ({data.get('movers_note', '')})", styles["Normal"]))
    story.append(Spacer(1, 0.2 * inch))
    story.append(Paragraph("<b>Actions</b>", styles["Normal"]))
    for a in data["actions"]:
        story.append(Paragraph(f"• {a}", styles["Normal"]))
    story.append(PageBreak())

    # ----- Page 5: Model & Governance (optional) -----
    has_ml = (
        (data["model_sel_df"] is not None and not data["model_sel_df"].empty)
        or (data["renewal_bt"] is not None and not data["renewal_bt"].empty)
        or (data["pipeline_bt"] is not None and not data["pipeline_bt"].empty)
    )
    if has_ml or data["coverage_dict"] or data["conf_score"] is not None or (data["drift_df"] is not None and not data["drift_df"].empty):
        story.append(Paragraph("Model & Governance", styles["Heading2"]))
        story.append(Spacer(1, 0.1 * inch))
        if data["model_sel_df"] is not None and not data["model_sel_df"].empty:
            story.append(Paragraph("<b>Champion selection</b>", styles["Normal"]))
            tdata = _df_to_table_data(data["model_sel_df"])
            if tdata:
                ncol = len(tdata[0])
                t = Table(tdata, colWidths=[5.5 * inch / max(ncol, 1)] * ncol)
                t.setStyle(_table_style())
                story.append(t)
            story.append(Spacer(1, 0.1 * inch))
        if (data["renewal_bt"] is not None and not data["renewal_bt"].empty) or (data["pipeline_bt"] is not None and not data["pipeline_bt"].empty):
            story.append(Paragraph("<b>Latest backtest metrics (AUC / logloss / brier)</b>", styles["Normal"]))
            for name, bt in [("Renewals", data["renewal_bt"]), ("Pipeline", data["pipeline_bt"])]:
                if bt is not None and not bt.empty:
                    tdata = _df_to_table_data(bt)
                    if tdata:
                        ncol = len(tdata[0])
                        t = Table(tdata, colWidths=[5.5 * inch / max(ncol, 1)] * ncol)
                        t.setStyle(_table_style())
                        story.append(t)
                    story.append(Spacer(1, 0.08 * inch))
        if data["conf_score"] is not None or data["coverage_dict"]:
            story.append(Paragraph("<b>Confidence & coverage</b>", styles["Normal"]))
            lines = []
            if data["conf_score"] is not None:
                lines.append(f"Confidence score: {data['conf_score']:.0f}/100.")
            if data["coverage_dict"]:
                c = data["coverage_dict"]
                lines.append(f"Pipeline coverage ratio: {c['pipeline_coverage_ratio']:.2f}; Renewal coverage: {c['renewal_coverage_ratio']:.2f}; Concentration (top 5): {c['concentration_ratio_top5']:.2f}.")
            story.append(Paragraph(" ".join(lines), styles["Normal"]))
        if data["drift_df"] is not None and not data["drift_df"].empty:
            months_drift = data["drift_df"]["month"].astype(str).tolist()
            story.append(Paragraph("Drift flags (months): " + ", ".join(months_drift), styles["Normal"]))
    else:
        story.append(Paragraph("Model & Governance", styles["Heading2"]))
        story.append(Paragraph("Section unavailable—run ML pipeline (publish model selection, train, backtest) and dbt build for coverage/confidence.", styles["Normal"]))

    doc.build(story)


def _main() -> int:
    parser = argparse.ArgumentParser(description="Generate investor-ready PDF revenue intelligence report from DuckDB.")
    parser.add_argument("--duckdb-path", default=DEFAULT_DUCKDB_PATH, help="Path to revenue_forecasting.duckdb")
    parser.add_argument("--scenario", default=DEFAULT_SCENARIO, help="Forecast scenario")
    parser.add_argument("--segment", default=DEFAULT_SEGMENT, help="Segment filter (e.g. All)")
    parser.add_argument("--months", type=int, default=DEFAULT_MONTHS, help="Last N months for forecast vs actual")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="Output PDF path")
    args = parser.parse_args()

    try:
        conn = nr._connect(args.duckdb_path)
        try:
            output_path = Path(args.output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            build_pdf(conn, scenario=args.scenario, segment=args.segment, months=args.months, output_path=output_path)
        finally:
            conn.close()
        print(f"PDF written to {args.output}", file=sys.stderr)
        return 0
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 1
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(_main())
