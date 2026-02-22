"""
One-click Export Pack: generate artifact CSVs, narrative Markdown, and PDF report
using existing Python modules (no shell). Used by the Streamlit cockpit.
"""

from __future__ import annotations

import sys
import zipfile
from pathlib import Path
from typing import Any

# Ensure repo root is on path so forecasting.* can be imported
_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))


def _repo_root() -> Path:
    return _REPO_ROOT


def _ensure_dirs(repo_root: Path) -> tuple[Path, Path]:
    artifacts_dir = repo_root / "docs" / "artifacts"
    reports_dir = repo_root / "docs" / "reports"
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    reports_dir.mkdir(parents=True, exist_ok=True)
    return artifacts_dir, reports_dir


def _actionable_message(exc: BaseException) -> str:
    """Turn an exception into an actionable message for the user."""
    msg = str(exc).strip()
    if "mart_executive_forecast_summary" in msg or "fct_revenue_forecast" in msg or "does not exist" in msg:
        return "Missing forecast tables. Run: make build (or make demo_pack)"
    if "mart_arr_waterfall" in msg or "mart_churn_risk" in msg or "mart_top_arr" in msg:
        return "Missing marts. Run: make build (or make demo_pack)"
    if "ml_model_selection" in msg or "ml_renewal_backtest" in msg or "ml_pipeline_backtest" in msg:
        return "Missing ML tables. Run: make build (or make demo_pack)"
    if "duckdb" in msg.lower() or "not found" in msg.lower():
        return "DuckDB not found or not readable. Run: make build (or make demo_pack)"
    return f"{type(exc).__name__}: {msg}"


def generate_export_pack(
    db_path: str | Path,
    scenario: str = "base",
    months: int = 6,
) -> dict[str, Any]:
    """
    Generate export pack: CSVs in docs/artifacts/, Markdown and PDF in docs/reports/.
    Calls existing Python modules directly (no shell). Returns dict with keys:
      - artifacts: list of paths to generated CSV files
      - reports: list of paths to .md and .pdf
      - errors: list of actionable error strings (non-fatal per step)
      - zip_path: path to export_pack.zip if created (optional)
    """
    db_path = Path(db_path)
    repo_root = _repo_root()
    artifacts_dir, reports_dir = _ensure_dirs(repo_root)

    out: dict[str, Any] = {
        "artifacts": [],
        "reports": [],
        "errors": [],
        "zip_path": None,
    }

    # 1) CSV artifacts (existing module; uses base scenario internally)
    try:
        from forecasting.src import export_artifacts as exp_art

        exp_art.export_artifacts(db_path, artifacts_dir, warehouse_dir=db_path.parent)
        for f in sorted(artifacts_dir.glob("*.csv")):
            out["artifacts"].append(str(f))
    except FileNotFoundError as e:
        out["errors"].append(_actionable_message(e))
    except Exception as e:
        out["errors"].append(_actionable_message(e))

    # 2) Narrative Markdown report (uses selected scenario)
    md_path = reports_dir / "revenue_intelligence_report.md"
    try:
        from forecasting.src import narrative_report as nr

        conn = nr._connect(str(db_path))
        try:
            nr._build_report(
                conn,
                scenario=scenario,
                segment="All",
                months=months,
                output_path=md_path,
            )
            out["reports"].append(str(md_path))
        finally:
            conn.close()
    except FileNotFoundError as e:
        out["errors"].append(_actionable_message(e))
    except Exception as e:
        out["errors"].append(_actionable_message(e))

    # 3) PDF report (uses selected scenario; reuse conn for efficiency is optional, we open again for simplicity)
    pdf_path = reports_dir / "revenue_intelligence_report.pdf"
    try:
        from forecasting.src import pdf_report as pdf_mod

        conn = pdf_mod.nr._connect(str(db_path))
        try:
            pdf_mod.build_pdf(
                conn,
                scenario=scenario,
                segment="All",
                months=months,
                output_path=pdf_path,
            )
            out["reports"].append(str(pdf_path))
        finally:
            conn.close()
    except FileNotFoundError as e:
        out["errors"].append(_actionable_message(e))
    except Exception as e:
        out["errors"].append(_actionable_message(e))

    # 4) Optional: ZIP of CSVs
    csv_files = list(artifacts_dir.glob("*.csv"))
    if csv_files:
        zip_path = artifacts_dir / "export_pack.zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
                for f in sorted(csv_files):
                    zf.write(f, f.name)
            out["zip_path"] = str(zip_path)
        except Exception as e:
            out["errors"].append(f"ZIP creation: {e}")

    return out
