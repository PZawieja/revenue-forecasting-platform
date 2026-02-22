# Revenue Intelligence Cockpit — Founder one-pager

One-page pitch for the product built on this platform.

---

## Product name

**Revenue Intelligence Cockpit**

---

## Ideal customer profile (ICP)

- **RevOps, Finance, and Customer Success leaders** in B2B subscription and SaaS.
- Teams that care about **recurring revenue accuracy**, **board-ready ARR storytelling**, and **early risk detection**.
- Orgs tired of **spreadsheet-based plans** and **CRM snapshots** with no single source of truth.

---

## Value proposition

- **Accurate, explainable forecasting** — Scenario-based (base/upside/downside) with confidence intervals; logic is deterministic first, ML-calibrated where it helps.
- **Early risk detection** — Churn risk watchlist and top ARR movers so CS and sales know who to focus on and why.

---

## Differentiators

| Differentiator | What it means |
|----------------|----------------|
| **Waterfall reconciliation** | ARR movement (new, expansion, churn) ties to a single bridge; reconciliation checks keep the story auditable. |
| **Confidence scoring** | Forecasts come with confidence and pipeline coverage so you plan on ranges, not blind point estimates. |
| **ML calibration** | Renewal and pipeline models are calibrated and backtested; champion/challenger selection picks the best model per domain. |
| **Drift monitoring** | Quality gates and backtest metrics in CI so model degradation is caught before it hits the board. |
| **Governed outputs** | dbt lineage, tests, and a clear data contract; one DuckDB warehouse that can run locally or in CI. |

---

## MVP scope (v1)

- Ingest via **data contract** (CSV seeds or mapped CRM/billing/usage/pipeline).
- **dbt** staging → marts → **deterministic forecast engine** (renewals, expansion, new biz).
- **Optional ML** (renewals + pipeline), backtests, calibration, champion selection.
- **Executive Cockpit** (Streamlit): Home, Forecast vs Actual, ARR Waterfall, Risk Radar, Model Intelligence.
- **Run locally** or in CI; DuckDB as the single warehouse.

---

## v2 scope (product direction)

- **Connectors** to live CRM, billing, and usage sources.
- **Scheduled runs** and refresh pipelines.
- **Hosted cockpit** (multi-tenant or single-tenant).
- **Alerts** on risk and reconciliation (e.g. Slack/email).
- **Role-based views** (Finance vs CS vs RevOps).
- **Audit and lineage** in the UI (who saw what, when; which model version).

---

## Pricing idea (high-level)

| Tier | Positioning | Notional scope |
|------|--------------|----------------|
| **Starter** | Single team, single source of truth, core forecast + waterfall. | SMB / single division; limited history and connectors. |
| **Growth** | Full cockpit, risk radar, ML calibration, reconciliation and alerts. | Mid-market; more history, scheduled runs, key connectors. |
| **Enterprise** | Governance, drift monitoring, audit, SLA, dedicated support. | Large orgs; full connectors, SSO, custom models or assumptions. |

*No specific list prices here; structure supports a land-and-expand motion from “one source of truth” to “full revenue intelligence.”*

---

## Summary

**Revenue Intelligence Cockpit** gives RevOps, Finance, and CS leaders **accurate, explainable forecasting** and **early risk detection** on a **governed pipeline**—waterfall reconciliation, confidence scoring, ML calibration, and drift monitoring. MVP is this repo (data contract → dbt → forecast engine → optional ML → Streamlit cockpit); v2 is connectors, scheduling, hosted cockpit, and tiers from Starter to Enterprise.
