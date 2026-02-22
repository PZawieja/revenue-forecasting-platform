# Configuration: Behavior via Config Seeds

The Revenue Forecasting Platform is controlled by **config seeds** (YAML or CSV loaded as dbt seeds). Behavior is driven by these configs so the same codebase can be reused across companies and use cases without hardcoding assumptions. This doc describes what is controlled and how.

---

## Overview

- **Config seeds** live in `dbt/seeds/` (e.g. CSV or YAML-backed tables). Models and macros read them via `ref('seed_name')`.
- All config is **scoped by `company_id`** where multi-tenant; single-tenant deployments may use a single default company.
- Changes to config require a `dbt seed` (and optionally `dbt run`) to take effect; no code change.

---

## 1. Segment definitions and baselines

**Purpose:** Define which customer segments exist and their baseline parameters (e.g. renewal baselines, expansion uplift).

**Typical seed(s):** `segment_config` or equivalent.

**Controls:**

- **Segment keys:** e.g. `enterprise`, `large`, `medium`, `smb` (or company-specific labels).
- **Segment grouping:** Mapping of segment → `segment_group` (e.g. `enterprise_large` vs `mid_smb`) for behavior that differs by group.
- **Renewal baselines:** Base renewal probability per segment (e.g. enterprise 0.92, smb 0.75) used in renewal probability and forecast components.
- **Expansion baselines:** Base uplift percentages or curves by segment/segment_group (e.g. enterprise_large growing 1.5%, mid_smb flat 0.3%).

**Use in models:** Staging or intermediate models join segment config to apply baselines; segment lists drive `accepted_values` tests.

---

## 2. Stage mappings and probabilities

**Purpose:** Map raw pipeline stages to canonical stages and assign close (or stage-exit) probabilities per segment for pipeline weighting.

**Typical seed(s):** `stage_mapping`, `stage_probabilities` (or inline in code until moved to seed).

**Controls:**

- **Stage mapping:** Source stage name → canonical stage (e.g. Prospecting → prospecting, Closed Won → closed won).
- **Stage probabilities:** Per (segment, stage): probability of closing (or moving forward), e.g. `p_base`, `p_upside`, `p_downside` for scenario-aware pipeline value.
- **Canonical stage list:** Prospecting, Discovery, Proposal, Negotiation, Closed Won, Closed Lost (or config-defined list).

**Use in models:** Staging normalizes stage; intermediate pipeline models join stage probabilities to compute expected value by scenario.

---

## 3. Slippage assumptions

**Purpose:** Model delay between expected close date and actual start (pipeline slippage) for new business and expansion.

**Typical seed(s):** `slippage_config` or columns in a pipeline/forecast config.

**Controls:**

- **Slippage distribution or default:** e.g. probability of 0-, 1-, 2-month delay by segment or opportunity type.
- **Expected start month:** How to derive “expected start month” from expected close date (e.g. close month + slippage offset).

**Use in models:** Pipeline weighted value or new_biz component uses slippage to allocate expected value to future months.

---

## 4. Health score weights by segment_group

**Purpose:** Define how health score (e.g. 0–1 or 1–10) is computed from CRM, usage, and trend inputs, with weights that can differ by segment group.

**Typical seed(s):** `health_score_weights` or `segment_weights`.

**Controls:**

- **Inputs:** Which inputs are used (e.g. CRM health 1–10, usage score, trend score).
- **Weights per segment_group:** e.g. enterprise_large: 50% CRM, 35% usage, 15% trend; mid_smb: 30% CRM, 50% usage, 20% trend.
- **Clamping:** Min/max and scaling (e.g. raw 0–1 → 1–10).

**Use in models:** Intermediate health model (e.g. int_customer_health_monthly) joins config to compute health_score_1_10 per customer/month.

---

## 5. Onboarding / start-lag assumptions for new biz

**Purpose:** Model the lag between contract start and revenue recognition or “go-live” for new business (e.g. implementation period).

**Typical seed(s):** `onboarding_config` or `new_biz_start_lag`.

**Controls:**

- **Start-lag by segment or type:** e.g. enterprise +2 months, smb +0 months from close to first recognized MRR.
- **Allocation rule:** Whether to allocate full expected value to “start month” or spread over first N months.

**Use in models:** New business forecast component shifts or spreads expected MRR by start-lag so forecast aligns with when revenue is recognized.

---

## 6. Currency configuration

**Purpose:** Define base currency and how to convert non-base amounts so all metrics are comparable within and across companies.

**Typical seed(s):** `currency_config`, optionally `fx_rates_monthly` (see [data_contract.md](data_contract.md)).

**Controls:**

- **Base currency:** ISO code (e.g. USD, EUR) for the company or deployment. All reported MRR/forecasts are in base unless otherwise stated.
- **FX conversion approach:**  
  - **Single currency:** No conversion; all source data assumed in base.  
  - **Multi-currency with rates:** Use `fx_rates_monthly` (or equivalent) to convert to base by month; rate convention (e.g. 1 base = X foreign) must be documented.  
  - **Static rate:** One rate per currency pair (e.g. for backfill or simple setups).
- **Rounding and precision:** How to round converted amounts (e.g. 2 decimals).

**Use in models:** Staging or an early intermediate layer converts amounts to base using config and optional FX seed; downstream models see base-currency only.

---

## Summary

| Area | Config concept | Typical seed / table |
|------|----------------|----------------------|
| Segments | Definitions, baselines, grouping | segment_config, segment_baselines |
| Pipeline | Stage mapping, close probabilities | stage_mapping, stage_probabilities |
| Slippage | Delay from close to start | slippage_config |
| Health | Weights by segment_group | health_score_weights |
| New biz | Start-lag, onboarding | onboarding_config, new_biz_start_lag |
| Currency | Base currency, FX approach | currency_config, fx_rates_monthly |

All behavior above is intended to be config-driven so the framework remains reusable and multi-company without code changes.
