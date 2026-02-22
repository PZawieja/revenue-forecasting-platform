# Metrics definitions

Canonical definitions for metrics used in the Revenue Forecasting Platform. All formulas assume the data contract grains and conventions (see [data_contract.md](data_contract.md)).

**Scope:** All metrics are computed **per `company_id`**. Each metric (MRR, churn, pipeline coverage, WAPE, etc.) is defined at company level; comparisons and targets are company-scoped. Metrics can be **rolled up across companies** (e.g. group or portfolio totals) by summing or aggregating company-level results at read/reporting time—the framework does not prescribe a single rollup grain.

---

## MRR (Monthly Recurring Revenue)

**Definition:** Revenue from recurring subscriptions normalized to a monthly amount for a given period.

**Calculation:**

- From `subscription_line_items`: for each **active** line, compute **monthly** revenue:
  - If `billing_frequency = 'monthly'`: `quantity × unit_price × (1 - discount_pct)`.
  - If `billing_frequency = 'annual'`: `(quantity × unit_price × (1 - discount_pct)) / 12`.
- Sum over all active lines whose contract interval overlaps the **target month** (e.g. `contract_start_date <= month_end` and `contract_end_date >= month_start`).
- MRR for a **month** = sum of such monthly-normalized revenue for that month.

**Scope:** Recurring only; one-time or usage-based revenue excluded unless defined separately (e.g. “MRR + usage”).

**Enterprise / large:** Often annual contracts; divide by 12 for MRR. Multi-year contracts still contribute 1/12 of annual value per month until end date.

---

## ARR (Annual Recurring Revenue)

**Definition:** MRR × 12 for a given month (or snapshot date).

**Calculation:** `ARR = MRR × 12` for the same period and same line-item logic as MRR.

**Use:** Reporting, targets, and pipeline coverage (e.g. pipeline / ARR).

---

## Gross revenue churn

**Definition:** Revenue lost in a period from **contractions and cancellations**, excluding expansion. Expressed as a rate of beginning-of-period recurring revenue.

**Calculation:**

- **Churned MRR** (period) = MRR from contracts that were active at **start** of period and are **no longer** active at **end** (cancelled, non-renewed, or ended), plus any **contraction** (reduction in MRR on still-active contracts).
- **Gross revenue churn rate** = Churned MRR in period / MRR at start of period.

**Pitfalls:**

- Count only revenue that actually left; avoid double counting if a contract is amended and replaced.
- Use consistent **as-of** dates (start/end of month) and status rules (e.g. treat `non_renewing` as churn from renewal date).

---

## Net revenue churn

**Definition:** Revenue lost in a period from churn and contraction **minus** revenue gained from **expansion** (upsell, add-ons) within the **existing** customer base. Expressed as a rate of beginning-of-period recurring revenue.

**Calculation:**

- **Net churned MRR** = Churned MRR (as in gross) − Expansion MRR (increase in MRR from existing customers in the period).
- **Net revenue churn rate** = Net churned MRR / MRR at start of period. Can be negative (net expansion).

**Pitfalls:**

- “Existing” = customer had recurring revenue at start of period; new logos are not expansion.
- Expansion must be measured on same MRR logic (same product/contract rules).

---

## Logo churn

**Definition:** Count (or proportion) of **customers** that churned in a period, regardless of revenue size.

**Calculation:**

- **Churned logos** (period) = customers who had at least one active subscription at **start** of period and **no** active subscription at **end** of period (and did not renew).
- **Logo churn rate** = Churned logos / Total customers at start of period.

**Use:** Complements revenue churn; highlights loss of small accounts or many small logos.

**Pitfalls:**

- Parent/child or multi-entity customers: define “customer” once (e.g. billing account) and stick to it.

---

## Cohort churn

**Definition:** Churn measured within a **cohort** (e.g. customers who became paying in the same month or quarter). Typically revenue or logo churn of that cohort in subsequent periods.

**Calculation:**

- Define **cohort** (e.g. by `created_date` or first contract start, bucketed by month/quarter).
- For each cohort and each **period N** after cohort start: compute MRR (or logo count) of that cohort at start of period N and at end of period N.
- **Cohort churn rate (period N)** = (MRR or logos lost in period N) / (MRR or logos at start of period N) for that cohort.

**Use:** Retention curves, cohort retention tables, and LTV assumptions.

**Pitfalls:**

- Cohort definition must be consistent (e.g. first paid month); avoid mixing product or segment unless explicitly sliced.

---

## Pipeline coverage

**Definition:** Ratio of **pipeline value** (often weighted or staged) to **target** (e.g. ARR or revenue to be closed in a period). Indicates whether there is enough pipeline to meet the target.

**Calculation:**

- **Pipeline** = from `pipeline_opportunities_snapshot`: sum of `amount` for opportunities in scope (e.g. open stages only, or stage-weighted). Snapshot = one date (e.g. month-end); one row per opportunity per snapshot.
- **Target** = e.g. ARR gap to plan, or next 12 months’ required new + expansion revenue.
- **Pipeline coverage** = Pipeline / Target (e.g. “2.5x” = 2.5× the target).

**Variants:**

- **Stage-weighted pipeline:** Multiply amount by a probability per stage (e.g. Proposal = 0.5, Negotiation = 0.75, Closed Won = 1.0) before summing.
- **By type:** Separate coverage for new_biz vs expansion vs renewal if targets are split.

**Pitfalls:**

- Use **snapshot_date** only; do not mix in post-close outcomes (leakage).
- Enterprise/large: longer cycles → may use longer-horizon targets and stage curves.

---

## Forecast WAPE (Weighted Absolute Percentage Error)

**Definition:** Accuracy metric for **revenue (or MRR) forecasts** that weights errors by actual size so large accounts don’t dominate.

**Calculation:**

- For each period (e.g. month) and optionally segment or cohort:
  - **Actual** = actual MRR (or revenue) in that period.
  - **Forecast** = forecasted MRR (or revenue) for that period.
- **WAPE** = Σ |Actual − Forecast| / Σ Actual over the evaluation window (e.g. 12 months).
- Expressed as a decimal (e.g. 0.12 = 12%) or percentage (12%).

**Formula:**  
`WAPE = sum(|Actual - Forecast|) / sum(Actual)`

**Use:** Compare models or scenarios; lower WAPE = better accuracy. Weights avoid small-denominator distortion from tiny accounts.

**Pitfalls:**

- If Σ Actual = 0 (no revenue in window), WAPE is undefined; exclude or cap.
- Choose evaluation window (in-sample vs holdout) and level (total, segment, cohort) consistently.
