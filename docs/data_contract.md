# Data Contract: Canonical Input Tables

## Purpose

This contract defines the **canonical input tables** for the Revenue Forecasting Platform. The goal is a **truly reusable, multi-company forecasting framework**: any source system (CRM, billing, product) should be mapped into these shapes so downstream models (staging → intermediate → marts) and metrics (MRR, churn, pipeline coverage) are consistent and comparable **per company** and can be rolled up across companies if desired.

---

## Multi-company design: `company_id`

- **`company_id` is mandatory** in **all** canonical input tables. Every table must include `company_id` so the framework can support multiple tenants (companies, BUs, or brands) in one deployment.
- **All primary keys are composite keys** that include `company_id`. Grains are always scoped to a company; identifiers (`customer_id`, `product_id`, etc.) are unique **within** a company.
- **Type:** `company_id` is a string (opaque, stable identifier for the tenant).
- Downstream models and metrics compute per `company_id`; rollups across companies are optional and done at read/reporting time.

---

## Naming and grain conventions

- **Identifiers:** Use stable, opaque IDs: `company_id`, `customer_id`, `product_id`, `contract_id`, `opportunity_id`. Prefer strings for portability.
- **Dates:** `YYYY-MM-DD` for calendar dates; **month** as first day of month `YYYY-MM-01` for monthly grain.
- **Monetary:** Numeric; currency handling (base currency, FX) is configuration-driven (see [configuration.md](configuration.md)).
- **Rates / percentages:** Decimals 0–1 (e.g. `0.15` = 15%); document if stored as 0–100.
- **Grain:** Always state grain explicitly; primary key = grain columns (including `company_id` where applicable).

---

## Required source tables

### 1. `customers`

| Attribute | Definition |
|-----------|------------|
| **Grain** | One row per customer per company. |
| **Primary key** | `company_id`, `customer_id` |

**Required columns**

| Column | Type | Description |
|--------|------|-------------|
| `company_id` | string | Tenant identifier (mandatory). |
| `customer_id` | string | Unique customer identifier within company. |
| `customer_name` | string | Legal or display name. |
| `segment` | string | Tier: `enterprise`, `large`, `medium`, `smb` (or as defined in config). |
| `created_date` | date | When the customer was created (YYYY-MM-DD). |

**Optional columns**

| Column | Type | Description |
|--------|------|-------------|
| `region` | string | e.g. DACH, EU, US. |
| `industry` | string | e.g. Logistics, Retail, SaaS. |
| `crm_health_input` | integer | Health score 1–10; null if not used. |

**Pitfalls**

- Duplicate `customer_id` from merged accounts or bad ETL → enforce unique.
- `segment` changes over time → use point-in-time or snapshot logic if doing cohort by segment.
- Null `created_date` breaks cohort age; treat as required for forecasting.

**Enterprise / large**

- Fewer rows, higher ACV; longer sales cycles and contract terms. Segment drives term and discount assumptions.

---

### 2. `products`

| Attribute | Definition |
|-----------|------------|
| **Grain** | One row per product per company. |
| **Primary key** | `company_id`, `product_id` |

**Required columns**

| Column | Type | Description |
|--------|------|-------------|
| `company_id` | string | Tenant identifier (mandatory). |
| `product_id` | string | Unique product identifier within company. |
| `product_family` | string | Grouping (e.g. A, B, C, D). |
| `is_recurring` | boolean | Recurring vs one-time. |
| `default_term_months` | integer | Default contract length (e.g. 1, 12, 24, 36). |

**Optional columns**

| Column | Type | Description |
|--------|------|-------------|
| `product_name` | string | Display name. |
| `unit` | string | Seat, license, etc. |

**Pitfalls**

- Product retired but still on active contracts → keep in table; filter by usage in subscriptions.
- `default_term_months` ≠ actual term on line items; use subscription data for actuals.

**Enterprise / large**

- More 24/36 month default terms; recurring products dominate ACV.

---

### 3. `subscription_line_items`

| Attribute | Definition |
|-----------|------------|
| **Grain** | One row per **company × customer × product × contract** (one line per contract line). |
| **Primary key** | `company_id`, `contract_id`, `customer_id`, `product_id` (or `company_id`, `contract_id`, `line_id` if multiple lines per contract). |

**Required columns**

| Column | Type | Description |
|--------|------|-------------|
| `company_id` | string | Tenant identifier (mandatory). |
| `contract_id` | string | Contract identifier within company. |
| `customer_id` | string | Customer (within company). |
| `product_id` | string | Product (within company). |
| `contract_start_date` | date | Start of contract. |
| `contract_end_date` | date | End of contract. |
| `billing_frequency` | string | `monthly` or `annual`. |
| `quantity` | numeric | Seats/units. |
| `unit_price` | numeric | Price per unit per billing period. |
| `status` | string | `active`, `non_renewing`, `cancelled`. |

**Optional columns**

| Column | Type | Description |
|--------|------|-------------|
| `discount_pct` | numeric (0–1) | Discount. |
| `renewal_date` | date | Next renewal (derivable from end date). |

**Pitfalls**

- **Double counting:** Same logical subscription with multiple contracts (amendments, renewals) → define rules for “current” contract or MRR attribution window.
- **Leakage:** Using `contract_end_date` or status from future in historical metrics → use as-of dates / snapshots.
- Null `quantity` or `unit_price` breaks MRR; treat as required for revenue tables.

**Enterprise / large**

- Longer terms (24/36 months), higher quantity and ACV; more multi-year contracts; expansion often as new line or amendment.

---

### 4. `pipeline_opportunities_snapshot`

| Attribute | Definition |
|-----------|------------|
| **Grain** | One row per **company × opportunity × snapshot_date** (point-in-time pipeline). |
| **Primary key** | `company_id`, `snapshot_date`, `opportunity_id` |

**Required columns**

| Column | Type | Description |
|--------|------|-------------|
| `company_id` | string | Tenant identifier (mandatory). |
| `snapshot_date` | date | Date of snapshot (e.g. month-end). |
| `opportunity_id` | string | Opportunity identifier within company. |
| `stage` | string | e.g. Prospecting, Discovery, Proposal, Negotiation, Closed Won, Closed Lost (or as mapped in config). |
| `amount` | numeric | Opportunity value. |
| `opportunity_type` | string | `new_biz`, `expansion`, `renewal`. |

**Optional columns**

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | string | Null for net-new. |
| `segment` | string | Segment for weighting. |
| `expected_close_date` | date | For aging and coverage. |

**Pitfalls**

- **Double counting:** Same opportunity in multiple snapshots with different stages → use snapshot grain; for “pipeline at month-end” take one row per opportunity per snapshot_date.
- **Leakage:** Using closed outcome in pipeline coverage before close date → join only on snapshot_date and stage, not post-close data.
- Stage name changes across systems → map to canonical stage list.

**Enterprise / large**

- Longer cycles; larger deal sizes; more expansion/renewal opportunities; stage definitions may differ (e.g. legal, security).

---

### 5. `usage_monthly`

| Attribute | Definition |
|-----------|------------|
| **Grain** | **company_id × customer_id × month (YYYY-MM-01) × feature_key** (or equivalent usage dimension). |
| **Primary key** | `company_id`, `month`, `customer_id`, `feature_key` |

**Required columns**

| Column | Type | Description |
|--------|------|-------------|
| `company_id` | string | Tenant identifier (mandatory). |
| `month` | date | First day of month. |
| `customer_id` | string | Customer (within company). |
| `feature_key` | string | Feature or usage dimension (e.g. feature_a, feature_b). |
| `usage_count` | numeric | Count of events/usage. |
| `active_users` | numeric | Active users in period (optional but recommended). |

**Optional columns**

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | string | If usage is product-scoped. |

**Pitfalls**

- **Leakage:** Using same-month or future usage to predict churn → lag usage or use prior-month only for leading indicators.
- Sparse rows (missing month × customer × feature) → treat as zero or “no usage” per contract.
- Different definitions of “active” across products → document and align.

**Enterprise / large**

- Higher usage volume; more features; usage often leading indicator for expansion/churn.

---

## Optional source tables

### 6. `support_tickets_monthly` (optional)

| Attribute | Definition |
|-----------|------------|
| **Grain** | **company_id × customer_id × month**. |
| **Primary key** | `company_id`, `month`, `customer_id` |

**Suggested columns**

- `company_id` (string), `month` (date), `customer_id` (string), `tickets_opened` (integer), `tickets_closed` (integer), `avg_resolution_days` (numeric), optionally `severity_high_count` (integer).

**Pitfalls**

- Aggregation level (ticket vs case) must be consistent; avoid double counting if one ticket spans months.

---

### 7. `payments_monthly` (optional)

| Attribute | Definition |
|-----------|------------|
| **Grain** | **company_id × customer_id × month** (or **company_id × customer_id × product_id × month** if needed). |
| **Primary key** | `company_id`, `month`, `customer_id` (and `product_id` if in grain). |

**Suggested columns**

- `company_id` (string), `month` (date), `customer_id` (string), `amount_collected` (numeric), `amount_invoiced` (numeric), `currency` (string). Optional: `product_id`, `payment_status`.

**Pitfalls**

- Timing: cash vs accrual; invoice date vs payment date. Define which is used for revenue recognition and forecast alignment.
- Refunds and credits → net vs gross; document clearly.

---

### 8. `fx_rates_monthly` (optional)

| Attribute | Definition |
|-----------|------------|
| **Grain** | **company_id × month × from_currency** (or **company_id × month** if single base per company). |
| **Primary key** | `company_id`, `month`, `from_currency` (or `company_id`, `month` if one rate set per month). |

**Suggested columns**

- `company_id` (string), `month` (date), `base_currency` (string), `to_currency` (string), `rate` (numeric). Optional: `from_currency` if multiple source currencies. Used for converting to base currency when config specifies FX conversion (see [configuration.md](configuration.md)).

**Pitfalls**

- Rate convention (e.g. 1 USD = X EUR) must be consistent; document which currency is the base in config.
- Missing month → use last known rate or config default.

---

## Summary

- **Multi-company:** `company_id` is mandatory in all canonical tables; all primary keys are composite and include `company_id`.
- **Required:** `customers`, `products`, `subscription_line_items`, `pipeline_opportunities_snapshot`, `usage_monthly`.
- **Optional:** `support_tickets_monthly`, `payments_monthly`, `fx_rates_monthly`.
- All tables use stable IDs, explicit grains, and documented pitfalls; enterprise/large segments are called out where behavior (terms, ACV, cycles) differs.
