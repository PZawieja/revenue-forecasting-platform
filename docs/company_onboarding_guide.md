# Company Onboarding Guide

A practical guide to mapping common source systems into this platform’s **canonical data contract**, so you can onboard new companies without changing code—only data and config.

---

## 1. Overview

### What “canonical inputs” mean in this repo

The platform expects data in **canonical input tables** (seeds or tables that staging models read from). These tables define a **contract**: column names, grains, and allowed values. Your job when onboarding a company is to **map** each source system (CRM, billing, usage) into this contract. The dbt project then builds staging → intermediate → marts from that contract. **Ingestion** (how data gets from Salesforce, Stripe, etc. into your warehouse) is out of scope here; this guide focuses on the **mapping layer** that produces the canonical tables.

### Required canonical tables (all include `company_id`)

| Table | Purpose |
|-------|--------|
| **customers** | Customer master: one row per customer per company. Drives segment, health inputs, and FK for subscriptions/pipeline/usage. |
| **products** | Product catalog: one row per product per company. Drives subscription line-item product references. |
| **subscription_line_items** | Contracted recurring revenue drivers: one row per contract line (customer × product × contract). Must be expandable to monthly grain for MRR. |
| **pipeline_opportunities_snapshot** | Pipeline snapshots by date: one row per opportunity per snapshot date. Used for new business and expansion forecast components. |
| **usage_monthly** | Monthly usage aggregates by customer and feature. Used for health scoring and expansion signals. |

### Optional canonical tables

| Table | Purpose |
|-------|--------|
| **support_tickets_monthly** | Monthly ticket counts by customer (e.g. for health or risk signals). Not required for core forecast. |
| **payments_monthly** | Monthly payment amounts by customer/contract (e.g. for reconciliation). Not required for core forecast. |
| **fx_rates_monthly** | Monthly FX rates (e.g. currency_code, month, rate_to_base). Used when normalizing multi-currency line items to a single reporting currency. |

### Key principle: keep ingestion/staging separate from canonical marts

- **Ingestion:** Extract from source (Salesforce API, Stripe, data warehouse export) and land in raw or staging tables in your warehouse. No platform logic.
- **Mapping:** Transform raw/source tables into the **canonical input tables** (the contract). This is where you apply system-specific mapping rules (e.g. Stripe `subscription.status` → canonical `status`).
- **Platform:** dbt reads **only** from the canonical tables (or seeds that mirror them). Staging models clean and standardize (e.g. lowercase enums, month truncation); intermediate and marts never touch source-specific fields.

This separation lets you swap or add sources per company without changing the forecasting models.

---

## 2. System-to-Contract Mapping (by canonical table)

For each canonical table below: **purpose**, **required columns**, **source-system mapping examples** (Salesforce, HubSpot, Stripe, Chargebee), and **common pitfalls**.

### customers

| Aspect | Detail |
|--------|--------|
| **Purpose** | Single customer master per company; segment and optional CRM health input for forecasting and health scoring. |
| **Required columns** | `company_id`, `customer_id`, `segment`. Strongly recommended: `customer_name`, `created_date`. Optional: `region`, `industry`, `crm_health_input`. |
| **Grain** | One row per (`company_id`, `customer_id`). |

**Source mapping examples:**

| Source | Field or rule | Canonical column |
|--------|----------------|------------------|
| **Salesforce** | Account.Id | customer_id |
| | Account.Name | customer_name |
| | Account.Segment__c or derived rules | segment |
| | Account.CreatedDate | created_date |
| | Customer_Health__c / Renewal_Risk__c / CSM_Sentiment__c (or composite) | crm_health_input (1–10 proxy) |
| **HubSpot** | companies.companyId | customer_id |
| | companies.name | customer_name |
| | lifecycleStage or custom property (e.g. segment) | segment |
| | custom health score property | crm_health_input |
| **Stripe** | Customer.id (if CRM is Stripe) | customer_id |
| **Chargebee** | customer.id | customer_id |
| | customer.company (or similar) | customer_name |

**Pitfalls:** Using names or emails as `customer_id` (unstable; use stable internal IDs). Mixing IDs from different systems without a mapping table—see “Unified customer key strategy” in Section 3A.

---

### products

| Aspect | Detail |
|--------|--------|
| **Purpose** | Product catalog for subscription line items; product_family and is_recurring drive reporting and expansion logic. |
| **Required columns** | `company_id`, `product_id`, `product_family`, `is_recurring`. Optional: `default_term_months`. |
| **Grain** | One row per (`company_id`, `product_id`). |

**Source mapping examples:**

| Source | Field or rule | Canonical column |
|--------|----------------|------------------|
| **Stripe** | Product.id | product_id |
| | Product.metadata.product_family or Product.name | product_family |
| | Price.recurring != null | is_recurring |
| | recurring.interval_count and interval (e.g. 12 + "month") | default_term_months |
| **Chargebee** | item.id | product_id |
| | item.item_family_id or metadata | product_family |
| | item.type / recurring flag | is_recurring |
| | plan billing period | default_term_months |
| **Salesforce** | Product2.Id (if SFDC is product master) | product_id |
| | Product2.Family or custom | product_family |

**Pitfalls:** Treating every Price or plan variant as a separate product when the contract expects product-level grain—aggregate or choose a single representative price per product if needed.

---

### subscription_line_items

| Aspect | Detail |
|--------|--------|
| **Purpose** | Contracted recurring revenue; must be expandable to monthly grain for MRR and renewal month logic. |
| **Required columns** | `company_id`, `contract_id`, `customer_id`, `product_id`, `contract_start_date`, `contract_end_date`, `billing_frequency`, `quantity`, `unit_price`, `discount_pct`, `status`. |
| **Grain** | One row per (`company_id`, `contract_id`, `customer_id`, `product_id`). |
| **Allowed status** | `active`, `non_renewing`, `cancelled`. |
| **Allowed billing_frequency** | `monthly`, `annual`. |

**Source mapping examples:**

| Source | Field or rule | Canonical column |
|--------|----------------|------------------|
| **Stripe** | Subscription.id (or SubscriptionItem.id if one line per item) | contract_id |
| | Subscription.customer | customer_id (map to canonical customer_id) |
| | SubscriptionItem.price.product | product_id |
| | Subscription.current_period_start / end | contract_start_date / contract_end_date |
| | SubscriptionItem.quantity | quantity |
| | Price.unit_amount (normalize to currency units) | unit_price |
| | Discount/coupon effective rate | discount_pct |
| | subscription.status | status (see status mapping table below) |
| **Chargebee** | subscription.id (+ item if line-level) | contract_id |
| | customer.id | customer_id |
| | subscription_item.item_price_id or item_id | product_id |
| | subscription.start_date / end_date | contract_start_date / contract_end_date |
| | item quantity and unit price | quantity, unit_price |
| | discounts | discount_pct |
| | subscription.status | status (see status mapping table below) |
| **Salesforce** | Contract.Id or Order/OrderItem / CPQ subscription object | contract_id |
| | Account Id (mapped to canonical customer_id) | customer_id |
| | Product2 Id | product_id |
| | Start/End dates | contract_start_date, contract_end_date |
| | Renewal date → contract_end_date for renewal_month derivation | — |

**Status mapping (example):**

| Stripe subscription.status | Chargebee subscription.status | Canonical status |
|----------------------------|-------------------------------|------------------|
| active | active, in_trial | active |
| past_due, unpaid (still in period) | past_due | active or non_renewing (define policy) |
| cancel_at_period_end, cancelled but in period | non_renewing | non_renewing |
| cancelled, ended | cancelled | cancelled |

**Pitfalls:** Using invoice or payment dates instead of **contract period** dates for start/end; mixing currencies without `currency_code` and FX handling; one-to-many Subscription → line items: ensure grain is one row per contract line (e.g. contract_id may be subscription_id + item key).

---

### pipeline_opportunities_snapshot

| Aspect | Detail |
|--------|--------|
| **Purpose** | Historical pipeline state for weighted value and slippage; snapshot prevents future-stage leakage in backtests. |
| **Required columns** | `company_id`, `snapshot_date`, `opportunity_id`, `segment`, `stage`, `amount`, `expected_close_date`, `opportunity_type`. Optional: `customer_id` (null for net-new). |
| **Grain** | One row per (`company_id`, `snapshot_date`, `opportunity_id`). |

**Source mapping examples:**

| Source | Field or rule | Canonical column |
|--------|----------------|------------------|
| **Salesforce** | Opportunity.Id | opportunity_id |
| | Opportunity.AccountId (mapped to canonical customer_id) | customer_id (null if net-new) |
| | Opportunity.StageName | stage (map to canonical stage list) |
| | Opportunity.Amount | amount |
| | Opportunity.CloseDate | expected_close_date |
| | Type or custom (New Business / Expansion / Renewal) | opportunity_type |
| | Snapshot: nightly export with snapshot_date | snapshot_date |
| **HubSpot** | deals.dealId | opportunity_id |
| | Associated company (mapped to customer_id) | customer_id |
| | dealstage | stage |
| | amount | amount |
| | closedate | expected_close_date |
| | dealtype or custom | opportunity_type |
| | Daily export or warehouse snapshot | snapshot_date |

**Pitfalls:** Using **current** stage only (no history)—forecast and backtest require **snapshots** so past months see only past stage. Missing net-new deals (customer_id null) breaks new-business forecast. Stage names must map to canonical: e.g. Prospecting, Discovery, Proposal, Negotiation, Closed Won, Closed Lost (staging normalizes to lowercase).

---

### usage_monthly

| Aspect | Detail |
|--------|--------|
| **Purpose** | Monthly usage by customer and feature for health scoring and expansion signals. |
| **Required columns** | `company_id`, `month`, `customer_id`, `feature_key`, `usage_count`, `active_users`. |
| **Grain** | One row per (`company_id`, `month`, `customer_id`, `feature_key`). |
| **Month** | First day of month (e.g. YYYY-MM-01). |

**Source mapping examples:**

| Source | Notes |
|--------|--------|
| **Product event logs / warehouse** | Aggregate events to monthly counts per customer and feature; count distinct users → active_users. |
| **Analytics tools** | Export monthly aggregates by customer (and feature if available); map dimensions to stable feature_key. |
| **CRM or CS tools** | Rarely source of usage; prefer product telemetry. |

**Pitfalls:** Using unstable feature names (rename to a small taxonomy, e.g. feature_a, feature_b, or company-specific keys in config). Missing months for inactive customers—decide whether to backfill zeros or leave sparse (health logic often uses trailing periods).

---

## 3. Detailed mapping guidance

### A) customers

**Salesforce**

- Account.Id → `customer_id` (stable internal ID).
- Account.Name → `customer_name`.
- Segment: Account.Segment__c, or derived from AnnualRevenue / EmployeeCount / custom tier → map to `enterprise`, `large`, `medium`, `smb`.
- Optional: Account.OwnerId → store as account_owner in a custom extension if needed; not in base contract.
- Health: Customer_Health__c, Renewal_Risk__c, or CSM_Sentiment__c → normalize to 1–10 and put in `crm_health_input` (null if not available; platform uses default from config).

**HubSpot**

- companies.companyId → `customer_id`.
- companies.name → `customer_name`.
- lifecycleStage or custom property (e.g. segment) → `segment` (map to enterprise/large/medium/smb).
- Custom health property → `crm_health_input` (1–10).

**Unified customer key strategy**

- Use a **stable internal customer_id** (e.g. UUID or source system primary key) in all canonical tables. Do **not** use names or emails as keys.
- When you have multiple source systems (e.g. Salesforce Account + Stripe Customer), maintain a **mapping table** (e.g. source_system, source_id, company_id, customer_id) and resolve to canonical `customer_id` in your mapping layer before writing to `customers` and `subscription_line_items`.
- Same `customer_id` must be used in customers, subscription_line_items, pipeline_opportunities_snapshot (where applicable), and usage_monthly.

---

### B) products

**Stripe**

- Product.id → `product_id`.
- Product.metadata or Product.name → `product_family`.
- Price.recurring != null → `is_recurring` (true/false).
- recurring.interval_count and interval (e.g. 12, "month") → `default_term_months` (e.g. 12 for annual).

**Chargebee**

- item.id → `product_id`.
- item.item_family_id or metadata → `product_family`.
- item.type or recurring flag → `is_recurring`.
- Plan period (month/year) → `default_term_months`.

**Salesforce**

- Product2.Id → `product_id` when Salesforce is the product master; map CPQ or Order product references to the same IDs.

---

### C) subscription_line_items (most important)

This table is the main driver of **contracted recurring revenue**. Each row must represent a **contract line** that can be expanded to a **monthly grain** (contract_start_date to contract_end_date, by month). MRR is derived from quantity × unit_price × (1 - discount_pct), normalized to monthly (e.g. annual / 12 when billing_frequency = annual).

**Stripe**

- Subscription.id (or Subscription.id + SubscriptionItem.id if multiple items per subscription) → `contract_id`.
- Subscription.customer → resolve to canonical `customer_id`.
- SubscriptionItem.price.product → `product_id`.
- Subscription.current_period_start / current_period_end → `contract_start_date`, `contract_end_date` (use period boundaries; for multi-year, may need term logic).
- SubscriptionItem.quantity → `quantity`.
- Price.unit_amount (cents or smallest currency unit) → convert to currency units for `unit_price`.
- Discount/coupon → effective `discount_pct` (0–1).
- subscription.status → map to `active` | `non_renewing` | `cancelled` (see status table in Section 2).

**Chargebee**

- subscription.id (and item key if line-level) → `contract_id`.
- customer.id → `customer_id`.
- subscription_item → product and pricing → `product_id`, `quantity`, `unit_price`; apply discounts to `discount_pct`.
- subscription.start_date, end_date (or current term) → `contract_start_date`, `contract_end_date`.
- subscription.status → canonical `status`.

**Salesforce**

- If SFDC is subscription source: Contract, Order/OrderItem, or CPQ subscription objects → one row per line; map renewal/term dates to contract_start_date and contract_end_date.

**Annual billing**

- Store `billing_frequency` = annual; platform converts to monthly equivalent MRR (e.g. annual contract value / 12) for forecasting. Keep quantity and unit_price as per the contract; MRR is computed in models.

**Multi-currency**

- Keep `currency_code` at line level if you support it (optional column); use `fx_rates_monthly` (or equivalent) to convert to a single reporting currency in a later layer. Do not mix reporting currency in the same company’s `unit_price` without explicit conversion.

---

### D) pipeline_opportunities_snapshot

**Why snapshots matter**

- Forecast and backtest use **pipeline state as of a given date**. If you only load “current” stage, you introduce **look-ahead**: historical months would see future stage changes. Snapshots (e.g. nightly or daily) with `snapshot_date` and stage at that time prevent leakage and allow correct weighted value and slippage by period.

**Salesforce**

- Opportunity.Id → `opportunity_id`.
- Opportunity.AccountId → `customer_id` (mapped to canonical; null for net-new).
- Opportunity.StageName → `stage` (map to canonical: Prospecting, Discovery, Proposal, Negotiation, Closed Won, Closed Lost).
- Opportunity.Amount → `amount`.
- Opportunity.CloseDate → `expected_close_date`.
- Type or custom → `opportunity_type` (new_biz, expansion, renewal).
- **Snapshot strategy:** Nightly (or daily) export of opportunities with snapshot_date; include stage and amount as of that date. Load into a table keyed by (company_id, snapshot_date, opportunity_id).

**HubSpot**

- deals.dealId → `opportunity_id`.
- Associated company → `customer_id`.
- dealstage → `stage`.
- amount, closedate → `amount`, `expected_close_date`.
- dealtype (or custom) → `opportunity_type`.
- **Snapshot:** Daily export or warehouse snapshot with snapshot_date.

---

### E) usage_monthly

**Generic approach**

- Source: product telemetry (event logs, warehouse event tables) or analytics exports.
- Aggregate to **monthly** by (customer_id, feature_key): e.g. event count → `usage_count`, distinct users → `active_users`. Platform can derive usage_per_user from usage_count / active_users.

**Feature taxonomy**

- Use a **stable feature_key** per feature (e.g. feature_a, feature_b, or company-defined keys). Same keys across months and companies (or per-company config) so health and expansion logic can rely on them.

**Example transformations**

- Raw events → group by customer, feature, month → sum(events) → `usage_count`; count(distinct user_id) → `active_users`.
- usage_per_user can be computed in staging as usage_count / nullif(active_users, 0).

---

## 4. Minimum viable onboarding checklist (fast path)

Use this order to get one company to a runnable forecast with minimal scope:

1. **Create company_id and currency**  
   Choose a stable `company_id` (e.g. `co_alpha`). Set reporting currency and optional fx_rates if multi-currency.

2. **Load customers**  
   Map CRM (or billing) to `customers` with company_id, customer_id, segment, customer_name, created_date; optional crm_health_input.

3. **Load subscription_line_items**  
   Map billing (Stripe/Chargebee) or SFDC subscriptions to canonical line items with contract dates, billing_frequency, quantity, unit_price, discount_pct, status. Ensure customer_id and product_id match customers and products.

4. **Compute actuals**  
   Run platform models; confirm fct_revenue_actuals_monthly (or equivalent) shows expected MRR by month/segment.

5. **Load pipeline snapshots**  
   Export pipeline by snapshot_date; map to pipeline_opportunities_snapshot with stage, amount, expected_close_date, opportunity_type. Include net-new (customer_id null).

6. **Load usage**  
   Aggregate usage to usage_monthly (company_id, month, customer_id, feature_key, usage_count, active_users).

7. **Fill config seeds**  
   Populate segment_config, health_weights_config, stage_probability_config, slippage_config (and scenario_config if used) for this company_id. Match segment and segment_group names to your customers and pipeline.

8. **Run dbt seed, dbt run, dbt test**  
   Load seeds (or canonical tables), build staging → intermediate → marts, run tests.

9. **Run backtest and sanity checks**  
   Compare forecast vs actuals for historical months; check segment and scenario coverage and pipeline-weighted value reasonableness.

---

## 5. Data quality & risk controls

### Grain checks (uniqueness)

- **customers:** Unique (company_id, customer_id).
- **products:** Unique (company_id, product_id).
- **subscription_line_items:** Unique (company_id, contract_id, customer_id, product_id).
- **pipeline_opportunities_snapshot:** Unique (company_id, snapshot_date, opportunity_id).
- **usage_monthly:** Unique (company_id, month, customer_id, feature_key).

Enforce these in your mapping layer or with dbt uniqueness tests on the canonical/staging models.

### Reconciliation checks

- **MRR from line items vs invoices:** Where you have invoice or payments data, compare sum of MRR (from subscription line items expanded to monthly) to invoiced amounts by month; investigate gaps (timing, proration, credits).
- **Pipeline stage probability completeness:** For each (company_id, segment, stage) used in pipeline data, ensure stage_probability_config has a row (no missing stages in config).

### Leakage prevention

- **Never join “current” pipeline stage into historical months.** Use only snapshot-based pipeline: for a given snapshot_date, use stage and amount as of that date. Joining today’s stage to past months would leak future information into backtests and historical analysis.

### Customers joining during the year

- **Net-new opportunities** (customer_id null) have no prior subscription; revenue start month is driven by expected_close_date plus slippage. Ensure pipeline snapshots and slippage_config are populated so new-business component reflects start lag correctly.

### Segment-specific pitfalls

- **Enterprise / large:** Bespoke contracts, delayed go-live, co-terming and amendments can create multiple contracts per customer or non-standard periods; map to clear contract_id and contract dates. Health and renewal logic may need segment-specific config.
- **SMB:** Higher churn and month-to-month variability; ensure status (active/non_renewing/cancelled) and end dates are updated promptly so MRR and churn are not overstated or lagged.

---

## 6. Appendix: suggested canonical naming conventions

| Convention | Recommendation |
|------------|----------------|
| **company_id** | Short, stable string (e.g. `co_alpha`, `acme_prod`). No spaces; lowercase optional. |
| **ID scoping** | All entity IDs (customer_id, product_id, contract_id, opportunity_id) are unique **within** a company. Composite key is always (company_id, entity_id). |
| **Date fields** | Use date type. For “month” fields use **first day of month** (YYYY-MM-01). contract_start_date / contract_end_date are full dates; contract_start_month / contract_end_month are derived as first day of that month. |
| **Enums** | segment: `enterprise`, `large`, `medium`, `smb`. segment_group: `enterprise_large`, `mid_smb`. status (subscriptions): `active`, `non_renewing`, `cancelled`. billing_frequency: `monthly`, `annual`. stage: Prospecting, Discovery, Proposal, Negotiation, Closed Won, Closed Lost (staging may normalize to lowercase). opportunity_type: `new_biz`, `expansion`, `renewal`. |
| **Numeric** | unit_price in currency units (not cents). discount_pct as decimal 0–1. amount in currency units. crm_health_input 1–10 integer when present. |

---

*This guide is company-agnostic and does not reference internal or product-specific object names. Adjust mapping examples to your exact source schemas and enrichment rules.*
