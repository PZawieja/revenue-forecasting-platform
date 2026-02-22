# 3-minute demo script

Crisp, executive-grade walkthrough of the Revenue Intelligence Cockpit. Use this to demo the app in three minutes.

---

## 0:00–0:30 — Problem framing

> “Revenue plans are usually scattered: spreadsheets, CRM snapshots, no single source of truth. Finance and GTM end up with different numbers and no clear way to run what‑if scenarios.
>
> This platform fixes that. We take your data contract—customers, subscriptions, pipeline, usage—run it through a governed dbt pipeline and a deterministic forecast engine, then optionally calibrate with ML. What you see in the cockpit is one place to align on forecast, risk, and reconciliation.”

*[Open the app; land on Home.]*

---

## 0:30–1:15 — Forecast page

> “Forecast vs Actual is where we align on the number and the range.”
>
> *[Select scenario: base / upside / downside.]*
>
> “We don’t give one number—we give bands. Best, base, worst—or confidence intervals—so you can plan on ranges, not point estimates.”
>
> *[Show forecast line vs actuals, and intervals if available.]*
>
> “Confidence and pipeline coverage sit underneath: you see how much of the forecast is driven by renewals vs new business and how confident the model is. That’s what supports planning and variance review.”

---

## 1:15–2:00 — ARR waterfall

> “The board wants to know: where did ARR come from and where did it go?”
>
> *[Open ARR Waterfall; pick a month and scenario.]*
>
> “Starting ARR, new, expansion, contraction, churn, ending ARR. One bridge, one story. We also run reconciliation checks—so the movement ties to the numbers you trust. This is the page for board-level ARR storytelling and reconciliation.”

---

## 2:00–2:30 — Risk radar

> “Who’s at risk and why?”
>
> *[Open Risk Radar; show Churn Risk Watchlist and Top ARR Movers.]*
>
> “Watchlist: customers by renewal probability, health score, months to renewal, and a short risk reason. Movers: who drove ARR up or down. Use this for renewal prioritization and account-level action—CSM and sales alignment.”

---

## 2:30–3:00 — Model intelligence

> “How do we choose and trust the models?”
>
> *[Open Model Intelligence.]*
>
> “Champion selection: we run backtests and pick the best model per domain—renewals and pipeline—using Brier and logloss, with a stability guardrail so we don’t overfit to noise. Calibration: predicted vs actual rate by bin. If the line tracks the diagonal, the model is well calibrated. This is how we justify ML choices and monitor forecast quality.”

---

## Close — How it becomes a product

> “Today this runs on your data in a single DuckDB warehouse—dbt + optional ML, all in this repo. The same architecture can become a product: same data contract, same forecast engine and ML calibration, same governance and reconciliation. We add connectors, scheduling, and a hosted cockpit—Revenue Intelligence as a service for RevOps, Finance, and CS leaders who want accurate, explainable forecasting and early risk detection without the spreadsheet mess.”

---

## Quick reference

| Time | Page | Message |
|------|------|--------|
| 0:00–0:30 | — | Problem: scattered plans; solution: governed pipeline, one place to align. |
| 0:30–1:15 | Forecast | Bands and scenarios; confidence and coverage; planning and variance. |
| 1:15–2:00 | ARR Waterfall | Bridge and reconciliation; board-level ARR story. |
| 2:00–2:30 | Risk Radar | Who’s at risk and why; renewal prioritization and movers. |
| 2:30–3:00 | Model Intelligence | Champion selection and calibration; justify and monitor ML. |
| 3:00 | — | Product angle: same stack, add connectors and hosting. |
