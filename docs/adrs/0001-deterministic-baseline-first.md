# ADR 0001: Deterministic baseline first

**Status:** Accepted  
**Date:** 2025-02-22

## Context

Revenue forecasts must be explainable and auditable for finance and RevOps. Stakeholders need a single source of truth that does not depend on black-box models. We also want to add ML later to improve accuracy without replacing the entire pipeline.

## Decision

We build a **deterministic, driver-based baseline** before any ML. Renewal and pipeline probabilities are rule-based (config-driven or segment-based) by default. Forecast = renewal MRR + new business + expansion, with scenario adjustments (base/upside/downside). ML is layered on top as an optional calibration of those probabilities, not as an end-to-end revenue predictor.

## Alternatives considered

- **ML-first or ML-only:** Let a model predict revenue directly. Rejected because it is hard to explain, audit, or reconcile to actuals; finance and boards need a clear driver story.
- **Hybrid from day one:** Build rules and ML in parallel. Rejected to avoid scope creep and to establish a stable contract (marts, scenarios) first; ML can plug into the same contract.

## Consequences

- **Positive:** Explainable forecasts, easier reconciliation, board-ready storytelling; ML can be added or swapped without re-architecting.
- **Negative:** Rule-based baseline may be less accurate than a full ML model; we accept that tradeoff for trust and governance.
- **Neutral:** We must keep scenario and driver definitions documented (e.g. in config and ADRs).

## How to revisit

Revisit if we need real-time or highly personalized forecasts where explainability is secondary, or if a regulatory/audit requirement forces a different structure.
