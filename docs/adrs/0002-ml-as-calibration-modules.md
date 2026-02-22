# ADR 0002: ML as calibration modules

**Status:** Accepted  
**Date:** 2025-02-22

## Context

We want ML to improve forecast accuracy (especially renewal and pipeline conversion) without replacing the deterministic engine. We need to choose probabilities (renewal, close) that feed into the same driver-based forecast, and we must be able to compare and govern model quality (backtest, calibration).

## Decision

ML is implemented as **calibration modules** that predict **probabilities** (renewal probability, pipeline close probability), not end-to-end revenue. Two domains: renewals and pipeline. Each has its own feature table, labels, training, backtest, and calibration; champion/challenger selection picks the best model per domain. The forecast engine consumes these probabilities via the same staging/mart interface as the rule-based baseline; when ML is missing, rules are used.

## Alternatives considered

- **End-to-end revenue model:** One model predicts total revenue. Rejected because it is hard to reconcile to drivers (renewal vs new vs expansion) and to explain; also harder to backtest by component.
- **Single unified ML model:** One model for both renewals and pipeline. Rejected because data grain and label definitions differ; separate modules allow independent iteration and clearer ownership.

## Consequences

- **Positive:** Clear separation of concerns; backtest and calibration per domain; we can ship or roll back ML without changing the forecast structure.
- **Negative:** Two pipelines to maintain; we must keep feature/label contracts stable for reproducibility.
- **Neutral:** Champion selection and model versioning must be documented and visible (e.g. ml_model_selection, Model Intelligence page).

## How to revisit

Revisit if we introduce a new probability domain (e.g. expansion) or if we decide to move to a single unified model with a different abstraction.
