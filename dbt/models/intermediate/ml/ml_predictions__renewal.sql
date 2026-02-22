-- Reads ML renewal predictions from Parquet produced by ml.src.predict_renewal.
-- Path is relative to dbt project dir (dbt/): ../ml/outputs/predictions/renewal_predictions.parquet.
-- TODO: Blend p_renew_ml with deterministic int_renewal_probabilities.p_renew in forecast.
-- If the Parquet file does not exist, dbt run will fail; run predict_renewal first or create an empty file.

select * from read_parquet('../ml/outputs/predictions/renewal_predictions.parquet')
