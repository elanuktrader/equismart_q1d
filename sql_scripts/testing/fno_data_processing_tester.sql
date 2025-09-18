--EXPLAIN ANALYZE 
--SELECT *
--FROM compute_features(8000, 'ABB');
--SELECT *
--FROM compute_features(60, 'ABB');

--EXPLAIN ANALYZE 
SELECT *
--FROM compute_fno_summary(60, 'ABB');
--FROM compute_fno_summary(60);
--FROM compute_fno_summary(60::INTEGER, 'ABB'::VARCHAR,'2024-09-01'::DATE,'2024-11-28'::DATE)
FROM compute_derived_fno_summary (90::INTEGER, 'TCS'::VARCHAR,NULL,NULL)

--EXPLAIN ANALYZE
--SELECT *
--FROM compute_fno_features(60, 'TCS');