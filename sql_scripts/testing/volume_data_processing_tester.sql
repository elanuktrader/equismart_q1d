--EXPLAIN ANALYZE 
--SELECT *
--FROM compute_features(8000, 'ABB');
--SELECT *
--FROM compute_features(60, 'ABB');

--EXPLAIN ANALYZE 
SELECT *
FROM compute_cm_summary(60::INTEGER,'ABB'::VARCHAR );
--FROM compute_cm_summary(60, 'ABB','2024-09-01','2024-11-28');

--EXPLAIN ANALYZE
--SELECT *
--FROM compute_cm_features(60, 'ABB', '2024-09-01');