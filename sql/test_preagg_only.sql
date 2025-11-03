-- Test Pre-aggregation with GPU after adding pavg()/psum() helpers
SET search_path = public, pg_catalog;
\timing on

\echo '=== Pre-aggregation : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT category, AVG(value1) AS avg_v1, SUM(value3) AS sum_v3
FROM bench_numeric_xxlarge
WHERE value2 BETWEEN 1000 AND 9000
GROUP BY category;

\echo '=== Pre-aggregation : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT category, AVG(value1) AS avg_v1, SUM(value3) AS sum_v3
FROM bench_numeric_xxlarge
WHERE value2 BETWEEN 1000 AND 9000
GROUP BY category;
