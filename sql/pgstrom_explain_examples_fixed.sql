-- PG-Strom GPU vs CPU EXPLAIN ANALYZE examples (fixed)
-- How to run:
--   psql -p 5434 -d pgstrom_test -f sql/pgstrom_explain_examples_fixed.sql

-- Provide nrows() helper functions used by some EXPLAIN displays
CREATE OR REPLACE FUNCTION public.nrows(bigint)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0), 'FM999,999,999,999')$$;

CREATE OR REPLACE FUNCTION public.nrows(numeric)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0)::numeric, 'FM999,999,999,999')$$;

CREATE OR REPLACE FUNCTION public.nrows(integer)
RETURNS text LANGUAGE sql IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0)::bigint, 'FM999,999,999,999')$$;

SET search_path = public, pg_catalog;
\timing on

-- Baseline SELECT * : CPU vs GPU
\echo '=== Baseline SELECT * : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT * FROM bench_numeric_xxlarge;

\echo '=== Baseline SELECT * : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT * FROM bench_numeric_xxlarge;

-- Simple filter: CPU vs GPU
\echo '=== Simple filter : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT * FROM bench_numeric_xxlarge WHERE value1 > 5000;

\echo '=== Simple filter : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT * FROM bench_numeric_xxlarge WHERE value1 > 5000;

-- Projection + math: CPU vs GPU
\echo '=== Projection + math : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT id, category, value1, value2, value3,
       (value1 * value2 / NULLIF(value3, 0)) AS complex_expr
FROM bench_numeric_xxlarge
WHERE value6 < 8000;

\echo '=== Projection + math : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT id, category, value1, value2, value3,
       (value1 * value2 / NULLIF(value3, 0)) AS complex_expr
FROM bench_numeric_xxlarge
WHERE value6 < 8000;

-- Pre-aggregation: CPU vs GPU
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
