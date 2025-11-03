-- PG-Strom GPU vs CPU EXPLAIN ANALYZE examples
--
-- How to use (example):
--   psql -p 5434 -d pgstrom_test -f sql/pgstrom_explain_examples.sql
--
-- Notes:
-- - There is no special "GPU SQL". PG-Strom offloads eligible parts when pg_strom.enabled = on
--   and the planner picks GpuScan/GpuJoin/GpuPreAgg.
-- - A plain SELECT * often remains CPU (Seq Scan) because there is little work to offload.
-- - The optional block below provides nrows() helpers sometimes referenced in EXPLAIN output.

-- Optional: create helper functions nrows() (created unconditionally; idempotent semantics via OR REPLACE)
CREATE OR REPLACE FUNCTION public.nrows(bigint)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0), 'FM999,999,999,999')$$;

CREATE OR REPLACE FUNCTION public.nrows(numeric)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0)::numeric, 'FM999,999,999,999')$$;

CREATE OR REPLACE FUNCTION public.nrows(integer)
RETURNS text
LANGUAGE sql
IMMUTABLE PARALLEL SAFE
AS $$SELECT to_char(COALESCE($1,0)::bigint, 'FM999,999,999,999')$$;

SET search_path = public, pg_catalog;
    iming on
-- (auto-fix) previous stray line disabled:
-- iming on

-- ============================================================================
-- Baseline (SELECT *): CPU vs GPU
-- Execution Time previously observed: 233,708.245 ms (reference only)
-- ============================================================================
\echo '=== Baseline SELECT * : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT *
FROM bench_numeric_xxlarge;

\echo '=== Baseline SELECT * : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT *
FROM bench_numeric_xxlarge;

-- ============================================================================
-- GPU-friendly simple filter: CPU vs GPU
-- ============================================================================
\echo '=== Simple filter : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT *
FROM bench_numeric_xxlarge
WHERE value1 > 5000;

\echo '=== Simple filter : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT *
FROM bench_numeric_xxlarge
WHERE value1 > 5000;

-- ============================================================================
-- Projection + math (heavier compute): CPU vs GPU
-- ============================================================================
\echo '=== Projection + math : CPU (pg_strom.enabled=off) ==='
SET pg_strom.enabled = off; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT id, category,
       value1, value2, value3,
       (value1 * value2 / NULLIF(value3, 0)) AS complex_expr
FROM bench_numeric_xxlarge
WHERE value6 < 8000;

\echo '=== Projection + math : GPU (pg_strom.enabled=on) ==='
SET pg_strom.enabled = on; SHOW pg_strom.enabled;
EXPLAIN (ANALYZE, SUMMARY, BUFFERS)
SELECT id, category,
       value1, value2, value3,
       (value1 * value2 / NULLIF(value3, 0)) AS complex_expr
FROM bench_numeric_xxlarge
WHERE value6 < 8000;

-- ============================================================================
-- Pre-aggregation: CPU vs GPU
-- ============================================================================
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
