-- PG-Strom CPU vs GPU Benchmark (SQL-only)
-- Run with: sudo -u postgres psql -p 5434 -d pgstrom_test -f sql/benchmark_suite.sql
-- You can tweak these:
\timing on
\set suffix small
\set rows 100000

-- Cleanup existing tables for this suffix
DROP TABLE IF EXISTS bench_numeric_:suffix CASCADE;
DROP TABLE IF EXISTS bench_mixed_:suffix CASCADE;
DROP TABLE IF EXISTS bench_join_a_:suffix CASCADE;
DROP TABLE IF EXISTS bench_join_b_:suffix CASCADE;
DROP TABLE IF EXISTS bench_tree_:suffix CASCADE;

-- Numeric table (GPU-friendly)
CREATE TABLE bench_numeric_:suffix (
    id BIGSERIAL PRIMARY KEY,
    value1 NUMERIC(15,2),
    value2 NUMERIC(15,2),
    value3 NUMERIC(15,2),
    value4 NUMERIC(15,2),
    value5 NUMERIC(15,2),
    value6 NUMERIC(15,2),
    value7 NUMERIC(15,2),
    value8 NUMERIC(15,2),
    value9 NUMERIC(15,2),
    value10 NUMERIC(15,2),
    value11 NUMERIC(15,2),
    value12 NUMERIC(15,2),
    value13 NUMERIC(15,2),
    value14 NUMERIC(15,2),
    value15 NUMERIC(15,2),
    category INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO bench_numeric_:suffix (
    value1,value2,value3,value4,value5,value6,value7,value8,value9,value10,
    value11,value12,value13,value14,value15,category
)
SELECT 
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (1+random()*9999)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (1+random()*9999)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    (1+random()*9999)::NUMERIC(15,2),
    (1+random()*9999)::NUMERIC(15,2),
    (random()*10000)::NUMERIC(15,2),
    1 + (random()*9)::INT
FROM generate_series(1, :rows);

-- Mixed table (strings/CLOB)
CREATE TABLE bench_mixed_:suffix (
    id BIGSERIAL PRIMARY KEY,
    name TEXT,
    email TEXT,
    age INTEGER,
    salary NUMERIC(12,2),
    bonus NUMERIC(12,2),
    hire_date DATE,
    is_active BOOLEAN,
    score DOUBLE PRECISION,
    department TEXT,
    city TEXT,
    rating INTEGER,
    commission NUMERIC(12,2),
    years_exp INTEGER,
    performance_score DOUBLE PRECISION,
    remote_work BOOLEAN,
    project_count INTEGER,
    training_hours INTEGER,
    certifications INTEGER,
    description TEXT,
    notes TEXT,
    profile_summary TEXT,
    work_history TEXT
);

INSERT INTO bench_mixed_:suffix (
    name,email,age,salary,bonus,hire_date,is_active,score,department,city,
    rating,commission,years_exp,performance_score,remote_work,project_count,
    training_hours,certifications,description,notes,profile_summary,work_history
)
SELECT 
    'User_' || i,
    'user' || i || '@example.com',
    20 + (random() * 45)::INTEGER,
    30000 + random() * 120000,
    1000 + random() * 20000,
    CURRENT_DATE - (random() * 3650)::INTEGER,
    random() > 0.3,
    random() * 100,
    CASE (random() * 5)::INTEGER 
        WHEN 0 THEN 'Engineering'
        WHEN 1 THEN 'Sales'
        WHEN 2 THEN 'Marketing'
        WHEN 3 THEN 'HR'
        ELSE 'Operations'
    END,
    CASE (random() * 4)::INTEGER
        WHEN 0 THEN 'New York'
        WHEN 1 THEN 'San Francisco'
        WHEN 2 THEN 'Chicago'
        ELSE 'Austin'
    END,
    1 + (random() * 5)::INTEGER,
    500 + random() * 5000,
    (random() * 20)::INTEGER,
    50 + random() * 50,
    random() > 0.4,
    (random() * 30)::INTEGER,
    (random() * 500)::INTEGER,
    (random() * 10)::INTEGER,
    'Employee description for user ' || i || '. ' || repeat('Additional details and information. ', 10),
    'Notes: ' || repeat('Important notes regarding employee performance and projects. ', 5) || ' User ID: ' || i,
    'Professional summary: Experienced professional with ' || (random() * 20)::INTEGER || ' years in the field. ' || repeat('Skilled in various technologies and methodologies. ', 8),
    'Work History: ' || repeat('Company ' || (random() * 10)::INTEGER || ' from year ' || (2000 + (random() * 20)::INTEGER) || '. ', 15)
FROM generate_series(1, :rows) AS i;

-- Indexes
CREATE INDEX idx_numeric_cat_:suffix ON bench_numeric_:suffix(category);
CREATE INDEX idx_numeric_v1_:suffix ON bench_numeric_:suffix(value1);
CREATE INDEX idx_mixed_age_:suffix ON bench_mixed_:suffix(age);
CREATE INDEX idx_mixed_salary_:suffix ON bench_mixed_:suffix(salary);

-- Join helper tables
CREATE TABLE bench_join_a_:suffix (
    id BIGSERIAL PRIMARY KEY,
    ref_numeric_id BIGINT,
    dept TEXT,
    amount NUMERIC(12,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE bench_join_b_:suffix (
    category INTEGER,
    dept TEXT,
    weight NUMERIC(12,2),
    descr TEXT
);

INSERT INTO bench_join_a_:suffix (ref_numeric_id, dept, amount)
SELECT n.id,
       m.department,
       (random()*10000)::NUMERIC
FROM bench_numeric_:suffix n
JOIN bench_mixed_:suffix m ON m.id = n.id;

INSERT INTO bench_join_b_:suffix (category, dept, weight, descr)
SELECT ((random()*9)::INT)+1 AS category,
       d AS dept,
       (random()*100)::NUMERIC,
       'Dept weight info '
FROM (VALUES ('Engineering'),('Sales'),('Marketing'),('HR'),('Operations')) AS t(d)
CROSS JOIN generate_series(1, 100) gs;

-- Hierarchy table for recursive test
CREATE TABLE bench_tree_:suffix (
    id BIGINT PRIMARY KEY,
    parent_id BIGINT,
    name TEXT
);

WITH RECURSIVE seed AS (
    SELECT 1 AS id, NULL::BIGINT AS parent_id, 'root'::TEXT AS name
    UNION ALL
    SELECT id+1, CASE WHEN id < 50 THEN 1 ELSE ((random()*49)::INT)+2 END, 'node_'||id
    FROM generate_series(1, LEAST(100000, :rows)) AS id
)
INSERT INTO bench_tree_:suffix (id, parent_id, name)
SELECT id, parent_id, name FROM seed;

ANALYZE bench_numeric_:suffix;
ANALYZE bench_mixed_:suffix;

-- ========================= Tests =========================
-- 1) Simple Aggregation
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT category, COUNT(*) cnt, AVG(value1) avg1, AVG(value2) avg2, SUM(value3) sum3,
       MAX(value4) max4, MIN(value5) min5, AVG(value6) avg6, SUM(value7) sum7,
       MAX(value8) max8, MIN(value9) min9, AVG(value10) avg10
FROM bench_numeric_:suffix
WHERE value1 > 5000 AND value6 < 8000
GROUP BY category;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT category, COUNT(*) cnt, AVG(value1) avg1, AVG(value2) avg2, SUM(value3) sum3,
       MAX(value4) max4, MIN(value5) min5, AVG(value6) avg6, SUM(value7) sum7,
       MAX(value8) max8, MIN(value9) min9, AVG(value10) avg10
FROM bench_numeric_:suffix
WHERE value1 > 5000 AND value6 < 8000
GROUP BY category;

-- 2) Complex Math
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT category, COUNT(*) cnt,
       AVG(value1 * value2 / NULLIF(value3, 0)) complex_avg,
       SUM(SQRT(value1 * value1 + value2 * value2)) distance_sum,
       STDDEV(value4) stddev4,
       AVG(value5 * value6 / NULLIF(value7, 0)) ratio1,
       SUM(value8 + value9 + value10) total_sum,
       MAX(value11 * value12) max_product,
       MIN(value13 / NULLIF(value14, 0)) min_ratio,
       AVG(SQRT(value15 * value15 + value1 * value1)) dist_avg
FROM bench_numeric_:suffix
WHERE value1 > 1000 AND value2 < 9000 AND value11 > 2000
GROUP BY category
HAVING COUNT(*) > 100;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT category, COUNT(*) cnt,
       AVG(value1 * value2 / NULLIF(value3, 0)) complex_avg,
       SUM(SQRT(value1 * value1 + value2 * value2)) distance_sum,
       STDDEV(value4) stddev4,
       AVG(value5 * value6 / NULLIF(value7, 0)) ratio1,
       SUM(value8 + value9 + value10) total_sum,
       MAX(value11 * value12) max_product,
       MIN(value13 / NULLIF(value14, 0)) min_ratio,
       AVG(SQRT(value15 * value15 + value1 * value1)) dist_avg
FROM bench_numeric_:suffix
WHERE value1 > 1000 AND value2 < 9000 AND value11 > 2000
GROUP BY category
HAVING COUNT(*) > 100;

-- 3) Complex Filtering
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT COUNT(*), AVG(salary), MAX(bonus), AVG(commission), SUM(training_hours),
       AVG(performance_score), COUNT(DISTINCT department)
FROM bench_mixed_:suffix
WHERE age BETWEEN 30 AND 50 AND salary > 50000 AND is_active = true AND score > 50
  AND rating >= 3 AND years_exp > 2 AND project_count > 5;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT COUNT(*), AVG(salary), MAX(bonus), AVG(commission), SUM(training_hours),
       AVG(performance_score), COUNT(DISTINCT department)
FROM bench_mixed_:suffix
WHERE age BETWEEN 30 AND 50 AND salary > 50000 AND is_active = true AND score > 50
  AND rating >= 3 AND years_exp > 2 AND project_count > 5;

-- 4) Window Functions (limited rows)
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT category, value1, value2,
       AVG(value1) OVER (PARTITION BY category) avg_by_cat,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY value1 DESC) rank_in_cat
FROM bench_numeric_:suffix
WHERE value1 > 3000
LIMIT 10000;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT category, value1, value2,
       AVG(value1) OVER (PARTITION BY category) avg_by_cat,
       ROW_NUMBER() OVER (PARTITION BY category ORDER BY value1 DESC) rank_in_cat
FROM bench_numeric_:suffix
WHERE value1 > 3000
LIMIT 10000;

-- 5) String Operations
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT department, COUNT(*) emp_count, COUNT(DISTINCT city) city_count,
       LENGTH(STRING_AGG(name, ', ')) concat_length,
       AVG(LENGTH(description)) avg_desc_len,
       MAX(LENGTH(notes)) max_notes_len,
       COUNT(*) FILTER (WHERE description LIKE '%details%') keyword_count
FROM bench_mixed_:suffix
WHERE is_active = true AND rating >= 3
GROUP BY department;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT department, COUNT(*) emp_count, COUNT(DISTINCT city) city_count,
       LENGTH(STRING_AGG(name, ', ')) concat_length,
       AVG(LENGTH(description)) avg_desc_len,
       MAX(LENGTH(notes)) max_notes_len,
       COUNT(*) FILTER (WHERE description LIKE '%details%') keyword_count
FROM bench_mixed_:suffix
WHERE is_active = true AND rating >= 3
GROUP BY department;

-- 6) CLOB Operations
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT city, COUNT(*) total, AVG(salary) avg_salary,
       LENGTH(STRING_AGG(SUBSTRING(profile_summary, 1, 100), ' | ')) summary_concat_len,
       SUM(LENGTH(work_history)) total_history_size,
       COUNT(*) FILTER (WHERE work_history LIKE '%Company%') with_history
FROM bench_mixed_:suffix
WHERE LENGTH(description) > 100 AND profile_summary IS NOT NULL
GROUP BY city;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT city, COUNT(*) total, AVG(salary) avg_salary,
       LENGTH(STRING_AGG(SUBSTRING(profile_summary, 1, 100), ' | ')) summary_concat_len,
       SUM(LENGTH(work_history)) total_history_size,
       COUNT(*) FILTER (WHERE work_history LIKE '%Company%') with_history
FROM bench_mixed_:suffix
WHERE LENGTH(description) > 100 AND profile_summary IS NOT NULL
GROUP BY city;

-- 7) Join Aggregation (GpuJoin candidate)
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT n.category, ja.dept, COUNT(*) cnt, AVG(n.value3) avg_v3
FROM bench_numeric_:suffix n
JOIN bench_join_a_:suffix ja ON ja.ref_numeric_id = n.id
JOIN bench_join_b_:suffix jb ON jb.category = n.category AND jb.dept = ja.dept
WHERE n.value1 > 1000 AND n.value6 < 9000
GROUP BY n.category, ja.dept;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT n.category, ja.dept, COUNT(*) cnt, AVG(n.value3) avg_v3
FROM bench_numeric_:suffix n
JOIN bench_join_a_:suffix ja ON ja.ref_numeric_id = n.id
JOIN bench_join_b_:suffix jb ON jb.category = n.category AND jb.dept = ja.dept
WHERE n.value1 > 1000 AND n.value6 < 9000
GROUP BY n.category, ja.dept;

-- 8) Correlated Subquery
SET pg_strom.enabled = off; EXPLAIN ANALYZE
SELECT m.department, AVG(m.salary)
FROM bench_mixed_:suffix m
WHERE EXISTS (
  SELECT 1 FROM bench_numeric_:suffix n
  WHERE n.category = m.rating AND n.value1 > 8000
)
GROUP BY m.department;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
SELECT m.department, AVG(m.salary)
FROM bench_mixed_:suffix m
WHERE EXISTS (
  SELECT 1 FROM bench_numeric_:suffix n
  WHERE n.category = m.rating AND n.value1 > 8000
)
GROUP BY m.department;

-- 9) Recursive CTE (CONNECT BY analog)
SET pg_strom.enabled = off; EXPLAIN ANALYZE
WITH RECURSIVE tree AS (
  SELECT id, parent_id, name, 1 AS depth FROM bench_tree_:suffix WHERE parent_id IS NULL
  UNION ALL
  SELECT t.id, t.parent_id, t.name, tree.depth+1
  FROM bench_tree_:suffix t
  JOIN tree ON t.parent_id = tree.id
)
SELECT MAX(depth) max_depth, COUNT(*) total FROM tree;

SET pg_strom.enabled = on; EXPLAIN ANALYZE
WITH RECURSIVE tree AS (
  SELECT id, parent_id, name, 1 AS depth FROM bench_tree_:suffix WHERE parent_id IS NULL
  UNION ALL
  SELECT t.id, t.parent_id, t.name, tree.depth+1
  FROM bench_tree_:suffix t
  JOIN tree ON t.parent_id = tree.id
)
SELECT MAX(depth) max_depth, COUNT(*) total FROM tree;
