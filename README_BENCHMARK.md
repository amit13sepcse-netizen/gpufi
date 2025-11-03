# PG-Strom CPU vs GPU Benchmark Guide

This guide explains how to use the benchmark script to compare CPU and GPU performance for PostgreSQL queries with PG-Strom.

## Overview

The benchmark script (`benchmark_pgstrom.sh`) creates test tables with varying data sizes and runs identical queries with CPU-only execution and GPU-accelerated execution to measure performance differences.

## Prerequisites

- PostgreSQL 15 with PG-Strom installed and running
- The `postgres_gpu` cluster active on port 5434 (or adjust `DB_PORT`)
- Sufficient disk space for test tables (10M rows ≈ 1-2 GB per table)

## Quick Start

### Run the benchmark:

```bash
sudo ./benchmark_pgstrom.sh
```

### View results:

```bash
cat benchmark_results_*.log
```

## Configuration

You can customize the benchmark by setting environment variables:

```bash
# Use a different database
DB_NAME=my_test_db ./benchmark_pgstrom.sh

# Use a different port
DB_PORT=5432 ./benchmark_pgstrom.sh

# Use a different user
DB_USER=myuser ./benchmark_pgstrom.sh
```

## What the Benchmark Tests

### 1. Data Generation
- **Small dataset**: 100,000 rows
- **Medium dataset**: 1,000,000 rows
- **Large dataset**: 10,000,000 rows (optional, disabled by default)

### 2. Table Types

#### Numeric Table (`bench_numeric_*`)
- Optimized for GPU computation
- Contains 5 numeric columns + category
- Good for testing aggregations and mathematical operations

#### Mixed Table (`bench_mixed_*`)
- Real-world scenario with various data types
- TEXT, INTEGER, NUMERIC, DATE, BOOLEAN, DOUBLE PRECISION
- Tests filtering and mixed operations

### 3. Query Types Tested

#### Test 1: Simple Aggregation
- `COUNT(*)`, `AVG()`, `SUM()`, `MAX()`, `MIN()`
- With WHERE clause filtering
- Grouped by category

#### Test 2: Complex Mathematical Calculations
- Division, multiplication, square root
- Statistical functions (STDDEV)
- Complex WHERE conditions with HAVING clause

#### Test 3: Complex Filtering
- Multiple AND conditions
- BETWEEN ranges
- Boolean logic
- Aggregation on filtered data

#### Test 4: Window Functions
- Partitioned averages
- ROW_NUMBER() ranking
- ORDER BY within partitions

## Understanding Results

### Performance Metrics

Each test shows:
- **CPU time**: Execution time with `pg_strom.enabled = off`
- **GPU time**: Execution time with `pg_strom.enabled = on`
- **Speedup**: Ratio of CPU time to GPU time (higher is better)

Example output:
```
[RESULT] Aggregation (CPU) completed in 2.45s (2450ms)
[RESULT] Aggregation (GPU) completed in 0.82s (820ms)
[RESULT] Speedup for aggregation: 2.99x
```

This means the GPU was **2.99 times faster** than CPU for this query.

### When GPU Helps Most

GPU acceleration typically provides the best speedup for:
1. **Large datasets** (millions of rows)
2. **Aggregation operations** (SUM, AVG, COUNT)
3. **Mathematical computations** (multiplications, divisions)
4. **Wide scans** with filtering
5. **Parallel processing** of independent rows

### When CPU May Be Faster

CPU execution may be faster for:
1. **Small datasets** (< 10,000 rows)
2. **Index lookups** (B-tree traversal)
3. **String operations** (LIKE, regex)
4. **Highly selective queries** (returning few rows)
5. **Complex joins** with small tables

## Customizing the Benchmark

### Adjust Data Sizes

Edit `benchmark_pgstrom.sh` and modify:

```bash
SMALL_SIZE=100000      # 100K rows
MEDIUM_SIZE=1000000    # 1M rows
LARGE_SIZE=10000000    # 10M rows
```

### Enable Large Dataset

In the `main()` function, add `"LARGE"` to the test loop:

```bash
for size in "SMALL" "MEDIUM" "LARGE"; do
```

### Add Custom Tests

Add new test queries in the `benchmark_select()` function:

```bash
# Test 5: Your custom query
print_info "Test 5: Custom query description"
local cpu_time_5=$(execute_explain_analyze "Custom (CPU)" \
    "YOUR SQL QUERY HERE;" "off")

local gpu_time_5=$(execute_explain_analyze "Custom (GPU)" \
    "YOUR SQL QUERY HERE;" "on")

if [ "$gpu_time_5" != "0" ] && [ "$cpu_time_5" != "0" ]; then
    local speedup=$(echo "scale=2; $cpu_time_5 / $gpu_time_5" | bc)
    print_result "Speedup for custom query: ${speedup}x"
fi
```

## Cleanup

Remove test tables after benchmarking:

```bash
sudo -u postgres psql -p 5434 -d pgstrom_test -c "
    DROP TABLE IF EXISTS bench_numeric_small CASCADE;
    DROP TABLE IF EXISTS bench_numeric_medium CASCADE;
    DROP TABLE IF EXISTS bench_numeric_large CASCADE;
    DROP TABLE IF EXISTS bench_mixed_small CASCADE;
    DROP TABLE IF EXISTS bench_mixed_medium CASCADE;
    DROP TABLE IF EXISTS bench_mixed_large CASCADE;
"
```

## Troubleshooting

### Script can't connect to database

Check that:
1. PostgreSQL is running: `systemctl status postgresql@15-postgres_gpu`
2. Cluster is active: `pg_lsclusters`
3. Port is correct: use `pg_lsclusters` to find the port

### GPU not being used

Check PG-Strom is loaded:
```bash
sudo -u postgres psql -p 5434 -d pgstrom_test -c "SHOW shared_preload_libraries;"
```

Should show `pg_strom`.

### Verify GPU visibility:
```bash
sudo -u postgres psql -p 5434 -d pgstrom_test -c "SELECT pgstrom.githash();"
```

### Performance not as expected

1. **Check GPU is being used**: Look for "GpuScan" or "GpuPreAgg" in EXPLAIN ANALYZE output
2. **Increase work_mem**: Larger work_mem can help GPU operations
3. **Analyze tables**: Run `ANALYZE table_name;` before benchmarking
4. **Warm up**: Run queries once before timing to warm caches

### View query plans

To see if GPU is actually being used:

```bash
sudo -u postgres psql -p 5434 -d pgstrom_test <<EOF
SET pg_strom.enabled = on;
EXPLAIN (ANALYZE, BUFFERS) 
SELECT category, COUNT(*), AVG(value1)
FROM bench_numeric_small
WHERE value1 > 5000
GROUP BY category;
EOF
```

Look for nodes like:
- `Custom Scan (GpuScan)`
- `Custom Scan (GpuPreAgg)`
- `Custom Scan (GpuJoin)`

## Advanced: Monitoring GPU Usage

While benchmark is running, monitor GPU in another terminal:

```bash
watch -n 1 nvidia-smi
```

Look for:
- **GPU-Util**: Should spike during GPU queries
- **Memory-Usage**: Should increase during large operations

## Tips for Best Results

1. **Close other applications** to reduce system load
2. **Run multiple times** and average results
3. **Test with your actual data** - create tables similar to your workload
4. **Tune PostgreSQL settings**:
   - `shared_buffers = 10GB` (or 25% of RAM)
   - `work_mem = 1GB` (for large sorts/hashes)
   - `max_worker_processes = 100` (for parallelism)
5. **Consider data distribution** - skewed data may affect GPU efficiency

## Example Real-World Use Cases

### Analytics Dashboard
```sql
-- Daily aggregations across millions of events
SELECT date_trunc('day', event_time) as day,
       event_type,
       COUNT(*) as events,
       AVG(value) as avg_value,
       SUM(revenue) as total_revenue
FROM events
WHERE event_time >= CURRENT_DATE - INTERVAL '90 days'
GROUP BY 1, 2
ORDER BY 1 DESC;
```

### Time-Series Analysis
```sql
-- Compute moving averages on sensor data
SELECT sensor_id,
       reading_time,
       temperature,
       AVG(temperature) OVER (
           PARTITION BY sensor_id 
           ORDER BY reading_time 
           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW
       ) as moving_avg
FROM sensor_readings
WHERE reading_time >= CURRENT_TIMESTAMP - INTERVAL '7 days';
```

### Large Table Scans
```sql
-- Filter and aggregate across billions of rows
SELECT customer_segment,
       product_category,
       COUNT(*) as purchase_count,
       SUM(amount) as total_spend,
       AVG(amount) as avg_spend
FROM transactions
WHERE purchase_date BETWEEN '2024-01-01' AND '2024-12-31'
  AND amount > 100
GROUP BY customer_segment, product_category
HAVING COUNT(*) > 1000;
```

## Additional Resources

- [PG-Strom Documentation](https://heterodb.github.io/pg-strom/)
- [PostgreSQL Performance Tuning](https://www.postgresql.org/docs/15/performance-tips.html)
- [NVIDIA GPU Monitoring](https://developer.nvidia.com/nvidia-system-management-interface)

## Getting Help

If you encounter issues:

1. Check PostgreSQL logs: `sudo tail -f /var/log/postgresql/postgresql-15-postgres_gpu.log`
2. Review benchmark log: `cat benchmark_results_*.log`
3. Test PG-Strom manually: Connect via psql and run simple queries with EXPLAIN ANALYZE
4. Verify GPU drivers: `nvidia-smi`

---

**Note**: GPU acceleration is most beneficial for OLAP (analytical) workloads with large datasets. OLTP (transactional) workloads with many small queries may not see significant benefits.
