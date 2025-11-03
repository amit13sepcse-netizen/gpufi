#!/bin/bash
# Log resource usage (CPU and GPU) with timestamp, test name, and mode
log_resource_usage() {
    local test_name="$1"
    local mode="$2" # CPU or GPU
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    # Get CPU usage (system-wide, top 1s sample)
    local cpu_percent=$(top -b -n2 -d 0.5 | grep "Cpu(s)" | tail -1 | awk '{print $2+$4}')
    # Get GPU usage (NVIDIA, first GPU)
    local gpu_percent="-"
    if command -v nvidia-smi &>/dev/null; then
        gpu_percent=$(nvidia-smi --query-gpu=utilization.gpu --format=csv,noheader,nounits | head -1)
    fi
    echo "$timestamp,$test_name,$mode,CPU:${cpu_percent}%,GPU:${gpu_percent}%" >> "$DATA_DIR/benchmark_resource_usage.log"
}

# Parse resource usage log and build usage maps
declare -A CPU_USAGE_MAP
declare -A GPU_USAGE_MAP
if [ -f "$DATA_DIR/benchmark_resource_usage.log" ]; then
    while IFS=',' read -r ts tname mode cpu gpu; do
        cpu_val=$(echo "$cpu" | grep -o '[0-9.]*')
        gpu_val=$(echo "$gpu" | grep -o '[0-9.]*')
        key="$tname"
        # Store all samples for each test
        CPU_USAGE_MAP[$key]="${CPU_USAGE_MAP[$key]} $cpu_val"
        GPU_USAGE_MAP[$key]="${GPU_USAGE_MAP[$key]} $gpu_val"
    done < "$DATA_DIR/benchmark_resource_usage.log"
fi

###############################################################################
# PG-Strom CPU vs GPU Benchmark Script
# Tests INSERT and SELECT performance with various data sizes
###############################################################################

set -e

# Parse command line arguments
CLEANUP_ONLY=false
SKIP_CLEANUP=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --cleanup)
            CLEANUP_ONLY=true
            shift
            ;;
        --skip-cleanup)
            SKIP_CLEANUP=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --cleanup         Only clean up test tables and exit"
            echo "  --skip-cleanup    Skip cleanup prompt at the end"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  DB_NAME           Database name (default: pgstrom_test)"
            echo "  DB_PORT           Database port (default: 5434)"
            echo "  DB_USER           Database user (default: postgres)"
            echo "  RUN_SIZES         Dataset sizes to run (default: 'SMALL MEDIUM LARGE')"
            echo ""
            echo "Dataset Sizes:"
            echo "  SMALL             100,000 rows (100K)"
            echo "  MEDIUM            1,000,000 rows (1M)"
            echo "  LARGE             10,000,000 rows (10M)"
            echo "  XLARGE            100,000,000 rows (100M)"
            echo "  XXLARGE           1,000,000,000 rows (1B)"
            echo ""
            echo "Examples:"
            echo "  # Run default sizes (SMALL, MEDIUM, LARGE)"
            echo "  sudo ./benchmark_pgstrom.sh"
            echo ""
            echo "  # Run only small and medium datasets"
            echo "  RUN_SIZES='SMALL MEDIUM' sudo ./benchmark_pgstrom.sh"
            echo ""
            echo "  # Run large datasets (may take hours)"
            echo "  RUN_SIZES='LARGE XLARGE' sudo ./benchmark_pgstrom.sh"
            echo ""
            echo "  # Run all datasets including 1 billion rows (may take many hours)"
            echo "  RUN_SIZES='SMALL MEDIUM LARGE XLARGE XXLARGE' sudo ./benchmark_pgstrom.sh"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Configuration
DB_NAME="${DB_NAME:-pgstrom_test}"
DB_PORT="${DB_PORT:-5434}"
DB_USER="${DB_USER:-postgres}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA_DIR="${PROJECT_ROOT}/data"
mkdir -p "$DATA_DIR"
BENCHMARK_RESULTS="${DATA_DIR}/benchmark_results_$(date +%Y%m%d_%H%M%S).log"
HTML_REPORT="${DATA_DIR}/benchmark_report_$(date +%Y%m%d_%H%M%S).html"
QUERY_AUDIT_LOG="${DATA_DIR}/query_audit_$(date +%Y%m%d_%H%M%S).log"

# Initialize query audit log
touch "$QUERY_AUDIT_LOG"
chmod 666 "$QUERY_AUDIT_LOG" 2>/dev/null || true

# Optional top-like GPU monitoring
# Enable with: MONITOR_GPU=1 ./benchmark_pgstrom.sh
MONITOR_GPU="${MONITOR_GPU:-0}"
GPU_TOP_INTERVAL="${GPU_TOP_INTERVAL:-1}"
GPU_TOP_MAX_PROCS="${GPU_TOP_MAX_PROCS:-10}"
GPU_TOP_SORT_UTIL="${GPU_TOP_SORT_UTIL:-1}"
GPU_TOP_LOG=""   # will be set if monitor starts
GPU_TOP_PID=""   # background monitor pid

# Test data sizes (number of rows)
SMALL_SIZE=100000          # 100K rows
MEDIUM_SIZE=1000000        # 1M rows
LARGE_SIZE=10000000        # 10M rows
XLARGE_SIZE=100000000      # 100M rows
XXLARGE_SIZE=1000000000    # 1B rows (1 billion)

# Which dataset sizes to run (can be overridden by environment variable)
# Example: RUN_SIZES="SMALL MEDIUM LARGE" ./benchmark_pgstrom.sh
RUN_SIZES="${RUN_SIZES:-SMALL MEDIUM LARGE}"

print_info() {
    echo -e "${GREEN}[INFO]${NC} $1" | tee -a ${BENCHMARK_RESULTS}
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1" | tee -a ${BENCHMARK_RESULTS}
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a ${BENCHMARK_RESULTS}
}

print_success() {
    echo -e "${CYAN}[SUCCESS]${NC} $1" | tee -a ${BENCHMARK_RESULTS}
}

print_result() {
    echo -e "${BLUE}[RESULT]${NC} $1" | tee -a ${BENCHMARK_RESULTS}
}

# ---------------- GPU top-like monitor helpers ----------------
start_gpu_monitor() {
    # Only start if requested and nvidia-smi + gpu_top.py are available
    if [[ "$MONITOR_GPU" != "1" ]]; then
        return 0
    fi
    if ! command -v nvidia-smi >/dev/null 2>&1; then
        print_warn "MONITOR_GPU=1 requested, but nvidia-smi not found; skipping gpu_top monitor"
        return 0
    fi
    if [[ ! -x "$SCRIPT_DIR/gpu_top.py" ]]; then
        print_warn "gpu_top.py not found or not executable at $SCRIPT_DIR; skipping monitor"
        return 0
    fi

    GPU_TOP_LOG="${DATA_DIR}/gpu_top_${DB_NAME}_$(date +%Y%m%d_%H%M%S).log"
    # Pre-create with relaxed perms so non-root shells can read while script runs under sudo
    touch "$GPU_TOP_LOG" 2>/dev/null || true
    chmod 666 "$GPU_TOP_LOG" 2>/dev/null || true

    print_info "Starting GPU top-like monitor -> ${GPU_TOP_LOG} (interval=${GPU_TOP_INTERVAL}s)"
    # Background loop: snapshot once per interval
    (
        while true; do
            "$SCRIPT_DIR/gpu_top.py" --once >> "$GPU_TOP_LOG" 2>&1
            echo "" >> "$GPU_TOP_LOG"
            sleep "$GPU_TOP_INTERVAL"
        done
    ) &
    GPU_TOP_PID=$!
}

stop_gpu_monitor() {
    if [[ -n "$GPU_TOP_PID" ]] && kill -0 "$GPU_TOP_PID" 2>/dev/null; then
        print_info "Stopping GPU top-like monitor (pid=$GPU_TOP_PID)"
        kill "$GPU_TOP_PID" 2>/dev/null || true
        wait "$GPU_TOP_PID" 2>/dev/null || true
        GPU_TOP_PID=""
    fi
}

# Ensure monitor is stopped on exit
trap stop_gpu_monitor EXIT

# Function to execute SQL and measure time
execute_timed_sql() {
    local description="$1"
    local sql="$2"
    
    print_info "Running: $description"
    
    local start_time=$(date +%s.%N)
    local start_timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    local result
    
    result=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -c \"$sql\"" 2>&1)
    local exit_code=$?
    
    local end_time=$(date +%s.%N)
    local end_timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    local duration=$(echo "$end_time - $start_time" | bc)
    
    echo "$result" >> ${BENCHMARK_RESULTS}
    
    # Log query execution details
    if [ -n "$QUERY_AUDIT_LOG" ]; then
        echo "=== QUERY AUDIT ===" >> "$QUERY_AUDIT_LOG"
        echo "Description: $description" >> "$QUERY_AUDIT_LOG"
        echo "Mode: TIMED_SQL" >> "$QUERY_AUDIT_LOG"
        echo "Start: $start_timestamp" >> "$QUERY_AUDIT_LOG"
        echo "End: $end_timestamp" >> "$QUERY_AUDIT_LOG"
        echo "Duration: ${duration}s" >> "$QUERY_AUDIT_LOG"
        echo "Exit Code: $exit_code" >> "$QUERY_AUDIT_LOG"
        echo "SQL: $sql" >> "$QUERY_AUDIT_LOG"
        echo "" >> "$QUERY_AUDIT_LOG"
    fi
    
    if [ $exit_code -eq 0 ]; then
        print_result "$description completed in ${duration}s"
        return 0
    else
        print_error "$description failed"
        echo "Error output:" >> ${BENCHMARK_RESULTS}
        echo "$result" >> ${BENCHMARK_RESULTS}
        return 1
    fi
}

# Function to get timing from EXPLAIN ANALYZE
execute_explain_analyze() {
    local description="$1"
    local sql="$2"
    local use_gpu="$3"  # "on" or "off"
    
    print_info "Running: $description (GPU: $use_gpu)"
    
    local full_sql="SET pg_strom.enabled = $use_gpu; EXPLAIN ANALYZE $sql"
    local start_timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    
    local output=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -c \"$full_sql\"" 2>&1)
    local end_timestamp=$(date '+%Y-%m-%d %H:%M:%S.%3N')
    
    echo "$output" >> ${BENCHMARK_RESULTS}
    
    # Extract execution time
    local exec_time=$(echo "$output" | grep "Execution Time:" | sed -n 's/.*Execution Time: \([0-9.]*\) ms/\1/p')
    
    if [ -n "$exec_time" ]; then
        local exec_time_sec=$(echo "scale=3; $exec_time / 1000" | bc)
        
        # Log query execution details
        if [ -n "$QUERY_AUDIT_LOG" ]; then
            echo "=== QUERY AUDIT ===" >> "$QUERY_AUDIT_LOG"
            echo "Description: $description" >> "$QUERY_AUDIT_LOG"
            echo "Mode: $([ "$use_gpu" = "on" ] && echo "GPU" || echo "CPU")" >> "$QUERY_AUDIT_LOG"
            echo "Start: $start_timestamp" >> "$QUERY_AUDIT_LOG"
            echo "End: $end_timestamp" >> "$QUERY_AUDIT_LOG"
            echo "Duration: ${exec_time_sec}s (${exec_time}ms)" >> "$QUERY_AUDIT_LOG"
            echo "SQL: $sql" >> "$QUERY_AUDIT_LOG"
            echo "" >> "$QUERY_AUDIT_LOG"
        fi
        
        print_result "$description (GPU: $use_gpu) completed in ${exec_time_sec}s (${exec_time}ms)" >&2
        # Return only the numeric value
        echo "$exec_time_sec"
    else
        print_warn "Could not extract execution time for $description" >&2
        echo "0"
    fi
}

# Cleanup function to drop test tables
cleanup_test_tables() {
    print_info "=========================================="
    print_info "Cleaning up test tables..."
    print_info "=========================================="
    
    local cleanup_result
    cleanup_result=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME}" 2>&1 <<EOF
DROP TABLE IF EXISTS bench_numeric_small CASCADE;
DROP TABLE IF EXISTS bench_numeric_medium CASCADE;
DROP TABLE IF EXISTS bench_numeric_large CASCADE;
DROP TABLE IF EXISTS bench_numeric_xlarge CASCADE;
DROP TABLE IF EXISTS bench_numeric_xxlarge CASCADE;
DROP TABLE IF EXISTS bench_mixed_small CASCADE;
DROP TABLE IF EXISTS bench_mixed_medium CASCADE;
DROP TABLE IF EXISTS bench_mixed_large CASCADE;
DROP TABLE IF EXISTS bench_mixed_xlarge CASCADE;
DROP TABLE IF EXISTS bench_mixed_xxlarge CASCADE;
DROP TABLE IF EXISTS bench_numeric CASCADE;
DROP TABLE IF EXISTS bench_mixed CASCADE;
DROP TABLE IF EXISTS bench_aggregation CASCADE;
DROP TABLE IF EXISTS bench_join_a CASCADE;
DROP TABLE IF EXISTS bench_join_b CASCADE;
EOF
)
    
    local cleanup_status=$?
    if [ $cleanup_status -eq 0 ]; then
        print_success "Test tables cleaned up successfully"
        return 0
    else
        print_error "Failed to clean up test tables"
        echo "$cleanup_result"
        return 1
    fi
}

# Setup test database
setup_database() {
    print_info "=========================================="
    print_info "Setting up benchmark database"
    print_info "=========================================="
    
    # Enable timing
    sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -c '\\timing on'" >> ${BENCHMARK_RESULTS} 2>&1
    
    # Drop existing test tables with proper error handling
    print_info "Cleaning up existing test tables..."
    local drop_result
    drop_result=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME}" 2>&1 <<EOF
DROP TABLE IF EXISTS bench_numeric_small CASCADE;
DROP TABLE IF EXISTS bench_numeric_medium CASCADE;
DROP TABLE IF EXISTS bench_numeric_large CASCADE;
DROP TABLE IF EXISTS bench_numeric_xlarge CASCADE;
DROP TABLE IF EXISTS bench_numeric_xxlarge CASCADE;
DROP TABLE IF EXISTS bench_mixed_small CASCADE;
DROP TABLE IF EXISTS bench_mixed_medium CASCADE;
DROP TABLE IF EXISTS bench_mixed_large CASCADE;
DROP TABLE IF EXISTS bench_mixed_xlarge CASCADE;
DROP TABLE IF EXISTS bench_mixed_xxlarge CASCADE;
DROP TABLE IF EXISTS bench_numeric CASCADE;
DROP TABLE IF EXISTS bench_mixed CASCADE;
DROP TABLE IF EXISTS bench_aggregation CASCADE;
DROP TABLE IF EXISTS bench_join_a CASCADE;
DROP TABLE IF EXISTS bench_join_b CASCADE;
EOF
)
    
    local drop_status=$?
    if [ $drop_status -ne 0 ]; then
        print_error "Failed to drop existing tables"
        echo "$drop_result"
        return 1
    fi
    
    print_success "Database setup complete - all existing test tables dropped"
}

# Create and populate test tables
create_test_tables() {
    local row_count=$1
    local table_suffix=$2
    
    print_info "=========================================="
    print_info "Creating test tables with $row_count rows"
    print_info "=========================================="
    
    # Table 1: Numeric-heavy operations (good for GPU)
    print_info "Creating bench_numeric_${table_suffix}..."
    if ! execute_timed_sql "Create numeric table" "
    CREATE TABLE bench_numeric_${table_suffix} (
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
    );"; then
        print_error "Failed to create bench_numeric_${table_suffix} table"
        return 1
    fi
    
    print_info "Inserting $row_count rows into bench_numeric_${table_suffix}..."
    if ! execute_timed_sql "Insert numeric data" "
    INSERT INTO bench_numeric_${table_suffix} (value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11, value12, value13, value14, value15, category)
    SELECT 
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        random() * 10000,
        (random() * 100)::INTEGER
    FROM generate_series(1, $row_count);"; then
        print_error "Failed to insert data into bench_numeric_${table_suffix}"
        return 1
    fi
    
    # Table 2: Mixed data types with CLOB fields
    print_info "Creating bench_mixed_${table_suffix}..."
    if ! execute_timed_sql "Create mixed table" "
    CREATE TABLE bench_mixed_${table_suffix} (
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
    );"; then
        print_error "Failed to create bench_mixed_${table_suffix} table"
        return 1
    fi
    
    print_info "Inserting $row_count rows into bench_mixed_${table_suffix}..."
    if ! execute_timed_sql "Insert mixed data" "
    INSERT INTO bench_mixed_${table_suffix} (name, email, age, salary, bonus, hire_date, is_active, score, department, city, rating, commission, years_exp, performance_score, remote_work, project_count, training_hours, certifications, description, notes, profile_summary, work_history)
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
    FROM generate_series(1, $row_count) AS i;"; then
        print_error "Failed to insert data into bench_mixed_${table_suffix}"
        return 1
    fi
    
    # Create indexes
    print_info "Creating indexes..."
    if ! execute_timed_sql "Create indexes" "
    CREATE INDEX idx_numeric_cat_${table_suffix} ON bench_numeric_${table_suffix}(category);
    CREATE INDEX idx_numeric_v1_${table_suffix} ON bench_numeric_${table_suffix}(value1);
    CREATE INDEX idx_mixed_age_${table_suffix} ON bench_mixed_${table_suffix}(age);
    CREATE INDEX idx_mixed_salary_${table_suffix} ON bench_mixed_${table_suffix}(salary);"; then
        print_error "Failed to create indexes"
        return 1
    fi
    
    # Analyze tables
    print_info "Analyzing tables..."
    if ! execute_timed_sql "Analyze tables" "
    ANALYZE bench_numeric_${table_suffix};
    ANALYZE bench_mixed_${table_suffix};"; then
        print_error "Failed to analyze tables"
        return 1
    fi
    
    print_success "Test tables created and populated successfully"
    return 0
}

# Arrays to store benchmark results for side-by-side comparison
declare -a TEST_NAMES
declare -a CPU_TIMES
declare -a GPU_TIMES
declare -a SPEEDUPS
declare -a DATASET_SEPARATORS  # Track where dataset boundaries are

# Benchmark SELECT operations
benchmark_select() {
    local table_suffix=$1
    local row_count=$2
    
    print_info "=========================================="
    print_info "Benchmarking SELECT operations on ${table_suffix} dataset"
    print_info "=========================================="
    
    # Add a separator marker for this dataset
    DATASET_SEPARATORS+=("${#TEST_NAMES[@]}")  # Store index where this dataset starts
    TEST_NAMES+=("=== ${table_suffix^^} (${row_count} rows) ===")
    CPU_TIMES+=("-")
    GPU_TIMES+=("-")
    SPEEDUPS+=("-")
    
    # Test 1: Simple aggregation
    print_info "Test 1: Simple aggregation with WHERE clause"
    log_resource_usage "Simple Aggregation" "CPU-start"
    local cpu_time_1=$(execute_explain_analyze "Aggregation (CPU)" \
        "SELECT category, 
                COUNT(*) as cnt,
                AVG(value1) as avg1,
                AVG(value2) as avg2,
                SUM(value3) as sum3,
                MAX(value4) as max4,
                MIN(value5) as min5,
                AVG(value6) as avg6,
                SUM(value7) as sum7,
                MAX(value8) as max8,
                MIN(value9) as min9,
                AVG(value10) as avg10
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 5000 AND value6 < 8000
         GROUP BY category;" "off")
    log_resource_usage "Simple Aggregation" "CPU-end"
    
    log_resource_usage "Simple Aggregation" "GPU-start"
    local gpu_time_1=$(execute_explain_analyze "Aggregation (GPU)" \
        "SELECT category, 
                COUNT(*) as cnt,
                AVG(value1) as avg1,
                AVG(value2) as avg2,
                SUM(value3) as sum3,
                MAX(value4) as max4,
                MIN(value5) as min5,
                AVG(value6) as avg6,
                SUM(value7) as sum7,
                MAX(value8) as max8,
                MIN(value9) as min9,
                AVG(value10) as avg10
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 5000 AND value6 < 8000
         GROUP BY category;" "on")
    log_resource_usage "Simple Aggregation" "GPU-end"
    
    if [ "$gpu_time_1" != "0" ] && [ "$cpu_time_1" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_1=$(echo "$cpu_time_1" | tr -cd '0-9.')
        gpu_time_1=$(echo "$gpu_time_1" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_1 / $gpu_time_1" | bc)
        print_result "Speedup for aggregation: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("Simple Aggregation")
        CPU_TIMES+=("$cpu_time_1")
        GPU_TIMES+=("$gpu_time_1")
        SPEEDUPS+=("$speedup")
    fi
    
    # Test 2: Complex calculation
    print_info "Test 2: Complex mathematical calculations"
    log_resource_usage "Complex Math" "CPU-start"
    local cpu_time_2=$(execute_explain_analyze "Math operations (CPU)" \
        "SELECT 
            category,
            COUNT(*) as cnt,
            AVG(value1 * value2 / NULLIF(value3, 0)) as complex_avg,
            SUM(SQRT(value1 * value1 + value2 * value2)) as distance_sum,
            STDDEV(value4) as stddev4,
            AVG(value5 * value6 / NULLIF(value7, 0)) as ratio1,
            SUM(value8 + value9 + value10) as total_sum,
            MAX(value11 * value12) as max_product,
            MIN(value13 / NULLIF(value14, 0)) as min_ratio,
            AVG(SQRT(value15 * value15 + value1 * value1)) as dist_avg
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 1000 AND value2 < 9000 AND value11 > 2000
         GROUP BY category
         HAVING COUNT(*) > 100;" "off")
    log_resource_usage "Complex Math" "CPU-end"
    
    log_resource_usage "Complex Math" "GPU-start"
    local gpu_time_2=$(execute_explain_analyze "Math operations (GPU)" \
        "SELECT 
            category,
            COUNT(*) as cnt,
            AVG(value1 * value2 / NULLIF(value3, 0)) as complex_avg,
            SUM(SQRT(value1 * value1 + value2 * value2)) as distance_sum,
            STDDEV(value4) as stddev4,
            AVG(value5 * value6 / NULLIF(value7, 0)) as ratio1,
            SUM(value8 + value9 + value10) as total_sum,
            MAX(value11 * value12) as max_product,
            MIN(value13 / NULLIF(value14, 0)) as min_ratio,
            AVG(SQRT(value15 * value15 + value1 * value1)) as dist_avg
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 1000 AND value2 < 9000 AND value11 > 2000
         GROUP BY category
         HAVING COUNT(*) > 100;" "on")
    log_resource_usage "Complex Math" "GPU-end"
    
    if [ "$gpu_time_2" != "0" ] && [ "$cpu_time_2" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_2=$(echo "$cpu_time_2" | tr -cd '0-9.')
        gpu_time_2=$(echo "$gpu_time_2" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_2 / $gpu_time_2" | bc)
        print_result "Speedup for complex math: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("Complex Math")
        CPU_TIMES+=("$cpu_time_2")
        GPU_TIMES+=("$gpu_time_2")
        SPEEDUPS+=("$speedup")
    fi
    
    # Test 3: Filtering with multiple conditions
        print_info "Test 3: Complex filtering"
        log_resource_usage "Complex Filtering" "CPU-start"
    local cpu_time_3=$(execute_explain_analyze "Complex filter (CPU)" \
        "SELECT COUNT(*), AVG(salary), MAX(bonus), 
                AVG(commission), SUM(training_hours),
                AVG(performance_score), COUNT(DISTINCT department)
         FROM bench_mixed_${table_suffix}
         WHERE age BETWEEN 30 AND 50
           AND salary > 50000
           AND is_active = true
           AND score > 50
           AND rating >= 3
           AND years_exp > 2
           AND project_count > 5;" "off")
        log_resource_usage "Complex Filtering" "CPU-end"
    
        log_resource_usage "Complex Filtering" "GPU-start"
    local gpu_time_3=$(execute_explain_analyze "Complex filter (GPU)" \
        "SELECT COUNT(*), AVG(salary), MAX(bonus), 
                AVG(commission), SUM(training_hours),
                AVG(performance_score), COUNT(DISTINCT department)
         FROM bench_mixed_${table_suffix}
         WHERE age BETWEEN 30 AND 50
           AND salary > 50000
           AND is_active = true
           AND score > 50
           AND rating >= 3
           AND years_exp > 2
           AND project_count > 5;" "on")
        log_resource_usage "Complex Filtering" "GPU-end"
    
    if [ "$gpu_time_3" != "0" ] && [ "$cpu_time_3" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_3=$(echo "$cpu_time_3" | tr -cd '0-9.')
        gpu_time_3=$(echo "$gpu_time_3" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_3 / $gpu_time_3" | bc)
        print_result "Speedup for complex filter: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("Complex Filtering")
        CPU_TIMES+=("$cpu_time_3")
        GPU_TIMES+=("$gpu_time_3")
        SPEEDUPS+=("$speedup")
    fi
    
    # Test 4: Window functions
    print_info "Test 4: Window functions"
    log_resource_usage "Window Functions" "CPU-start"
    local cpu_time_4=$(execute_explain_analyze "Window functions (CPU)" \
        "SELECT 
            category,
            value1,
            value2,
            AVG(value1) OVER (PARTITION BY category) as avg_by_cat,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY value1 DESC) as rank_in_cat
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 3000
         LIMIT 10000;" "off")
    log_resource_usage "Window Functions" "CPU-end"
    
    log_resource_usage "Window Functions" "GPU-start"
    local gpu_time_4=$(execute_explain_analyze "Window functions (GPU)" \
        "SELECT 
            category,
            value1,
            value2,
            AVG(value1) OVER (PARTITION BY category) as avg_by_cat,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY value1 DESC) as rank_in_cat
         FROM bench_numeric_${table_suffix}
         WHERE value1 > 3000
         LIMIT 10000;" "on")
    log_resource_usage "Window Functions" "GPU-end"
    
    if [ "$gpu_time_4" != "0" ] && [ "$cpu_time_4" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_4=$(echo "$cpu_time_4" | tr -cd '0-9.')
        gpu_time_4=$(echo "$gpu_time_4" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_4 / $gpu_time_4" | bc)
        print_result "Speedup for window functions: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("Window Functions")
        CPU_TIMES+=("$cpu_time_4")
        GPU_TIMES+=("$gpu_time_4")
        SPEEDUPS+=("$speedup")
    fi
    
    # Test 5: String concatenation operations
    print_info "Test 5: String concatenation and text operations"
    log_resource_usage "String Operations" "CPU-start"
    local cpu_time_5=$(execute_explain_analyze "String operations (CPU)" \
        "SELECT 
            department,
            COUNT(*) as emp_count,
            COUNT(DISTINCT city) as city_count,
            LENGTH(STRING_AGG(name, ', ')) as concat_length,
            AVG(LENGTH(description)) as avg_desc_len,
            MAX(LENGTH(notes)) as max_notes_len,
            COUNT(*) FILTER (WHERE description LIKE '%details%') as keyword_count
         FROM bench_mixed_${table_suffix}
         WHERE is_active = true
           AND rating >= 3
         GROUP BY department;" "off")
    log_resource_usage "String Operations" "CPU-end"
    
    log_resource_usage "String Operations" "GPU-start"
    local gpu_time_5=$(execute_explain_analyze "String operations (GPU)" \
        "SELECT 
            department,
            COUNT(*) as emp_count,
            COUNT(DISTINCT city) as city_count,
            LENGTH(STRING_AGG(name, ', ')) as concat_length,
            AVG(LENGTH(description)) as avg_desc_len,
            MAX(LENGTH(notes)) as max_notes_len,
            COUNT(*) FILTER (WHERE description LIKE '%details%') as keyword_count
         FROM bench_mixed_${table_suffix}
         WHERE is_active = true
           AND rating >= 3
         GROUP BY department;" "on")
    log_resource_usage "String Operations" "GPU-end"
    
    if [ "$gpu_time_5" != "0" ] && [ "$cpu_time_5" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_5=$(echo "$cpu_time_5" | tr -cd '0-9.')
        gpu_time_5=$(echo "$gpu_time_5" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_5 / $gpu_time_5" | bc)
        print_result "Speedup for string operations: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("String Operations")
        CPU_TIMES+=("$cpu_time_5")
        GPU_TIMES+=("$gpu_time_5")
        SPEEDUPS+=("$speedup")
    fi
    
    # Test 6: CLOB field text search and concatenation
    print_info "Test 6: CLOB text search with concatenation"
    log_resource_usage "CLOB Operations" "CPU-start"
    local cpu_time_6=$(execute_explain_analyze "CLOB operations (CPU)" \
        "SELECT 
            city,
            COUNT(*) as total,
            AVG(salary) as avg_salary,
            LENGTH(STRING_AGG(SUBSTRING(profile_summary, 1, 100), ' | ')) as summary_concat_len,
            SUM(LENGTH(work_history)) as total_history_size,
            COUNT(*) FILTER (WHERE work_history LIKE '%Company%') as with_history
         FROM bench_mixed_${table_suffix}
         WHERE LENGTH(description) > 100
           AND profile_summary IS NOT NULL
         GROUP BY city;" "off")
    log_resource_usage "CLOB Operations" "CPU-end"
    
    log_resource_usage "CLOB Operations" "GPU-start"
    local gpu_time_6=$(execute_explain_analyze "CLOB operations (GPU)" \
        "SELECT 
            city,
            COUNT(*) as total,
            AVG(salary) as avg_salary,
            LENGTH(STRING_AGG(SUBSTRING(profile_summary, 1, 100), ' | ')) as summary_concat_len,
            SUM(LENGTH(work_history)) as total_history_size,
            COUNT(*) FILTER (WHERE work_history LIKE '%Company%') as with_history
         FROM bench_mixed_${table_suffix}
         WHERE LENGTH(description) > 100
           AND profile_summary IS NOT NULL
         GROUP BY city;" "on")
    log_resource_usage "CLOB Operations" "GPU-end"
    
    if [ "$gpu_time_6" != "0" ] && [ "$cpu_time_6" != "0" ]; then
        # Strip any non-numeric characters except period
        cpu_time_6=$(echo "$cpu_time_6" | tr -cd '0-9.')
        gpu_time_6=$(echo "$gpu_time_6" | tr -cd '0-9.')
        local speedup=$(echo "scale=2; $cpu_time_6 / $gpu_time_6" | bc)
        print_result "Speedup for CLOB operations: ${speedup}x"
        
        # Store results for comparison table
        TEST_NAMES+=("CLOB Operations")
        CPU_TIMES+=("$cpu_time_6")
        GPU_TIMES+=("$gpu_time_6")
        SPEEDUPS+=("$speedup")
    fi
    
    print_success "Benchmark completed for ${table_suffix} dataset"
    return 0
}

# Generate side-by-side comparison table
generate_comparison_table() {
    local border_top="╔════════════════════════════════════════════════════════════════════════════════════════╗"
    local border_mid="╠════════════════════════════════════════════════════════════════════════════════════════╣"
    local border_bot="╚════════════════════════════════════════════════════════════════════════════════════════╝"
    local line_sep="├────────────────────────────────┬───────────────┬───────────────┬────────────┬────────────┤"
    
    echo ""
    echo ""
    
    # Print to terminal with colors and box drawing
    printf "${CYAN}${border_top}${NC}\n"
    printf "${CYAN}║${NC}${GREEN}%-86s${NC}${CYAN}║${NC}\n" "                        CPU vs GPU Performance Comparison"
    printf "${CYAN}${border_mid}${NC}\n"
    printf "${CYAN}║${NC} ${YELLOW}%-30s${NC} ${CYAN}│${NC} ${YELLOW}%-13s${NC} ${CYAN}│${NC} ${YELLOW}%-13s${NC} ${CYAN}│${NC} ${YELLOW}%-10s${NC} ${CYAN}│${NC} ${YELLOW}%-10s${NC} ${CYAN}║${NC}\n" \
        "Test Name" "CPU Time (s)" "GPU Time (s)" "Speedup" "Status"
    printf "${CYAN}${line_sep}${NC}\n"
    
    # Print to log file without colors
    {
        echo ""
        echo ""
        echo "╔════════════════════════════════════════════════════════════════════════════════════════╗"
        echo "║                        CPU vs GPU Performance Comparison                               ║"
        echo "╠════════════════════════════════════════════════════════════════════════════════════════╣"
        printf "║ %-30s │ %-13s │ %-13s │ %-10s │ %-10s ║\n" "Test Name" "CPU Time (s)" "GPU Time (s)" "Speedup" "Status"
        echo "├────────────────────────────────┬───────────────┬───────────────┬────────────┬────────────┤"
    } >> ${BENCHMARK_RESULTS}
    
    # Print results
    for i in "${!TEST_NAMES[@]}"; do
        local test_name="${TEST_NAMES[$i]}"
        local cpu_time="${CPU_TIMES[$i]}"
        local gpu_time="${GPU_TIMES[$i]}"
        local speedup="${SPEEDUPS[$i]}"
        
        # Check if this is a separator row (dataset header)
        if [[ "$test_name" == "==="* ]]; then
            # Print dataset separator with highlighting
            printf "${CYAN}║${NC}\n"
            printf "${CYAN}║${NC} ${GREEN}${BOLD}%-86s${NC} ${CYAN}║${NC}\n" "$test_name"
            printf "${CYAN}║${NC}\n"
            
            printf "║\n" >> ${BENCHMARK_RESULTS}
            printf "║ %-86s ║\n" "$test_name" >> ${BENCHMARK_RESULTS}
            printf "║\n" >> ${BENCHMARK_RESULTS}
            continue
        fi
        
        # Determine status based on speedup
        local status
        local status_color
        local status_icon
        if [[ "$speedup" == "-" ]]; then
            status="-"
            status_color="${NC}"
            status_icon=""
        elif (( $(echo "$speedup >= 2.0" | bc -l) )); then
            status="Excellent"
            status_color="${GREEN}"
            status_icon="🚀"
        elif (( $(echo "$speedup >= 1.2" | bc -l) )); then
            status="Good"
            status_color="${GREEN}"
            status_icon="✓"
        elif (( $(echo "$speedup >= 1.0" | bc -l) )); then
            status="Similar"
            status_color="${YELLOW}"
            status_icon="≈"
        else
            status="Slower"
            status_color="${RED}"
            status_icon="⚠"
        fi
        
        # Format speedup display
        local speedup_display
        if [[ "$speedup" != "-" ]]; then
            speedup_display="${speedup}x"
        else
            speedup_display="-"
        fi
        
        # Print to terminal with colors and box drawing
        printf "${CYAN}║${NC} %-30s ${CYAN}│${NC} ${BLUE}%13s${NC} ${CYAN}│${NC} ${GREEN}%13s${NC} ${CYAN}│${NC} ${YELLOW}%10s${NC} ${CYAN}│${NC} ${status_color}%-2s %-7s${NC} ${CYAN}║${NC}\n" \
            "$test_name" "$cpu_time" "$gpu_time" "$speedup_display" "$status_icon" "$status"
        
        # Print to log file without colors
        printf "║ %-30s │ %13s │ %13s │ %10s │ %-2s %-7s ║\n" \
            "$test_name" "$cpu_time" "$gpu_time" "$speedup_display" "$status_icon" "$status" >> ${BENCHMARK_RESULTS}
    done
    
    # Close the table
    printf "${CYAN}${border_bot}${NC}\n"
    echo "╚════════════════════════════════════════════════════════════════════════════════════════╝" >> ${BENCHMARK_RESULTS}
    
    echo ""
    echo "" >> ${BENCHMARK_RESULTS}
    
    # Calculate average speedup (excluding separator rows)
    if [ ${#SPEEDUPS[@]} -gt 0 ]; then
        local total_speedup=0
        local count=0
        for speedup in "${SPEEDUPS[@]}"; do
            # Skip separator rows (marked with "-")
            if [[ "$speedup" != "-" ]]; then
                total_speedup=$(echo "$total_speedup + $speedup" | bc)
                count=$((count + 1))
            fi
        done
        
        if [ $count -gt 0 ]; then
            local avg_speedup=$(echo "scale=2; $total_speedup / $count" | bc)
            
            # Print summary box
            local summary_box_top="┌────────────────────────────────────────────────────────────────┐"
            local summary_box_bot="└────────────────────────────────────────────────────────────────┘"
            
            printf "${CYAN}${summary_box_top}${NC}\n"
            printf "${CYAN}│${NC} ${YELLOW}Average GPU Speedup:${NC} ${GREEN}${BOLD}%-5s${NC} ${YELLOW}across${NC} ${GREEN}${BOLD}%-2s${NC} ${YELLOW}tests${NC}%20s ${CYAN}│${NC}\n" \
                "${avg_speedup}x" "$count" ""
            
            {
                echo "┌────────────────────────────────────────────────────────────────┐"
                printf "│ Average GPU Speedup: %-5s across %-2s tests                  │\n" "${avg_speedup}x" "$count"
            } >> ${BENCHMARK_RESULTS}
            
            # Performance interpretation
            if (( $(echo "$avg_speedup >= 2.0" | bc -l) )); then
                printf "${CYAN}│${NC} ${GREEN}Status: 🚀 GPU shows excellent performance improvement!${NC}%6s ${CYAN}│${NC}\n" ""
                echo "│ Status: 🚀 GPU shows excellent performance improvement!        │" >> ${BENCHMARK_RESULTS}
            elif (( $(echo "$avg_speedup >= 1.2" | bc -l) )); then
                printf "${CYAN}│${NC} ${GREEN}Status: ✓ GPU shows good performance improvement${NC}%10s ${CYAN}│${NC}\n" ""
                echo "│ Status: ✓ GPU shows good performance improvement              │" >> ${BENCHMARK_RESULTS}
            elif (( $(echo "$avg_speedup >= 1.0" | bc -l) )); then
                printf "${CYAN}│${NC} ${YELLOW}Status: ≈ GPU performance is similar to CPU${NC}%14s ${CYAN}│${NC}\n" ""
                echo "│ Status: ≈ GPU performance is similar to CPU                   │" >> ${BENCHMARK_RESULTS}
            else
                printf "${CYAN}│${NC} ${RED}Status: ⚠ CPU outperforms GPU on these workloads${NC}%8s ${CYAN}│${NC}\n" ""
                echo "│ Status: ⚠ CPU outperforms GPU on these workloads              │" >> ${BENCHMARK_RESULTS}
            fi
            
            printf "${CYAN}${summary_box_bot}${NC}\n"
            echo "└────────────────────────────────────────────────────────────────┘" >> ${BENCHMARK_RESULTS}
        fi
    fi
    
    echo ""
    echo "" >> ${BENCHMARK_RESULTS}
}

# Generate HTML report
generate_html_report() {
    print_info "Generating HTML report: ${HTML_REPORT}"
    
    local timestamp=$(date '+%B %d, %Y at %H:%M:%S')
    
    cat > "${HTML_REPORT}" << 'EOF_HTML'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PG-Strom CPU vs GPU Benchmark Results</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 20px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }
        
        .header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }
        
        .header h1 {
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.2);
        }
        
        .header .subtitle {
            font-size: 1.2em;
            opacity: 0.9;
        }
        
        .header .timestamp {
            margin-top: 15px;
            font-size: 0.9em;
            opacity: 0.8;
        }
        
        .content {
            padding: 40px;
        }
        
        .summary-cards {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 40px;
        }
        
        .card {
            background: linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%);
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }
        
        .card:hover {
            transform: translateY(-5px);
            box-shadow: 0 10px 25px rgba(0,0,0,0.15);
        }
        
        .card h3 {
            color: #667eea;
            font-size: 0.9em;
            text-transform: uppercase;
            margin-bottom: 10px;
            letter-spacing: 1px;
        }
        
        .card .value {
            font-size: 2em;
            font-weight: bold;
            color: #333;
        }
        
        .card .unit {
            font-size: 0.8em;
            color: #666;
            margin-left: 5px;
        }
        
        .section-title {
            font-size: 1.8em;
            color: #333;
            margin: 40px 0 20px 0;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }
        
        .table-container {
            overflow-x: auto;
            margin: 20px 0;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.1);
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
            background: white;
        }
        
        thead {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
        }
        
        th {
            padding: 18px;
            text-align: left;
            font-weight: 600;
            text-transform: uppercase;
            font-size: 0.85em;
            letter-spacing: 1px;
        }
        
        td {
            padding: 15px 18px;
            border-bottom: 1px solid #e0e0e0;
        }
        
        tbody tr:hover {
            background-color: #f8f9fa;
        }
        
        tbody tr:last-child td {
            border-bottom: none;
        }
        
        .dataset-header {
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            font-weight: bold;
            font-size: 1.1em;
            text-align: center;
        }
        
        .dataset-header td {
            padding: 15px;
            border: none;
        }
        
        .status-excellent {
            background: #10b981;
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            display: inline-block;
        }
        
        .status-good {
            background: #3b82f6;
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            display: inline-block;
        }
        
        .status-similar {
            background: #f59e0b;
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            display: inline-block;
        }
        
        .status-slower {
            background: #ef4444;
            color: white;
            padding: 6px 12px;
            border-radius: 20px;
            font-size: 0.85em;
            font-weight: 600;
            display: inline-block;
        }
        
        .cpu-time {
            color: #3b82f6;
            font-weight: 600;
        }
        
        .gpu-time {
            color: #10b981;
            font-weight: 600;
        }
        
        .speedup {
            color: #f59e0b;
            font-weight: 700;
            font-size: 1.1em;
        }
        
        .chart-container {
            margin: 30px 0;
            padding: 20px;
            background: #f8f9fa;
            border-radius: 10px;
        }
        
        .bar-chart {
            margin: 20px 0;
        }
        
        .bar-item {
            margin: 15px 0;
        }
        
        .bar-label {
            display: flex;
            justify-content: space-between;
            margin-bottom: 5px;
            font-size: 0.9em;
            color: #666;
        }
        
        .bar-container {
            background: #e0e0e0;
            border-radius: 10px;
            height: 30px;
            overflow: hidden;
            position: relative;
        }
        
        .bar-fill {
            height: 100%;
            background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: flex-end;
            padding-right: 10px;
            color: white;
            font-weight: bold;
            transition: width 1s ease;
        }
        
        .footer {
            background: #f8f9fa;
            padding: 30px;
            text-align: center;
            color: #666;
            border-top: 1px solid #e0e0e0;
        }
        
        .footer a {
            color: #667eea;
            text-decoration: none;
            font-weight: 600;
        }
        
        .footer a:hover {
            text-decoration: underline;
        }
        
        @media (max-width: 768px) {
            .header h1 {
                font-size: 1.8em;
            }
            
            .summary-cards {
                grid-template-columns: 1fr;
            }
            
            table {
                font-size: 0.85em;
            }
            
            th, td {
                padding: 10px;
            }
        }
        
        .icon {
            margin-right: 8px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 PG-Strom Benchmark Results</h1>
            <div class="subtitle">CPU vs GPU Performance Comparison</div>
            <div class="timestamp">EOF_HTML
    
    echo "Generated on ${timestamp}" >> "${HTML_REPORT}"
    
    cat >> "${HTML_REPORT}" << 'EOF_HTML'
</div>
        </div>
        
        <div class="content">
            <div class="summary-cards">
EOF_HTML
    
    # Calculate summary statistics
    if [ ${#SPEEDUPS[@]} -gt 0 ]; then
        local total_speedup=0
        local count=0
        local max_speedup=0
        local tests_run=0
        
        for speedup in "${SPEEDUPS[@]}"; do
            if [[ "$speedup" != "-" ]]; then
                total_speedup=$(echo "$total_speedup + $speedup" | bc)
                count=$((count + 1))
                if (( $(echo "$speedup > $max_speedup" | bc -l) )); then
                    max_speedup=$speedup
                fi
            fi
        done
        
        # Count total tests (including separator rows)
        tests_run=${#TEST_NAMES[@]}
        
        if [ $count -gt 0 ]; then
            local avg_speedup=$(echo "scale=2; $total_speedup / $count" | bc)
            
            cat >> "${HTML_REPORT}" << EOF
                <div class="card">
                    <h3>Average Speedup</h3>
                    <div class="value">${avg_speedup}<span class="unit">x</span></div>
                </div>
                <div class="card">
                    <h3>Max Speedup</h3>
                    <div class="value">${max_speedup}<span class="unit">x</span></div>
                </div>
                <div class="card">
                    <h3>Tests Run</h3>
                    <div class="value">${count}</div>
                </div>
                <div class="card">
                    <h3>Datasets</h3>
                    <div class="value">$(echo "$RUN_SIZES" | wc -w)</div>
                </div>
EOF
        fi
    fi
    
    cat >> "${HTML_REPORT}" << 'EOF_HTML'
            </div>
            
            <h2 class="section-title">📊 Detailed Results</h2>
            
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Test Name</th>
                            <th>CPU Time (s)</th>
                            <th>GPU Time (s)</th>
                            <th>CPU Usage</th>
                            <th>GPU Usage</th>
                            <th>Speedup</th>
                            <th>Status</th>
                        </tr>
                    </thead>
                    <tbody>
EOF_HTML
    
    # Generate table rows
    for i in "${!TEST_NAMES[@]}"; do
        local test_name="${TEST_NAMES[$i]}"
        local cpu_time="${CPU_TIMES[$i]}"
        local gpu_time="${GPU_TIMES[$i]}"
        local speedup="${SPEEDUPS[$i]}"
        # Check if this is a separator row (dataset header)
        if [[ "$test_name" == "==="* ]]; then
            cat >> "${HTML_REPORT}" << EOF
                            <tr class="dataset-header">
                                <td colspan="9">${test_name}</td>
                            </tr>
EOF
            continue
        fi
        # Determine status class
        local status_class=""
        local status_text=""
        local status_icon=""
        if [[ "$speedup" == "-" ]]; then
            status_class="status-similar"
            status_text="N/A"
            status_icon=""
        elif (( $(echo "$speedup >= 2.0" | bc -l) )); then
            status_class="status-excellent"
            status_text="Excellent"
            status_icon="🚀"
        elif (( $(echo "$speedup >= 1.2" | bc -l) )); then
            status_class="status-good"
            status_text="Good"
            status_icon="✓"
        elif (( $(echo "$speedup >= 1.0" | bc -l) )); then
            status_class="status-similar"
            status_text="Similar"
            status_icon="≈"
        else
            status_class="status-slower"
            status_text="Slower"
            status_icon="⚠"
        fi
        local speedup_display
        if [[ "$speedup" != "-" ]]; then
            speedup_display="${speedup}x"
        else
            speedup_display="-"
        fi
        # Compute average CPU/GPU usage for this test
        local cpu_usages="${CPU_USAGE_MAP[$test_name]}"
        local gpu_usages="${GPU_USAGE_MAP[$test_name]}"
        local cpu_sum=0
        local cpu_count=0
        for val in $cpu_usages; do cpu_sum=$(echo "$cpu_sum + $val" | bc); cpu_count=$((cpu_count+1)); done
        local avg_cpu_usage="-"
        if [ $cpu_count -gt 0 ]; then avg_cpu_usage=$(echo "scale=1; $cpu_sum / $cpu_count" | bc); fi
        local gpu_sum=0
        local gpu_count=0
        for val in $gpu_usages; do gpu_sum=$(echo "$gpu_sum + $val" | bc); gpu_count=$((gpu_count+1)); done
        local avg_gpu_usage="-"
        if [ $gpu_count -gt 0 ]; then avg_gpu_usage=$(echo "scale=1; $gpu_sum / $gpu_count" | bc); fi
        # Usage indicators
        local cpu_usage_icon="🔥"
        local gpu_usage_icon="⚡"
        cat >> "${HTML_REPORT}" << EOF
                        <tr>
                            <td>${test_name}</td>
                            <td class="cpu-time">${cpu_time}</td>
                            <td class="gpu-time">${gpu_time}</td>
                            <td style="color:#3b82f6;font-weight:600;">${cpu_usage_icon} ${avg_cpu_usage}%</td>
                            <td style="color:#10b981;font-weight:600;">${gpu_usage_icon} ${avg_gpu_usage}%</td>
                            <td class="speedup">${speedup_display}</td>
                            <td><span class="${status_class}"><span class="icon">${status_icon}</span>${status_text}</span></td>
                        </tr>
EOF
    done
    
    cat >> "${HTML_REPORT}" << 'EOF_HTML'
                    </tbody>
                </table>
            </div>
            <div style="margin:20px 0 40px 0;">
                <strong>Legend:</strong>
                <span style="margin-left:20px;color:#3b82f6;font-weight:600;">🔥 High CPU Usage</span>
                <span style="margin-left:20px;color:#10b981;font-weight:600;">⚡ High GPU Usage</span>
                <span style="margin-left:20px;color:#888;">Low = minimal utilization for that mode</span>
            </div>
            
            <h2 class="section-title">📈 Performance Visualization</h2>
            
            <div class="chart-container">
                <h3 style="margin-bottom: 20px; color: #333;">Speedup Comparison</h3>
                <div class="bar-chart">
EOF_HTML
    
    # Generate bar chart
    for i in "${!TEST_NAMES[@]}"; do
        local test_name="${TEST_NAMES[$i]}"
        local speedup="${SPEEDUPS[$i]}"
        
        # Skip separator rows
        if [[ "$test_name" == "==="* ]] || [[ "$speedup" == "-" ]]; then
            continue
        fi
        
        # Calculate bar width (max speedup of 10x = 100%)
        local bar_width=$(echo "scale=2; ($speedup / 10) * 100" | bc)
        if (( $(echo "$bar_width > 100" | bc -l) )); then
            bar_width=100
        fi
        
        cat >> "${HTML_REPORT}" << EOF
                    <div class="bar-item">
                        <div class="bar-label">
                            <span>${test_name}</span>
                            <span>${speedup}x</span>
                        </div>
                        <div class="bar-container">
                            <div class="bar-fill" style="width: ${bar_width}%">${speedup}x</div>
                        </div>
                    </div>
EOF
    done
    
    cat >> "${HTML_REPORT}" << 'EOF_HTML'
                </div>
            </div>
        </div>
        
        <div class="footer">
            <p>Generated by PG-Strom Benchmark Script</p>
            <p style="margin-top: 10px;">
                <a href="https://github.com/heterodb/pg-strom" target="_blank">PG-Strom Project</a>
            </p>
        </div>
    </div>
</body>
</html>
EOF_HTML
    
    print_success "HTML report generated: ${HTML_REPORT}"
    print_info "Open in browser: file://$(pwd)/${HTML_REPORT}"
}

# Generate summary report
generate_summary() {
    print_info "=========================================="
    print_info "Benchmark Summary"
    print_info "=========================================="
    
    # Get GPU device info
    print_info "GPU Information:"
    sudo -u ${DB_USER} psql -p ${DB_PORT} -d ${DB_NAME} -c "SHOW shared_preload_libraries;" >> ${BENCHMARK_RESULTS} 2>&1
    
    # Get PG-Strom settings
    print_info "PG-Strom Configuration:"
    sudo -u ${DB_USER} psql -p ${DB_PORT} -d ${DB_NAME} -c "
    SELECT name, setting, unit
    FROM pg_settings 
    WHERE name LIKE 'pg_strom%' OR name IN ('work_mem', 'shared_buffers', 'max_worker_processes');" >> ${BENCHMARK_RESULTS} 2>&1
    
    print_info "Full benchmark results saved to: ${BENCHMARK_RESULTS}"
    print_info "Query audit log saved to: ${QUERY_AUDIT_LOG}"
    if [[ -n "$GPU_TOP_LOG" ]]; then
        print_info "GPU monitor snapshots: ${GPU_TOP_LOG}"
    fi
    print_success "Benchmark complete!"
}

# Precheck: PG-Strom enabled, GPU detected, supported query
precheck_pgstrom() {
    print_info "[Precheck] Checking PG-Strom status..."
    local version=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -t -c \"SELECT extversion FROM pg_extension WHERE extname = 'pg_strom';\"" | xargs)
    if [[ -z "$version" ]]; then
        print_error "PG-Strom extension not found in PostgreSQL!"
        exit 1
    fi
    local enabled=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -t -c \"SHOW pg_strom.enabled;\"" | xargs)
    if [[ "$enabled" != "on" ]]; then
        print_error "PG-Strom is not enabled in PostgreSQL!"
        exit 1
    fi
    print_success "PG-Strom version: $version, enabled: $enabled"
}

precheck_gpu() {
    print_info "[Precheck] Checking GPU availability..."
    if ! command -v nvidia-smi &>/dev/null; then
        print_error "nvidia-smi not found. NVIDIA GPU not detected!"
        exit 1
    fi
    local gpu_count=$(nvidia-smi --query-gpu=name --format=csv,noheader | wc -l)
    if (( gpu_count < 1 )); then
        print_error "No NVIDIA GPU detected!"
        exit 1
    fi
    print_success "NVIDIA GPU(s) detected: $gpu_count"
}

precheck_supported_query() {
    print_info "[Precheck] Checking for supported query types..."
    local test_query="SELECT COUNT(*) FROM pg_class WHERE relkind = 'r';"
    local explain=$(sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -t -c \"EXPLAIN $test_query\"" | grep -i 'GpuScan')
    if [[ -z "$explain" ]]; then
        print_warn "Test query does not trigger GPU acceleration (GpuScan not found)."
    else
        print_success "Test query triggers GPU acceleration."
    fi
}

run_prechecks() {
    precheck_pgstrom
    precheck_gpu
    precheck_supported_query
}

# Ensure compatibility helper functions expected by PG-Strom exist
ensure_pgstrom_helper_funcs() {
    print_info "[Precheck] Ensuring PG-Strom helper functions exist (nrows)"
    sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -v ON_ERROR_STOP=1 -c \"
    DO $$
    BEGIN
        -- bigint variant
        IF NOT EXISTS (
            SELECT 1 FROM pg_proc
            WHERE proname = 'nrows'
              AND oid = 'nrows(bigint)'::regprocedure
        ) THEN
            EXECUTE $$CREATE OR REPLACE FUNCTION public.nrows(bigint)
                     RETURNS text
                     LANGUAGE sql
                     IMMUTABLE PARALLEL SAFE
                     AS $$SELECT to_char(COALESCE($1,0), 'FM999,999,999,999')$$;$$;
        END IF;

        -- numeric variant (in case extension looks this up)
        IF NOT EXISTS (
            SELECT 1 FROM pg_proc
            WHERE proname = 'nrows'
              AND oid = 'nrows(numeric)'::regprocedure
        ) THEN
            EXECUTE $$CREATE OR REPLACE FUNCTION public.nrows(numeric)
                     RETURNS text
                     LANGUAGE sql
                     IMMUTABLE PARALLEL SAFE
                     AS $$SELECT to_char(COALESCE($1,0)::numeric, 'FM999,999,999,999')$$;$$;
        END IF;

        -- int variant convenience
        IF NOT EXISTS (
            SELECT 1 FROM pg_proc
            WHERE proname = 'nrows'
              AND oid = 'nrows(integer)'::regprocedure
        ) THEN
            EXECUTE $$CREATE OR REPLACE FUNCTION public.nrows(integer)
                     RETURNS text
                     LANGUAGE sql
                     IMMUTABLE PARALLEL SAFE
                     AS $$SELECT to_char(COALESCE($1,0)::bigint, 'FM999,999,999,999')$$;$$;
        END IF;
    END$$;\"" >> ${BENCHMARK_RESULTS} 2>&1 || true
}

# Main execution
main() {
    # Check if we're in cleanup-only mode
    if [ "$CLEANUP_ONLY" = true ]; then
        print_info "=========================================="
        print_info "Cleanup Mode - Removing Test Tables"
        print_info "=========================================="
        print_info "Database: ${DB_NAME}"
        print_info "Port: ${DB_PORT}"
        print_info "=========================================="
        echo ""
        
        # Check if database is accessible
        if ! sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -c 'SELECT 1;'" > /dev/null 2>&1; then
            print_error "Cannot connect to database ${DB_NAME} on port ${DB_PORT}"
            print_info "Make sure PostgreSQL is running"
            exit 1
        fi
        
        cleanup_test_tables
        exit $?
    fi
    
    print_info "=========================================="
    print_info "PG-Strom CPU vs GPU Benchmark"
    print_info "=========================================="
    print_info "Database: ${DB_NAME}"
    print_info "Port: ${DB_PORT}"
    print_info "Results file: ${BENCHMARK_RESULTS}"
    print_info "Dataset sizes: ${RUN_SIZES}"
    print_info "=========================================="
    echo ""
    
    # Check if database is accessible
    if ! sudo -u ${DB_USER} bash -c "cd /tmp && psql -p ${DB_PORT} -d ${DB_NAME} -c 'SELECT 1;'" > /dev/null 2>&1; then
        print_error "Cannot connect to database ${DB_NAME} on port ${DB_PORT}"
        print_info "Make sure PostgreSQL is running and PG-Strom is loaded"
        exit 1
    fi
    
    # Ensure required helper functions are present (workaround for minimal PG-Strom SQL install)
    ensure_pgstrom_helper_funcs

    # Setup database
    if ! setup_database; then
        print_error "Database setup failed. Aborting benchmark."
        exit 1
    fi
    
    # Run prechecks
    run_prechecks

    # Optional: start GPU monitor in background
    start_gpu_monitor
    
    # Run benchmarks for configured data sizes
    for size in $RUN_SIZES; do
        case $size in
            SMALL)
                row_count=$SMALL_SIZE
                suffix="small"
                ;;
            MEDIUM)
                row_count=$MEDIUM_SIZE
                suffix="medium"
                ;;
            LARGE)
                row_count=$LARGE_SIZE
                suffix="large"
                ;;
            XLARGE)
                row_count=$XLARGE_SIZE
                suffix="xlarge"
                ;;
            XXLARGE)
                row_count=$XXLARGE_SIZE
                suffix="xxlarge"
                ;;
            *)
                print_warn "Unknown size: $size. Skipping..."
                continue
                ;;
        esac
        
        print_info ""
        print_info "=========================================="
        print_info "Testing with $size dataset ($row_count rows)"
        print_info "=========================================="
        
        # Warn for very large datasets
        if [[ "$size" == "XLARGE" ]]; then
            print_warn "⚠️  XLARGE dataset (100M rows) will take significant time and disk space!"
            print_info "Estimated time: 5-15 minutes per dataset"
        elif [[ "$size" == "XXLARGE" ]]; then
            print_warn "⚠️  XXLARGE dataset (1B rows) will take VERY long time and significant disk space!"
            print_info "Estimated time: 1-3 hours per dataset"
            print_info "Required disk space: ~100GB+"
        fi
        
        if ! create_test_tables $row_count $suffix; then
            print_error "Failed to create test tables for $size dataset. Skipping..."
            continue
        fi
        
        if ! benchmark_select $suffix $row_count; then
            print_error "Failed to benchmark $size dataset. Continuing to next dataset..."
            continue
        fi
        
        echo "" | tee -a ${BENCHMARK_RESULTS}
    done
    
    # Generate side-by-side comparison table
    generate_comparison_table
    
    # Generate HTML report
    generate_html_report
    
    # Generate summary
    generate_summary
    
    print_info ""
    print_success "Benchmark completed successfully!"
    print_info "Results saved to: ${BENCHMARK_RESULTS}"
    print_info "HTML report saved to: ${HTML_REPORT}"
    print_info "Query audit log saved to: ${QUERY_AUDIT_LOG}"
    print_info ""
    
    # Cleanup handling
    if [ "$SKIP_CLEANUP" = true ]; then
        print_info "Skipping cleanup (--skip-cleanup flag set)"
        print_info "Test tables kept in database."
        print_info "To manually clean up later, run:"
        print_info "  ./benchmark_pgstrom.sh --cleanup"
    else
        # Ask if user wants to clean up tables
        print_info "Do you want to clean up test tables? (y/N)"
        read -t 10 -n 1 cleanup_choice
        echo ""
        
        if [[ "$cleanup_choice" =~ ^[Yy]$ ]]; then
            cleanup_test_tables
        else
            print_info "Test tables kept in database."
            print_info "To manually clean up later, run:"
            print_info "  ./benchmark_pgstrom.sh --cleanup"
        fi
    fi
    
    # Ensure data directory and contents are owned by the invoking user (not root) and have proper permissions
    OWNER_USER="${SUDO_USER:-$USER}"
    sudo chown -R "$OWNER_USER":"$OWNER_USER" "$DATA_DIR" 2>/dev/null || true
    chmod -R 770 "$DATA_DIR" 2>/dev/null || true

    # Stop GPU monitor if running
    stop_gpu_monitor
}

# Run main function
run_prechecks
main "$@"
