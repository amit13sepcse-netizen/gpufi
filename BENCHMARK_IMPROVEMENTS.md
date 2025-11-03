# Benchmark Script Improvements

## Summary of Changes

The `benchmark_pgstrom.sh` script has been enhanced with robust error handling, automatic cleanup, and improved reliability.

## Key Improvements

### 1. **Automatic Cleanup Before Execution**
- Drops all existing test tables before starting benchmark
- Prevents conflicts with previous runs
- Ensures clean state for accurate measurements
- Tables cleaned up:
  - `bench_numeric_small`, `bench_numeric_medium`, `bench_numeric_large`
  - `bench_mixed_small`, `bench_mixed_medium`, `bench_mixed_large`
  - Legacy tables: `bench_numeric`, `bench_mixed`, `bench_aggregation`
  - Join tables: `bench_join_a`, `bench_join_b`

### 2. **Comprehensive Error Handling**
- All database operations check for failures
- Proper exit codes returned from functions
- Failed operations logged with detailed error messages
- Graceful degradation: continues with next dataset if one fails
- Critical failures halt execution with clear error messages

**Functions with error handling:**
- `execute_timed_sql()` - Returns 0 on success, 1 on failure
- `setup_database()` - Validates table drops
- `create_test_tables()` - Checks each CREATE/INSERT operation
- `benchmark_select()` - Validates completion
- `cleanup_test_tables()` - Dedicated cleanup function

### 3. **Flexible Cleanup Options**

#### Command-line Flags
```bash
# Show help
./benchmark_pgstrom.sh --help

# Run benchmark, skip cleanup prompt (keep tables)
sudo ./benchmark_pgstrom.sh --skip-cleanup

# Cleanup existing tables only (no benchmark)
sudo ./benchmark_pgstrom.sh --cleanup
```

#### Interactive Cleanup
- After benchmark completes, prompts: "Do you want to clean up test tables? (y/N)"
- 10-second timeout (defaults to No)
- Press 'y' or 'Y' to clean up immediately
- Any other key or timeout keeps tables

### 4. **Improved Reliability**
- `execute_timed_sql()` captures exit codes properly
- Table creation validates each step before proceeding
- Index creation and ANALYZE operations checked
- Benchmark continues even if one dataset fails

## Usage Examples

### Basic Usage
```bash
# Normal run (will prompt for cleanup at end)
sudo ./benchmark_pgstrom.sh
```

### Skip Cleanup Prompt
```bash
# Keep all tables after benchmark
sudo ./benchmark_pgstrom.sh --skip-cleanup
```

### Cleanup Only
```bash
# Remove test tables without running benchmark
sudo ./benchmark_pgstrom.sh --cleanup
```

### Custom Database Configuration
```bash
# Use different database/port
DB_NAME=mydb DB_PORT=5432 sudo ./benchmark_pgstrom.sh

# Combine with skip-cleanup
DB_NAME=mydb DB_PORT=5432 sudo ./benchmark_pgstrom.sh --skip-cleanup
```

## Error Recovery

### If Benchmark Fails
The script now handles errors gracefully:
- Database connection errors: Exits with clear message
- Table creation errors: Skips that dataset, continues with next
- Benchmark query errors: Logs error, continues
- Cleanup errors: Reports failure but doesn't crash

### Manual Cleanup
If you need to clean up manually:
```bash
# Option 1: Use cleanup mode
sudo ./benchmark_pgstrom.sh --cleanup

# Option 2: Direct SQL
sudo -u postgres psql -p 5434 -d pgstrom_test -c "
DROP TABLE IF EXISTS bench_numeric_small, bench_numeric_medium, 
                     bench_mixed_small, bench_mixed_medium CASCADE;
"
```

## Workflow Changes

### Before (Old Behavior)
1. Run benchmark
2. Tables might conflict with previous run
3. No cleanup prompt
4. Manual cleanup required
5. Errors might crash script

### After (New Behavior)
1. Run benchmark
2. **Automatically drops old tables first**
3. Creates fresh tables
4. Runs benchmarks with error handling
5. **Interactive cleanup prompt** (optional)
6. **Graceful error handling** throughout
7. Clear error messages and recovery suggestions

## Technical Details

### Error Handling Pattern
```bash
if ! execute_timed_sql "Create table" "CREATE TABLE ..."; then
    print_error "Failed to create table"
    return 1
fi
```

### Cleanup Function
```bash
cleanup_test_tables() {
    # Drops all test tables
    # Returns 0 on success, 1 on failure
    # Logs detailed errors
}
```

### Command-line Parsing
```bash
while [[ $# -gt 0 ]]; do
    case $1 in
        --cleanup) CLEANUP_ONLY=true ;;
        --skip-cleanup) SKIP_CLEANUP=true ;;
        --help|-h) show_help; exit 0 ;;
    esac
done
```

## Benefits

✅ **Reliability**: Comprehensive error checking prevents silent failures  
✅ **Cleanliness**: Automatic cleanup before/after ensures no table conflicts  
✅ **Flexibility**: Command-line options for different workflows  
✅ **User-friendly**: Clear prompts and helpful error messages  
✅ **Robustness**: Continues operation even if non-critical steps fail  
✅ **Debugging**: Detailed error logging for troubleshooting  

## Migration Notes

Existing workflows will continue to work, but you now have:
- Automatic cleanup before execution (no manual intervention needed)
- Optional cleanup prompt at the end
- Better error messages if something goes wrong

No changes required to your existing commands!
