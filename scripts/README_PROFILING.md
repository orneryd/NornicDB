# Write Performance Profiling Scripts

This directory contains scripts for profiling NornicDB write performance under various conditions.

## Quick Start

### 1. Quick Test (5 queries)
```bash
./scripts/quick_write_test.sh
```

This runs 5 basic write operations and shows timing for each.

### 2. Full Profiling Suite
```bash
./scripts/profile_write_performance.sh
```

This runs comprehensive tests:
- Single node creation (100 iterations)
- Batch node creation (10 nodes per query)
- MERGE operations (upsert)
- Relationship creation
- Property updates
- Concurrent writes
- Large payloads

Results are saved to `profiling_results_<timestamp>/` directory.

### 3. Real-time File Monitoring
```bash
# In one terminal, monitor file writes:
./scripts/monitor_writes.sh

# In another terminal, run your tests:
./scripts/quick_write_test.sh
```

## Configuration

Set environment variables to customize behavior:

```bash
# HTTP port (default: 7474)
export HTTP_PORT=7474

# Database name (default: nornic)
export DB_NAME=nornic

# Data directory to monitor (default: ./data/test)
export DATA_DIR=./data/test

# Number of iterations per test (default: 100)
export ITERATIONS=100

# Number of concurrent workers (default: 10)
export CONCURRENT=10
```

## Example: Profiling with Custom Settings

```bash
# Run 500 iterations with 20 concurrent workers
ITERATIONS=500 CONCURRENT=20 ./scripts/profile_write_performance.sh
```

## Monitoring File Writes

The scripts monitor file system activity in the data directory:

- **SST files** (`.sst`) - BadgerDB sorted string tables
- **VLOG files** (`.vlog`) - BadgerDB value log
- **MEM files** (`.mem`) - In-memory tables
- **MANIFEST** - BadgerDB manifest
- **WAL files** - Write-ahead log segments

Watch for:
- File size growth during writes
- New file creation
- File modification timestamps

## Understanding Results

### Latency Metrics
- **Min**: Fastest operation
- **Max**: Slowest operation
- **Avg**: Average latency
- **Median**: 50th percentile
- **P95**: 95th percentile (most operations are faster)
- **P99**: 99th percentile (99% of operations are faster)

### Throughput
Operations per second (ops/sec) = total operations / total time

### File System Activity
Check `profiling_results_<timestamp>/filesystem_activity.log` for:
- File write patterns
- I/O frequency
- Disk sync operations

## Troubleshooting

### Server not running
```
Error: NornicDB server is not running on port 7474
```
Make sure NornicDB is running: `./bin/nornicdb serve`

### Wrong database name
If you get errors about database not found, check your default database:
```bash
# Check config
grep default_database ~/.nornicdb/config.yaml

# Or use environment variable
DB_NAME=your_db_name ./scripts/profile_write_performance.sh
```

### Permission errors
Make sure the data directory is writable:
```bash
chmod -R u+w ./data/test
```

## Advanced: Using with Process Monitoring

Monitor the NornicDB process while profiling:

```bash
# Terminal 1: Monitor process
watch -n 0.5 'ps aux | grep nornicdb | grep -v grep'

# Terminal 2: Monitor file writes
./scripts/monitor_writes.sh

# Terminal 3: Run profiling
./scripts/profile_write_performance.sh
```

## Analyzing Results

After running the full profiling suite:

1. **Check latency distribution**: Look at P95 and P99 values
2. **Compare test types**: Single vs batch vs concurrent
3. **File system impact**: Check filesystem_activity.log for I/O patterns
4. **Throughput analysis**: Compare ops/sec across different test types

Example analysis:
```bash
# View results
cat profiling_results_*/test1_single_create.log

# Check file sizes before/after
ls -lh data/test/*.sst data/test/*.vlog
```
