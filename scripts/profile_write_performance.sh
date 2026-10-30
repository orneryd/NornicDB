#!/bin/bash
# Write Performance Profiling Script for NornicDB
# Tests write latency under various conditions

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BOLT_PORT=${BOLT_PORT:-7687}
HTTP_PORT=${HTTP_PORT:-7474}
DATA_DIR=${DATA_DIR:-./data/test}
ITERATIONS=${ITERATIONS:-100}
CONCURRENT=${CONCURRENT:-10}

# Check if server is running
check_server() {
    if ! curl -s "http://localhost:${HTTP_PORT}/health" > /dev/null 2>&1; then
        echo -e "${RED}Error: NornicDB server is not running on port ${HTTP_PORT}${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Server is running${NC}"
}

# Send Cypher query via HTTP API (Neo4j-compatible endpoint)
send_query() {
    local query="$1"
    local label="$2"
    local db_name=${3:-nornic}  # Default database name
    
    # Escape quotes in query for JSON
    local escaped_query=$(echo "$query" | sed 's/"/\\"/g')
    
    # Measure latency: time from request start to response received
    # This includes: network round-trip + server processing + database write
    # Comparable to Neo4j benchmarks which measure end-to-end latency
    local start_time=$(date +%s.%N)
    local response=$(curl -s --max-time 30 -X POST "http://localhost:${HTTP_PORT}/db/${db_name}/tx/commit" \
        -H "Content-Type: application/json" \
        -d "{\"statements\": [{\"statement\": \"$escaped_query\"}]}" 2>&1)
    local end_time=$(date +%s.%N)
    
    # Calculate duration in seconds (with microsecond precision)
    # Use bc if available for precision, otherwise awk
    local duration
    if command -v bc >/dev/null 2>&1; then
        duration=$(echo "$end_time - $start_time" | bc -l)
    else
        duration=$(awk "BEGIN {printf \"%.9f\", $end_time - $start_time}")
    fi
    
    # Check for actual errors in JSON (non-empty errors array or error code)
    if echo "$response" | grep -q '"errors":\s*\[[^]]' || (echo "$response" | grep -q '"code"' && ! echo "$response" | grep -q '"errors":\s*\[\]'); then
        echo -e "${RED}✗ ${label}: ERROR${NC}" >&2
        echo "  Response: $response" | head -3 >&2
        return 1
    else
        # Output to stderr for display, stdout for data collection (numeric only)
        echo -e "${GREEN}✓ ${label}: ${duration}s${NC}" >&2
        echo "$duration"  # This goes to stdout for capture
        return 0
    fi
}

# Monitor file system activity (simplified - just log file sizes periodically)
monitor_files() {
    local duration=$1
    local output_file="$2"
    
    echo -e "${BLUE}Monitoring file writes in ${DATA_DIR}...${NC}" >&2
    
    # Simple monitoring: log file sizes every second
    (
        local end_time=$(($(date +%s) + duration))
        while [ $(date +%s) -lt $end_time ]; do
            echo "$(date +%H:%M:%S) $(du -sh ${DATA_DIR} 2>/dev/null | cut -f1)" >> "$output_file" 2>&1
            sleep 1
        done
    ) &
    MONITOR_PID=$!
    
    echo $MONITOR_PID
}

# Test 1: Single node creation
test_single_create() {
    echo -e "\n${YELLOW}=== Test 1: Single Node Creation ===${NC}"
    
    local latencies=()
    for i in $(seq 1 $ITERATIONS); do
        local query="CREATE (n:TestNode {id: $i, timestamp: timestamp()}) RETURN n.id"
        # Capture both stdout and stderr, extract numeric latency from last line
        local output=$(send_query "$query" "Create node $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        # Verify it's a valid number (format: 0.012700000 or .012700000)
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 2: Batch node creation
test_batch_create() {
    echo -e "\n${YELLOW}=== Test 2: Batch Node Creation (10 nodes per query) ===${NC}"
    
    local latencies=()
    local batch_size=10
    for i in $(seq 1 $((ITERATIONS / batch_size))); do
        local start=$((i * batch_size - batch_size + 1))
        local end=$((i * batch_size))
        local query="CREATE "
        for j in $(seq $start $end); do
            if [ $j -gt $start ]; then
                query+=", "
            fi
            query+="(n$j:TestNode {id: $j, batch: $i})"
        done
        query+=" RETURN count(*) as created"
        
        local output=$(send_query "$query" "Batch create $start-$end" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 3: MERGE operations (upsert)
test_merge() {
    echo -e "\n${YELLOW}=== Test 3: MERGE Operations (Upsert) ===${NC}"
    
    local latencies=()
    for i in $(seq 1 $ITERATIONS); do
        local query="MERGE (n:TestNode {id: $i}) ON CREATE SET n.created = timestamp() ON MATCH SET n.updated = timestamp() RETURN n.id"
        local output=$(send_query "$query" "MERGE node $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 4: Relationship creation
test_relationship_create() {
    echo -e "\n${YELLOW}=== Test 4: Relationship Creation ===${NC}"
    
    # First create some nodes
    send_query "MATCH (n:TestNode) DELETE n" "Cleanup" > /dev/null
    send_query "CREATE (n1:TestNode {id: 1}), (n2:TestNode {id: 2})" "Setup nodes" > /dev/null
    
    local latencies=()
    for i in $(seq 1 $ITERATIONS); do
        local query="MATCH (a:TestNode {id: 1}), (b:TestNode {id: 2}) CREATE (a)-[r:RELATES {id: $i, timestamp: timestamp()}]->(b) RETURN r.id"
        local output=$(send_query "$query" "Create relationship $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        # Verify it's a valid number (format: 0.012700000 or .012700000)
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 5: Property updates (SET)
test_property_update() {
    echo -e "\n${YELLOW}=== Test 5: Property Updates (SET) ===${NC}"
    
    local latencies=()
    for i in $(seq 1 $ITERATIONS); do
        local query="MATCH (n:TestNode {id: 1}) SET n.value = $i, n.updated = timestamp() RETURN n.value"
        local output=$(send_query "$query" "Update property $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 6: Concurrent writes
test_concurrent_writes() {
    echo -e "\n${YELLOW}=== Test 6: Concurrent Writes ($CONCURRENT concurrent) ===${NC}"
    
    local start_time=$(date +%s.%N)
    
    for i in $(seq 1 $CONCURRENT); do
        (
            for j in $(seq 1 $((ITERATIONS / CONCURRENT))); do
                local id=$((i * 1000 + j))
                local query="CREATE (n:ConcurrentTest {id: $id, worker: $i, iteration: $j}) RETURN n.id"
                send_query "$query" "Worker $i, iter $j" > /dev/null 2>&1
            done
        ) &
    done
    
    wait
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)
    local total_ops=$((CONCURRENT * (ITERATIONS / CONCURRENT)))
    local throughput=$(echo "scale=2; $total_ops / $total_duration" | bc)
    
    echo -e "${GREEN}Total time: ${total_duration}s${NC}"
    echo -e "${GREEN}Total operations: ${total_ops}${NC}"
    echo -e "${GREEN}Throughput: ${throughput} ops/sec${NC}"
}

# Test 7: Large property payloads
test_large_payloads() {
    echo -e "\n${YELLOW}=== Test 7: Large Property Payloads ===${NC}"
    
    local latencies=()
    for i in $(seq 1 10); do
        # Use Cypher REPEAT function to create large string (1000 chars)
        # Use double quotes in REPEAT - they'll be escaped by send_query
        local query="CREATE (n:LargePayload {id: $i, data: REPEAT(\"x\", 1000)}) RETURN n.id"
        # Capture both stdout and stderr, extract numeric latency from last line
        local output=$(send_query "$query" "Large payload $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        # Verify it's a valid number (format: 0.012700000 or .012700000)
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Calculate statistics
calculate_stats() {
    local latencies=("$@")
    local count=${#latencies[@]}
    
    if [ $count -eq 0 ]; then
        echo -e "${RED}No successful operations${NC}"
        return
    fi
    
    # Sort latencies
    IFS=$'\n' sorted=($(sort -n <<<"${latencies[*]}"))
    unset IFS
    
    # Calculate stats
    local sum=0
    for lat in "${latencies[@]}"; do
        sum=$(echo "$sum + $lat" | bc)
    done
    
    local avg=$(echo "scale=4; $sum / $count" | bc)
    local min=${sorted[0]}
    local max=${sorted[$((count-1))]}
    
    # Median
    local median_idx=$((count / 2))
    local median=${sorted[$median_idx]}
    
    # P95
    local p95_idx=$((count * 95 / 100))
    local p95=${sorted[$p95_idx]}
    
    # P99
    local p99_idx=$((count * 99 / 100))
    local p99=${sorted[$p99_idx]}
    
    echo -e "${GREEN}Statistics:${NC}"
    echo "  Count:    $count"
    
    # Convert to milliseconds for easier comparison with Neo4j benchmarks
    local min_ms=$(echo "scale=3; $min * 1000" | bc)
    local max_ms=$(echo "scale=3; $max * 1000" | bc)
    local avg_ms=$(echo "scale=3; $avg * 1000" | bc)
    local median_ms=$(echo "scale=3; $median * 1000" | bc)
    local p95_ms=$(echo "scale=3; $p95 * 1000" | bc)
    local p99_ms=$(echo "scale=3; $p99 * 1000" | bc)
    
    echo "  Min:       ${min}s (${min_ms}ms)"
    echo "  Max:       ${max}s (${max_ms}ms)"
    echo "  Avg:       ${avg}s (${avg_ms}ms)"
    echo "  Median:    ${median}s (${median_ms}ms)"
    echo "  P95:       ${p95}s (${p95_ms}ms)"
    echo "  P99:       ${p99}s (${p99_ms}ms)"
    
    # Throughput
    local total_time=$(echo "$sum" | bc)
    local throughput=$(echo "scale=2; $count / $total_time" | bc)
    echo "  Throughput: ${throughput} ops/sec"
    
    # Comparison with Neo4j (labeled nodes: ~1.67ms = 600 ops/sec)
    local neo4j_labeled_ms=1.67
    local speedup=$(echo "scale=2; $neo4j_labeled_ms / $avg_ms" | bc)
    if (( $(echo "$avg_ms < $neo4j_labeled_ms" | bc -l) )); then
        echo -e "  ${GREEN}vs Neo4j: ${speedup}x faster${NC} (Neo4j: ~${neo4j_labeled_ms}ms for labeled nodes)"
    else
        local slowdown=$(echo "scale=2; $avg_ms / $neo4j_labeled_ms" | bc)
        echo -e "  ${YELLOW}vs Neo4j: ${slowdown}x slower${NC} (Neo4j: ~${neo4j_labeled_ms}ms for labeled nodes)"
    fi
}

# Monitor process stats
monitor_process() {
    local pid=$1
    local output_file="$2"
    
    echo -e "${BLUE}Monitoring process $pid...${NC}"
    
    while kill -0 $pid 2>/dev/null; do
        ps -p $pid -o pid,pcpu,pmem,rss,vsz,time,command >> "$output_file" 2>&1
        sleep 0.1
    done
}

# Main execution
main() {
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     NornicDB Write Performance Profiling                    ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Configuration:"
    echo "  HTTP Port:    $HTTP_PORT"
    echo "  Data Dir:     $DATA_DIR"
    echo "  Iterations:   $ITERATIONS"
    echo "  Concurrent:   $CONCURRENT"
    echo ""
    
    check_server
    
    # Cleanup before starting
    echo -e "\n${YELLOW}Cleaning up test data...${NC}"
    # Use separate queries for each label to avoid complex WHERE clauses
    send_query "MATCH (n:TestNode) DETACH DELETE n" "Cleanup TestNode" > /dev/null 2>&1 || true
    send_query "MATCH (n:ConcurrentTest) DETACH DELETE n" "Cleanup ConcurrentTest" > /dev/null 2>&1 || true
    send_query "MATCH (n:LargePayload) DETACH DELETE n" "Cleanup LargePayload" > /dev/null 2>&1 || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    
    # Create output directory
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local output_dir="profiling_results_${timestamp}"
    mkdir -p "$output_dir"
    
    # Record initial directory size
    local initial_size=$(du -sh "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${BLUE}Initial data directory size: ${initial_size}${NC}"
    
    # Run tests (without file monitoring to avoid hangs)
    echo -e "${BLUE}Running tests...${NC}"
    test_single_create 2>&1 | tee "${output_dir}/test1_single_create.log"
    test_batch_create 2>&1 | tee "${output_dir}/test2_batch_create.log"
    test_merge 2>&1 | tee "${output_dir}/test3_merge.log"
    test_relationship_create 2>&1 | tee "${output_dir}/test4_relationship.log"
    test_property_update 2>&1 | tee "${output_dir}/test5_property_update.log"
    test_concurrent_writes 2>&1 | tee "${output_dir}/test6_concurrent.log"
    test_large_payloads 2>&1 | tee "${output_dir}/test7_large_payloads.log"
    
    # Record final directory size
    local final_size=$(du -sh "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${BLUE}Final data directory size: ${final_size}${NC}"
    echo "Initial size: ${initial_size}" > "${output_dir}/filesystem_activity.log"
    echo "Final size: ${final_size}" >> "${output_dir}/filesystem_activity.log"
    
    # Summary
    echo -e "\n${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     Profiling Complete                                      ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
    
    # Performance comparison with Neo4j benchmarks
    echo -e "\n${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║     Performance Comparison (Neo4j Benchmarks)               ║${NC}"
    echo -e "${BLUE}╠══════════════════════════════════════════════════════════════╣${NC}"
    echo -e "${BLUE}║ Neo4j Typical Write Latency (database operation only):      ║${NC}"
    echo -e "${BLUE}║   • Individual labeled nodes: ~1.67ms (600 ops/sec)        ║${NC}"
    echo -e "${BLUE}║   • Unlabeled nodes: ~0.125ms (8,000 ops/sec)               ║${NC}"
    echo -e "${BLUE}║   • Bulk operations: ~0.00017ms (600,000 ops/sec)           ║${NC}"
    echo -e "${BLUE}║                                                              ║${NC}"
    echo -e "${BLUE}║ NornicDB Measurement Method:                                ║${NC}"
    echo -e "${BLUE}║   • End-to-end latency: HTTP request → response            ║${NC}"
    echo -e "${BLUE}║   • Includes: network, JSON parsing, DB operation          ║${NC}"
    echo -e "${BLUE}║   • Measured using: date +%s.%N (nanosecond precision)    ║${NC}"
    echo -e "${BLUE}║                                                              ║${NC}"
    echo -e "${BLUE}║ Note: NornicDB numbers include HTTP API overhead           ║${NC}"
    echo -e "${BLUE}║       (~5-10ms typical for localhost HTTP requests)        ║${NC}"
    echo -e "${BLUE}║       For fair comparison, subtract HTTP overhead          ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    
    echo -e "\nResults saved to: ${output_dir}/"
    echo ""
    echo "To view file system activity:"
    echo "  cat ${output_dir}/filesystem_activity.log"
    echo ""
    echo "To analyze disk I/O:"
    echo "  ls -lh ${DATA_DIR}/*.sst ${DATA_DIR}/*.vlog"
}

# Check dependencies
if ! command -v bc &> /dev/null; then
    echo -e "${RED}Error: 'bc' command not found. Please install it.${NC}"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo -e "${RED}Error: 'curl' command not found. Please install it.${NC}"
    exit 1
fi

# Run main
main "$@"
