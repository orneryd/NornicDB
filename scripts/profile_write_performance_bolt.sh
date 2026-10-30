#!/bin/bash
# Write Performance Profiling Script for NornicDB (Bolt Protocol)
# Tests write latency over Bolt protocol (more efficient than HTTP)

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BOLT_PORT=${BOLT_PORT:-7687}
BOLT_URI="bolt://localhost:${BOLT_PORT}"
DATA_DIR=${DATA_DIR:-./data/test}
ITERATIONS=${ITERATIONS:-100}
CONCURRENT=${CONCURRENT:-10}
DB_NAME=${DB_NAME:-nornic}

# Path to Bolt client script
BOLT_CLIENT_SCRIPT="./scripts/bolt_client.py"

# Check if server is running
check_server() {
    if ! python3 -c "from neo4j import GraphDatabase; driver = GraphDatabase.driver('${BOLT_URI}', auth=None); driver.verify_connectivity(); driver.close()" 2>/dev/null; then
        echo -e "${RED}Error: NornicDB Bolt server is not running on port ${BOLT_PORT}${NC}"
        echo -e "${YELLOW}Make sure NornicDB is running with Bolt enabled${NC}"
        exit 1
    fi
    echo -e "${GREEN}✓ Bolt server is running on port ${BOLT_PORT}${NC}"
}

# Send Cypher query via Bolt protocol
send_query() {
    local query="$1"
    local label="$2"
    local db_name=${3:-$DB_NAME}
    local with_result=${4:-""}
    
    # Use Python Bolt client script (measures query execution time internally)
    # This gives us the actual query time, not Python startup overhead
    local output
    if [ "$with_result" = "with_result" ]; then
        output=$(BOLT_CLIENT_OUTPUT_RESULT=1 python3 "$BOLT_CLIENT_SCRIPT" "$BOLT_URI" "$query" "$db_name" 2>&1)
    else
        output=$(python3 "$BOLT_CLIENT_SCRIPT" "$BOLT_URI" "$query" "$db_name" 2>&1)
    fi
    local exit_code=$?
    
    # The Python script outputs the duration directly
    local duration=$(echo "$output" | grep -E '^[0-9]*\.[0-9]+$' | head -1)
    local result_value=""
    if [ "$with_result" = "with_result" ]; then
        result_value=$(echo "$output" | grep -E '^[0-9]+$' | tail -1)
    fi
    
    if [ $exit_code -ne 0 ] || echo "$output" | grep -q "ERROR" || [ -z "$duration" ]; then
        echo -e "${RED}✗ ${label}: ERROR${NC}" >&2
        echo "  Response: $output" | head -3 >&2
        return 1
    else
        # Output to stderr for display, stdout for data collection (numeric only)
        echo -e "${GREEN}✓ ${label}: ${duration}s${NC}" >&2
        echo "$duration"  # This goes to stdout for capture
        if [ "$with_result" = "with_result" ] && [ -n "$result_value" ]; then
            echo "$result_value"
        fi
        return 0
    fi
}

# Test 1: Single node creation
test_single_create() {
    echo -e "\n${YELLOW}=== Test 1: Single Node Creation (Bolt) ===${NC}"
    
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
    echo -e "\n${YELLOW}=== Test 2: Batch Node Creation (10 nodes per query, Bolt) ===${NC}"
    
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
    echo -e "\n${YELLOW}=== Test 3: MERGE Operations (Upsert, Bolt) ===${NC}"
    
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
    echo -e "\n${YELLOW}=== Test 4: Relationship Creation (Bolt) ===${NC}"
    
    # First create some nodes (show errors if they occur)
    echo -e "${BLUE}Setting up test nodes...${NC}"
    send_query "MATCH (n:TestNode) DELETE n" "Cleanup" > /dev/null 2>&1 || true
    
    local setup_output=$(send_query "CREATE (n1:TestNode {id: 1}), (n2:TestNode {id: 2}) RETURN count(*) as count" "Setup nodes" 2>&1)
    if echo "$setup_output" | grep -q "ERROR"; then
        echo -e "${RED}Failed to create setup nodes:${NC}"
        echo "$setup_output" | grep -A 5 "ERROR" >&2
        echo -e "${RED}No successful operations${NC}"
        return
    fi
    
    # Verify nodes were created
    local verify_output=$(send_query "MATCH (n:TestNode) WHERE n.id IN [1, 2] RETURN count(n) as count" "Verify nodes" "$DB_NAME" "with_result" 2>&1)
    local node_count=$(echo "$verify_output" | grep -E '^[0-9]+$' | head -1)
    if [ -z "$node_count" ] || [ "$node_count" -lt 2 ]; then
        echo -e "${RED}Warning: Expected 2 nodes, found ${node_count:-0}. Relationship creation may fail.${NC}" >&2
        echo -e "${YELLOW}Setup output: $setup_output${NC}" >&2
        echo -e "${YELLOW}Verify output: $verify_output${NC}" >&2
    else
        echo -e "${GREEN}✓ Setup complete: 2 nodes ready${NC}"
    fi
    
    local latencies=()
    for i in $(seq 1 $ITERATIONS); do
        local query="MATCH (a:TestNode {id: 1}), (b:TestNode {id: 2}) CREATE (a)-[r:RELATES {id: $i, timestamp: timestamp()}]->(b) RETURN r.id"
        local output=$(send_query "$query" "Create relationship $i" 2>&1)
        local latency=$(echo "$output" | tail -1 | tr -d '[:space:]')
        # Verify it's a valid number (format: 0.012700000 or .012700000)
        if [ ! -z "$latency" ] && echo "$latency" | grep -qE '^[0-9]*\.[0-9]+$'; then
            latencies+=($latency)
        else
            # Log first few failures for debugging
            if [ $i -le 3 ]; then
                echo -e "${YELLOW}Relationship $i failed. Output: $output${NC}" >&2
            fi
        fi
    done
    
    calculate_stats "${latencies[@]}"
}

# Test 5: Property updates (SET)
test_property_update() {
    echo -e "\n${YELLOW}=== Test 5: Property Updates (SET, Bolt) ===${NC}"
    
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
    echo -e "\n${YELLOW}=== Test 6: Concurrent Writes ($CONCURRENT concurrent, Bolt) ===${NC}"
    
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
    echo -e "\n${YELLOW}=== Test 7: Large Property Payloads (Bolt) ===${NC}"
    
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

# Main execution
main() {
    echo -e "${BLUE}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║  NornicDB Write Performance Profiling (Bolt Protocol)     ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Configuration:"
    echo "  Bolt URI:    $BOLT_URI"
    echo "  Data Dir:    $DATA_DIR"
    echo "  Iterations:  $ITERATIONS"
    echo "  Concurrent:  $CONCURRENT"
    echo "  Database:    $DB_NAME"
    echo ""
    
    # Check dependencies
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Error: 'python3' command not found. Please install Python 3.${NC}"
        exit 1
    fi
    
    if ! python3 -c "import neo4j" 2>/dev/null; then
        echo -e "${YELLOW}Installing neo4j Python driver...${NC}"
        pip3 install neo4j > /dev/null 2>&1 || {
            echo -e "${RED}Error: Failed to install neo4j driver. Please run: pip3 install neo4j${NC}"
            exit 1
        }
    fi
    
    if ! command -v bc &> /dev/null; then
        echo -e "${RED}Error: 'bc' command not found. Please install it.${NC}"
        exit 1
    fi
    
    check_server
    
    # Cleanup before starting
    echo -e "\n${YELLOW}Cleaning up test data...${NC}"
    # Use separate queries for each label to avoid complex WHERE clauses
    send_query "MATCH (n:TestNode) DETACH DELETE n" "Cleanup TestNode" > /dev/null 2>&1 || true
    send_query "MATCH (n:ConcurrentTest) DETACH DELETE n" "Cleanup ConcurrentTest" > /dev/null 2>&1 || true
    send_query "MATCH (n:LargePayload) DETACH DELETE n" "Cleanup LargePayload" > /dev/null 2>&1 || true
    echo -e "${GREEN}✓ Cleanup complete${NC}"
    
    # Create output directory first
    local timestamp=$(date +%Y%m%d_%H%M%S)
    local output_dir="profiling_results_bolt_${timestamp}"
    mkdir -p "$output_dir"
    
    # Record initial directory size
    local initial_size=$(du -sh "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${BLUE}Initial data directory size: ${initial_size}${NC}"
    
    # Run tests
    echo -e "${BLUE}Running tests over Bolt protocol...${NC}"
    test_single_create 2>&1 | tee "${output_dir}/test1_single_create_bolt.log"
    test_batch_create 2>&1 | tee "${output_dir}/test2_batch_create_bolt.log"
    test_merge 2>&1 | tee "${output_dir}/test3_merge_bolt.log"
    test_relationship_create 2>&1 | tee "${output_dir}/test4_relationship_bolt.log"
    test_property_update 2>&1 | tee "${output_dir}/test5_property_update_bolt.log"
    test_concurrent_writes 2>&1 | tee "${output_dir}/test6_concurrent_bolt.log"
    test_large_payloads 2>&1 | tee "${output_dir}/test7_large_payloads_bolt.log"
    
    # Record final directory size
    local final_size=$(du -sh "${DATA_DIR}" 2>/dev/null | cut -f1 || echo "unknown")
    echo -e "${BLUE}Final data directory size: ${final_size}${NC}"
    echo "Initial size: ${initial_size}" > "${output_dir}/filesystem_activity.log"
    echo "Final size: ${final_size}" >> "${output_dir}/filesystem_activity.log"
    
    # Summary
    echo -e "\n${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     Profiling Complete (Bolt Protocol)                      ║${NC}"
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
    echo -e "${BLUE}║ NornicDB Measurement Method (Bolt):                         ║${NC}"
    echo -e "${BLUE}║   • End-to-end latency: Bolt request → response            ║${NC}"
    echo -e "${BLUE}║   • Includes: network, protocol overhead, DB operation    ║${NC}"
    echo -e "${BLUE}║   • Measured using: time.perf_counter() (Python)           ║${NC}"
    echo -e "${BLUE}║                                                              ║${NC}"
    echo -e "${BLUE}║ Note: Bolt protocol has less overhead than HTTP            ║${NC}"
    echo -e "${BLUE}║       (binary protocol vs JSON, no HTTP headers)            ║${NC}"
    echo -e "${BLUE}╚══════════════════════════════════════════════════════════════╝${NC}"
    
    echo -e "\nResults saved to: ${output_dir}/"
    echo ""
    echo "To compare with HTTP results:"
    echo "  Compare avg latency: Bolt should be faster than HTTP"
    echo ""
    echo "To analyze disk I/O:"
    echo "  ls -lh ${DATA_DIR}/*.sst ${DATA_DIR}/*.vlog"
}

# Run main
main "$@"
