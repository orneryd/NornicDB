#!/bin/bash
# Test script to debug send_query output

HTTP_PORT=7474
DB_NAME=nornic

send_query() {
    local query="$1"
    local label="$2"
    local db_name=${3:-nornic}
    
    local escaped_query=$(echo "$query" | sed 's/"/\\"/g')
    
    local start_time=$(date +%s.%N)
    local response=$(curl -s --max-time 30 -X POST "http://localhost:${HTTP_PORT}/db/${db_name}/tx/commit" \
        -H "Content-Type: application/json" \
        -d "{\"statements\": [{\"statement\": \"$escaped_query\"}]}" 2>&1)
    local end_time=$(date +%s.%N)
    
    local duration=$(echo "$end_time - $start_time" | bc -l)
    
    # Check for actual errors in the JSON response
    if echo "$response" | grep -q '"errors":\s*\[[^]]' || echo "$response" | grep -q '"code"'; then
        echo "ERROR: $label" >&2
        echo "Response: $response" >&2
        return 1
    else
        echo "SUCCESS: $label - ${duration}s" >&2
        echo "$duration"
        return 0
    fi
}

echo "Testing send_query..."
result=$(send_query "CREATE (n:TestNode {id: 77777}) RETURN n.id" "Test" 2>&1)
echo "Full result:"
echo "$result"
echo ""
echo "Just the number:"
echo "$result" | grep -E '^[0-9]+\.[0-9]+$'
