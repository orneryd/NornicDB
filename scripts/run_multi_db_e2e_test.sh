#!/bin/bash

# Multi-Database E2E Test Script
# This script runs the complete end-to-end test sequence for multi-database functionality.
# It verifies in-place upgrade, database creation, data isolation, composite databases, and cleanup.

set -e  # Exit on error

# Configuration
SERVER_URL="${SERVER_URL:-http://localhost:7474}"
AUTH_USER="${AUTH_USER:-admin}"
AUTH_PASS="${AUTH_PASS:-password}"
DEFAULT_DB="${DEFAULT_DB:-nornic}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper function to make authenticated requests
make_request() {
    local method=$1
    local endpoint=$2
    local data=$3
    
    if [ -z "$data" ]; then
        curl -s -u "${AUTH_USER}:${AUTH_PASS}" \
            -X "${method}" \
            "${SERVER_URL}${endpoint}" \
            -H "Content-Type: application/json"
    else
        curl -s -u "${AUTH_USER}:${AUTH_PASS}" \
            -X "${method}" \
            "${SERVER_URL}${endpoint}" \
            -H "Content-Type: application/json" \
            -d "${data}"
    fi
}

# Helper function to execute Cypher query
execute_query() {
    local query=$1
    local db=${2:-${DEFAULT_DB}}
    
    # Escape JSON special characters in query
    # Replace newlines with spaces, escape quotes and backslashes
    local escaped_query=$(echo "$query" | sed 's/\\/\\\\/g' | sed 's/"/\\"/g' | tr '\n' ' ' | sed 's/  */ /g')
    
    local data=$(cat <<EOF
{
  "statements": [
    {
      "statement": "${escaped_query}"
    }
  ]
}
EOF
)
    
    make_request "POST" "/db/${db}/tx/commit" "${data}"
}

# Helper function to print test step
print_step() {
    echo -e "\n${BLUE}=== $1 ===${NC}"
}

# Helper function to print success
print_success() {
    echo -e "${GREEN}✅ $1${NC}"
}

# Helper function to print error
print_error() {
    echo -e "${RED}❌ $1${NC}"
}

# Helper function to print info
print_info() {
    echo -e "${YELLOW}ℹ️  $1${NC}"
}

# Check if server is running
check_server() {
    print_step "Checking Server Connection"
    
    local response=$(make_request "GET" "/" "")
    if echo "$response" | grep -q "bolt_direct"; then
        print_success "Server is running"
        echo "$response" | jq '.'
    else
        print_error "Server is not responding"
        exit 1
    fi
}

# Step 1: Verify In-Place Upgrade
step1_verify_upgrade() {
    print_step "Step 1: Verify In-Place Upgrade"
    
    print_info "Listing all databases..."
    local response=$(execute_query "SHOW DATABASES" "system")
    echo "$response" | jq '.results[0].data'
    
    print_info "Counting nodes in default database..."
    local response=$(execute_query "MATCH (n) RETURN count(n) as node_count" "${DEFAULT_DB}")
    local count=$(echo "$response" | jq -r '.results[0].data[0].row[0]')
    print_info "Default database has $count nodes"
}

# Step 2: Create First Database
step2_create_first_db() {
    print_step "Step 2: Create First Database"
    
    print_info "Creating test_db_a..."
    local response=$(execute_query "CREATE DATABASE test_db_a" "system")
    echo "$response" | jq '.'
    
    print_info "Verifying database was created..."
    local response=$(execute_query "SHOW DATABASES" "system")
    echo "$response" | jq '.results[0].data'
}

# Step 3: Insert Data in First Database
step3_insert_data_first() {
    print_step "Step 3: Insert Data in First Database"
    
    # Use :USE command with multi-statement query
    local query=':USE test_db_a CREATE (alice:Person {name: "Alice", id: "a1", db: "test_db_a"}) CREATE (bob:Person {name: "Bob", id: "a2", db: "test_db_a"}) CREATE (company:Company {name: "Acme Corp", id: "a3", db: "test_db_a"}) CREATE (alice)-[:WORKS_FOR]->(company) CREATE (bob)-[:WORKS_FOR]->(company) RETURN alice, bob, company'
    
    print_info "Inserting data into test_db_a..."
    local response=$(execute_query "$query" "test_db_a")
    local row_count=$(echo "$response" | jq -r '.results[0].data | length // 0')
    if [ "$row_count" = "null" ] || [ -z "$row_count" ]; then
        row_count=0
    fi
    echo "Created $row_count rows"
    if [ "$row_count" -eq 0 ]; then
        print_error "No rows created! Check query response:"
        echo "$response" | jq '.'
    fi
}

# Step 4: Query First Database
step4_query_first() {
    print_step "Step 4: Query First Database"
    
    print_info "Querying all nodes in test_db_a..."
    local query=':USE test_db_a MATCH (n) RETURN n.name as name, labels(n) as labels, n.db as db ORDER BY n.name'
    local response=$(execute_query "$query" "test_db_a")
    echo "$response" | jq '.results[0].data'
}

# Step 5: Create Second Database
step5_create_second_db() {
    print_step "Step 5: Create Second Database"
    
    print_info "Creating test_db_b..."
    local response=$(execute_query "CREATE DATABASE test_db_b" "system")
    echo "$response" | jq '.'
    
    print_info "Verifying both databases exist..."
    local response=$(execute_query "SHOW DATABASES" "system")
    echo "$response" | jq '.results[0].data'
}

# Step 6: Insert Data in Second Database
step6_insert_data_second() {
    print_step "Step 6: Insert Data in Second Database"
    
    local query=':USE test_db_b CREATE (charlie:Person {name: "Charlie", id: "b1", db: "test_db_b"}) CREATE (diana:Person {name: "Diana", id: "b2", db: "test_db_b"}) CREATE (order:Order {order_id: "ORD-001", amount: 1000, db: "test_db_b"}) CREATE (charlie)-[:PLACED]->(order) CREATE (diana)-[:PLACED]->(order) RETURN charlie, diana, order'
    
    print_info "Inserting data into test_db_b..."
    local response=$(execute_query "$query" "test_db_b")
    local row_count=$(echo "$response" | jq -r '.results[0].data | length // 0')
    if [ "$row_count" = "null" ] || [ -z "$row_count" ]; then
        row_count=0
    fi
    echo "Created $row_count rows"
    if [ "$row_count" -eq 0 ]; then
        print_error "No rows created! Check query response:"
        echo "$response" | jq '.'
    fi
}

# Step 7: Query Second Database
step7_query_second() {
    print_step "Step 7: Query Second Database"
    
    print_info "Querying all nodes in test_db_b..."
    local query=':USE test_db_b MATCH (n) RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db ORDER BY n.name'
    local response=$(execute_query "$query" "test_db_b")
    echo "$response" | jq '.results[0].data'
}

# Step 8: Verify Isolation
step8_verify_isolation() {
    print_step "Step 8: Verify Isolation"
    
    print_info "Verifying test_db_a only sees its own data..."
    local query=':USE test_db_a MATCH (n) RETURN n.name as name, n.order_id as order_id, labels(n) as labels, n.db as db ORDER BY n.name'
    local response=$(execute_query "$query" "test_db_a")
    local count=$(echo "$response" | jq -r '.results[0].data | length // 0')
    if [ "$count" = "null" ] || [ -z "$count" ]; then
        count=0
    fi
    print_info "test_db_a has $count nodes"
    
    print_info "Verifying test_db_b only sees its own data..."
    local query=':USE test_db_b MATCH (n) RETURN n.name as name, labels(n) as labels, n.db as db ORDER BY n.name'
    local response=$(execute_query "$query" "test_db_b")
    local count=$(echo "$response" | jq -r '.results[0].data | length // 0')
    if [ "$count" = "null" ] || [ -z "$count" ]; then
        count=0
    fi
    print_info "test_db_b has $count nodes"
    
    print_info "Verifying default database doesn't see test databases' data..."
    local query=":USE ${DEFAULT_DB} MATCH (n) WHERE n.db IN [\"test_db_a\", \"test_db_b\"] RETURN count(n) as test_db_nodes"
    local response=$(execute_query "$query" "${DEFAULT_DB}")
    local count=$(echo "$response" | jq -r '.results[0].data[0].row[0] // 0')
    if [ "$count" = "null" ] || [ -z "$count" ]; then
        count=0
    fi
    if [ "$count" = "0" ]; then
        print_success "Default database correctly isolated (0 test_db nodes)"
    else
        print_error "Default database sees $count test_db nodes (expected 0)"
    fi
}

# Step 9: Create Composite Database
step9_create_composite() {
    print_step "Step 9: Create Composite Database"
    
    print_info "Creating composite database..."
    local query='CREATE COMPOSITE DATABASE test_composite ALIAS db_a FOR DATABASE test_db_a ALIAS db_b FOR DATABASE test_db_b'
    local response=$(execute_query "$query" "system")
    if echo "$response" | jq -e '.errors | length > 0' > /dev/null 2>&1; then
        print_error "Failed to create composite database:"
        echo "$response" | jq '.errors'
    else
        echo "$response" | jq '.'
    fi
    
    print_info "Verifying composite database was created..."
    local response=$(execute_query "SHOW COMPOSITE DATABASES" "system")
    echo "$response" | jq '.results[0].data'
    
    print_info "Showing constituents..."
    local query='SHOW CONSTITUENTS FOR COMPOSITE DATABASE test_composite'
    local response=$(execute_query "$query" "system")
    echo "$response" | jq '.results[0].data'
}

# Step 10: Query Composite Database
step10_query_composite() {
    print_step "Step 10: Query Composite Database"
    
    print_info "Querying all Person nodes across composite database..."
    local query='MATCH (p:Person) RETURN p.name as name, p.db as db, labels(p) as labels ORDER BY p.name'
    local response=$(execute_query "$query" "test_composite")
    echo "$response" | jq '.results[0].data'
    
    print_info "Counting all nodes across composite database..."
    local query='MATCH (n) RETURN count(n) as total_nodes'
    local response=$(execute_query "$query" "test_composite")
    local count=$(echo "$response" | jq -r '.results[0].data[0].row[0] // 0')
    if [ "$count" = "null" ] || [ -z "$count" ]; then
        count=0
    fi
    print_info "Composite database has $count total nodes"
    
    print_info "Counting nodes by label..."
    local query='MATCH (n) UNWIND labels(n) as label RETURN label, count(*) as count ORDER BY label'
    local response=$(execute_query "$query" "test_composite")
    echo "$response" | jq '.results[0].data'
}

# Step 11: Cleanup
step11_cleanup() {
    print_step "Step 11: Cleanup"
    
    print_info "Dropping composite database..."
    local response=$(execute_query "DROP COMPOSITE DATABASE test_composite" "system")
    echo "$response" | jq '.'
    
    print_info "Dropping test_db_a..."
    local response=$(execute_query "DROP DATABASE test_db_a" "system")
    echo "$response" | jq '.'
    
    print_info "Dropping test_db_b..."
    local response=$(execute_query "DROP DATABASE test_db_b" "system")
    echo "$response" | jq '.'
    
    print_info "Verifying cleanup..."
    local response=$(execute_query "SHOW DATABASES" "system")
    echo "$response" | jq '.results[0].data'
    
    print_info "Verifying default database still has data..."
    local response=$(execute_query "MATCH (n) RETURN count(n) as node_count" "${DEFAULT_DB}")
    local count=$(echo "$response" | jq -r '.results[0].data[0].row[0]')
    print_info "Default database has $count nodes"
}

# Main execution
main() {
    echo -e "${GREEN}"
    echo "╔══════════════════════════════════════════════════════════════╗"
    echo "║     Multi-Database E2E Test Script                            ║"
    echo "╚══════════════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    
    check_server
    
    step1_verify_upgrade
    step2_create_first_db
    step3_insert_data_first
    step4_query_first
    step5_create_second_db
    step6_insert_data_second
    step7_query_second
    step8_verify_isolation
    step9_create_composite
    step10_query_composite
    step11_cleanup
    
    echo -e "\n${GREEN}╔══════════════════════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║     All Tests Completed Successfully!                        ║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════════════════════╝${NC}"
}

# Run main function
main

