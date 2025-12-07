#!/bin/bash
# Test semantic search performance with and without K-means clustering

API_URL="http://localhost:7474"
NUM_QUERIES=20
QUERY_SAMPLES=(
    "genetically enhanced human in an underground bunker discovers an alien civilization"
    "psychological character study of identity and genetic engineering in confined spaces"
    "first contact and alien civilization revealed through subterranean research facilities"
    "robot companions and the ethics of human-AI bonds inside isolated bunkers"
    "space exploration as inner and outer journey: space travel launched from underground"
    "best-friend-turned-lover in Paris choosing between two lovers"
    "second-chance romance and heartfelt confessions in a charming Paris coffee shop"
    "love triangle on romantic Paris streets with gestures that prove true love"
    "love at first sight and fate-driven reunions in intimate Parisian settings"
    "character-driven romance about choices, sacrifice, and getting another chance at love"
)

echo "🔍 Testing Semantic Search Performance"
echo "======================================"
echo "Server: $API_URL"
echo "Number of queries: $NUM_QUERIES"
echo ""

# Function to run search and measure time
run_search_test() {
    local test_name=$1
    local total_time=0
    local query_times=()
    
    echo "Running $test_name..."
    echo ""
    
    for i in $(seq 1 $NUM_QUERIES); do
        # Pick a random query from samples
        query="${QUERY_SAMPLES[$((i % ${#QUERY_SAMPLES[@]}))]}"
        
        # Measure time (macOS compatible - use perl for milliseconds)
        start=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000')
        response=$(curl -s -X POST "$API_URL/nornicdb/search" \
            -H "Content-Type: application/json" \
            -d "{\"query\": \"$query\", \"limit\": 10}" \
            --max-time 30)
        end=$(perl -MTime::HiRes=time -e 'printf "%.0f\n", time * 1000')
        
        elapsed=$((end - start))
        query_times+=($elapsed)
        total_time=$((total_time + elapsed))
        
        # Check if successful
        if echo "$response" | grep -q "node"; then
            echo "  Query $i: $elapsed ms - '$query' ✓"
        else
            echo "  Query $i: $elapsed ms - '$query' ✗ (no results)"
        fi
        
        # Small delay between queries
        sleep 0.1
    done
    
    # Calculate statistics
    avg_time=$((total_time / NUM_QUERIES))
    
    # Find min and max
    min_time=${query_times[0]}
    max_time=${query_times[0]}
    for time in "${query_times[@]}"; do
        if [ $time -lt $min_time ]; then
            min_time=$time
        fi
        if [ $time -gt $max_time ]; then
            max_time=$time
        fi
    done
    
    echo ""
    echo "📊 Results for $test_name:"
    echo "  Total time:    ${total_time} ms"
    echo "  Average:       ${avg_time} ms"
    echo "  Min:           ${min_time} ms"
    echo "  Max:           ${max_time} ms"
    echo "  Queries/sec:   $(echo "scale=2; 1000 / $avg_time" | bc)"
    echo ""
    
    return $avg_time
}

# Get current server status
echo "Checking server status..."
status=$(curl -s "$API_URL/status")
if echo "$status" | grep -q "running"; then
    nodes=$(echo "$status" | jq -r '.database.nodes // 0')
    echo "✓ Server is running with $nodes nodes"
    echo ""
else
    echo "✗ Server is not responding"
    exit 1
fi

# Run test with K-means enabled
echo "════════════════════════════════════════"
echo "TEST 1: WITH K-MEANS CLUSTERING"
echo "════════════════════════════════════════"
run_search_test "K-means ENABLED"
kmeans_enabled_avg=$?

echo ""
echo "════════════════════════════════════════"
echo "⚠️  STOP THE SERVER NOW"
echo "════════════════════════════════════════"
echo ""
echo "Please:"
echo "1. Stop the container/server"
echo "2. Remove or set NORNICDB_KMEANS_CLUSTERING_ENABLED=false"
echo "3. Restart the server"
echo "4. Press ENTER when ready to continue..."
read -r

# Wait for server to be ready
echo "Waiting for server to be ready..."
for i in {1..30}; do
    if curl -s "$API_URL/health" > /dev/null 2>&1; then
        echo "✓ Server is ready"
        sleep 2
        break
    fi
    echo "  Waiting... ($i/30)"
    sleep 1
done

# Run test without K-means
echo ""
echo "════════════════════════════════════════"
echo "TEST 2: WITHOUT K-MEANS CLUSTERING"
echo "════════════════════════════════════════"
run_search_test "K-means DISABLED"
kmeans_disabled_avg=$?

# Compare results
echo ""
echo "════════════════════════════════════════"
echo "📈 PERFORMANCE COMPARISON"
echo "════════════════════════════════════════"
echo ""
echo "K-means ENABLED:  ${kmeans_enabled_avg} ms avg"
echo "K-means DISABLED: ${kmeans_disabled_avg} ms avg"
echo ""

if [ $kmeans_enabled_avg -lt $kmeans_disabled_avg ]; then
    improvement=$((kmeans_disabled_avg - kmeans_enabled_avg))
    percentage=$(echo "scale=1; ($improvement * 100) / $kmeans_disabled_avg" | bc)
    echo "✅ K-means is FASTER by ${improvement} ms (${percentage}% improvement)"
    echo "   Recommendation: Keep K-means clustering ENABLED"
else
    slowdown=$((kmeans_enabled_avg - kmeans_disabled_avg))
    percentage=$(echo "scale=1; ($slowdown * 100) / $kmeans_disabled_avg" | bc)
    echo "⚠️  K-means is SLOWER by ${slowdown} ms (${percentage}% slower)"
    echo "   Recommendation: Consider disabling K-means for this dataset size"
fi
echo ""
