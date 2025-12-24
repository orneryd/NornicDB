package bolt

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// performanceQueryExecutor wraps the Cypher executor for performance tests.
type performanceQueryExecutor struct {
	executor *cypher.StorageExecutor
}

func (p *performanceQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	result, err := p.executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

// startPerfTestServer starts a server for performance testing
func startPerfTestServer(t *testing.T) (*Server, int) {
	store := storage.NewMemoryEngine()
	cypherExec := cypher.NewStorageExecutor(store)
	executor := &performanceQueryExecutor{executor: cypherExec}

	config := &Config{
		Port:            0,
		MaxConnections:  10,
		ReadBufferSize:  8192,
		WriteBufferSize: DefaultConfig().WriteBufferSize,
	}

	server := New(config, executor)

	go func() {
		if err := server.ListenAndServe(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(50 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	t.Logf("Bolt server listening on bolt://localhost:%d", port)

	return server, port
}

// TestPerformance_RelationshipVsNode tests that relationship operations
// should be within 10x of node operations (they're the same complexity).
// FAILING: Currently relationship is 36x slower than node on JS driver.
func TestPerformance_RelationshipVsNode(t *testing.T) {
	server, port := startPerfTestServer(t)
	defer server.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	defer conn.Close()

	// Handshake and HELLO
	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Setup: Create nodes for relationship test
	for i := 0; i < 10; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	for i := 0; i < 10; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	iterations := 50

	// Test 1: Node create/delete (two queries)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		SendRun(t, conn, "CREATE (n:TestNode {id: 1}) RETURN n", nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)

		SendRun(t, conn, "MATCH (n:TestNode) DELETE n", nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	nodeTime := time.Since(start)
	nodeOpsPerSec := float64(iterations) / nodeTime.Seconds()

	// Test 2: Relationship create/delete (compound query)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		SendRun(t, conn, `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r`, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	relTime := time.Since(start)
	relOpsPerSec := float64(iterations) / relTime.Seconds()

	t.Logf("Node operations: %.0f ops/sec (%.2f ms/op)", nodeOpsPerSec, float64(nodeTime.Milliseconds())/float64(iterations))
	t.Logf("Relationship operations: %.0f ops/sec (%.2f ms/op)", relOpsPerSec, float64(relTime.Milliseconds())/float64(iterations))

	// ASSERTION: Relationship should be within 10x of node performance
	// Both are simple operations - the gap should not be huge
	ratio := nodeOpsPerSec / relOpsPerSec
	t.Logf("Performance ratio (node/rel): %.1fx", ratio)

	assert.Less(t, ratio, 10.0,
		"Relationship operations should not be more than 10x slower than node operations. "+
			"Current ratio: %.1fx. This indicates a performance bug in compound query handling.", ratio)
}

// TestPerformance_CompoundQueryShouldMatchSeparateQueries tests that
// a compound MATCH...CREATE...DELETE should be similar speed to separate queries.
func TestPerformance_CompoundQueryShouldMatchSeparateQueries(t *testing.T) {
	server, port := startPerfTestServer(t)
	defer server.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	defer conn.Close()

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Setup
	for i := 0; i < 10; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	for i := 0; i < 10; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	iterations := 50

	// Test 1: Compound query (one roundtrip)
	start := time.Now()
	for i := 0; i < iterations; i++ {
		SendRun(t, conn, `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r`, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	compoundTime := time.Since(start)
	compoundOps := float64(iterations) / compoundTime.Seconds()

	// Test 2: Two separate queries (two roundtrips)
	start = time.Now()
	for i := 0; i < iterations; i++ {
		SendRun(t, conn, `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) RETURN r`, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)

		SendRun(t, conn, "MATCH ()-[r:TEMP_REL]->() DELETE r", nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	separateTime := time.Since(start)
	separateOps := float64(iterations) / separateTime.Seconds()

	t.Logf("Compound query: %.0f ops/sec", compoundOps)
	t.Logf("Separate queries: %.0f ops/sec", separateOps)

	// ASSERTION: Compound query (1 roundtrip) should be FASTER than separate (2 roundtrips)
	// If compound is slower, there's a bug in our handling
	assert.Greater(t, compoundOps, separateOps*0.5,
		"Compound query should be at least half as fast as separate queries (1 vs 2 roundtrips)")
}

// TestPerformance_MinimumThroughput tests that we achieve minimum acceptable throughput
// for relationship operations over Bolt.
func TestPerformance_MinimumThroughput(t *testing.T) {
	server, port := startPerfTestServer(t)
	defer server.Close()

	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	require.NoError(t, err)
	defer conn.Close()

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Setup
	for i := 0; i < 100; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	for i := 0; i < 150; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	iterations := 100
	query := `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r`

	start := time.Now()
	for i := 0; i < iterations; i++ {
		SendRun(t, conn, query, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	elapsed := time.Since(start)
	opsPerSec := float64(iterations) / elapsed.Seconds()

	t.Logf("Relationship create/delete: %.0f ops/sec", opsPerSec)

	// MINIMUM: Should achieve at least 1000 ops/sec for simple relationship operations
	// Neo4j achieves ~1800 ops/sec, so 1000 is a reasonable minimum
	assert.Greater(t, opsPerSec, 1000.0,
		"Should achieve at least 1000 ops/sec for relationship create/delete. "+
			"Current: %.0f ops/sec. This is a performance regression.", opsPerSec)
}
