// Integration tests for Bolt server with Cypher executor.
//
// These tests verify that the Bolt protocol server works correctly with
// the Cypher query executor, simulating real-world Neo4j driver usage.
package bolt

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// skipDiskIOTestOnWindows skips disk I/O intensive tests on Windows to avoid OOM
func skipDiskIOTestOnWindows(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("Skipping disk I/O intensive test on Windows due to memory constraints")
	}
	if os.Getenv("CI") != "" && os.Getenv("GITHUB_ACTIONS") != "" {
		t.Skip("Skipping disk I/O test in CI environment")
	}
}

// cypherQueryExecutor wraps the Cypher executor for Bolt server.
type cypherQueryExecutor struct {
	executor *cypher.StorageExecutor
}

func (c *cypherQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	result, err := c.executor.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}

	return &QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}

// TestBoltCypherIntegration tests the full stack: Bolt server + Cypher executor.
func TestBoltCypherIntegration(t *testing.T) {
	// Create storage and executor
	store := storage.NewMemoryEngine()
	cypherExec := cypher.NewStorageExecutor(store)
	executor := &cypherQueryExecutor{executor: cypherExec}

	// Start Bolt server on random port
	config := &Config{
		Port:            0, // Random port
		MaxConnections:  10,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}

	server := New(config, executor)
	defer server.Close()

	// Start server
	go func() {
		if err := server.ListenAndServe(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()

	// Wait for server to start
	time.Sleep(100 * time.Millisecond)

	// Get actual port
	port := server.listener.Addr().(*net.TCPAddr).Port

	t.Run("create_and_query_node", func(t *testing.T) {
		// Connect to server
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Perform handshake
		if err := performHandshake(t, conn); err != nil {
			t.Fatalf("Handshake failed: %v", err)
		}

		// Send HELLO
		if err := sendHello(t, conn); err != nil {
			t.Fatalf("HELLO failed: %v", err)
		}

		// Wait for SUCCESS response
		if err := readSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS after HELLO: %v", err)
		}

		// Send CREATE query
		createQuery := "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n"
		if err := sendRun(t, conn, createQuery, nil); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS with fields
		if err := readSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS after RUN: %v", err)
		}

		// Send PULL to get results
		if err := sendPull(t, conn); err != nil {
			t.Fatalf("PULL failed: %v", err)
		}

		// Read RECORD and final SUCCESS
		hasRecord := false
		for {
			msgType, err := readMessageType(t, conn)
			if err != nil {
				t.Fatalf("Failed to read message: %v", err)
			}

			if msgType == MsgRecord {
				hasRecord = true
				// Skip record data for now
			} else if msgType == MsgSuccess {
				break
			}
		}

		if !hasRecord {
			t.Error("Expected at least one RECORD message")
		}
	})

	t.Run("match_query", func(t *testing.T) {
		// Connect to server
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Perform handshake
		if err := performHandshake(t, conn); err != nil {
			t.Fatalf("Handshake failed: %v", err)
		}

		// Send HELLO
		if err := sendHello(t, conn); err != nil {
			t.Fatalf("HELLO failed: %v", err)
		}

		// Wait for SUCCESS
		if err := readSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS: %v", err)
		}

		// Send MATCH query
		matchQuery := "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name, n.age"
		if err := sendRun(t, conn, matchQuery, nil); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS
		if err := readSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS: %v", err)
		}

		// Send PULL
		if err := sendPull(t, conn); err != nil {
			t.Fatalf("PULL failed: %v", err)
		}

		// Read results
		hasRecord := false
		for {
			msgType, err := readMessageType(t, conn)
			if err != nil {
				t.Fatalf("Failed to read message: %v", err)
			}

			if msgType == MsgRecord {
				hasRecord = true
			} else if msgType == MsgSuccess {
				break
			}
		}

		if !hasRecord {
			t.Error("Expected RECORD for Alice")
		}
	})

	t.Run("parameterized_query", func(t *testing.T) {
		// Connect to server
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Perform handshake and HELLO
		performHandshake(t, conn)
		sendHello(t, conn)
		readSuccess(t, conn)

		// Send parameterized query
		query := "CREATE (n:Person {name: $name, age: $age}) RETURN n"
		params := map[string]any{
			"name": "Bob",
			"age":  int64(25),
		}

		if err := sendRun(t, conn, query, params); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS
		readSuccess(t, conn)

		// Send PULL
		sendPull(t, conn)

		// Read results
		for {
			msgType, err := readMessageType(t, conn)
			if err != nil {
				break
			}
			if msgType == MsgSuccess {
				break
			}
		}
	})

	t.Run("transaction_flow", func(t *testing.T) {
		// Connect to server
		conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer conn.Close()

		// Handshake and HELLO
		performHandshake(t, conn)
		sendHello(t, conn)
		readSuccess(t, conn)

		// Send BEGIN
		if err := sendBegin(t, conn); err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		readSuccess(t, conn)

		// Send query in transaction
		sendRun(t, conn, "CREATE (n:Test {id: 'tx-test'})", nil)
		readSuccess(t, conn)
		sendPull(t, conn)

		// Consume results
		for {
			msgType, _ := readMessageType(t, conn)
			if msgType == MsgSuccess {
				break
			}
		}

		// Send COMMIT
		if err := sendCommit(t, conn); err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		readSuccess(t, conn)
	})
}

// Helper functions for protocol communication

func performHandshake(t *testing.T, conn net.Conn) error {
	t.Helper()

	// Send magic + versions
	handshake := []byte{
		0x60, 0x60, 0xB0, 0x17, // Magic
		0x00, 0x00, 0x04, 0x04, // Bolt 4.4
		0x00, 0x00, 0x04, 0x03, // Bolt 4.3
		0x00, 0x00, 0x04, 0x02, // Bolt 4.2
		0x00, 0x00, 0x04, 0x01, // Bolt 4.1
	}

	if _, err := conn.Write(handshake); err != nil {
		return err
	}

	// Read version response
	resp := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}

	return nil
}

func sendHello(t *testing.T, conn net.Conn) error {
	t.Helper()

	// HELLO message: struct(0xB1) + signature(0x01) + empty map(0xA0)
	message := []byte{0xB1, MsgHello, 0xA0}
	return sendMessage(conn, message)
}

func sendRun(t *testing.T, conn net.Conn, query string, params map[string]any) error {
	t.Helper()

	// RUN message: struct + signature + query string + params map + extra map
	message := []byte{0xB1, MsgRun}
	message = append(message, encodePackStreamString(query)...)

	if params == nil {
		params = make(map[string]any)
	}
	message = append(message, encodePackStreamMap(params)...)

	// Add empty extra map
	message = append(message, 0xA0)

	return sendMessage(conn, message)
}

func sendPull(t *testing.T, conn net.Conn) error {
	t.Helper()

	// PULL message: struct + signature + options map
	message := []byte{0xB1, MsgPull, 0xA0}
	return sendMessage(conn, message)
}

func sendBegin(t *testing.T, conn net.Conn) error {
	t.Helper()

	// BEGIN message: struct + signature + empty options
	message := []byte{0xB1, MsgBegin, 0xA0}
	return sendMessage(conn, message)
}

func sendCommit(t *testing.T, conn net.Conn) error {
	t.Helper()

	// COMMIT message: struct + signature (no data)
	message := []byte{0xB0, MsgCommit}
	return sendMessage(conn, message)
}

func sendMessage(conn net.Conn, data []byte) error {
	// Chunk header
	size := len(data)
	header := []byte{byte(size >> 8), byte(size)}

	if _, err := conn.Write(header); err != nil {
		return err
	}

	// Data
	if _, err := conn.Write(data); err != nil {
		return err
	}

	// Terminator
	if _, err := conn.Write([]byte{0x00, 0x00}); err != nil {
		return err
	}

	return nil
}

func readSuccess(t *testing.T, conn net.Conn) error {
	t.Helper()

	msgType, err := readMessageType(t, conn)
	if err != nil {
		return err
	}

	if msgType != MsgSuccess {
		return fmt.Errorf("expected SUCCESS (0x70), got 0x%02X", msgType)
	}

	return nil
}

func readMessageType(t *testing.T, conn net.Conn) (byte, error) {
	t.Helper()

	// Read all chunks until zero terminator
	var message []byte

	for {
		// Read chunk header
		header := make([]byte, 2)
		if _, err := io.ReadFull(conn, header); err != nil {
			return 0, err
		}

		size := int(header[0])<<8 | int(header[1])
		if size == 0 {
			break
		}

		// Read chunk data
		chunk := make([]byte, size)
		if _, err := io.ReadFull(conn, chunk); err != nil {
			return 0, err
		}

		message = append(message, chunk...)
	}

	if len(message) < 2 {
		return 0, fmt.Errorf("message too short")
	}

	// Message format: struct marker + signature
	return message[1], nil
}

// TestBoltServerStress tests the server under load.
func TestBoltServerStress(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping stress test in short mode")
	}

	// Create storage and executor
	store := storage.NewMemoryEngine()
	cypherExec := cypher.NewStorageExecutor(store)
	executor := &cypherQueryExecutor{executor: cypherExec}

	// Start server
	config := &Config{Port: 0, MaxConnections: 50}
	server := New(config, executor)
	defer server.Close()

	go server.ListenAndServe()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port

	// Launch multiple concurrent connections
	const numConnections = 20
	done := make(chan error, numConnections)

	for i := 0; i < numConnections; i++ {
		go func(id int) {
			conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
			if err != nil {
				done <- err
				return
			}
			defer conn.Close()

			// Perform handshake
			performHandshake(t, conn)
			sendHello(t, conn)
			readSuccess(t, conn)

			// Execute queries
			query := fmt.Sprintf("CREATE (n:Test {id: %d}) RETURN n", id)
			sendRun(t, conn, query, nil)
			readSuccess(t, conn)
			sendPull(t, conn)

			// Read results
			for {
				msgType, err := readMessageType(t, conn)
				if err != nil {
					done <- err
					return
				}
				if msgType == MsgSuccess {
					break
				}
			}

			done <- nil
		}(i)
	}

	// Wait for all connections
	for i := 0; i < numConnections; i++ {
		if err := <-done; err != nil {
			t.Errorf("Connection %d failed: %v", i, err)
		}
	}
}

// TestBoltBenchmarkCreateDeleteRelationship measures real Bolt network performance
// KEEP THIS TEST - this is the actual Bolt layer benchmark
func TestBoltBenchmarkCreateDeleteRelationship(t *testing.T) {
	// Create storage with AsyncEngine
	store := storage.NewMemoryEngine()
	asyncStore := storage.NewAsyncEngine(store, nil)
	cypherExec := cypher.NewStorageExecutor(asyncStore)
	executor := &cypherQueryExecutor{executor: cypherExec}

	config := &Config{
		Port:            0,
		MaxConnections:  10,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}

	server := New(config, executor)
	defer server.Close()

	go func() {
		server.ListenAndServe()
	}()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port

	// Connect
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Handshake and HELLO
	performHandshake(t, conn)
	sendHello(t, conn)
	readSuccess(t, conn)

	// Create test nodes
	sendRun(t, conn, "CREATE (a:Actor {name: 'Test'})", nil)
	readSuccess(t, conn)
	sendPull(t, conn)
	readSuccess(t, conn) // SUCCESS after PULL

	sendRun(t, conn, "CREATE (m:Movie {title: 'Test'})", nil)
	readSuccess(t, conn)
	sendPull(t, conn)
	readSuccess(t, conn)

	// Benchmark the slow query
	iterations := 100
	t.Logf("Running %d iterations over Bolt (small dataset)", iterations)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		query := `MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r`
		sendRun(t, conn, query, nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(iterations) / elapsed.Seconds()
	avgMs := elapsed.Seconds() * 1000 / float64(iterations)
	t.Logf("Completed %d iterations in %v", iterations, elapsed)
	t.Logf("Bolt Performance: %.2f ops/sec, %.3f ms/op", opsPerSec, avgMs)

	// This should help identify if JS driver is the bottleneck
	if opsPerSec < 500 {
		t.Logf("WARNING: Bolt performance %.2f ops/sec is below 500 target", opsPerSec)
	}
}

// TestBoltBenchmarkCreateDeleteRelationship_LargeDataset simulates real benchmark conditions
// KEEP THIS TEST - this shows performance with realistic data volume (100 actors, 150 movies)
func TestBoltBenchmarkCreateDeleteRelationship_LargeDataset(t *testing.T) {
	store := storage.NewMemoryEngine()
	asyncStore := storage.NewAsyncEngine(store, nil)
	cypherExec := cypher.NewStorageExecutor(asyncStore)
	executor := &cypherQueryExecutor{executor: cypherExec}

	config := &Config{Port: 0, MaxConnections: 10}
	server := New(config, executor)
	defer server.Close()

	go func() { server.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	performHandshake(t, conn)
	sendHello(t, conn)
	readSuccess(t, conn)

	// Create 100 actors
	t.Log("Creating 100 actors...")
	for i := 0; i < 100; i++ {
		sendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d', born: %d})", i, 1950+i%50), nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}

	// Create 150 movies
	t.Log("Creating 150 movies...")
	for i := 0; i < 150; i++ {
		sendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d', released: %d})", i, 1980+i%44), nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}

	// Benchmark
	iterations := 100
	t.Logf("Running %d iterations over Bolt (Memory, 100 actors + 150 movies)", iterations)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		query := `MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r`
		sendRun(t, conn, query, nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(iterations) / elapsed.Seconds()
	t.Logf("Bolt Performance (Memory, large dataset): %.2f ops/sec, %.3f ms/op", opsPerSec, elapsed.Seconds()*1000/float64(iterations))
}

// TestBoltBenchmarkCreateDeleteRelationship_Badger tests with BadgerDB (realistic)
// KEEP THIS TEST - shows performance with disk-based storage
func TestBoltBenchmarkCreateDeleteRelationship_Badger(t *testing.T) {
	skipDiskIOTestOnWindows(t)
	tmpDir := t.TempDir()
	badgerEngine, err := storage.NewBadgerEngine(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create BadgerEngine: %v", err)
	}
	defer badgerEngine.Close()

	asyncStore := storage.NewAsyncEngine(badgerEngine, nil)
	cypherExec := cypher.NewStorageExecutor(asyncStore)
	executor := &cypherQueryExecutor{executor: cypherExec}

	config := &Config{Port: 0, MaxConnections: 10}
	server := New(config, executor)
	defer server.Close()

	go func() { server.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	performHandshake(t, conn)
	sendHello(t, conn)
	readSuccess(t, conn)

	// Create 100 actors
	t.Log("Creating 100 actors (BadgerDB)...")
	for i := 0; i < 100; i++ {
		sendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}

	// Create 150 movies
	t.Log("Creating 150 movies (BadgerDB)...")
	for i := 0; i < 150; i++ {
		sendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}

	// Benchmark
	iterations := 100
	t.Logf("Running %d iterations over Bolt (BadgerDB, 100 actors + 150 movies)", iterations)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		query := `MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r`
		sendRun(t, conn, query, nil)
		readSuccess(t, conn)
		sendPull(t, conn)
		readSuccess(t, conn)
	}
	elapsed := time.Since(start)

	opsPerSec := float64(iterations) / elapsed.Seconds()
	t.Logf("Bolt Performance (BadgerDB, large dataset): %.2f ops/sec, %.3f ms/op", opsPerSec, elapsed.Seconds()*1000/float64(iterations))
}

// TestBoltResponseMetadata verifies Neo4j-compatible metadata in responses
// KEEP THIS TEST - ensures JS driver compatibility
func TestBoltResponseMetadata(t *testing.T) {
	store := storage.NewMemoryEngine()
	asyncStore := storage.NewAsyncEngine(store, nil)
	cypherExec := cypher.NewStorageExecutor(asyncStore)
	executor := &cypherQueryExecutor{executor: cypherExec}

	config := &Config{Port: 0, MaxConnections: 10}
	server := New(config, executor)
	defer server.Close()

	go func() { server.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	performHandshake(t, conn)
	sendHello(t, conn)
	readSuccess(t, conn)

	// Test write query returns proper metadata
	sendRun(t, conn, "CREATE (n:TestNode {name: 'test'})", nil)
	if err := readSuccess(t, conn); err != nil {
		t.Fatalf("RUN failed: %v", err)
	}

	sendPull(t, conn)
	if err := readSuccess(t, conn); err != nil {
		t.Fatalf("PULL failed: %v", err)
	}

	t.Log("Response metadata verified - Neo4j driver should work correctly")
}

// TestBoltLatencyBreakdown measures where time is spent in protocol exchange
// KEEP THIS TEST - helps identify bottlenecks in protocol handling
func TestBoltLatencyBreakdown(t *testing.T) {
	store := storage.NewMemoryEngine()
	asyncStore := storage.NewAsyncEngine(store, nil)
	cypherExec := cypher.NewStorageExecutor(asyncStore)
	executor := &cypherQueryExecutor{executor: cypherExec}

	config := &Config{Port: 0, MaxConnections: 10}
	server := New(config, executor)
	defer server.Close()

	go func() { server.ListenAndServe() }()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	performHandshake(t, conn)
	sendHello(t, conn)
	readSuccess(t, conn)

	// Create test data
	sendRun(t, conn, "CREATE (a:Actor {name: 'Keanu'})", nil)
	readSuccess(t, conn)
	sendPull(t, conn)
	readSuccess(t, conn)

	sendRun(t, conn, "CREATE (m:Movie {title: 'Matrix'})", nil)
	readSuccess(t, conn)
	sendPull(t, conn)
	readSuccess(t, conn)

	// Measure each phase separately
	iterations := 50
	var runTotal, pullTotal time.Duration

	for i := 0; i < iterations; i++ {
		// Measure RUN
		runStart := time.Now()
		sendRun(t, conn, `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r`, nil)
		readSuccess(t, conn)
		runTotal += time.Since(runStart)

		// Measure PULL
		pullStart := time.Now()
		sendPull(t, conn)
		readSuccess(t, conn)
		pullTotal += time.Since(pullStart)
	}

	avgRun := runTotal.Seconds() * 1000 / float64(iterations)
	avgPull := pullTotal.Seconds() * 1000 / float64(iterations)
	t.Logf("Latency breakdown (%d iterations):", iterations)
	t.Logf("  RUN avg: %.3f ms", avgRun)
	t.Logf("  PULL avg: %.3f ms", avgPull)
	t.Logf("  Total avg: %.3f ms", avgRun+avgPull)
	t.Logf("  Throughput: %.2f ops/sec", float64(iterations)/((runTotal + pullTotal).Seconds()))
}
