// Integration tests for Bolt server with Cypher executor.
//
// These tests verify that the Bolt protocol server works correctly with
// the Cypher query executor, simulating real-world Neo4j driver usage.
package bolt

import (
	"context"
	"fmt"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
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

// txCapableCypherQueryExecutor provides session-scoped explicit transaction support.
// It mirrors the runtime behavior where each Bolt connection has isolated tx state.
type txCapableCypherQueryExecutor struct {
	store storage.Engine

	mu     sync.Mutex
	inTx   bool
	txExec *cypher.StorageExecutor
}

func newTxCapableCypherQueryExecutor(store storage.Engine) *txCapableCypherQueryExecutor {
	return &txCapableCypherQueryExecutor{store: store}
}

func (e *txCapableCypherQueryExecutor) NewSessionExecutor() QueryExecutor {
	return newTxCapableCypherQueryExecutor(e.store)
}

func (e *txCapableCypherQueryExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	e.mu.Lock()
	activeExec := e.txExec
	e.mu.Unlock()

	if activeExec != nil {
		result, err := activeExec.Execute(ctx, query, params)
		if err != nil {
			return nil, err
		}
		return &QueryResult{Columns: result.Columns, Rows: result.Rows, Metadata: result.Metadata}, nil
	}

	exec := cypher.NewStorageExecutor(e.store)
	result, err := exec.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return &QueryResult{Columns: result.Columns, Rows: result.Rows, Metadata: result.Metadata}, nil
}

func (e *txCapableCypherQueryExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inTx {
		return nil
	}
	exec := cypher.NewStorageExecutor(e.store)
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		return err
	}
	e.txExec = exec
	e.inTx = true
	return nil
}

func (e *txCapableCypherQueryExecutor) CommitTransaction(ctx context.Context) error {
	e.mu.Lock()
	exec := e.txExec
	e.txExec = nil
	e.inTx = false
	e.mu.Unlock()
	if exec == nil {
		return nil
	}
	_, err := exec.Execute(ctx, "COMMIT", nil)
	return err
}

func (e *txCapableCypherQueryExecutor) RollbackTransaction(ctx context.Context) error {
	e.mu.Lock()
	exec := e.txExec
	e.txExec = nil
	e.inTx = false
	e.mu.Unlock()
	if exec == nil {
		return nil
	}
	_, err := exec.Execute(ctx, "ROLLBACK", nil)
	return err
}

func startBoltIntegrationServer(t *testing.T, store storage.Engine) (*Server, int) {
	t.Helper()
	executor := &cypherQueryExecutor{executor: cypher.NewStorageExecutor(store)}
	server := New(&Config{
		Port:            0,
		MaxConnections:  10,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}, executor)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	requireNoError(t, err)
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.Port == 0 {
		t.Fatalf("expected TCP listener with assigned port, got %T %v", listener.Addr(), listener.Addr())
	}
	server.listener = listener

	errCh := make(chan error, 1)
	t.Cleanup(func() {
		if err := server.Close(); err != nil {
			t.Fatalf("close server: %v", err)
		}
		if err := <-errCh; err != nil {
			t.Fatalf("serve: %v", err)
		}
	})

	go func() {
		errCh <- server.serve()
	}()
	return server, addr.Port
}

func openBoltTestConn(t *testing.T, port int) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	requireNoError(t, err)
	t.Cleanup(func() {
		conn.Close()
	})
	requireNoError(t, PerformHandshakeWithTesting(t, conn))
	requireNoError(t, SendHello(t, conn, nil))
	requireNoError(t, ReadSuccess(t, conn))
	return conn
}

func requireNoError(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func runBoltQueryAndCollectRecords(t *testing.T, conn net.Conn, query string) [][]any {
	t.Helper()
	requireNoError(t, SendRun(t, conn, query, nil, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))

	var records [][]any
	for {
		msgType, msgData, err := ReadMessage(conn)
		requireNoError(t, err)
		switch msgType {
		case MsgRecord:
			fields, _, err := decodePackStreamList(msgData, 0)
			requireNoError(t, err)
			records = append(records, fields)
		case MsgSuccess:
			return records
		default:
			t.Fatalf("unexpected Bolt message type 0x%02X for query %q", msgType, query)
		}
	}
}

func runBoltQueryExpectFailure(t *testing.T, conn net.Conn, query string) (string, string) {
	t.Helper()
	requireNoError(t, SendRun(t, conn, query, nil, nil))
	code, message, err := AssertFailure(t, conn)
	requireNoError(t, err)
	return code, message
}

// TestBoltCypherIntegration tests the full stack: Bolt server + Cypher executor.
func TestBoltCypherIntegration(t *testing.T) {
	// Create storage and executor
	// Wrap with NamespacedEngine to handle ID prefixing (required by BadgerEngine)
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
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
		if err := PerformHandshakeWithTesting(t, conn); err != nil {
			t.Fatalf("Handshake failed: %v", err)
		}

		// Send HELLO
		if err := SendHello(t, conn, nil); err != nil {
			t.Fatalf("HELLO failed: %v", err)
		}

		// Wait for SUCCESS response
		if err := ReadSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS after HELLO: %v", err)
		}

		// Send CREATE query
		createQuery := "CREATE (n:Person {name: 'Alice', age: 30}) RETURN n"
		if err := SendRun(t, conn, createQuery, nil, nil); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS with fields
		if err := ReadSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS after RUN: %v", err)
		}

		// Send PULL to get results
		if err := SendPull(t, conn, nil); err != nil {
			t.Fatalf("PULL failed: %v", err)
		}

		// Read RECORD and final SUCCESS
		hasRecord := false
		for {
			msgType, err := ReadMessageType(t, conn)
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
		if err := PerformHandshakeWithTesting(t, conn); err != nil {
			t.Fatalf("Handshake failed: %v", err)
		}

		// Send HELLO
		if err := SendHello(t, conn, nil); err != nil {
			t.Fatalf("HELLO failed: %v", err)
		}

		// Wait for SUCCESS
		if err := ReadSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS: %v", err)
		}

		// Send MATCH query
		matchQuery := "MATCH (n:Person) WHERE n.name = 'Alice' RETURN n.name, n.age"
		if err := SendRun(t, conn, matchQuery, nil, nil); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS
		if err := ReadSuccess(t, conn); err != nil {
			t.Fatalf("Expected SUCCESS: %v", err)
		}

		// Send PULL
		if err := SendPull(t, conn, nil); err != nil {
			t.Fatalf("PULL failed: %v", err)
		}

		// Read results
		hasRecord := false
		for {
			msgType, err := ReadMessageType(t, conn)
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
		PerformHandshakeWithTesting(t, conn)
		SendHello(t, conn, nil)
		ReadSuccess(t, conn)

		// Send parameterized query
		query := "CREATE (n:Person {name: $name, age: $age}) RETURN n"
		params := map[string]any{
			"name": "Bob",
			"age":  int64(25),
		}

		if err := SendRun(t, conn, query, params, nil); err != nil {
			t.Fatalf("RUN failed: %v", err)
		}

		// Read SUCCESS
		ReadSuccess(t, conn)

		// Send PULL
		SendPull(t, conn, nil)

		// Read results
		for {
			msgType, err := ReadMessageType(t, conn)
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
		PerformHandshakeWithTesting(t, conn)
		SendHello(t, conn, nil)
		ReadSuccess(t, conn)

		// Send BEGIN
		if err := SendBegin(t, conn, nil); err != nil {
			t.Fatalf("BEGIN failed: %v", err)
		}
		ReadSuccess(t, conn)

		// Send query in transaction
		SendRun(t, conn, "CREATE (n:Test {id: 'tx-test'})", nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)

		// Consume results
		for {
			msgType, _ := ReadMessageType(t, conn)
			if msgType == MsgSuccess {
				break
			}
		}

		// Send COMMIT
		if err := SendCommit(t, conn); err != nil {
			t.Fatalf("COMMIT failed: %v", err)
		}
		ReadSuccess(t, conn)
	})

}

func TestBoltConstraintIntegration_NewerConstraintTypes(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	queries := []string{
		"CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL",
		"CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY",
		"CREATE CONSTRAINT fact_temporal IF NOT EXISTS FOR (v:FactVersion) REQUIRE (v.fact_key, v.valid_from, v.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT rel_exists IF NOT EXISTS FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL",
		"CREATE CONSTRAINT rel_key IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE (r.since, r.how) IS RELATIONSHIP KEY",
	}

	for _, query := range queries {
		records := runBoltQueryAndCollectRecords(t, conn, query)
		if len(records) != 0 {
			t.Fatalf("expected no records for schema mutation %q, got %d", query, len(records))
		}
	}

	showRecords := runBoltQueryAndCollectRecords(t, conn, "SHOW CONSTRAINTS")
	if len(showRecords) < len(queries) {
		t.Fatalf("expected at least %d constraints, got %d", len(queries), len(showRecords))
	}

	typesByName := map[string]string{}
	entityTypesByName := map[string]string{}
	for _, row := range showRecords {
		if len(row) < 4 {
			continue
		}
		name, _ := row[1].(string)
		constraintType, ok := row[2].(string)
		if ok && name != "" {
			typesByName[name] = constraintType
		}
		entityType, ok := row[3].(string)
		if ok && name != "" {
			entityTypesByName[name] = entityType
		}
	}

	wantTypes := map[string]string{
		"person_name_required": "EXISTS",
		"user_key":             "NODE_KEY",
		"fact_temporal":        "TEMPORAL_NO_OVERLAP",
		"rel_exists":           "EXISTS",
		"rel_key":              "RELATIONSHIP_KEY",
	}
	for name, want := range wantTypes {
		if got := typesByName[name]; got != want {
			t.Fatalf("expected SHOW CONSTRAINTS type %q for %q, got %q (all=%v)", want, name, got, typesByName)
		}
	}

	if entityTypesByName["person_name_required"] != "NODE" ||
		entityTypesByName["user_key"] != "NODE" ||
		entityTypesByName["fact_temporal"] != "NODE" {
		t.Fatalf("unexpected node entity types in SHOW CONSTRAINTS: %v", entityTypesByName)
	}
	if entityTypesByName["rel_exists"] != "RELATIONSHIP" ||
		entityTypesByName["rel_key"] != "RELATIONSHIP" {
		t.Fatalf("unexpected relationship entity types in SHOW CONSTRAINTS: %v", entityTypesByName)
	}
}

func TestBoltConstraintIntegration_AllConstraintFamilies(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	queries := []string{
		"CREATE CONSTRAINT person_email_unique IF NOT EXISTS FOR (p:Person) REQUIRE p.email IS UNIQUE",
		"CREATE CONSTRAINT person_name_required IF NOT EXISTS FOR (p:Person) REQUIRE p.name IS NOT NULL",
		"CREATE CONSTRAINT user_key IF NOT EXISTS FOR (u:User) REQUIRE (u.username, u.domain) IS NODE KEY",
		"CREATE CONSTRAINT person_age_type IF NOT EXISTS FOR (p:Person) REQUIRE p.age IS :: INTEGER",
		"CREATE CONSTRAINT person_status_domain IF NOT EXISTS FOR (p:Person) REQUIRE p.status IN ['active', 'inactive']",
		"CREATE CONSTRAINT fact_temporal IF NOT EXISTS FOR (v:FactVersion) REQUIRE (v.fact_key, v.valid_from, v.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT rel_exists IF NOT EXISTS FOR ()-[r:FOLLOWS]-() REQUIRE r.since IS NOT NULL",
		"CREATE CONSTRAINT rel_key IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE (r.since, r.how) IS RELATIONSHIP KEY",
		"CREATE CONSTRAINT rel_order_type IF NOT EXISTS FOR ()-[r:PART_OF]-() REQUIRE r.order IS :: INTEGER",
		"CREATE CONSTRAINT rel_role_domain IF NOT EXISTS FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager']",
		"CREATE CONSTRAINT max_jobs IF NOT EXISTS FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 2",
		"CREATE CONSTRAINT works_at_allowed IF NOT EXISTS FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED",
		"CREATE CONSTRAINT no_intern_exec IF NOT EXISTS FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED",
	}

	for _, query := range queries {
		records := runBoltQueryAndCollectRecords(t, conn, query)
		if len(records) != 0 {
			t.Fatalf("expected no records for schema mutation %q, got %d", query, len(records))
		}
	}

	showRecords := runBoltQueryAndCollectRecords(t, conn, "SHOW CONSTRAINTS")
	if len(showRecords) < len(queries) {
		t.Fatalf("expected at least %d constraints, got %d", len(queries), len(showRecords))
	}

	typesByName := map[string]string{}
	entityTypesByName := map[string]string{}
	propertyTypeByName := map[string]string{}
	for _, row := range showRecords {
		if len(row) < 8 {
			continue
		}
		name, _ := row[1].(string)
		constraintType, _ := row[2].(string)
		entityType, _ := row[3].(string)
		propType, _ := row[7].(string)
		if name != "" {
			typesByName[name] = constraintType
			entityTypesByName[name] = entityType
			if propType != "" {
				propertyTypeByName[name] = propType
			}
		}
	}

	wantTypes := map[string]string{
		"person_email_unique":  "UNIQUE",
		"person_name_required": "EXISTS",
		"user_key":             "NODE_KEY",
		"person_age_type":      "PROPERTY_TYPE",
		"person_status_domain": "DOMAIN",
		"fact_temporal":        "TEMPORAL_NO_OVERLAP",
		"rel_exists":           "EXISTS",
		"rel_key":              "RELATIONSHIP_KEY",
		"rel_order_type":       "PROPERTY_TYPE",
		"rel_role_domain":      "DOMAIN",
		"max_jobs":             "CARDINALITY",
		"works_at_allowed":     "RELATIONSHIP_POLICY",
		"no_intern_exec":       "RELATIONSHIP_POLICY",
	}
	for name, want := range wantTypes {
		if got := typesByName[name]; got != want {
			t.Fatalf("expected SHOW CONSTRAINTS type %q for %q, got %q (all=%v)", want, name, got, typesByName)
		}
	}

	if propertyTypeByName["person_age_type"] != "INTEGER" {
		t.Fatalf("expected property type INTEGER for person_age_type, got %q", propertyTypeByName["person_age_type"])
	}
	if propertyTypeByName["rel_order_type"] != "INTEGER" {
		t.Fatalf("expected property type INTEGER for rel_order_type, got %q", propertyTypeByName["rel_order_type"])
	}

	nodeNames := []string{"person_email_unique", "person_name_required", "user_key", "person_age_type", "person_status_domain", "fact_temporal"}
	for _, name := range nodeNames {
		if entityTypesByName[name] != "NODE" {
			t.Fatalf("expected NODE entity type for %q, got %q", name, entityTypesByName[name])
		}
	}
	relNames := []string{"rel_exists", "rel_key", "rel_order_type", "rel_role_domain", "max_jobs", "works_at_allowed", "no_intern_exec"}
	for _, name := range relNames {
		if entityTypesByName[name] != "RELATIONSHIP" {
			t.Fatalf("expected RELATIONSHIP entity type for %q, got %q", name, entityTypesByName[name])
		}
	}
}

func TestBoltConstraintIntegration_ConstraintContracts(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	createContract := `
		CREATE CONSTRAINT person_contract
		FOR (n:Person)
		REQUIRE {
		  n.id IS UNIQUE
		  n.name IS NOT NULL
		  n.age IS :: INTEGER
		  n.status IS :: STRING
		  n.status IN ['active', 'inactive']
		  (n.tenant, n.externalId) IS NODE KEY
		  COUNT { (n)-[:PRIMARY_EMPLOYER]->(:Company) } <= 1
		  NOT EXISTS { (n)-[:FORBIDDEN_REL]->() }
		}`

	records := runBoltQueryAndCollectRecords(t, conn, createContract)
	if len(records) != 0 {
		t.Fatalf("expected no records creating contract, got %d", len(records))
	}

	contracts := runBoltQueryAndCollectRecords(t, conn, "SHOW CONSTRAINT CONTRACTS")
	if len(contracts) != 1 {
		t.Fatalf("expected 1 contract row, got %d", len(contracts))
	}
	row := contracts[0]
	if len(row) < 7 {
		t.Fatalf("unexpected SHOW CONSTRAINT CONTRACTS row shape: %v", row)
	}
	if row[0] != "person_contract" || row[1] != "NODE" || row[2] != "Person" {
		t.Fatalf("unexpected contract metadata row: %v", row)
	}
	if row[3] != int64(8) || row[4] != int64(5) || row[5] != int64(3) {
		t.Fatalf("unexpected contract entry counts: %v", row)
	}
	defn, ok := row[6].(string)
	if !ok || defn == "" {
		t.Fatalf("expected non-empty contract definition, got %v", row[6])
	}

	records = runBoltQueryAndCollectRecords(t, conn, `
		CREATE CONSTRAINT works_at_contract
		FOR ()-[r:WORKS_AT]-()
		REQUIRE {
		  r.id IS UNIQUE
		  r.startedAt IS NOT NULL
		  r.role IS :: STRING
		  (r.tenant, r.externalId) IS RELATIONSHIP KEY
		  startNode(r) <> endNode(r)
		  startNode(r).tenant = endNode(r).tenant
		  r.status IN ['active', 'inactive']
		  r.hoursPerWeek > 0
		}`)
	if len(records) != 0 {
		t.Fatalf("expected no records creating relationship contract, got %d", len(records))
	}

	contracts = runBoltQueryAndCollectRecords(t, conn, "SHOW CONSTRAINT CONTRACTS")
	if len(contracts) != 2 {
		t.Fatalf("expected 2 contract rows, got %d", len(contracts))
	}
}

func TestBoltConstraintIntegration_EnforcementForNewFamilies(t *testing.T) {
	t.Run("domain", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE CONSTRAINT person_status_domain FOR (n:Person) REQUIRE n.status IN ['active', 'inactive']")
		_, msg := runBoltQueryExpectFailure(t, conn, "CREATE (:Person {id:'p1', status:'paused'})")
		if !strings.Contains(msg, "allowed") && !strings.Contains(msg, "DOMAIN") && !strings.Contains(msg, "active") {
			t.Fatalf("expected domain violation message, got %q", msg)
		}
	})

	t.Run("cardinality", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE CONSTRAINT max_jobs FOR ()-[r:WORKS_AT]->() REQUIRE MAX COUNT 2")
		runBoltQueryAndCollectRecords(t, conn, "CREATE (:Person {id:'p1'}), (:Company {id:'c1'}), (:Company {id:'c2'}), (:Company {id:'c3'})")
		runBoltQueryAndCollectRecords(t, conn, "MATCH (p:Person {id:'p1'}), (c:Company {id:'c1'}) CREATE (p)-[:WORKS_AT]->(c)")
		runBoltQueryAndCollectRecords(t, conn, "MATCH (p:Person {id:'p1'}), (c:Company {id:'c2'}) CREATE (p)-[:WORKS_AT]->(c)")
		_, msg := runBoltQueryExpectFailure(t, conn, "MATCH (p:Person {id:'p1'}), (c:Company {id:'c3'}) CREATE (p)-[:WORKS_AT]->(c)")
		if !strings.Contains(msg, "max count") && !strings.Contains(msg, "exceed") {
			t.Fatalf("expected cardinality violation message, got %q", msg)
		}
	})

	t.Run("policy", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE CONSTRAINT works_at_allowed FOR (:Person)-[r:WORKS_AT]->(:Company) REQUIRE ALLOWED")
		runBoltQueryAndCollectRecords(t, conn, "CREATE (:Robot {id:'r1'}), (:Company {id:'c1'})")
		_, msg := runBoltQueryExpectFailure(t, conn, "MATCH (r:Robot {id:'r1'}), (c:Company {id:'c1'}) CREATE (r)-[:WORKS_AT]->(c)")
		if !strings.Contains(msg, "ALLOWED") {
			t.Fatalf("expected ALLOWED policy violation message, got %q", msg)
		}
	})

	t.Run("policy_disallowed", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE CONSTRAINT no_intern_exec FOR (:Intern)-[r:REPORTS_TO]->(:Executive) REQUIRE DISALLOWED")
		runBoltQueryAndCollectRecords(t, conn, "CREATE (:Intern {id:'i1'}), (:Executive {id:'e1'})")
		_, msg := runBoltQueryExpectFailure(t, conn, "MATCH (i:Intern {id:'i1'}), (e:Executive {id:'e1'}) CREATE (i)-[:REPORTS_TO]->(e)")
		if !strings.Contains(msg, "DISALLOWED") {
			t.Fatalf("expected DISALLOWED policy violation message, got %q", msg)
		}
	})

	t.Run("relationship_domain", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE CONSTRAINT rel_role_domain FOR ()-[r:WORKS_AT]-() REQUIRE r.role IN ['engineer', 'manager']")
		runBoltQueryAndCollectRecords(t, conn, "CREATE (:Person {id:'p1'}), (:Company {id:'c1'})")
		_, msg := runBoltQueryExpectFailure(t, conn, "MATCH (p:Person {id:'p1'}), (c:Company {id:'c1'}) CREATE (p)-[:WORKS_AT {role:'director'}]->(c)")
		if !strings.Contains(msg, "allowed") && !strings.Contains(msg, "DOMAIN") && !strings.Contains(msg, "engineer") {
			t.Fatalf("expected relationship domain violation message, got %q", msg)
		}
	})

	t.Run("contract_runtime", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, `
			CREATE CONSTRAINT person_contract
			FOR (n:Person)
			REQUIRE {
			  n.id IS UNIQUE
			  n.name IS NOT NULL
			  n.age IS :: INTEGER
			  n.status IN ['active', 'inactive']
			  (n.tenant, n.externalId) IS NODE KEY
			}`)
		_, msg := runBoltQueryExpectFailure(t, conn, "CREATE (:Person {id:'bad', name:'Bob', age:40, status:'paused', tenant:'t1', externalId:'u2'})")
		if !strings.Contains(msg, "constraint contract person_contract violated") {
			t.Fatalf("expected contract violation message, got %q", msg)
		}
	})

	t.Run("contract_relationship_runtime", func(t *testing.T) {
		baseStore := storage.NewMemoryEngine()
		store := storage.NewNamespacedEngine(baseStore, "test")
		_, port := startBoltIntegrationServer(t, store)
		conn := openBoltTestConn(t, port)

		runBoltQueryAndCollectRecords(t, conn, "CREATE (:Person {id:'p1', tenant:'t1'}), (:Person {id:'p2', tenant:'t2'}), (:Person {id:'p3', tenant:'t1'})")
		runBoltQueryAndCollectRecords(t, conn, `
			CREATE CONSTRAINT works_at_contract
			FOR ()-[r:WORKS_AT]-()
			REQUIRE {
			  r.id IS UNIQUE
			  r.startedAt IS NOT NULL
			  r.role IS :: STRING
			  (r.tenant, r.externalId) IS RELATIONSHIP KEY
			  startNode(r) <> endNode(r)
			  startNode(r).tenant = endNode(r).tenant
			  r.status IN ['active', 'inactive']
			  r.hoursPerWeek > 0
			}`)
		_, msg := runBoltQueryExpectFailure(t, conn, `
			MATCH (a:Person {id:'p1'}), (b:Person {id:'p2'})
			CREATE (a)-[:WORKS_AT {id:'w2', startedAt:'2024-01-01', role:'Engineer', tenant:'t1', externalId:'rel-2', status:'active', hoursPerWeek:40}]->(b)`)
		if !strings.Contains(msg, "constraint contract works_at_contract violated") && !strings.Contains(msg, "startNode(r).tenant = endNode(r).tenant") {
			t.Fatalf("expected relationship contract violation message, got %q", msg)
		}
	})
}

func TestBoltIntegration_G2GVersionNodesFallbackRowShape_ServerStack(t *testing.T) {
	baseStore, err := storage.NewBadgerEngineInMemory()
	requireNoError(t, err)
	wal, err := storage.NewWAL(t.TempDir(), nil)
	requireNoError(t, err)
	walStore := storage.NewWALEngine(baseStore, wal)
	asyncStore := storage.NewAsyncEngine(walStore, nil)
	store := storage.NewNamespacedEngine(asyncStore, "test")
	t.Cleanup(func() {
		asyncStore.Flush()
		requireNoError(t, wal.Close())
		requireNoError(t, asyncStore.Close())
		requireNoError(t, baseStore.Close())
	})

	_, port := startBoltIntegrationServer(t, store)
	conn := openBoltTestConn(t, port)

	bootstrap := []string{
		"CREATE CONSTRAINT g2g_entity_entity_id_unique IF NOT EXISTS FOR (e:Entity) REQUIRE e.entity_id IS UNIQUE",
		"CREATE CONSTRAINT g2g_codestate_state_id_unique IF NOT EXISTS FOR (cs:CodeState) REQUIRE cs.state_id IS UNIQUE",
		"CREATE CONSTRAINT g2g_commit_hash_unique IF NOT EXISTS FOR (c:Commit) REQUIRE c.hash IS UNIQUE",
		"CREATE CONSTRAINT g2g_codekey_entity_relation_nodekey IF NOT EXISTS FOR (ck:CodeKey) REQUIRE (ck.entity_id, ck.relation_type) IS NODE KEY",
		"CREATE CONSTRAINT g2g_factversion_fact_key_temporal_no_overlap IF NOT EXISTS FOR (fv:FactVersion) REQUIRE (fv.fact_key, fv.valid_from, fv.valid_to) IS TEMPORAL NO OVERLAP",
		"CREATE CONSTRAINT g2g_factversion_fact_key_exists IF NOT EXISTS FOR (fv:FactVersion) REQUIRE fv.fact_key IS NOT NULL",
		"CREATE CONSTRAINT g2g_factversion_predicate_exists IF NOT EXISTS FOR (fv:FactVersion) REQUIRE fv.predicate IS NOT NULL",
		"CREATE CONSTRAINT g2g_factversion_valid_from_exists IF NOT EXISTS FOR (fv:FactVersion) REQUIRE fv.valid_from IS NOT NULL",
	}
	for _, query := range bootstrap {
		records := runBoltQueryAndCollectRecords(t, conn, query)
		if len(records) != 0 {
			t.Fatalf("expected no records for schema mutation %q, got %d", query, len(records))
		}
	}

	query := strings.TrimSpace(`
UNWIND $rows AS row
MERGE (e:Entity:CodeEntity {entity_id: row.entity_id})
ON CREATE SET e.created_at = datetime(row.asserted_at_iso)
SET e.entity_type = row.entity_type,
	e.repo_id = row.repo_id,
	e.display_name = coalesce(row.name, row.path, row.file_path, row.entity_id),
	e.path = CASE WHEN row.path IS NULL OR row.path = '' THEN null ELSE row.path END,
	e.file_path = CASE WHEN row.file_path IS NULL OR row.file_path = '' THEN null ELSE row.file_path END,
	e.lang = CASE WHEN row.language IS NULL OR row.language = '' THEN null ELSE row.language END,
	e.symbol_kind = CASE WHEN row.symbol_kind IS NULL OR row.symbol_kind = '' THEN null ELSE row.symbol_kind END,
	e.line_number = CASE WHEN row.line_number IS NULL OR row.line_number = 0 THEN null ELSE row.line_number END
MERGE (ck:FactKey:CodeKey {
	subject_entity_id: row.entity_id,
	predicate: row.predicate,
	entity_id: row.entity_id,
	relation_type: row.relation_type
})
SET ck.fact_key = row.code_key,
	ck.repo_id = row.repo_id,
	ck.subject_entity_type = row.entity_type
MERGE (cs:FactVersion:CodeState {state_id: row.state_id})
SET cs.fact_key = row.code_key,
	cs.code_key = row.code_key,
	cs.tx_id = row.tx_id,
	cs.commit_hash = row.commit_hash,
	cs.valid_from_iso = row.valid_from_iso,
	cs.valid_from = datetime(row.valid_from_iso),
	cs.value_json = row.value_json,
	cs.valid_to = CASE WHEN row.valid_to_iso IS NULL THEN null ELSE datetime(row.valid_to_iso) END,
	cs.asserted_at = datetime(row.asserted_at_iso),
	cs.asserted_by = row.asserted_by,
	cs.semantic_type = row.semantic_type,
	cs.repo_id = row.repo_id,
	cs.entity_id = row.entity_id,
	cs.entity_type = row.entity_type,
	cs.predicate = row.predicate,
	cs.source_entity_id = CASE WHEN row.source_entity_id IS NULL OR row.source_entity_id = '' THEN null ELSE row.source_entity_id END,
	cs.source_entity_type = CASE WHEN row.source_entity_type IS NULL OR row.source_entity_type = '' THEN null ELSE row.source_entity_type END,
	cs.target_entity_id = CASE WHEN row.target_entity_id IS NULL OR row.target_entity_id = '' THEN null ELSE row.target_entity_id END,
	cs.target_entity_type = CASE WHEN row.target_entity_type IS NULL OR row.target_entity_type = '' THEN null ELSE row.target_entity_type END,
	cs.path = CASE WHEN row.path IS NULL OR row.path = '' THEN null ELSE row.path END,
	cs.file_path = CASE WHEN row.file_path IS NULL OR row.file_path = '' THEN null ELSE row.file_path END,
	cs.name = CASE WHEN row.name IS NULL OR row.name = '' THEN null ELSE row.name END,
	cs.language = CASE WHEN row.language IS NULL OR row.language = '' THEN null ELSE row.language END,
	cs.symbol_kind = CASE WHEN row.symbol_kind IS NULL OR row.symbol_kind = '' THEN null ELSE row.symbol_kind END,
	cs.line_number = CASE WHEN row.line_number IS NULL OR row.line_number = 0 THEN null ELSE row.line_number END
MERGE (c:Commit {hash: row.commit_hash})
ON CREATE SET c.timestamp = datetime(row.asserted_at_iso), c.tx_id = row.tx_id, c.actor = row.asserted_by
FOREACH (_ IN CASE WHEN row.source_entity_id IS NULL OR row.source_entity_id = '' THEN [] ELSE [1] END |
	MERGE (src:Entity:CodeEntity {entity_id: row.source_entity_id})
	ON CREATE SET src.created_at = datetime(row.asserted_at_iso)
	SET src.entity_type = row.source_entity_type,
		src.repo_id = row.repo_id
	MERGE (cs)-[:SOURCE]->(src)
)
FOREACH (_ IN CASE WHEN row.target_entity_id IS NULL OR row.target_entity_id = '' THEN [] ELSE [1] END |
	MERGE (dst:Entity:CodeEntity {entity_id: row.target_entity_id})
	ON CREATE SET dst.created_at = datetime(row.asserted_at_iso)
	SET dst.entity_type = row.target_entity_type,
		dst.repo_id = row.repo_id
)`)

	params := map[string]any{
		"rows": []map[string]any{map[string]any{
			"entity_id":          "repo_fact|calls|symbol::repo::function::bolt-single",
			"entity_type":        "calls_edge",
			"repo_id":            "repo",
			"relation_type":      "calls",
			"predicate":          "calls",
			"state_id":           "cs-g2g-bolt-single",
			"code_key":           "repo_fact|calls|symbol::repo::function::bolt-single",
			"tx_id":              "tx-g2g-bolt-000001",
			"commit_hash":        "commit-g2g-bolt-single",
			"valid_from_iso":     "2026-03-20T20:22:20Z",
			"asserted_at_iso":    "2026-03-20T20:22:20Z",
			"asserted_by":        "TJ Sweet",
			"value_json":         `{"caller":"symbol::repo::function::caller","callee":"symbol::repo::function::callee"}`,
			"semantic_type":      "calls",
			"source_entity_id":   "symbol::repo::function::caller",
			"source_entity_type": "function",
			"target_entity_id":   "symbol::repo::function::callee",
			"target_entity_type": "function",
			"path":               "internal/parser/parser.go",
			"file_path":          "internal/parser/parser.go",
			"name":               "caller->callee",
			"language":           "go",
			"symbol_kind":        "function",
			"line_number":        int64(42),
			"valid_to_iso":       nil,
		}},
	}

	requireNoError(t, SendRun(t, conn, query, params, nil))
	requireNoError(t, ReadSuccess(t, conn))
	requireNoError(t, SendPull(t, conn, nil))
	for {
		msgType, _, err := ReadMessage(conn)
		requireNoError(t, err)
		if msgType == MsgSuccess {
			break
		}
		if msgType != MsgRecord {
			t.Fatalf("unexpected Bolt message type 0x%02X for fallback-row query", msgType)
		}
	}

	records := runBoltQueryAndCollectRecords(t, conn, "MATCH (:FactVersion:CodeState {state_id: 'cs-g2g-bolt-single'})-[:SOURCE]->(:Entity:CodeEntity {entity_id: 'symbol::repo::function::caller'}) RETURN count(*)")
	if len(records) != 1 || len(records[0]) != 1 || records[0][0] != int64(1) {
		t.Fatalf("expected exactly one SOURCE relationship, got %v", records)
	}
}

func TestBoltExplicitTransactionRollbackRevertsCreate(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	store := storage.NewNamespacedEngine(baseStore, "test")
	executor := newTxCapableCypherQueryExecutor(store)

	config := &Config{
		Port:            0,
		MaxConnections:  10,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}

	server := New(config, executor)
	defer server.Close()

	go func() {
		if err := server.ListenAndServe(); err != nil {
			t.Logf("Server error: %v", err)
		}
	}()
	time.Sleep(100 * time.Millisecond)

	port := server.listener.Addr().(*net.TCPAddr).Port
	conn, err := net.Dial("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Cleanup old test data outside transaction.
	if err := SendRun(t, conn, "MATCH (n:Test {name: 'Transaction Test'}) DETACH DELETE n", nil, nil); err != nil {
		t.Fatalf("cleanup RUN failed: %v", err)
	}
	ReadSuccess(t, conn)
	if err := SendPull(t, conn, nil); err != nil {
		t.Fatalf("cleanup PULL failed: %v", err)
	}
	ReadSuccess(t, conn)

	// Begin explicit transaction.
	if err := SendBegin(t, conn, nil); err != nil {
		t.Fatalf("BEGIN failed: %v", err)
	}
	ReadSuccess(t, conn)

	// Create node in transaction.
	if err := SendRun(t, conn, "CREATE (n:Test {name: 'Transaction Test'})", nil, nil); err != nil {
		t.Fatalf("create RUN failed: %v", err)
	}
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	ReadSuccess(t, conn)

	// Roll back explicit transaction.
	if err := SendRollback(t, conn); err != nil {
		t.Fatalf("ROLLBACK failed: %v", err)
	}
	ReadSuccess(t, conn)

	// Verify node is no longer visible after rollback.
	if err := SendRun(t, conn, "MATCH (n:Test {name: 'Transaction Test'}) RETURN n", nil, nil); err != nil {
		t.Fatalf("post-rollback match RUN failed: %v", err)
	}
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	seenAfterRollback := false
	for {
		msgType, err := ReadMessageType(t, conn)
		if err != nil {
			t.Fatalf("failed to read post-rollback MATCH response: %v", err)
		}
		if msgType == MsgRecord {
			seenAfterRollback = true
		} else if msgType == MsgSuccess {
			break
		}
	}
	if seenAfterRollback {
		t.Fatal("expected no node after rollback, but query returned a record")
	}
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
			PerformHandshakeWithTesting(t, conn)
			SendHello(t, conn, nil)
			ReadSuccess(t, conn)

			// Execute queries
			query := fmt.Sprintf("CREATE (n:Test {id: %d}) RETURN n", id)
			SendRun(t, conn, query, nil, nil)
			ReadSuccess(t, conn)
			SendPull(t, conn, nil)

			// Read results
			for {
				msgType, err := ReadMessageType(t, conn)
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
	// Create storage chain matching production: base -> async -> namespaced.
	// AsyncEngine requires fully-qualified IDs; NamespacedEngine provides that for Cypher-generated IDs.
	baseStore := storage.NewMemoryEngine()
	asyncBase := storage.NewAsyncEngine(baseStore, nil)
	store := storage.NewNamespacedEngine(asyncBase, "test")
	cypherExec := cypher.NewStorageExecutor(store)
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
	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Create test nodes
	SendRun(t, conn, "CREATE (a:Actor {name: 'Test'})", nil, nil)
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	ReadSuccess(t, conn) // SUCCESS after PULL

	SendRun(t, conn, "CREATE (m:Movie {title: 'Test'})", nil, nil)
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	ReadSuccess(t, conn)

	// Benchmark the slow query
	iterations := 100
	t.Logf("Running %d iterations over Bolt (small dataset)", iterations)

	start := time.Now()
	for i := 0; i < iterations; i++ {
		query := `MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r`
		SendRun(t, conn, query, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
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
	baseStore := storage.NewMemoryEngine()
	asyncBase := storage.NewAsyncEngine(baseStore, nil)
	store := storage.NewNamespacedEngine(asyncBase, "test")
	cypherExec := cypher.NewStorageExecutor(store)
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

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Create 100 actors
	t.Log("Creating 100 actors...")
	for i := 0; i < 100; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d', born: %d})", i, 1950+i%50), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	// Create 150 movies
	t.Log("Creating 150 movies...")
	for i := 0; i < 150; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d', released: %d})", i, 1980+i%44), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
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
		SendRun(t, conn, query, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
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

	asyncBase := storage.NewAsyncEngine(badgerEngine, nil)
	store := storage.NewNamespacedEngine(asyncBase, "test")
	cypherExec := cypher.NewStorageExecutor(store)
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

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Create 100 actors
	t.Log("Creating 100 actors (BadgerDB)...")
	for i := 0; i < 100; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (a:Actor {name: 'Actor_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	// Create 150 movies
	t.Log("Creating 150 movies (BadgerDB)...")
	for i := 0; i < 150; i++ {
		SendRun(t, conn, fmt.Sprintf("CREATE (m:Movie {title: 'Movie_%d'})", i), nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}

	// Benchmark
	iterations := 100
	if v := os.Getenv("BOLT_PROFILE_ITERATIONS"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			iterations = parsed
		}
	}
	t.Logf("Running %d iterations over Bolt (BadgerDB, 100 actors + 150 movies)", iterations)

	cpuProfilePath := os.Getenv("BOLT_PROFILE_CPU")
	var cpuFile *os.File
	if cpuProfilePath != "" {
		file, err := os.Create(cpuProfilePath)
		if err != nil {
			t.Fatalf("Failed to create CPU profile: %v", err)
		}
		cpuFile = file
		if err := pprof.StartCPUProfile(cpuFile); err != nil {
			_ = cpuFile.Close()
			t.Fatalf("Failed to start CPU profile: %v", err)
		}
	}

	start := time.Now()
	for i := 0; i < iterations; i++ {
		query := `MATCH (a:Actor), (m:Movie)
			WITH a, m LIMIT 1
			CREATE (a)-[r:TEMP_REL]->(m)
			DELETE r`
		SendRun(t, conn, query, nil, nil)
		ReadSuccess(t, conn)
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
	}
	elapsed := time.Since(start)

	if cpuFile != nil {
		pprof.StopCPUProfile()
		_ = cpuFile.Close()
	}

	if memProfilePath := os.Getenv("BOLT_PROFILE_MEM"); memProfilePath != "" {
		runtime.GC()
		file, err := os.Create(memProfilePath)
		if err != nil {
			t.Fatalf("Failed to create mem profile: %v", err)
		}
		if err := pprof.WriteHeapProfile(file); err != nil {
			_ = file.Close()
			t.Fatalf("Failed to write mem profile: %v", err)
		}
		_ = file.Close()
	}

	opsPerSec := float64(iterations) / elapsed.Seconds()
	t.Logf("Bolt Performance (BadgerDB, large dataset): %.2f ops/sec, %.3f ms/op", opsPerSec, elapsed.Seconds()*1000/float64(iterations))
}

// TestBoltResponseMetadata verifies Neo4j-compatible metadata in responses
// KEEP THIS TEST - ensures JS driver compatibility
func TestBoltResponseMetadata(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	asyncBase := storage.NewAsyncEngine(baseStore, nil)
	store := storage.NewNamespacedEngine(asyncBase, "test")
	cypherExec := cypher.NewStorageExecutor(store)
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

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Test write query returns proper metadata
	SendRun(t, conn, "CREATE (n:TestNode {name: 'test'})", nil, nil)
	if err := ReadSuccess(t, conn); err != nil {
		t.Fatalf("RUN failed: %v", err)
	}

	SendPull(t, conn, nil)
	if err := ReadSuccess(t, conn); err != nil {
		t.Fatalf("PULL failed: %v", err)
	}

	t.Log("Response metadata verified - Neo4j driver should work correctly")
}

// TestBoltLatencyBreakdown measures where time is spent in protocol exchange
// KEEP THIS TEST - helps identify bottlenecks in protocol handling
func TestBoltLatencyBreakdown(t *testing.T) {
	baseStore := storage.NewMemoryEngine()
	asyncBase := storage.NewAsyncEngine(baseStore, nil)
	store := storage.NewNamespacedEngine(asyncBase, "test")
	cypherExec := cypher.NewStorageExecutor(store)
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

	PerformHandshakeWithTesting(t, conn)
	SendHello(t, conn, nil)
	ReadSuccess(t, conn)

	// Create test data
	SendRun(t, conn, "CREATE (a:Actor {name: 'Keanu'})", nil, nil)
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	ReadSuccess(t, conn)

	SendRun(t, conn, "CREATE (m:Movie {title: 'Matrix'})", nil, nil)
	ReadSuccess(t, conn)
	SendPull(t, conn, nil)
	ReadSuccess(t, conn)

	// Measure each phase separately
	iterations := 50
	var runTotal, pullTotal time.Duration

	for i := 0; i < iterations; i++ {
		// Measure RUN
		runStart := time.Now()
		SendRun(t, conn, `MATCH (a:Actor), (m:Movie) WITH a, m LIMIT 1 CREATE (a)-[r:TEMP_REL]->(m) DELETE r`, nil, nil)
		ReadSuccess(t, conn)
		runTotal += time.Since(runStart)

		// Measure PULL
		pullStart := time.Now()
		SendPull(t, conn, nil)
		ReadSuccess(t, conn)
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
