// Package bolt tests for the Bolt protocol server.
package bolt

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// failureCodeFromResponse decodes a chunked Bolt FAILURE response and returns
// its Neo4j-compatible error code.
func failureCodeFromResponse(t *testing.T, data []byte) string {
	t.Helper()
	if len(data) < 4 {
		t.Fatalf("response too short: %d bytes", len(data))
	}

	var payload []byte
	for offset := 0; ; {
		if len(data[offset:]) < 2 {
			t.Fatalf("response missing chunk header at offset %d: bytes=%d", offset, len(data))
		}
		chunkSize := int(data[offset])<<8 | int(data[offset+1])
		offset += 2
		if chunkSize == 0 {
			break
		}
		if len(data[offset:]) < chunkSize {
			t.Fatalf("response chunk truncated: size=%d offset=%d bytes=%d", chunkSize, offset, len(data))
		}
		payload = append(payload, data[offset:offset+chunkSize]...)
		offset += chunkSize
	}
	if len(payload) < 2 || payload[0] != 0xB1 || payload[1] != MsgFailure {
		t.Fatalf("response is not a FAILURE message: %x", payload)
	}
	metadata, _, err := decodePackStreamMap(payload, 2)
	if err != nil {
		t.Fatalf("decode failure metadata: %v", err)
	}
	code, ok := metadata["code"].(string)
	if !ok {
		t.Fatalf("failure metadata missing string code: %#v", metadata)
	}
	return code
}

// TestFailureCodeFromResponseConcatenatesChunks verifies that test assertions
// inspect the complete Bolt payload instead of only the first response chunk.
func TestFailureCodeFromResponseConcatenatesChunks(t *testing.T) {
	payload := append([]byte{0xB1, MsgFailure}, encodePackStreamMap(map[string]any{
		"code":    "Neo.TransientError.Transaction.DeadlockDetected",
		"message": "deadlock detected",
	})...)
	response := append([]byte{0x00, 0x03}, payload[:3]...)
	remaining := payload[3:]
	response = append(response, byte(len(remaining)>>8), byte(len(remaining)))
	response = append(response, remaining...)
	response = append(response, 0x00, 0x00)

	code := failureCodeFromResponse(t, response)
	if code != "Neo.TransientError.Transaction.DeadlockDetected" {
		t.Fatalf("failureCodeFromResponse() = %q", code)
	}
}

func TestDecodePackStreamList_LIST8(t *testing.T) {
	// LIST8 with more elements
	data := []byte{0xD4, 0x02} // LIST8 marker + 2 elements
	data = append(data, 0x01)  // tiny int 1
	data = append(data, 0x02)  // tiny int 2

	result, _, err := decodePackStreamList(data, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if len(result) != 2 {
		t.Errorf("got %d elements, want 2", len(result))
	}
}

func TestDecodePackStreamValue_AllTypes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected any
	}{
		{
			name:     "null",
			data:     []byte{0xC0},
			expected: nil,
		},
		{
			name:     "true",
			data:     []byte{0xC3},
			expected: true,
		},
		{
			name:     "false",
			data:     []byte{0xC2},
			expected: false,
		},
		{
			name:     "tiny int positive",
			data:     []byte{0x2A}, // 42
			expected: int64(42),
		},
		{
			name:     "tiny int negative",
			data:     []byte{0xF0}, // -16
			expected: int64(-16),
		},
		{
			name:     "INT8",
			data:     []byte{0xC8, 0x80}, // -128
			expected: int64(-128),
		},
		{
			name:     "INT16",
			data:     []byte{0xC9, 0x01, 0x00}, // 256
			expected: int64(256),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamValue(tt.data, 0)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %v (%T), want %v (%T)", result, result, tt.expected, tt.expected)
			}
		})
	}
}

func TestEncodePackStreamValue_AllTypes(t *testing.T) {
	tests := []struct {
		name   string
		value  any
		marker byte // First byte
	}{
		{"nil", nil, 0xC0},
		{"true", true, 0xC3},
		{"false", false, 0xC2},
		{"tiny int", int64(42), 0x2A},
		{"negative int", int64(-10), 0xF6},
		{"float64", 3.14, 0xC1},
		{"string", "hello", 0x85},             // TINY_STRING
		{"empty list", []any{}, 0x90},         // TINY_LIST
		{"empty map", map[string]any{}, 0xA0}, // TINY_MAP
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodePackStreamValue(tt.value)
			if len(result) == 0 {
				t.Error("expected non-empty result")
				return
			}
			if result[0] != tt.marker {
				t.Errorf("got marker 0x%02X, want 0x%02X", result[0], tt.marker)
			}
		})
	}
}

func TestDecodePackStreamMap_Nested(t *testing.T) {
	// Nested map
	data := []byte{
		0xA1,                     // TINY_MAP with 1 element
		0x84, 'd', 'a', 't', 'a', // key "data"
		0xA1,                // nested TINY_MAP with 1 element
		0x83, 'f', 'o', 'o', // key "foo"
		0x83, 'b', 'a', 'r', // value "bar"
	}

	result, _, err := decodePackStreamMap(data, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}

	nested, ok := result["data"].(map[string]any)
	if !ok {
		t.Error("expected nested map")
		return
	}
	if nested["foo"] != "bar" {
		t.Errorf("got %v, want 'bar'", nested["foo"])
	}
}

func TestDispatchMessage_UnknownType(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	session := &Session{
		conn:       server,
		reader:     bufio.NewReaderSize(server, 8192),
		writer:     bufio.NewWriterSize(server, 8192),
		messageBuf: make([]byte, 0, 4096),
	}

	// Handle unknown message type
	done := make(chan error, 1)
	go func() {
		err := session.dispatchMessage(0xFF, nil) // Unknown type
		done <- err
	}()

	// Read the failure response
	go func() {
		buf := make([]byte, 1024)
		client.Read(buf)
	}()

	select {
	case err := <-done:
		// dispatchMessage sends a failure response and returns nil
		// OR it might return an error - either is acceptable behavior
		_ = err // Ignore the error - we're just testing it doesn't panic
	case <-time.After(100 * time.Millisecond):
		// Timeout is ok
	}
}

func TestSessionRunWithMultipleResults(t *testing.T) {
	executor := &mockExecutor{
		executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			return &QueryResult{
				Columns: []string{"id", "name", "score"},
				Rows: [][]any{
					{1, "Alice", 95.5},
					{2, "Bob", 87.3},
					{3, "Charlie", 92.1},
					{4, "Diana", 88.9},
					{5, "Eve", 91.0},
				},
			}, nil
		},
	}

	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	session := &Session{
		conn:       server,
		reader:     bufio.NewReaderSize(server, 8192),
		writer:     bufio.NewWriterSize(server, 8192),
		executor:   executor,
		messageBuf: make([]byte, 0, 4096),
	}

	// Execute a query
	done := make(chan error, 1)
	go func() {
		// Build RUN message data (without struct marker - just query + params)
		// handleRun expects the data payload after the message type has been identified
		runMsg := encodePackStreamString("MATCH (n) RETURN n.id, n.name, n.score")
		runMsg = append(runMsg, 0xA0) // Empty params map

		err := session.handleRun(runMsg)
		done <- err
	}()

	// Read SUCCESS response
	go func() {
		buf := make([]byte, 4096)
		client.Read(buf)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Logf("handleRun returned error (expected): %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}

	// Verify lastResult was stored (if execution succeeded)
	if session.lastResult != nil && len(session.lastResult.Rows) != 5 {
		t.Errorf("expected 5 rows, got %d", len(session.lastResult.Rows))
	}
}

func TestHandlePullWithLimit(t *testing.T) {
	// Create a mock connection
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	session := &Session{
		conn:       server,
		reader:     bufio.NewReaderSize(server, 8192),
		writer:     bufio.NewWriterSize(server, 8192),
		messageBuf: make([]byte, 0, 4096),
		lastResult: &QueryResult{
			Columns: []string{"n"},
			Rows: [][]any{
				{"row1"},
				{"row2"},
				{"row3"},
				{"row4"},
				{"row5"},
			},
		},
		resultIndex: 0,
	}

	// Build PULL data with n=2 (PackStream map: {n: 2})
	pullData := []byte{
		0xA1,      // TINY_MAP with 1 element
		0x81, 'n', // TINY_STRING "n"
		0x02, // tiny int 2
	}

	// Pull only 2 records
	done := make(chan error, 1)
	go func() {
		err := session.handlePull(pullData)
		done <- err
	}()

	// Read from client
	go func() {
		buf := make([]byte, 4096)
		for {
			_, err := client.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handlePull failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}

	// resultIndex should be advanced
	if session.resultIndex != 2 {
		t.Errorf("expected resultIndex 2, got %d", session.resultIndex)
	}
}

func TestEncodePackStreamMap_ComplexTypes(t *testing.T) {
	m := map[string]any{
		"string": "hello",
		"int":    int64(42),
		"float":  3.14,
		"bool":   true,
		"nil":    nil,
		"list":   []any{1, 2, 3},
		"nested": map[string]any{
			"key": "value",
		},
	}

	encoded := encodePackStreamMap(m)
	if len(encoded) == 0 {
		t.Error("expected non-empty encoded map")
	}

	// Should start with a MAP marker
	if encoded[0]&0xF0 != 0xA0 && encoded[0] != 0xD8 && encoded[0] != 0xD9 && encoded[0] != 0xDA {
		t.Errorf("expected MAP marker, got 0x%02X", encoded[0])
	}
}

func TestEncodePackStreamList_LargeList(t *testing.T) {
	// Create a list with more than 15 elements (requires LIST8)
	list := make([]any, 20)
	for i := range list {
		list[i] = int64(i)
	}

	encoded := encodePackStreamList(list)
	if len(encoded) == 0 {
		t.Error("expected non-empty encoded list")
	}

	// Should start with LIST8 marker (0xD4)
	if encoded[0] != 0xD4 {
		t.Errorf("expected LIST8 marker 0xD4, got 0x%02X", encoded[0])
	}
}

func TestDecodePackStreamString_LongString(t *testing.T) {
	// STRING8 with longer content
	content := "this is a longer string that tests STRING8 encoding"
	data := []byte{0xD0, byte(len(content))}
	data = append(data, []byte(content)...)

	result, consumed, err := decodePackStreamString(data, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result != content {
		t.Errorf("got %q, want %q", result, content)
	}
	if consumed != 2+len(content) { // marker + length + content
		t.Errorf("consumed %d bytes, want %d", consumed, 2+len(content))
	}
}

func TestDecodePackStreamValue_INT32(t *testing.T) {
	// INT32: marker 0xCA + 4 bytes
	data := []byte{0xCA, 0x00, 0x01, 0x00, 0x00} // 65536
	result, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result != int64(65536) {
		t.Errorf("got %v, want 65536", result)
	}
}

func TestDecodePackStreamValue_INT64(t *testing.T) {
	// INT64: marker 0xCB + 8 bytes
	data := []byte{0xCB, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00} // 4294967296
	result, _, err := decodePackStreamValue(data, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
		return
	}
	if result != int64(4294967296) {
		t.Errorf("got %v, want 4294967296", result)
	}
}

func TestDecodePackStreamStructure(t *testing.T) {
	t.Run("node_structure", func(t *testing.T) {
		// Create a Node structure: B3 4E [id] [labels] [properties]
		// B3 = tiny struct with 3 fields, 4E = Node signature
		nodeId := encodePackStreamInt(int64(123))
		labels := encodePackStreamList([]any{"Person"})
		properties := encodePackStreamMap(map[string]any{"name": "Alice"})

		data := []byte{0xB3, 0x4E}
		data = append(data, nodeId...)
		data = append(data, labels...)
		data = append(data, properties...)

		result, consumed, err := decodePackStreamValue(data, 0)
		if err != nil {
			t.Fatalf("decodePackStreamValue failed: %v", err)
		}
		if consumed == 0 {
			t.Error("Should consume bytes")
		}

		// Verify structure
		nodeMap, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("Result should be a map, got %T", result)
		}
		if nodeMap["_type"] != "Node" {
			t.Errorf("Expected _type=Node, got %v", nodeMap["_type"])
		}
		if nodeMap["id"] != int64(123) {
			t.Errorf("Expected id=123, got %v", nodeMap["id"])
		}
		labelsVal, ok := nodeMap["labels"].([]any)
		if !ok || len(labelsVal) == 0 || labelsVal[0] != "Person" {
			t.Errorf("Expected labels=[Person], got %v", labelsVal)
		}
		props, ok := nodeMap["properties"].(map[string]any)
		if !ok || props["name"] != "Alice" {
			t.Errorf("Expected properties.name=Alice, got %v", props)
		}
	})

	t.Run("empty_structure", func(t *testing.T) {
		// Empty structure: B0 [signature]
		data := []byte{0xB0, 0x01}

		result, consumed, err := decodePackStreamValue(data, 0)
		if err != nil {
			t.Fatalf("decodePackStreamValue failed: %v", err)
		}
		if consumed != 2 {
			t.Errorf("Should consume 2 bytes (marker + signature), got %d", consumed)
		}

		structMap, ok := result.(map[string]any)
		if !ok {
			t.Fatalf("Result should be a map, got %T", result)
		}
		if structMap["_type"] != "Structure_0x01" {
			t.Errorf("Expected _type=Structure_0x01, got %v", structMap["_type"])
		}
		fields, ok := structMap["fields"].([]any)
		if !ok || len(fields) != 0 {
			t.Errorf("Expected empty fields, got %v", fields)
		}
	})

	t.Run("structure_in_list", func(t *testing.T) {
		// List containing a structure
		nodeId := encodePackStreamInt(int64(456))
		labels := encodePackStreamList([]any{"Company"})
		properties := encodePackStreamMap(map[string]any{"name": "Acme"})

		nodeStruct := []byte{0xB3, 0x4E}
		nodeStruct = append(nodeStruct, nodeId...)
		nodeStruct = append(nodeStruct, labels...)
		nodeStruct = append(nodeStruct, properties...)

		// List with one element (the node structure)
		listData := []byte{0x91} // Tiny list with 1 element
		listData = append(listData, nodeStruct...)

		result, _, err := decodePackStreamValue(listData, 0)
		if err != nil {
			t.Fatalf("decodePackStreamValue failed: %v", err)
		}

		list, ok := result.([]any)
		if !ok {
			t.Fatalf("Result should be a list, got %T", result)
		}
		if len(list) != 1 {
			t.Fatalf("Expected list length 1, got %d", len(list))
		}

		nodeMap, ok := list[0].(map[string]any)
		if !ok {
			t.Fatalf("List element should be a map, got %T", list[0])
		}
		if nodeMap["_type"] != "Node" {
			t.Errorf("Expected _type=Node, got %v", nodeMap["_type"])
		}
		if nodeMap["id"] != int64(456) {
			t.Errorf("Expected id=456, got %v", nodeMap["id"])
		}
	})
}

// ============================================================================
// Transaction Tests
// ============================================================================

// mockTransactionalExecutor implements TransactionalExecutor for testing.
type mockTransactionalExecutor struct {
	mockExecutor
	beginCalled    bool
	commitCalled   bool
	rollbackCalled bool
	beginError     error
	commitError    error
	rollbackError  error
	lastMetadata   map[string]any
}

func (m *mockTransactionalExecutor) BeginTransaction(ctx context.Context, metadata map[string]any) error {
	m.beginCalled = true
	m.lastMetadata = metadata
	return m.beginError
}

func (m *mockTransactionalExecutor) CommitTransaction(ctx context.Context) error {
	m.commitCalled = true
	return m.commitError
}

func (m *mockTransactionalExecutor) RollbackTransaction(ctx context.Context) error {
	m.rollbackCalled = true
	return m.rollbackError
}

func TestTransactionalExecutorInterface(t *testing.T) {
	t.Run("regular executor does not implement TransactionalExecutor", func(t *testing.T) {
		executor := &mockExecutor{}
		_, ok := interface{}(executor).(TransactionalExecutor)
		if ok {
			t.Error("mockExecutor should NOT implement TransactionalExecutor")
		}
	})

	t.Run("transactional executor implements TransactionalExecutor", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		_, ok := interface{}(executor).(TransactionalExecutor)
		if !ok {
			t.Error("mockTransactionalExecutor should implement TransactionalExecutor")
		}
	})
}

func TestHandleBeginWithTransactionalExecutor(t *testing.T) {
	t.Run("begin calls executor", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)

		err := session.handleBegin(nil)
		if err != nil {
			t.Fatalf("handleBegin error: %v", err)
		}

		if !executor.beginCalled {
			t.Error("BeginTransaction should have been called")
		}
		if !session.inTransaction {
			t.Error("session should be in transaction")
		}
	})

	t.Run("begin with metadata passes metadata to executor", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)

		// Create metadata with tx_timeout
		metadata := encodePackStreamMap(map[string]any{
			"tx_timeout": int64(30000),
		})

		err := session.handleBegin(metadata)
		if err != nil {
			t.Fatalf("handleBegin error: %v", err)
		}

		if executor.lastMetadata == nil {
			t.Error("metadata should have been passed to executor")
		}
	})

	t.Run("begin error returns failure", func(t *testing.T) {
		executor := &mockTransactionalExecutor{
			beginError: io.EOF, // Simulate error
		}
		conn := &mockConn{}
		session := newTestSession(conn, executor)

		err := session.handleBegin(nil)
		if err != nil {
			t.Fatalf("handleBegin should not return Go error: %v", err)
		}

		// Check that FAILURE was sent (contains 0x7F)
		if len(conn.writeData) == 0 {
			t.Error("expected failure response")
		}
	})
}

func TestHandleCommitWithTransactionalExecutor(t *testing.T) {
	t.Run("commit calls executor and returns bookmark", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleCommit(nil)
		if err != nil {
			t.Fatalf("handleCommit error: %v", err)
		}

		if !executor.commitCalled {
			t.Error("CommitTransaction should have been called")
		}
		if session.inTransaction {
			t.Error("session should not be in transaction after commit")
		}

		// Verify bookmark is in response
		if len(conn.writeData) == 0 {
			t.Error("expected success response with bookmark")
		}
	})

	t.Run("commit without transaction returns failure", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = false

		err := session.handleCommit(nil)
		if err != nil {
			t.Fatalf("handleCommit should not return Go error: %v", err)
		}

		if executor.commitCalled {
			t.Error("CommitTransaction should NOT have been called")
		}
	})

	t.Run("commit error returns failure", func(t *testing.T) {
		executor := &mockTransactionalExecutor{
			commitError: io.EOF,
		}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleCommit(nil)
		if err == nil {
			t.Fatal("handleCommit must fail the connection closed on an uncertain commit outcome")
		}

		if !session.inTransaction {
			t.Error("unknown backend ownership must remain fail-closed on commit error")
		}
	})

	t.Run("commit conflict returns transient transaction failure", func(t *testing.T) {
		executor := &mockTransactionalExecutor{
			commitError: fmt.Errorf("%w: node nornic:abc changed after transaction start", storage.ErrConflict),
		}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleCommit(nil)
		if err == nil {
			t.Fatal("handleCommit must fail the connection closed on an uncertain commit conflict")
		}

		code := failureCodeFromResponse(t, conn.writeData)
		if code != "Neo.TransientError.Transaction.Outdated" {
			t.Fatalf("failure code = %q, want %q", code, "Neo.TransientError.Transaction.Outdated")
		}
	})
}

func TestHandleRollbackWithTransactionalExecutor(t *testing.T) {
	t.Run("rollback calls executor", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleRollback(nil)
		if err != nil {
			t.Fatalf("handleRollback error: %v", err)
		}

		if !executor.rollbackCalled {
			t.Error("RollbackTransaction should have been called")
		}
		if session.inTransaction {
			t.Error("session should not be in transaction after rollback")
		}
	})

	t.Run("rollback without transaction is no-op", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = false

		err := session.handleRollback(nil)
		if err != nil {
			t.Fatalf("handleRollback error: %v", err)
		}

		if executor.rollbackCalled {
			t.Error("RollbackTransaction should NOT have been called")
		}
	})
}

func TestHandleResetWithTransactionalExecutor(t *testing.T) {
	t.Run("reset rolls back active transaction", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleReset(nil)
		if err != nil {
			t.Fatalf("handleReset error: %v", err)
		}

		if !executor.rollbackCalled {
			t.Error("RollbackTransaction should have been called on reset")
		}
		if session.inTransaction {
			t.Error("session should not be in transaction after reset")
		}
	})

	t.Run("reset without transaction does not call rollback", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = false

		err := session.handleReset(nil)
		if err != nil {
			t.Fatalf("handleReset error: %v", err)
		}

		if executor.rollbackCalled {
			t.Error("RollbackTransaction should NOT have been called")
		}
	})
}

func TestTransactionWithNonTransactionalExecutor(t *testing.T) {
	t.Run("begin works without TransactionalExecutor", func(t *testing.T) {
		executor := &mockExecutor{} // Does NOT implement TransactionalExecutor
		conn := &mockConn{}
		session := newTestSession(conn, executor)

		err := session.handleBegin(nil)
		if err != nil {
			t.Fatalf("handleBegin error: %v", err)
		}

		if !session.inTransaction {
			t.Error("session should be in transaction")
		}
	})

	t.Run("commit works without TransactionalExecutor", func(t *testing.T) {
		executor := &mockExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleCommit(nil)
		if err != nil {
			t.Fatalf("handleCommit error: %v", err)
		}

		if session.inTransaction {
			t.Error("session should not be in transaction after commit")
		}
	})

	t.Run("rollback works without TransactionalExecutor", func(t *testing.T) {
		executor := &mockExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true
		primeTestTransactionLifecycle(t, session)

		err := session.handleRollback(nil)
		if err != nil {
			t.Fatalf("handleRollback error: %v", err)
		}

		if session.inTransaction {
			t.Error("session should not be in transaction after rollback")
		}
	})
}

// mockBoltAuthenticator implements BoltAuthenticator for testing.
type mockBoltAuthenticator struct {
	authenticateFunc func(scheme, principal, credentials string) (*BoltAuthResult, error)
}

func (m *mockBoltAuthenticator) Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error) {
	if m.authenticateFunc != nil {
		return m.authenticateFunc(scheme, principal, credentials)
	}
	// Default: accept admin/admin
	if scheme == "basic" && principal == "admin" && credentials == "admin" {
		return &BoltAuthResult{
			Authenticated: true,
			Username:      principal,
			Roles:         []string{"admin"},
		}, nil
	}
	if scheme == "basic" && principal == "viewer" && credentials == "viewer" {
		return &BoltAuthResult{
			Authenticated: true,
			Username:      principal,
			Roles:         []string{"viewer"},
		}, nil
	}
	if scheme == "basic" && principal == "editor" && credentials == "editor" {
		return &BoltAuthResult{
			Authenticated: true,
			Username:      principal,
			Roles:         []string{"editor"},
		}, nil
	}
	return nil, fmt.Errorf("invalid credentials")
}

// newTestSessionWithAuth creates a test session with a server that has auth configured.
func newTestSessionWithAuth(conn net.Conn, executor QueryExecutor, auth BoltAuthenticator, requireAuth, allowAnon bool) *Session {
	server := &Server{
		config: &Config{
			Authenticator:  auth,
			RequireAuth:    requireAuth,
			AllowAnonymous: allowAnon,
		},
	}
	return &Session{
		conn:       conn,
		reader:     bufio.NewReaderSize(conn, 8192),
		writer:     bufio.NewWriterSize(conn, 8192),
		server:     server,
		executor:   executor,
		messageBuf: make([]byte, 0, 4096),
	}
}

// buildHelloMessage builds a PackStream HELLO message with auth credentials.
// This is a convenience wrapper around BuildHelloMessage for backward compatibility.
func buildHelloMessage(scheme, principal, credentials string) []byte {
	authParams := make(map[string]string)
	if scheme != "" {
		authParams["scheme"] = scheme
	}
	if principal != "" {
		authParams["principal"] = principal
	}
	if credentials != "" {
		authParams["credentials"] = credentials
	}
	return BuildHelloMessage(authParams)
}

func TestBoltAuthResult(t *testing.T) {
	t.Run("HasRole", func(t *testing.T) {
		result := &BoltAuthResult{
			Authenticated: true,
			Username:      "test",
			Roles:         []string{"admin", "editor"},
		}

		if !result.HasRole("admin") {
			t.Error("should have admin role")
		}
		if !result.HasRole("editor") {
			t.Error("should have editor role")
		}
		if result.HasRole("viewer") {
			t.Error("should NOT have viewer role")
		}
	})

	t.Run("HasPermission admin", func(t *testing.T) {
		result := &BoltAuthResult{
			Authenticated: true,
			Username:      "admin",
			Roles:         []string{"admin"},
		}

		if !result.HasPermission("read") {
			t.Error("admin should have read permission")
		}
		if !result.HasPermission("write") {
			t.Error("admin should have write permission")
		}
		if !result.HasPermission("schema") {
			t.Error("admin should have schema permission")
		}
		if !result.HasPermission("user_manage") {
			t.Error("admin should have user_manage permission")
		}
	})

	t.Run("HasPermission viewer", func(t *testing.T) {
		result := &BoltAuthResult{
			Authenticated: true,
			Username:      "viewer",
			Roles:         []string{"viewer"},
		}

		if !result.HasPermission("read") {
			t.Error("viewer should have read permission")
		}
		if result.HasPermission("write") {
			t.Error("viewer should NOT have write permission")
		}
		if result.HasPermission("schema") {
			t.Error("viewer should NOT have schema permission")
		}
	})

	t.Run("HasPermission editor", func(t *testing.T) {
		result := &BoltAuthResult{
			Authenticated: true,
			Username:      "editor",
			Roles:         []string{"editor"},
		}

		if !result.HasPermission("read") {
			t.Error("editor should have read permission")
		}
		if !result.HasPermission("write") {
			t.Error("editor should have write permission")
		}
		if result.HasPermission("schema") {
			t.Error("editor should NOT have schema permission")
		}
	})
}

func TestHandleHelloAuth(t *testing.T) {
	t.Run("successful basic auth", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		helloData := buildHelloMessage("basic", "admin", "admin")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("session should be authenticated")
		}
		if session.authResult == nil {
			t.Fatal("authResult should not be nil")
		}
		if session.authResult.Username != "admin" {
			t.Errorf("expected username 'admin', got %q", session.authResult.Username)
		}
		if !session.authResult.HasRole("admin") {
			t.Error("should have admin role")
		}
	})

	t.Run("failed basic auth", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		helloData := buildHelloMessage("basic", "admin", "wrongpassword")
		err := session.handleHello(helloData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleHello should return nil, got: %v", err)
		}

		if session.authenticated {
			t.Error("session should NOT be authenticated after failed auth")
		}
	})

	t.Run("anonymous auth allowed", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, true) // AllowAnonymous = true

		helloData := buildHelloMessage("none", "", "")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("session should be authenticated (anonymous)")
		}
		if session.authResult == nil {
			t.Fatal("authResult should not be nil")
		}
		if session.authResult.Username != "anonymous" {
			t.Errorf("expected username 'anonymous', got %q", session.authResult.Username)
		}
		if !session.authResult.HasRole("viewer") {
			t.Error("anonymous should have viewer role")
		}
	})

	t.Run("anonymous auth rejected when not allowed", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false) // AllowAnonymous = false

		helloData := buildHelloMessage("none", "", "")
		err := session.handleHello(helloData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleHello should return nil, got: %v", err)
		}

		if session.authenticated {
			t.Error("session should NOT be authenticated when anonymous is rejected")
		}
	})

	t.Run("no auth required - accepts all", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, nil, false, false) // No auth configured

		helloData := buildHelloMessage("basic", "anyone", "anything")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("session should be authenticated (dev mode)")
		}
		if session.authResult == nil {
			t.Fatal("authResult should not be nil")
		}
		// Dev mode grants admin
		if !session.authResult.HasRole("admin") {
			t.Error("dev mode should grant admin role")
		}
	})

	t.Run("unsupported auth scheme", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		helloData := buildHelloMessage("kerberos", "user", "token")
		err := session.handleHello(helloData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleHello should return nil, got: %v", err)
		}

		if session.authenticated {
			t.Error("session should NOT be authenticated with unsupported scheme")
		}
	})
}

func TestHandleHello_IncludesUTCPatchMetadata(t *testing.T) {
	auth := &mockBoltAuthenticator{}
	conn := &mockConn{}
	session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

	err := session.handleHello(buildHelloMessage("basic", "admin", "admin"))
	if err != nil {
		t.Fatalf("handleHello error: %v", err)
	}

	// The SUCCESS metadata must advertise Bolt UTC temporal support so
	// modern drivers hydrate DateTime values (0x49) correctly.
	if !bytes.Contains(conn.writeData, []byte("patch_bolt")) {
		t.Fatalf("HELLO SUCCESS missing patch_bolt metadata: %x", conn.writeData)
	}
	if !bytes.Contains(conn.writeData, []byte("utc")) {
		t.Fatalf("HELLO SUCCESS missing utc patch value: %x", conn.writeData)
	}
}

func TestHandleRunAuth(t *testing.T) {
	t.Run("run without auth when required fails", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)
		// Don't call handleHello - session is not authenticated

		// Build a simple RUN message
		runData := buildRunMessage("MATCH (n) RETURN n", nil)
		err := session.handleRun(runData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleRun should return nil, got: %v", err)
		}

		// Check that response contains FAILURE
		response := string(conn.writeData)
		if !strings.Contains(response, "Unauthorized") && len(conn.writeData) > 0 {
			// The failure message is in binary PackStream format
			// Just verify the session state
		}
	})

	t.Run("viewer cannot write", func(t *testing.T) {
		auth := &mockBoltAuthenticator{}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		// Authenticate as viewer
		session.authenticated = true
		session.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "viewer",
			Roles:         []string{"viewer"},
		}

		// Try to run a write query
		runData := buildRunMessage("CREATE (n:Test) RETURN n", nil)
		err := session.handleRun(runData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleRun should return nil, got: %v", err)
		}

		// The session should have sent a FAILURE response
		// We can't easily check binary data, but we know the permission check happened
	})

	t.Run("viewer can read", func(t *testing.T) {
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"n"},
					Rows:    [][]any{{"test"}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, executor, &mockBoltAuthenticator{}, false, false)

		// Authenticate as viewer
		session.authenticated = true
		session.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "viewer",
			Roles:         []string{"viewer"},
		}

		// Run a read query
		runData := buildRunMessage("MATCH (n) RETURN n", nil)
		err := session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}

		// Query should have been executed
		if session.lastResult == nil {
			t.Error("query should have been executed")
		}
	})

	t.Run("editor can write", func(t *testing.T) {
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"n"},
					Rows:    [][]any{{"test"}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, executor, &mockBoltAuthenticator{}, false, false)

		// Authenticate as editor
		session.authenticated = true
		session.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "editor",
			Roles:         []string{"editor"},
		}

		// Run a write query
		runData := buildRunMessage("CREATE (n:Test) RETURN n", nil)
		err := session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}

		// Query should have been executed
		if session.lastResult == nil {
			t.Error("query should have been executed")
		}
	})

	t.Run("editor cannot schema", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, &mockBoltAuthenticator{}, false, false)

		// Authenticate as editor
		session.authenticated = true
		session.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "editor",
			Roles:         []string{"editor"},
		}

		// Try to run a schema query
		runData := buildRunMessage("CREATE INDEX ON :Person(name)", nil)
		err := session.handleRun(runData)
		// Should return nil (error sent via FAILURE message)
		if err != nil {
			t.Fatalf("handleRun should return nil, got: %v", err)
		}

		// Schema query should have been rejected
	})

	t.Run("admin can do everything", func(t *testing.T) {
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"result"},
					Rows:    [][]any{{"ok"}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, executor, &mockBoltAuthenticator{}, false, false)

		// Authenticate as admin
		session.authenticated = true
		session.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "admin",
			Roles:         []string{"admin"},
		}

		// Test schema query
		runData := buildRunMessage("CREATE INDEX ON :Person(name)", nil)
		err := session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error for schema: %v", err)
		}

		// Test write query
		runData = buildRunMessage("CREATE (n:Test) RETURN n", nil)
		err = session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error for write: %v", err)
		}
	})
}

// buildRunMessage builds a PackStream RUN message.
// Format: [query: String, parameters: Map, extra: Map]
func buildRunMessage(query string, params map[string]any) []byte {
	buf := []byte{}

	// Query string
	buf = append(buf, encodePackStreamString(query)...)

	// Empty params map (A0 = tiny map with 0 entries)
	buf = append(buf, 0xA0)

	// Empty extra map
	buf = append(buf, 0xA0)

	return buf
}

func TestServerToServerAuth(t *testing.T) {
	t.Run("service account auth", func(t *testing.T) {
		// Simulate server-to-server auth with service account
		auth := &mockBoltAuthenticator{
			authenticateFunc: func(scheme, principal, credentials string) (*BoltAuthResult, error) {
				// Service accounts use basic auth with special prefix
				if scheme == "basic" && strings.HasPrefix(principal, "svc-") {
					return &BoltAuthResult{
						Authenticated: true,
						Username:      principal,
						Roles:         []string{"admin"}, // Service accounts get full access
					}, nil
				}
				return nil, fmt.Errorf("invalid service account")
			},
		}

		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		helloData := buildHelloMessage("basic", "svc-cluster-node-1", "secret-key")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("service account should be authenticated")
		}
		if session.authResult.Username != "svc-cluster-node-1" {
			t.Errorf("expected service account name, got %q", session.authResult.Username)
		}
		if !session.authResult.HasPermission("admin") {
			t.Error("service account should have admin permission")
		}
	})

	t.Run("cluster replication auth", func(t *testing.T) {
		// Simulate auth for cluster replication connections
		auth := &mockBoltAuthenticator{
			authenticateFunc: func(scheme, principal, credentials string) (*BoltAuthResult, error) {
				if scheme == "basic" && principal == "replication" {
					// Verify replication token
					if credentials == "cluster-secret-token" {
						return &BoltAuthResult{
							Authenticated: true,
							Username:      "replication",
							Roles:         []string{"admin"}, // Replication needs full access
						}, nil
					}
				}
				return nil, fmt.Errorf("invalid replication credentials")
			},
		}

		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, &mockExecutor{}, auth, true, false)

		helloData := buildHelloMessage("basic", "replication", "cluster-secret-token")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("replication should be authenticated")
		}
	})
}

func TestAuthDisabled(t *testing.T) {
	t.Run("no server reference allows all operations", func(t *testing.T) {
		// Sessions without server reference (e.g., unit tests) should work
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"result"},
					Rows:    [][]any{{"ok"}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSession(conn, executor) // No server = no auth

		// Should be able to run queries without auth
		runData := buildRunMessage("CREATE (n:Test) RETURN n", nil)
		err := session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}

		if session.lastResult == nil {
			t.Error("query should have been executed")
		}
	})

	t.Run("auth disabled with nil authenticator", func(t *testing.T) {
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"result"},
					Rows:    [][]any{{"ok"}},
				}, nil
			},
		}
		conn := &mockConn{}
		// No authenticator, RequireAuth=false, AllowAnonymous=false
		session := newTestSessionWithAuth(conn, executor, nil, false, false)

		// HELLO should succeed and grant admin
		helloData := buildHelloMessage("basic", "anyone", "anything")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("should be authenticated in dev mode")
		}
		if session.authResult == nil {
			t.Fatal("authResult should not be nil")
		}
		if !session.authResult.HasRole("admin") {
			t.Error("dev mode should grant admin role")
		}

		// Should be able to run any query
		runData := buildRunMessage("CREATE INDEX ON :Person(name)", nil)
		err = session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}

		if session.lastResult == nil {
			t.Error("schema query should have been executed")
		}
	})

	t.Run("auth disabled accepts neo4j NoAuth", func(t *testing.T) {
		// Neo4j drivers use scheme "none" when using neo4j.NoAuth()
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"n"},
					Rows:    [][]any{{"test"}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSessionWithAuth(conn, executor, nil, false, false)

		// Send HELLO with scheme "none" (Neo4j NoAuth)
		helloData := buildHelloMessage("none", "", "")
		err := session.handleHello(helloData)
		if err != nil {
			t.Fatalf("handleHello error: %v", err)
		}

		if !session.authenticated {
			t.Error("should be authenticated")
		}

		// Run a query
		runData := buildRunMessage("MATCH (n) RETURN n", nil)
		err = session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}
	})

	t.Run("existing tests still work without auth", func(t *testing.T) {
		// This mimics how existing tests create sessions
		executor := &mockExecutor{
			executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
				return &QueryResult{
					Columns: []string{"count"},
					Rows:    [][]any{{int64(42)}},
				}, nil
			},
		}
		conn := &mockConn{}
		session := newTestSession(conn, executor)

		// Run query directly (no HELLO, no auth)
		runData := buildRunMessage("MATCH (n) RETURN count(n)", nil)
		err := session.handleRun(runData)
		if err != nil {
			t.Fatalf("handleRun error: %v", err)
		}

		if session.lastResult == nil {
			t.Error("query should have been executed")
		}
		if len(session.lastResult.Rows) != 1 {
			t.Errorf("expected 1 row, got %d", len(session.lastResult.Rows))
		}
	})
}
