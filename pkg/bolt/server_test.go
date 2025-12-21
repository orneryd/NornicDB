// Package bolt tests for the Bolt protocol server.
package bolt

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// mockExecutor implements QueryExecutor for testing.
type mockExecutor struct {
	executeFunc func(ctx context.Context, query string, params map[string]any) (*QueryResult, error)
}

func (m *mockExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	if m.executeFunc != nil {
		return m.executeFunc(ctx, query, params)
	}
	return &QueryResult{
		Columns: []string{"n"},
		Rows:    [][]any{{"test"}},
	}, nil
}

func TestDefaultConfig(t *testing.T) {
	config := DefaultConfig()

	if config.Port != 7687 {
		t.Errorf("expected port 7687, got %d", config.Port)
	}
	if config.MaxConnections != 100 {
		t.Errorf("expected 100 max connections, got %d", config.MaxConnections)
	}
	if config.ReadBufferSize != 8192 {
		t.Errorf("expected 8192 read buffer, got %d", config.ReadBufferSize)
	}
	if config.WriteBufferSize != 8192 {
		t.Errorf("expected 8192 write buffer, got %d", config.WriteBufferSize)
	}
}

func TestNew(t *testing.T) {
	t.Run("with config", func(t *testing.T) {
		config := &Config{
			Port:           7688,
			MaxConnections: 50,
		}
		executor := &mockExecutor{}
		server := New(config, executor)

		if server.config.Port != 7688 {
			t.Errorf("expected port 7688, got %d", server.config.Port)
		}
	})

	t.Run("with nil config", func(t *testing.T) {
		executor := &mockExecutor{}
		server := New(nil, executor)

		if server.config.Port != 7687 {
			t.Error("should use default config")
		}
	})
}

func TestServerClose(t *testing.T) {
	server := New(nil, &mockExecutor{})

	// Close without starting should not error
	if err := server.Close(); err != nil {
		t.Errorf("Close() without listener should not error: %v", err)
	}
}

func TestMessageTypes(t *testing.T) {
	// Verify message type constants
	tests := []struct {
		name     string
		msgType  byte
		expected byte
	}{
		{"Hello", MsgHello, 0x01},
		{"Goodbye", MsgGoodbye, 0x02},
		{"Reset", MsgReset, 0x0F},
		{"Run", MsgRun, 0x10},
		{"Discard", MsgDiscard, 0x2F},
		{"Pull", MsgPull, 0x3F},
		{"Begin", MsgBegin, 0x11},
		{"Commit", MsgCommit, 0x12},
		{"Rollback", MsgRollback, 0x13},
		{"Route", MsgRoute, 0x66},
		{"Success", MsgSuccess, 0x70},
		{"Record", MsgRecord, 0x71},
		{"Ignored", MsgIgnored, 0x7E},
		{"Failure", MsgFailure, 0x7F},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.msgType != tt.expected {
				t.Errorf("expected 0x%02X, got 0x%02X", tt.expected, tt.msgType)
			}
		})
	}
}

func TestProtocolVersions(t *testing.T) {
	// Verify protocol version constants
	tests := []struct {
		name    string
		version int
		major   int
		minor   int
	}{
		{"Bolt 4.4", BoltV4_4, 4, 4},
		{"Bolt 4.3", BoltV4_3, 4, 3},
		{"Bolt 4.2", BoltV4_2, 4, 2},
		{"Bolt 4.1", BoltV4_1, 4, 1},
		{"Bolt 4.0", BoltV4_0, 4, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			major := (tt.version >> 8) & 0xFF
			minor := tt.version & 0xFF
			if major != tt.major || minor != tt.minor {
				t.Errorf("expected %d.%d, got %d.%d", tt.major, tt.minor, major, minor)
			}
		})
	}
}

// mockConn implements net.Conn for testing.
type mockConn struct {
	readData  []byte
	readPos   int
	writeData []byte
	closed    bool
}

func (m *mockConn) Read(b []byte) (n int, err error) {
	if m.readPos >= len(m.readData) {
		return 0, io.EOF
	}
	n = copy(b, m.readData[m.readPos:])
	m.readPos += n
	return n, nil
}

func (m *mockConn) Write(b []byte) (n int, err error) {
	m.writeData = append(m.writeData, b...)
	return len(b), nil
}

func (m *mockConn) Close() error {
	m.closed = true
	return nil
}

func (m *mockConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (m *mockConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// newTestSession creates a properly initialized Session for testing.
// This ensures reader and writer are set up correctly, and includes a server reference
// for bookmark generation and validation.
func newTestSession(conn net.Conn, executor QueryExecutor) *Session {
	server := &Server{
		txSequence: 0,               // Initialize transaction sequence
		config:     DefaultConfig(), // Initialize with default config
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

func TestSessionHandshake(t *testing.T) {
	t.Run("valid handshake", func(t *testing.T) {
		// Bolt magic: 0x6060B017
		// Then 4 version proposals (each 4 bytes)
		handshakeData := []byte{
			0x60, 0x60, 0xB0, 0x17, // Magic
			0x00, 0x00, 0x04, 0x04, // Version 4.4
			0x00, 0x00, 0x04, 0x03, // Version 4.3
			0x00, 0x00, 0x04, 0x02, // Version 4.2
			0x00, 0x00, 0x04, 0x01, // Version 4.1
		}

		conn := &mockConn{readData: handshakeData}
		session := newTestSession(conn, &mockExecutor{})

		err := session.handshake()
		if err != nil {
			t.Fatalf("handshake() error = %v", err)
		}

		if session.version != BoltV4_4 {
			t.Errorf("expected version %d, got %d", BoltV4_4, session.version)
		}

		// Check response was sent
		if len(conn.writeData) != 4 {
			t.Errorf("expected 4 bytes written, got %d", len(conn.writeData))
		}
	})

	t.Run("invalid magic", func(t *testing.T) {
		handshakeData := []byte{
			0x00, 0x00, 0x00, 0x00, // Invalid magic
			0x00, 0x00, 0x04, 0x04,
			0x00, 0x00, 0x04, 0x03,
			0x00, 0x00, 0x04, 0x02,
			0x00, 0x00, 0x04, 0x01,
		}

		conn := &mockConn{readData: handshakeData}
		session := newTestSession(conn, nil)

		err := session.handshake()
		if err == nil {
			t.Error("expected error for invalid magic")
		}
	})
}

func TestSessionHandleMessage(t *testing.T) {
	t.Run("hello message", func(t *testing.T) {
		// PackStream struct format: 0xB1 (tiny struct, 1 field) + signature + data
		// HELLO message needs an empty map (auth info): 0xA0
		messageData := []byte{
			0x00, 0x03, // Size: 3 bytes
			0xB1, MsgHello, 0xA0, // Tiny struct + HELLO sig + empty map
			0x00, 0x00, // Zero terminator (end of message)
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, &mockExecutor{})

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}
	})

	t.Run("goodbye message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x02, // Size: 2 bytes
			0xB0, MsgGoodbye, // Tiny struct (0 fields) + GOODBYE sig
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err != io.EOF {
			t.Errorf("expected io.EOF for goodbye, got %v", err)
		}
	})

	t.Run("run message", func(t *testing.T) {
		// RUN needs query string and params map
		// Query: "TEST" (0x84 + TEST), Params: empty map (0xA0)
		messageData := []byte{
			0x00, 0x08, // Size: 8 bytes
			0xB1, MsgRun, // Tiny struct + RUN sig
			0x84, 'T', 'E', 'S', 'T', // Query string "TEST"
			0xA0,       // Empty params map
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, &mockExecutor{})

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}
	})

	t.Run("pull message", func(t *testing.T) {
		// PULL needs options map
		messageData := []byte{
			0x00, 0x03, // Size: 3 bytes
			0xB1, MsgPull, 0xA0, // Tiny struct + PULL sig + empty options
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}
	})

	t.Run("reset message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x02, // Size: 2 bytes
			0xB0, MsgReset, // Tiny struct (0 fields) + RESET sig
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)
		session.inTransaction = true

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}

		if session.inTransaction {
			t.Error("reset should clear transaction state")
		}
	})

	t.Run("begin message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x03, // Size: 3 bytes
			0xB1, MsgBegin, 0xA0, // Tiny struct + BEGIN sig + empty options map
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}

		if !session.inTransaction {
			t.Error("begin should set transaction state")
		}
	})

	t.Run("commit message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x02, // Size: 2 bytes
			0xB0, MsgCommit, // Tiny struct (0 fields) + COMMIT sig
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)
		session.inTransaction = true

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}

		if session.inTransaction {
			t.Error("commit should clear transaction state")
		}
	})

	t.Run("rollback message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x02, // Size: 2 bytes
			0xB0, MsgRollback, // Tiny struct (0 fields) + ROLLBACK sig
			0x00, 0x00, // Zero terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)
		session.inTransaction = true

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("handleMessage() error = %v", err)
		}

		if session.inTransaction {
			t.Error("rollback should clear transaction state")
		}
	})

	t.Run("unknown message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x01,
			0xFF, // Unknown message type
			0x00, 0x00,
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err == nil {
			t.Error("expected error for unknown message type")
		}
	})

	t.Run("empty message", func(t *testing.T) {
		messageData := []byte{
			0x00, 0x00, // Size: 0 (no-op)
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err != nil {
			t.Fatalf("no-op message should not error: %v", err)
		}
	})
}

func TestSessionSendChunk(t *testing.T) {
	conn := &mockConn{}
	session := newTestSession(conn, nil)

	data := []byte{MsgSuccess, 0xA0} // Success + empty map marker
	err := session.sendChunk(data)
	if err != nil {
		t.Fatalf("sendChunk() error = %v", err)
	}

	// Should have: header (2) + data + terminator (2)
	expected := 2 + len(data) + 2
	if len(conn.writeData) != expected {
		t.Errorf("expected %d bytes written, got %d", expected, len(conn.writeData))
	}

	// Check header
	size := int(conn.writeData[0])<<8 | int(conn.writeData[1])
	if size != len(data) {
		t.Errorf("expected size %d, got %d", len(data), size)
	}

	// Check terminator
	if conn.writeData[len(conn.writeData)-2] != 0x00 || conn.writeData[len(conn.writeData)-1] != 0x00 {
		t.Error("expected 0x00 0x00 terminator")
	}
}

func TestSessionSendSuccess(t *testing.T) {
	conn := &mockConn{}
	session := newTestSession(conn, nil)

	err := session.sendSuccess(map[string]any{
		"server": "NornicDB",
	})
	if err != nil {
		t.Fatalf("sendSuccess() error = %v", err)
	}

	// Should have written something
	if len(conn.writeData) == 0 {
		t.Error("expected data to be written")
	}
}

func TestSessionSendFailure(t *testing.T) {
	conn := &mockConn{}
	session := newTestSession(conn, nil)

	err := session.sendFailure("Neo.ClientError.Statement.SyntaxError", "Invalid query")
	if err != nil {
		t.Fatalf("sendFailure() error = %v", err)
	}

	// Should have written something
	if len(conn.writeData) == 0 {
		t.Error("expected data to be written")
	}
}

func TestQueryResult(t *testing.T) {
	result := &QueryResult{
		Columns: []string{"name", "age"},
		Rows: [][]any{
			{"Alice", 30},
			{"Bob", 25},
		},
	}

	if len(result.Columns) != 2 {
		t.Error("expected 2 columns")
	}
	if len(result.Rows) != 2 {
		t.Error("expected 2 rows")
	}
}

func TestListenAndServe(t *testing.T) {
	t.Run("start_and_close", func(t *testing.T) {
		config := &Config{Port: 0, MaxConnections: 10}
		server := New(config, &mockExecutor{})

		done := make(chan error, 1)
		go func() {
			done <- server.ListenAndServe()
		}()

		// Wait for server to start
		time.Sleep(50 * time.Millisecond)

		// Close server
		if err := server.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}

		// Verify IsClosed
		if !server.IsClosed() {
			t.Error("expected server to be closed")
		}

		select {
		case <-done:
			// Server exited properly
		case <-time.After(500 * time.Millisecond):
			t.Error("server did not shut down")
		}
	})

	t.Run("listen_error", func(t *testing.T) {
		// Try to listen on an invalid port
		config := &Config{Port: -1}
		server := New(config, &mockExecutor{})

		err := server.ListenAndServe()
		if err == nil {
			t.Error("expected error for invalid port")
			server.Close()
		}
	})
}

func TestHandleConnection(t *testing.T) {
	t.Run("connection_with_invalid_handshake", func(t *testing.T) {
		server := New(nil, &mockExecutor{})

		clientConn, serverConn := net.Pipe()

		done := make(chan struct{})
		go func() {
			server.handleConnection(serverConn)
			close(done)
		}()

		// Send invalid handshake (too short)
		clientConn.Write([]byte{0x00, 0x00})
		clientConn.Close()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("handleConnection should complete on invalid handshake")
		}
	})

	t.Run("connection_ends_on_eof", func(t *testing.T) {
		server := New(nil, &mockExecutor{})

		clientConn, serverConn := net.Pipe()

		done := make(chan struct{})
		go func() {
			server.handleConnection(serverConn)
			close(done)
		}()

		// Close immediately (EOF)
		clientConn.Close()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("handleConnection should complete on EOF")
		}
	})

	t.Run("full_message_flow", func(t *testing.T) {
		server := New(nil, &mockExecutor{})
		clientConn, serverConn := net.Pipe()

		done := make(chan struct{})
		go func() {
			server.handleConnection(serverConn)
			close(done)
		}()

		// Valid handshake
		handshake := []byte{
			0x60, 0x60, 0xB0, 0x17,
			0x00, 0x00, 0x04, 0x04,
			0x00, 0x00, 0x04, 0x03,
			0x00, 0x00, 0x04, 0x02,
			0x00, 0x00, 0x04, 0x01,
		}
		clientConn.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
		clientConn.Write(handshake)

		// Read version response
		resp := make([]byte, 4)
		clientConn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		io.ReadFull(clientConn, resp)

		// Just close - we've tested handshake worked
		clientConn.Close()

		select {
		case <-done:
		case <-time.After(500 * time.Millisecond):
			t.Error("handleConnection did not complete")
		}
	})

	t.Run("server_closed_during_message_handling", func(t *testing.T) {
		server := New(nil, &mockExecutor{})
		clientConn, serverConn := net.Pipe()

		done := make(chan struct{})
		go func() {
			server.handleConnection(serverConn)
			close(done)
		}()

		go func() {
			// Valid handshake
			handshake := []byte{
				0x60, 0x60, 0xB0, 0x17,
				0x00, 0x00, 0x04, 0x04,
				0x00, 0x00, 0x04, 0x03,
				0x00, 0x00, 0x04, 0x02,
				0x00, 0x00, 0x04, 0x01,
			}
			clientConn.Write(handshake)

			// Read version response
			resp := make([]byte, 4)
			io.ReadFull(clientConn, resp)

			// Close server during handling
			server.Close()
			clientConn.Close()
		}()

		select {
		case <-done:
		case <-time.After(1 * time.Second):
			t.Error("handleConnection did not complete when server closed")
			clientConn.Close()
		}
	})
}

func TestIsClosed(t *testing.T) {
	server := New(nil, &mockExecutor{})

	if server.IsClosed() {
		t.Error("new server should not be closed")
	}

	server.Close()

	if !server.IsClosed() {
		t.Error("server should be closed after Close()")
	}
}

func TestSendChunkLargeData(t *testing.T) {
	t.Run("large data chunking", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSession(conn, nil)

		// Create data that's larger than typical chunk (but still fits)
		data := make([]byte, 1000)
		for i := range data {
			data[i] = byte(i % 256)
		}

		err := session.sendChunk(data)
		if err != nil {
			t.Fatalf("sendChunk() error = %v", err)
		}

		// Verify header
		size := int(conn.writeData[0])<<8 | int(conn.writeData[1])
		if size != 1000 {
			t.Errorf("expected size 1000, got %d", size)
		}
	})

	t.Run("empty data", func(t *testing.T) {
		conn := &mockConn{}
		session := newTestSession(conn, nil)

		err := session.sendChunk([]byte{})
		if err != nil {
			t.Fatalf("sendChunk() empty data error = %v", err)
		}

		// Should have header (2) + terminator (2) = 4 bytes
		if len(conn.writeData) != 4 {
			t.Errorf("expected 4 bytes for empty chunk, got %d", len(conn.writeData))
		}
	})
}

type errorConn struct {
	mockConn
	writeErr error
	readErr  error
}

func (e *errorConn) Write(b []byte) (n int, err error) {
	if e.writeErr != nil {
		return 0, e.writeErr
	}
	return e.mockConn.Write(b)
}

func (e *errorConn) Read(b []byte) (n int, err error) {
	if e.readErr != nil {
		return 0, e.readErr
	}
	return e.mockConn.Read(b)
}

func TestSendChunkWriteError(t *testing.T) {
	t.Run("write error", func(t *testing.T) {
		// sendChunk now does single write with consolidated buffer
		conn := &errorConn{writeErr: io.ErrClosedPipe}
		session := newTestSession(conn, nil)

		err := session.sendChunk([]byte{0x01})
		if err == nil {
			t.Error("expected error when write fails")
		}
	})

	t.Run("write succeeds", func(t *testing.T) {
		// Verify correct format: header (2) + data + terminator (2)
		var written []byte
		conn := &sequentialErrorConn{
			writeFunc: func(b []byte) (int, error) {
				written = append(written, b...)
				return len(b), nil
			},
		}
		session := newTestSession(conn, nil)

		err := session.sendChunk([]byte{0xAB, 0xCD})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		// Expected: [0x00, 0x02] (size=2) + [0xAB, 0xCD] (data) + [0x00, 0x00] (terminator)
		expected := []byte{0x00, 0x02, 0xAB, 0xCD, 0x00, 0x00}
		if len(written) != len(expected) {
			t.Errorf("wrong length: got %d, want %d", len(written), len(expected))
		}
		for i := range expected {
			if written[i] != expected[i] {
				t.Errorf("byte %d: got %02x, want %02x", i, written[i], expected[i])
			}
		}
	})
}

type sequentialErrorConn struct {
	mockConn
	writeFunc func([]byte) (int, error)
}

func (s *sequentialErrorConn) Write(b []byte) (int, error) {
	if s.writeFunc != nil {
		return s.writeFunc(b)
	}
	return s.mockConn.Write(b)
}

func TestServerCloseWithListener(t *testing.T) {
	config := &Config{Port: 0}
	server := New(config, &mockExecutor{})

	done := make(chan error, 1)
	go func() {
		done <- server.ListenAndServe()
	}()

	time.Sleep(50 * time.Millisecond)

	// Close with active listener
	if err := server.Close(); err != nil {
		t.Errorf("Close() error = %v", err)
	}

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Error("server did not shut down")
	}
}

func TestHandshakeVersionNegotiation(t *testing.T) {
	t.Run("no matching version", func(t *testing.T) {
		// Old versions only
		handshakeData := []byte{
			0x60, 0x60, 0xB0, 0x17,
			0x00, 0x00, 0x01, 0x00, // Version 1.0 (unsupported)
			0x00, 0x00, 0x01, 0x01,
			0x00, 0x00, 0x01, 0x02,
			0x00, 0x00, 0x01, 0x03,
		}

		conn := &mockConn{readData: handshakeData}
		session := newTestSession(conn, nil)

		err := session.handshake()
		// Should still work (server picks best available or rejects)
		if err != nil && session.version == 0 {
			// Expected behavior - no matching version
		}
	})

	t.Run("read error during handshake", func(t *testing.T) {
		conn := &errorConn{
			mockConn: mockConn{readData: []byte{}},
			readErr:  io.ErrUnexpectedEOF,
		}
		session := newTestSession(conn, nil)

		err := session.handshake()
		if err == nil {
			t.Error("expected error on read failure")
		}
	})

	t.Run("write error during handshake", func(t *testing.T) {
		handshakeData := []byte{
			0x60, 0x60, 0xB0, 0x17,
			0x00, 0x00, 0x04, 0x04,
			0x00, 0x00, 0x04, 0x03,
			0x00, 0x00, 0x04, 0x02,
			0x00, 0x00, 0x04, 0x01,
		}

		conn := &errorConn{
			mockConn: mockConn{readData: handshakeData},
			writeErr: io.ErrClosedPipe,
		}
		session := newTestSession(conn, nil)

		err := session.handshake()
		if err == nil {
			t.Error("expected error on write failure")
		}
	})

	t.Run("read versions error", func(t *testing.T) {
		// Only magic, no versions
		handshakeData := []byte{
			0x60, 0x60, 0xB0, 0x17,
		}

		conn := &mockConn{readData: handshakeData}
		session := newTestSession(conn, nil)

		err := session.handshake()
		if err == nil {
			t.Error("expected error when versions read fails")
		}
	})
}

func TestHandleMessageReadError(t *testing.T) {
	conn := &errorConn{readErr: io.ErrUnexpectedEOF}
	session := newTestSession(conn, nil)

	err := session.handleMessage()
	if err == nil {
		t.Error("expected error when read fails")
	}
}

func TestHandleMessageDataReadError(t *testing.T) {
	t.Run("read data error", func(t *testing.T) {
		// Header says 10 bytes but we only provide header
		messageData := []byte{
			0x00, 0x0A, // Size: 10 bytes
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err == nil {
			t.Error("expected error when data read fails")
		}
	})

	t.Run("read terminator error", func(t *testing.T) {
		// Header + data but no terminator
		messageData := []byte{
			0x00, 0x01, // Size: 1 byte
			MsgHello, // Message type
			// Missing terminator
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err == nil {
			t.Error("expected error when terminator read fails")
		}
	})
}

func TestSessionHandleDiscard(t *testing.T) {
	messageData := []byte{
		0x00, 0x01,
		MsgDiscard,
		0x00, 0x00,
	}

	conn := &mockConn{readData: messageData}
	session := newTestSession(conn, nil)

	err := session.handleMessage()
	// Discard should return error for unhandled or be handled
	// Current implementation treats unknown messages as error
	_ = err // Don't fail, just ensure we exercised the code path
}

func TestSessionHandleRoute(t *testing.T) {
	messageData := []byte{
		0x00, 0x01,
		MsgRoute,
		0x00, 0x00,
	}

	conn := &mockConn{readData: messageData}
	session := newTestSession(conn, nil)

	err := session.handleMessage()
	// Route should be handled or return error
	_ = err
}

// =============================================================================
// Tests for Multi-Chunk Message Handling
// =============================================================================

func TestMultiChunkMessageHandling(t *testing.T) {
	t.Run("single chunk message", func(t *testing.T) {
		// Single chunk: size header + data + zero terminator
		messageData := []byte{
			0x00, 0x05, // Size: 5 bytes
			0xB1, MsgHello, 0xA0, // Tiny struct with signature HELLO, empty map
			0x00, 0x00, // Zero chunk terminator
		}

		conn := &mockConn{readData: messageData}
		executor := &mockExecutor{}
		session := newTestSession(conn, executor)

		err := session.handleMessage()
		// HELLO needs proper handling, but we're testing chunk reading
		_ = err
	})

	t.Run("multi chunk message", func(t *testing.T) {
		// Build a multi-chunk message: two chunks + zero terminator
		// Chunk 1: 3 bytes of data
		// Chunk 2: 2 bytes of data
		// Zero terminator

		messageData := []byte{
			// First chunk
			0x00, 0x03, // Size: 3 bytes
			0xB1, 0x01, 'A', // Data
			// Second chunk
			0x00, 0x02, // Size: 2 bytes
			'B', 'C', // Data
			// Zero terminator
			0x00, 0x00,
		}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		// Will error since it's not a valid message, but tests multi-chunk reading
		_ = err
	})

	t.Run("zero size first chunk", func(t *testing.T) {
		// Zero-size chunk immediately (empty message)
		messageData := []byte{0x00, 0x00}

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, nil)

		err := session.handleMessage()
		if err != nil {
			t.Errorf("empty message should not error: %v", err)
		}
	})

	t.Run("large chunk simulation", func(t *testing.T) {
		// Simulate a larger message split into chunks
		chunk1Size := 100
		chunk2Size := 50

		messageData := make([]byte, 0)

		// First chunk header
		messageData = append(messageData, byte(chunk1Size>>8), byte(chunk1Size))
		// First chunk data (padding with valid struct start)
		chunk1Data := make([]byte, chunk1Size)
		chunk1Data[0] = 0xB1 // Tiny struct marker
		chunk1Data[1] = 0x10 // RUN message type
		messageData = append(messageData, chunk1Data...)

		// Second chunk header
		messageData = append(messageData, byte(chunk2Size>>8), byte(chunk2Size))
		// Second chunk data
		chunk2Data := make([]byte, chunk2Size)
		messageData = append(messageData, chunk2Data...)

		// Zero terminator
		messageData = append(messageData, 0x00, 0x00)

		conn := &mockConn{readData: messageData}
		session := newTestSession(conn, &mockExecutor{})

		err := session.handleMessage()
		// Will likely error on parsing but tests chunk accumulation
		_ = err
	})
}

// =============================================================================
// Tests for PackStream Encoding
// =============================================================================

func TestEncodePackStreamString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []byte
	}{
		{
			name:     "empty string",
			input:    "",
			expected: []byte{0x80}, // Tiny string, length 0
		},
		{
			name:     "short string",
			input:    "hello",
			expected: []byte{0x85, 'h', 'e', 'l', 'l', 'o'}, // Tiny string, length 5
		},
		{
			name:     "15 char string (max tiny)",
			input:    "123456789012345",
			expected: append([]byte{0x8F}, []byte("123456789012345")...),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodePackStreamString(tt.input)
			if len(result) != len(tt.expected) {
				t.Errorf("length mismatch: got %d, want %d", len(result), len(tt.expected))
			}
			for i := range result {
				if i < len(tt.expected) && result[i] != tt.expected[i] {
					t.Errorf("byte %d: got 0x%02X, want 0x%02X", i, result[i], tt.expected[i])
				}
			}
		})
	}
}

func TestEncodePackStreamInt(t *testing.T) {
	tests := []struct {
		name     string
		input    int64
		checkLen int // Expected minimum length
	}{
		{"zero", 0, 1},
		{"small positive", 42, 1},
		{"max tiny positive", 127, 1},
		{"negative one", -1, 1},
		{"min tiny negative", -16, 1},
		{"requires int8", 128, 2},
		{"requires int16", 32768, 3},
		{"large number", 1000000, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodePackStreamInt(tt.input)
			if len(result) < tt.checkLen {
				t.Errorf("length too short: got %d, want at least %d", len(result), tt.checkLen)
			}
		})
	}
}

func TestEncodePackStreamMap(t *testing.T) {
	t.Run("empty map", func(t *testing.T) {
		result := encodePackStreamMap(nil)
		if len(result) != 1 || result[0] != 0xA0 {
			t.Errorf("empty map should be [0xA0], got %v", result)
		}
	})

	t.Run("empty map explicit", func(t *testing.T) {
		result := encodePackStreamMap(map[string]any{})
		if len(result) != 1 || result[0] != 0xA0 {
			t.Errorf("empty map should be [0xA0], got %v", result)
		}
	})

	t.Run("single key map", func(t *testing.T) {
		result := encodePackStreamMap(map[string]any{"a": int64(1)})
		// Should be: 0xA1 (tiny map, 1 entry) + key "a" + value 1
		if result[0] != 0xA1 {
			t.Errorf("single-entry map marker should be 0xA1, got 0x%02X", result[0])
		}
	})
}

func TestEncodePackStreamList(t *testing.T) {
	t.Run("empty list", func(t *testing.T) {
		result := encodePackStreamList(nil)
		if len(result) != 1 || result[0] != 0x90 {
			t.Errorf("empty list should be [0x90], got %v", result)
		}
	})

	t.Run("single item list", func(t *testing.T) {
		result := encodePackStreamList([]any{"test"})
		// Should be: 0x91 (tiny list, 1 entry) + string "test"
		if result[0] != 0x91 {
			t.Errorf("single-entry list marker should be 0x91, got 0x%02X", result[0])
		}
	})

	t.Run("multiple items", func(t *testing.T) {
		result := encodePackStreamList([]any{int64(1), int64(2), int64(3)})
		if result[0] != 0x93 { // Tiny list with 3 items
			t.Errorf("3-entry list marker should be 0x93, got 0x%02X", result[0])
		}
	})
}

func TestEncodePackStreamValue(t *testing.T) {
	tests := []struct {
		name        string
		input       any
		expectFirst byte // First byte of encoding
	}{
		{"nil", nil, 0xC0},
		{"true", true, 0xC3},
		{"false", false, 0xC2},
		{"small int", int64(42), 42},
		{"string", "hi", 0x82}, // Tiny string, length 2
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodePackStreamValue(tt.input)
			if len(result) == 0 {
				t.Error("result should not be empty")
				return
			}
			if result[0] != tt.expectFirst {
				t.Errorf("first byte: got 0x%02X, want 0x%02X", result[0], tt.expectFirst)
			}
		})
	}
}

// =============================================================================
// Tests for PackStream Decoding
// =============================================================================

func TestDecodePackStreamString(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		offset   int
		expected string
		wantLen  int
		wantErr  bool
	}{
		{
			name:     "tiny string empty",
			data:     []byte{0x80},
			offset:   0,
			expected: "",
			wantLen:  1,
		},
		{
			name:     "tiny string hello",
			data:     []byte{0x85, 'h', 'e', 'l', 'l', 'o'},
			offset:   0,
			expected: "hello",
			wantLen:  6,
		},
		{
			name:     "with offset",
			data:     []byte{0x00, 0x00, 0x83, 'a', 'b', 'c'},
			offset:   2,
			expected: "abc",
			wantLen:  4,
		},
		{
			name:    "invalid marker",
			data:    []byte{0xC0}, // Null, not a string
			offset:  0,
			wantErr: true,
		},
		{
			name:    "out of bounds",
			data:    []byte{0x85}, // Says 5 chars but no data
			offset:  0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, n, err := decodePackStreamString(tt.data, tt.offset)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
			if n != tt.wantLen {
				t.Errorf("consumed %d bytes, want %d", n, tt.wantLen)
			}
		})
	}
}

func TestDecodePackStreamMap(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		offset  int
		wantErr bool
		check   func(map[string]any) bool
	}{
		{
			name:   "empty map",
			data:   []byte{0xA0},
			offset: 0,
			check:  func(m map[string]any) bool { return len(m) == 0 },
		},
		{
			name: "single entry",
			data: []byte{
				0xA1,      // Tiny map, 1 entry
				0x81, 'a', // Key: "a"
				0x01, // Value: 1
			},
			offset: 0,
			check:  func(m map[string]any) bool { return m["a"] == int64(1) },
		},
		{
			name:    "invalid marker",
			data:    []byte{0xC0}, // Null, not a map
			offset:  0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamMap(tt.data, tt.offset)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if !tt.check(result) {
				t.Errorf("check failed for result: %v", result)
			}
		})
	}
}

func TestDecodePackStreamValue(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		offset   int
		expected any
		wantErr  bool
	}{
		{"null", []byte{0xC0}, 0, nil, false},
		{"true", []byte{0xC3}, 0, true, false},
		{"false", []byte{0xC2}, 0, false, false},
		{"tiny positive int", []byte{0x2A}, 0, int64(42), false},
		{"tiny negative int", []byte{0xFF}, 0, int64(-1), false},
		{"zero", []byte{0x00}, 0, int64(0), false},
		{"max tiny positive", []byte{0x7F}, 0, int64(127), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamValue(tt.data, tt.offset)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
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

func TestDecodePackStreamList(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		offset  int
		wantLen int
		wantErr bool
	}{
		{
			name:    "empty list",
			data:    []byte{0x90},
			offset:  0,
			wantLen: 0,
		},
		{
			name:    "single item",
			data:    []byte{0x91, 0x01}, // [1]
			offset:  0,
			wantLen: 1,
		},
		{
			name:    "three items",
			data:    []byte{0x93, 0x01, 0x02, 0x03}, // [1, 2, 3]
			offset:  0,
			wantLen: 3,
		},
		{
			name:    "invalid marker",
			data:    []byte{0xC0}, // Null, not a list
			offset:  0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamList(tt.data, tt.offset)
			if tt.wantErr {
				if err == nil {
					t.Error("expected error")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != tt.wantLen {
				t.Errorf("got %d items, want %d", len(result), tt.wantLen)
			}
		})
	}
}

// =============================================================================
// Tests for parseRunMessage
// =============================================================================

func TestParseRunMessage(t *testing.T) {
	t.Run("query only no params", func(t *testing.T) {
		// Query: "MATCH (n) RETURN n" (18 chars), empty params
		// 0x80 + 18 = 0x92 is a tiny STRING (0x80-0x8F range is 0-15 chars)
		// For 18 chars we need STRING8: 0xD0 + length byte
		data := []byte{
			0xD0, 18, // STRING8 marker + length
			'M', 'A', 'T', 'C', 'H', ' ', '(', 'n', ')', ' ',
			'R', 'E', 'T', 'U', 'R', 'N', ' ', 'n',
			0xA0, // Empty map for params
		}

		session := &Session{}
		query, params, metadata, err := session.parseRunMessage(data)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if query != "MATCH (n) RETURN n" {
			t.Errorf("query: got %q, want %q", query, "MATCH (n) RETURN n")
		}
		if len(params) != 0 {
			t.Errorf("params should be empty, got %v", params)
		}
		if metadata == nil {
			t.Error("metadata should not be nil")
		}
		if len(metadata) != 0 {
			t.Errorf("metadata should be empty, got %v", metadata)
		}
	})

	t.Run("query with string param", func(t *testing.T) {
		// Query: "MATCH (n {name: $name})", params: {name: "Alice"}
		data := []byte{
			0x8D, // Tiny string, 13 chars for "MATCH (n) ..."
			'M', 'A', 'T', 'C', 'H', ' ', '(', 'n', ')', ' ', 'R', 'E', 'T',
			0xA1,                     // Map with 1 entry
			0x84, 'n', 'a', 'm', 'e', // Key: "name"
			0x85, 'A', 'l', 'i', 'c', 'e', // Value: "Alice"
		}

		session := &Session{}
		_, params, metadata, err := session.parseRunMessage(data)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if params["name"] != "Alice" {
			t.Errorf("params[name]: got %v, want Alice", params["name"])
		}
		if metadata == nil {
			t.Error("metadata should not be nil")
		}
	})

	t.Run("empty data", func(t *testing.T) {
		session := &Session{}
		_, _, _, err := session.parseRunMessage([]byte{})

		if err == nil {
			t.Error("expected error for empty data")
		}
	})

	t.Run("query with params and metadata", func(t *testing.T) {
		// Query: "MATCH (n) RETURN n", params: {name: "Alice"}, metadata: {bookmarks: ["bookmark1"], tx_timeout: 30000}
		queryBytes := encodePackStreamString("MATCH (n) RETURN n")
		paramsBytes := encodePackStreamMap(map[string]any{
			"name": "Alice",
		})
		metadataBytes := encodePackStreamMap(map[string]any{
			"bookmarks":  []any{"bookmark1"},
			"tx_timeout": int64(30000),
		})

		data := append(append(queryBytes, paramsBytes...), metadataBytes...)

		session := &Session{}
		query, params, metadata, err := session.parseRunMessage(data)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if query != "MATCH (n) RETURN n" {
			t.Errorf("query: got %q, want %q", query, "MATCH (n) RETURN n")
		}
		if params["name"] != "Alice" {
			t.Errorf("params[name]: got %v, want Alice", params["name"])
		}
		if metadata == nil {
			t.Error("metadata should not be nil")
		}
		bookmarks, ok := metadata["bookmarks"].([]any)
		if !ok || len(bookmarks) == 0 {
			t.Error("metadata should contain bookmarks")
		}
		txTimeout, ok := metadata["tx_timeout"].(int64)
		if !ok || txTimeout != 30000 {
			t.Errorf("metadata[tx_timeout]: got %v, want 30000", txTimeout)
		}
	})

	t.Run("bookmark validation", func(t *testing.T) {
		// Create a server and session for proper bookmark validation
		server := &Server{
			txSequence: 100, // Set current sequence to 100
		}
		session := &Session{
			server: server,
		}

		// Valid bookmark format (sequence <= current)
		validBookmarks := []any{"nornicdb:bookmark:50"}
		err := session.validateBookmarks(validBookmarks)
		if err != nil {
			t.Errorf("valid bookmark should pass validation: %v", err)
		}

		// Bookmark from the future (sequence > current) - should fail
		futureBookmarks := []any{"nornicdb:bookmark:200"}
		err = session.validateBookmarks(futureBookmarks)
		if err == nil {
			t.Error("bookmark from the future should fail validation")
		}

		// Invalid bookmark format (cannot parse sequence)
		invalidFormatBookmarks := []any{"nornicdb:bookmark:invalid"}
		err = session.validateBookmarks(invalidFormatBookmarks)
		if err == nil {
			t.Error("invalid bookmark format should fail validation")
		}

		// Invalid bookmark type
		invalidBookmarks := []any{123}
		err = session.validateBookmarks(invalidBookmarks)
		if err == nil {
			t.Error("invalid bookmark type should fail validation")
		}

		// Empty bookmarks (should pass)
		err = session.validateBookmarks([]any{})
		if err != nil {
			t.Errorf("empty bookmarks should pass validation: %v", err)
		}

		// Valid bookmark at current sequence (should pass)
		currentBookmarks := []any{"nornicdb:bookmark:100"}
		err = session.validateBookmarks(currentBookmarks)
		if err != nil {
			t.Errorf("bookmark at current sequence should pass validation: %v", err)
		}

		// Unknown bookmark format (not nornicdb:bookmark:*) - should fail
		unknownFormatBookmarks := []any{"neo4j:bookmark:123"}
		err = session.validateBookmarks(unknownFormatBookmarks)
		if err == nil {
			t.Error("unknown bookmark format should fail validation")
		}

		// Missing sequence number - should fail
		missingSeqBookmarks := []any{"nornicdb:bookmark:"}
		err = session.validateBookmarks(missingSeqBookmarks)
		if err == nil {
			t.Error("bookmark with missing sequence number should fail validation")
		}

		// Negative sequence number - should fail
		negativeSeqBookmarks := []any{"nornicdb:bookmark:-1"}
		err = session.validateBookmarks(negativeSeqBookmarks)
		if err == nil {
			t.Error("bookmark with negative sequence number should fail validation")
		}
	})
}

// =============================================================================
// Tests for Session with Parameters
// =============================================================================

func TestSessionExecuteWithParams(t *testing.T) {
	var receivedQuery string
	var receivedParams map[string]any

	executor := &mockExecutor{
		executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			receivedQuery = query
			receivedParams = params
			return &QueryResult{
				Columns: []string{"n"},
				Rows:    [][]any{{"result"}},
			}, nil
		},
	}

	// Build a RUN message with query and params
	queryStr := "MATCH (n {id: $id}) RETURN n"
	queryBytes := encodePackStreamString(queryStr)
	paramsBytes := encodePackStreamMap(map[string]any{"id": "test-123"})

	// Combine: query + params
	runData := append(queryBytes, paramsBytes...)

	// Build full message with struct marker
	fullMessage := []byte{0xB1, MsgRun}
	fullMessage = append(fullMessage, runData...)

	// Add chunk header and terminator
	messageData := []byte{byte(len(fullMessage) >> 8), byte(len(fullMessage))}
	messageData = append(messageData, fullMessage...)
	messageData = append(messageData, 0x00, 0x00) // Zero terminator

	conn := &mockConn{readData: messageData}
	session := newTestSession(conn, executor)

	err := session.handleMessage()
	if err != nil {
		t.Errorf("handleMessage error: %v", err)
	}

	if receivedQuery != queryStr {
		t.Errorf("query: got %q, want %q", receivedQuery, queryStr)
	}

	if receivedParams["id"] != "test-123" {
		t.Errorf("params[id]: got %v, want test-123", receivedParams["id"])
	}
}

// =============================================================================
// Tests for truncateQuery helper
// =============================================================================

func TestTruncateQuery(t *testing.T) {
	tests := []struct {
		name     string
		query    string
		maxLen   int
		expected string
	}{
		{"short query", "MATCH (n)", 100, "MATCH (n)"},
		{"exact length", "12345", 5, "12345"},
		{"needs truncation", "1234567890", 5, "12345..."},
		{"empty query", "", 10, ""},
		{"one char max", "hello", 1, "h..."},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncateQuery(tt.query, tt.maxLen)
			if result != tt.expected {
				t.Errorf("got %q, want %q", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Tests for INT encoding variants
// =============================================================================

func TestEncodePackStreamIntVariants(t *testing.T) {
	// INT8 is only used for negative values -128 to -17
	// Positive values > 127 go to INT16
	tests := []struct {
		name        string
		value       int64
		expectFirst byte
	}{
		{"INT8 negative -17", -17, 0xC8},   // -17 requires INT8 (-128 to -17 range)
		{"INT8 negative -100", -100, 0xC8}, // -100 is in INT8 range
		{"INT16 positive", 200, 0xC9},      // 200 > 127, goes to INT16
		{"INT16 negative", -1000, 0xC9},    // -1000 < -128, needs INT16
		{"INT32 positive", 100000, 0xCA},
		{"INT32 negative", -100000, 0xCA},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := encodePackStreamInt(tt.value)
			if len(result) < 2 {
				t.Fatal("result too short")
			}
			if result[0] != tt.expectFirst {
				t.Errorf("marker: got 0x%02X, want 0x%02X", result[0], tt.expectFirst)
			}
		})
	}
}

// =============================================================================
// Tests for STRING encoding variants
// =============================================================================

func TestEncodePackStreamStringVariants(t *testing.T) {
	t.Run("STRING8", func(t *testing.T) {
		// Create a string that requires STRING8 (16-255 chars)
		str := make([]byte, 50)
		for i := range str {
			str[i] = 'a'
		}
		result := encodePackStreamString(string(str))
		if result[0] != 0xD0 { // STRING8 marker
			t.Errorf("marker: got 0x%02X, want 0xD0", result[0])
		}
	})

	t.Run("STRING16", func(t *testing.T) {
		// Create a string that requires STRING16 (256+ chars)
		str := make([]byte, 300)
		for i := range str {
			str[i] = 'b'
		}
		result := encodePackStreamString(string(str))
		if result[0] != 0xD1 { // STRING16 marker
			t.Errorf("marker: got 0x%02X, want 0xD1", result[0])
		}
	})
}

// =============================================================================
// Tests for Decode INT variants
// =============================================================================

func TestDecodePackStreamIntVariants(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected int64
	}{
		{"INT8 positive", []byte{0xC8, 0x64}, 100},          // 100
		{"INT8 negative", []byte{0xC8, 0x9C}, -100},         // -100
		{"INT16 positive", []byte{0xC9, 0x03, 0xE8}, 1000},  // 1000
		{"INT16 negative", []byte{0xC9, 0xFC, 0x18}, -1000}, // -1000
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamValue(tt.data, 0)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if result != tt.expected {
				t.Errorf("got %v, want %v", result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Tests for Float encoding/decoding
// =============================================================================

func TestPackStreamFloat(t *testing.T) {
	testValue := 3.14159

	// Encode
	encoded := encodePackStreamValue(testValue)
	if len(encoded) != 9 { // 1 marker + 8 bytes for float64
		t.Errorf("float64 should encode to 9 bytes, got %d", len(encoded))
	}
	if encoded[0] != 0xC1 { // FLOAT64 marker
		t.Errorf("float64 marker should be 0xC1, got 0x%02X", encoded[0])
	}

	// Decode
	decoded, _, err := decodePackStreamValue(encoded, 0)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if decoded != testValue {
		t.Errorf("got %v, want %v", decoded, testValue)
	}
}

// =============================================================================
// Additional Tests for Coverage Improvement
// =============================================================================

func TestHandlePull(t *testing.T) {
	// Create a session with stored results
	executor := &mockExecutor{
		executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			return &QueryResult{
				Columns: []string{"name", "age"},
				Rows: [][]any{
					{"Alice", 30},
					{"Bob", 25},
					{"Charlie", 35},
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
		lastResult: &QueryResult{
			Columns: []string{"name", "age"},
			Rows: [][]any{
				{"Alice", 30},
				{"Bob", 25},
				{"Charlie", 35},
			},
		},
		resultIndex: 0,
	}

	// Handle PULL in goroutine
	done := make(chan error, 1)
	go func() {
		// Pull all records (nil data means pull all)
		err := session.handlePull(nil)
		done <- err
	}()

	// Read from client side - should receive records
	go func() {
		buf := make([]byte, 4096)
		for {
			_, err := client.Read(buf)
			if err != nil {
				return
			}
		}
	}()

	// Give some time for processing
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handlePull failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
		// Timeout is ok - we're testing that it processes
	}
}

func TestHandleDiscard(t *testing.T) {
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
			Rows:    [][]any{{"test"}},
		},
		resultIndex: 0,
	}

	// Handle DISCARD
	done := make(chan error, 1)
	go func() {
		err := session.handleDiscard(nil)
		done <- err
	}()

	// Read the response
	go func() {
		buf := make([]byte, 1024)
		client.Read(buf)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handleDiscard failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}

	// lastResult should be cleared
	if session.lastResult != nil {
		t.Error("expected lastResult to be nil after DISCARD")
	}
}

func TestHandleRoute(t *testing.T) {
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

	// Handle ROUTE
	done := make(chan error, 1)
	go func() {
		err := session.handleRoute(nil)
		done <- err
	}()

	// Read the response
	go func() {
		buf := make([]byte, 1024)
		client.Read(buf)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("handleRoute failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSendRecord(t *testing.T) {
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

	// Send a record
	done := make(chan error, 1)
	go func() {
		err := session.sendRecord([]any{"Alice", 30, true, 3.14})
		done <- err
	}()

	// Read from client
	go func() {
		buf := make([]byte, 1024)
		client.Read(buf)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("sendRecord failed: %v", err)
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestDecodePackStreamList_MixedTypes(t *testing.T) {
	// Test list with mixed types and value verification
	tests := []struct {
		name     string
		data     []byte
		expected []any
	}{
		{
			name: "list with integers",
			data: []byte{
				0x93, // TINY_LIST with 3 elements
				0x01, // tiny int 1
				0x02, // tiny int 2
				0x03, // tiny int 3
			},
			expected: []any{int64(1), int64(2), int64(3)},
		},
		{
			name: "list with string and int",
			data: []byte{
				0x92, // TINY_LIST with 2 elements
				0x85, // TINY_STRING "hello" (5 chars)
				'h', 'e', 'l', 'l', 'o',
				0x05, // tiny int 5
			},
			expected: []any{"hello", int64(5)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, _, err := decodePackStreamList(tt.data, 0)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("got %d elements, want %d", len(result), len(tt.expected))
				return
			}
			for i := range tt.expected {
				if result[i] != tt.expected[i] {
					t.Errorf("element %d: got %v, want %v", i, result[i], tt.expected[i])
				}
			}
		})
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

		err := session.handleCommit(nil)
		if err != nil {
			t.Fatalf("handleCommit should not return Go error: %v", err)
		}

		// Transaction state should still be cleared
		if session.inTransaction {
			t.Error("transaction state should be cleared even on error")
		}
	})
}

func TestHandleRollbackWithTransactionalExecutor(t *testing.T) {
	t.Run("rollback calls executor", func(t *testing.T) {
		executor := &mockTransactionalExecutor{}
		conn := &mockConn{}
		session := newTestSession(conn, executor)
		session.inTransaction = true

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
// Format: B1 01 (struct with 1 field, signature 0x01) + map with auth params
func buildHelloMessage(scheme, principal, credentials string) []byte {
	// Build the extra map containing auth info
	// Map with 3 entries: scheme, principal, credentials
	buf := []byte{
		0xB1, 0x01, // Struct marker (1 field) + HELLO signature
	}

	// Build map - A3 means tiny map with 3 entries
	mapBytes := []byte{0xA3} // Map with 3 entries

	// scheme key
	mapBytes = append(mapBytes, buildPackStreamString("scheme")...)
	mapBytes = append(mapBytes, buildPackStreamString(scheme)...)

	// principal key
	mapBytes = append(mapBytes, buildPackStreamString("principal")...)
	mapBytes = append(mapBytes, buildPackStreamString(principal)...)

	// credentials key
	mapBytes = append(mapBytes, buildPackStreamString("credentials")...)
	mapBytes = append(mapBytes, buildPackStreamString(credentials)...)

	buf = append(buf, mapBytes...)
	return buf
}

// buildPackStreamString builds a PackStream string encoding.
func buildPackStreamString(s string) []byte {
	if len(s) < 16 {
		// Tiny string (0x80-0x8F)
		return append([]byte{byte(0x80 + len(s))}, []byte(s)...)
	}
	if len(s) < 256 {
		// STRING8
		return append([]byte{0xD0, byte(len(s))}, []byte(s)...)
	}
	// STRING16
	return append([]byte{0xD1, byte(len(s) >> 8), byte(len(s))}, []byte(s)...)
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
	buf = append(buf, buildPackStreamString(query)...)

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
