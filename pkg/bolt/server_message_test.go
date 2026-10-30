package bolt

import (
	"io"
	"testing"
)

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
		primeTestTransactionLifecycle(t, session)

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
		primeTestTransactionLifecycle(t, session)

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
		primeTestTransactionLifecycle(t, session)

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
