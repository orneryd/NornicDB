// Package bolt implements the Neo4j Bolt protocol server for NornicDB.
package bolt

import (
	"fmt"
	"io"
	"net"
	"testing"
)

// ============================================================================
// Message Builders
// ============================================================================

// BuildHelloMessage builds a PackStream HELLO message.
// If authParams is nil, uses an empty map (0xA0).
// If authParams is provided, encodes it as a map with scheme, principal, credentials.
func BuildHelloMessage(authParams map[string]string) []byte {
	buf := []byte{0xB1, MsgHello} // Struct marker (1 field) + HELLO signature

	if len(authParams) == 0 {
		// Empty map
		buf = append(buf, 0xA0)
		return buf
	}

	// Build map with auth params
	mapSize := len(authParams)
	var mapBytes []byte

	if mapSize < 16 {
		// Tiny map (0xA0-0xAF)
		mapBytes = []byte{byte(0xA0 + mapSize)}
	} else if mapSize < 256 {
		// MAP8
		mapBytes = []byte{0xD8, byte(mapSize)}
	} else {
		// MAP16
		mapBytes = []byte{0xD9, byte(mapSize >> 8), byte(mapSize)}
	}

	// Add key-value pairs
	for k, v := range authParams {
		mapBytes = append(mapBytes, encodePackStreamString(k)...)
		mapBytes = append(mapBytes, encodePackStreamString(v)...)
	}

	buf = append(buf, mapBytes...)
	return buf
}

// BuildRunMessage builds a PackStream RUN message with query, params, and metadata.
// If params is nil, uses an empty map.
// If metadata is nil, uses an empty map.
func BuildRunMessage(query string, params map[string]any, metadata map[string]any) []byte {
	buf := []byte{0xB1, MsgRun} // Struct marker + RUN signature

	// Query string
	buf = append(buf, encodePackStreamString(query)...)

	// Params map
	if params == nil {
		buf = append(buf, 0xA0) // Empty map
	} else {
		buf = append(buf, encodePackStreamMap(params)...)
	}

	// Extra metadata map (Bolt v4+)
	if metadata == nil {
		buf = append(buf, 0xA0) // Empty map
	} else {
		buf = append(buf, encodePackStreamMap(metadata)...)
	}

	return buf
}

// BuildPullMessage builds a PULL message with options.
// If options is nil, uses an empty map.
func BuildPullMessage(options map[string]any) []byte {
	buf := []byte{0xB1, MsgPull} // Struct marker + PULL signature

	if options == nil {
		buf = append(buf, 0xA0) // Empty map
	} else {
		buf = append(buf, encodePackStreamMap(options)...)
	}

	return buf
}

// BuildBeginMessage builds a BEGIN message with transaction metadata.
// If metadata is nil, uses an empty map.
func BuildBeginMessage(metadata map[string]any) []byte {
	buf := []byte{0xB1, MsgBegin} // Struct marker + BEGIN signature

	if metadata == nil {
		buf = append(buf, 0xA0) // Empty map
	} else {
		buf = append(buf, encodePackStreamMap(metadata)...)
	}

	return buf
}

// BuildCommitMessage builds a COMMIT message (no data).
func BuildCommitMessage() []byte {
	return []byte{0xB0, MsgCommit} // Tiny struct (0 fields) + COMMIT signature
}

// BuildRollbackMessage builds a ROLLBACK message (no data).
func BuildRollbackMessage() []byte {
	return []byte{0xB0, MsgRollback} // Tiny struct (0 fields) + ROLLBACK signature
}

// BuildResetMessage builds a RESET message (no data).
func BuildResetMessage() []byte {
	return []byte{0xB0, MsgReset} // Tiny struct (0 fields) + RESET signature
}

// BuildGoodbyeMessage builds a GOODBYE message (no data).
func BuildGoodbyeMessage() []byte {
	return []byte{0xB0, MsgGoodbye} // Tiny struct (0 fields) + GOODBYE signature
}

// ============================================================================
// Chunk Framing Helpers
// ============================================================================

// SendMessage sends a PackStream message with proper chunk framing.
// Format: [size:2][chunk...][size:2][chunk...][size:2==0]
func SendMessage(conn net.Conn, data []byte) error {
	const maxChunkSize = 0xFFFF

	remaining := data
	for len(remaining) > 0 {
		chunkSize := len(remaining)
		if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}

		header := []byte{byte(chunkSize >> 8), byte(chunkSize)}
		if _, err := conn.Write(header); err != nil {
			return fmt.Errorf("failed to write chunk header: %w", err)
		}
		if _, err := conn.Write(remaining[:chunkSize]); err != nil {
			return fmt.Errorf("failed to write chunk data: %w", err)
		}
		remaining = remaining[chunkSize:]
	}

	// Terminator chunk (size 0)
	if _, err := conn.Write([]byte{0x00, 0x00}); err != nil {
		return fmt.Errorf("failed to write terminator: %w", err)
	}
	return nil
}

// SendMessageWithTesting sends a message and marks the test helper.
func SendMessageWithTesting(t *testing.T, conn net.Conn, data []byte) error {
	t.Helper()
	return SendMessage(conn, data)
}

// ReadMessage reads a complete message from the connection.
// Returns the message type and data, or an error.
func ReadMessage(conn net.Conn) (byte, []byte, error) {
	// Bolt messages are chunked:
	//   [size:2][chunk...][size:2][chunk...][size:2==0]
	// This reads all chunks until the 0-sized terminator chunk.
	var full []byte
	var header [2]byte

	for {
		if _, err := io.ReadFull(conn, header[:]); err != nil {
			return 0, nil, fmt.Errorf("failed to read chunk header: %w", err)
		}
		size := int(header[0])<<8 | int(header[1])
		if size == 0 {
			break
		}

		oldLen := len(full)
		full = append(full, make([]byte, size)...)
		if _, err := io.ReadFull(conn, full[oldLen:oldLen+size]); err != nil {
			return 0, nil, fmt.Errorf("failed to read chunk data: %w", err)
		}
	}

	if len(full) == 0 {
		return 0, nil, fmt.Errorf("message too short: 0 bytes")
	}

	// Bolt messages are PackStream structures. On the wire they are typically:
	//   [struct marker 0xB0-0xBF][signature][fields...]
	// Some older tests historically used "signature-first" payloads; we support both.
	if len(full) >= 2 && full[0] >= 0xB0 && full[0] <= 0xBF {
		return full[1], full[2:], nil
	}

	return full[0], full[1:], nil
}

// ReadMessageWithTesting reads a message and marks the test helper.
func ReadMessageWithTesting(t *testing.T, conn net.Conn) (byte, []byte, error) {
	t.Helper()
	return ReadMessage(conn)
}

// ============================================================================
// Handshake Helpers
// ============================================================================

// PerformHandshake performs the Bolt protocol handshake.
// Sends magic bytes and version negotiation, reads the selected version.
func PerformHandshake(conn net.Conn) error {
	// Send magic + versions
	handshake := []byte{
		0x60, 0x60, 0xB0, 0x17, // Magic
		0x00, 0x00, 0x04, 0x04, // Bolt 4.4
		0x00, 0x00, 0x04, 0x03, // Bolt 4.3
		0x00, 0x00, 0x04, 0x02, // Bolt 4.2
		0x00, 0x00, 0x04, 0x01, // Bolt 4.1
	}

	if _, err := conn.Write(handshake); err != nil {
		return fmt.Errorf("failed to write handshake: %w", err)
	}

	// Read version response (4 bytes)
	resp := make([]byte, 4)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("failed to read version response: %w", err)
	}

	return nil
}

// PerformHandshakeWithTesting performs handshake and marks the test helper.
func PerformHandshakeWithTesting(t *testing.T, conn net.Conn) error {
	t.Helper()
	return PerformHandshake(conn)
}

// ============================================================================
// Convenience Wrappers for Common Test Patterns
// ============================================================================

// SendHello sends a HELLO message with optional auth params.
func SendHello(t *testing.T, conn net.Conn, authParams map[string]string) error {
	t.Helper()
	message := BuildHelloMessage(authParams)
	return SendMessage(conn, message)
}

// SendRun sends a RUN message with query and optional params/metadata.
func SendRun(t *testing.T, conn net.Conn, query string, params map[string]any, metadata map[string]any) error {
	t.Helper()
	message := BuildRunMessage(query, params, metadata)
	return SendMessage(conn, message)
}

// SendPull sends a PULL message with optional options.
func SendPull(t *testing.T, conn net.Conn, options map[string]any) error {
	t.Helper()
	message := BuildPullMessage(options)
	return SendMessage(conn, message)
}

// SendBegin sends a BEGIN message with optional metadata.
func SendBegin(t *testing.T, conn net.Conn, metadata map[string]any) error {
	t.Helper()
	message := BuildBeginMessage(metadata)
	return SendMessage(conn, message)
}

// SendCommit sends a COMMIT message.
func SendCommit(t *testing.T, conn net.Conn) error {
	t.Helper()
	message := BuildCommitMessage()
	return SendMessage(conn, message)
}

// SendRollback sends a ROLLBACK message.
func SendRollback(t *testing.T, conn net.Conn) error {
	t.Helper()
	message := BuildRollbackMessage()
	return SendMessage(conn, message)
}

// SendReset sends a RESET message.
func SendReset(t *testing.T, conn net.Conn) error {
	t.Helper()
	message := BuildResetMessage()
	return SendMessage(conn, message)
}

// SendGoodbye sends a GOODBYE message.
func SendGoodbye(t *testing.T, conn net.Conn) error {
	t.Helper()
	message := BuildGoodbyeMessage()
	return SendMessage(conn, message)
}

// ============================================================================
// Message Assertions
// ============================================================================

// AssertSuccess reads a message and asserts it's a SUCCESS message.
// Returns the metadata map if successful.
func AssertSuccess(t *testing.T, conn net.Conn) (map[string]any, error) {
	t.Helper()

	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	if msgType != MsgSuccess {
		return nil, fmt.Errorf("expected SUCCESS (0x%02X), got 0x%02X", MsgSuccess, msgType)
	}

	// Parse metadata map
	if len(msgData) == 0 {
		return make(map[string]any), nil
	}

	metadata, _, err := decodePackStreamMap(msgData, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to decode SUCCESS metadata: %w", err)
	}

	return metadata, nil
}

// AssertFailure reads a message and asserts it's a FAILURE message.
// Returns the error code and message.
func AssertFailure(t *testing.T, conn net.Conn) (string, string, error) {
	t.Helper()

	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return "", "", fmt.Errorf("failed to read message: %w", err)
	}

	if msgType != MsgFailure {
		return "", "", fmt.Errorf("expected FAILURE (0x%02X), got 0x%02X", MsgFailure, msgType)
	}

	// Parse metadata map
	metadata, _, err := decodePackStreamMap(msgData, 0)
	if err != nil {
		return "", "", fmt.Errorf("failed to decode FAILURE metadata: %w", err)
	}

	code, _ := metadata["code"].(string)
	message, _ := metadata["message"].(string)

	return code, message, nil
}

// AssertRecord reads a message and asserts it's a RECORD message.
// Returns the record fields.
func AssertRecord(t *testing.T, conn net.Conn) ([]any, error) {
	t.Helper()

	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	if msgType != MsgRecord {
		return nil, fmt.Errorf("expected RECORD (0x%02X), got 0x%02X", MsgRecord, msgType)
	}

	// Parse list of fields
	fields, _, err := decodePackStreamList(msgData, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to decode RECORD fields: %w", err)
	}

	return fields, nil
}

// AssertMessageType reads a message and asserts it has the expected type.
// Returns the message data.
func AssertMessageType(t *testing.T, conn net.Conn, expectedType byte) ([]byte, error) {
	t.Helper()

	msgType, msgData, err := ReadMessage(conn)
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %w", err)
	}

	if msgType != expectedType {
		return nil, fmt.Errorf("expected message type 0x%02X, got 0x%02X", expectedType, msgType)
	}

	return msgData, nil
}

// ReadSuccess is a convenience alias for AssertSuccess (backward compatibility).
func ReadSuccess(t *testing.T, conn net.Conn) error {
	t.Helper()
	_, err := AssertSuccess(t, conn)
	return err
}

// ReadMessageType reads a message and returns its type (for backward compatibility).
func ReadMessageType(t *testing.T, conn net.Conn) (byte, error) {
	t.Helper()
	msgType, _, err := ReadMessage(conn)
	return msgType, err
}
