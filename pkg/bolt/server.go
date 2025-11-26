// Package bolt implements the Neo4j Bolt protocol server for NornicDB.
// This allows existing Neo4j drivers to connect to NornicDB.
package bolt

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
)

// Protocol versions supported
const (
	BoltV4_4 = 0x0404 // Bolt 4.4
	BoltV4_3 = 0x0403 // Bolt 4.3
	BoltV4_2 = 0x0402 // Bolt 4.2
	BoltV4_1 = 0x0401 // Bolt 4.1
	BoltV4_0 = 0x0400 // Bolt 4.0
)

// Message types
const (
	MsgHello    byte = 0x01
	MsgGoodbye  byte = 0x02
	MsgReset    byte = 0x0F
	MsgRun      byte = 0x10
	MsgDiscard  byte = 0x2F
	MsgPull     byte = 0x3F
	MsgBegin    byte = 0x11
	MsgCommit   byte = 0x12
	MsgRollback byte = 0x13
	MsgRoute    byte = 0x66

	// Response messages
	MsgSuccess byte = 0x70
	MsgRecord  byte = 0x71
	MsgIgnored byte = 0x7E
	MsgFailure byte = 0x7F
)

// Server implements a Bolt protocol server.
type Server struct {
	config   *Config
	listener net.Listener
	mu       sync.RWMutex
	sessions map[string]*Session
	closed   atomic.Bool

	// Query executor (injected dependency)
	executor QueryExecutor
}

// QueryExecutor executes Cypher queries.
type QueryExecutor interface {
	Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error)
}

// QueryResult holds the result of a query.
type QueryResult struct {
	Columns []string
	Rows    [][]any
}

// Config holds Bolt server configuration.
type Config struct {
	Port            int
	MaxConnections  int
	ReadBufferSize  int
	WriteBufferSize int
}

// DefaultConfig returns sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Port:            7687,
		MaxConnections:  100,
		ReadBufferSize:  8192,
		WriteBufferSize: 8192,
	}
}

// New creates a new Bolt server.
func New(config *Config, executor QueryExecutor) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	return &Server{
		config:   config,
		sessions: make(map[string]*Session),
		executor: executor,
	}
}

// ListenAndServe starts the Bolt server.
func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf(":%d", s.config.Port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	fmt.Printf("Bolt server listening on bolt://localhost:%d\n", s.config.Port)

	return s.serve()
}

// serve accepts connections in a loop.
func (s *Server) serve() error {
	for {
		if s.closed.Load() {
			return nil
		}

		conn, err := s.listener.Accept()
		if err != nil {
			if s.closed.Load() {
				return nil // Clean shutdown
			}
			continue
		}

		go s.handleConnection(conn)
	}
}

// Close stops the Bolt server.
func (s *Server) Close() error {
	s.closed.Store(true)
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// IsClosed returns whether the server is closed.
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// handleConnection handles a single client connection.
func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	session := &Session{
		conn:     conn,
		server:   s,
		executor: s.executor,
	}

	// Perform handshake
	if err := session.handshake(); err != nil {
		fmt.Printf("Handshake failed: %v\n", err)
		return
	}

	// Handle messages
	for {
		if s.closed.Load() {
			return
		}
		if err := session.handleMessage(); err != nil {
			if err == io.EOF {
				return
			}
			fmt.Printf("Message handling error: %v\n", err)
			return
		}
	}
}

// Session represents a client session.
type Session struct {
	conn     net.Conn
	server   *Server
	executor QueryExecutor
	version  uint32

	// Transaction state
	inTransaction bool

	// Query result state (for streaming with PULL)
	lastResult  *QueryResult
	resultIndex int
}

// handshake performs the Bolt handshake.
func (s *Session) handshake() error {
	// Read magic number (4 bytes: 0x60 0x60 0xB0 0x17)
	magic := make([]byte, 4)
	if _, err := io.ReadFull(s.conn, magic); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}

	if magic[0] != 0x60 || magic[1] != 0x60 || magic[2] != 0xB0 || magic[3] != 0x17 {
		return fmt.Errorf("invalid magic number: %x", magic)
	}

	// Read supported versions (4 x 4 bytes)
	versions := make([]byte, 16)
	if _, err := io.ReadFull(s.conn, versions); err != nil {
		return fmt.Errorf("failed to read versions: %w", err)
	}

	// Select highest supported version
	s.version = BoltV4_4

	// Send selected version
	response := []byte{0x00, 0x00, 0x04, 0x04} // Bolt 4.4
	if _, err := s.conn.Write(response); err != nil {
		return fmt.Errorf("failed to send version: %w", err)
	}

	return nil
}

// handleMessage handles a single Bolt message.
// Bolt messages can span multiple chunks - we read until we get a 0-size chunk.
func (s *Session) handleMessage() error {
	var message []byte
	
	// Read chunks until we get a zero-size chunk (message terminator)
	for {
		// Read chunk header (2 bytes: size)
		header := make([]byte, 2)
		if _, err := io.ReadFull(s.conn, header); err != nil {
			return err
		}

		size := int(header[0])<<8 | int(header[1])
		if size == 0 {
			// Zero-size chunk marks end of message
			break
		}

		// Read chunk data
		chunk := make([]byte, size)
		if _, err := io.ReadFull(s.conn, chunk); err != nil {
			return err
		}
		
		message = append(message, chunk...)
	}

	if len(message) == 0 {
		return nil // Empty message (no-op)
	}

	// Parse and handle message
	if len(message) < 2 {
		return fmt.Errorf("message too short: %d bytes", len(message))
	}

	// Bolt messages are PackStream structures
	structMarker := message[0]

	// Check if it's a tiny struct (0xB0-0xBF)
	if structMarker >= 0xB0 && structMarker <= 0xBF {
		msgType := message[1]
		msgData := message[2:]
		return s.dispatchMessage(msgType, msgData)
	}

	// For non-struct markers, try direct message type (fallback)
	return s.dispatchMessage(message[0], message[1:])
}

// dispatchMessage routes the message to the appropriate handler.
func (s *Session) dispatchMessage(msgType byte, data []byte) error {
	switch msgType {
	case MsgHello:
		return s.handleHello(data)
	case MsgGoodbye:
		return io.EOF
	case MsgRun:
		return s.handleRun(data)
	case MsgPull:
		return s.handlePull(data)
	case MsgDiscard:
		return s.handleDiscard(data)
	case MsgReset:
		return s.handleReset(data)
	case MsgBegin:
		return s.handleBegin(data)
	case MsgCommit:
		return s.handleCommit(data)
	case MsgRollback:
		return s.handleRollback(data)
	case MsgRoute:
		return s.handleRoute(data)
	default:
		return fmt.Errorf("unknown message type: 0x%02X", msgType)
	}
}

// handleHello handles the HELLO message.
func (s *Session) handleHello(data []byte) error {
	// Parse HELLO to extract auth (we accept all for now)
	return s.sendSuccess(map[string]any{
		"server":        "NornicDB/0.1.0",
		"connection_id": "nornic-1",
		"hints":         map[string]any{},
	})
}

// handleRun handles the RUN message (execute Cypher).
func (s *Session) handleRun(data []byte) error {
	// Parse PackStream to extract query and params
	query, params, err := s.parseRunMessage(data)
	if err != nil {
		return s.sendFailure("Neo.ClientError.Request.Invalid", fmt.Sprintf("Failed to parse RUN message: %v", err))
	}

	// Execute query
	ctx := context.Background()
	result, err := s.executor.Execute(ctx, query, params)
	if err != nil {
		return s.sendFailure("Neo.ClientError.Statement.SyntaxError", err.Error())
	}

	// Store result for PULL
	s.lastResult = result
	s.resultIndex = 0

	// Return SUCCESS with field names
	return s.sendSuccess(map[string]any{
		"fields":  result.Columns,
		"t_first": 0,
	})
}

// truncateQuery truncates a query for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// parseRunMessage parses a RUN message to extract query and parameters.
// Bolt v4+ RUN message format: [query: String, parameters: Map, extra: Map]
func (s *Session) parseRunMessage(data []byte) (string, map[string]any, error) {
	if len(data) == 0 {
		return "", nil, fmt.Errorf("empty RUN message")
	}

	offset := 0

	// Parse query string
	query, n, err := decodePackStreamString(data, offset)
	if err != nil {
		return "", nil, fmt.Errorf("failed to parse query: %w", err)
	}
	offset += n

	// Parse parameters map
	params := make(map[string]any)
	if offset < len(data) {
		p, consumed, err := decodePackStreamMap(data, offset)
		if err != nil {
			// Params parse failed, use empty map
			params = make(map[string]any)
		} else {
			params = p
			offset += consumed
		}
	}
	
	// Bolt v4+ has an extra metadata map after params (for bookmarks, tx_timeout, etc.)
	// We can ignore it for now, but we should parse it to avoid issues
	// offset is now pointing to the extra map if present

	return query, params, nil
}

// handlePull handles the PULL message.
func (s *Session) handlePull(data []byte) error {
	if s.lastResult == nil {
		return s.sendSuccess(map[string]any{
			"has_more": false,
		})
	}

	// Parse PULL options (n = number of records to pull)
	pullN := -1 // Default: all records
	if len(data) > 0 {
		opts, _, err := decodePackStreamMap(data, 0)
		if err == nil {
			if n, ok := opts["n"]; ok {
				switch v := n.(type) {
				case int64:
					pullN = int(v)
				case int:
					pullN = v
				}
			}
		}
	}

	// Stream records
	for s.resultIndex < len(s.lastResult.Rows) {
		if pullN == 0 {
			break
		}

		row := s.lastResult.Rows[s.resultIndex]
		if err := s.sendRecord(row); err != nil {
			return err
		}

		s.resultIndex++
		if pullN > 0 {
			pullN--
		}
	}

	// Check if more records available
	hasMore := s.resultIndex < len(s.lastResult.Rows)

	// Clear result if done
	if !hasMore {
		s.lastResult = nil
		s.resultIndex = 0
	}

	return s.sendSuccess(map[string]any{
		"has_more": hasMore,
	})
}

// handleDiscard handles the DISCARD message.
func (s *Session) handleDiscard(data []byte) error {
	s.lastResult = nil
	s.resultIndex = 0
	return s.sendSuccess(map[string]any{
		"has_more": false,
	})
}

// handleRoute handles the ROUTE message (for cluster routing).
func (s *Session) handleRoute(data []byte) error {
	return s.sendSuccess(map[string]any{
		"rt": map[string]any{
			"ttl":     300,
			"servers": []map[string]any{},
		},
	})
}

// handleReset handles the RESET message.
func (s *Session) handleReset(data []byte) error {
	s.inTransaction = false
	s.lastResult = nil
	s.resultIndex = 0
	return s.sendSuccess(nil)
}

// handleBegin handles the BEGIN message.
func (s *Session) handleBegin(data []byte) error {
	s.inTransaction = true
	return s.sendSuccess(nil)
}

// handleCommit handles the COMMIT message.
func (s *Session) handleCommit(data []byte) error {
	s.inTransaction = false
	return s.sendSuccess(nil)
}

// handleRollback handles the ROLLBACK message.
func (s *Session) handleRollback(data []byte) error {
	s.inTransaction = false
	return s.sendSuccess(nil)
}

// sendRecord sends a RECORD response.
func (s *Session) sendRecord(fields []any) error {
	// Format: <struct marker 0xB1> <signature 0x71> <list of fields>
	buf := []byte{0xB1, MsgRecord}
	buf = append(buf, encodePackStreamList(fields)...)
	return s.sendChunk(buf)
}

// sendSuccess sends a SUCCESS response with PackStream encoding.
func (s *Session) sendSuccess(metadata map[string]any) error {
	buf := []byte{0xB1, MsgSuccess}
	buf = append(buf, encodePackStreamMap(metadata)...)
	return s.sendChunk(buf)
}

// sendFailure sends a FAILURE response.
func (s *Session) sendFailure(code, message string) error {
	buf := []byte{0xB1, MsgFailure}
	metadata := map[string]any{
		"code":    code,
		"message": message,
	}
	buf = append(buf, encodePackStreamMap(metadata)...)
	return s.sendChunk(buf)
}

// sendChunk sends a chunk to the client.
func (s *Session) sendChunk(data []byte) error {
	size := len(data)
	header := []byte{byte(size >> 8), byte(size)}

	if _, err := s.conn.Write(header); err != nil {
		return err
	}

	if _, err := s.conn.Write(data); err != nil {
		return err
	}

	// Chunk terminator
	terminator := []byte{0x00, 0x00}
	if _, err := s.conn.Write(terminator); err != nil {
		return err
	}

	return nil
}

// ============================================================================
// PackStream Encoding
// ============================================================================

func encodePackStreamMap(m map[string]any) []byte {
	if m == nil || len(m) == 0 {
		return []byte{0xA0}
	}

	var buf []byte
	size := len(m)
	if size < 16 {
		buf = append(buf, byte(0xA0+size))
	} else if size < 256 {
		buf = append(buf, 0xD8, byte(size))
	} else {
		buf = append(buf, 0xD9, byte(size>>8), byte(size))
	}

	for k, v := range m {
		buf = append(buf, encodePackStreamString(k)...)
		buf = append(buf, encodePackStreamValue(v)...)
	}

	return buf
}

func encodePackStreamList(items []any) []byte {
	if items == nil || len(items) == 0 {
		return []byte{0x90}
	}

	var buf []byte
	size := len(items)
	if size < 16 {
		buf = append(buf, byte(0x90+size))
	} else if size < 256 {
		buf = append(buf, 0xD4, byte(size))
	} else {
		buf = append(buf, 0xD5, byte(size>>8), byte(size))
	}

	for _, item := range items {
		buf = append(buf, encodePackStreamValue(item)...)
	}

	return buf
}

func encodePackStreamString(s string) []byte {
	length := len(s)
	var buf []byte

	if length < 16 {
		buf = append(buf, byte(0x80+length))
	} else if length < 256 {
		buf = append(buf, 0xD0, byte(length))
	} else if length < 65536 {
		buf = append(buf, 0xD1, byte(length>>8), byte(length))
	} else {
		buf = append(buf, 0xD2, byte(length>>24), byte(length>>16), byte(length>>8), byte(length))
	}

	buf = append(buf, []byte(s)...)
	return buf
}

func encodePackStreamValue(v any) []byte {
	switch val := v.(type) {
	case nil:
		return []byte{0xC0}
	case bool:
		if val {
			return []byte{0xC3}
		}
		return []byte{0xC2}
	case int:
		return encodePackStreamInt(int64(val))
	case int64:
		return encodePackStreamInt(val)
	case float64:
		buf := make([]byte, 9)
		buf[0] = 0xC1
		binary.BigEndian.PutUint64(buf[1:], math.Float64bits(val))
		return buf
	case string:
		return encodePackStreamString(val)
	case []string:
		items := make([]any, len(val))
		for i, s := range val {
			items[i] = s
		}
		return encodePackStreamList(items)
	case []any:
		return encodePackStreamList(val)
	case []map[string]any:
		items := make([]any, len(val))
		for i, m := range val {
			items[i] = m
		}
		return encodePackStreamList(items)
	case map[string]any:
		return encodePackStreamMap(val)
	default:
		// Try to encode as a node structure
		return []byte{0xC0}
	}
}

func encodePackStreamInt(val int64) []byte {
	if val >= -16 && val <= 127 {
		return []byte{byte(val)}
	}
	if val >= -128 && val < -16 {
		return []byte{0xC8, byte(val)}
	}
	if val >= -32768 && val <= 32767 {
		return []byte{0xC9, byte(val >> 8), byte(val)}
	}
	if val >= -2147483648 && val <= 2147483647 {
		return []byte{0xCA, byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
	}
	return []byte{0xCB, byte(val >> 56), byte(val >> 48), byte(val >> 40), byte(val >> 32),
		byte(val >> 24), byte(val >> 16), byte(val >> 8), byte(val)}
}

// ============================================================================
// PackStream Decoding
// ============================================================================

func decodePackStreamString(data []byte, offset int) (string, int, error) {
	if offset >= len(data) {
		return "", 0, fmt.Errorf("offset out of bounds")
	}

	startOffset := offset
	marker := data[offset]
	offset++

	var length int

	// Tiny string (0x80-0x8F)
	if marker >= 0x80 && marker <= 0x8F {
		length = int(marker - 0x80)
	} else if marker == 0xD0 { // STRING8
		if offset >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING8")
		}
		length = int(data[offset])
		offset++
	} else if marker == 0xD1 { // STRING16
		if offset+1 >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING16")
		}
		length = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else if marker == 0xD2 { // STRING32
		if offset+3 >= len(data) {
			return "", 0, fmt.Errorf("incomplete STRING32")
		}
		length = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
	} else {
		return "", 0, fmt.Errorf("not a string marker: 0x%02X", marker)
	}

	if offset+length > len(data) {
		return "", 0, fmt.Errorf("string data out of bounds")
	}

	str := string(data[offset : offset+length])
	return str, (offset + length) - startOffset, nil
}

func decodePackStreamMap(data []byte, offset int) (map[string]any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]
	startOffset := offset
	offset++

	var size int

	// Tiny map (0xA0-0xAF)
	if marker >= 0xA0 && marker <= 0xAF {
		size = int(marker - 0xA0)
	} else if marker == 0xD8 { // MAP8
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("incomplete MAP8")
		}
		size = int(data[offset])
		offset++
	} else if marker == 0xD9 { // MAP16
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete MAP16")
		}
		size = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else {
		return nil, 0, fmt.Errorf("not a map marker: 0x%02X", marker)
	}

	result := make(map[string]any)

	for i := 0; i < size; i++ {
		// Decode key (must be string)
		key, n, err := decodePackStreamString(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode map key: %w", err)
		}
		offset += n

		// Decode value
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode map value for key %s: %w", key, err)
		}
		offset += n

		result[key] = value
	}

	return result, offset - startOffset, nil
}

func decodePackStreamValue(data []byte, offset int) (any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]

	// Null
	if marker == 0xC0 {
		return nil, 1, nil
	}

	// Boolean
	if marker == 0xC2 {
		return false, 1, nil
	}
	if marker == 0xC3 {
		return true, 1, nil
	}

	// Tiny positive int (0x00-0x7F)
	if marker <= 0x7F {
		return int64(marker), 1, nil
	}

	// Tiny negative int (0xF0-0xFF = -16 to -1)
	if marker >= 0xF0 {
		return int64(int8(marker)), 1, nil
	}

	// INT8
	if marker == 0xC8 {
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT8")
		}
		return int64(int8(data[offset+1])), 2, nil
	}

	// INT16
	if marker == 0xC9 {
		if offset+2 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT16")
		}
		val := int16(data[offset+1])<<8 | int16(data[offset+2])
		return int64(val), 3, nil
	}

	// INT32
	if marker == 0xCA {
		if offset+4 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT32")
		}
		val := int32(data[offset+1])<<24 | int32(data[offset+2])<<16 | int32(data[offset+3])<<8 | int32(data[offset+4])
		return int64(val), 5, nil
	}

	// INT64
	if marker == 0xCB {
		if offset+8 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete INT64")
		}
		val := int64(data[offset+1])<<56 | int64(data[offset+2])<<48 | int64(data[offset+3])<<40 | int64(data[offset+4])<<32 |
			int64(data[offset+5])<<24 | int64(data[offset+6])<<16 | int64(data[offset+7])<<8 | int64(data[offset+8])
		return val, 9, nil
	}

	// Float64
	if marker == 0xC1 {
		if offset+8 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete Float64")
		}
		bits := binary.BigEndian.Uint64(data[offset+1 : offset+9])
		return math.Float64frombits(bits), 9, nil
	}

	// String
	if marker >= 0x80 && marker <= 0x8F || marker == 0xD0 || marker == 0xD1 || marker == 0xD2 {
		return decodePackStreamString(data, offset)
	}

	// List
	if marker >= 0x90 && marker <= 0x9F || marker == 0xD4 || marker == 0xD5 || marker == 0xD6 {
		return decodePackStreamList(data, offset)
	}

	// Map
	if marker >= 0xA0 && marker <= 0xAF || marker == 0xD8 || marker == 0xD9 || marker == 0xDA {
		return decodePackStreamMap(data, offset)
	}

	// Structure (for nodes, relationships, etc.) - skip for now
	if marker >= 0xB0 && marker <= 0xBF {
		// Tiny structure - skip
		return nil, 1, nil
	}

	return nil, 0, fmt.Errorf("unknown marker: 0x%02X", marker)
}

func decodePackStreamList(data []byte, offset int) ([]any, int, error) {
	if offset >= len(data) {
		return nil, 0, fmt.Errorf("offset out of bounds")
	}

	marker := data[offset]
	startOffset := offset
	offset++

	var size int

	// Tiny list (0x90-0x9F)
	if marker >= 0x90 && marker <= 0x9F {
		size = int(marker - 0x90)
	} else if marker == 0xD4 { // LIST8
		if offset >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST8")
		}
		size = int(data[offset])
		offset++
	} else if marker == 0xD5 { // LIST16
		if offset+1 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST16")
		}
		size = int(data[offset])<<8 | int(data[offset+1])
		offset += 2
	} else if marker == 0xD6 { // LIST32
		if offset+3 >= len(data) {
			return nil, 0, fmt.Errorf("incomplete LIST32")
		}
		size = int(data[offset])<<24 | int(data[offset+1])<<16 | int(data[offset+2])<<8 | int(data[offset+3])
		offset += 4
	} else {
		return nil, 0, fmt.Errorf("not a list marker: 0x%02X", marker)
	}

	result := make([]any, size)

	for i := 0; i < size; i++ {
		value, n, err := decodePackStreamValue(data, offset)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to decode list item %d: %w", i, err)
		}
		result[i] = value
		offset += n
	}

	return result, offset - startOffset, nil
}
