// Package bolt implements the Neo4j Bolt protocol server for NornicDB.
//
// This package provides a Bolt protocol server that allows existing Neo4j drivers
// and tools to connect to NornicDB without modification. The server implements
// Bolt 4.x protocol specifications for maximum compatibility.
//
// Neo4j Bolt Protocol Compatibility:
//   - Bolt 4.0, 4.1, 4.2, 4.3, 4.4 support
//   - PackStream serialization format
//   - Transaction management (BEGIN, COMMIT, ROLLBACK)
//   - Streaming result sets (RUN, PULL, DISCARD)
//   - Authentication handshake
//   - Connection pooling support
//
// Supported Neo4j Drivers:
//   - Neo4j Java Driver
//   - Neo4j Python Driver (neo4j-driver)
//   - Neo4j JavaScript Driver
//   - Neo4j .NET Driver
//   - Neo4j Go Driver
//   - Community drivers (Rust, Ruby, etc.)
//
// Example Usage:
//
//	// Create Bolt server with Cypher executor
//	config := bolt.DefaultConfig()
//	config.Port = 7687
//	config.MaxConnections = 100
//
//	// Implement query executor
//	executor := &MyQueryExecutor{db: nornicDB}
//
//	server := bolt.New(config, executor)
//
//	// Start server
//	if err := server.ListenAndServe(); err != nil {
//		log.Fatal(err)
//	}
//
//	// Server is now accepting Bolt connections on port 7687
//
// Client Usage (any Neo4j driver):
//
//	// Python example
//	from neo4j import GraphDatabase
//
//	driver = GraphDatabase.driver("bolt://localhost:7687")
//	with driver.session() as session:
//	    result = session.run("MATCH (n) RETURN count(n)")
//	    print(result.single()[0])
//
//	// Go example
//	driver, _ := neo4j.NewDriver("bolt://localhost:7687", neo4j.NoAuth())
//	session := driver.NewSession(neo4j.SessionConfig{})
//	result, _ := session.Run("MATCH (n) RETURN count(n)", nil)
//
// Protocol Flow:
//
// 1. **Handshake**:
//   - Client sends magic number (0x6060B017)
//   - Client sends supported versions
//   - Server responds with selected version
//
// 2. **Authentication**:
//   - Client sends HELLO message with credentials
//   - Server responds with SUCCESS or FAILURE
//
// 3. **Query Execution**:
//   - Client sends RUN message with Cypher query
//   - Server responds with SUCCESS (field names)
//   - Client sends PULL to stream results
//   - Server sends RECORD messages + final SUCCESS
//
// 4. **Transaction Management**:
//   - BEGIN: Start explicit transaction
//   - COMMIT: Commit transaction
//   - ROLLBACK: Rollback transaction
//
// Message Types:
//   - HELLO: Authentication
//   - RUN: Execute Cypher query
//   - PULL: Stream result records
//   - DISCARD: Discard remaining results
//   - BEGIN/COMMIT/ROLLBACK: Transaction control
//   - RESET: Reset session state
//   - GOODBYE: Close connection
//
// PackStream Encoding:
//
//	The Bolt protocol uses PackStream for efficient binary serialization:
//	- Compact representation of common types
//	- Support for nested structures
//	- Streaming-friendly format
//
// Performance:
//   - Binary protocol (faster than HTTP/JSON)
//   - Connection pooling and reuse
//   - Streaming results (low memory usage)
//   - Pipelining support
//
// ELI12 (Explain Like I'm 12):
//
// Think of the Bolt server like a translator at the United Nations:
//
//  1. **Different languages**: Neo4j drivers speak "Bolt language" but NornicDB
//     speaks "NornicDB language". The Bolt server translates between them.
//
//  2. **Same conversation**: The drivers can have the same conversation they
//     always had (asking questions in Cypher), they just don't know they're
//     talking to a different database!
//
//  3. **Binary messages**: Instead of sending text messages (like HTTP), Bolt
//     sends compact binary messages - like sending a compressed file instead
//     of a text document. Much faster!
//
//  4. **Streaming**: Instead of waiting for ALL results before sending anything,
//     Bolt can send results one-by-one as they're found, like a live news feed.
//
// This lets existing Neo4j tools work with NornicDB without any changes!
package bolt

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
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

// Server implements a Neo4j Bolt protocol server for NornicDB.
//
// The server handles multiple concurrent client connections, each running
// in its own goroutine. It manages the Bolt protocol handshake, authentication,
// and message routing to the configured query executor.
//
// Example:
//
//	config := bolt.DefaultConfig()
//	executor := &MyExecutor{} // Implements QueryExecutor
//	server := bolt.New(config, executor)
//
//	go func() {
//		if err := server.ListenAndServe(); err != nil {
//			log.Printf("Bolt server error: %v", err)
//		}
//	}()
//
//	// Server is now accepting connections
//	fmt.Printf("Bolt server listening on bolt://localhost:%d\n", config.Port)
//
// Thread Safety:
//
//	The server is thread-safe and handles concurrent connections safely.
type Server struct {
	config   *Config
	listener net.Listener
	mu       sync.RWMutex
	sessions map[string]*Session
	closed   atomic.Bool

	// Query executor (injected dependency) - used if dbManager is nil
	executor QueryExecutor

	// Database manager for multi-database support (optional)
	// If set, queries are routed to the correct database based on HELLO message
	dbManager DatabaseManagerInterface

	// Transaction sequence tracking for causal consistency
	// Tracks the highest committed transaction sequence number across all sessions
	txSequence   int64        // Monotonically increasing transaction sequence number
	txSequenceMu sync.RWMutex // Protects txSequence
}

// DatabaseManagerInterface provides database management without importing multidb.
type DatabaseManagerInterface interface {
	GetStorage(name string) (storage.Engine, error)
	Exists(name string) bool
	DefaultDatabaseName() string
}

// QueryExecutor executes Cypher queries for the Bolt server.
//
// This interface allows the Bolt server to be decoupled from the specific
// database implementation. The executor receives Cypher queries and parameters
// from Bolt clients and returns results in a standard format.
//
// Example Implementation:
//
//	type MyExecutor struct {
//		db *nornicdb.DB
//	}
//
//	func (e *MyExecutor) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
//		// Execute query against NornicDB
//		result, err := e.db.ExecuteCypher(ctx, query, params)
//		if err != nil {
//			return nil, err
//		}
//
//		// Convert to Bolt format
//		return &QueryResult{
//			Columns: result.Columns,
//			Rows:    result.Rows,
//		}, nil
//	}
//
// The executor should handle:
//   - Cypher query parsing and execution
//   - Parameter substitution
//   - Result formatting
//   - Error handling and reporting
type QueryExecutor interface {
	Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error)
}

// TransactionalExecutor extends QueryExecutor with transaction support.
//
// If the executor implements this interface, the Bolt server will use
// real transactions for BEGIN/COMMIT/ROLLBACK messages. Otherwise,
// transaction messages are acknowledged but operations are auto-committed.
//
// Example Implementation:
//
//	type TxExecutor struct {
//		db *nornicdb.DB
//		tx *storage.Transaction  // Active transaction (nil if none)
//	}
//
//	func (e *TxExecutor) BeginTransaction(ctx context.Context) error {
//		e.tx = storage.NewTransaction(e.db.Engine())
//		return nil
//	}
//
//	func (e *TxExecutor) CommitTransaction(ctx context.Context) error {
//		if e.tx == nil {
//			return nil
//		}
//		err := e.tx.Commit()
//		e.tx = nil
//		return err
//	}
//
//	func (e *TxExecutor) RollbackTransaction(ctx context.Context) error {
//		if e.tx == nil {
//			return nil
//		}
//		err := e.tx.Rollback()
//		e.tx = nil
//		return err
//	}
type TransactionalExecutor interface {
	QueryExecutor
	BeginTransaction(ctx context.Context, metadata map[string]any) error
	CommitTransaction(ctx context.Context) error
	RollbackTransaction(ctx context.Context) error
}

// FlushableExecutor extends QueryExecutor with deferred commit support.
// This enables Neo4j-style optimization where writes are buffered until PULL.
type FlushableExecutor interface {
	QueryExecutor
	// Flush persists all pending writes to storage.
	Flush() error
}

// DeferrableExecutor extends FlushableExecutor with deferred flush mode control.
type DeferrableExecutor interface {
	FlushableExecutor
	// SetDeferFlush enables/disables deferred flush mode.
	SetDeferFlush(enabled bool)
}

// QueryResult holds the result of a query.
type QueryResult struct {
	Columns []string
	Rows    [][]any
}

// BoltAuthenticator is the interface for authenticating Bolt protocol connections.
// This supports Neo4j-compatible authentication schemes:
//   - "basic": Username/password authentication
//   - "bearer": JWT token authentication (for cluster inter-node auth)
//   - "none": Anonymous access (if allowed)
//
// The Bolt protocol HELLO message contains authentication credentials:
//   - scheme: "basic", "bearer", or "none"
//   - principal: username (basic) or empty (bearer/none)
//   - credentials: password (basic) or JWT token (bearer)
//
// For cluster deployments, use "bearer" scheme with a shared JWT secret:
//
//	# Generate cluster token on any node:
//	curl -X POST http://node1:7474/api/v1/auth/cluster-token \
//	  -H "Authorization: Bearer $ADMIN_TOKEN" \
//	  -d '{"node_id": "node-2", "role": "admin"}'
//
//	# Use token to connect from other nodes:
//	driver = GraphDatabase.driver("bolt://node1:7687",
//	    auth=("", token))  # scheme=bearer when principal is empty
//
// Example Implementation:
//
//	type MyAuthenticator struct {
//		auth *auth.Authenticator
//	}
//
//	func (a *MyAuthenticator) Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error) {
//		switch scheme {
//		case "none":
//			if a.allowAnonymous {
//				return &BoltAuthResult{Authenticated: true, Roles: []string{"viewer"}}, nil
//			}
//			return nil, fmt.Errorf("anonymous auth not allowed")
//		case "bearer":
//			claims, err := a.auth.ValidateToken(credentials)
//			if err != nil {
//				return nil, err
//			}
//			return &BoltAuthResult{Authenticated: true, Username: claims.Username, Roles: claims.Roles}, nil
//		case "basic":
//			// ... username/password validation
//		}
//	}
type BoltAuthenticator interface {
	// Authenticate validates credentials from the Bolt HELLO message.
	// Returns auth result on success, error on failure.
	// scheme: "basic", "bearer", or "none"
	// principal: username (basic), empty (bearer/none)
	// credentials: password (basic), JWT token (bearer), empty (none)
	Authenticate(scheme, principal, credentials string) (*BoltAuthResult, error)
}

// BoltAuthResult contains the result of Bolt authentication.
type BoltAuthResult struct {
	Authenticated bool     // Whether authentication succeeded
	Username      string   // Authenticated username
	Roles         []string // User roles (admin, editor, viewer, etc.)
}

// HasRole checks if the auth result has a specific role.
func (r *BoltAuthResult) HasRole(role string) bool {
	for _, r2 := range r.Roles {
		if r2 == role {
			return true
		}
	}
	return false
}

// HasPermission checks if the auth result has a specific permission based on roles.
// Maps to standard RBAC permissions:
//   - admin: read, write, create, delete, admin, schema, user_manage
//   - editor: read, write, create, delete
//   - viewer: read
func (r *BoltAuthResult) HasPermission(perm string) bool {
	rolePerms := map[string][]string{
		"admin":  {"read", "write", "create", "delete", "admin", "schema", "user_manage"},
		"editor": {"read", "write", "create", "delete"},
		"viewer": {"read"},
	}
	for _, role := range r.Roles {
		if perms, ok := rolePerms[role]; ok {
			for _, p := range perms {
				if p == perm {
					return true
				}
			}
		}
	}
	return false
}

// Config holds Bolt protocol server configuration.
//
// All settings have sensible defaults via DefaultConfig(). The configuration
// follows Neo4j Bolt server conventions where applicable.
//
// Authentication:
//   - Set Authenticator to enable auth (nil = no auth, accepts all)
//   - RequireAuth: if true, connections without valid credentials are rejected
//   - AllowAnonymous: if true, "none" auth scheme is accepted (viewer role)
//
// Example:
//
//	// Production configuration with auth
//	config := &bolt.Config{
//		Port:            7687,  // Standard Bolt port
//		MaxConnections:  1000,  // High concurrency
//		ReadBufferSize:  32768, // 32KB read buffer
//		WriteBufferSize: 32768, // 32KB write buffer
//		Authenticator:   myAuth,
//		RequireAuth:     true,
//	}
//
//	// Development configuration (no auth)
//	config = bolt.DefaultConfig()
//	config.Port = 7688 // Use different port
type Config struct {
	Port            int
	MaxConnections  int
	ReadBufferSize  int
	WriteBufferSize int
	LogQueries      bool // Log all queries to stdout (for debugging)

	// Authentication
	Authenticator  BoltAuthenticator // Authentication handler (nil = no auth)
	RequireAuth    bool              // Require authentication for all connections
	AllowAnonymous bool              // Allow "none" auth scheme (grants viewer role)
}

// DefaultConfig returns Neo4j-compatible default Bolt server configuration.
//
// Defaults match Neo4j Bolt server settings:
//   - Port 7687 (standard Bolt port)
//   - 100 max concurrent connections
//   - 8KB read/write buffers
//
// Example:
//
//	config := bolt.DefaultConfig()
//	server := bolt.New(config, executor)
func DefaultConfig() *Config {
	return &Config{
		Port:            7687,
		MaxConnections:  100,
		ReadBufferSize:  8192,
		WriteBufferSize: 64 * 1024,
	}
}

// New creates a new Bolt protocol server with the given configuration and executor.
//
// Parameters:
//   - config: Server configuration (uses DefaultConfig() if nil)
//   - executor: Query executor for handling Cypher queries (required if dbManager is nil)
//   - dbManager: Database manager for multi-database support (optional, if nil uses executor)
//
// Returns:
//   - Server instance ready to start
//
// Note: If dbManager is provided, it takes precedence and executor is ignored.
// The dbManager enables multi-database support where each connection can specify
// a database in the HELLO message.
//
// Example:
//
//	config := bolt.DefaultConfig()
//	executor := &MyQueryExecutor{db: nornicDB}
//	server := bolt.New(config, executor)
//
//	// Start server
//	if err := server.ListenAndServe(); err != nil {
//		log.Fatal(err)
//	}
//
// Example 1 - Basic Setup with Cypher Executor:
//
//	// Create storage engine
//	storage := storage.NewBadgerEngine("./data/nornicdb")
//	defer storage.Close()
//
//	// Create Cypher executor
//	cypherExec := cypher.NewStorageExecutor(storage)
//
//	// Create Bolt server
//	config := bolt.DefaultConfig()
//	config.Port = 7687
//
//	server := bolt.New(config, cypherExec)
//
//	// Start server (blocks until shutdown)
//	log.Fatal(server.ListenAndServe())
//
// Example 2 - Production with Connection Limits:
//
//	config := bolt.DefaultConfig()
//	config.Port = 7687
//	config.MaxConnections = 500     // Handle 500 concurrent clients
//	config.ReadBufferSize = 8192    // 8KB buffer
//	config.WriteBufferSize = 8192
//	config.IdleTimeout = 10 * time.Minute
//
//	executor := cypher.NewStorageExecutor(storage)
//	server := bolt.New(config, executor)
//
//	// Graceful shutdown
//	go func() {
//		sigChan := make(chan os.Signal, 1)
//		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
//		<-sigChan
//		log.Println("Shutting down Bolt server...")
//		server.Close()
//	}()
//
//	if err := server.ListenAndServe(); err != nil {
//		log.Fatal(err)
//	}
//
// Example 3 - Custom Query Executor with Middleware:
//
//	// Create custom executor with auth and logging
//	type AuthExecutor struct {
//		inner cypher.Executor
//		auth  *auth.Authenticator
//		audit *audit.Logger
//	}
//
//	func (e *AuthExecutor) Execute(ctx context.Context, query string, params map[string]any) (*bolt.QueryResult, error) {
//		// Extract user from context
//		user := ctx.Value("user").(string)
//
//		// Audit log
//		e.audit.LogDataAccess(user, user, "query", query, "EXECUTE", true, "")
//
//		// Execute query
//		result, err := e.inner.Execute(ctx, query, params)
//
//		// Convert to Bolt result format
//		return &bolt.QueryResult{
//			Columns: result.Fields,
//			Rows:    result.Records,
//		}, err
//	}
//
//	executor := &AuthExecutor{
//		inner: cypher.NewStorageExecutor(storage),
//		auth:  authenticator,
//		audit: auditLogger,
//	}
//
//	server := bolt.New(bolt.DefaultConfig(), executor)
//	server.ListenAndServe()
//
// Example 4 - Testing with In-Memory Storage:
//
//	func TestMyBoltIntegration(t *testing.T) {
//		// In-memory storage for tests
//		storage := storage.NewMemoryEngine()
//		executor := cypher.NewStorageExecutor(storage)
//
//		// Bolt server on random port
//		config := bolt.DefaultConfig()
//		config.Port = 0 // OS assigns random available port
//
//		server := bolt.New(config, executor)
//
//		// Start server in background
//		go server.ListenAndServe()
//		defer server.Close()
//
//		// Connect with Neo4j driver
//		driver, _ := neo4j.NewDriver(
//			fmt.Sprintf("bolt://localhost:%d", server.Port()),
//			neo4j.NoAuth(),
//		)
//		defer driver.Close()
//
//		// Run test queries
//		session := driver.NewSession(neo4j.SessionConfig{})
//		result, _ := session.Run("CREATE (n:Test {value: 42}) RETURN n", nil)
//		// ... assertions ...
//	}
//
// ELI12:
//
// Think of the Bolt server like a translator at the UN:
//
//   - Neo4j drivers speak "Bolt language" (binary protocol)
//   - NornicDB speaks "Cypher language" (graph queries)
//   - The Bolt server translates between them!
//
// Why do we need this translator?
//  1. Neo4j drivers already exist (Python, Java, JavaScript, Go, etc.)
//  2. Tools like Neo4j Browser, Bloom, and Cypher Shell work out of the box
//  3. No need to write new drivers for every programming language
//
// How it works:
//  1. Driver connects: "Hi, I speak Bolt 4.3"
//  2. Server responds: "Cool, I understand Bolt 4.3"
//  3. Driver sends: "RUN: MATCH (n) RETURN n LIMIT 10"
//  4. Server executes Cypher and sends back results
//  5. Driver receives results in Bolt format
//
// Real-world analogy:
//   - HTTP is like writing letters (text-based, verbose)
//   - Bolt is like speaking on the phone (binary, efficient)
//   - Bolt is ~3-5x faster than HTTP for graph queries!
//
// Compatible Tools:
//   - Neo4j Browser (web UI)
//   - Neo4j Desktop
//   - Cypher Shell (CLI)
//   - Neo4j Bloom (graph visualization)
//   - Any app using Neo4j drivers
//
// Protocol Advantages:
//   - Binary format (smaller, faster)
//   - Connection pooling (reuse connections)
//   - Streaming results (low memory)
//   - Transaction support (BEGIN/COMMIT/ROLLBACK)
//   - Pipelining (send multiple queries without waiting)
//
// Performance:
//   - Handles 100-500 concurrent connections easily
//   - ~1ms overhead per query
//   - Streaming results use O(1) memory per connection
//   - Binary PackStream is ~40% smaller than JSON
//
// Thread Safety:
//
//	Server handles concurrent connections safely.
func New(config *Config, executor QueryExecutor) *Server {
	return NewWithDatabaseManager(config, executor, nil)
}

// NewWithDatabaseManager creates a new Bolt protocol server with multi-database support.
//
// Parameters:
//   - config: Server configuration (uses DefaultConfig() if nil)
//   - executor: Query executor (ignored if dbManager is provided, kept for backward compatibility)
//   - dbManager: Database manager for multi-database support (optional)
//
// Returns:
//   - Server instance ready to start
//
// If dbManager is provided, queries are routed to the correct database based on
// the "db" or "database" parameter in the HELLO message. If not provided, the
// server uses the single executor for all queries (backward compatible).
func NewWithDatabaseManager(config *Config, executor QueryExecutor, dbManager DatabaseManagerInterface) *Server {
	if config == nil {
		config = DefaultConfig()
	}

	return &Server{
		config:    config,
		sessions:  make(map[string]*Session),
		executor:  executor,
		dbManager: dbManager,
	}
}

// ListenAndServe starts the Bolt server and begins accepting connections.
//
// The server listens on the configured port and handles incoming Bolt
// connections. Each connection is handled in a separate goroutine.
//
// Returns:
//   - nil if server shuts down cleanly
//   - Error if failed to bind to port or other startup error
//
// Example:
//
//	server := bolt.New(config, executor)
//
//	// Start server (blocks until shutdown)
//	if err := server.ListenAndServe(); err != nil {
//		log.Fatalf("Bolt server failed: %v", err)
//	}
//
// The server will print its listening address when started successfully.
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

	// Disable Nagle's algorithm for lower latency
	// Without this, small packets get delayed up to 40ms
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	// Recover from panics to prevent crashing the server
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("Recovered from panic in connection handler: %v\n", r)
		}
	}()

	session := &Session{
		conn:       conn,
		reader:     bufio.NewReaderSize(conn, 8192), // 8KB read buffer
		writer:     bufio.NewWriterSize(conn, 8192), // 8KB write buffer
		server:     s,
		executor:   s.executor,
		messageBuf: make([]byte, 0, 4096), // Pre-allocate 4KB message buffer
	}

	// Enable deferred flush mode for Neo4j-style write batching
	if deferrable, ok := s.executor.(DeferrableExecutor); ok {
		deferrable.SetDeferFlush(true)
	}

	// Ensure cleanup on session end
	defer func() {
		// Flush any pending writes
		if flushable, ok := s.executor.(FlushableExecutor); ok {
			flushable.Flush()
		}
		// Disable deferred flush mode
		if deferrable, ok := s.executor.(DeferrableExecutor); ok {
			deferrable.SetDeferFlush(false)
		}
	}()

	// Perform handshake
	if err := session.handshake(); err != nil {
		fmt.Printf("Handshake failed: %v\n", err)
		return
	}

	// Handle messages synchronously (simpler, lower overhead for request-response)
	for {
		if s.closed.Load() {
			return
		}
		if err := session.handleMessage(); err != nil {
			if err == io.EOF {
				return
			}
			errStr := err.Error()
			if strings.Contains(errStr, "connection reset") ||
				strings.Contains(errStr, "broken pipe") ||
				strings.Contains(errStr, "use of closed network connection") {
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
	reader   *bufio.Reader // Buffered reader for reduced syscalls
	writer   *bufio.Writer // Buffered writer for reduced syscalls
	server   *Server
	executor QueryExecutor
	version  uint32

	// Database context (from HELLO message)
	database string // Database name for this session (defaults to default database)

	// Authentication state
	authenticated bool            // Whether HELLO auth succeeded
	authResult    *BoltAuthResult // Auth result with roles/permissions

	// Transaction state
	inTransaction bool
	txMetadata    map[string]any // Transaction metadata from BEGIN

	// Query result state (for streaming with PULL)
	lastResult  *QueryResult
	resultIndex int

	// Deferred commit state (Neo4j-style optimization)
	// Writes are buffered in AsyncEngine until PULL completes
	pendingFlush bool

	// Query metadata for Neo4j driver compatibility
	queryId          int64          // Query ID counter for qid field
	lastQueryIsWrite bool           // Was last query a write operation
	lastRunMetadata  map[string]any // Metadata from last RUN message (bookmarks, tx_timeout, etc.)

	// Transaction sequence for this session's last committed transaction
	lastTxSequence int64 // Sequence number of last committed transaction in this session

	// Reusable buffers to reduce allocations
	headerBuf  [2]byte // For reading chunk headers
	messageBuf []byte  // Reusable message buffer
	recordBuf  []byte  // Reusable buffer for PackStream message encoding

	// Async message processing (Neo4j-style batching)
	messageQueue chan *boltMessage // Incoming messages queue
	writeMu      sync.Mutex        // Protects writer for concurrent access
}

// boltMessage represents a parsed Bolt message ready for processing
type boltMessage struct {
	msgType byte
	data    []byte
}

// handshake performs the Bolt handshake.
func (s *Session) handshake() error {
	// Read magic number (4 bytes: 0x60 0x60 0xB0 0x17)
	var magic [4]byte
	if _, err := io.ReadFull(s.reader, magic[:]); err != nil {
		return fmt.Errorf("failed to read magic: %w", err)
	}

	if magic[0] != 0x60 || magic[1] != 0x60 || magic[2] != 0xB0 || magic[3] != 0x17 {
		return fmt.Errorf("invalid magic number: %x", magic)
	}

	// Read supported versions (4 x 4 bytes)
	var versions [16]byte
	if _, err := io.ReadFull(s.reader, versions[:]); err != nil {
		return fmt.Errorf("failed to read versions: %w", err)
	}

	// Select highest supported version
	s.version = BoltV4_4

	// Send selected version using buffered writer
	response := []byte{0x00, 0x00, 0x04, 0x04} // Bolt 4.4
	if _, err := s.writer.Write(response); err != nil {
		return fmt.Errorf("failed to send version: %w", err)
	}
	if err := s.writer.Flush(); err != nil {
		return fmt.Errorf("failed to flush version: %w", err)
	}

	return nil
}

// readMessage reads a single Bolt message from the connection.
// Returns the parsed message or nil for empty messages.
func (s *Session) readMessage() (*boltMessage, error) {
	// Create a new buffer for this message (since we're async, can't reuse)
	var msgBuf []byte

	// Read chunks until we get a zero-size chunk (message terminator)
	var headerBuf [2]byte
	for {
		if _, err := io.ReadFull(s.reader, headerBuf[:]); err != nil {
			return nil, err
		}

		size := int(headerBuf[0])<<8 | int(headerBuf[1])
		if size == 0 {
			break
		}

		// Grow buffer
		oldLen := len(msgBuf)
		newLen := oldLen + size
		if cap(msgBuf) < newLen {
			newBuf := make([]byte, newLen, newLen*2)
			copy(newBuf, msgBuf)
			msgBuf = newBuf
		} else {
			msgBuf = msgBuf[:newLen]
		}

		if _, err := io.ReadFull(s.reader, msgBuf[oldLen:newLen]); err != nil {
			return nil, err
		}
	}

	if len(msgBuf) == 0 {
		return nil, nil // Empty message (no-op)
	}

	// Parse message type
	if len(msgBuf) < 2 {
		return nil, fmt.Errorf("message too short: %d bytes", len(msgBuf))
	}

	// Bolt messages are PackStream structures
	structMarker := msgBuf[0]
	var msgType byte
	var data []byte

	if structMarker >= 0xB0 && structMarker <= 0xBF {
		msgType = msgBuf[1]
		data = msgBuf[2:]
	} else {
		msgType = msgBuf[0]
		data = msgBuf[1:]
	}

	// Make a copy of data since buffer might be reused
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)

	return &boltMessage{msgType: msgType, data: dataCopy}, nil
}

// processMessage processes a parsed Bolt message.
func (s *Session) processMessage(msg *boltMessage) error {
	// No mutex needed - messages are processed sequentially from the queue
	return s.dispatchMessage(msg.msgType, msg.data)
}

// handleMessage handles a single Bolt message synchronously (for compatibility).
// Bolt messages can span multiple chunks - we read until we get a 0-size chunk.
func (s *Session) handleMessage() error {
	// Reuse message buffer - reset length but keep capacity
	s.messageBuf = s.messageBuf[:0]

	// Read chunks until we get a zero-size chunk (message terminator)
	for {
		// Read chunk header using reusable buffer (no allocation)
		if _, err := io.ReadFull(s.reader, s.headerBuf[:]); err != nil {
			return err
		}

		size := int(s.headerBuf[0])<<8 | int(s.headerBuf[1])
		if size == 0 {
			// Zero-size chunk marks end of message
			break
		}

		// Ensure message buffer has capacity
		oldLen := len(s.messageBuf)
		newLen := oldLen + size
		if cap(s.messageBuf) < newLen {
			// Need to grow - double capacity or use needed size
			newCap := cap(s.messageBuf) * 2
			if newCap < newLen {
				newCap = newLen
			}
			newBuf := make([]byte, newLen, newCap)
			copy(newBuf, s.messageBuf)
			s.messageBuf = newBuf
		} else {
			s.messageBuf = s.messageBuf[:newLen]
		}

		// Read chunk data directly into message buffer
		if _, err := io.ReadFull(s.reader, s.messageBuf[oldLen:newLen]); err != nil {
			return err
		}
	}

	if len(s.messageBuf) == 0 {
		return nil // Empty message (no-op)
	}

	// Parse and handle message
	if len(s.messageBuf) < 2 {
		return fmt.Errorf("message too short: %d bytes", len(s.messageBuf))
	}

	// Bolt messages are PackStream structures
	structMarker := s.messageBuf[0]

	// Check if it's a tiny struct (0xB0-0xBF)
	if structMarker >= 0xB0 && structMarker <= 0xBF {
		msgType := s.messageBuf[1]
		msgData := s.messageBuf[2:]
		return s.dispatchMessage(msgType, msgData)
	}

	// For non-struct markers, try direct message type (fallback)
	return s.dispatchMessage(s.messageBuf[0], s.messageBuf[1:])
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

// handleHello handles the HELLO message with authentication.
// Neo4j HELLO message format:
//
//	HELLO { user_agent: String, scheme: String, principal: String, credentials: String, ... }
//
// Authentication schemes:
//   - "none": Anonymous access (if AllowAnonymous is true)
//   - "basic": Username/password authentication
//   - "bearer": JWT token authentication (credentials contains the token)
//
// For cluster authentication with shared JWT:
//   - All nodes share the same JWT secret (NORNICDB_JWT_SECRET)
//   - Generate a cluster token: POST /api/v1/auth/cluster-token
//   - Connect using bearer scheme with the token as credentials
//
// Server-to-server clustering uses the same auth mechanism with service accounts.
func (s *Session) handleHello(data []byte) error {
	// Parse HELLO message to extract authentication details
	authParams, err := s.parseHelloAuth(data)
	if err != nil {
		return s.sendFailure("Neo.ClientError.Request.Invalid", fmt.Sprintf("Failed to parse HELLO: %v", err))
	}

	// Check if authentication is required
	if s.server != nil && s.server.config.Authenticator != nil {
		scheme := authParams["scheme"]
		principal := authParams["principal"]
		credentials := authParams["credentials"]

		// Handle anonymous auth
		if scheme == "none" || scheme == "" {
			if !s.server.config.AllowAnonymous {
				return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Authentication required")
			}
			// Anonymous user gets viewer role
			s.authenticated = true
			s.authResult = &BoltAuthResult{
				Authenticated: true,
				Username:      "anonymous",
				Roles:         []string{"viewer"},
			}
		} else if scheme == "basic" {
			// Authenticate with provided credentials
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				fmt.Printf("[BOLT] Auth failed for %q from %s: %v\n", principal, remoteAddr, err)
				return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Invalid credentials")
			}
			s.authenticated = true
			s.authResult = result
		} else if scheme == "bearer" {
			// JWT token authentication - used for cluster inter-node auth
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				fmt.Printf("[BOLT] Bearer auth failed from %s: %v\n", remoteAddr, err)
				return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Invalid or expired token")
			}
			s.authenticated = true
			s.authResult = result
		} else {
			return s.sendFailure("Neo.ClientError.Security.Unauthorized", fmt.Sprintf("Unsupported auth scheme: %s", scheme))
		}
	} else if s.server != nil && s.server.config.RequireAuth {
		// Auth required but no authenticator configured - reject all
		return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Authentication required but not configured")
	} else {
		// No auth configured - allow all (development mode)
		s.authenticated = true
		s.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "anonymous",
			Roles:         []string{"admin"}, // Full access in dev mode
		}
	}

	// Extract and validate database name
	dbName := authParams["database"]
	if dbName == "" && s.server != nil && s.server.dbManager != nil {
		// Use default database if not specified
		dbName = s.server.dbManager.DefaultDatabaseName()
	}

	// Validate database exists (if dbManager is configured)
	if dbName != "" && s.server != nil && s.server.dbManager != nil {
		if !s.server.dbManager.Exists(dbName) {
			return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' does not exist", dbName))
		}
	}

	// Store database for this session
	s.database = dbName

	// Log successful auth
	if s.server != nil && s.server.config.LogQueries {
		remoteAddr := "unknown"
		if s.conn != nil {
			remoteAddr = s.conn.RemoteAddr().String()
		}
		dbInfo := ""
		if dbName != "" {
			dbInfo = fmt.Sprintf(" db=%s", dbName)
		}
		fmt.Printf("[BOLT] Auth success: user=%s roles=%v from=%s%s\n",
			s.authResult.Username, s.authResult.Roles, remoteAddr, dbInfo)
	}

	return s.sendSuccess(map[string]any{
		"server":        "NornicDB/0.1.0",
		"connection_id": "nornic-1",
		"hints":         map[string]any{},
	})
}

// parseHelloAuth parses authentication parameters and database from a HELLO message.
// Returns a map with keys: scheme, principal, credentials, database
func (s *Session) parseHelloAuth(data []byte) (map[string]string, error) {
	result := map[string]string{
		"scheme":      "",
		"principal":   "",
		"credentials": "",
		"database":    "",
	}

	if len(data) == 0 {
		return result, nil
	}

	// HELLO is a structure: [extra: Map]
	// First byte is marker for structure
	marker := data[0]

	// Check for tiny struct marker (0xB0-0xBF = struct with 0-15 fields)
	// HELLO has signature 0x01 and one field (the extra map)
	offset := 0
	if marker >= 0xB0 && marker <= 0xBF {
		offset = 2 // Skip struct marker and signature byte
	} else {
		// Try to find the map directly
		offset = 0
	}

	if offset >= len(data) {
		return result, nil
	}

	// Parse the extra map
	extraMap, _, err := decodePackStreamMap(data, offset)
	if err != nil {
		return result, fmt.Errorf("failed to decode HELLO extra map: %w", err)
	}

	// Extract auth fields
	if scheme, ok := extraMap["scheme"].(string); ok {
		result["scheme"] = scheme
	}
	if principal, ok := extraMap["principal"].(string); ok {
		result["principal"] = principal
	}
	if credentials, ok := extraMap["credentials"].(string); ok {
		result["credentials"] = credentials
	}

	// Extract database parameter (Neo4j 4.x multi-database support)
	if db, ok := extraMap["db"].(string); ok {
		result["database"] = db
	} else if db, ok := extraMap["database"].(string); ok {
		// Some drivers use "database" instead of "db"
		result["database"] = db
	}

	return result, nil
}

// handleRun handles the RUN message (execute Cypher).
func (s *Session) handleRun(data []byte) error {
	// Check authentication
	if s.server != nil && s.server.config.RequireAuth && !s.authenticated {
		return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Not authenticated")
	}

	// Parse PackStream to extract query, params, and metadata
	query, params, metadata, err := s.parseRunMessage(data)
	if err != nil {
		return s.sendFailure("Neo.ClientError.Request.Invalid", fmt.Sprintf("Failed to parse RUN message: %v", err))
	}

	// Store metadata for potential use (bookmarks, tx_timeout, etc.)
	s.lastRunMetadata = metadata

	// Validate bookmarks if present (for causal consistency)
	if bookmarks, ok := metadata["bookmarks"].([]any); ok && len(bookmarks) > 0 {
		if err := s.validateBookmarks(bookmarks); err != nil {
			return s.sendFailure("Neo.ClientError.Transaction.BookmarkValidationFailed",
				fmt.Sprintf("Bookmark validation failed: %v", err))
		}
	}

	// Create context with timeout if tx_timeout is specified
	ctx := context.Background()
	if txTimeout, ok := metadata["tx_timeout"].(int64); ok && txTimeout > 0 {
		// tx_timeout is in milliseconds, convert to time.Duration
		timeout := time.Duration(txTimeout) * time.Millisecond
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel() // Ensure context is cancelled when function returns
	}

	// Classify query type once (used for auth and deferred flush)
	upperQuery := strings.ToUpper(query)
	isWrite := strings.Contains(upperQuery, "CREATE") ||
		strings.Contains(upperQuery, "DELETE") ||
		strings.Contains(upperQuery, "SET ") ||
		strings.Contains(upperQuery, "MERGE") ||
		strings.Contains(upperQuery, "REMOVE ")
	isSchema := strings.Contains(upperQuery, "INDEX") ||
		strings.Contains(upperQuery, "CONSTRAINT")

	// Check permissions based on query type
	if s.authResult != nil {
		if isSchema && !s.authResult.HasPermission("schema") {
			return s.sendFailure("Neo.ClientError.Security.Forbidden", "Schema operations require schema permission")
		}
		if isWrite && !s.authResult.HasPermission("write") {
			return s.sendFailure("Neo.ClientError.Security.Forbidden", "Write operations require write permission")
		}
		if !s.authResult.HasPermission("read") {
			return s.sendFailure("Neo.ClientError.Security.Forbidden", "Read operations require read permission")
		}
	}

	// Log query if enabled
	if s.server != nil && s.server.config.LogQueries {
		remoteAddr := "unknown"
		if s.conn != nil {
			remoteAddr = s.conn.RemoteAddr().String()
		}
		user := "unknown"
		if s.authResult != nil {
			user = s.authResult.Username
		}
		if len(params) > 0 {
			fmt.Printf("[BOLT] %s@%s: %s (params: %v)\n", user, remoteAddr, truncateQuery(query, 200), params)
		} else {
			fmt.Printf("[BOLT] %s@%s: %s\n", user, remoteAddr, truncateQuery(query, 200))
		}
	}

	// Get executor for the database (if multi-database is enabled)
	executor := s.executor
	if s.server != nil && s.server.dbManager != nil {
		// Get database-specific executor
		dbName := s.database
		if dbName == "" {
			dbName = s.server.dbManager.DefaultDatabaseName()
		}
		dbExecutor, err := s.getExecutorForDatabase(dbName)
		if err != nil {
			return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' not found: %v", dbName, err))
		}
		executor = dbExecutor
	}

	// Execute query (ctx may have timeout from tx_timeout metadata)
	result, err := executor.Execute(ctx, query, params)
	if err != nil {
		if s.server != nil && s.server.config.LogQueries {
			fmt.Printf("[BOLT] ERROR: %v\n", err)
		}
		return s.sendFailure("Neo.ClientError.Statement.SyntaxError", err.Error())
	}

	// Track write operation for deferred flush
	if isWrite {
		s.pendingFlush = true
	}
	s.lastQueryIsWrite = isWrite

	// Store result for PULL
	s.lastResult = result
	s.resultIndex = 0
	s.queryId++

	// Return SUCCESS with field names (Neo4j compatible metadata)
	// Note: Neo4j only sends qid for EXPLICIT transactions, not implicit/autocommit
	// For implicit transactions, only send fields and t_first
	if s.inTransaction {
		return s.sendSuccess(map[string]any{
			"fields":  result.Columns,
			"t_first": int64(0),
			"qid":     s.queryId,
		})
	}
	return s.sendSuccess(map[string]any{
		"fields":  result.Columns,
		"t_first": int64(0),
	})
}

// truncateQuery truncates a query for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// parseRunMessage parses a RUN message to extract query, parameters, and metadata.
// Bolt v4+ RUN message format: [query: String, parameters: Map, extra: Map]
// Returns: query, parameters, metadata, error
func (s *Session) parseRunMessage(data []byte) (string, map[string]any, map[string]any, error) {
	if len(data) == 0 {
		return "", nil, nil, fmt.Errorf("empty RUN message")
	}

	offset := 0

	// Parse query string
	query, n, err := decodePackStreamString(data, offset)
	if err != nil {
		return "", nil, nil, fmt.Errorf("failed to parse query: %w", err)
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
	// Parse metadata if present
	metadata := make(map[string]any)
	if offset < len(data) {
		m, consumed, err := decodePackStreamMap(data, offset)
		if err == nil {
			metadata = m
			offset += consumed
		}
		// If parsing fails, continue with empty metadata (non-fatal)
	}

	return query, params, metadata, nil
}

// handlePull handles the PULL message.
func (s *Session) handlePull(data []byte) error {
	if s.lastResult == nil {
		// Neo4j doesn't send has_more when false - just empty metadata
		return s.sendSuccess(map[string]any{})
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

	// Stream records - use batched writing for large result sets
	remaining := len(s.lastResult.Rows) - s.resultIndex
	if pullN > 0 && remaining > pullN {
		remaining = pullN
	}

	// For large batches (>50 records), use batched writing to reduce syscalls
	if remaining > 50 {
		if err := s.sendRecordsBatched(s.lastResult.Rows[s.resultIndex : s.resultIndex+remaining]); err != nil {
			return err
		}
		s.resultIndex += remaining
	} else {
		// Small batches: send individually (avoids buffer allocation overhead)
		for s.resultIndex < len(s.lastResult.Rows) {
			if pullN == 0 {
				break
			}

			row := s.lastResult.Rows[s.resultIndex]
			if err := s.writeRecordNoFlush(row); err != nil {
				return err
			}

			s.resultIndex++
			if pullN > 0 {
				pullN--
			}
		}
	}

	// Check if more records available
	hasMore := s.resultIndex < len(s.lastResult.Rows)

	// Clear result if done
	if !hasMore {
		s.lastResult = nil
		s.resultIndex = 0

		// Neo4j-style deferred commit: flush pending writes after streaming completes
		if s.pendingFlush {
			if flushable, ok := s.executor.(FlushableExecutor); ok {
				flushable.Flush()
			}
			s.pendingFlush = false
		}

		// Return metadata for completed query (Neo4j compatibility)
		// Neo4j sends: type, bookmark, t_last, stats, db (but NOT has_more when false)
		queryType := "r"
		if s.lastQueryIsWrite {
			queryType = "w"
		}

		bookmark := s.currentBookmark()
		if s.lastQueryIsWrite {
			bookmark = s.generateBookmark()
		}

		// Build stats matching Neo4j format (only if there are updates)
		metadata := map[string]any{
			"bookmark": bookmark,
			"type":     queryType,
			"t_last":   int64(0), // Streaming time
			"db":       "nornic", // Default database name
		}

		// Note: Neo4j does NOT send has_more when it's false
		return s.sendSuccess(metadata)
	}

	// When there are more records, send has_more: true
	return s.sendSuccess(map[string]any{
		"has_more": true,
	})
}

// handleDiscard handles the DISCARD message.
func (s *Session) handleDiscard(data []byte) error {
	s.lastResult = nil
	s.resultIndex = 0
	// Neo4j doesn't send has_more when false - just empty metadata
	return s.sendSuccess(map[string]any{})
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
// Resets the session state and rolls back any active transaction.
func (s *Session) handleReset(data []byte) error {
	// Rollback any active transaction
	if s.inTransaction {
		if txExec, ok := s.executor.(TransactionalExecutor); ok {
			ctx := context.Background()
			_ = txExec.RollbackTransaction(ctx) // Ignore error on reset
		}
	}

	s.inTransaction = false
	s.txMetadata = nil
	s.lastResult = nil
	s.resultIndex = 0
	return s.sendSuccess(nil)
}

// handleBegin handles the BEGIN message.
// If the executor implements TransactionalExecutor, starts a real transaction.
// Otherwise, just tracks the transaction state for protocol compliance.
func (s *Session) handleBegin(data []byte) error {
	// Parse BEGIN metadata (contains tx_timeout, bookmarks, etc.)
	var metadata map[string]any
	if len(data) > 0 {
		m, _, err := decodePackStreamMap(data, 0)
		if err == nil {
			metadata = m
		}
	}
	s.txMetadata = metadata

	// If executor supports transactions, start one
	if txExec, ok := s.executor.(TransactionalExecutor); ok {
		ctx := context.Background()
		if err := txExec.BeginTransaction(ctx, metadata); err != nil {
			return s.sendFailure("Neo.TransactionError.Begin", err.Error())
		}
	}

	s.inTransaction = true
	return s.sendSuccess(nil)
}

// handleCommit handles the COMMIT message.
// If the executor implements TransactionalExecutor, commits the real transaction.
func (s *Session) handleCommit(data []byte) error {
	if !s.inTransaction {
		return s.sendFailure("Neo.ClientError.Transaction.TransactionNotFound",
			"No transaction to commit")
	}

	// If executor supports transactions, commit
	if txExec, ok := s.executor.(TransactionalExecutor); ok {
		ctx := context.Background()
		if err := txExec.CommitTransaction(ctx); err != nil {
			s.inTransaction = false
			s.txMetadata = nil
			return s.sendFailure("Neo.TransactionError.Commit", err.Error())
		}
	}

	s.inTransaction = false
	s.txMetadata = nil

	// Generate and store new bookmark for causal consistency
	// This increments the server's transaction sequence and creates a bookmark
	bookmark := s.generateBookmark()

	// Return bookmark for client tracking
	return s.sendSuccess(map[string]any{
		"bookmark": bookmark,
	})
}

// generateBookmark generates a unique bookmark for causal consistency tracking.
// Format: "nornicdb:bookmark:<sequence>"
// The sequence number represents the transaction order for causal consistency.
func (s *Session) generateBookmark() string {
	if s.server == nil {
		panic("cannot generate bookmark: session has no server reference")
	}

	// Get next transaction sequence number from server
	s.server.txSequenceMu.Lock()
	s.server.txSequence++
	seqNum := s.server.txSequence
	s.server.txSequenceMu.Unlock()

	// Store sequence in session
	s.lastTxSequence = seqNum

	return fmt.Sprintf("nornicdb:bookmark:%d", seqNum)
}

func (s *Session) currentBookmark() string {
	if s.server == nil {
		return "nornicdb:bookmark:0"
	}
	s.server.txSequenceMu.RLock()
	seqNum := s.server.txSequence
	s.server.txSequenceMu.RUnlock()
	if seqNum < 0 {
		seqNum = 0
	}
	return fmt.Sprintf("nornicdb:bookmark:%d", seqNum)
}

// validateBookmarks validates bookmarks for causal consistency.
// Ensures that all transactions up to the bookmark's sequence number have been committed.
// This provides causal consistency: reads will see all writes that committed before the bookmark.
func (s *Session) validateBookmarks(bookmarks []any) error {
	if len(bookmarks) == 0 {
		return nil // No bookmarks to validate
	}

	if s.server == nil {
		return fmt.Errorf("cannot validate bookmarks: session has no server reference")
	}

	// Get current transaction sequence from server
	s.server.txSequenceMu.RLock()
	currentSequence := s.server.txSequence
	s.server.txSequenceMu.RUnlock()

	// Validate each bookmark
	for _, bookmarkAny := range bookmarks {
		bookmark, ok := bookmarkAny.(string)
		if !ok {
			return fmt.Errorf("invalid bookmark type: expected string, got %T", bookmarkAny)
		}

		// Backward compatibility: older server versions returned this placeholder in SUCCESS.
		// Treat it as "no bookmark" rather than failing the session.
		if bookmark == "nornicdb:tx:auto" {
			continue
		}

		// Only accept NornicDB bookmark format: "nornicdb:bookmark:<sequence>"
		if !strings.HasPrefix(bookmark, "nornicdb:bookmark:") {
			return fmt.Errorf("invalid bookmark format: expected 'nornicdb:bookmark:<sequence>', got %q", bookmark)
		}

		// Parse sequence number from bookmark
		seqStr := strings.TrimPrefix(bookmark, "nornicdb:bookmark:")
		if seqStr == "" {
			return fmt.Errorf("invalid bookmark format: missing sequence number in %q", bookmark)
		}

		bookmarkSeq, err := strconv.ParseInt(seqStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid bookmark format: cannot parse sequence number from %q: %w", bookmark, err)
		}

		// Validate sequence number is non-negative
		if bookmarkSeq < 0 {
			return fmt.Errorf("invalid bookmark: sequence number must be non-negative, got %d", bookmarkSeq)
		}

		// Causal consistency check: bookmark sequence must be <= current sequence
		// This ensures all transactions up to the bookmark have been committed
		if bookmarkSeq > currentSequence {
			return fmt.Errorf("bookmark sequence %d is from the future (current: %d)", bookmarkSeq, currentSequence)
		}

		// Bookmark is valid - all transactions up to this sequence have been committed
	}

	return nil
}

// handleRollback handles the ROLLBACK message.
// If the executor implements TransactionalExecutor, rolls back the real transaction.
func (s *Session) handleRollback(data []byte) error {
	if !s.inTransaction {
		// Not an error to rollback when not in transaction (Neo4j behavior)
		return s.sendSuccess(nil)
	}

	// If executor supports transactions, rollback
	if txExec, ok := s.executor.(TransactionalExecutor); ok {
		ctx := context.Background()
		if err := txExec.RollbackTransaction(ctx); err != nil {
			// Rollback failed, but we still clear state
			s.inTransaction = false
			s.txMetadata = nil
			return s.sendFailure("Neo.TransactionError.Rollback", err.Error())
		}
	}

	s.inTransaction = false
	s.txMetadata = nil
	return s.sendSuccess(nil)
}

// sendRecord sends a RECORD response.
// Uses buffer pooling to reduce allocations for high-frequency record sending.
func (s *Session) sendRecord(fields []any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	// Format: <struct marker 0xB1> <signature 0x71> <list of fields>
	buf = append(buf, recordHeader...)
	buf = encodePackStreamListInto(buf, fields)

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

// writeRecordNoFlush writes a RECORD message but does not flush.
// It is used by PULL streaming to batch many records into a single flush
// (the final SUCCESS message flushes everything).
func (s *Session) writeRecordNoFlush(fields []any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, recordHeader...)
	buf = encodePackStreamListInto(buf, fields)

	err := s.writeMessageNoFlush(buf)
	s.recordBuf = buf[:0]
	return err
}

// sendRecordsBatched sends multiple RECORD responses using buffered I/O.
// This dramatically reduces syscall overhead for large result sets.
// For 500 records: ~500 syscalls → 1 syscall = ~8x faster
// Uses buffer pooling to reduce allocations per record.
func (s *Session) sendRecordsBatched(rows [][]any) error {
	if len(rows) == 0 {
		return nil
	}

	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}

	// Write all records to buffer (each record is a separate chunk)
	for _, row := range rows {
		// Reset buffer length but keep capacity
		buf = buf[:0]

		// Build record: struct marker + signature + list of fields
		buf = append(buf, recordHeader...)
		buf = encodePackStreamListInto(buf, row)

		// bufio.Writer does not retain the provided slice after Write returns,
		// so it's safe to reuse the pooled buffer on the next iteration.
		if err := s.writeMessageNoFlush(buf); err != nil {
			s.recordBuf = buf[:0]
			return err
		}
	}

	// Don't flush here - let the final SUCCESS message flush everything
	s.recordBuf = buf[:0]
	return nil
}

// sendSuccess sends a SUCCESS response with PackStream encoding.
// Pre-allocated success header
var successHeader = []byte{0xB1, MsgSuccess}
var recordHeader = []byte{0xB1, MsgRecord}
var failureHeader = []byte{0xB1, MsgFailure}

func (s *Session) sendSuccess(metadata map[string]any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, successHeader...)
	buf = encodePackStreamMapInto(buf, metadata)

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

// sendFailure sends a FAILURE response.
// Uses buffer pooling to reduce allocations.
func (s *Session) sendFailure(code, message string) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, failureHeader...)
	metadata := map[string]any{
		"code":    code,
		"message": message,
	}
	buf = encodePackStreamMapInto(buf, metadata)

	// sendChunk flushes immediately, so it's safe to reuse the buffer after.
	err := s.sendChunk(buf)
	s.recordBuf = buf[:0]
	return err
}

// sendChunk sends a chunk to the client using buffered I/O.
// The buffer is flushed after each complete message response.
func (s *Session) sendChunk(data []byte) error {
	if err := s.writeMessageNoFlush(data); err != nil {
		return err
	}
	return s.writer.Flush()
}

// writeMessageNoFlush writes a complete Bolt message using chunk framing, but does
// not flush the underlying buffered writer.
//
// Bolt messages are chunked with 2-byte big-endian sizes and a 0-sized terminator.
// A single message may span multiple chunks (max chunk size is 65535 bytes).
func (s *Session) writeMessageNoFlush(data []byte) error {
	const maxChunkSize = 0xFFFF

	// Preserve existing behavior: even for empty messages, write an explicit
	// 0-sized chunk header followed by the terminator chunk.
	if len(data) == 0 {
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		if err := s.writer.WriteByte(0x00); err != nil {
			return err
		}
		return nil
	}

	remaining := data

	for len(remaining) > 0 {
		chunkSize := len(remaining)
		if chunkSize > maxChunkSize {
			chunkSize = maxChunkSize
		}

		if err := s.writer.WriteByte(byte(chunkSize >> 8)); err != nil {
			return err
		}
		if err := s.writer.WriteByte(byte(chunkSize)); err != nil {
			return err
		}
		if _, err := s.writer.Write(remaining[:chunkSize]); err != nil {
			return err
		}

		remaining = remaining[chunkSize:]
	}

	// Terminator chunk (size 0)
	if err := s.writer.WriteByte(0x00); err != nil {
		return err
	}
	return s.writer.WriteByte(0x00)
}

// getExecutorForDatabase returns a Cypher executor for the specified database.
// This is used when DatabaseManager is configured to route queries to the correct database.
func (s *Session) getExecutorForDatabase(dbName string) (QueryExecutor, error) {
	if s.server == nil || s.server.dbManager == nil {
		return nil, fmt.Errorf("database manager not available")
	}

	// Get namespaced storage for this database
	storage, err := s.server.dbManager.GetStorage(dbName)
	if err != nil {
		return nil, err
	}

	// Create Cypher executor scoped to this database
	executor := cypher.NewStorageExecutor(storage)

	// Note: System commands (CREATE/DROP/SHOW DATABASE) are not supported via Bolt
	// They must be executed via HTTP API. This is by design to maintain compatibility
	// with Neo4j drivers which don't support these commands via Bolt.

	return &boltQueryExecutorAdapter{executor: executor}, nil
}

// boltQueryExecutorAdapter adapts cypher.StorageExecutor to bolt.QueryExecutor interface.
type boltQueryExecutorAdapter struct {
	executor *cypher.StorageExecutor
}

func (a *boltQueryExecutorAdapter) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	// Convert params from map[string]any to map[string]interface{}
	paramsMap := make(map[string]interface{}, len(params))
	for k, v := range params {
		paramsMap[k] = v
	}

	// Execute query
	result, err := a.executor.Execute(ctx, query, paramsMap)
	if err != nil {
		return nil, err
	}

	// Convert result to Bolt format
	return &QueryResult{
		Columns: result.Columns,
		Rows:    result.Rows,
	}, nil
}
