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
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
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
//			// Operators should route this through the structured logger
//			// returned by observability.NewLogger; the server itself
//			// announces its listening address via slog at INFO once
//			// ListenAndServe binds successfully.
//			_ = err
//		}
//	}()
//
//	// Server is now accepting connections (announce log emitted by the
//	// server's own structured logger).
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

	executorsMu sync.RWMutex
	executors   map[string]QueryExecutor

	// Query executor (injected dependency) - used if dbManager is nil
	executor QueryExecutor

	// Database manager for multi-database support (optional)
	// If set, queries are routed to the correct database based on HELLO message
	dbManager DatabaseManagerInterface

	// Per-database access control (optional). When set, CanAccessDatabase is checked before running queries.
	databaseAccessMode auth.DatabaseAccessMode
	// Resolver for per-principal mode (Phase 3). When set, used with session roles instead of databaseAccessMode.
	databaseAccessModeResolver func(roles []string) auth.DatabaseAccessMode
	// Resolver for per-DB read/write (Phase 4). When set, used for mutation write check.
	resolvedAccessResolver func(roles []string, dbName string) auth.ResolvedAccess

	// Transaction sequence tracking for causal consistency
	// Tracks the highest committed transaction sequence number across all sessions
	txSequence   int64        // Monotonically increasing transaction sequence number
	txSequenceMu sync.RWMutex // Protects txSequence

	// log is the structured-logging entrypoint for the Bolt server. It is
	// derived from config.Logger at NewWithDatabaseManager() with
	// .With("component","bolt") so every record automatically carries the
	// component attribute (D-10a: replaces the legacy "[BOLT]" bracket
	// prefix). If config.Logger was nil at ctor entry, a discard-handler
	// fallback is installed (D-01a) so existing callers that never set the
	// field compile and run unchanged.
	log *slog.Logger

	// metricsState is the Plan-04-02 BoltMetrics bag + pre-built per-op
	// BoundLatencyObserver cache (CONTEXT D-02 / MET-25 hot-path). Nil
	// until SetBoltMetrics is called from cmd/nornicdb startup; the
	// observation sites in handleConnection / dispatchMessage / packstream
	// nil-check this field so test fixtures that construct a Server
	// literal continue to work unchanged.
	metricsState *boltMetricsState

	// authMetrics is the Plan-04-06 AuthMetrics bag for the D-11/D-05e
	// auth_attempts_total{result, protocol="bolt"} crosswire. Plan 04-02
	// adds the call site behind a nil-check (observeAuthAttempt no-ops);
	// Plan 04-06 ships the GREEN bag and wires it via SetAuthMetrics.
	authMetrics *observability.AuthMetrics
}

// DatabaseManagerInterface provides database management without importing multidb.
type DatabaseManagerInterface interface {
	GetStorage(name string) (storage.Engine, error)
	Exists(name string) bool
	DefaultDatabaseName() string
}

// constituentAwareExists checks whether a database name refers to an existing
// database or a valid composite constituent (e.g. "composite.alias").
// It tries the ExistsOrIsConstituent method if the manager supports it,
// otherwise falls back to Exists.
func constituentAwareExists(mgr DatabaseManagerInterface, name string) bool {
	type constituentResolver interface {
		ExistsOrIsConstituent(name string) bool
	}
	if cr, ok := mgr.(constituentResolver); ok {
		return cr.ExistsOrIsConstituent(name)
	}
	return mgr.Exists(name)
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

// SessionExecutorFactory creates per-connection executors.
// Use this when executor implementations keep session-local state (e.g., explicit tx IDs).
type SessionExecutorFactory interface {
	NewSessionExecutor() QueryExecutor
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
	Columns  []string
	Rows     [][]any
	Metadata map[string]any
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
	Permissions   []string // Effective entitlement IDs (when set, used by HasPermission; else fallback to rolePerms)
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

// boltRolePermsFallback is the default role→permission IDs when Permissions is not set (from auth.RolePermissions).
var boltRolePermsFallback = auth.RolePermissionsAsStrings()

// HasPermission checks if the auth result has a specific permission.
// When Permissions is set (from role entitlements store), uses that list; else falls back to auth.RolePermissions.
func (r *BoltAuthResult) HasPermission(perm string) bool {
	perm = strings.ToLower(strings.TrimSpace(perm))
	if len(r.Permissions) > 0 {
		for _, p := range r.Permissions {
			if p == perm || p == string(auth.PermAdmin) {
				return true
			}
		}
		return false
	}
	for _, role := range r.Roles {
		if perms, ok := boltRolePermsFallback[role]; ok {
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
	Host            string
	Port            int
	MaxConnections  int
	ReadBufferSize  int
	WriteBufferSize int
	LogQueries      bool // Log all queries to stdout (for debugging)
	// ServerAnnouncement overrides the Bolt HELLO SUCCESS "server" metadata.
	// Leave empty to advertise NornicDB natively.
	ServerAnnouncement string

	// Authentication
	Authenticator  BoltAuthenticator // Authentication handler (nil = no auth)
	RequireAuth    bool              // Require authentication for all connections
	AllowAnonymous bool              // Allow "none" auth scheme (grants viewer role)

	// Logger is the structured-logging entrypoint per D-01. If nil, a
	// discard-handler fallback (D-01a) is installed at
	// NewWithDatabaseManager() so existing callers compile unchanged. The
	// Bolt HELLO message's "credentials" field is auto-redacted by the
	// Plan 02-01 redactingHandler chain (D-03a) — DefaultRedactKeys
	// already includes "credentials", so per-call scrubbing is not
	// required in pkg/bolt.
	Logger *slog.Logger
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
		Host:            "127.0.0.1",
		Port:            7687,
		MaxConnections:  100,
		ReadBufferSize:  8192,
		WriteBufferSize: 64 * 1024,
	}
}

func (c *Config) serverAnnouncement() string {
	if c == nil {
		return buildinfo.ServerAnnouncement()
	}
	if v := strings.TrimSpace(c.ServerAnnouncement); v != "" {
		return v
	}
	return buildinfo.ServerAnnouncement()
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
//	// Graceful shutdown — operators wire this through lifecycle.Run so
//	// the slog-bound logger emits the shutdown notice with structured
//	// component=bolt attribution.
//	go func() {
//		sigChan := make(chan os.Signal, 1)
//		signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
//		<-sigChan
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
	// D-01a discard-fallback: callers that don't set Config.Logger get a
	// no-op logger so production code paths that emit structured records
	// don't have to nil-check. Production callers (cmd/nornicdb/main.go)
	// pass obs.Logger() so records flow through the 4-layer
	// recovering→mandatory→redact→json handler stack — including the
	// D-03a credentials redaction.
	if config.Logger == nil {
		config.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	return &Server{
		config:    config,
		sessions:  make(map[string]*Session),
		executors: make(map[string]QueryExecutor),
		executor:  executor,
		dbManager: dbManager,
		// D-10a: component attribute replaces the legacy "[BOLT]" bracket
		// prefix; every record emitted via s.log automatically carries it.
		log: config.Logger.With("component", "bolt"),
	}
}

// discardBoltLogger is the package-level fallback logger used when a Server
// is constructed via a struct literal (test fixtures) instead of through
// NewWithDatabaseManager. It is allocated once at package init so the
// logger() accessor is allocation-free and race-free on the read path.
var discardBoltLogger = slog.New(slog.NewTextHandler(io.Discard, nil)).With("component", "bolt")

// logger returns the server's structured logger, falling back to a shared
// discard handler when callers construct a Server literal directly (test
// fixtures outside of NewWithDatabaseManager). The production ctor always
// populates s.log, so this is purely defensive — and read-only on the
// fallback path so concurrent callers do not race on the shared field.
func (s *Server) logger() *slog.Logger {
	if s == nil || s.log == nil {
		return discardBoltLogger
	}
	return s.log
}

// SetDatabaseAccessMode sets the per-database access mode (e.g. from HTTP server).
// When set, CanAccessDatabase(dbName) is checked before running each query.
func (s *Server) SetDatabaseAccessMode(mode auth.DatabaseAccessMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databaseAccessMode = mode
}

// SetDatabaseAccessModeResolver sets a per-principal resolver (e.g. from HTTP server for Phase 3 allowlist).
// When set, the resolver is called with the session's roles to get the mode for each query.
func (s *Server) SetDatabaseAccessModeResolver(resolver func(roles []string) auth.DatabaseAccessMode) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.databaseAccessModeResolver = resolver
}

// SetResolvedAccessResolver sets a per-(principal, db) resolver for Phase 4 write checks.
func (s *Server) SetResolvedAccessResolver(resolver func(roles []string, dbName string) auth.ResolvedAccess) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.resolvedAccessResolver = resolver
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
	host := strings.TrimSpace(s.config.Host)
	if host == "" {
		host = "127.0.0.1"
	}
	addr := net.JoinHostPort(host, strconv.Itoa(s.config.Port))
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", addr, err)
	}
	s.listener = listener

	announceHost := host
	if host == "0.0.0.0" || host == "::" || host == "" {
		announceHost = "localhost"
	}
	actualPort := s.config.Port
	if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok && tcpAddr.Port > 0 {
		actualPort = tcpAddr.Port
	}
	s.logger().Info("bolt server listening", "host", announceHost, "port", actualPort)

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

	// Plan 04-02 D-11 session-lifecycle instrumentation: ConnectionsActive
	// gauge Inc on accept, Dec on close (deferred); SessionDuration
	// observed on close; ConnectionsTotal{result} incremented with the
	// terminal-result enum (success | error | timeout). Result=success
	// is the default; sessionResult mutates from the message-handling
	// loop on error paths and from the panic-recover handler.
	sessionStart := time.Now()
	sessionResult := "success"
	if ms := s.metricsState; ms != nil && ms.bag != nil {
		ms.bag.ConnectionsActive.Inc()
		defer func() {
			ms.bag.ConnectionsActive.Dec()
			ms.bag.ConnectionsTotal.WithLabelValues(sessionResult).Inc()
			ms.bag.SessionDuration.Bind().Observe(context.Background(), time.Since(sessionStart).Seconds())
		}()
	}

	// Disable Nagle's algorithm for lower latency
	// Without this, small packets get delayed up to 40ms
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	// Recover from panics to prevent crashing the server
	defer func() {
		if r := recover(); r != nil {
			s.logger().Error("connection handler panic", slog.Any("recover", r))
			sessionResult = "error"
		}
	}()

	session := &Session{
		conn:       conn,
		reader:     bufio.NewReaderSize(conn, s.config.ReadBufferSize),
		writer:     bufio.NewWriterSize(conn, s.config.WriteBufferSize),
		server:     s,
		baseExec:   s.executor,
		executor:   s.executor,
		messageBuf: make([]byte, 0, 4096), // Pre-allocate 4KB message buffer
	}
	if factory, ok := s.executor.(SessionExecutorFactory); ok {
		session.executor = factory.NewSessionExecutor()
		session.baseExec = session.executor
	}

	// Enable deferred flush mode for Neo4j-style write batching
	if deferrable, ok := session.executor.(DeferrableExecutor); ok {
		deferrable.SetDeferFlush(true)
	}

	// Ensure cleanup on session end
	defer func() {
		// Flush any pending writes
		if flushable, ok := session.executor.(FlushableExecutor); ok {
			flushable.Flush()
		}
		// Disable deferred flush mode
		if deferrable, ok := session.executor.(DeferrableExecutor); ok {
			deferrable.SetDeferFlush(false)
		}
	}()

	// Perform handshake
	if err := session.handshake(); err != nil {
		s.logger().Warn("handshake failed", "remote", conn.RemoteAddr().String(), slog.Any("error", err))
		sessionResult = "error"
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
			s.logger().Warn("message handling error", "remote", conn.RemoteAddr().String(), slog.Any("error", err))
			sessionResult = "error"
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
	baseExec QueryExecutor
	executor QueryExecutor
	version  uint32

	// Database context (from HELLO message)
	database string // Database name for this session (defaults to default database)

	// Authentication state
	authenticated bool            // Whether HELLO auth succeeded
	authResult    *BoltAuthResult // Auth result with roles/permissions
	// forwardedAuthHeader carries caller identity for downstream remote constituent
	// routing (e.g. OIDC credential forwarding over Fabric/USE paths).
	forwardedAuthHeader string

	// Transaction state
	inTransaction bool
	txMetadata    map[string]any // Transaction metadata from BEGIN
	txDatabase    string

	// Query result state (for streaming with PULL)
	lastResult  *QueryResult
	resultIndex int

	// Deferred commit state (Neo4j-style optimization)
	// Writes are buffered in AsyncEngine until PULL completes
	pendingFlush bool
	flushPending bool

	// Query metadata for Neo4j driver compatibility
	queryId           int64          // Query ID counter for qid field
	lastQueryIsWrite  bool           // Was last query a write operation
	lastQueryDatabase string         // Effective database for last RUN
	lastRunMetadata   map[string]any // Metadata from last RUN message (bookmarks, tx_timeout, etc.)

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
//
// Plan 04-02 D-11a per-message instrumentation: this is the SOLE
// per-message observation site (DRY) — wraps the inner switch with a
// pre-bound MessageDuration{op} observer (MET-25 hot-path) and a
// MessagesTotal{op, result} counter increment. PULL chunks roll up into
// the parent PULL message_duration_seconds (D-11b — chunk timing is NOT
// observed separately). The observation runs in a deferred closure so
// even handler panics still emit a metric (result="error") before the
// panic re-propagates to the connection-handler's outer recover.
func (s *Session) dispatchMessage(msgType byte, data []byte) (retErr error) {
	op := boltOpName(msgType)
	start := time.Now()

	if s.server != nil {
		if ms := s.server.metricsState; ms != nil && ms.bag != nil {
			defer func() {
				rec := recover()
				result := "success"
				if rec != nil || (retErr != nil && retErr != io.EOF) {
					result = "error"
				}
				if obs, ok := ms.msgDur[op]; ok {
					obs.Observe(context.Background(), time.Since(start).Seconds())
				}
				ms.bag.MessagesTotal.WithLabelValues(op, result).Inc()

				// Plan 04-02 D-11c packstream decode-error reason
				// classification: when a handler returned an error
				// that classifies as a packstream decode failure,
				// increment packstream_decode_errors_total{reason}
				// under the closed enum. observePackstreamDecodeError
				// is a no-op for non-decode errors.
				if retErr != nil && retErr != io.EOF {
					_ = s.server.observePackstreamDecodeError(retErr)
				}

				if rec != nil {
					// Re-panic AFTER observation so the outer
					// connection-handler panic-recover (Plan 04-02
					// D-11) still fires (T-04-08-style discipline).
					panic(rec)
				}
			}()
		}
	}

	return s.dispatchInner(msgType, data)
}

// dispatchInner is the unobserved body of dispatchMessage; kept as a
// sibling so the metrics wrapper above stays a thin chokepoint.
func (s *Session) dispatchInner(msgType byte, data []byte) error {
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
	// Plan 04-02 D-11 / D-05e auth-attempts crosswire: observe a single
	// auth attempt per HELLO message at the function-exit chokepoint.
	// Result enum closed (success | failure | denied):
	//   - success: handshake completed and session.authenticated = true
	//   - failure: auth credentials rejected by the authenticator
	//   - denied:  request lacked auth when RequireAuth was true (or
	//              an unsupported scheme was offered).
	// No-op until Plan 04-06 wires the AuthMetrics bag.
	wasAuthenticatedBefore := s.authenticated
	defer func() {
		if s.server == nil {
			return
		}
		var result string
		switch {
		case s.authenticated && !wasAuthenticatedBefore:
			result = "success"
		case !s.authenticated:
			// Either auth failed (failure) or auth was required but
			// rejected the request before any credentials could
			// validate (denied). The two are bucketed as "failure" /
			// "denied" by the Phase 5 K8s-side aggregation; the Bolt
			// path classifies based on whether the failure path took a
			// scheme decision (failure) vs. a require-auth gate (denied).
			// Keep the call site simple: any non-success outcome is
			// "failure"; "denied" is reserved for explicit rejection
			// without credential evaluation.
			result = "failure"
		default:
			// Already authenticated (re-HELLO) — no new attempt.
			return
		}
		s.server.observeAuthAttempt(result)
	}()

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
			// Anonymous user gets viewer role (canonical from auth)
			s.authenticated = true
			s.authResult = &BoltAuthResult{
				Authenticated: true,
				Username:      "anonymous",
				Roles:         []string{string(auth.RoleViewer)},
			}
			s.forwardedAuthHeader = ""
		} else if scheme == "basic" {
			// Authenticate with provided credentials
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				s.server.logger().Warn("auth failed", "scheme", "basic", "principal", principal, "remote", remoteAddr, slog.Any("error", err))
				return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Invalid credentials")
			}
			s.authenticated = true
			s.authResult = result
			s.forwardedAuthHeader = ""
		} else if scheme == "bearer" {
			// JWT token authentication - used for cluster inter-node auth
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				s.server.logger().Warn("auth failed", "scheme", "bearer", "remote", remoteAddr, slog.Any("error", err))
				return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Invalid or expired token")
			}
			s.authenticated = true
			s.authResult = result
			if strings.TrimSpace(credentials) != "" {
				s.forwardedAuthHeader = "Bearer " + credentials
			}
		} else {
			return s.sendFailure("Neo.ClientError.Security.Unauthorized", fmt.Sprintf("Unsupported auth scheme: %s", scheme))
		}
	} else if s.server != nil && s.server.config.RequireAuth {
		// Auth required but no authenticator configured - reject all
		return s.sendFailure("Neo.ClientError.Security.Unauthorized", "Authentication required but not configured")
	} else {
		// No auth configured - allow all (development mode, canonical role from auth)
		s.authenticated = true
		s.authResult = &BoltAuthResult{
			Authenticated: true,
			Username:      "anonymous",
			Roles:         []string{string(auth.RoleAdmin)},
		}
		s.forwardedAuthHeader = ""
	}

	// Extract and validate database name
	dbName := authParams["database"]
	if dbName == "" && s.server != nil && s.server.dbManager != nil {
		// Use default database if not specified
		dbName = s.server.dbManager.DefaultDatabaseName()
	}

	// Validate database exists (if dbManager is configured).
	// Use constituentAwareExists to accept dotted composite.alias references.
	if dbName != "" && s.server != nil && s.server.dbManager != nil {
		if !constituentAwareExists(s.server.dbManager, dbName) {
			return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' does not exist", dbName))
		}
	}

	// Store database for this session
	s.database = dbName

	if s.server != nil && s.server.config.LogQueries {
		remoteAddr := "unknown"
		if s.conn != nil {
			remoteAddr = s.conn.RemoteAddr().String()
		}
		// D-10a: drop the "[BOLT]" bracket — component=bolt is already
		// baked in via .With at ctor time. D-03a: any "credentials" attr
		// would be auto-redacted by the redactingHandler chain (it is in
		// DefaultRedactKeys); we deliberately do NOT log credentials
		// here, but the redaction guard remains in force as a defense
		// in depth.
		s.server.logger().Info("hello",
			"remote", remoteAddr,
			"user", s.authResult.Username,
			"roles", s.authResult.Roles,
			"database", dbName,
		)
	}

	return s.sendSuccess(map[string]any{
		"server":        s.server.config.serverAnnouncement(),
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
	ctx = cypher.WithAuthToken(ctx, s.forwardedAuthHeader)

	// Classify query type once (used for auth and deferred flush)
	upperQuery := strings.ToUpper(query)
	isWrite := strings.Contains(upperQuery, "CREATE") ||
		strings.Contains(upperQuery, "DELETE") ||
		strings.Contains(upperQuery, "SET ") ||
		strings.Contains(upperQuery, "MERGE") ||
		strings.Contains(upperQuery, "REMOVE ")
	isSchema := strings.Contains(upperQuery, "INDEX") ||
		strings.Contains(upperQuery, "CONSTRAINT")

	// Check permissions based on query type (use canonical entitlement IDs from auth)
	if s.authResult != nil {
		if isSchema && !s.authResult.HasPermission(string(auth.PermSchema)) {
			return s.sendFailure("Neo.ClientError.Security.Forbidden", "Schema operations require schema permission")
		}
		if isWrite && !s.authResult.HasPermission(string(auth.PermWrite)) {
			return s.sendFailure("Neo.ClientError.Security.Forbidden", "Write operations require write permission")
		}
		if !s.authResult.HasPermission(string(auth.PermRead)) {
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
		// D-10a: "[BOLT]" bracket dropped (component attribute carries
		// it). D-03a: any "credentials"/"password"/"token" key in the
		// `params` map is auto-redacted by the Plan 02-01 redactingHandler
		// chain via DefaultRedactKeys.
		if len(params) > 0 {
			s.server.logger().Debug("query",
				"user", user, "remote", remoteAddr,
				"query", truncateQuery(query, 200),
				"params", params,
			)
		} else {
			s.server.logger().Debug("query",
				"user", user, "remote", remoteAddr,
				"query", truncateQuery(query, 200),
			)
		}
	}

	// Resolve effective database name (Neo4j-compatible precedence):
	// 1) RUN metadata db/database
	// 2) active transaction metadata db/database (BEGIN)
	// 3) session database (HELLO)
	// 4) server default
	dbName := ""
	if runDB, ok := databaseFromMetadata(metadata); ok {
		dbName = runDB
	} else if s.inTransaction {
		if txDB, ok := databaseFromMetadata(s.txMetadata); ok {
			dbName = txDB
		}
	}
	if dbName == "" {
		dbName = s.database
	}
	if dbName == "" {
		if s.server != nil && s.server.dbManager != nil {
			dbName = s.server.dbManager.DefaultDatabaseName()
		} else {
			dbName = "nornic" // single-DB mode default
		}
	}
	if s.server != nil && s.server.dbManager != nil && dbName != "" && !constituentAwareExists(s.server.dbManager, dbName) {
		return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
			fmt.Sprintf("Database '%s' does not exist", dbName))
	}
	// Per-database access: deny if principal may not access this database (Neo4j-aligned).
	var mode auth.DatabaseAccessMode
	if s.server != nil && s.server.databaseAccessModeResolver != nil {
		var roles []string
		if s.authResult != nil {
			roles = s.authResult.Roles
		}
		mode = s.server.databaseAccessModeResolver(roles)
	} else if s.server != nil && s.server.databaseAccessMode != nil {
		mode = s.server.databaseAccessMode
	}
	// When auth is required but no resolver/mode was set (e.g. standalone Bolt), deny all DB access (secure default).
	if mode == nil && s.server != nil && s.server.config.RequireAuth {
		mode = auth.DenyAllDatabaseAccessMode
	}
	if mode != nil && !mode.CanAccessDatabase(dbName) {
		return s.sendFailure("Neo.ClientError.Security.Forbidden",
			fmt.Sprintf("Access to database '%s' is not allowed.", dbName))
	}

	// Per-DB write: for mutations, require ResolvedAccess.Write for this (principal, db).
	if isWrite && s.server != nil && s.server.resolvedAccessResolver != nil {
		var roles []string
		if s.authResult != nil {
			roles = s.authResult.Roles
		}
		ra := s.server.resolvedAccessResolver(roles, dbName)
		if !ra.Write {
			return s.sendFailure("Neo.ClientError.Security.Forbidden",
				fmt.Sprintf("Write on database '%s' is not allowed.", dbName))
		}
	}

	// Keep explicit transactions pinned to the executor selected at BEGIN.
	executor := s.executor
	if s.inTransaction {
		if s.txDatabase != "" && !strings.EqualFold(dbName, s.txDatabase) {
			return s.sendFailure("Neo.ClientError.Transaction.InvalidBookmark",
				fmt.Sprintf("Explicit transaction is bound to database '%s', got '%s'", s.txDatabase, dbName))
		}
	} else if s.server != nil && s.server.dbManager != nil {
		dbExecutor, err := s.getExecutorForDatabase(dbName)
		if err != nil {
			return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' not found: %v", dbName, err))
		}
		executor = dbExecutor
	}

	// Execute query (ctx may have timeout from tx_timeout metadata)
	runStart := time.Now()
	result, err := executor.Execute(ctx, query, params)
	if err != nil {
		s.logRunTiming("ERROR", dbName, query, time.Since(runStart), 0, err)
		if s.server != nil && s.server.config.LogQueries {
			s.server.logger().Warn("query error", slog.Any("error", err))
		}
		code, msg := mapBoltQueryError(err)
		return s.sendFailure(code, msg)
	}
	rows := 0
	if result != nil && result.Rows != nil {
		rows = len(result.Rows)
	}
	s.logRunTiming("OK", dbName, query, time.Since(runStart), rows, nil)

	// Per-database RBAC: filter SHOW DATABASES results by CanSeeDatabase so principals only see DBs they may access
	if isShowDatabasesQuery(query) && result.Rows != nil && mode != nil {
		filtered := make([][]interface{}, 0, len(result.Rows))
		for _, row := range result.Rows {
			if len(row) > 0 {
				if name, ok := row[0].(string); ok && mode.CanSeeDatabase(name) {
					filtered = append(filtered, row)
				}
			}
		}
		result.Rows = filtered
	}

	// Track write operation for deferred flush
	if isWrite {
		s.pendingFlush = true
	}
	s.lastQueryIsWrite = isWrite
	s.lastQueryDatabase = dbName

	// Store result for PULL
	s.lastResult = result
	s.resultIndex = 0
	s.queryId++

	// Return SUCCESS with field names (Neo4j compatible metadata)
	// Note: Neo4j only sends qid for EXPLICIT transactions, not implicit/autocommit
	// For implicit transactions, only send fields and t_first
	if s.inTransaction {
		if err := s.sendSuccessNoFlush(map[string]any{
			"fields":  result.Columns,
			"t_first": int64(0),
			"qid":     s.queryId,
		}); err != nil {
			return err
		}
		return s.flushIfPending()
	}
	if err := s.sendSuccessNoFlush(map[string]any{
		"fields":  result.Columns,
		"t_first": int64(0),
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

func (s *Session) logRunTiming(status, dbName, query string, duration time.Duration, rows int, runErr error) {
	includeQuery := s.server != nil && s.server.config.LogQueries
	if runErr == nil && !includeQuery {
		return
	}
	if s.server == nil {
		return
	}

	remoteAddr := "unknown"
	if s.conn != nil {
		remoteAddr = s.conn.RemoteAddr().String()
	}
	user := "unknown"
	if s.authResult != nil {
		user = s.authResult.Username
	}

	// D-10a: "[BOLT]" bracket dropped (component attribute carries it).
	// Errors emit at WARN; successful query timing (only fires when
	// LogQueries=true) emits at DEBUG so it doesn't pollute production
	// stdout at INFO level.
	if runErr != nil {
		attrs := []any{
			"user", user, "remote", remoteAddr,
			"database", dbName, "status", status,
			"rows", rows, "duration", duration,
			slog.Any("error", runErr),
		}
		if includeQuery {
			attrs = append(attrs, "query", truncateQuery(query, 200))
		}
		s.server.logger().Warn("run", attrs...)
		return
	}

	attrs := []any{
		"user", user, "remote", remoteAddr,
		"database", dbName, "status", status,
		"rows", rows, "duration", duration,
	}
	if includeQuery {
		attrs = append(attrs, "query", truncateQuery(query, 200))
	}
	s.server.logger().Debug("run", attrs...)
}

func mapBoltQueryError(err error) (code, message string) {
	if err == nil {
		return "Neo.ClientError.Statement.SyntaxError", ""
	}
	msg := err.Error()
	if strings.HasPrefix(msg, "Neo.") {
		if idx := strings.Index(msg, ":"); idx > 0 {
			return strings.TrimSpace(msg[:idx]), strings.TrimSpace(msg[idx+1:])
		}
		return msg, msg
	}
	if start := strings.Index(msg, "Neo."); start >= 0 {
		rest := msg[start:]
		if idx := strings.Index(rest, ":"); idx > 0 {
			return strings.TrimSpace(rest[:idx]), strings.TrimSpace(rest[idx+1:])
		}
	}
	if transientCode, ok := nornicerrors.MapTransientTransactionError(err); ok {
		return transientCode, msg
	}
	return "Neo.ClientError.Statement.SyntaxError", msg
}

// mapBoltCommitError preserves Bolt's commit-failed fallback for ordinary
// errors while allowing retryable transaction conflicts to surface as Neo4j
// transient errors.
func mapBoltCommitError(err error) (code, message string) {
	code, message = mapBoltQueryError(err)
	if code == "Neo.ClientError.Statement.SyntaxError" {
		return "Neo.ClientError.Transaction.TransactionCommitFailed", message
	}
	return code, message
}

// truncateQuery truncates a query for logging.
func truncateQuery(q string, maxLen int) string {
	if len(q) <= maxLen {
		return q
	}
	return q[:maxLen] + "..."
}

// isShowDatabasesQuery returns true if the normalized statement is SHOW DATABASES (flexible whitespace).
// Used to filter SHOW DATABASES results by CanSeeDatabase when per-database RBAC is enabled.
func isShowDatabasesQuery(query string) bool {
	norm := strings.TrimSpace(query)
	norm = strings.Join(strings.Fields(norm), " ")
	return strings.EqualFold(norm, "SHOW DATABASES")
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
		if err := s.sendSuccessNoFlush(map[string]any{}); err != nil {
			return err
		}
		return s.flushIfPending()
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
			if receiptBookmark, ok := s.bookmarkFromReceipt(); ok {
				bookmark = receiptBookmark
			} else {
				bookmark = s.generateBookmark()
			}
		}

		// Build stats matching Neo4j format (only if there are updates)
		metadata := map[string]any{
			"bookmark": bookmark,
			"type":     queryType,
			"t_last":   int64(0), // Streaming time
		}
		if s.lastQueryDatabase != "" {
			metadata["db"] = s.lastQueryDatabase
		} else if s.database != "" {
			metadata["db"] = s.database
		} else {
			metadata["db"] = "nornic"
		}

		// Note: Neo4j does NOT send has_more when it's false
		if err := s.sendSuccessNoFlush(metadata); err != nil {
			return err
		}
		return s.flushIfPending()
	}

	// When there are more records, send has_more: true
	if err := s.sendSuccessNoFlush(map[string]any{
		"has_more": true,
	}); err != nil {
		return err
	}
	return s.flushIfPending()
}

func databaseFromMetadata(metadata map[string]any) (string, bool) {
	if len(metadata) == 0 {
		return "", false
	}
	if raw, ok := metadata["db"]; ok {
		if db, ok := raw.(string); ok {
			db = strings.TrimSpace(db)
			if db != "" {
				return db, true
			}
		}
	}
	if raw, ok := metadata["database"]; ok {
		if db, ok := raw.(string); ok {
			db = strings.TrimSpace(db)
			if db != "" {
				return db, true
			}
		}
	}
	return "", false
}

// handleDiscard handles the DISCARD message.
func (s *Session) handleDiscard(data []byte) error {
	s.lastResult = nil
	s.resultIndex = 0
	// Neo4j doesn't send has_more when false - just empty metadata
	if err := s.sendSuccessNoFlush(map[string]any{}); err != nil {
		return err
	}
	return s.flushIfPending()
}

// handleRoute handles the ROUTE message (for cluster routing).
func (s *Session) handleRoute(data []byte) error {
	address := "localhost:7687"
	if s.conn != nil {
		if tcp, ok := s.conn.LocalAddr().(*net.TCPAddr); ok {
			host := tcp.IP.String()
			if host == "" || host == "0.0.0.0" || host == "::" {
				host = "localhost"
			}
			address = fmt.Sprintf("%s:%d", host, tcp.Port)
		}
	}

	if err := s.sendSuccessNoFlush(map[string]any{
		"rt": map[string]any{
			"ttl": 300,
			"servers": []map[string]any{
				{"role": "ROUTE", "addresses": []string{address}},
				{"role": "READ", "addresses": []string{address}},
				{"role": "WRITE", "addresses": []string{address}},
			},
		},
	}); err != nil {
		return err
	}
	return s.flushIfPending()
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
	s.txDatabase = ""
	if s.baseExec != nil {
		s.executor = s.baseExec
	}
	s.lastResult = nil
	s.resultIndex = 0
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
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

	if s.server != nil && s.server.dbManager != nil {
		dbName := s.database
		if txDB, ok := databaseFromMetadata(metadata); ok {
			dbName = txDB
		}
		if dbName == "" {
			dbName = s.server.dbManager.DefaultDatabaseName()
		}
		txExec, err := s.getTransactionalExecutorForDatabase(dbName)
		if err != nil {
			return s.sendFailure("Neo.ClientError.Database.DatabaseNotFound",
				fmt.Sprintf("Database '%s' not found: %v", dbName, err))
		}
		s.executor = txExec
		s.txDatabase = dbName
	} else {
		s.txDatabase = ""
	}

	// If executor supports transactions, start one
	if txExec, ok := s.executor.(TransactionalExecutor); ok {
		ctx := context.Background()
		if err := txExec.BeginTransaction(ctx, metadata); err != nil {
			return s.sendFailure("Neo.ClientError.Transaction.TransactionStartFailed", err.Error())
		}
	}

	s.inTransaction = true
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
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
			s.txDatabase = ""
			if s.baseExec != nil {
				s.executor = s.baseExec
			}
			code, message := mapBoltCommitError(err)
			return s.sendFailure(code, message)
		}
	}

	s.inTransaction = false
	s.txMetadata = nil
	s.txDatabase = ""
	if s.baseExec != nil {
		s.executor = s.baseExec
	}

	// Generate and store new bookmark for causal consistency
	// This increments the server's transaction sequence and creates a bookmark
	bookmark := s.generateBookmark()

	// Return bookmark for client tracking
	if err := s.sendSuccessNoFlush(map[string]any{
		"bookmark": bookmark,
	}); err != nil {
		return err
	}
	return s.flushIfPending()
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

	return formatBookmark(uint64(seqNum))
}

func (s *Session) currentBookmark() string {
	if s.server == nil {
		return formatBookmark(0)
	}
	s.server.txSequenceMu.RLock()
	seqNum := s.server.txSequence
	s.server.txSequenceMu.RUnlock()
	if seqNum < 0 {
		seqNum = 0
	}
	return formatBookmark(uint64(seqNum))
}

func (s *Session) bookmarkFromReceipt() (string, bool) {
	if s.lastResult == nil || s.lastResult.Metadata == nil {
		return "", false
	}

	receiptAny, ok := s.lastResult.Metadata["receipt"]
	if !ok || receiptAny == nil {
		return "", false
	}

	var seq uint64
	switch r := receiptAny.(type) {
	case *storage.Receipt:
		if r != nil {
			seq = r.WALSeqEnd
		}
	case storage.Receipt:
		seq = r.WALSeqEnd
	case map[string]interface{}:
		if val, ok := r["wal_seq_end"].(uint64); ok {
			seq = val
		} else if val, ok := r["wal_seq_end"].(float64); ok {
			seq = uint64(val)
		} else if val, ok := r["wal_seq_end"].(int64); ok {
			seq = uint64(val)
		}
	}

	if seq == 0 {
		return "", false
	}

	s.updateBookmarkSequence(seq)
	return formatBookmark(seq), true
}

func (s *Session) updateBookmarkSequence(seq uint64) {
	if s.server == nil {
		return
	}

	s.server.txSequenceMu.Lock()
	if int64(seq) > s.server.txSequence {
		s.server.txSequence = int64(seq)
	}
	s.server.txSequenceMu.Unlock()
}

func formatBookmark(seq uint64) string {
	return fmt.Sprintf("nornicdb:bookmark:%d", seq)
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
		if err := s.sendSuccessNoFlush(nil); err != nil {
			return err
		}
		return s.flushIfPending()
	}

	// If executor supports transactions, rollback
	if txExec, ok := s.executor.(TransactionalExecutor); ok {
		ctx := context.Background()
		if err := txExec.RollbackTransaction(ctx); err != nil {
			// Rollback failed, but we still clear state
			s.inTransaction = false
			s.txMetadata = nil
			if err := s.sendFailure("Neo.ClientError.Transaction.TransactionRollbackFailed", err.Error()); err != nil {
				return err
			}
			return s.flushIfPending()
		}
	}

	s.inTransaction = false
	s.txMetadata = nil
	s.txDatabase = ""
	if s.baseExec != nil {
		s.executor = s.baseExec
	}
	if err := s.sendSuccessNoFlush(nil); err != nil {
		return err
	}
	return s.flushIfPending()
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

func (s *Session) sendSuccessNoFlush(metadata map[string]any) error {
	buf := s.recordBuf
	if cap(buf) < 16*1024 {
		buf = make([]byte, 0, 16*1024)
	}
	buf = buf[:0]

	buf = append(buf, successHeader...)
	buf = encodePackStreamMapInto(buf, metadata)

	if err := s.writeMessageNoFlush(buf); err != nil {
		return err
	}
	s.recordBuf = buf[:0]
	s.flushPending = true
	return nil
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

func (s *Session) flushIfPending() error {
	if !s.flushPending {
		return nil
	}
	s.flushPending = false
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

	useAuthScopedResolver := false
	if resolver, ok := s.server.dbManager.(authAwareStorageResolver); ok && strings.TrimSpace(s.forwardedAuthHeader) != "" {
		_ = resolver
		useAuthScopedResolver = true
	}

	if !useAuthScopedResolver {
		s.server.executorsMu.RLock()
		if executor, ok := s.server.executors[dbName]; ok {
			s.server.executorsMu.RUnlock()
			return executor, nil
		}
		s.server.executorsMu.RUnlock()
	}

	executor, err := s.newDatabaseScopedCypherExecutor(dbName, useAuthScopedResolver)
	if err != nil {
		return nil, err
	}

	dbExecutor := &boltQueryExecutorAdapter{executor: executor}
	if useAuthScopedResolver {
		return dbExecutor, nil
	}

	s.server.executorsMu.Lock()
	if s.server.executors == nil {
		s.server.executors = make(map[string]QueryExecutor)
	}
	if existing, ok := s.server.executors[dbName]; ok {
		s.server.executorsMu.Unlock()
		return existing, nil
	}
	s.server.executors[dbName] = dbExecutor
	s.server.executorsMu.Unlock()

	return dbExecutor, nil
}

type authAwareStorageResolver interface {
	GetStorageWithAuth(name string, authToken string) (storage.Engine, error)
}

type databaseExecutorConfigurator interface {
	ConfigureDatabaseExecutor(exec *cypher.StorageExecutor, dbName string, storageEngine storage.Engine)
}

type baseCypherExecutorProvider interface {
	BaseCypherExecutor() *cypher.StorageExecutor
}

func (s *Session) getTransactionalExecutorForDatabase(dbName string) (QueryExecutor, error) {
	if s.server == nil || s.server.dbManager == nil {
		return nil, fmt.Errorf("database manager not available")
	}
	useAuthScopedResolver := false
	if resolver, ok := s.server.dbManager.(authAwareStorageResolver); ok && strings.TrimSpace(s.forwardedAuthHeader) != "" {
		_ = resolver
		useAuthScopedResolver = true
	}
	executor, err := s.newDatabaseScopedCypherExecutor(dbName, useAuthScopedResolver)
	if err != nil {
		return nil, err
	}
	return &transactionalBoltQueryExecutorAdapter{
		boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: executor},
	}, nil
}

func (s *Session) newDatabaseScopedCypherExecutor(dbName string, useAuthScopedResolver bool) (*cypher.StorageExecutor, error) {
	var (
		storageEngine storage.Engine
		err           error
	)
	if resolver, ok := s.server.dbManager.(authAwareStorageResolver); ok && useAuthScopedResolver {
		storageEngine, err = resolver.GetStorageWithAuth(dbName, s.forwardedAuthHeader)
	} else {
		storageEngine, err = s.server.dbManager.GetStorage(dbName)
	}
	if err != nil {
		return nil, err
	}

	executor := cypher.NewStorageExecutor(storageEngine)
	if baseAdapter, ok := s.server.executor.(*boltQueryExecutorAdapter); ok && baseAdapter != nil && baseAdapter.executor != nil {
		if emb := baseAdapter.executor.GetEmbedder(); emb != nil {
			executor.SetEmbedder(emb)
		}
	}
	if cfg, ok := s.server.executor.(databaseExecutorConfigurator); ok && cfg != nil {
		cfg.ConfigureDatabaseExecutor(executor, dbName, storageEngine)
	}
	if provider, ok := s.server.executor.(baseCypherExecutorProvider); ok && provider != nil {
		if baseExec := provider.BaseCypherExecutor(); baseExec != nil {
			if emb := baseExec.GetEmbedder(); emb != nil {
				executor.SetEmbedder(emb)
			}
		}
	}
	if mgr, ok := s.server.dbManager.(*multidb.DatabaseManager); ok {
		executor.SetDatabaseManager(&boltDatabaseManagerAdapter{manager: mgr})
	}
	return executor, nil
}

// boltDatabaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface inside the Bolt package.
type boltDatabaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *boltDatabaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}
func (a *boltDatabaseManagerAdapter) DropDatabase(name string) error {
	return a.manager.DropDatabase(name)
}
func (a *boltDatabaseManagerAdapter) Exists(name string) bool { return a.manager.Exists(name) }
func (a *boltDatabaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}
func (a *boltDatabaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}
func (a *boltDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}
func (a *boltDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}
func (a *boltDatabaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}
func (a *boltDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}
func (a *boltDatabaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        getStringFromMap(m, "alias"),
					DatabaseName: getStringFromMap(m, "database_name"),
					Type:         getStringFromMap(m, "type"),
					AccessMode:   getStringFromMap(m, "access_mode"),
					URI:          getStringFromMap(m, "uri"),
					SecretRef:    getStringFromMap(m, "secret_ref"),
					AuthMode:     getStringFromMap(m, "auth_mode"),
					User:         getStringFromMap(m, "user"),
					Password:     getStringFromMap(m, "password"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}
func (a *boltDatabaseManagerAdapter) DropCompositeDatabase(name string) error {
	return a.manager.DropCompositeDatabase(name)
}
func (a *boltDatabaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	if m, ok := constituent.(map[string]interface{}); ok {
		return a.manager.AddConstituent(compositeName, multidb.ConstituentRef{
			Alias:        getStringFromMap(m, "alias"),
			DatabaseName: getStringFromMap(m, "database_name"),
			Type:         getStringFromMap(m, "type"),
			AccessMode:   getStringFromMap(m, "access_mode"),
			URI:          getStringFromMap(m, "uri"),
			SecretRef:    getStringFromMap(m, "secret_ref"),
			AuthMode:     getStringFromMap(m, "auth_mode"),
			User:         getStringFromMap(m, "user"),
			Password:     getStringFromMap(m, "password"),
		})
	}
	ref, ok := constituent.(multidb.ConstituentRef)
	if !ok {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}
func (a *boltDatabaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}
func (a *boltDatabaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	cons, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(cons))
	for i, c := range cons {
		out[i] = c
	}
	return out, nil
}
func (a *boltDatabaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &boltDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *boltDatabaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &boltDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *boltDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}
func (a *boltDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

type boltDatabaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *boltDatabaseInfoAdapter) Name() string         { return a.info.Name }
func (a *boltDatabaseInfoAdapter) Type() string         { return a.info.Type }
func (a *boltDatabaseInfoAdapter) Status() string       { return a.info.Status }
func (a *boltDatabaseInfoAdapter) IsDefault() bool      { return a.info.IsDefault }
func (a *boltDatabaseInfoAdapter) CreatedAt() time.Time { return a.info.CreatedAt }

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
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
		Columns:  result.Columns,
		Rows:     result.Rows,
		Metadata: result.Metadata,
	}, nil
}

// transactionalBoltQueryExecutorAdapter owns one database-scoped explicit
// transaction executor. It is intentionally not cached across sessions.
type transactionalBoltQueryExecutorAdapter struct {
	boltQueryExecutorAdapter

	mu   sync.Mutex
	inTx bool
}

func (a *transactionalBoltQueryExecutorAdapter) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.boltQueryExecutorAdapter.Execute(ctx, query, params)
}

func (a *transactionalBoltQueryExecutorAdapter) BeginTransaction(ctx context.Context, _ map[string]any) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.inTx {
		return nil
	}
	if _, err := a.boltQueryExecutorAdapter.Execute(ctx, "BEGIN", nil); err != nil {
		return err
	}
	a.inTx = true
	return nil
}

func (a *transactionalBoltQueryExecutorAdapter) CommitTransaction(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.inTx {
		return nil
	}
	_, err := a.boltQueryExecutorAdapter.Execute(ctx, "COMMIT", nil)
	a.inTx = false
	return err
}

func (a *transactionalBoltQueryExecutorAdapter) RollbackTransaction(ctx context.Context) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if !a.inTx {
		return nil
	}
	_, err := a.boltQueryExecutorAdapter.Execute(ctx, "ROLLBACK", nil)
	a.inTx = false
	return err
}
