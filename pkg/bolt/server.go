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
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/text/language"
)

var boltTxWriteAnalyzer = cypher.NewQueryAnalyzer(256)

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
	config                         *Config
	listener                       net.Listener
	mu                             sync.RWMutex
	sessions                       map[string]*Session
	closed                         atomic.Bool
	rawTransactionExecutorPoisoned atomic.Bool

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
	resolvedAccessResolver    func(roles []string, dbName string) auth.ResolvedAccess
	databaseConnectionAdmit   func(databaseName string) error
	databaseConnectionRelease func(databaseName string)

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

	// activeConnections is a single shared cap across all four transports
	// (tcp, tcp_tls, ws, ws_tls); the accept loop closes new conns past
	// MaxConnections.
	activeConnections atomic.Int64

	// discoveryResponse holds the pre-encoded HTTP/1.1 200 response served
	// to plain GET / probes on the Bolt port. Refreshed by a 1s ticker so
	// the Date header stays current.
	discoveryResponse atomic.Pointer[[]byte]
	// discoveryStop cancels the discovery refresh goroutine on Close.
	discoveryStop chan struct{}

	// upgrader is the gorilla/websocket Upgrader configured per Config at
	// ListenAndServe time. Reused across all WS upgrades.
	upgrader *websocket.Upgrader
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

// SessionExecutorFactory creates per-connection executors. For explicit
// transactions, each call must return a distinct TransactionalExecutor; a
// shared instance cannot safely represent concurrent transaction ownership.
type SessionExecutorFactory interface {
	NewSessionExecutor() QueryExecutor
}

// TransactionalExecutor extends QueryExecutor with transaction support.
//
// Atomic BEGIN/COMMIT/ROLLBACK requires NewWithDatabaseManager or a
// SessionExecutorFactory that returns a distinct TransactionalExecutor for
// each connection. A directly supplied TransactionalExecutor is supported only
// with MaxConnections set to 1 and is quarantined after cleanup failure or an
// uncertain commit outcome. Multi-connection servers reject BEGIN for a shared raw
// executor. Without this interface, transaction messages are acknowledged for
// compatibility but operations are auto-committed.
//
// A factory can adapt an application-specific constructor without sharing its
// transaction field between connections:
//
//	type TxExecutorFactory struct {
//		QueryExecutor
//		newExecutor func() TransactionalExecutor
//	}
//
//	func (f *TxExecutorFactory) NewSessionExecutor() QueryExecutor {
//		return f.newExecutor()
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
	Stats    *QueryStats // Write counters for ResultSummary
}

// QueryStats holds write counters emitted in the Bolt PULL completion metadata.
// Field names match the Neo4j Bolt protocol specification for "stats".
type QueryStats struct {
	NodesCreated         int
	NodesDeleted         int
	RelationshipsCreated int
	RelationshipsDeleted int
	PropertiesSet        int
	LabelsAdded          int
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

	// Localizer renders human-readable Bolt failures. Nil preserves en-US.
	Localizer *localization.Manager

	// TLSConfig, when non-nil, enables TLS-on-first-byte sniffing on the
	// Bolt port. Mirrors Neo4j's sslContext + requiresEncryption pattern:
	// one listener accepts plain bolt://, bolt+s://, ws://, and wss://
	// based on the first 5 bytes of the connection.
	TLSConfig *tls.Config
	// RequireTLS rejects any non-TLS connection on the Bolt port with the
	// canonical Neo4j error message.
	RequireTLS bool
	// BoltSniffTimeout bounds the transport-sniff peek (default 5s).
	BoltSniffTimeout time.Duration
	// BoltAuthTimeout bounds the pre-HELLO handshake/auth window after
	// transport selection (default 30s, matches Neo4j).
	BoltAuthTimeout time.Duration
	// BoltStatementTimeout bounds the wall-clock duration of a single RUN
	// when the client did not supply tx_timeout in the RUN/BEGIN extras.
	// Zero (the default) disables the server-side cap and matches the
	// historical behavior. The Bolt driver's per-call tx_timeout, when
	// present, takes precedence (Neo4j semantics: client-supplied
	// timeout wins; server cap is the fallback). The cancellation
	// propagates via context to the Cypher executor, which honors
	// ctx.Err() at every traversal/match boundary.
	BoltStatementTimeout time.Duration
	// WebSocketEnabled controls whether WebSocket transport is accepted on
	// the Bolt port. When false, the server returns the discovery response
	// for plain GET / and HTTP 426 for actual WS upgrade attempts.
	// Defaults to true.
	WebSocketEnabled bool
	// WebSocketAllowedOrigins is a comma-separated list of allowed Origin
	// header values for WS upgrades. Use "*" for any origin.
	WebSocketAllowedOrigins string
	// WebSocketMaxMessageSize bounds inbound WS BinaryMessage size
	// (default 65536, matches Neo4j MAX_WEBSOCKET_FRAME_SIZE).
	WebSocketMaxMessageSize int64
	// WebSocketWriteBufferSize is the bufio writer buffer size used for
	// sessions whose transport is WS or WS+TLS (default 256 KB).
	WebSocketWriteBufferSize int
	// WebSocketPingInterval is the cadence at which the server sends WS
	// ping control frames (default 30s).
	WebSocketPingInterval time.Duration
	// WebSocketPongTimeout is the deadline within which a pong must
	// arrive after a ping (default 60s).
	WebSocketPongTimeout time.Duration
	// OAuthConfig optionally provides the OAuth/OIDC discovery payload
	// emitted on plain GET / probes. When nil or unconfigured, the
	// discovery body is empty (Community-parity).
	OAuthConfig *auth.OAuthConfig
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
		Host:                     "127.0.0.1",
		Port:                     7687,
		MaxConnections:           100,
		ReadBufferSize:           8192,
		WriteBufferSize:          64 * 1024,
		BoltSniffTimeout:         5 * time.Second,
		BoltAuthTimeout:          30 * time.Second,
		WebSocketEnabled:         true,
		WebSocketAllowedOrigins:  "*",
		WebSocketMaxMessageSize:  65536,
		WebSocketWriteBufferSize: 256 * 1024,
		WebSocketPingInterval:    30 * time.Second,
		WebSocketPongTimeout:     60 * time.Second,
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

// SetDatabaseConnectionAdmission configures per-database connection admission.
func (s *Server) SetDatabaseConnectionAdmission(admit func(string) error, release func(string)) {
	s.databaseConnectionAdmit = admit
	s.databaseConnectionRelease = release
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
	s.logEvent(context.Background(), slog.LevelInfo, localization.BoltServerListeningEvent(announceHost, actualPort))

	// Validate the discovery body once at startup and start the 1s Date
	// refresh ticker. Startup fails if OAuthConfig has malformed URLs.
	if err := s.startDiscoveryRefresher(); err != nil {
		_ = s.listener.Close()
		return fmt.Errorf("discovery response startup: %w", err)
	}

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
	s.stopDiscoveryRefresher()
	if s.listener != nil {
		return s.listener.Close()
	}
	return nil
}

// IsClosed returns whether the server is closed.
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// handleConnection handles a single client connection. It performs
// transport selection (raw TCP / TLS / WS / WS+TLS) before constructing
// the Bolt Session and then drives the message loop.
func (s *Server) handleConnection(conn net.Conn) {
	// MaxConnections enforcement (single shared cap across all four
	// transports). Atomic CAS — no channel, no syscall on the hot path.
	if max := s.config.MaxConnections; max > 0 {
		if s.activeConnections.Add(1) > int64(max) {
			s.activeConnections.Add(-1)
			if ms := s.metricsState; ms != nil && ms.bag != nil {
				incBoltConnectionsRejected(ms.bag, "max_connections")
			}
			_ = conn.Close()
			return
		}
	} else {
		s.activeConnections.Add(1)
	}
	defer s.activeConnections.Add(-1)
	// closeTarget is the conn we close on exit. It starts as the raw
	// accepted conn and is replaced by the post-sniff wrapper (a
	// *tls.Conn, *wsConn, or both) once transport selection runs, so
	// the right shutdown semantics fire on the way out — TLS sends
	// close_notify, wsConn writes a WS close frame, etc. Closing the
	// raw conn directly would skip those.
	closeTarget := conn
	defer func() { _ = closeTarget.Close() }()

	// Disable Nagle's algorithm on the underlying TCP socket for lower
	// latency. Harmless no-op for non-TCP wrappers (tls.Conn, wsConn).
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		tcpConn.SetNoDelay(true)
	}

	// Plan 04-02 D-11 session-lifecycle instrumentation. We defer Inc
	// until after transport selection so the label that gets Inc'd
	// matches the label the deferred Dec sees — a connection that ends
	// up as ws_tls is recorded entirely under ws_tls, not Inc'd as tcp
	// and Dec'd as ws_tls (which would silently leak gauges).
	//
	// emitMetrics is set to true once a session actually starts; the
	// short-circuit paths (transport-sniff failure, discovery probe,
	// 426 refusal) record their own counters via incBoltConnectionsRejected
	// or skip the lifecycle metric entirely so refused upgrades aren't
	// counted as result=success.
	sessionStart := time.Now()
	sessionResult := "success"
	transportLabel := ""
	emitMetrics := false
	defer func() {
		if !emitMetrics {
			return
		}
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			decBoltConnectionsActive(ms.bag, transportLabel)
			incBoltConnectionsTotal(ms.bag, sessionResult, transportLabel)
			ms.bag.SessionDuration.Bind().Observe(context.Background(), time.Since(sessionStart).Seconds())
		}
	}()

	// Recover from panics to prevent crashing the server. Sits at the
	// outermost layer so a panic anywhere in transport selection or the
	// session loop is caught.
	defer func() {
		if r := recover(); r != nil {
			s.logEvent(context.Background(), slog.LevelError, localization.BoltConnectionHandlerPanicEvent(r))
			sessionResult = "error"
		}
	}()

	remoteAddr := conn.RemoteAddr().String()

	// Transport sniff: peek the first 5 bytes and dispatch.
	kind, sniffedConn, br, err := peekTransport(
		conn,
		s.config.TLSConfig,
		false, // not yet encrypted
		s.config.RequireTLS,
		s.config.ReadBufferSize,
		s.config.BoltSniffTimeout,
	)
	if err != nil {
		reason := classifySniffError(err)
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			incBoltConnectionsRejected(ms.bag, reason)
			// A failed sniff is still a Bolt-session-attempt termination
			// in the lifecycle ledger (the connection opened but never
			// produced enough bytes to identify itself). The transport
			// label is "tcp" because we never got far enough to know
			// what the client wanted.
			incBoltConnectionsActive(ms.bag, "tcp")
			transportLabel = "tcp"
			emitMetrics = true
		}
		if errors.Is(err, ErrUnencryptedRequired) {
			s.logEvent(context.Background(), slog.LevelWarn, localization.BoltUnencryptedConnectionRejectedEvent(remoteAddr))
		} else {
			s.logEvent(context.Background(), slog.LevelWarn, localization.BoltTransportSniffFailedEvent(remoteAddr, err))
		}
		if reason == "sniff_timeout" {
			sessionResult = "timeout"
		} else {
			sessionResult = "error"
		}
		return
	}

	// At this point sniffedConn is the (possibly TLS-wrapped) net.Conn and
	// br is the *bufio.Reader holding the peeked bytes. The caller MUST
	// pass br (not a fresh bufio.NewReaderSize) into Session so the peeked
	// bytes flow into the Bolt handshake.
	transportLabel = transportLabelFor(kind, sniffedConn)
	// Close the TLS wrapper on exit (sends close_notify); a *tls.Conn
	// returned from peekTransport otherwise wouldn't shut down cleanly.
	closeTarget = sniffedConn

	// On WS branch, run the HTTP request through the discovery handler
	// first; only then upgrade. wsConn replaces sniffedConn for the rest of
	// the session. For raw branches the conn passes through unchanged.
	var implicitBearer string
	switch kind {
	case transportWebSocket:
		res, err := s.acceptWebSocket(sniffedConn, br, remoteAddr)
		if err != nil {
			// Failed WS upgrade is a session-attempt termination.
			// acceptWebSocket already incremented the rejected counter
			// with the specific reason (ws_handshake / path_404).
			if ms := s.metricsState; ms != nil && ms.bag != nil {
				incBoltConnectionsActive(ms.bag, transportLabel)
				emitMetrics = true
			}
			sessionResult = "error"
			return
		}
		if res == nil {
			// Discovery probe or 426 path already wrote a response and
			// closed the conn. No Bolt session was ever attempted; do
			// NOT pollute connections_total with a fake success.
			return
		}
		sniffedConn = res.conn
		br = res.br
		implicitBearer = res.implicitBearer
		// Now the *wsConn is the outermost wrapper; closing it sends a
		// WS close frame before tearing down the underlying transport.
		closeTarget = sniffedConn
	case transportRaw:
		// nothing to do; sniffedConn + br carry the peeked Bolt magic
	}

	// A real Bolt session is about to start. Activate the lifecycle
	// metric: Inc here under the resolved label, Dec from the deferred
	// closure above. Discovery probes and 426 refusals returned earlier
	// without setting emitMetrics, so they do NOT pollute connections_total.
	if ms := s.metricsState; ms != nil && ms.bag != nil {
		incBoltConnectionsActive(ms.bag, transportLabel)
		emitMetrics = true
	}

	// TRC-13: session-level span wraps the entire connection lifetime.
	sessionCtx, sessionSpan := startSessionSpan(remoteAddr)
	defer func() {
		sessionSpan.SetAttributes(attribute.String("bolt.result", sessionResult))
		sessionSpan.SetAttributes(attribute.String("bolt.transport", transportLabel))
		if sessionResult != "success" {
			sessionSpan.SetStatus(codes.Error, sessionResult)
		}
		sessionSpan.End()
	}()

	// Bufio sizing: WS sessions get a larger write buffer so steady-state
	// RECORD batches don't fragment into 4KB frames.
	writeBufSize := s.config.WriteBufferSize
	if kind == transportWebSocket && s.config.WebSocketWriteBufferSize > 0 {
		writeBufSize = s.config.WebSocketWriteBufferSize
	}

	connCtx, connCancel := context.WithCancel(sessionCtx)
	// Cancel the connection-scoped context the moment handleConnection
	// returns. This is the load-bearing signal that propagates client
	// disconnects to in-flight RUN handlers — see Session.connCtx for
	// the full bug context.
	defer connCancel()

	session := &Session{
		conn:           sniffedConn,
		reader:         br, // load-bearing: holds peeked bytes from peekTransport
		writer:         bufio.NewWriterSize(sniffedConn, writeBufSize),
		server:         s,
		baseExec:       s.executor,
		executor:       s.executor,
		messageBuf:     make([]byte, 0, 4096),
		messageQueue:   make(chan *boltMessage, 16),
		readErrQueue:   make(chan error, 1),
		spanCtx:        sessionCtx,
		connCtx:        connCtx,
		connCancel:     connCancel,
		implicitBearer: implicitBearer,
	}
	_, sourceTransactional := s.executor.(TransactionalExecutor)
	if factory, ok := s.executor.(SessionExecutorFactory); ok {
		session.executor = factory.NewSessionExecutor()
		session.baseExec = session.executor
		if sourceTransactional {
			_, sessionTransactional := session.executor.(TransactionalExecutor)
			session.explicitTransactionsUnsupported = !sessionTransactional
		}
	} else if s.dbManager == nil && sourceTransactional {
		if s.config.MaxConnections == 1 {
			session.rawTransactionExecutor = true
		} else {
			session.explicitTransactionsUnsupported = true
		}
	}

	// Track the executor whose deferred mode was enabled independently from the
	// mutable transaction executor selected later. Teardown must always restore
	// that original mode, while a transaction whose rollback failed must never
	// be flushed as if its writes were safe to persist.
	deferrableOwner, deferrable := session.executor.(DeferrableExecutor)
	if deferrable {
		deferrableOwner.SetDeferFlush(true)
	}
	defer func() {
		session.releaseDatabaseConnection()
		_ = session.rollbackExplicitTransaction(transactionTerminalDisconnect)
		if !session.transactionCleanupFailed && session.pendingFlush {
			if flushable, ok := session.executor.(FlushableExecutor); ok {
				_ = flushable.Flush()
			}
			session.pendingFlush = false
		}
		if deferrable {
			deferrableOwner.SetDeferFlush(false)
		}
	}()

	// Pre-HELLO budget: the connection has BoltAuthTimeout to complete
	// the Bolt handshake + HELLO + auth. handshake() itself reads from
	// the bufio.Reader so the deadline applies to the underlying conn.
	authTimeout := s.config.BoltAuthTimeout
	if authTimeout > 0 {
		_ = sniffedConn.SetReadDeadline(time.Now().Add(authTimeout))
	}

	if err := session.handshake(); err != nil {
		s.logEvent(context.Background(), slog.LevelWarn, localization.BoltHandshakeFailedEvent(remoteAddr, err))
		if ms := s.metricsState; ms != nil && ms.bag != nil {
			reason := "unrecognized_prefix"
			if isDeadlineErr(err) {
				reason = "auth_timeout"
				sessionResult = "timeout"
			}
			incBoltConnectionsRejected(ms.bag, reason)
		}
		if sessionResult == "success" {
			sessionResult = "error"
		}
		return
	}

	// Clear the auth deadline; subsequent Bolt messages run with no read
	// deadline. WebSocket-level keepalive (ping/pong) handles idle WS
	// sessions independently — but the keepalive is installed only AFTER
	// HELLO succeeds, not before, so the pong handler can't override
	// BoltAuthTimeout during the pre-HELLO window.
	if authTimeout > 0 {
		_ = sniffedConn.SetReadDeadline(time.Time{})
	}
	session.startMessageReader()

	keepaliveStarted := false
	for {
		if s.closed.Load() {
			return
		}
		if err := session.handleQueuedMessage(); err != nil {
			if err == io.EOF {
				return
			}
			errStr := err.Error()
			if strings.Contains(errStr, "connection reset") ||
				strings.Contains(errStr, "broken pipe") ||
				strings.Contains(errStr, "use of closed network connection") {
				return
			}
			s.logEvent(context.Background(), slog.LevelWarn, localization.BoltMessageHandlingErrorEvent(remoteAddr, err))
			sessionResult = "error"
			return
		}
		// First successful message after HELLO → install WS keepalive
		// (no-op for raw TCP). Idempotent guard prevents re-installing.
		if !keepaliveStarted && session.authenticated {
			if wc, ok := sniffedConn.(*wsConn); ok {
				s.startWebSocketKeepalive(wc)
			}
			keepaliveStarted = true
		}
	}
}

// isDeadlineErr reports whether err is the i/o-deadline-exceeded variant
// returned by net.Conn.Read after SetReadDeadline fires. Used at the
// pre-HELLO handshake boundary to distinguish auth_timeout rejections
// from generic protocol errors.
func isDeadlineErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
}

// classifySniffError maps an error returned by peekTransport (or the
// surrounding TLS handshake) to a closed-enum reason label for
// connections_rejected_total. Unknown errors fall through to
// "unrecognized_prefix" so the cardinality budget stays bounded.
func classifySniffError(err error) string {
	if err == nil {
		return "unrecognized_prefix"
	}
	if errors.Is(err, ErrUnencryptedRequired) {
		return "requires_tls"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "transport sniff timeout"):
		return "sniff_timeout"
	case strings.Contains(msg, "tls handshake"):
		return "tls_handshake"
	}
	return "unrecognized_prefix"
}

// transportLabelFor maps the transport-sniff outcome to the closed-enum
// metric label per the Bolt observability schema. The four label values
// are tcp, tcp_tls, ws, ws_tls.
func transportLabelFor(kind transportKind, conn net.Conn) string {
	_, encrypted := unwrapTLS(conn)
	switch kind {
	case transportWebSocket:
		if encrypted {
			return "ws_tls"
		}
		return "ws"
	default:
		if encrypted {
			return "tcp_tls"
		}
		return "tcp"
	}
}

// unwrapTLS reports whether the conn is (or wraps) a *tls.Conn, returning
// the unwrapped underlying net.Conn for cases where callers need it. For
// wsConn, this checks the gorilla/websocket UnderlyingConn.
func unwrapTLS(conn net.Conn) (net.Conn, bool) {
	switch c := conn.(type) {
	case *tls.Conn:
		return c.NetConn(), true
	case *wsConn:
		// wsConn carries no direct TLS reference; encryption status is
		// captured at upgrade time and stored on the adapter when needed.
		// We rely on the localAddr being a *net.TCPAddr regardless and
		// on the upgrade pipeline to mark the session label.
		return conn, c.encrypted
	}
	return conn, false
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

	// TRC-13: session span context for parenting per-message spans.
	spanCtx context.Context

	// connCtx is a cancellable context derived from spanCtx whose lifetime
	// matches the connection handler. handleConnection installs a
	// `defer connCancel()` so the context cancels the moment the read
	// loop exits — for any reason: client disconnect, RESET, EOF,
	// transport error, panic-recover, or graceful shutdown. Per-RUN
	// contexts derive from this so a long-running Cypher query
	// terminates promptly when the connection drops, instead of pinning
	// a CPU core indefinitely (the v1.1.2 #184 bug).
	connCtx    context.Context
	connCancel context.CancelFunc

	// Database context (from HELLO message)
	database         string // Database name for this session (defaults to default database)
	admittedDatabase string
	language         language.Tag

	// Authentication state
	authenticated bool            // Whether HELLO auth succeeded
	authResult    *BoltAuthResult // Auth result with roles/permissions
	// forwardedAuthHeader carries caller identity for downstream remote constituent
	// routing (e.g. OIDC credential forwarding over Fabric/USE paths).
	forwardedAuthHeader string

	// Transaction state
	inTransaction                   bool
	txMetadata                      map[string]any // Transaction metadata from BEGIN
	txDatabase                      string
	txHasMerge                      bool
	txHasNonMergeWrite              bool
	txLifecycle                     transactionLifecycle
	failedUntilReset                bool
	transactionCleanupFailed        bool
	explicitTransactionsUnsupported bool
	rawTransactionExecutor          bool

	// Query result state (for streaming with PULL)
	lastResult  *QueryResult
	resultIndex int

	// Deferred commit state (Neo4j-style optimization)
	// Writes are buffered in AsyncEngine until PULL completes
	pendingFlush bool
	flushPending bool

	// Query metadata for Neo4j driver compatibility
	queryId            int64          // Query ID counter for qid field
	lastQueryIsWrite   bool           // Was last query a write operation
	lastQueryDatabase  string         // Effective database for last RUN
	lastRunMetadata    map[string]any // Metadata from last RUN message (bookmarks, tx_timeout, etc.)
	utcPatchNegotiated bool           // HELLO patch_bolt included "utc" for this connection

	// Transaction sequence for this session's last committed transaction
	lastTxSequence int64 // Sequence number of last committed transaction in this session

	// Reusable buffers to reduce allocations
	headerBuf  [2]byte // For reading chunk headers
	messageBuf []byte  // Reusable message buffer
	recordBuf  []byte  // Reusable buffer for PackStream message encoding

	// Async message processing (Neo4j-style batching)
	messageQueue chan *boltMessage // Incoming messages queue
	readErrQueue chan error        // Terminal read errors from the reader loop
	writeMu      sync.Mutex        // Protects writer for concurrent access
	activeRunMu  sync.Mutex
	activeRun    context.CancelFunc
	activeReason byte

	// implicitBearer carries a JWT pulled from the WS-upgrade cookie
	// (nornicdb_token / token). It is set by acceptWebSocket BEFORE the
	// session loop starts; handleHello consumes it on a scheme=none HELLO
	// to promote the session as if the driver had sent scheme=bearer.
	// Empty for raw TCP and TLS sessions — they have no HTTP layer.
	implicitBearer string
}

func (s *Session) useUTCDateTimeStructs() bool {
	if s.version >= 0x0500 {
		return true
	}
	return s.utcPatchNegotiated
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

func (s *Session) startMessageReader() {
	if s.reader == nil || s.messageQueue == nil || s.readErrQueue == nil {
		return
	}
	var connDone <-chan struct{}
	if s.connCtx != nil {
		connDone = s.connCtx.Done()
	}

	go func() {
		for {
			msg, err := s.readMessage()
			if err != nil {
				if s.connCancel != nil {
					s.connCancel()
				}
				s.cancelActiveRun(0)
				select {
				case s.readErrQueue <- err:
				default:
				}
				return
			}
			if msg == nil {
				continue
			}
			if msg.msgType == MsgReset || msg.msgType == MsgGoodbye {
				s.cancelActiveRun(msg.msgType)
			}
			select {
			case s.messageQueue <- msg:
			case <-connDone:
				return
			}
		}
	}()
}

func (s *Session) handleQueuedMessage() error {
	if s.messageQueue == nil || s.readErrQueue == nil {
		return s.handleMessage()
	}

	select {
	case msg := <-s.messageQueue:
		if msg == nil {
			return io.EOF
		}
		return s.processMessage(msg)
	case err := <-s.readErrQueue:
		return err
	}
}

func (s *Session) setActiveRun(cancel context.CancelFunc) {
	s.activeRunMu.Lock()
	defer s.activeRunMu.Unlock()
	s.activeRun = cancel
	s.activeReason = 0
}

func (s *Session) clearActiveRun() {
	s.activeRunMu.Lock()
	defer s.activeRunMu.Unlock()
	s.activeRun = nil
	s.activeReason = 0
}

func (s *Session) cancelActiveRun(reason byte) {
	s.activeRunMu.Lock()
	cancel := s.activeRun
	if cancel != nil && reason != 0 {
		s.activeReason = reason
	}
	s.activeRunMu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (s *Session) activeRunReason() byte {
	s.activeRunMu.Lock()
	defer s.activeRunMu.Unlock()
	return s.activeReason
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

	return s.dispatchInner(msgType, data, op)
}

// dispatchInner is the unobserved body of dispatchMessage; kept as a
// sibling so the metrics wrapper above stays a thin chokepoint.
func (s *Session) dispatchInner(msgType byte, data []byte, op string) error {
	// TRC-13: per-message span, child of session span.
	_, msgSpan := startMessageSpan(s.spanCtx, op)
	defer func() {
		msgSpan.End()
	}()
	if s.rawTransactionExecutor && s.server != nil &&
		s.server.rawTransactionExecutorPoisoned.Load() &&
		(msgType == MsgRun || msgType == MsgBegin) {
		if err := s.sendFailure(
			"Neo.TransientError.General.DatabaseUnavailable",
			"transaction executor is quarantined after an uncertain terminal outcome",
		); err != nil {
			return err
		}
		if err := s.flushIfPending(); err != nil {
			return err
		}
		return errors.New("raw transaction executor is quarantined")
	}
	if s.failedUntilReset && msgType != MsgReset && msgType != MsgGoodbye {
		return s.sendIgnored()
	}
	if s.txLifecycle.isTimedOut() && msgType != MsgReset && msgType != MsgGoodbye {
		return s.sendTransactionTimeoutAfterJoin()
	}

	var err error
	switch msgType {
	case MsgHello:
		err = s.handleHello(data)
	case MsgGoodbye:
		_ = s.rollbackExplicitTransaction(transactionTerminalGoodbye)
		return io.EOF
	case MsgRun:
		err = s.handleRun(data)
	case MsgPull:
		err = s.handlePull(data)
	case MsgDiscard:
		err = s.handleDiscard(data)
	case MsgReset:
		err = s.handleReset(data)
	case MsgBegin:
		err = s.handleBegin(data)
	case MsgCommit:
		err = s.handleCommit(data)
	case MsgRollback:
		err = s.handleRollback(data)
	case MsgRoute:
		err = s.handleRoute(data)
	default:
		err = fmt.Errorf("unknown message type: 0x%02X", msgType)
	}
	recordBoltError(msgSpan, err)
	return err
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
	s.utcPatchNegotiated = false
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
			// Cookie-as-implicit-bearer: if the WS upgrade carried a
			// nornicdb_token cookie that validated, treat scheme=none as
			// scheme=bearer with that token. This lets first-party browser
			// clients (the NornicDB UI) connect via neo4j.auth.none() and
			// have the same-origin cookie carry the credential. HELLO
			// bearer always wins over the cookie because we only enter
			// this branch when the driver explicitly chose scheme=none.
			s.server.logEvent(context.Background(), slog.LevelDebug,
				localization.BoltHelloSchemeNoneEvent(s.implicitBearer != "", s.server.config.AllowAnonymous))
			cookieAuthed := false
			if s.implicitBearer != "" {
				result, err := s.server.config.Authenticator.Authenticate("bearer", "", s.implicitBearer)
				if err == nil {
					s.authenticated = true
					s.authResult = result
					s.forwardedAuthHeader = "Bearer " + s.implicitBearer
					cookieAuthed = true
				} else {
					// Logging at warn keeps the operator-visible signal
					// without leaking the token; we do NOT short-circuit
					// to a failure response — the driver still has the
					// AllowAnonymous fallback below.
					remoteAddr := "unknown"
					if s.conn != nil {
						remoteAddr = s.conn.RemoteAddr().String()
					}
					s.server.logEvent(context.Background(), slog.LevelWarn,
						localization.BoltWebSocketCookieBearerRejectedEvent(remoteAddr, err))
				}
			}
			if !cookieAuthed {
				if !s.server.config.AllowAnonymous {
					return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltAuthenticationRequired())
				}
				s.authenticated = true
				s.authResult = &BoltAuthResult{
					Authenticated: true,
					Username:      "anonymous",
					Roles:         []string{string(auth.RoleViewer)},
				}
				s.forwardedAuthHeader = ""
			}
		} else if scheme == "basic" {
			// Authenticate with provided credentials
			result, err := s.server.config.Authenticator.Authenticate(scheme, principal, credentials)
			if err != nil {
				remoteAddr := "unknown"
				if s.conn != nil {
					remoteAddr = s.conn.RemoteAddr().String()
				}
				s.server.logEvent(context.Background(), slog.LevelWarn,
					localization.BoltBasicAuthenticationFailedEvent(principal, remoteAddr, err))
				return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltInvalidCredentials())
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
				s.server.logEvent(context.Background(), slog.LevelWarn,
					localization.BoltBearerAuthenticationFailedEvent(remoteAddr, err))
				return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltInvalidOrExpiredToken())
			}
			s.authenticated = true
			s.authResult = result
			if strings.TrimSpace(credentials) != "" {
				s.forwardedAuthHeader = "Bearer " + credentials
			}
		} else {
			return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltUnsupportedAuthScheme(scheme))
		}
	} else if s.server != nil && s.server.config.RequireAuth {
		// Auth required but no authenticator configured - reject all
		return s.sendLocalizedFailure("Neo.ClientError.Security.Unauthorized", localization.BoltAuthenticationNotConfigured())
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
			return s.sendLocalizedFailure("Neo.ClientError.Database.DatabaseNotFound", localization.DatabaseNotFound(dbName))
		}
	}
	if err := s.bindDatabaseConnection(dbName); err != nil {
		return s.sendFailure("Neo.TransientError.General.DatabaseUnavailable", err.Error())
	}

	// Store database for this session
	s.database = dbName

	// SEC-03 / TRC-13: enrich session span with auth outcome (never credentials).
	if s.spanCtx != nil {
		if span := trace.SpanFromContext(s.spanCtx); span.IsRecording() {
			span.SetAttributes(
				attribute.String("bolt.user", s.authResult.Username),
				attribute.String("bolt.database", dbName),
			)
		}
	}

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
		s.server.logEvent(context.Background(), slog.LevelInfo,
			localization.BoltHelloEvent(remoteAddr, s.authResult.Username, s.authResult.Roles, dbName))
	}

	return s.sendSuccess(map[string]any{
		"server":        s.server.config.serverAnnouncement(),
		"connection_id": "nornic-1",
		"hints":         map[string]any{},
		"patch_bolt":    []any{"utc"},
	})
}

func (s *Session) bindDatabaseConnection(databaseName string) error {
	if s == nil || s.server == nil || s.admittedDatabase != "" || databaseName == "" || s.server.databaseConnectionAdmit == nil {
		return nil
	}
	if err := s.server.databaseConnectionAdmit(databaseName); err != nil {
		return err
	}
	s.admittedDatabase = databaseName
	return nil
}

func (s *Session) releaseDatabaseConnection() {
	if s == nil || s.server == nil || s.admittedDatabase == "" {
		return
	}
	databaseName := s.admittedDatabase
	s.admittedDatabase = ""
	if s.server.databaseConnectionRelease != nil {
		s.server.databaseConnectionRelease(databaseName)
	}
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
	s.utcPatchNegotiated = helloExtraRequestsUTCPatch(extraMap)

	return result, nil
}

func helloExtraRequestsUTCPatch(extraMap map[string]any) bool {
	raw, ok := extraMap["patch_bolt"]
	if !ok {
		return false
	}
	items, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, item := range items {
		if token, ok := item.(string); ok && strings.EqualFold(strings.TrimSpace(token), "utc") {
			return true
		}
	}
	return false
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
	if s.server.config != nil {
		executor.SetLocalizationRenderer(s.server.config.Localizer)
	}
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
	qr := &QueryResult{
		Columns:  result.Columns,
		Rows:     result.Rows,
		Metadata: result.Metadata,
	}
	if result.Stats != nil {
		qr.Stats = &QueryStats{
			NodesCreated:         result.Stats.NodesCreated,
			NodesDeleted:         result.Stats.NodesDeleted,
			RelationshipsCreated: result.Stats.RelationshipsCreated,
			RelationshipsDeleted: result.Stats.RelationshipsDeleted,
			PropertiesSet:        result.Stats.PropertiesSet,
			LabelsAdded:          result.Stats.LabelsAdded,
		}
	}
	return qr, nil
}
