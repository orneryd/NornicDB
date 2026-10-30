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
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/buildinfo"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/resultstream"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/text/language"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var boltTxWriteAnalyzer = cypher.NewQueryAnalyzer(256)

// Protocol versions supported
const (
	BoltV5_0 = 0x0500 // Bolt 5.0 (element ids in node/relationship structures)
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
	lifecycleMu                    sync.Mutex
	work                           *util.WorkLifetime
	serving                        bool
	stopStarted                    bool
	stopDone                       chan struct{}
	stopErr                        error
	mu                             sync.RWMutex
	sessions                       map[string]*Session
	connectionListings             map[string]cypher.ConnectionListing
	closed                         atomic.Bool
	rawTransactionExecutorPoisoned atomic.Bool
	// nextConnectionID numbers connections for SHOW TRANSACTIONS
	// (connectionId bolt-N).
	nextConnectionID atomic.Uint64

	statementCacheIndex multidb.StatementCacheIndex
	executorsMu         sync.RWMutex
	executors           map[string]QueryExecutor
	// Auth-scoped executors remain fresh; only their text caches live here.
	statementCaches     *cypher.StatementCaches
	statementCachesOnce sync.Once
	memoryPolicy        *memory.Policy

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
	procedureGrantResolver    func(roles []string) func(procedure string) bool
	databaseConnectionResolve func(databaseName string) (string, error)
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
	// connectionLimit counts and logs the connections MaxConnections turns
	// away (logConnectionLimit).
	connectionLimit connectionLimitLog

	// discoveryResponse holds the pre-encoded HTTP/1.1 200 response served
	// to plain GET / probes on the Bolt port. Refreshed by a 1s ticker so
	// the Date header stays current.
	discoveryResponse atomic.Pointer[[]byte]
	// The shared lifetime cancels and joins the one discovery worker.
	discoveryStarted bool

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
	Stats    *QueryStats // Write counters for ResultSummary.
}

type resultStream struct {
	result   *QueryResult
	delivery *resultstream.Delivery
	index    int
	isWrite  bool
	database string
	// lazy produces the rows after result's when the statement is still
	// running (#939); nil once it has finished.
	lazy *lazyResult
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
	LabelsRemoved        int
	IndexesAdded         int
	IndexesRemoved       int
	ConstraintsAdded     int
	ConstraintsRemoved   int
}

// boltStatsMetadata is the "stats" entry of a PULL / DISCARD SUCCESS: every
// counter, or nil when the statement wrote nothing (Neo4j then sends no
// stats). The drivers read it as the summary counters.
func boltStatsMetadata(stats *QueryStats) map[string]any {
	if stats == nil || *stats == (QueryStats{}) {
		return nil
	}
	return map[string]any{
		"nodes-created":         int64(stats.NodesCreated),
		"nodes-deleted":         int64(stats.NodesDeleted),
		"relationships-created": int64(stats.RelationshipsCreated),
		"relationships-deleted": int64(stats.RelationshipsDeleted),
		"properties-set":        int64(stats.PropertiesSet),
		"labels-added":          int64(stats.LabelsAdded),
		"labels-removed":        int64(stats.LabelsRemoved),
		"indexes-added":         int64(stats.IndexesAdded),
		"indexes-removed":       int64(stats.IndexesRemoved),
		"constraints-added":     int64(stats.ConstraintsAdded),
		"constraints-removed":   int64(stats.ConstraintsRemoved),
	}
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
	PrincipalID   string   // Stable identity derived from validated authentication
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

// HasPermission checks if the auth result has a specific permission, through
// the one entitlement check (auth.EntitlementsAllow). When Permissions is set
// (from role entitlements store), uses that list; else the built-in roles'
// entitlements (auth.PermissionsForRoles).
func (r *BoltAuthResult) HasPermission(perm string) bool {
	entitlements := r.Permissions
	if len(entitlements) == 0 {
		entitlements = auth.PermissionsForRoles(r.Roles, nil)
	}
	return auth.EntitlementsAllow(entitlements, perm)
}

// Config holds Bolt protocol server configuration.
//
// All settings have sensible defaults via DefaultConfig(). The configuration
// follows Neo4j Bolt server conventions where applicable.
//
// Authentication:
//   - Set Authenticator to enable auth (nil = no auth, accepts all)
//   - RequireAuth: if true, connections without valid credentials are rejected
//   - AllowAnonymous: legacy adapter knob; no-auth access requires Authenticator=nil
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
	// Lifetime binds resources constructed before New (such as TLS polling)
	// to the same native service admission, cancellation and completion owner.
	// Nil gives New a fresh owner. A supplied owner is immutable for this service.
	Lifetime        *util.WorkLifetime
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
	AllowAnonymous bool              // Legacy adapter knob; no-auth Bolt access requires Authenticator=nil

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
	// ctx.Err() at every traversal/match boundary. An auto-commit read
	// whose rows are produced as PULL asks for them (#939) runs until its
	// last row is pulled or discarded, so the cap covers that time too, as
	// Neo4j's transaction timeout does.
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
		work:      config.Lifetime,
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

// SetProcedureGrantResolver sets how a session's roles resolve to their
// EXECUTE BOOSTED PROCEDURE grants, so a role granted a procedure that needs
// administrator rights may run it (the HTTP server's ProcedureGrantChecker).
func (s *Server) SetProcedureGrantResolver(resolver func(roles []string) func(procedure string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.procedureGrantResolver = resolver
}

// procedureGrants is the session roles' boosted procedure grant check, or nil.
func (s *Server) procedureGrants(roles []string) func(procedure string) bool {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	resolver := s.procedureGrantResolver
	s.mu.RUnlock()
	if resolver == nil {
		return nil
	}
	return resolver(roles)
}

// SetDatabaseConnectionAdmission configures canonical per-database connection admission.
func (s *Server) SetDatabaseConnectionAdmission(resolve func(string) (string, error), admit func(string) error, release func(string)) {
	s.databaseConnectionResolve = resolve
	s.databaseConnectionAdmit = admit
	s.databaseConnectionRelease = release
}

// SetMemoryPolicy uses the database-owned soft admission policy for Bolt RUN.
// Configure it before ListenAndServe so all sessions share one budget.
func (s *Server) SetMemoryPolicy(policy *memory.Policy) {
	s.memoryPolicy = policy
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
func (s *Server) ListenAndServe() error { return s.Run(context.Background()) }

// Run bounds its caller by ctx while retaining actual native startup/serving
// work in the service owner. Cancellation initiates that same retirement.
func (s *Server) Run(ctx context.Context) error {
	err, _ := s.runServing(ctx, func(native context.Context) (net.Listener, error) {
		host := strings.TrimSpace(s.config.Host)
		if host == "" {
			host = "127.0.0.1"
		}
		addr := net.JoinHostPort(host, strconv.Itoa(s.config.Port))
		listener, err := (&net.ListenConfig{}).Listen(native, "tcp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen on %s: %w", addr, err)
		}
		return listener, nil
	})
	return err
}

// Serve accepts Bolt connections from an already-bound listener.
//
// Serve is useful when a caller must reserve an ephemeral port without a
// close-and-rebind race, such as an integration test or an embedded deployment.
// The listener is owned by the server after this call and is closed by Close.
//
// Example:
//
//	listener, err := net.Listen("tcp", "127.0.0.1:0")
//	if err != nil {
//		return err
//	}
//	server := bolt.New(bolt.DefaultConfig(), executor)
//	go func() { _ = server.Serve(listener) }()
func (s *Server) Serve(listener net.Listener) error {
	if listener == nil {
		return fmt.Errorf("bolt listener is nil")
	}
	err, accepted := s.runServing(context.Background(), func(context.Context) (net.Listener, error) { return listener, nil })
	if !accepted {
		return errors.Join(err, listener.Close())
	}
	return err
}

func (s *Server) serveListener(ctx context.Context, listener net.Listener) error {
	s.lifecycleMu.Lock()
	s.listener = listener
	s.lifecycleMu.Unlock()
	closed := make(chan struct{})
	stop := context.AfterFunc(s.lifetime().Context(), func() {
		defer close(closed)
		if err := s.closeListener(); !util.ErrorIsOnly(err, net.ErrClosed) {
			s.lifetime().RecordError("close Bolt listener", err)
		}
	})
	defer func() {
		if stop() {
			if err := s.closeListener(); !util.ErrorIsOnly(err, net.ErrClosed) {
				s.lifetime().RecordError("close Bolt listener", err)
			}
		} else {
			<-closed
		}
	}()

	announceHost := strings.TrimSpace(s.config.Host)
	if announceHost == "0.0.0.0" || announceHost == "::" || announceHost == "" {
		announceHost = "localhost"
	}
	actualPort := s.config.Port
	if tcpAddr, ok := listener.Addr().(*net.TCPAddr); ok && tcpAddr.Port > 0 {
		actualPort = tcpAddr.Port
	}
	s.logEvent(context.Background(), slog.LevelInfo, localization.BoltServerListeningEvent(announceHost, actualPort))

	// Validate the discovery body once at startup and start the 1s Date
	// refresh ticker. Startup fails if OAuthConfig has malformed URLs.
	if err := s.startDiscoveryRefresher(ctx); err != nil {
		return fmt.Errorf("discovery response startup: %w", err)
	}

	return s.serveAccepted(ctx)
}

func (s *Server) listenerSnapshot() net.Listener {
	s.lifecycleMu.Lock()
	defer s.lifecycleMu.Unlock()
	return s.listener
}

// Startup failure and shutdown can overlap. Transfer ownership before closing
// so only one path closes the published listener.
func (s *Server) closeListener() error {
	s.lifecycleMu.Lock()
	listener := s.listener
	s.listener = nil
	s.lifecycleMu.Unlock()
	if listener != nil {
		return listener.Close()
	}
	return nil
}

// serve accepts connections in a loop.
func (s *Server) serve() error { return s.Serve(s.listenerSnapshot()) }

func (s *Server) serveAccepted(ctx context.Context) error {
	listener := s.listenerSnapshot()
	for {
		if s.closed.Load() || ctx.Err() != nil {
			return nil
		}

		conn, err := listener.Accept()
		if err != nil {
			if s.closed.Load() || ctx.Err() != nil {
				if !util.ErrorIsOnly(err, net.ErrClosed) && !util.ErrorIsOnly(err, ctx.Err()) {
					s.lifetime().RecordError("finish Bolt accept", err)
				}
				return nil // Clean shutdown
			}
			continue
		}

		s.startConnection(ctx, conn)
	}
}

// IsClosed returns whether the server is closed.
func (s *Server) IsClosed() bool {
	return s.closed.Load()
}

// handleConnection handles a single client connection. It performs
// transport selection (raw TCP / TLS / WS / WS+TLS) before constructing
// the Bolt Session and then drives the message loop.
func (s *Server) handleConnectionOwned(workCtx context.Context, conn net.Conn) {
	// MaxConnections enforcement (single shared cap across all four
	// transports). Atomic CAS — no channel, no syscall on the hot path.
	if max := s.config.MaxConnections; max > 0 {
		if s.activeConnections.Add(1) > int64(max) {
			s.activeConnections.Add(-1)
			if ms := s.metricsState; ms != nil && ms.bag != nil {
				incBoltConnectionsRejected(ms.bag, "max_connections")
			}
			s.logConnectionLimit(conn, max, time.Now())
			if err := conn.Close(); !util.ErrorIsOnly(err, net.ErrClosed) {
				s.lifetime().RecordError("close excess Bolt connection", err)
			}
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
	joinInterrupt := s.interruptConnection(conn)
	var transportClose util.OnceCompletion
	closeTransport := func() {
		transportClose.Run(func() {
			joinInterrupt()
			if native, ok := conn.(*lifetimeConn); ok {
				native.closing.Store(true)
			}
			if err := closeTarget.Close(); !util.ErrorIsOnly(err, net.ErrClosed) {
				s.lifetime().RecordError("close Bolt connection", err)
			}
		})
	}
	defer func() {
		closeTransport()
	}()

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
	_, sessionSpan := startSessionSpan(remoteAddr)
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

	sessionCtx := trace.ContextWithSpan(workCtx, sessionSpan)
	connCtx, connCancel := context.WithCancel(sessionCtx)
	cancelled := make(chan struct{})
	stopCancellation := context.AfterFunc(s.lifetime().Context(), func() { connCancel(); close(cancelled) })
	// Cancel the connection-scoped context the moment handleConnection
	// returns. This is the load-bearing signal that propagates client
	// disconnects to in-flight RUN handlers — see Session.connCtx for
	// the full bug context.
	defer func() {
		connCancel()
		if !stopCancellation() {
			<-cancelled
		}
	}()

	session := &Session{
		connectionID:   "bolt-" + strconv.FormatUint(s.nextConnectionID.Add(1), 10),
		connectedAt:    time.Now().UTC(),
		conn:           sniffedConn,
		closeTransport: closeTransport,
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
	session.publishConnectionListing()
	session.connCtx = resultstream.WithDelivery(session.connCtx, &session.continuationDelivery)
	defer session.continuationDelivery.Fail()
	defer session.abandonContinuationResults()
	defer session.removeConnectionListing()
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
		s.lifetime().RecordError("rollback disconnected Bolt transaction", session.rollbackExplicitTransaction(transactionTerminalDisconnect))
		if !session.transactionCleanupFailed && session.pendingFlush {
			if flushable, ok := session.executor.(FlushableExecutor); ok {
				s.lifetime().RecordError("flush disconnected Bolt session", flushable.Flush())
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
	defer session.releaseIncomingReservations()

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
				s.startWebSocketKeepalive(connCtx, wc)
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
	closeTransport func() // same physical-close owner for timeout and teardown
	// connectionID identifies the connection in SHOW TRANSACTIONS.
	connectionID string
	connectedAt  time.Time
	userAgent    string
	conn         net.Conn
	reader       *bufio.Reader // Buffered reader for reduced syscalls
	writer       *bufio.Writer // Buffered writer for reduced syscalls
	server       *Server
	baseExec     QueryExecutor
	executor     QueryExecutor
	version      uint32

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
	connCtx              context.Context
	connCancel           context.CancelFunc
	continuationDelivery resultstream.Delivery
	lastDelivery         *resultstream.Delivery

	// Database context (from HELLO message)
	database         string // Database name for this session (defaults to default database)
	admittedDatabase string
	language         language.Tag

	// Authentication state
	authenticated bool            // Whether HELLO auth succeeded
	authResult    *BoltAuthResult // Auth result with roles/permissions
	// identity is the cypher request identity built for authResult
	// (identityAuth); requestIdentity rebuilds it after LOGON / LOGOFF.
	identity     *cypher.RequestIdentity
	identityAuth *BoltAuthResult
	// forwardedAuthHeader carries caller identity for downstream remote constituent
	// routing (e.g. OIDC credential forwarding over Fabric/USE paths).
	forwardedAuthHeader string

	// Transaction state
	inTransaction                   bool
	txMetadata                      map[string]any // Transaction metadata from BEGIN
	txDatabase                      string
	txHasMerge                      bool
	txHasNonMergeWrite              bool
	txMergeStatements               []cypher.CommitStatement
	txWriteInfos                    []*cypher.QueryInfo // write statements; MERGE-only shape decided when a commit fails
	txLifecycle                     transactionLifecycle
	failedUntilReset                bool
	transactionCleanupFailed        bool
	explicitTransactionsUnsupported bool
	rawTransactionExecutor          bool

	// Query result state (for streaming with PULL)
	lastResult            *QueryResult
	lastResultMemoryLease *memory.Lease
	resultMemoryLeases    map[int64]*memory.Lease
	resultIndex           int
	lastLazy              *lazyResult
	// pendingMessage and pendingErr are what a lazy result's helper read
	// and handed back to the message loop (serveLazyResult).
	pendingMessage    *boltMessage
	pendingErr        error
	resultStreams     map[int64]*resultStream
	latestStatementID int64

	// Deferred commit state (Neo4j-style optimization)
	// Writes are buffered in AsyncEngine until PULL completes
	pendingFlush bool
	flushPending bool

	// Query metadata for Neo4j driver compatibility
	queryId            int64          // Next explicit-transaction qid
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
	messageQueue      chan *boltMessage // Incoming messages queue
	messageReaderDone chan struct{}     // reader joined before queued reservations are released
	readErrQueue      chan error        // Terminal read errors from the reader loop
	writeMu           sync.Mutex        // Protects writer for concurrent access
	activeRunMu       sync.Mutex
	activeRun         context.CancelFunc
	activeReason      byte

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

// boltV5 reports whether the session negotiated Bolt 5.0 or newer: node and
// relationship structures then carry their element id (#745).
func (s *Session) boltV5() bool {
	return s.version >= 0x0500
}

// selectBoltVersion picks the highest mutually supported protocol version
// from a client's four handshake proposals. Each proposal is the wire
// [0x00, back, minor, major] encoding and names the range [minor-back,
// minor] of one major version (a Python-driver offer of 5.8 with back=8
// covers 5.0..5.8); the first proposal may be the Bolt manifest marker
// (0x000001FF), which this server does not speak — clients that offer it
// also offer concrete versions. It returns the selected major.minor
// version, the reply bytes [0x00, 0x00, minor, major] (a concrete version,
// never a range), and whether any proposal was mutually supported.
func selectBoltVersion(offers []uint32) (version uint32, reply uint32, ok bool) {
	selected := uint32(0)
	selectedWire := uint32(0)
	for _, offered := range offers {
		if offered&0xFF000000 != 0 {
			continue // manifest marker or reserved range
		}
		major := int(offered & 0xFF)
		minor := int((offered >> 8) & 0xFF)
		back := int((offered >> 16) & 0xFF)
		lowest := minor - back
		if lowest < 0 {
			lowest = 0
		}
		// A proposal covers [minor-back, minor]. Pick the highest supported
		// minor in the range; lower candidates can't beat it.
		for candidate := minor; candidate >= lowest; candidate-- {
			mm := uint32(major<<8 | candidate)
			if mm == BoltV5_0 || mm == BoltV4_4 || mm == BoltV4_3 || mm == BoltV4_2 || mm == BoltV4_1 || mm == BoltV4_0 {
				if mm > selected {
					selected = mm
					selectedWire = uint32(candidate<<8 | major)
				}
				break
			}
		}
	}
	return selected, selectedWire, selected != 0
}

// boltMessage represents a parsed Bolt message ready for processing
type boltMessage struct {
	msgType     byte
	data        []byte
	memoryLease *memory.WireRequest
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

	// Select the highest version the client offers that we support, so a
	// Neo4j 5 driver negotiates Bolt 5.0 and receives element ids in the
	// node/relationship structures (#745). Older clients keep 4.x. The reply
	// is the concrete selected version (wire format [0x00, 0x00, minor,
	// major]); the session records the major.minor version.
	offers := []uint32{
		binary.BigEndian.Uint32(versions[0:4]),
		binary.BigEndian.Uint32(versions[4:8]),
		binary.BigEndian.Uint32(versions[8:12]),
		binary.BigEndian.Uint32(versions[12:16]),
	}
	selected, reply, ok := selectBoltVersion(offers)
	if !ok {
		// Bolt handshake spec: a rejection is four zero bytes on the wire
		// before closing, so clients see the protocol-level no-version
		// result instead of an EOF.
		_, _ = s.writer.Write([]byte{0, 0, 0, 0})
		_ = s.writer.Flush()
		return fmt.Errorf("no mutually supported Bolt protocol version")
	}
	s.version = selected

	// Send selected version using buffered writer
	response := []byte{byte(reply >> 24), byte(reply >> 16), byte(reply >> 8), byte(reply)}
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
	msg, _, err := s.readWireMessage(nil)
	return msg, err
}

// processMessage processes a parsed Bolt message.
func (s *Session) processMessage(msg *boltMessage) error {
	// No mutex needed - messages are processed sequentially from the queue
	return msg.memoryLease.Run(func() error { return s.dispatchMessage(msg.msgType, msg.data, msg.memoryLease) })
}

func (s *Session) handleQueuedMessage() error {
	if s.messageQueue == nil || s.readErrQueue == nil {
		return s.handleMessage()
	}
	msg, err := s.nextQueuedMessage()
	if err != nil {
		var admission *wireAdmissionError
		if errors.As(err, &admission) {
			if sendErr := s.rejectWireAdmission(admission); sendErr != nil {
				return sendErr
			}
		}
		return err
	}
	return s.processMessage(msg)
}

// nextQueuedMessage is the next message to process: one a lazy result's
// helper handed back (pendingMessage) or the next from the reader; or the
// error that ends the connection (pendingErr, a read error, or io.EOF).
func (s *Session) nextQueuedMessage() (*boltMessage, error) {
	if err := s.pendingErr; err != nil {
		s.pendingErr = nil
		return nil, err
	}
	if msg := s.pendingMessage; msg != nil {
		s.pendingMessage = nil
		return msg, nil
	}
	select {
	case msg := <-s.messageQueue:
		if msg == nil {
			return nil, io.EOF
		}
		return msg, nil
	case err := <-s.readErrQueue:
		return nil, err
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
	msg, buffer, err := s.readWireMessage(s.messageBuf)
	s.messageBuf = buffer
	if err != nil {
		var admission *wireAdmissionError
		if errors.As(err, &admission) {
			if sendErr := s.rejectWireAdmission(admission); sendErr != nil {
				return sendErr
			}
		}
		return err
	}
	if msg == nil {
		return nil
	}
	return s.processMessage(msg)
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
func (s *Session) dispatchMessage(msgType byte, data []byte, reservations ...*memory.WireRequest) (retErr error) {
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

	var lease *memory.WireRequest
	if len(reservations) > 0 {
		lease = reservations[0]
	}
	if lease == nil && messageDecodesPayload(msgType) && s.server != nil {
		lease = memory.NewWireRequest(s.server.memoryPolicy)
	}
	return lease.Run(func() error { return s.dispatchInner(msgType, data, op, lease) })
}

// dispatchInner is the unobserved body of dispatchMessage; kept as a
// sibling so the metrics wrapper above stays a thin chokepoint.
func (s *Session) dispatchInner(msgType byte, data []byte, op string, lease *memory.WireRequest) error {
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
	// Framed messages already carry their accumulation reservation. Synthetic
	// dispatchers still pass through the same pre-decode admission owner.
	if lease != nil && messageDecodesPayload(msgType) {
		if err := lease.Grow(int64(len(data))); err != nil {
			return s.sendApplicationAdmissionFailure(err)
		}
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

		if s.explicitTransactionsUnsupported {
			err = s.sendTransactionControlFailure(
				"Neo.ClientError.Transaction.TransactionStartFailed",
				"explicit transactions require a SessionExecutorFactory that returns a distinct executor per connection",
			)
			break
		}
		// Reject a protocol-invalid nested BEGIN before decoding metadata or
		// resolving another database executor. The active transaction retains its
		// original executor and remains the sole owner of its storage transaction.
		if s.inTransaction {
			err = s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionStartFailed",
				"an explicit transaction is already active")
			break
		}
		s.refreshAuthResult()

		// Parse BEGIN metadata (contains tx_timeout, bookmarks, etc.)
		var metadata map[string]any
		if len(data) > 0 {
			m, _, beginErr := decodePackStreamMap(data, 0, s.useUTCDateTimeStructs())
			if beginErr != nil {
				if errors.Is(beginErr, errDateTimeSignature) {
					err = beginErr
					break
				}
				err = s.sendTransactionControlFailure("Neo.ClientError.Request.Invalid",
					fmt.Sprintf("Failed to parse BEGIN metadata: %v", beginErr))
				break
			}
			metadata = m
		}
		txTimeout, beginErr := validateTransactionTimeout(metadata)
		if beginErr != nil {
			err = s.sendTransactionControlFailure("Neo.ClientError.Request.Invalid", beginErr.Error())
			break
		}

		// Pin every explicit transaction provider to the BEGIN database, not only
		// database-manager adapters. Factory and single-connection raw executors are
		// session-scoped rather than database-resolving, so a later RUN naming a
		// different database must fail before it reaches that executor.
		txDatabase := s.database
		if metadataDB, ok := databaseFromMetadata(metadata); ok {
			txDatabase = metadataDB
		}
		if txDatabase == "" {
			txDatabase = "nornic"
		}
		if s.server != nil && s.server.dbManager != nil {
			dbName := txDatabase
			if _, explicitlyNamed := databaseFromMetadata(metadata); !explicitlyNamed && s.database == "" {
				dbName = s.server.dbManager.DefaultDatabaseName()
			}
			txExec, beginErr := s.getExecutorForDatabase(dbName, true)
			if beginErr != nil {
				err = s.sendTransactionControlFailure("Neo.ClientError.Database.DatabaseNotFound",
					s.localize(localization.BoltDatabaseNotFoundWithCause(dbName, beginErr)))
				break
			}
			s.executor = txExec
			txDatabase = dbName
		}
		if beginErr := s.bindDatabaseConnection(txDatabase); beginErr != nil {
			err = s.sendTransactionControlFailure("Neo.TransientError.General.DatabaseUnavailable", beginErr.Error())
			break
		}

		// TRC-14: extract traceparent from BEGIN metadata for distributed tracing.
		txParent := s.connCtx
		if txParent == nil {
			txParent = s.spanCtx
			if txParent == nil {
				txParent = context.Background()
			}
		}
		txParent = extractTraceparent(txParent, metadata)
		// BEGIN's tx_metadata is the transaction's metadata.
		txParent = cypher.WithTransactionMetadata(txParent, boltTxMetadata(metadata))
		// The transaction is the session's: SHOW TRANSACTIONS lists it with the
		// session's connection and user from BEGIN on.
		txParent = s.withSessionIdentity(txParent)

		txExec, _ := s.executor.(TransactionalExecutor)
		s.txLifecycle.setTimeoutCleanupFailureHandler(s.failClosedTransactionCleanup)
		if beginErr := s.txLifecycle.begin(
			txParent,
			txTimeout,
			txDatabase,
			txExec,
			metadata,
			s.observeTransactionTerminal,
		); beginErr != nil {
			if s.baseExec != nil {
				s.executor = s.baseExec
			}
			if s.txLifecycle.isDefunct() {
				s.markTransactionCleanupFailed()
				if sendErr := s.sendTransactionControlFailure(
					"Neo.ClientError.Transaction.TransactionStartFailed", beginErr.Error()); sendErr != nil {
					err = sendErr
					break
				}
				if flushErr := s.flushIfPending(); flushErr != nil {
					err = flushErr
					break
				}
				err = fmt.Errorf("BEGIN transaction cleanup failed: %w", beginErr)
				break
			}
			err = s.sendTransactionControlFailure("Neo.ClientError.Transaction.TransactionStartFailed", beginErr.Error())
			break
		}

		s.inTransaction = true
		s.txMetadata = metadata
		s.txDatabase = txDatabase
		s.txHasMerge = false
		s.txHasNonMergeWrite = false
		s.txMergeStatements = nil
		s.txWriteInfos = nil
		s.queryId = 0
		s.latestStatementID = -1
		s.resultStreams = make(map[int64]*resultStream)
		if beginErr := s.sendSuccessNoFlush(nil); beginErr != nil {
			err = beginErr
			break
		}
		err = s.flushIfPending()
		break

	case MsgCommit:
		err = s.handleCommit(data)
	case MsgRollback:
		err = s.handleRollback(data)
	case MsgRoute:
		err = s.handleRoute(data)
	default:
		err = fmt.Errorf("unknown message type: 0x%02X", msgType)
	}
	// Neo4j reports a disallowed temporal signature as a decode failure
	// before applying the message's state transition. Preserve the current
	// transaction/stream and permit a subsequent valid message without RESET.
	if errors.Is(err, errDateTimeSignature) {
		return s.sendFailure("Neo.ClientError.Request.Invalid", err.Error())
	}
	recordBoltError(msgSpan, err)
	return err
}

// getExecutorForDatabase returns a Cypher executor for the specified database.
// This is used when DatabaseManager is configured to route queries to the correct database.
func (s *Session) getExecutorForDatabase(dbName string, transaction ...bool) (QueryExecutor, error) {
	if s.server == nil || s.server.dbManager == nil {
		return nil, fmt.Errorf("database manager not available")
	}

	s.server.bindStatementCacheIndex()

	useAuthScopedResolver := false
	if resolver, ok := s.server.dbManager.(authAwareStorageResolver); ok && strings.TrimSpace(s.forwardedAuthHeader) != "" {
		_ = resolver
		useAuthScopedResolver = true
	}

	explicit := len(transaction) > 0 && transaction[0]
	if !useAuthScopedResolver && !explicit {
		return cypher.CachedDatabaseExecutor(&s.server.executorsMu, &s.server.executors, dbName,
			func(executor QueryExecutor) bool { return !boltExecutorCachesDisposed(executor) },
			func() (QueryExecutor, error) {
				executor, err := cypher.NewDatabaseScopedExecutor(s.databaseScopedExecutorOptions(dbName, false))
				if err != nil {
					return nil, err
				}
				// Auto-commit shares this view; explicit transactions build fresh ones.
				executor.SetSharedExecutor(true)
				return &boltQueryExecutorAdapter{executor: executor}, nil
			})
	}

	// Auth-scoped resolvers cannot be cached under a database-only key.
	executor, err := cypher.NewDatabaseScopedExecutor(s.databaseScopedExecutorOptions(dbName, useAuthScopedResolver))
	if err != nil {
		return nil, err
	}

	if explicit {
		base, err := s.getExecutorForDatabase(dbName)
		if err != nil {
			return nil, err
		}
		if adapter, ok := base.(*boltQueryExecutorAdapter); ok {
			executor.ShareQueryStatisticsFrom(adapter.executor)
		}
		return &transactionalBoltQueryExecutorAdapter{
			boltQueryExecutorAdapter: boltQueryExecutorAdapter{executor: executor},
		}, nil
	}

	// The cached executor is shared by every auto-commit RUN of this
	// database: a statement must never leave it inside a transaction.
	// Explicit transactions use getExecutorForDatabase with explicit ownership, which
	// builds a fresh per-connection executor.
	executor.SetSharedExecutor(true)

	return &boltQueryExecutorAdapter{executor: executor}, nil
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

func (s *Session) databaseScopedExecutorOptions(dbName string, useAuthScopedResolver bool) cypher.DatabaseScopedExecutorOptions {
	options := cypher.DatabaseScopedExecutorOptions{
		Name: dbName, Storage: s.server.dbManager,
		AuthToken: s.forwardedAuthHeader, UseAuthenticatedStorage: useAuthScopedResolver,
	}
	if manager, ok := s.server.dbManager.(*multidb.DatabaseManager); ok {
		options.StatementCacheManager = manager
	} else {
		// Injected managers without catalog lifecycle use one bounded server owner.
		options.FallbackStatementCaches = s.server.fallbackStatementCaches()
	}
	if s.server.config != nil {
		options.LocalizationRenderer = s.server.config.Localizer
	}
	if baseAdapter, ok := s.server.executor.(*boltQueryExecutorAdapter); ok && baseAdapter != nil && baseAdapter.executor != nil {
		options.BaseExecutor = baseAdapter.executor
	}
	options.Configure = func(executor *cypher.StorageExecutor, storageEngine storage.Engine) {
		if cfg, ok := s.server.executor.(databaseExecutorConfigurator); ok && cfg != nil {
			cfg.ConfigureDatabaseExecutor(executor, dbName, storageEngine)
		}
		if provider, ok := s.server.executor.(baseCypherExecutorProvider); ok && provider != nil {
			if baseExec := provider.BaseCypherExecutor(); baseExec != nil {
				executor.ShareEmbedderFrom(baseExec)
				executor.ShareInferenceManagerFrom(baseExec)
			}
		}
		if mgr, ok := s.server.dbManager.(*multidb.DatabaseManager); ok {
			executor.SetDatabaseManager(&boltDatabaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(mgr)})
		}
	}
	return options
}

// boltQueryExecutorAdapter adapts cypher.StorageExecutor to bolt.QueryExecutor interface.
type boltQueryExecutorAdapter struct {
	executor *cypher.StorageExecutor
}

func (a *boltQueryExecutorAdapter) Execute(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
	// Convert params from map[string]any to map[string]interface{} only when
	// there are any: the conversion is a per-request allocation on the hot
	// autocommit path, and most RUNs carry no parameters (PR #1043 server-path
	// profile).
	var paramsMap map[string]interface{}
	if len(params) > 0 {
		paramsMap = make(map[string]interface{}, len(params))
		for k, v := range params {
			paramsMap[k] = v
		}
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
			LabelsRemoved:        result.Stats.LabelsRemoved,
			IndexesAdded:         result.Stats.IndexesAdded,
			IndexesRemoved:       result.Stats.IndexesRemoved,
			ConstraintsAdded:     result.Stats.ConstraintsAdded,
			ConstraintsRemoved:   result.Stats.ConstraintsRemoved,
		}
	}
	return qr, nil
}

func (s *Server) fallbackStatementCaches() *cypher.StatementCaches {
	s.statementCachesOnce.Do(func() { s.statementCaches = cypher.NewStatementCaches() })
	return s.statementCaches
}

func boltExecutorCachesDisposed(executor QueryExecutor) bool {
	adapter, ok := executor.(*boltQueryExecutorAdapter)
	return ok && !adapter.executor.StatementCacheSelectionValid()
}

// One registration covers every cached execution view owned by this server.
func (s *Server) bindStatementCacheIndex() {
	manager, ok := s.dbManager.(*multidb.DatabaseManager)
	if !ok {
		return
	}
	s.statementCacheIndex.Bind(manager, func() {
		s.executorsMu.Lock()
		defer s.executorsMu.Unlock()
		for name, executor := range s.executors {
			if boltExecutorCachesDisposed(executor) {
				delete(s.executors, name)
			}
		}
	})
}
