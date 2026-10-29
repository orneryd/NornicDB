package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/httpresponse"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/internal/localizationboundary"
	"io"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	math "github.com/orneryd/nornicdb/pkg/math/libm"

	"github.com/orneryd/nornicdb/internal/catalogadapter"
	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/txphase"
	"github.com/orneryd/nornicdb/pkg/txsession"
)

// =============================================================================
// Neo4j-Compatible Database Endpoint Handler
// =============================================================================

// handleDatabaseEndpoint routes /db/{databaseName}/... requests
// Implements Neo4j HTTP API transaction model:
//
//	POST /db/{dbName}/tx/commit - implicit transaction (query and commit)
//	POST /db/{dbName}/tx - open explicit transaction
//	POST /db/{dbName}/tx/{txId} - execute in open transaction
//	POST /db/{dbName}/tx/{txId}/commit - commit transaction
//	DELETE /db/{dbName}/tx/{txId} - rollback transaction
func (s *Server) handleDatabaseEndpoint(w http.ResponseWriter, r *http.Request) {
	// Parse path: /db/{databaseName}/...
	path := strings.TrimPrefix(r.URL.Path, "/db/")
	parts := strings.Split(path, "/")

	if len(parts) < 1 || parts[0] == "" {
		s.writeLocalizedNeo4jError(w, r, http.StatusBadRequest, "Neo.ClientError.Request.Invalid", localization.DatabaseNameRequired())
		return
	}

	dbName := parts[0]
	remaining := parts[1:]

	// Route based on remaining path
	switch {
	case len(remaining) == 0:
		// /db/{dbName} - database info
		s.handleDatabaseInfo(w, r, dbName)

	case remaining[0] == "tx":
		// Transaction endpoints
		s.handleTransactionEndpoint(w, r, dbName, remaining[1:])

	case remaining[0] == "cluster":
		// /db/{dbName}/cluster - cluster status
		s.handleClusterStatus(w, r, dbName)

	default:
		s.writeLocalizedNeo4jError(w, r, http.StatusNotFound, "Neo.ClientError.Request.Invalid", localization.UnknownEndpoint())
	}
}

// hasPrefixFold reports whether s starts with prefix, ignoring ASCII case.
func hasPrefixFold(s, prefix string) bool {
	return len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix)
}

// withRequestIdentity attaches the signed-in user, user directory and client
// connection to administration statements.
func (s *Server) withRequestIdentity(ctx context.Context, r *http.Request, claims *auth.JWTClaims) context.Context {
	identity := &cypher.RequestIdentity{Connection: cypher.ClientConnection{Protocol: "http"}}
	identity.Connections = s.connectionLister
	if claims != nil && strings.TrimSpace(claims.Username) != "" {
		identity.User = &cypher.AuthenticatedUser{Name: claims.Username, Roles: claims.Roles}
	}
	if authenticator := s.auth; authenticator != nil {
		identity.Users = func() []cypher.UserListing {
			return cypher.UserListingsFromAuth(authenticator.ListUsers())
		}
	}
	if r != nil {
		identity.Connection.ID = "http-" + strconv.FormatUint(s.nextHTTPConnectionID.Add(1), 10)
		identity.Connection.Address = r.RemoteAddr
	}
	return cypher.WithRequestIdentity(ctx, identity)
}

func transactionOwnerKey(_ *http.Request, claims *auth.JWTClaims) string {
	return auth.PrincipalID(claims)
}

func (s *Server) databaseHasRemoteConstituent(dbName string) bool {
	info, err := s.dbManager.GetDatabase(dbName)
	if err != nil || info == nil || info.Type != "composite" {
		return false
	}
	for _, ref := range info.Constituents {
		if ref.Type == "remote" {
			return true
		}
	}
	return false
}

// newExecutorForDatabase creates a fresh executor scoped to a single database.
// Unlike executorCache.Get, this does not cache the executor and is intended
// for per-transaction session state (explicit HTTP transactions).
func (s *Server) newExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	base, err := s.executorCache.Get(dbName)
	if err != nil {
		return nil, err
	}
	executor, err := cypher.NewDatabaseScopedExecutor(s.databaseScopedExecutorOptions(dbName, ""))
	if err != nil {
		return nil, err
	}
	executor.ShareQueryStatisticsFrom(base)
	return executor, nil
}

func (s *Server) databaseScopedExecutorOptions(dbName, authToken string) cypher.DatabaseScopedExecutorOptions {
	overrides := s.dbConfigStore.GetOverrides(dbName)
	resolved := dbconfig.Resolve(s.processConfig, overrides)
	s.applyTransactionMemoryLimits(dbName, resolved)
	base := s.db.GetCypherExecutor()
	return cypher.DatabaseScopedExecutorOptions{
		Name: dbName, Storage: s.dbManager,
		AuthToken: authToken, UseAuthenticatedStorage: authToken != "" && s.databaseHasRemoteConstituent(dbName),
		QueryCachePolicy: true, QueryCacheMaxEntries: resolved.QueryCacheMaxEntries, QueryCacheTTL: resolved.QueryCacheTTL,
		StatementCacheManager: s.dbManager, LocalizationRenderer: s.localizer, BaseExecutor: base,
		Configure: func(executor *cypher.StorageExecutor, storageEngine storage.Engine) {
			executor.SetSettingsResolver(func() cypher.SettingsSnapshot {
				overrides := s.dbConfigStore.GetOverrides(dbName)
				current := dbconfig.Resolve(s.processConfig, overrides)
				active, _ := s.databaseConfigRuntimeState(dbName, current)
				return cypher.SettingsSnapshot{Configured: overrides, Active: active}
			})
			executor.SetDatabaseManager(s.databaseManager())

			// Reuse DB's cached search service instead of creating a new one.
			// Composite roots resolve constituent services without creating a root index.
			executor.SetSearchServiceResolver(dbName, s.db.SearchServiceResolver())
			if !s.dbManager.IsCompositeDatabase(dbName) {
				if _, err := s.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
					executor.SetSearchServiceResolver(dbName, s.db.SearchServiceResolver())
				}
			}

			// Copy query embedder from the base DB executor so string-input vector procedures work.
			if base != nil {
				executor.ShareQueryStatisticsFrom(base)
				// NornicDB issue #563 defect 1: this is the executor behind every
				// HTTP POST /db/{name}/tx/commit for a non-default database — the
				// overwhelming majority of this HTTP traffic. Without inheriting
				// the base executor's logger + threshold here, none of it ever
				// emits a slow_query record.
				executor.SetLogger(base.Logger())
				executor.SetSlowQueryThreshold(base.SlowQueryThreshold())
			}

			// Wire embed queue callback for per-database executor mutations.
			if q := s.db.GetEmbedQueue(); q != nil {
				executor.SetNodeMutatedCallback(func(nodeID string) {
					q.Enqueue(nodeID)
				})
			}
		},
	}
}

// invalidateExecutor removes a cached executor for a dropped database.
func (s *Server) invalidateExecutor(dbName string) {
	s.executorCache.Delete(dbName)
}

// invalidateAllExecutors clears all cached executors to force fresh database manager references.
// This is used when database metadata changes (e.g., dropping databases) to ensure
// all executors see the updated state.
func (s *Server) invalidateAllExecutors() {
	s.executorCache.Clear()
}

// databaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface, avoiding import cycles. The required
// operations come from the shared contract in catalogadapter.Catalog (see
// catalogadapter/catalog_operations.go) and the optional database-option,
// statement-cache-scope and administrative-identity capabilities come from the
// shared catalogadapter.IdentifiedExecutionCatalog view. HTTP keeps only its own
// server lifecycle fields and the two drop lifecycle overrides below.
//
// The required methods use pointer receivers, so only *databaseManagerAdapter,
// not the value type, implements that interface.
type databaseManagerAdapter struct {
	catalogadapter.IdentifiedExecutionCatalog
	db     *nornicdb.DB
	server *Server // Reference to server for cache invalidation
}

// databaseManager is the server's database administration (CREATE, DROP and
// SHOW DATABASE, aliases, composites) for Cypher and the Qdrant API: it keeps
// the state the server derives from a database (search services, cached
// executors) in step with it.
func (s *Server) databaseManager() *databaseManagerAdapter {
	return &databaseManagerAdapter{IdentifiedExecutionCatalog: catalogadapter.ForIdentifiedExecution(s.dbManager), db: s.db, server: s}
}

func (a *databaseManagerAdapter) DropDatabase(ctx context.Context, name string) error {
	return a.dropCatalogDatabase(ctx, name, catalogDropRegular)
}
func (a *databaseManagerAdapter) DropCompositeDatabase(ctx context.Context, name string) error {
	return a.dropCatalogDatabase(ctx, name, catalogDropComposite)
}

// handleDatabaseInfo returns database information for the specified database.
//
// This endpoint provides metadata about a database including its name, status,
// whether it's the default database, and current statistics (node and edge counts).
//
// Endpoint: GET /db/{dbName}
//
// Parameters:
//   - dbName: The name of the database to get information about
//
// Response (200 OK):
//
//	{
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 1234,
//	  "edgeCount": 5678,
//	  "nodeStorageBytes": 123456,
//	  "managedEmbeddingBytes": 4194304
//	}
//
// Errors:
//   - 404 Not Found: Database doesn't exist (Neo.ClientError.Database.DatabaseNotFound)
//   - 500 Internal Server Error: Failed to access database (Neo.ClientError.Database.General)
//
// Example:
//
//	GET /db/tenant_a
//	Response: {
//	  "name": "tenant_a",
//	  "status": "online",
//	  "default": false,
//	  "nodeCount": 100,
//	  "edgeCount": 50,
//	  "nodeStorageBytes": 20480,
//	  "managedEmbeddingBytes": 12288
//	}
//
// Thread Safety:
//   - Safe for concurrent use
//   - Statistics are read from namespaced storage (thread-safe)
//
// Performance:
//   - Node and edge counts are computed on-demand
//   - For large databases, this may take a few milliseconds
//   - Consider caching if this endpoint is called frequently
func (s *Server) handleDatabaseInfo(w http.ResponseWriter, r *http.Request, dbName string) {
	if !s.authorizeDatabaseRead(w, r, dbName) {
		return
	}
	// Check if database exists (also accepts dotted composite.alias references).
	// This is a fast lookup in the DatabaseManager's metadata.
	if !s.dbManager.ExistsOrIsConstituent(dbName) {
		s.writeNeo4jDatabaseNotFound(w, r, dbName)
		return
	}

	// Check if this is the default database
	defaultDB := s.dbManager.DefaultDatabaseName()

	// For composite databases, return constituent info instead of size stats.
	if s.dbManager.IsCompositeDatabase(dbName) {
		constituents, _ := s.dbManager.GetCompositeConstituents(dbName)
		for _, constituent := range constituents {
			if !s.authorizeDatabaseRead(w, r, dbName+"."+constituent.Alias) {
				return
			}
		}
		sort.Slice(constituents, func(i, j int) bool {
			return cyphertext.NameLower(constituents[i].Alias) < cyphertext.NameLower(constituents[j].Alias)
		})
		consList := make([]map[string]interface{}, 0, len(constituents))
		for _, c := range constituents {
			entry := map[string]interface{}{
				"alias":        c.Alias,
				"databaseName": c.DatabaseName,
				"type":         c.Type,
				"accessMode":   c.AccessMode,
			}
			if c.Type == "remote" {
				if view, err := s.dbManager.RemoteConstituentForDisplay(c); err == nil && view.URI != "" {
					entry["uri"] = observability.RedactURL(view.URI)
				}
			}
			consList = append(consList, entry)
		}
		stats, partial := s.compositeConstituentStats(r, dbName)
		var totalNodes int64
		var totalEdges int64
		var totalNodeStorage int64
		var totalEmbeddingBytes int64
		aggReady := len(stats) > 0 && !partial
		aggBuilding := false
		aggInitialized := len(stats) > 0 && !partial
		aggStrategy := "unknown"
		var aggProcessed int64
		var aggTotal int64
		var aggRate float64
		aggETA := int64(-1)
		strategySet := make(map[string]struct{}, len(stats))
		for i, item := range stats {
			totalNodes += item["nodeCount"].(int64)
			totalEdges += item["edgeCount"].(int64)
			totalNodeStorage += item["nodeStorageBytes"].(int64)
			totalEmbeddingBytes += item["managedEmbeddingBytes"].(int64)
			ready := item["searchReady"].(bool)
			building := item["searchBuilding"].(bool)
			initialized := item["searchInitialized"].(bool)
			aggReady = aggReady && ready
			aggBuilding = aggBuilding || building
			aggInitialized = aggInitialized && initialized
			aggProcessed += item["searchProcessed"].(int64)
			aggTotal += item["searchTotal"].(int64)
			aggRate += item["searchRate"].(float64)
			if stg, ok := item["searchStrategy"].(string); ok && stg != "" && stg != "unknown" {
				strategySet[stg] = struct{}{}
			}
			if i == 0 || item["searchEtaSeconds"].(int64) > aggETA {
				aggETA = item["searchEtaSeconds"].(int64)
			}
		}
		switch len(strategySet) {
		case 0:
			aggStrategy = "unknown"
		case 1:
			for k := range strategySet {
				aggStrategy = k
			}
		default:
			aggStrategy = "mixed"
		}
		response := map[string]interface{}{
			"name":                  dbName,
			"status":                "online",
			"default":               dbName == defaultDB,
			"type":                  "composite",
			"constituents":          consList,
			"nodeCount":             totalNodes,
			"edgeCount":             totalEdges,
			"nodeStorageBytes":      totalNodeStorage,
			"managedEmbeddingBytes": totalEmbeddingBytes,
			"searchReady":           aggReady,
			"searchBuilding":        aggBuilding,
			"searchInitialized":     aggInitialized,
			"searchStrategy":        aggStrategy,
			"searchPhase":           "constituent_aggregate",
			"searchProcessed":       aggProcessed,
			"searchTotal":           aggTotal,
			"searchRate":            aggRate,
			"searchEtaSeconds":      aggETA,
			"statsAggregation":      "constituent_sum",
			"statsPartial":          partial,
			"statsProvenance":       stats,
		}
		httpresponse.WriteJSON(w, http.StatusOK, response)
		return
	}

	// Standard database: return size and search stats.
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.Database.General",
			fmt.Sprintf("Failed to access database: %v", err))
		return
	}

	nodeCount, err := storage.NodeCount()
	if err != nil {
		nodeCount = 0
	}
	edgeCount, err := storage.EdgeCount()
	if err != nil {
		edgeCount = 0
	}
	_, nodeStorageBytes, _ := s.dbManager.GetStorageSize(dbName)
	_, _, managedEmbeddingBytes := s.db.GetDatabaseManagedEmbeddingStats(dbName)

	searchStatus := s.db.GetDatabaseSearchStatus(dbName)

	response := map[string]interface{}{
		"name":                  dbName,
		"status":                "online",
		"default":               dbName == defaultDB,
		"type":                  "standard",
		"nodeCount":             nodeCount,
		"edgeCount":             edgeCount,
		"nodeStorageBytes":      nodeStorageBytes,
		"managedEmbeddingBytes": managedEmbeddingBytes,
		"searchReady":           searchStatus.Ready,
		"searchBuilding":        searchStatus.Building,
		"searchInitialized":     searchStatus.Initialized,
		"searchStrategy":        searchStatus.Strategy,
		"searchPhase":           searchStatus.Phase,
		"searchProcessed":       searchStatus.ProcessedNodes,
		"searchTotal":           searchStatus.TotalNodes,
		"searchRate":            searchStatus.RateNodesPerSec,
		"searchEtaSeconds":      searchStatus.ETASeconds,
	}
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) compositeConstituentStats(r *http.Request, compositeName string) ([]map[string]interface{}, bool) {
	constituents, err := s.dbManager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, true
	}
	sort.Slice(constituents, func(i, j int) bool {
		return cyphertext.NameLower(constituents[i].Alias) < cyphertext.NameLower(constituents[j].Alias)
	})
	authToken := strings.TrimSpace(r.Header.Get("Authorization"))
	stats := make([]map[string]interface{}, 0, len(constituents))
	partial := false
	for _, c := range constituents {
		row := map[string]interface{}{
			"alias":                 c.Alias,
			"database":              c.DatabaseName,
			"type":                  c.Type,
			"accessMode":            c.AccessMode,
			"reachable":             true,
			"nodeCount":             int64(0),
			"edgeCount":             int64(0),
			"nodeStorageBytes":      int64(0),
			"managedEmbeddingBytes": int64(0),
			"searchReady":           false,
			"searchBuilding":        false,
			"searchInitialized":     false,
			"searchStrategy":        "unknown",
			"searchPhase":           "not_initialized",
			"searchProcessed":       int64(0),
			"searchTotal":           int64(0),
			"searchRate":            float64(0),
			"searchEtaSeconds":      int64(-1),
		}
		target := compositeName + "." + c.Alias
		engine, getErr := s.dbManager.GetStorageWithAuth(target, authToken)
		if getErr != nil {
			partial = true
			row["reachable"] = false
			row["error"] = getErr.Error()
			stats = append(stats, row)
			continue
		}
		nodeCount, nErr := engine.NodeCount()
		edgeCount, eErr := engine.EdgeCount()
		if nErr != nil || eErr != nil {
			partial = true
			row["reachable"] = false
			if nErr != nil {
				row["error"] = nErr.Error()
			} else {
				row["error"] = eErr.Error()
			}
			stats = append(stats, row)
			continue
		}
		_, nodeStorageBytes, _ := s.dbManager.GetStorageSize(c.DatabaseName)
		_, _, managedEmbeddingBytes := s.db.GetDatabaseManagedEmbeddingStats(c.DatabaseName)
		searchStatus := s.db.GetDatabaseSearchStatus(c.DatabaseName)
		row["nodeCount"] = nodeCount
		row["edgeCount"] = edgeCount
		row["nodeStorageBytes"] = nodeStorageBytes
		row["managedEmbeddingBytes"] = managedEmbeddingBytes
		row["searchReady"] = searchStatus.Ready
		row["searchBuilding"] = searchStatus.Building
		row["searchInitialized"] = searchStatus.Initialized
		row["searchStrategy"] = searchStatus.Strategy
		row["searchPhase"] = searchStatus.Phase
		row["searchProcessed"] = searchStatus.ProcessedNodes
		row["searchTotal"] = searchStatus.TotalNodes
		row["searchRate"] = searchStatus.RateNodesPerSec
		row["searchEtaSeconds"] = searchStatus.ETASeconds
		stats = append(stats, row)
	}
	return stats, partial
}

// handleClusterStatus returns cluster status (standalone mode)
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request, dbName string) {
	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(getClaims(r), dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}
	response := map[string]interface{}{
		"mode":     "standalone",
		"database": dbName,
		"status":   "online",
	}
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

// handleTransactionEndpoint routes transaction-related requests
func (s *Server) handleTransactionEndpoint(w http.ResponseWriter, r *http.Request, dbName string, remaining []string) {
	switch {
	case len(remaining) == 0:
		// POST /db/{dbName}/tx - open new transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
			return
		}
		s.handleOpenTransaction(w, r, dbName)

	case remaining[0] == "commit" && len(remaining) == 1:
		// POST /db/{dbName}/tx/commit - implicit transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
			return
		}
		s.handleImplicitTransaction(w, r, dbName)

	case len(remaining) == 1:
		// POST/DELETE /db/{dbName}/tx/{txId}
		txID := remaining[0]
		switch r.Method {
		case http.MethodPost:
			s.handleExecuteInTransaction(w, r, dbName, txID)
		case http.MethodDelete:
			s.handleRollbackTransaction(w, r, dbName, txID)
		default:
			s.writeNeo4jPostOrDeleteRequired(w, r)
		}

	case len(remaining) == 2 && remaining[1] == "commit":
		// POST /db/{dbName}/tx/{txId}/commit
		if r.Method != http.MethodPost {
			s.writeNeo4jPostRequired(w, r, "Neo.ClientError.Request.Invalid")
			return
		}
		txID := remaining[0]
		s.handleCommitTransaction(w, r, dbName, txID)

	default:
		s.writeLocalizedNeo4jError(w, r, http.StatusNotFound, "Neo.ClientError.Request.Invalid", localization.UnknownTransactionEndpoint())
	}
}

func (s *Server) writeRequestAdmissionError(w http.ResponseWriter, r *http.Request, err error) {
	message := localization.ApplicationMemoryPressure()
	if errors.Is(err, memory.ErrStaleSample) {
		message = localization.ApplicationMemoryUnavailable()
	}
	s.writeLocalizedNeo4jError(w, r, http.StatusServiceUnavailable,
		"Neo.TransientError.General.MemoryPoolOutOfMemoryError", message)
}

// TransactionRequest follows Neo4j HTTP API format. TransactionMetadata is
// NornicDB's HTTP equivalent of Bolt BEGIN's standard tx_metadata field.
type TransactionRequest struct {
	Statements          []StatementRequest `json:"statements"`
	TransactionMetadata jsontext.NumberMap `json:"tx_metadata,omitempty"`
}

// StatementRequest is a single Cypher statement.
type StatementRequest struct {
	Statement          string             `json:"statement"`
	Parameters         jsontext.NumberMap `json:"parameters,omitempty"`
	ResultDataContents []string           `json:"resultDataContents,omitempty"` // ["row", "graph"]
	IncludeStats       bool               `json:"includeStats,omitempty"`
}

// decodeTransactionRequest decodes a transaction request body like Neo4j's
// HTTP API: a JSON number in the statement parameters without a fraction or
// exponent is a Cypher INTEGER (int64), any other number is a FLOAT, at any
// depth of lists and maps. A plain decode into interface{} makes every number
// a float64, so large IDs lose precision, SKIP $n / range(1, $n) reject the
// value, and $a / 2 is fractional. The body is decoded once with UseNumber
// (parameters are the request's only numbers) and the parameter numbers are
// converted in place.
var errInvalidTransactionRequestFormat = errors.New("invalid transaction request format")

type transactionRequestLimit struct {
	bounded bool
	bytes   int64
}

func decodeTransactionRequest(body io.Reader, req *TransactionRequest, limit transactionRequestLimit) error {
	if limit.bounded {
		body = http.MaxBytesReader(nil, io.NopCloser(body), limit.bytes)
	}
	decoder := jsontext.NewDecoder(body)
	if err := decoder.Decode(req); err != nil {
		var tooLarge *http.MaxBytesError
		if errors.Is(err, io.EOF) || errors.As(err, &tooLarge) {
			return err
		}
		return fmt.Errorf("%w: %w", errInvalidTransactionRequestFormat, err)
	}
	if req.Statements == nil {
		return fmt.Errorf("%w: transaction request must contain a statements list", errInvalidTransactionRequestFormat)
	}
	if _, err := io.Copy(io.Discard, body); err != nil {
		return err
	}
	for i := range req.Statements {
		for key, value := range req.Statements[i].Parameters {
			req.Statements[i].Parameters[key] = cypher.JSONNumbers(value)
		}
	}
	return nil
}

// readTransactionRequest reads a transaction request body (size-limited like
// readJSON) through decodeTransactionRequest. Every transaction endpoint
// reads its statements through here.

// TransactionResponse follows Neo4j HTTP API format exactly.
type TransactionResponse struct {
	Results       []QueryResult        `json:"results"`
	Errors        []QueryError         `json:"errors"`
	Commit        string               `json:"commit,omitempty"`        // URL to commit (for open transactions)
	Transaction   *TransactionInfo     `json:"transaction,omitempty"`   // Transaction state
	LastBookmarks []string             `json:"lastBookmarks,omitempty"` // Bookmark for causal consistency
	Notifications []ServerNotification `json:"notifications,omitempty"` // Server notifications
	Receipt       interface{}          `json:"receipt,omitempty"`       // Mutation receipt (tx_id, wal_seq_start, wal_seq_end, hash)
	Optimistic    interface{}          `json:"optimistic,omitempty"`    // Optimistic mutation metadata (e.g., created IDs)
	// Eventual marks a commit acknowledged through the write-behind buffer
	// (not yet durable). Internal to status selection; not serialized.
	Eventual bool `json:"-"`
}

// TransactionInfo holds transaction state.
type TransactionInfo struct {
	Expires string `json:"expires"` // RFC1123 format
}

// QueryResult is a single query result.
type QueryResult struct {
	QueryType string         `json:"query_type,omitempty"`
	Columns   []string       `json:"columns"`
	Data      []ResultRow    `json:"data"`
	Stats     *QueryStats    `json:"stats,omitempty"`
	Plan      map[string]any `json:"plan,omitempty"`    // EXPLAIN plan (Neo4j-shaped)
	Profile   map[string]any `json:"profile,omitempty"` // PROFILE plan with runtime counters
}

// ResultRow is a row of results with metadata.
type ResultRow struct {
	Row   []interface{} `json:"row"`
	Meta  []interface{} `json:"meta,omitempty"`
	Graph *GraphResult  `json:"graph,omitempty"`
	REST  []interface{} `json:"rest,omitempty"`
}

// GraphResult holds graph-format results.
type GraphResult struct {
	Nodes         []GraphNode         `json:"nodes"`
	Relationships []GraphRelationship `json:"relationships"`
}

// GraphNode is a node in graph format.
type GraphNode struct {
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Labels     []string               `json:"labels"`
	Properties map[string]interface{} `json:"properties"`
}

// GraphRelationship is a relationship in graph format.
type GraphRelationship struct {
	StartNodeID string                 `json:"startNode,omitempty"`
	EndNodeID   string                 `json:"endNode,omitempty"`
	ID          string                 `json:"id"`
	ElementID   string                 `json:"elementId"`
	Type        string                 `json:"type"`
	StartNode   string                 `json:"startNodeElementId"`
	EndNode     string                 `json:"endNodeElementId"`
	Properties  map[string]interface{} `json:"properties"`
}

// QueryStats holds query execution statistics.
//
// The field set and names follow Neo4j's HTTP API exactly (including the
// singular "relationship_deleted"), and every counter is always present, as
// Neo4j sends them.
type QueryStats struct {
	ContainsUpdates       bool `json:"contains_updates"`
	NodesCreated          int  `json:"nodes_created"`
	NodesDeleted          int  `json:"nodes_deleted"`
	PropertiesSet         int  `json:"properties_set"`
	RelationshipsCreated  int  `json:"relationships_created"`
	RelationshipsDeleted  int  `json:"relationship_deleted"`
	LabelsAdded           int  `json:"labels_added"`
	LabelsRemoved         int  `json:"labels_removed"`
	IndexesAdded          int  `json:"indexes_added"`
	IndexesRemoved        int  `json:"indexes_removed"`
	ConstraintsAdded      int  `json:"constraints_added"`
	ConstraintsRemoved    int  `json:"constraints_removed"`
	ContainsSystemUpdates bool `json:"contains_system_updates"`
	SystemUpdates         int  `json:"system_updates"`
}

// queryStatsFromResult is the includeStats object of one statement, built
// from the counters the executor reported (the same counters Bolt sends).
// contains_updates is true when any counter is non-zero.
func queryStatsFromResult(result *cypher.ExecuteResult) *QueryStats {
	stats := &QueryStats{}
	if result != nil && result.Stats != nil {
		stats.NodesCreated = result.Stats.NodesCreated
		stats.NodesDeleted = result.Stats.NodesDeleted
		stats.PropertiesSet = result.Stats.PropertiesSet
		stats.RelationshipsCreated = result.Stats.RelationshipsCreated
		stats.RelationshipsDeleted = result.Stats.RelationshipsDeleted
		stats.LabelsAdded = result.Stats.LabelsAdded
		stats.LabelsRemoved = result.Stats.LabelsRemoved
		stats.IndexesAdded = result.Stats.IndexesAdded
		stats.IndexesRemoved = result.Stats.IndexesRemoved
		stats.ConstraintsAdded = result.Stats.ConstraintsAdded
		stats.ConstraintsRemoved = result.Stats.ConstraintsRemoved
	}
	stats.ContainsUpdates = stats.NodesCreated > 0 || stats.NodesDeleted > 0 ||
		stats.PropertiesSet > 0 || stats.RelationshipsCreated > 0 ||
		stats.RelationshipsDeleted > 0 || stats.LabelsAdded > 0 ||
		stats.LabelsRemoved > 0 || stats.IndexesAdded > 0 ||
		stats.IndexesRemoved > 0 || stats.ConstraintsAdded > 0 ||
		stats.ConstraintsRemoved > 0
	return stats
}

// statementError is the error entry for a statement that failed: the
// Neo4j code the engine raised (or a transient transaction code), with the
// message without that code prefix. Every transaction endpoint reports
// statement errors through here.
func statementError(err error) QueryError {
	code, message := mapSessionExecError(err)
	return QueryError{Code: code, Message: message}
}

// statementFailure is a failed statement's error. When the statement
// compiled and failed while running, its result with its columns and no rows
// is recorded in response first, as Neo4j's HTTP API reports it (#668); a
// statement failing at compile time has no result.
func statementFailure(ctx context.Context, response *TransactionResponse, executor *cypher.StorageExecutor, query string, err error, localize func(localization.Message) string) QueryError {
	failure := statementError(err)
	localizeStatementError(&failure, err, localize)
	if executor != nil && !nornicerrors.IsCompileTimeError(err) {
		columns, columnErr := executor.StatementResultColumnsContext(ctx, query, cypher.RawSource)
		if columnErr != nil {
			// The engine's failure is authoritative. A failed statement does
			// not become a successful empty result when no fixed header is
			// available, and metadata inspection cannot replace its status.
			return failure
		}
		if len(columns) > 0 {
			response.Results = append(response.Results, QueryResult{Columns: columns, Data: []ResultRow{}})
		}
	}
	return failure
}

func localizeStatementError(failure *QueryError, err error, localize func(localization.Message) string) {
	var localized *localization.LocalizedError
	if errors.As(err, &localized) && localized.Code == failure.Code && localize != nil {
		failure.Message = localize(localized.Message)
	}
}

// QueryError is an error from a query (Neo4j format).
type QueryError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ServerNotification is a warning/info from the server.
type ServerNotification struct {
	Code        string           `json:"code"`
	Severity    string           `json:"severity"`
	Title       string           `json:"title"`
	Description string           `json:"description"`
	Position    *NotificationPos `json:"position,omitempty"`
}

// NotificationPos is the position of a notification in the query.
type NotificationPos struct {
	Offset int `json:"offset"`
	Line   int `json:"line"`
	Column int `json:"column"`
}

// handleImplicitTransaction executes statements in an implicit transaction.
// This is the main query endpoint: POST /db/{dbName}/tx/commit
func (s *Server) handleImplicitTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	var req TransactionRequest
	if err := decodeTransactionRequest((r).Body, &req, transactionRequestLimit{bounded: true, bytes: s.config.MaxRequestSize}); err != nil && err != io.EOF {
		s.writeNeo4jInvalidTransactionBody(w, r, err)
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0, len(req.Statements)),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	claims := getClaims(r)
	localize := func(message localization.Message) string { return s.localizedText(w, r, message) }
	if len(req.Statements) > 1 {
		// Several statements are one transaction, as in Neo4j: a failing
		// statement rolls back the ones before it.
		s.runOneShotTransaction(r, claims, dbName, req.Statements, localize, &response)
	} else {
		s.runRequestStatements(s.withRequestIdentity(r.Context(), r, claims), r.Header.Get("Authorization"), claims, dbName, req.Statements, s.autoCommitStatementRunner(r.Header.Get("Authorization")), localize, &response)
	}

	// Determine appropriate HTTP status code
	// Neo4j behavior: Query errors return 200 OK with errors in response body
	// Only infrastructure errors (database not found) return 4xx status codes
	status := http.StatusOK

	// Check for infrastructure errors (these return 4xx status codes)
	if len(response.Errors) > 0 {
		for _, err := range response.Errors {
			// Database not found is an infrastructure error - return 404
			if err.Code == "Neo.ClientError.Database.DatabaseNotFound" && (s.dbManager == nil || !s.dbManager.ExistsOrIsConstituent(dbName)) {
				status = http.StatusNotFound
				break
			}
			// Database access errors are infrastructure errors - return 500
			if err.Code == "Neo.ClientError.Database.General" {
				status = http.StatusInternalServerError
				break
			}
			// Query syntax errors, security errors, etc. return 200 OK
			// with errors in the response body (Neo4j standard behavior)
		}
	} else if s.db.IsAsyncWritesEnabled() {
		// Only return 202 for mutations that actually completed through the
		// eventual-consistency path.
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) && shouldUseAcceptedStatusForMutation(&response) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.applyMVCCPressureWarnings(w, dbName, &response)
	httpresponse.WriteJSON(w, status, response)
}

// grantAccessToNewDatabase grants the admin role and the creating principal's roles full access
// (see, access, read, write) to a newly created database. Called after successful CREATE DATABASE
// or CREATE COMPOSITE DATABASE. No-op when RBAC stores are not loaded.
func (s *Server) grantAccessToNewDatabase(ctx context.Context, dbName string, claims *auth.JWTClaims) {
	if s.allowlistStore == nil || s.privilegesStore == nil {
		return
	}
	allowlist := s.allowlistStore.Allowlist()

	// Ensure admin role has full access to the new database.
	adminRole := string(auth.RoleAdmin)
	if dbs, ok := allowlist[adminRole]; ok && len(dbs) > 0 {
		// Explicit allowlist: add new DB if not present.
		seen := false
		for _, d := range dbs {
			if d == dbName {
				seen = true
				break
			}
		}
		if !seen {
			_ = s.allowlistStore.SaveRoleDatabases(ctx, adminRole, append(append([]string(nil), dbs...), dbName))
		}
	}
	_ = s.privilegesStore.SavePrivilege(ctx, adminRole, dbName, true, true)

	// Grant the creating principal's roles full access.
	if claims != nil && len(claims.Roles) > 0 {
		seenRoles := map[string]struct{}{adminRole: {}}
		for _, r := range claims.Roles {
			role := auth.NormalizeRoleName(r)
			if role == "" {
				continue
			}
			if _, done := seenRoles[role]; done {
				continue
			}
			seenRoles[role] = struct{}{}
			if dbs, ok := allowlist[role]; ok && len(dbs) > 0 {
				seen := false
				for _, d := range dbs {
					if d == dbName {
						seen = true
						break
					}
				}
				if !seen {
					_ = s.allowlistStore.SaveRoleDatabases(ctx, role, append(append([]string(nil), dbs...), dbName))
				}
			}
			_ = s.privilegesStore.SavePrivilege(ctx, role, dbName, true, true)
		}
	}
}

// convertRowToNeo4jFormat returns transaction row values without entity envelopes.
func (s *Server) convertRowToNeo4jFormat(row []interface{}, dbName string) []interface{} {
	converted := make([]interface{}, len(row))
	for i, val := range row {
		converted[i] = s.convertValueToNeo4jFormat(val, dbName)
	}
	return converted
}

// convertValueToNeo4jFormat converts a single value to Neo4j HTTP format.
// Handles storage.Node, storage.Edge, maps, and slices recursively.
func (s *Server) convertValueToNeo4jFormat(val interface{}, dbName string) interface{} {
	value, _ := s.transactionHTTPValue(val, dbName)
	return value
}

func (s *Server) transactionHTTPValue(value interface{}, dbName string, graph ...*transactionHTTPValueState) (interface{}, []interface{}) {
	entityMeta := func(id, elementID, entityType string) []interface{} {
		return []interface{}{map[string]interface{}{
			"id": s.hashStringToInt64(id), "elementId": elementID,
			"type": entityType, "deleted": false,
		}}
	}
	switch typed := value.(type) {
	case []byte:
		// Neo4j HTTP writes the signed Java byte values, including in maps.
		bytes := make([]int8, len(typed))
		for index, value := range typed {
			bytes[index] = int8(value)
		}
		return bytes, []interface{}{nil}
	case float32:
		return transactionHTTPFloat(float64(typed), 32), []interface{}{nil}
	case float64:
		return transactionHTTPFloat(typed, 64), []interface{}{nil}
	case cypher.CypherDate, cypher.CypherLocalTime, cypher.CypherTime, cypher.CypherLocalDateTime, cypher.CypherDateTime:
		return typed.(fmt.Stringer).String(), []interface{}{nil}
	case *cypher.CypherDuration:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		return typed.String(), []interface{}{nil}
	case cypher.CypherUUID:
		return typed.String(), []interface{}{nil}
	case cypher.CypherVector:
		return transactionHTTPVector(typed), []interface{}{nil}
	case cypher.CypherPoint:
		return transactionHTTPPoint(typed), []interface{}{map[string]interface{}{"type": "point"}}
	case *cypher.CypherPoint:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		return transactionHTTPPoint(*typed), []interface{}{map[string]interface{}{"type": "point"}}
	case storage.RemoteEntityValue:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		id, startID, endID := typed.LegacyIDs()
		elementID, _ := typed["elementId"].(string)
		props, _ := typed["properties"].(map[string]interface{})
		if props == nil {
			props = map[string]interface{}{}
		}
		converted, _ := s.transactionHTTPValue(props, dbName)
		properties := converted.(map[string]interface{})
		kind := "node"
		if relationshipType, relationship := typed["type"].(string); relationship {
			kind = "relationship"
			if len(graph) > 0 && graph[0].graph != nil {
				start, _ := typed["startNodeElementId"].(string)
				end, _ := typed["endNodeElementId"].(string)
				graph[0].addEdge(GraphRelationship{StartNodeID: strconv.FormatInt(startID, 10), EndNodeID: strconv.FormatInt(endID, 10), ID: strconv.FormatInt(id, 10), ElementID: elementID, Type: relationshipType, StartNode: start, EndNode: end, Properties: properties})
			}
		} else if len(graph) > 0 && graph[0].graph != nil {
			var labels []string
			switch values := typed["labels"].(type) {
			case []string:
				labels = values
			case []interface{}:
				for _, value := range values {
					if label, ok := value.(string); ok {
						labels = append(labels, label)
					}
				}
			}
			graph[0].addNode(GraphNode{ID: strconv.FormatInt(id, 10), ElementID: elementID, Labels: labels, Properties: properties})
		}
		meta := map[string]interface{}{"id": id, "elementId": elementID, "type": kind, "deleted": false}
		if transactionHTTPWantsREST(graph) {
			return transactionHTTPRESTEntity(kind, dbName, meta, properties, typed), []interface{}{meta}
		}
		return properties, []interface{}{meta}
	case storage.RemotePathValue:
		if _, _, valid := typed.Parts(); !valid {
			return nil, []interface{}{nil}
		}
		entities := make([]interface{}, len(typed))
		for i, entity := range typed {
			entities[i] = entity
		}
		if transactionHTTPWantsREST(graph) {
			return s.transactionHTTPRESTPath(entities, dbName, graph[0]), nil
		}
		row, metadata := s.transactionHTTPValue(entities, dbName, graph...)
		return row, []interface{}{metadata}
	case *storage.Node:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		converted, _ := s.transactionHTTPValue(typed.Properties, dbName)
		properties := converted.(map[string]interface{})
		elementID := typed.ElementID(s.entityDatabase(dbName, typed.ID, ""))
		if len(graph) > 0 && graph[0].graph != nil {
			graph[0].addNode(GraphNode{ID: strconv.FormatInt(s.hashStringToInt64(string(typed.ID)), 10), ElementID: elementID, Labels: typed.Labels, Properties: properties})
		}
		meta := entityMeta(string(typed.ID), elementID, "node")
		if transactionHTTPWantsREST(graph) {
			return transactionHTTPRESTEntity("node", dbName, meta[0].(map[string]interface{}), properties, map[string]interface{}{"labels": typed.Labels}), meta
		}
		return properties, meta
	case *storage.Edge:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		converted, _ := s.transactionHTTPValue(typed.Properties, dbName)
		properties := converted.(map[string]interface{})
		elementID := typed.ElementID(s.entityDatabase(dbName, "", typed.ID))
		if len(graph) > 0 && graph[0].graph != nil {
			graph[0].addEdge(GraphRelationship{StartNodeID: strconv.FormatInt(s.hashStringToInt64(string(typed.StartNode)), 10), EndNodeID: strconv.FormatInt(s.hashStringToInt64(string(typed.EndNode)), 10), ID: strconv.FormatInt(s.hashStringToInt64(string(typed.ID)), 10), ElementID: elementID, Type: typed.Type, StartNode: typed.StartElementID(s.entityDatabase(dbName, typed.StartNode, "")), EndNode: typed.EndElementID(s.entityDatabase(dbName, typed.EndNode, "")), Properties: properties})
		}
		meta := entityMeta(string(typed.ID), elementID, "relationship")
		if transactionHTTPWantsREST(graph) {
			return transactionHTTPRESTEntity("relationship", dbName, meta[0].(map[string]interface{}), properties, map[string]interface{}{"type": typed.Type, "startNodeElementId": typed.StartElementID(s.entityDatabase(dbName, typed.StartNode, "")), "endNodeElementId": typed.EndElementID(s.entityDatabase(dbName, typed.EndNode, ""))}), meta
		}
		return properties, meta
	case *cypher.PathResult:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		return s.transactionHTTPValue(*typed, dbName, graph...)
	case cypher.PathResult:
		entities := make([]interface{}, 0, len(typed.Nodes)+len(typed.Relationships))
		for index, node := range typed.Nodes {
			entities = append(entities, node)
			if index < len(typed.Relationships) {
				entities = append(entities, typed.Relationships[index])
			}
		}
		if transactionHTTPWantsREST(graph) {
			return s.transactionHTTPRESTPath(entities, dbName, graph[0]), nil
		}
		row, metadata := s.transactionHTTPValue(entities, dbName, graph...)
		return row, []interface{}{metadata}
	case map[string]interface{}:
		typed = storage.PublicRemoteMap(typed)
		switch path := typed["_pathResult"].(type) {
		case cypher.PathResult:
			return s.transactionHTTPValue(path, dbName, graph...)
		case *cypher.PathResult:
			return s.transactionHTTPValue(path, dbName, graph...)
		}
		result := make(map[string]interface{}, len(typed))
		metadata := make([]interface{}, 0)
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		ordered := false
		if len(graph) > 0 {
			if evaluatedKeys, ok := graph[0].mapKeyOrders[uintptr(reflect.ValueOf(typed).UnsafePointer())]; ok {
				keys, ordered = evaluatedKeys, true
			}
		}
		for _, key := range keys {
			converted, nested := s.transactionHTTPValue(typed[key], dbName, graph...)
			result[key] = converted
			metadata = append(metadata, nested...)
		}
		if ordered {
			return transactionHTTPOrderedMap{keys: keys, values: result}, metadata
		}
		return result, metadata
	default:
		if value != nil {
			sequence := reflect.ValueOf(value)
			if sequence.Kind() == reflect.Slice || sequence.Kind() == reflect.Array {
				result := make([]interface{}, sequence.Len())
				metadata := make([]interface{}, 0, sequence.Len())
				for index := 0; index < sequence.Len(); index++ {
					converted, nested := s.transactionHTTPValue(sequence.Index(index).Interface(), dbName, graph...)
					result[index] = converted
					metadata = append(metadata, nested...)
				}
				return result, metadata
			}
		}
		return value, []interface{}{nil}
	}
}

// transactionHTTPFloat is a float as Neo4j's HTTP API writes it: a JSON
// number in Java's text (cypher.FormatFloat: 1.0, 1.0E20, 1.0E-7), or, for a
// value JSON can't hold, the string "Infinity", "-Infinity" or "NaN".
func transactionHTTPFloat(value float64, bits int) interface{} {
	text := cypher.FormatFloat(value, bits)
	if math.IsInf(value, 0) || math.IsNaN(value) {
		return text
	}
	return json.Number(text)
}

// entityDatabase resolves the database an entity actually lives in for
// element-id projection: for a composite execution database, the constituent
// that holds the node or edge; otherwise dbName itself (#745 §3, HTTP row
// and meta output). A miss (deleted entity, unavailable constituent) falls
// back to dbName.
func (s *Server) entityDatabase(dbName string, nodeID storage.NodeID, edgeID storage.EdgeID) string {
	if s.dbManager == nil || !s.dbManager.IsCompositeDatabase(dbName) {
		return dbName
	}
	engine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return dbName
	}
	composite, ok := engine.(*storage.CompositeEngine)
	if !ok {
		return dbName
	}
	if nodeID != "" {
		if name := composite.ConstituentDatabaseForNode(nodeID); name != "" {
			return name
		}
	}
	if edgeID != "" {
		if name := composite.ConstituentDatabaseForEdge(edgeID); name != "" {
			return name
		}
	}
	return dbName
}

// hashStringToInt64 converts a string ID to an int64 for Neo4j compatibility.
// Neo4j drivers expect numeric IDs in metadata.
func (s *Server) hashStringToInt64(id string) int64 {
	var hash int64
	for _, c := range id {
		hash = hash*31 + int64(c)
	}
	if hash < 0 {
		hash = -hash
	}
	return hash
}

// generateBookmark generates a bookmark for causal consistency
func (s *Server) generateBookmark() string {
	return fmt.Sprintf("FB:nornicdb:%d", time.Now().UnixNano())
}

// Transaction management (explicit transactions)
//
// Explicit HTTP transactions are bound to a per-tx executor instance:
//   1) open transaction => BEGIN on dedicated executor
//   2) execute in tx    => run statements on the same executor/tx context
//   3) commit           => optional final statements, then COMMIT
//   4) rollback         => ROLLBACK and discard tx session
//
// This ensures rollback semantics are real (writes are not persisted on rollback)
// and keeps implicit transaction behavior unchanged.

// transactionURL is the URL of an open explicit transaction, built from the
// request the client sent (scheme, host, base path via getBaseURL) as Neo4j
// does, so the client can follow it from wherever it reached the server.
func (s *Server) transactionURL(r *http.Request, dbName, txID string) string {
	return fmt.Sprintf("%s/db/%s/tx/%s", s.getBaseURL(r), dbName, txID)
}

// transactionCommitURL is the commit URL of an open explicit transaction.
func (s *Server) transactionCommitURL(r *http.Request, dbName, txID string) string {
	return s.transactionURL(r, dbName, txID) + "/commit"
}

func (s *Server) appendStatementResult(response *TransactionResponse, result *cypher.ExecuteResult, dbName string, includeStats bool, contents ...[]string) {
	if selected, ok := result.Metadata["db"].(string); ok && selected != "" {
		dbName = selected
	}
	columns := result.Columns
	if columns == nil {
		columns = []string{}
	}
	qr := QueryResult{
		Columns: columns,
		Data:    make([]ResultRow, len(result.Rows)),
	}
	for i, row := range result.Rows {
		var graph []*transactionHTTPValueState
		if len(result.MapKeyOrders) > 0 {
			graph = []*transactionHTTPValueState{{mapKeyOrders: result.MapKeyOrders}}
		}
		if len(contents) > 0 {
			for _, format := range contents[0] {
				if format == "graph" {
					graph = []*transactionHTTPValueState{{graph: &GraphResult{Nodes: []GraphNode{}, Relationships: []GraphRelationship{}}, mapKeyOrders: result.MapKeyOrders}}
					break
				}
			}
		}
		convertedRow := make([]interface{}, len(row))
		metadata := make([]interface{}, 0, len(row))
		for column, value := range row {
			converted, nested := s.transactionHTTPValue(value, dbName, graph...)
			convertedRow[column] = converted
			metadata = append(metadata, nested...)
		}
		qr.Data[i] = ResultRow{Row: convertedRow, Meta: metadata}
		if len(contents) > 0 {
			for _, format := range contents[0] {
				if format == "rest" {
					state := &transactionHTTPValueState{rest: true, mapKeyOrders: result.MapKeyOrders}
					qr.Data[i].REST = make([]interface{}, len(row))
					for column, value := range row {
						qr.Data[i].REST[column], _ = s.transactionHTTPValue(value, dbName, state)
					}
					break
				}
			}
		}
		if len(graph) > 0 && graph[0].graph != nil {
			qr.Data[i].Graph = graph[0].graph
		}
	}
	if includeStats {
		qr.QueryType, _ = cypher.ResolvedStatementQueryType(result.Metadata)
		qr.Stats = queryStatsFromResult(result)
	}
	if result.Metadata != nil {
		if rawPlan, ok := result.Metadata["plan"]; ok {
			if plan, ok := rawPlan.(*cypher.ExecutionPlan); ok && plan != nil {
				qr.Plan = cypher.Neo4jPlanMap(plan, plan.Mode == cypher.ModeProfile)
			}
		}
	}
	response.Results = append(response.Results, qr)
	applyResultMetadata(response, result.Metadata)
}

func shouldUseAcceptedStatusForMutation(resp *TransactionResponse) bool {
	if resp == nil {
		return false
	}
	// 202 Accepted follows the actual commit path, not a statement
	// classifier: every eligible write is acknowledged through the
	// write-behind buffer when it is enabled, and the executor marks that
	// commit as eventual. Receipts are attached on both paths, so they
	// cannot distinguish the two; the marker does.
	return resp.Eventual
}

// executeTxStatements runs a request's statements in an explicit
// transaction, in order, and stops at the first one that fails, as Neo4j
// does: the statements after it are not run. It reports whether a statement
// failed; the caller then rolls the transaction back (rollbackFailedTransaction).
func (s *Server) executeTxStatements(
	ctx context.Context,
	authToken string,
	claims *auth.JWTClaims,
	dbName string,
	lease *txsession.Lease,
	statements []StatementRequest,
	response *TransactionResponse,
) (failed bool) {
	localize := func(message localization.Message) string {
		text, _ := localizationboundary.Render(ctx, s.localizer, message, localizationboundary.FallbackOrID, localizationboundary.SourceEnglish)
		return text
	}
	return s.runRequestStatements(s.withRequestIdentity(ctx, nil, claims), authToken, claims, dbName, statements, s.sessionStatementRunner(lease), localize, response)
}

// statementRunner executes one statement of an HTTP /tx request on a
// database: auto-committed (autoCommitStatementRunner) or in the request's
// transaction (sessionStatementRunner). It returns the executor that ran it,
// which describes a failed statement's result (statementFailure).
type statementRunner func(ctx context.Context, dbName, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error)

// requestStatementError is a statement failure with its own Neo4j error, not
// derived from the error text (statementError).
type requestStatementError struct {
	QueryError
}

func (e *requestStatementError) Error() string { return e.Code + ": " + e.Message }

// autoCommitStatementRunner runs a statement on its database's executor,
// committed on its own.
func (s *Server) autoCommitStatementRunner(authToken string) statementRunner {
	return func(ctx context.Context, dbName, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		// For composite databases with remote constituents, the request's
		// auth token is forwarded to the remote constituent engines.
		var executor *cypher.StorageExecutor
		var err error
		if authToken != "" && s.databaseHasRemoteConstituent(dbName) {
			executor, err = cypher.NewDatabaseScopedExecutor(s.databaseScopedExecutorOptions(dbName, authToken))
		} else {
			executor, err = s.executorCache.Get(dbName)
		}
		if err != nil {
			return nil, nil, &requestStatementError{QueryError{
				Code:    "Neo.ClientError.Database.General",
				Message: fmt.Sprintf("Failed to access database '%s': %v", dbName, err),
			}}
		}
		result, err := executor.Execute(cypher.WithClientStatement(ctx), query, params)
		return result, executor, err
	}
}

// sessionStatementRunner runs the original statement in an open transaction.
// The executor owns USE, :USE and the transaction's database selection rules.
func (s *Server) sessionStatementRunner(lease *txsession.Lease) statementRunner {
	session := lease.Session
	return func(ctx context.Context, dbName, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		result, err := lease.ExecuteInSession(cypher.WithClientStatement(txphase.Statements(ctx)), query, params)
		return result, session.Executor, err
	}
}

// runRequestStatements runs an HTTP /tx request's statements in order with
// run, and stops at the first one that fails, as Neo4j does: the statements
// after it are not run. It reports whether a statement failed. Every /tx
// endpoint (one-shot commit, open, execute-in, commit) uses it, so each
// statement gets the same handling: :USE, database access and permission
// checks, comment and BOM removal, the empty-statement error, slow-query
// logging, CREATE DATABASE access grants, SHOW DATABASES filtering by
// visibility, and the result conversion.
func (s *Server) runRequestStatements(
	ctx context.Context,
	authToken string,
	claims *auth.JWTClaims,
	defaultDB string,
	statements []StatementRequest,
	run statementRunner,
	localize func(localization.Message) string,
	response *TransactionResponse,
) (failed bool) {
	ctx = cypher.WithAuthToken(ctx, authToken)
	ctx = cypher.WithAuthenticatedPrincipal(ctx, transactionOwnerKey(nil, claims))
	if claims != nil {
		ctx = cypher.WithProcedureGrantChecker(ctx, s.ProcedureGrantChecker(claims.Roles))
	}
	if !s.isRBACEnforced() {
		// Immutable privileges are administered only with authentication
		// disabled, as in Neo4j.
		ctx = cypher.WithAuthenticationDisabled(ctx)
	}
	// A name is checked like the request's database (graphAccess: a
	// composite constituent needs its composite and its database), and
	// read / write are the privileges of the database its data is in: the
	// principal's one resolver (principalAccess).
	ctx = cypher.WithDatabasePermissionResolver(ctx, defaultDB, s.principalAccess(claims).Allows)
	// The request owns the entire statement sequence and its per-statement
	// handling, including localized failure and partial-result publication.
	runOne := func(stmt StatementRequest) *QueryError {
		effectiveDB, queryStatement := defaultDB, stmt.Statement

		// Use ExistsOrIsConstituent to accept dotted composite.alias references.
		if !s.dbManager.ExistsOrIsConstituent(effectiveDB) {
			return &QueryError{
				Code:    "Neo.ClientError.Database.DatabaseNotFound",
				Message: localize(localization.HTTPDatabaseNotFound(effectiveDB)),
			}
		}

		// Preserve statement text for the shared syntax/column owner. Comments
		// inside an unaliased expression are part of its public column name.
		// Only transport whitespace and an optional UTF-8 BOM are framing.
		queryStatement = strings.TrimSpace(queryStatement)
		if strings.HasPrefix(queryStatement, "\xef\xbb\xbf") {
			queryStatement = strings.TrimSpace(strings.TrimPrefix(queryStatement, "\xef\xbb\xbf"))
		}
		// An empty statement (or only a comment) is Neo4j's SyntaxError, so it
		// fails its request like any other invalid statement.
		if err := cypher.ValidateStatementText(queryStatement); err != nil {
			failure := statementError(err)
			localizeStatementError(&failure, err, localize)
			return &failure
		}

		queryStart := time.Now()
		result, executor, err := run(ctx, effectiveDB, queryStatement, stmt.Parameters)
		s.logSlowQuery(stmt.Statement, stmt.Parameters, time.Since(queryStart), err)
		if err != nil {
			var own *requestStatementError
			if errors.As(err, &own) {
				return &own.QueryError
			}
			failure := statementError(err)
			localizeStatementError(&failure, err, localize)
			// Authorization failures never publish a statement result, even when
			// the executor supplies column metadata alongside the denial. Keep
			// runtime partial results for non-security statement failures below.
			if strings.HasPrefix(failure.Code, "Neo.ClientError.Security.") {
				return &failure
			}
			if result != nil && len(result.Columns) > 0 && !nornicerrors.IsCompileTimeStatus(failure.Code) {
				s.appendStatementResult(response, result, effectiveDB, stmt.IncludeStats, stmt.ResultDataContents)
				return &failure
			}
			queryErr := statementFailure(ctx, response, executor, queryStatement, err, localize)
			return &queryErr
		}

		// The administration owner supplies the resolved identity only after
		// successful creation. Source spelling and ordinary rows are not authority.
		if result != nil && result.Administration != nil && result.Administration.CreatedDatabase != "" {
			s.grantAccessToNewDatabase(ctx, result.Administration.CreatedDatabase, claims)
		}

		// Per-database RBAC: SHOW DATABASES lists only the databases the principal may see.
		if isShowDatabasesQuery(queryStatement) && result.Rows != nil {
			mode := s.getDatabaseAccessMode(claims)
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

		s.appendStatementResult(response, result, effectiveDB, stmt.IncludeStats, stmt.ResultDataContents)
		return nil
	}
	for _, stmt := range statements {
		if queryErr := runOne(stmt); queryErr != nil {
			response.Errors = append(response.Errors, *queryErr)
			return true
		}
	}
	return false
}

// runOneShotTransaction runs a one-shot /tx/commit request of several
// statements as one transaction on dbName, as Neo4j does: it commits when
// every statement succeeds, and a failing statement rolls back the ones
// before it, so nothing the request wrote is kept.
func (s *Server) runOneShotTransaction(
	r *http.Request,
	claims *auth.JWTClaims,
	dbName string,
	statements []StatementRequest,
	localize func(localization.Message) string,
	response *TransactionResponse,
) {
	if !s.canAccessGraph(claims, dbName) {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Security.Forbidden",
			Message: localize(localization.DatabaseAccessDenied(dbName)),
		})
		return
	}
	lease, openErr := s.openRequestTransaction(r, claims, dbName, localize)
	if openErr != nil {
		response.Errors = append(response.Errors, *openErr)
		return
	}
	defer func() { lease.Release() }()
	session := lease.Session
	wrote := false
	run := func(ctx context.Context, target, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		if session == nil {
			var openErr *QueryError
			lease, openErr = s.openRequestTransaction(r, claims, dbName, localize)
			if openErr != nil {
				return nil, nil, &requestStatementError{QueryError: *openErr}
			}
			session = lease.Session
		}
		result, executor, err := s.sessionStatementRunner(lease)(ctx, target, query, params)
		var detail interface{ BoltErrorDetail() string }
		code, _ := mapSessionExecError(err)
		if err != nil && code == "Neo.DatabaseError.Transaction.TransactionStartFailed" && errors.As(err, &detail) && detail.BoltErrorDetail() == "InvalidCallInTransactions" {
			if wrote {
				return nil, executor, &requestStatementError{QueryError{
					Code:    "Neo.DatabaseError.Statement.ExecutionFailed",
					Message: "Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)",
				}}
			}
			if rollbackErr := lease.RollbackAndDelete(ctx); rollbackErr != nil {
				return nil, executor, rollbackErr
			}
			lease.Release()
			lease = nil
			session = nil
			return s.autoCommitStatementRunner(r.Header.Get("Authorization"))(ctx, target, query, params)
		}
		if err == nil && queryStatsFromResult(result).ContainsUpdates {
			wrote = true
		}
		return result, executor, err
	}
	if s.runRequestStatements(s.withRequestIdentity(r.Context(), r, claims), r.Header.Get("Authorization"), claims, dbName, statements, run, localize, response) {
		if session != nil {
			s.rollbackFailedTransaction(r.Context(), lease, response)
		}
		return
	}
	if session != nil {
		s.commitRequestTransaction(r.Context(), lease, response)
	}
}

// openRequestTransaction opens a transaction on dbName for an HTTP request.
// A database with remote constituents gets an executor built with the
// request's auth token, which it forwards to the remote engines.
func (s *Server) openRequestTransaction(r *http.Request, claims *auth.JWTClaims, dbName string, localize func(localization.Message) string) (*txsession.Lease, *QueryError) {
	var lease *txsession.Lease
	var err error
	authToken := r.Header.Get("Authorization")
	ownerKey := transactionOwnerKey(r, claims)
	if authToken != "" && s.databaseHasRemoteConstituent(dbName) {
		executor, execErr := cypher.NewDatabaseScopedExecutor(s.databaseScopedExecutorOptions(dbName, authToken))
		if execErr != nil {
			return nil, &QueryError{Code: "Neo.ClientError.Transaction.TransactionStartFailed", Message: execErr.Error()}
		}
		lease, err = s.txSessions.OpenRequestWithExecutorForOwner(r.Context(), dbName, executor, ownerKey)
	} else {
		lease, err = s.txSessions.OpenRequestForOwner(r.Context(), dbName, ownerKey)
	}
	if err != nil {
		if errors.Is(err, multidb.ErrDatabaseNotFound) {
			return nil, &QueryError{
				Code:    "Neo.ClientError.Database.DatabaseNotFound",
				Message: localize(localization.HTTPDatabaseNotFound(dbName)),
			}
		}
		return nil, &QueryError{Code: "Neo.ClientError.Transaction.TransactionStartFailed", Message: err.Error()}
	}
	retainTransactionResponse(r.Context(), lease)
	return lease, nil
}

// commitRequestTransaction commits an HTTP request's transaction and records
// the commit's error, or its receipt and optimistic metadata, on response.
func (s *Server) commitRequestTransaction(ctx context.Context, lease *txsession.Lease, response *TransactionResponse) {
	commitResult, err := lease.CommitAndDelete(ctx)
	if err != nil {
		code, message := nornicerrors.Neo4jCommitStatus(err)
		response.Errors = append(response.Errors, QueryError{Code: code, Message: message})
		return
	}
	if commitResult != nil {
		applyResultMetadata(response, commitResult.Metadata)
	}
}

// applyResultMetadata copies a result's receipt, optimistic and eventual
// metadata to the response.
func applyResultMetadata(response *TransactionResponse, metadata map[string]interface{}) {
	if metadata == nil {
		return
	}
	if receipt, ok := metadata["receipt"]; ok && receipt != nil {
		response.Receipt = receipt
	}
	if optimistic, ok := metadata["optimistic"]; ok && optimistic != nil {
		response.Optimistic = optimistic
	}
	if eventual, ok := metadata["eventual"].(bool); ok && eventual {
		response.Eventual = true
	}
}

// rollbackFailedTransaction ends an HTTP request's transaction after a
// statement in it failed, as Neo4j's HTTP API does: the transaction is rolled
// back and forgotten, so nothing it wrote is kept, and a later request to it
// (a statement, commit or rollback) gets
// Neo.ClientError.Transaction.TransactionNotFound. The response's receipt and
// optimistic metadata described writes that are now discarded, so they are
// dropped.
func (s *Server) rollbackFailedTransaction(ctx context.Context, lease *txsession.Lease, response *TransactionResponse) {
	_ = lease.RollbackAndDelete(ctx)
	response.Receipt = nil
	response.Optimistic = nil
}

// transactionExpires formats an explicit transaction's expiry as an HTTP
// date (RFC 1123 in GMT), as Neo4j sends it.
func transactionExpires(expires time.Time) string {
	return expires.UTC().Format(http.TimeFormat)
}

func mapSessionExecError(err error) (code, message string) {
	return nornicerrors.Neo4jStatus(err)
}

func (s *Server) handleOpenTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	claims := getClaims(r)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(claims, dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}
	var req TransactionRequest
	if err := decodeTransactionRequest((r).Body, &req, transactionRequestLimit{bounded: true, bytes: s.config.MaxRequestSize}); err != nil && err != io.EOF {
		s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
		return
	}

	r = r.WithContext(cypher.WithTransactionMetadata(r.Context(), req.TransactionMetadata))
	lease, openErr := s.openRequestTransaction(r, claims, dbName, func(message localization.Message) string { return s.localizedText(w, r, message) })
	if openErr != nil {
		status := http.StatusInternalServerError
		if openErr.Code == "Neo.ClientError.Database.DatabaseNotFound" {
			status = http.StatusNotFound
		}
		httpresponse.WriteJSON(w, status, TransactionResponse{Results: make([]QueryResult, 0), Errors: []QueryError{*openErr}})
		return
	}
	defer lease.Release()
	txSession := lease.Session

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(r, dbName, txSession.ID),
		Transaction: &TransactionInfo{
			Expires: transactionExpires(txSession.ExpiresAt()),
		},
	}

	if len(req.Statements) > 0 {
		if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, lease, req.Statements, &response) {
			s.rollbackFailedTransaction(r.Context(), lease, &response)
		}
		response.Transaction.Expires = transactionExpires(txSession.ExpiresAt())
	}

	s.applyMVCCPressureWarnings(w, dbName, &response)
	w.Header().Set("Location", s.transactionURL(r, dbName, txSession.ID))
	httpresponse.WriteJSON(w, http.StatusCreated, response)
}

func (s *Server) handleExecuteInTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)
	if !s.canAccessGraph(claims, dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}
	// Keep the early not-found response for unknown IDs. This pointer is only
	// used for the immutable database check; the request acquires a lease after
	// body decoding and revalidates expiry before executing anything.
	if tx, ok := s.txSessions.GetForOwner(txID, transactionOwnerKey(r, claims)); !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jTransactionNotFound(w, r)
		return
	}

	var req TransactionRequest
	if err := decodeTransactionRequest((r).Body, &req, transactionRequestLimit{bounded: true, bytes: s.config.MaxRequestSize}); err != nil {
		lease, ok := s.leaseTransactionOrNotFound(w, r, dbName, txID, transactionOwnerKey(r, claims))
		if !ok {
			return
		}
		_ = lease.RollbackAndDelete(r.Context())
		lease.Release()
		s.writeNeo4jInvalidTransactionBody(w, r, err)
		return
	}
	lease, ok := s.leaseTransactionOrNotFound(w, r, dbName, txID, transactionOwnerKey(r, claims))
	if !ok {
		return
	}
	defer lease.Release()
	tx := lease.Session

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(r, dbName, txID),
		Transaction: &TransactionInfo{
			Expires: transactionExpires(tx.ExpiresAt()),
		},
	}

	if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, lease, req.Statements, &response) {
		s.rollbackFailedTransaction(r.Context(), lease, &response)
	}
	if len(req.Statements) == 0 {
		s.txSessions.Touch(tx)
	}
	response.Transaction.Expires = transactionExpires(tx.ExpiresAt())

	s.applyMVCCPressureWarnings(w, dbName, &response)
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(claims, dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}
	if tx, ok := s.txSessions.GetForOwner(txID, transactionOwnerKey(r, claims)); !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jTransactionNotFound(w, r)
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	var req TransactionRequest
	if err := decodeTransactionRequest((r).Body, &req, transactionRequestLimit{bounded: true, bytes: s.config.MaxRequestSize}); err != nil && err != io.EOF {
		lease, ok := s.leaseTransactionOrNotFound(w, r, dbName, txID, transactionOwnerKey(r, claims))
		if !ok {
			return
		}
		_ = lease.RollbackAndDelete(r.Context())
		lease.Release()
		s.writeNeo4jInvalidTransactionBody(w, r, err)
		return
	}
	lease, ok := s.leaseTransactionOrNotFound(w, r, dbName, txID, transactionOwnerKey(r, claims))
	if !ok {
		return
	}
	defer lease.Release()

	// Execute optional final statements in transaction context first.
	if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, lease, req.Statements, &response) {
		s.rollbackFailedTransaction(r.Context(), lease, &response)
		s.applyMVCCPressureWarnings(w, dbName, &response)
		httpresponse.WriteJSON(w, http.StatusOK, response)
		return
	}

	s.commitRequestTransaction(r.Context(), lease, &response)
	s.applyMVCCPressureWarnings(w, dbName, &response)
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) handleRollbackTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(getClaims(r), dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}

	lease, ok := s.leaseTransactionOrNotFound(w, r, dbName, txID, transactionOwnerKey(r, getClaims(r)))
	if !ok {
		return
	}
	defer lease.Release()

	_ = lease.RollbackAndDelete(r.Context())

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
	}
	s.applyMVCCPressureWarnings(w, dbName, &response)
	httpresponse.WriteJSON(w, http.StatusOK, response)
}

func (s *Server) leaseTransactionOrNotFound(w http.ResponseWriter, r *http.Request, dbName, txID, owner string) (*txsession.Lease, bool) {
	lease, ok := s.txSessions.AcquireForOwner(txID, owner)
	if !ok || lease == nil {
		s.writeNeo4jTransactionNotFound(w, r)
		return nil, false
	}
	if lease.Session.Database != dbName {
		lease.Release()
		s.writeNeo4jTransactionNotFound(w, r)
		return nil, false
	}
	retainTransactionResponse(r.Context(), lease)
	return lease, true
}

func (s *Server) bindStatementCacheIndex() {
	s.statementCacheIndex.Bind(s.dbManager, func() {
		s.executorCache.EvictInvalid()
	})
}
