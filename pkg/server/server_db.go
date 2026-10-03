package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/config/dbconfig"
	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
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

func statementTargetDatabase(defaultDB string, statement string) (string, error) {
	db := strings.TrimSpace(defaultDB)
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return db, nil
	}

	if hasPrefixFold(trimmed, ":USE ") {
		parts := strings.Fields(trimmed)
		if len(parts) < 2 {
			return "", fmt.Errorf(":USE requires a database name")
		}
		target := strings.TrimSpace(parts[1])
		if target == "" {
			return "", fmt.Errorf(":USE requires a database name")
		}
		return target, nil
	}

	// A USE clause runs on the request's database's executor, which routes
	// it (resolving aliases, composite constituents and dynamic references),
	// checks the principal's access to its target and keeps a transaction on
	// its database: one USE rule for every route (#738).
	return db, nil
}

func normalizeStatementForExecution(defaultDB string, statement string) (effectiveDB string, query string, err error) {
	effectiveDB = strings.TrimSpace(defaultDB)
	query = statement
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return effectiveDB, "", nil
	}

	if strings.HasPrefix(strings.ToUpper(trimmed), ":USE") {
		lines := strings.Split(statement, "\n")
		remainingLines := make([]string, 0, len(lines))
		foundUse := false
		for _, line := range lines {
			lineTrimmed := strings.TrimSpace(line)
			if !foundUse && strings.HasPrefix(strings.ToUpper(lineTrimmed), ":USE") {
				parts := strings.Fields(lineTrimmed)
				if len(parts) < 2 {
					return "", "", fmt.Errorf(":USE requires a database name")
				}
				effectiveDB = strings.TrimSpace(parts[1])
				if effectiveDB == "" {
					return "", "", fmt.Errorf(":USE requires a database name")
				}
				foundUse = true
				if len(parts) > 2 {
					remainingLines = append(remainingLines, strings.Join(parts[2:], " "))
				}
				continue
			}
			remainingLines = append(remainingLines, line)
		}
		if foundUse {
			query = strings.TrimSpace(strings.Join(remainingLines, "\n"))
		}
		return effectiveDB, query, nil
	}

	target, targetErr := statementTargetDatabase(defaultDB, statement)
	if targetErr != nil {
		return "", "", targetErr
	}
	return target, query, nil
}

// withRequestIdentity attaches the signed-in user, the user directory and
// the request's connection, for SHOW USERS, SHOW CURRENT USER and SHOW
// TRANSACTIONS (#718).
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

// getExecutorForDatabase returns a Cypher executor scoped to the specified database.
//
// This method provides database isolation by creating a Cypher executor that operates
// on a namespaced storage engine. All queries executed through the returned executor
// will only see data in the specified database.
//
// Executors are cached per database and reused across requests for efficiency.
// This dramatically reduces memory allocations (from 14% to near-zero).
//
// Parameters:
//   - dbName: The name of the database to get an executor for
//
// Returns:
//   - *cypher.StorageExecutor: A Cypher executor scoped to the database
//   - error: Returns an error if the database doesn't exist or cannot be accessed
//
// Example:
//
//	executor, err := server.getExecutorForDatabase("tenant_a")
//	if err != nil {
//		return err // Database doesn't exist
//	}
//	result, err := executor.Execute(ctx, "MATCH (n) RETURN count(n)", nil)
//
// Thread Safety:
//   - Safe for concurrent use
//   - Executors are cached and reused (thread-safe per StorageExecutor design)
//   - Multiple requests can use the same executor concurrently
//
// Performance:
//   - Executors are created once per database and cached
//   - Subsequent requests reuse the cached executor (zero allocation overhead)
//   - Storage engines are cached by DatabaseManager for efficiency
func (s *Server) getExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	// Check cache first (read lock for fast path)
	s.executorsMu.RLock()
	if executor, ok := s.executors[dbName]; ok {
		s.executorsMu.RUnlock()
		// Executors can be created before the embedding model finishes loading.
		// Ensure cached executors pick up the latest query embedder lazily.
		if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
			if emb := baseExec.GetEmbedder(); emb != nil && executor.GetEmbedder() == nil {
				executor.SetEmbedder(emb)
			}
		}
		return executor, nil
	}
	s.executorsMu.RUnlock()

	// Get namespaced storage for this database
	executor, err := s.newDatabaseScopedExecutor(dbName)
	if err != nil {
		return nil, err
	}
	// The cached executor is shared by every auto-commit request of this
	// database: a statement must never leave it inside a transaction.
	// Explicit transactions run on their own per-session executors.
	executor.SetSharedExecutor(true)

	// Cache the executor (write lock for cache update)
	s.executorsMu.Lock()
	// Double-check in case another goroutine created it while we were waiting
	if existing, ok := s.executors[dbName]; ok {
		s.executorsMu.Unlock()
		return existing, nil
	}
	s.executors[dbName] = executor
	s.executorsMu.Unlock()

	return executor, nil
}

// getExecutorForDatabaseWithAuth returns an executor for dbName and forwards authToken
// to remote constituent resolution when a composite database contains remote constituents.
func (s *Server) getExecutorForDatabaseWithAuth(dbName string, authToken string) (*cypher.StorageExecutor, error) {
	if authToken == "" || !s.databaseHasRemoteConstituent(dbName) {
		return s.getExecutorForDatabase(dbName)
	}

	storageEngine, err := s.dbManager.GetStorageWithAuth(dbName, authToken)
	if err != nil {
		return nil, err
	}

	overrides := s.dbConfigStore.GetOverrides(dbName)
	resolved := dbconfig.Resolve(s.processConfig, overrides)
	executor := cypher.NewStorageExecutorWithQueryCachePolicy(storageEngine, resolved.QueryCacheMaxEntries, resolved.QueryCacheTTL)
	executor.SetLocalizationRenderer(s.localizer)
	executor.SetSettingsResolver(func() cypher.SettingsSnapshot {
		overrides := s.dbConfigStore.GetOverrides(dbName)
		current := dbconfig.Resolve(s.processConfig, overrides)
		active, _ := s.databaseConfigRuntimeState(dbName, current)
		return cypher.SettingsSnapshot{Configured: overrides, Active: active}
	})
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s})

	if !s.dbManager.IsCompositeDatabase(dbName) {
		if searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
			executor.SetSearchService(searchSvc)
		}
	}

	if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
		executor.ShareQueryStatisticsFrom(baseExec)
		if emb := baseExec.GetEmbedder(); emb != nil {
			executor.SetEmbedder(emb)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			executor.SetInferenceManager(inferMgr)
		}
		// NornicDB issue #563 defect 1: without this, every query against a
		// composite/remote-auth-scoped database executor logs to io.Discard
		// and never emits a slow_query record, no matter how
		// NORNICDB_SLOW_QUERY_THRESHOLD is configured.
		executor.SetLogger(baseExec.Logger())
		executor.SetSlowQueryThreshold(baseExec.SlowQueryThreshold())
	}

	if q := s.db.GetEmbedQueue(); q != nil {
		executor.SetNodeMutatedCallback(func(nodeID string) { q.Enqueue(nodeID) })
	}

	return executor, nil
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
// Unlike getExecutorForDatabase, this does not cache the executor and is intended
// for per-transaction session state (explicit HTTP transactions).
func (s *Server) newExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	base, err := s.getExecutorForDatabase(dbName)
	if err != nil {
		return nil, err
	}
	executor, err := s.newDatabaseScopedExecutor(dbName)
	if err != nil {
		return nil, err
	}
	executor.ShareQueryStatisticsFrom(base)
	return executor, nil
}

func (s *Server) newDatabaseScopedExecutor(dbName string) (*cypher.StorageExecutor, error) {
	// This returns a NamespacedEngine that automatically prefixes all keys
	// with the database name, ensuring complete data isolation.
	storageEngine, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return nil, err
	}

	overrides := s.dbConfigStore.GetOverrides(dbName)
	resolved := dbconfig.Resolve(s.processConfig, overrides)
	executor := cypher.NewStorageExecutorWithQueryCachePolicy(storageEngine, resolved.QueryCacheMaxEntries, resolved.QueryCacheTTL)
	executor.SetLocalizationRenderer(s.localizer)
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager, db: s.db, server: s})

	// Reuse DB's cached search service instead of creating a new one.
	// Composite roots do not own a search service; search/index operations must target constituents.
	if !s.dbManager.IsCompositeDatabase(dbName) {
		if searchSvc, err := s.db.GetOrCreateSearchService(dbName, storageEngine); err == nil {
			executor.SetSearchService(searchSvc)
		}
	}

	// Copy query embedder from the base DB executor so string-input vector procedures work.
	if baseExec := s.db.GetCypherExecutor(); baseExec != nil {
		executor.ShareQueryStatisticsFrom(baseExec)
		if emb := baseExec.GetEmbedder(); emb != nil {
			executor.SetEmbedder(emb)
		}
		if inferMgr := baseExec.GetInferenceManager(); inferMgr != nil {
			executor.SetInferenceManager(inferMgr)
		}
		// NornicDB issue #563 defect 1: this is the executor behind every
		// HTTP POST /db/{name}/tx/commit for a non-default database — the
		// overwhelming majority of this HTTP traffic. Without inheriting
		// the base executor's logger + threshold here, none of it ever
		// emits a slow_query record.
		executor.SetLogger(baseExec.Logger())
		executor.SetSlowQueryThreshold(baseExec.SlowQueryThreshold())
	}

	// Wire embed queue callback for per-database executor mutations.
	if q := s.db.GetEmbedQueue(); q != nil {
		executor.SetNodeMutatedCallback(func(nodeID string) {
			q.Enqueue(nodeID)
		})
	}

	return executor, nil
}

// invalidateExecutor removes a cached executor for a dropped database.
func (s *Server) invalidateExecutor(dbName string) {
	s.executorsMu.Lock()
	defer s.executorsMu.Unlock()
	delete(s.executors, dbName)
}

// invalidateAllExecutors clears all cached executors to force fresh database manager references.
// This is used when database metadata changes (e.g., dropping databases) to ensure
// all executors see the updated state.
func (s *Server) invalidateAllExecutors() {
	s.executorsMu.Lock()
	defer s.executorsMu.Unlock()
	// Clear all executors - they will be recreated with fresh database manager references
	s.executors = make(map[string]*cypher.StorageExecutor)
}

// databaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface, avoiding import cycles.
type databaseManagerAdapter struct {
	manager *multidb.DatabaseManager
	db      *nornicdb.DB
	server  *Server // Reference to server for cache invalidation
}

func (a *databaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}

func (a *databaseManagerAdapter) ServerIdentity() (string, time.Time) {
	return a.manager.ServerIdentity()
}

func (a *databaseManagerAdapter) SystemDatabaseName() string {
	return a.manager.SystemDatabaseName()
}

func (a *databaseManagerAdapter) DatabaseIdentity(name string) (string, time.Time) {
	return a.manager.DatabaseIdentity(name)
}

func (a *databaseManagerAdapter) DropDatabase(name string) error {
	if err := a.manager.DropDatabase(name); err != nil {
		return err
	}
	if a.db != nil {
		a.db.DropSearchServiceState(name)
		a.db.ResetInferenceService(name)
	}
	// Invalidate cached executor for dropped database
	if a.server != nil {
		a.server.invalidateExecutor(name)
		// Also invalidate all executors to ensure fresh database manager references
		// This ensures queries from other databases see the updated database list
		a.server.invalidateAllExecutors()
	}
	return nil
}

func (a *databaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) Exists(name string) bool {
	return a.manager.Exists(name)
}

func (a *databaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}

func (a *databaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}

func (a *databaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}

func (a *databaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}

func (a *databaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	// Convert interface{} to *multidb.Limits
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}

func (a *databaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}

func (a *databaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	// Convert []interface{} to []multidb.ConstituentRef
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			// Try to convert from map
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        getString(m, "alias"),
					DatabaseName: getString(m, "database_name"),
					Type:         getString(m, "type"),
					AccessMode:   getString(m, "access_mode"),
					URI:          getString(m, "uri"),
					SecretRef:    getString(m, "secret_ref"),
					AuthMode:     getString(m, "auth_mode"),
					User:         getString(m, "user"),
					Password:     getString(m, "password"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}

func (a *databaseManagerAdapter) DropCompositeDatabase(name string) error {
	if err := a.manager.DropCompositeDatabase(name); err != nil {
		return err
	}
	// Invalidate any cached executors that might reference this composite database
	// Note: Composite databases don't have their own executors cached, but we should
	// invalidate the executor for the database we're querying from (usually "nornic")
	// to ensure subsequent queries see the updated state
	if a.server != nil {
		// Invalidate executor cache to force fresh database manager reference
		// This ensures all executors see the updated database list
		a.server.invalidateAllExecutors()
	}
	return nil
}

func (a *databaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getString(m, "alias"),
			DatabaseName: getString(m, "database_name"),
			Type:         getString(m, "type"),
			AccessMode:   getString(m, "access_mode"),
			URI:          getString(m, "uri"),
			SecretRef:    getString(m, "secret_ref"),
			AuthMode:     getString(m, "auth_mode"),
			User:         getString(m, "user"),
			Password:     getString(m, "password"),
		}
	} else if r, ok := constituent.(multidb.ConstituentRef); ok {
		ref = r
	} else {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}

func (a *databaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}

func (a *databaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	result := make([]interface{}, len(constituents))
	for i, c := range constituents {
		result[i] = c
	}
	return result, nil
}

func (a *databaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	result := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		result[i] = &databaseInfoAdapter{info: db}
	}
	return result
}

func (a *databaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}

func (a *databaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

// Helper function to get string from map
func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// databaseInfoAdapter wraps multidb.DatabaseInfo to implement
// cypher.DatabaseInfoInterface.
type databaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *databaseInfoAdapter) Name() string {
	return a.info.Name
}

func (a *databaseInfoAdapter) Type() string {
	return a.info.Type
}

func (a *databaseInfoAdapter) Status() string {
	return a.info.Status
}

func (a *databaseInfoAdapter) IsDefault() bool {
	return a.info.IsDefault
}

func (a *databaseInfoAdapter) CreatedAt() time.Time {
	return a.info.CreatedAt
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
	// Check if database exists (also accepts dotted composite.alias references).
	// This is a fast lookup in the DatabaseManager's metadata.
	if !s.dbManager.ExistsOrIsConstituent(dbName) {
		s.writeNeo4jDatabaseNotFound(w, r, dbName)
		return
	}

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(getClaims(r), dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}

	// Check if this is the default database
	defaultDB := s.dbManager.DefaultDatabaseName()

	// For composite databases, return constituent info instead of size stats.
	if s.dbManager.IsCompositeDatabase(dbName) {
		constituents, _ := s.dbManager.GetCompositeConstituents(dbName)
		sort.Slice(constituents, func(i, j int) bool {
			return strings.ToLower(constituents[i].Alias) < strings.ToLower(constituents[j].Alias)
		})
		consList := make([]map[string]interface{}, 0, len(constituents))
		for _, c := range constituents {
			entry := map[string]interface{}{
				"alias":        c.Alias,
				"databaseName": c.DatabaseName,
				"type":         c.Type,
				"accessMode":   c.AccessMode,
			}
			if c.Type == "remote" && c.URI != "" {
				entry["uri"] = c.URI
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
		s.writeJSON(w, http.StatusOK, response)
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
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) compositeConstituentStats(r *http.Request, compositeName string) ([]map[string]interface{}, bool) {
	constituents, err := s.dbManager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, true
	}
	sort.Slice(constituents, func(i, j int) bool {
		return strings.ToLower(constituents[i].Alias) < strings.ToLower(constituents[j].Alias)
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
	s.writeJSON(w, http.StatusOK, response)
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

// TransactionRequest follows Neo4j HTTP API format exactly.
type TransactionRequest struct {
	Statements []StatementRequest `json:"statements"`
}

// StatementRequest is a single Cypher statement.
type StatementRequest struct {
	Statement          string                 `json:"statement"`
	Parameters         map[string]interface{} `json:"parameters,omitempty"`
	ResultDataContents []string               `json:"resultDataContents,omitempty"` // ["row", "graph"]
	IncludeStats       bool                   `json:"includeStats,omitempty"`
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

func decodeTransactionRequest(body io.Reader, req *TransactionRequest) error {
	decoder := json.NewDecoder(body)
	decoder.UseNumber()
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
			req.Statements[i].Parameters[key] = cypherParameterNumbers(value)
		}
	}
	return nil
}

// readTransactionRequest reads a transaction request body (size-limited like
// readJSON) through decodeTransactionRequest. Every transaction endpoint
// reads its statements through here.
func (s *Server) readTransactionRequest(r *http.Request, req *TransactionRequest) error {
	return decodeTransactionRequest(http.MaxBytesReader(nil, r.Body, s.config.MaxRequestSize), req)
}

// cypherParameterNumbers converts the json.Number values of a decoded
// parameter value: integers (no '.', 'e' or 'E') that fit int64 become int64,
// everything else float64. Lists and maps are converted in place.
func cypherParameterNumbers(value interface{}) interface{} {
	switch v := value.(type) {
	case json.Number:
		if !strings.ContainsAny(string(v), ".eE") {
			if i, err := v.Int64(); err == nil {
				return i
			}
		}
		if f, err := v.Float64(); err == nil {
			return f
		}
		return string(v)
	case []interface{}:
		for i, item := range v {
			v[i] = cypherParameterNumbers(item)
		}
		return v
	case map[string]interface{}:
		for key, item := range v {
			v[key] = cypherParameterNumbers(item)
		}
		return v
	default:
		return value
	}
}

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
}

// TransactionInfo holds transaction state.
type TransactionInfo struct {
	Expires string `json:"expires"` // RFC1123 format
}

// QueryResult is a single query result.
type QueryResult struct {
	Columns []string       `json:"columns"`
	Data    []ResultRow    `json:"data"`
	Stats   *QueryStats    `json:"stats,omitempty"`
	Plan    map[string]any `json:"plan,omitempty"`    // EXPLAIN plan (Neo4j-shaped)
	Profile map[string]any `json:"profile,omitempty"` // PROFILE plan with runtime counters
}

// ResultRow is a row of results with metadata.
type ResultRow struct {
	Row   []interface{} `json:"row"`
	Meta  []interface{} `json:"meta,omitempty"`
	Graph *GraphResult  `json:"graph,omitempty"`
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
	ID         string                 `json:"id"`
	ElementID  string                 `json:"elementId"`
	Type       string                 `json:"type"`
	StartNode  string                 `json:"startNodeElementId"`
	EndNode    string                 `json:"endNodeElementId"`
	Properties map[string]interface{} `json:"properties"`
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
func statementFailure(response *TransactionResponse, executor *cypher.StorageExecutor, query string, err error) QueryError {
	failure := statementError(err)
	if executor != nil && !nornicerrors.IsCompileTimeError(err) {
		if columns := executor.StatementColumns(query); len(columns) > 0 {
			response.Results = append(response.Results, QueryResult{Columns: columns, Data: []ResultRow{}})
		}
	}
	return failure
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
	if err := s.readTransactionRequest(r, &req); err != nil && err != io.EOF {
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
	s.writeJSON(w, status, response)
}

// grantAccessToNewDatabase grants the admin role and the creating principal's roles full access
// (see, access, read, write) to a newly created database. Called after successful CREATE DATABASE
// or CREATE COMPOSITE DATABASE. No-op when RBAC stores are not loaded.
func (s *Server) grantAccessToNewDatabase(ctx context.Context, dbName string, claims *auth.JWTClaims) {
	if s.allowlistStore == nil || s.privilegesStore == nil {
		return
	}
	allowlist := s.allowlistStore.Allowlist()
	normalizeRole := func(r string) string {
		r = strings.TrimSpace(r)
		r = strings.ToLower(r)
		r = strings.TrimPrefix(r, "role_")
		return r
	}

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
			role := normalizeRole(r)
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
	case cypher.CypherPoint:
		return transactionHTTPPoint(typed), []interface{}{map[string]interface{}{"type": "point"}}
	case *cypher.CypherPoint:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		return transactionHTTPPoint(*typed), []interface{}{map[string]interface{}{"type": "point"}}
	case *storage.Node:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		converted, _ := s.transactionHTTPValue(typed.Properties, dbName)
		properties := converted.(map[string]interface{})
		elementID := storage.NodeElementID(s.entityDatabase(dbName, typed.ID, ""), typed.ID)
		if len(graph) > 0 && graph[0].graph != nil {
			graph[0].addNode(GraphNode{ID: strconv.FormatInt(s.hashStringToInt64(string(typed.ID)), 10), ElementID: elementID, Labels: typed.Labels, Properties: properties})
		}
		return properties, entityMeta(string(typed.ID), elementID, "node")
	case *storage.Edge:
		if typed == nil {
			return nil, []interface{}{nil}
		}
		converted, _ := s.transactionHTTPValue(typed.Properties, dbName)
		properties := converted.(map[string]interface{})
		elementID := storage.RelationshipElementID(s.entityDatabase(dbName, "", typed.ID), typed.ID)
		if len(graph) > 0 && graph[0].graph != nil {
			graph[0].addEdge(GraphRelationship{ID: strconv.FormatInt(s.hashStringToInt64(string(typed.ID)), 10), ElementID: elementID, Type: typed.Type, StartNode: storage.NodeElementID(s.entityDatabase(dbName, typed.StartNode, ""), typed.StartNode), EndNode: storage.NodeElementID(s.entityDatabase(dbName, typed.EndNode, ""), typed.EndNode), Properties: properties})
		}
		return properties, entityMeta(string(typed.ID), elementID, "relationship")
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
		row, metadata := s.transactionHTTPValue(entities, dbName, graph...)
		return row, []interface{}{metadata}
	case map[string]interface{}:
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

func transactionHTTPFloat(value float64, bits int) interface{} {
	switch {
	case math.IsInf(value, 1):
		return "Infinity"
	case math.IsInf(value, -1):
		return "-Infinity"
	case math.IsNaN(value):
		return "NaN"
	default:
		text := strconv.FormatFloat(value, 'g', -1, bits)
		if !strings.ContainsAny(text, ".eE") {
			text += ".0"
		}
		return json.Number(text)
	}
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
		if len(graph) > 0 && graph[0].graph != nil {
			qr.Data[i].Graph = graph[0].graph
		}
	}
	if includeStats {
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
	// Only report eventual consistency when the request completed without a
	// durable receipt and instead exposed optimistic metadata. This reflects the
	// actual async write-behind path rather than the global config toggle.
	return resp.Receipt == nil && resp.Optimistic != nil
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
	session *txsession.Session,
	statements []StatementRequest,
	response *TransactionResponse,
) (failed bool) {
	localize := func(message localization.Message) string {
		text, _ := s.renderMessage(ctx, message)
		return text
	}
	return s.runRequestStatements(s.withRequestIdentity(ctx, nil, claims), authToken, claims, dbName, statements, s.sessionStatementRunner(session), localize, response)
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
		executor, err := s.getExecutorForDatabaseWithAuth(dbName, authToken)
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

// sessionStatementRunner runs a statement in an open transaction. A
// statement whose :USE names a constituent of the transaction's composite
// database reaches it through a USE clause, the one way a composite
// transaction targets a constituent (as in Neo4j); a statement with its own
// USE clause keeps it.
func (s *Server) sessionStatementRunner(session *txsession.Session) statementRunner {
	return func(ctx context.Context, dbName, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		if queryErr := s.otherDatabaseInTransactionError(session.Database, dbName, query); queryErr != nil {
			return nil, nil, &requestStatementError{QueryError: *queryErr}
		}
		if s.isConstituentOf(session.Database, dbName) {
			if _, _, hasUse, _ := fabric.ParseUseClause(query, false); !hasUse {
				query = "USE " + dbName + " " + query
			}
		}
		result, err := s.txSessions.ExecuteInSession(cypher.WithClientStatement(ctx), session, query, params)
		return result, session.Executor, err
	}
}

// otherDatabaseInTransactionError is the error for a statement of a
// transaction on txDB that targets database target (with USE or :USE), or
// nil when target is txDB (by name or alias) or one of composite txDB's
// constituents. A transaction cannot span databases (Neo4j Operations
// Manual: "a transaction cannot span across multiple databases"), and a
// request that switches database with :USE partway through fails as a
// whole (#683). The statement fails, so the request's transaction is rolled
// back and nothing it wrote is kept.
//
// A write gets Neo4j's error, Neo.ClientError.Statement.AccessMode "Writing
// to more than one database per transaction is not allowed" (verified
// against Neo4j 5.26). Neo4j Community has one user database, so its error
// for a read of a second one could not be observed; a read gets the same
// code with the message worded for a read.
func (s *Server) otherDatabaseInTransactionError(txDB, target, query string) *QueryError {
	if s.sameDatabase(txDB, target) {
		return nil
	}
	if s.isConstituentOf(txDB, target) {
		return nil
	}
	requirements := cypher.QueryPermissionRequirements(query)
	if requirements.Write || requirements.Schema {
		return &QueryError{
			Code:    "Neo.ClientError.Statement.AccessMode",
			Message: fmt.Sprintf("Writing to more than one database per transaction is not allowed. Attempted write to %s, currently writing to %s", target, txDB),
		}
	}
	return &QueryError{
		Code:    "Neo.ClientError.Statement.AccessMode",
		Message: fmt.Sprintf("Accessing more than one database per transaction is not allowed. Attempted access to %s, currently using %s", target, txDB),
	}
}

// isConstituentOf reports whether target names a constituent of composite
// database composite (composite.alias).
func (s *Server) isConstituentOf(composite, target string) bool {
	return s.dbManager != nil && s.dbManager.IsCompositeDatabase(composite) && strings.HasPrefix(target, composite+".")
}

// sameDatabase reports whether two database names or aliases name the same
// database.
func (s *Server) sameDatabase(a, b string) bool {
	if a == b {
		return true
	}
	if s.dbManager == nil {
		return false
	}
	resolvedA, errA := s.dbManager.ResolveDatabase(a)
	resolvedB, errB := s.dbManager.ResolveDatabase(b)
	return errA == nil && errB == nil && resolvedA == resolvedB
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
	mode := s.getDatabaseAccessMode(claims)
	// A name is checked like the request's database (graphAccess: a
	// composite constituent needs its composite and its database), and
	// read / write are the privileges of the database its data is in.
	ctx = cypher.WithDatabasePermissionResolver(ctx, defaultDB, func(database, permission string) bool {
		target, allowed := s.graphAccess(mode, database)
		if !allowed {
			return false
		}
		if !s.isRBACEnforced() {
			return true
		}
		switch permission {
		case "read":
			return s.getResolvedAccess(claims, target).Read
		case "write":
			return s.getResolvedAccess(claims, target).Write
		case "schema", "admin":
			return claims != nil && hasPermission(s, claims.Roles, auth.Permission(permission))
		}
		return false
	})
	for _, stmt := range statements {
		if queryErr := s.runRequestStatement(ctx, claims, defaultDB, stmt, run, localize, response); queryErr != nil {
			response.Errors = append(response.Errors, *queryErr)
			return true
		}
	}
	return false
}

// runRequestStatement runs one statement of an HTTP /tx request and appends
// its result; it returns the statement's error.
func (s *Server) runRequestStatement(
	ctx context.Context,
	claims *auth.JWTClaims,
	defaultDB string,
	stmt StatementRequest,
	run statementRunner,
	localize func(localization.Message) string,
	response *TransactionResponse,
) *QueryError {
	// Each statement can override the URL's database with its own :USE.
	effectiveDB, queryStatement, resolveErr := normalizeStatementForExecution(defaultDB, stmt.Statement)
	if resolveErr != nil {
		queryErr := statementError(resolveErr)
		return &queryErr
	}

	// Per-database access: deny if principal may not access this database
	// (Neo4j-aligned). A statement's leading USE clause names the database
	// it runs on: its access and permissions are checked here with the
	// same messages as the request's database, before the executor routes
	// the statement there (the executor checks every database a statement
	// selects again, including subquery USE and composite constituents).
	checkedDB := effectiveDB
	if use, _, hasUse, err := fabric.ParseUseClause(queryStatement, false); err == nil && hasUse && !use.IsDynamic() {
		checkedDB = use.Name
	}
	if !s.canAccessGraph(claims, checkedDB) {
		return &QueryError{
			Code:    "Neo.ClientError.Security.Forbidden",
			Message: localize(localization.DatabaseAccessDenied(checkedDB)),
		}
	}
	if missing := s.missingQueryPermission(claims, checkedDB, queryStatement); missing != "" {
		message := localization.DatabaseWriteDenied(checkedDB)
		if missing == auth.PermSchema {
			message = localization.SchemaPermissionRequired()
		} else if missing == auth.PermAdmin {
			message = localization.AdminPermissionRequired()
		}
		return &QueryError{Code: "Neo.ClientError.Security.Forbidden", Message: localize(message)}
	}

	// Use ExistsOrIsConstituent to accept dotted composite.alias references.
	if !s.dbManager.ExistsOrIsConstituent(effectiveDB) {
		return &QueryError{
			Code:    "Neo.ClientError.Database.DatabaseNotFound",
			Message: localize(localization.HTTPDatabaseNotFound(effectiveDB)),
		}
	}

	// Cypher comments are removed before the statement checks and execution
	// (the executor's quote-aware rule: // in a string literal is text), and a
	// UTF-8 BOM (some clients send one; it breaks executor routing, e.g.
	// CREATE DATABASE).
	queryStatement = strings.TrimSpace(cypher.StripComments(queryStatement))
	if strings.HasPrefix(queryStatement, "\xef\xbb\xbf") {
		queryStatement = strings.TrimSpace(strings.TrimPrefix(queryStatement, "\xef\xbb\xbf"))
	}
	// An empty statement (or only a comment) is Neo4j's SyntaxError, so it
	// fails its request like any other invalid statement.
	if queryStatement == "" {
		return &QueryError{
			Code:    "Neo.ClientError.Statement.SyntaxError",
			Message: "Unexpected end of input: expected CYPHER, EXPLAIN, PROFILE or Query",
		}
	}

	queryStart := time.Now()
	result, executor, err := run(s.withDatabasePermissionChecker(ctx, claims, effectiveDB), effectiveDB, queryStatement, stmt.Parameters)
	s.logSlowQuery(stmt.Statement, stmt.Parameters, time.Since(queryStart), err)
	if err != nil {
		var own *requestStatementError
		if errors.As(err, &own) {
			return &own.QueryError
		}
		if failure := statementError(err); result != nil && len(result.Columns) > 0 && !nornicerrors.IsCompileTimeStatus(failure.Code) {
			s.appendStatementResult(response, result, effectiveDB, stmt.IncludeStats, stmt.ResultDataContents)
			return &failure
		}
		queryErr := statementFailure(response, executor, queryStatement, err)
		return &queryErr
	}

	// Auto-grant access when a new database is created: admins and the creating principal get full access.
	if isCreateDatabaseStatement(queryStatement) {
		if createdName, ok := parseCreatedDatabaseName(queryStatement); ok && createdName != "" {
			s.grantAccessToNewDatabase(ctx, createdName, claims)
			// Ensure CREATE DATABASE always returns a proper result (name + row).
			if len(result.Columns) == 0 && len(result.Rows) == 0 {
				s.logEvent(ctx, slog.LevelWarn, localization.ServerCreateDatabaseDefensiveFixEvent(createdName))
				result.Columns = []string{"name"}
				result.Rows = [][]interface{}{{createdName}}
			}
		}
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
	session, openErr := s.openRequestTransaction(r, claims, dbName, localize)
	if openErr != nil {
		response.Errors = append(response.Errors, *openErr)
		return
	}
	wrote := false
	run := func(ctx context.Context, target, query string, params map[string]interface{}) (*cypher.ExecuteResult, *cypher.StorageExecutor, error) {
		if session == nil {
			var openErr *QueryError
			session, openErr = s.openRequestTransaction(r, claims, dbName, localize)
			if openErr != nil {
				return nil, nil, &requestStatementError{QueryError: *openErr}
			}
		}
		result, executor, err := s.sessionStatementRunner(session)(ctx, target, query, params)
		var detail interface{ BoltErrorDetail() string }
		code, _ := mapSessionExecError(err)
		if err != nil && code == "Neo.DatabaseError.Transaction.TransactionStartFailed" && errors.As(err, &detail) && detail.BoltErrorDetail() == "InvalidCallInTransactions" {
			if wrote {
				return nil, executor, &requestStatementError{QueryError{
					Code:    "Neo.DatabaseError.Statement.ExecutionFailed",
					Message: "Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)",
				}}
			}
			if rollbackErr := s.txSessions.RollbackAndDelete(ctx, session); rollbackErr != nil {
				return nil, executor, rollbackErr
			}
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
			s.rollbackFailedTransaction(r.Context(), session, response)
		}
		return
	}
	if session != nil {
		s.commitRequestTransaction(r.Context(), session, response)
	}
}

// openRequestTransaction opens a transaction on dbName for an HTTP request.
// A database with remote constituents gets an executor built with the
// request's auth token, which it forwards to the remote engines.
func (s *Server) openRequestTransaction(r *http.Request, claims *auth.JWTClaims, dbName string, localize func(localization.Message) string) (*txsession.Session, *QueryError) {
	var session *txsession.Session
	var err error
	authToken := r.Header.Get("Authorization")
	ownerKey := transactionOwnerKey(r, claims)
	if authToken != "" && s.databaseHasRemoteConstituent(dbName) {
		executor, execErr := s.getExecutorForDatabaseWithAuth(dbName, authToken)
		if execErr != nil {
			return nil, &QueryError{Code: "Neo.ClientError.Transaction.TransactionStartFailed", Message: execErr.Error()}
		}
		session, err = s.txSessions.OpenWithExecutorForOwner(r.Context(), dbName, executor, ownerKey)
	} else {
		session, err = s.txSessions.OpenForOwner(r.Context(), dbName, ownerKey)
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
	return session, nil
}

// commitRequestTransaction commits an HTTP request's transaction and records
// the commit's error, or its receipt and optimistic metadata, on response.
func (s *Server) commitRequestTransaction(ctx context.Context, session *txsession.Session, response *TransactionResponse) {
	commitResult, err := s.txSessions.CommitAndDelete(ctx, session)
	if err != nil {
		code, message := nornicerrors.Neo4jCommitStatus(err)
		response.Errors = append(response.Errors, QueryError{Code: code, Message: message})
		return
	}
	if commitResult != nil {
		applyResultMetadata(response, commitResult.Metadata)
	}
}

// applyResultMetadata copies a result's receipt and optimistic metadata to
// the response.
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
}

// rollbackFailedTransaction ends an HTTP request's transaction after a
// statement in it failed, as Neo4j's HTTP API does: the transaction is rolled
// back and forgotten, so nothing it wrote is kept, and a later request to it
// (a statement, commit or rollback) gets
// Neo.ClientError.Transaction.TransactionNotFound. The response's receipt and
// optimistic metadata described writes that are now discarded, so they are
// dropped.
func (s *Server) rollbackFailedTransaction(ctx context.Context, session *txsession.Session, response *TransactionResponse) {
	_ = s.txSessions.RollbackAndDelete(ctx, session)
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
	if err := s.readTransactionRequest(r, &req); err != nil && err != io.EOF {
		s.writeNeo4jInvalidRequestBody(w, r, "Neo.ClientError.Request.InvalidFormat")
		return
	}

	txSession, openErr := s.openRequestTransaction(r, claims, dbName, func(message localization.Message) string { return s.localizedText(w, r, message) })
	if openErr != nil {
		status := http.StatusInternalServerError
		if openErr.Code == "Neo.ClientError.Database.DatabaseNotFound" {
			status = http.StatusNotFound
		}
		s.writeJSON(w, status, TransactionResponse{Results: make([]QueryResult, 0), Errors: []QueryError{*openErr}})
		return
	}

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(r, dbName, txSession.ID),
		Transaction: &TransactionInfo{
			Expires: transactionExpires(txSession.Expires),
		},
	}

	if len(req.Statements) > 0 {
		if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, txSession, req.Statements, &response) {
			s.rollbackFailedTransaction(r.Context(), txSession, &response)
		}
		response.Transaction.Expires = transactionExpires(txSession.Expires)
	}

	s.applyMVCCPressureWarnings(w, dbName, &response)
	w.Header().Set("Location", s.transactionURL(r, dbName, txSession.ID))
	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) handleExecuteInTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)
	if !s.canAccessGraph(claims, dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}

	tx, ok := s.txSessions.GetForOwner(txID, transactionOwnerKey(r, claims))
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jTransactionNotFound(w, r)
		return
	}

	var req TransactionRequest
	if err := s.readTransactionRequest(r, &req); err != nil {
		_ = s.txSessions.RollbackAndDelete(r.Context(), tx)
		s.writeNeo4jInvalidTransactionBody(w, r, err)
		return
	}

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  s.transactionCommitURL(r, dbName, txID),
		Transaction: &TransactionInfo{
			Expires: transactionExpires(tx.Expires),
		},
	}

	if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, tx, req.Statements, &response) {
		s.rollbackFailedTransaction(r.Context(), tx, &response)
	}
	response.Transaction.Expires = transactionExpires(tx.Expires)

	s.applyMVCCPressureWarnings(w, dbName, &response)
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	claims := getClaims(r)

	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(claims, dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	tx, ok := s.txSessions.GetForOwner(txID, transactionOwnerKey(r, claims))
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jTransactionNotFound(w, r)
		return
	}

	var req TransactionRequest
	if err := s.readTransactionRequest(r, &req); err != nil && err != io.EOF {
		_ = s.txSessions.RollbackAndDelete(r.Context(), tx)
		s.writeNeo4jInvalidTransactionBody(w, r, err)
		return
	}

	// Execute optional final statements in transaction context first.
	if s.executeTxStatements(r.Context(), r.Header.Get("Authorization"), claims, dbName, tx, req.Statements, &response) {
		s.rollbackFailedTransaction(r.Context(), tx, &response)
		s.applyMVCCPressureWarnings(w, dbName, &response)
		s.writeJSON(w, http.StatusOK, response)
		return
	}

	s.commitRequestTransaction(r.Context(), tx, &response)
	s.applyMVCCPressureWarnings(w, dbName, &response)
	s.writeJSON(w, http.StatusOK, response)
}

func (s *Server) handleRollbackTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Per-database RBAC: deny if principal may not access this database (Neo4j-aligned).
	if !s.canAccessGraph(getClaims(r), dbName) {
		s.writeNeo4jDatabaseAccessDenied(w, r, dbName)
		return
	}

	tx, ok := s.txSessions.GetForOwner(txID, transactionOwnerKey(r, getClaims(r)))
	if !ok || tx == nil || tx.Database != dbName {
		s.writeNeo4jTransactionNotFound(w, r)
		return
	}

	_ = s.txSessions.RollbackAndDelete(r.Context(), tx)

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
	}
	s.applyMVCCPressureWarnings(w, dbName, &response)
	s.writeJSON(w, http.StatusOK, response)
}
