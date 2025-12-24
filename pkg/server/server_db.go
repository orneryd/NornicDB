package server

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
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
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.Invalid", "database name required")
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
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown endpoint")
	}
}

// getExecutorForDatabase returns a Cypher executor scoped to the specified database.
//
// This method provides database isolation by creating a Cypher executor that operates
// on a namespaced storage engine. All queries executed through the returned executor
// will only see data in the specified database.
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
//   - Each call creates a new executor instance (lightweight operation)
//
// Performance:
//   - The executor is created on-demand for each request
//   - Storage engines are cached by DatabaseManager for efficiency
//   - Overhead is minimal (just executor creation, not storage initialization)
func (s *Server) getExecutorForDatabase(dbName string) (*cypher.StorageExecutor, error) {
	// Get namespaced storage for this database
	// This returns a NamespacedEngine that automatically prefixes all keys
	// with the database name, ensuring complete data isolation
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		return nil, err
	}

	// Create executor scoped to this database
	// The executor will only see data in the namespaced storage
	executor := cypher.NewStorageExecutor(storage)

	// Set DatabaseManager for system commands (CREATE/DROP/SHOW DATABASE)
	// Wrap DatabaseManager to implement the interface expected by executor
	executor.SetDatabaseManager(&databaseManagerAdapter{manager: s.dbManager, db: s.db})

	return executor, nil
}

// databaseManagerAdapter wraps multidb.DatabaseManager to implement
// cypher.DatabaseManagerInterface, avoiding import cycles.
type databaseManagerAdapter struct {
	manager *multidb.DatabaseManager
	db      *nornicdb.DB
}

func (a *databaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}

func (a *databaseManagerAdapter) DropDatabase(name string) error {
	if err := a.manager.DropDatabase(name); err != nil {
		return err
	}
	if a.db != nil {
		a.db.ResetSearchService(name)
		a.db.ResetInferenceService(name)
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
	return a.manager.DropCompositeDatabase(name)
}

func (a *databaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	var ref multidb.ConstituentRef
	if m, ok := constituent.(map[string]interface{}); ok {
		ref = multidb.ConstituentRef{
			Alias:        getString(m, "alias"),
			DatabaseName: getString(m, "database_name"),
			Type:         getString(m, "type"),
			AccessMode:   getString(m, "access_mode"),
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
//	  "edgeCount": 5678
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
//	  "edgeCount": 50
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
	// Check if database exists
	// This is a fast lookup in the DatabaseManager's metadata
	if !s.dbManager.Exists(dbName) {
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Database.DatabaseNotFound",
			fmt.Sprintf("Database '%s' not found", dbName))
		return
	}

	// Get storage for this database to get stats
	// This returns a NamespacedEngine that provides isolated access
	storage, err := s.dbManager.GetStorage(dbName)
	if err != nil {
		s.writeNeo4jError(w, http.StatusInternalServerError, "Neo.ClientError.Database.General",
			fmt.Sprintf("Failed to access database: %v", err))
		return
	}

	// Get stats from the namespaced storage
	// These counts reflect only data in this database (due to namespacing)
	nodeCount, err := storage.NodeCount()
	if err != nil {
		// Log error but continue with 0 count
		nodeCount = 0
	}
	edgeCount, err := storage.EdgeCount()
	if err != nil {
		// Log error but continue with 0 count
		edgeCount = 0
	}

	// Check if this is the default database
	// The default database is configured at startup and cannot be dropped
	defaultDB := s.dbManager.DefaultDatabaseName()

	response := map[string]interface{}{
		"name":      dbName,
		"status":    "online",
		"default":   dbName == defaultDB,
		"nodeCount": nodeCount,
		"edgeCount": edgeCount,
	}
	s.writeJSON(w, http.StatusOK, response)
}

// handleClusterStatus returns cluster status (standalone mode)
func (s *Server) handleClusterStatus(w http.ResponseWriter, r *http.Request, dbName string) {
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
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		s.handleOpenTransaction(w, r, dbName)

	case remaining[0] == "commit" && len(remaining) == 1:
		// POST /db/{dbName}/tx/commit - implicit transaction
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
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
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST or DELETE required")
		}

	case len(remaining) == 2 && remaining[1] == "commit":
		// POST /db/{dbName}/tx/{txId}/commit
		if r.Method != http.MethodPost {
			s.writeNeo4jError(w, http.StatusMethodNotAllowed, "Neo.ClientError.Request.Invalid", "POST required")
			return
		}
		txID := remaining[0]
		s.handleCommitTransaction(w, r, dbName, txID)

	default:
		s.writeNeo4jError(w, http.StatusNotFound, "Neo.ClientError.Request.Invalid", "unknown transaction endpoint")
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

// TransactionResponse follows Neo4j HTTP API format exactly.
type TransactionResponse struct {
	Results       []QueryResult        `json:"results"`
	Errors        []QueryError         `json:"errors"`
	Commit        string               `json:"commit,omitempty"`        // URL to commit (for open transactions)
	Transaction   *TransactionInfo     `json:"transaction,omitempty"`   // Transaction state
	LastBookmarks []string             `json:"lastBookmarks,omitempty"` // Bookmark for causal consistency
	Notifications []ServerNotification `json:"notifications,omitempty"` // Server notifications
}

// TransactionInfo holds transaction state.
type TransactionInfo struct {
	Expires string `json:"expires"` // RFC1123 format
}

// QueryResult is a single query result.
type QueryResult struct {
	Columns []string    `json:"columns"`
	Data    []ResultRow `json:"data"`
	Stats   *QueryStats `json:"stats,omitempty"`
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
type QueryStats struct {
	NodesCreated         int  `json:"nodes_created,omitempty"`
	NodesDeleted         int  `json:"nodes_deleted,omitempty"`
	RelationshipsCreated int  `json:"relationships_created,omitempty"`
	RelationshipsDeleted int  `json:"relationships_deleted,omitempty"`
	PropertiesSet        int  `json:"properties_set,omitempty"`
	LabelsAdded          int  `json:"labels_added,omitempty"`
	LabelsRemoved        int  `json:"labels_removed,omitempty"`
	IndexesAdded         int  `json:"indexes_added,omitempty"`
	IndexesRemoved       int  `json:"indexes_removed,omitempty"`
	ConstraintsAdded     int  `json:"constraints_added,omitempty"`
	ConstraintsRemoved   int  `json:"constraints_removed,omitempty"`
	ContainsUpdates      bool `json:"contains_updates,omitempty"`
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

// stripCypherComments removes Cypher comments from a query string.
// Supports both single-line comments (//) and multi-line comments (/* */).
// This follows the Cypher specification for comments.
//
// Examples:
//
//	"MATCH (n) RETURN n // comment" -> "MATCH (n) RETURN n "
//	"MATCH (n) /* comment */ RETURN n" -> "MATCH (n)  RETURN n"
//	"MATCH (n)\n// line comment\nRETURN n" -> "MATCH (n)\n\nRETURN n"
func stripCypherComments(query string) string {
	if query == "" {
		return query
	}

	var result strings.Builder
	result.Grow(len(query))

	lines := strings.Split(query, "\n")
	inMultiLineComment := false
	outputLines := []string{}

	for _, line := range lines {
		processed := line

		// Handle multi-line comments that span lines
		if inMultiLineComment {
			// Check if this line closes the multi-line comment
			if idx := strings.Index(processed, "*/"); idx >= 0 {
				// Multi-line comment ends on this line
				processed = processed[idx+2:]
				inMultiLineComment = false
				// Continue processing the rest of this line
			} else {
				// Still inside multi-line comment, skip entire line
				// Don't add anything for skipped comment-only lines
				continue
			}
		}

		// Process remaining line for comments
		var lineResult strings.Builder
		i := 0
		lineStartedMultiLineComment := false
		for i < len(processed) {
			// Check for start of multi-line comment
			if i+1 < len(processed) && processed[i:i+2] == "/*" {
				// Find end of multi-line comment
				endIdx := strings.Index(processed[i+2:], "*/")
				if endIdx >= 0 {
					// Multi-line comment ends on same line
					i = i + 2 + endIdx + 2
					continue
				} else {
					// Multi-line comment spans to next line
					inMultiLineComment = true
					lineStartedMultiLineComment = true
					break
				}
			}

			// Check for single-line comment
			if i+1 < len(processed) && processed[i:i+2] == "//" {
				// Rest of line is comment, stop processing
				break
			}

			// Regular character, keep it
			lineResult.WriteByte(processed[i])
			i++
		}

		// Add processed line to output
		// Don't add empty lines that started a multi-line comment (they're entirely comment)
		lineStr := lineResult.String()
		// Only trim if entire line is whitespace (preserve trailing spaces after comments)
		trimmed := strings.TrimSpace(lineStr)
		if trimmed == "" && lineStr != "" {
			// Entire line is whitespace, use empty string
			lineStr = ""
		}
		if !lineStartedMultiLineComment || lineStr != "" {
			outputLines = append(outputLines, lineStr)
		}
	}

	// Join lines, preserving original line structure
	resultStr := strings.Join(outputLines, "\n")

	// Preserve trailing newline if original had one
	if strings.HasSuffix(query, "\n") {
		resultStr += "\n"
	}

	return resultStr
}

// handleImplicitTransaction executes statements in an implicit transaction.
// This is the main query endpoint: POST /db/{dbName}/tx/commit
func (s *Server) handleImplicitTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	var req TransactionRequest
	if err := s.readJSON(r, &req); err != nil {
		s.writeNeo4jError(w, http.StatusBadRequest, "Neo.ClientError.Request.InvalidFormat", "invalid request body")
		return
	}

	response := TransactionResponse{
		Results:       make([]QueryResult, 0, len(req.Statements)),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	claims := getClaims(r)
	hasError := false

	// Default to database from URL path
	// Each statement can override this with its own :USE command
	defaultDbName := dbName

	for _, stmt := range req.Statements {
		if hasError {
			// Skip remaining statements after error (rollback semantics)
			break
		}

		// Check write permission for mutations
		if isMutationQuery(stmt.Statement) {
			if claims != nil && !hasPermission(claims.Roles, auth.PermWrite) {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Security.Forbidden",
					Message: "Write permission required",
				})
				hasError = true
				continue
			}
		}

		// Extract :USE command from this statement if present
		// Each statement can have its own :USE command to switch databases
		effectiveDbName := defaultDbName
		queryStatement := stmt.Statement

		// Check if statement starts with :USE
		trimmedStmt := strings.TrimSpace(stmt.Statement)
		if strings.HasPrefix(trimmedStmt, ":USE") || strings.HasPrefix(trimmedStmt, ":use") {
			lines := strings.Split(stmt.Statement, "\n")
			var remainingLines []string
			foundUse := false
			for _, line := range lines {
				trimmed := strings.TrimSpace(line)
				if !foundUse && (strings.HasPrefix(trimmed, ":USE") || strings.HasPrefix(trimmed, ":use")) {
					// Extract database name from :USE command
					parts := strings.Fields(trimmed)
					if len(parts) >= 2 {
						effectiveDbName = parts[1]
					}
					foundUse = true
					// Check if there's more content on this line after :USE command
					// Format: ":USE dbname rest of query"
					useIndex := strings.Index(trimmed, ":USE")
					if useIndex == -1 {
						useIndex = strings.Index(strings.ToUpper(trimmed), ":USE")
					}
					if useIndex >= 0 {
						// Find where :USE command ends (after database name)
						afterUse := trimmed[useIndex+4:] // Skip ":USE"
						afterUse = strings.TrimSpace(afterUse)
						// Extract database name and remaining query
						fields := strings.Fields(afterUse)
						if len(fields) > 0 {
							// Database name is first field, rest is the query
							if len(fields) > 1 {
								remainingQuery := strings.Join(fields[1:], " ")
								if remainingQuery != "" {
									remainingLines = append(remainingLines, remainingQuery)
								}
							}
						}
					}
					// Skip the :USE line itself (already processed)
					continue
				}
				remainingLines = append(remainingLines, line)
			}
			if foundUse {
				queryStatement = strings.Join(remainingLines, "\n")
				queryStatement = strings.TrimSpace(queryStatement)
			}
		}

		// Check if database exists before attempting to get executor
		if !s.dbManager.Exists(effectiveDbName) {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.DatabaseNotFound",
				Message: fmt.Sprintf("Database '%s' not found", effectiveDbName),
			})
			hasError = true
			continue
		}

		// Strip Cypher comments from query before execution
		// Comments are part of Cypher spec but should be removed before parsing
		queryStatement = stripCypherComments(queryStatement)
		queryStatement = strings.TrimSpace(queryStatement)

		// Skip empty statements (after comment removal)
		if queryStatement == "" {
			// Empty statement after comment removal - return empty result
			response.Results = append(response.Results, QueryResult{
				Columns: []string{},
				Data:    []ResultRow{},
			})
			continue
		}

		// Get executor for the specified database (or the one from :USE command)
		executor, err := s.getExecutorForDatabase(effectiveDbName)
		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Database.General",
				Message: fmt.Sprintf("Failed to access database '%s': %v", effectiveDbName, err),
			})
			hasError = true
			continue
		}

		// Track query execution time for slow query logging
		queryStart := time.Now()
		result, err := executor.Execute(r.Context(), queryStatement, stmt.Parameters)
		queryDuration := time.Since(queryStart)

		// Log slow queries
		s.logSlowQuery(stmt.Statement, stmt.Parameters, queryDuration, err)

		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Statement.SyntaxError",
				Message: err.Error(),
			})
			hasError = true
			continue
		}

		// Convert result to Neo4j format with metadata
		qr := QueryResult{
			Columns: result.Columns,
			Data:    make([]ResultRow, len(result.Rows)),
		}

		for i, row := range result.Rows {
			convertedRow := s.convertRowToNeo4jFormat(row)
			qr.Data[i] = ResultRow{
				Row:  convertedRow,
				Meta: s.generateRowMeta(convertedRow),
			}
		}

		if stmt.IncludeStats {
			qr.Stats = &QueryStats{ContainsUpdates: isMutationQuery(stmt.Statement)}
		}

		response.Results = append(response.Results, qr)
	}

	// Determine appropriate HTTP status code
	// Neo4j behavior: Query errors return 200 OK with errors in response body
	// Only infrastructure errors (database not found) return 4xx status codes
	status := http.StatusOK

	// Check for infrastructure errors (these return 4xx status codes)
	if len(response.Errors) > 0 {
		for _, err := range response.Errors {
			// Database not found is an infrastructure error - return 404
			if err.Code == "Neo.ClientError.Database.DatabaseNotFound" {
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
		// For eventual consistency (async writes), mutations return 202 Accepted
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, status, response)
}

// convertRowToNeo4jFormat converts each value in a row to Neo4j-compatible format.
// This ensures nodes and edges use elementId and have filtered properties.
func (s *Server) convertRowToNeo4jFormat(row []interface{}) []interface{} {
	converted := make([]interface{}, len(row))
	for i, val := range row {
		converted[i] = s.convertValueToNeo4jFormat(val)
	}
	return converted
}

// convertValueToNeo4jFormat converts a single value to Neo4j HTTP format.
// Handles storage.Node, storage.Edge, maps, and slices recursively.
func (s *Server) convertValueToNeo4jFormat(val interface{}) interface{} {
	if val == nil {
		return nil
	}

	switch v := val.(type) {
	case *storage.Node:
		return s.nodeToNeo4jHTTPFormat(v)
	case *storage.Edge:
		return s.edgeToNeo4jHTTPFormat(v)
	case map[string]interface{}:
		// Check if this is already a converted node (has elementId)
		if _, hasElementId := v["elementId"]; hasElementId {
			return v
		}
		// Check if this looks like a node map (has _nodeId or id + labels)
		if nodeId, hasNodeId := v["_nodeId"]; hasNodeId {
			return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
		}
		if nodeId, hasId := v["id"]; hasId {
			if _, hasLabels := v["labels"]; hasLabels {
				return s.mapNodeToNeo4jHTTPFormat(nodeId, v)
			}
		}
		// Regular map - convert nested values
		result := make(map[string]interface{}, len(v))
		for k, vv := range v {
			result[k] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	case []interface{}:
		result := make([]interface{}, len(v))
		for i, vv := range v {
			result[i] = s.convertValueToNeo4jFormat(vv)
		}
		return result
	default:
		return val
	}
}

// nodeToNeo4jHTTPFormat converts a storage.Node to Neo4j HTTP API format.
// Neo4j format: {"elementId": "4:db:id", "labels": [...], "properties": {...}}
func (s *Server) nodeToNeo4jHTTPFormat(node *storage.Node) map[string]interface{} {
	if node == nil {
		return nil
	}

	elementId := fmt.Sprintf("4:nornicdb:%s", node.ID)

	// Filter properties - remove only embedding arrays, keep metadata
	props := s.filterNodeProperties(node.Properties)

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     node.Labels,
		"properties": props,
	}
}

// mapNodeToNeo4jHTTPFormat converts a map representation to Neo4j HTTP format.
func (s *Server) mapNodeToNeo4jHTTPFormat(nodeId interface{}, m map[string]interface{}) map[string]interface{} {
	elementId := fmt.Sprintf("4:nornicdb:%v", nodeId)

	// Extract labels
	var labels []string
	if l, ok := m["labels"].([]string); ok {
		labels = l
	} else if l, ok := m["labels"].([]interface{}); ok {
		labels = make([]string, len(l))
		for i, v := range l {
			if s, ok := v.(string); ok {
				labels[i] = s
			}
		}
	}

	// Extract and filter properties
	var props map[string]interface{}
	if p, ok := m["properties"].(map[string]interface{}); ok {
		props = s.filterNodeProperties(p)
	} else {
		// Properties might be at top level - collect them
		props = make(map[string]interface{})
		for k, v := range m {
			// Skip metadata fields
			if k == "id" || k == "_nodeId" || k == "labels" || k == "properties" || k == "elementId" || k == "embedding" {
				continue
			}
			if !s.isEmbeddingArrayProperty(k, v) {
				props[k] = v
			}
		}
	}

	return map[string]interface{}{
		"elementId":  elementId,
		"labels":     labels,
		"properties": props,
	}
}

// edgeToNeo4jHTTPFormat converts a storage.Edge to Neo4j HTTP API format.
func (s *Server) edgeToNeo4jHTTPFormat(edge *storage.Edge) map[string]interface{} {
	if edge == nil {
		return nil
	}

	elementId := fmt.Sprintf("5:nornicdb:%s", edge.ID)
	startElementId := fmt.Sprintf("4:nornicdb:%s", edge.StartNode)
	endElementId := fmt.Sprintf("4:nornicdb:%s", edge.EndNode)

	return map[string]interface{}{
		"elementId":          elementId,
		"type":               edge.Type,
		"startNodeElementId": startElementId,
		"endNodeElementId":   endElementId,
		"properties":         edge.Properties,
	}
}

// filterNodeProperties filters out embedding arrays but keeps embedding metadata.
// Removes: embedding (array), embeddings, vector, vectors, chunk_embedding, etc.
// Keeps: embedding_model, embedding_dimensions, has_embedding, embedded_at
func (s *Server) filterNodeProperties(props map[string]interface{}) map[string]interface{} {
	if props == nil {
		return make(map[string]interface{})
	}

	filtered := make(map[string]interface{}, len(props))
	for k, v := range props {
		if s.isEmbeddingArrayProperty(k, v) {
			continue
		}
		filtered[k] = v
	}
	return filtered
}

// isEmbeddingArrayProperty returns true if this property is an embedding array
// that should be filtered from HTTP responses (too large to serialize).
// Returns false for embedding metadata like embedding_model, has_embedding, etc.
func (s *Server) isEmbeddingArrayProperty(key string, value interface{}) bool {
	lowerKey := strings.ToLower(key)

	// These are the actual embedding vector properties that contain large arrays
	arrayProps := map[string]bool{
		"embedding":        true,
		"embeddings":       true,
		"vector":           true,
		"vectors":          true,
		"_embedding":       true,
		"_embeddings":      true,
		"chunk_embedding":  true,
		"chunk_embeddings": true,
	}

	if !arrayProps[lowerKey] {
		return false
	}

	// Only filter if the value is actually an array/slice (not metadata)
	if value == nil {
		return false
	}

	// Check if it's a slice/array type
	rv := reflect.ValueOf(value)
	kind := rv.Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// generateRowMeta generates Neo4j-compatible metadata for each value in a row.
// Neo4j meta format: {"id": 123, "type": "node", "deleted": false, "elementId": "4:db:id"}
func (s *Server) generateRowMeta(row []interface{}) []interface{} {
	meta := make([]interface{}, len(row))
	for i, val := range row {
		switch v := val.(type) {
		case map[string]interface{}:
			// Check for elementId (Neo4j format node/edge)
			if elementId, ok := v["elementId"].(string); ok {
				// Determine if it's a node or relationship based on elementId prefix
				entityType := "node"
				if strings.HasPrefix(elementId, "5:") {
					entityType = "relationship"
				}
				// Extract numeric ID from elementId (4:nornicdb:uuid -> hash to int)
				idPart := strings.TrimPrefix(elementId, "4:nornicdb:")
				idPart = strings.TrimPrefix(idPart, "5:nornicdb:")
				numericId := s.hashStringToInt64(idPart)

				meta[i] = map[string]interface{}{
					"id":        numericId,
					"type":      entityType,
					"deleted":   false,
					"elementId": elementId,
				}
			} else {
				meta[i] = nil
			}
		default:
			meta[i] = nil
		}
	}
	return meta
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
// NornicDB implements a simplified transaction model where each request is
// treated as an implicit transaction. Explicit multi-request transactions
// are supported through the transaction ID endpoints, but each statement
// within a transaction is executed immediately (no deferred commit).
//
// This design provides:
//   - Simplicity: No complex transaction state management
//   - Performance: No transaction overhead for single-request operations
//   - Compatibility: Works with Neo4j drivers that expect transaction support
//
// Future enhancements may include:
//   - Deferred commit for explicit transactions
//   - Transaction isolation levels
//   - Long-running transaction support

func (s *Server) handleOpenTransaction(w http.ResponseWriter, r *http.Request, dbName string) {
	// Generate transaction ID
	txID := fmt.Sprintf("%d", time.Now().UnixNano())

	host := s.config.Address
	if host == "0.0.0.0" {
		host = "localhost"
	}

	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional body

	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
		Commit:  fmt.Sprintf("http://%s:%d/db/%s/tx/%s/commit", host, s.config.Port, dbName, txID),
		Transaction: &TransactionInfo{
			Expires: time.Now().Add(30 * time.Second).Format(time.RFC1123),
		},
	}

	// Get executor for the specified database
	executor, err := s.getExecutorForDatabase(dbName)
	if err != nil {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Database.DatabaseNotFound",
			Message: fmt.Sprintf("Database '%s' not found: %v", dbName, err),
		})
		s.writeJSON(w, http.StatusNotFound, response)
		return
	}

	// Execute any provided statements
	if len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			result, err := executor.Execute(r.Context(), stmt.Statement, stmt.Parameters)
			if err != nil {
				response.Errors = append(response.Errors, QueryError{
					Code:    "Neo.ClientError.Statement.SyntaxError",
					Message: err.Error(),
				})
				continue
			}

			qr := QueryResult{
				Columns: result.Columns,
				Data:    make([]ResultRow, len(result.Rows)),
			}
			for i, row := range result.Rows {
				convertedRow := s.convertRowToNeo4jFormat(row)
				qr.Data[i] = ResultRow{Row: convertedRow, Meta: s.generateRowMeta(convertedRow)}
			}
			response.Results = append(response.Results, qr)
		}
	}

	// For transaction open, 201 Created is correct (creating transaction resource)
	// But if mutations are included with async writes, add consistency header
	if s.db.IsAsyncWritesEnabled() && len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, http.StatusCreated, response)
}

func (s *Server) handleExecuteInTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Execute statements in open transaction
	// For simplified implementation, treat as immediate execution
	s.handleImplicitTransaction(w, r, dbName)
}

func (s *Server) handleCommitTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	var req TransactionRequest
	_ = s.readJSON(r, &req) // Optional final statements

	response := TransactionResponse{
		Results:       make([]QueryResult, 0),
		Errors:        make([]QueryError, 0),
		LastBookmarks: []string{s.generateBookmark()},
	}

	// Get executor for the specified database
	executor, err := s.getExecutorForDatabase(dbName)
	if err != nil {
		response.Errors = append(response.Errors, QueryError{
			Code:    "Neo.ClientError.Database.DatabaseNotFound",
			Message: fmt.Sprintf("Database '%s' not found: %v", dbName, err),
		})
		s.writeJSON(w, http.StatusNotFound, response)
		return
	}

	// Execute any final statements
	for _, stmt := range req.Statements {
		result, err := executor.Execute(r.Context(), stmt.Statement, stmt.Parameters)
		if err != nil {
			response.Errors = append(response.Errors, QueryError{
				Code:    "Neo.ClientError.Statement.SyntaxError",
				Message: err.Error(),
			})
			continue
		}

		qr := QueryResult{
			Columns: result.Columns,
			Data:    make([]ResultRow, len(result.Rows)),
		}
		for i, row := range result.Rows {
			convertedRow := s.convertRowToNeo4jFormat(row)
			qr.Data[i] = ResultRow{Row: convertedRow, Meta: s.generateRowMeta(convertedRow)}
		}
		response.Results = append(response.Results, qr)
	}

	// For commits with async writes and mutations, use 202 Accepted
	status := http.StatusOK
	if s.db.IsAsyncWritesEnabled() && len(req.Statements) > 0 {
		for _, stmt := range req.Statements {
			if isMutationQuery(stmt.Statement) {
				status = http.StatusAccepted
				w.Header().Set("X-NornicDB-Consistency", "eventual")
				break
			}
		}
	}

	s.writeJSON(w, status, response)
}

func (s *Server) handleRollbackTransaction(w http.ResponseWriter, r *http.Request, dbName, txID string) {
	// Rollback transaction (for simplified implementation, just acknowledge)
	response := TransactionResponse{
		Results: make([]QueryResult, 0),
		Errors:  make([]QueryError, 0),
	}
	s.writeJSON(w, http.StatusOK, response)
}
