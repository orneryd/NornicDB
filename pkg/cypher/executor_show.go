package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// ===== SHOW Commands (Neo4j compatibility) =====

// executeShowIndexes handles SHOW INDEXES command
func (e *StorageExecutor) executeShowIndexes(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, fmt.Errorf("Neo.ClientError.Statement.NotAllowed: " +
			"SHOW INDEXES on composite databases requires a constituent target. " +
			"Use USE <composite>.<alias> SHOW INDEXES")
	}
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	indexTypeFilter := ""
	switch {
	case strings.HasPrefix(upper, "SHOW FULLTEXT INDEX"):
		indexTypeFilter = "FULLTEXT"
	case strings.HasPrefix(upper, "SHOW RANGE INDEX"):
		indexTypeFilter = "RANGE"
	case strings.HasPrefix(upper, "SHOW VECTOR INDEX"):
		indexTypeFilter = "VECTOR"
	}
	if schema != nil {
		indexes := schema.GetIndexes()
		rows = make([][]interface{}, 0, len(indexes))
		for i, idx := range indexes {
			idxMap, ok := idx.(map[string]interface{})
			if !ok {
				continue
			}

			name := idxMap["name"]
			idxType := idxMap["type"]
			if indexTypeFilter != "" && !strings.EqualFold(fmt.Sprintf("%v", idxType), indexTypeFilter) {
				continue
			}

			var labelsOrTypes interface{} = []string{}
			var properties interface{} = []string{}
			if l, ok := idxMap["label"].(string); ok && l != "" {
				labelsOrTypes = []string{l}
			} else if ls, ok := idxMap["labels"]; ok {
				labelsOrTypes = ls
			}
			if p, ok := idxMap["property"].(string); ok && p != "" {
				properties = []string{p}
			} else if ps, ok := idxMap["properties"]; ok {
				properties = ps
			}

			// Determine entity type (default NODE)
			entityType := "NODE"
			if et, ok := idxMap["entityType"].(string); ok && et != "" {
				entityType = et
			}

			// Determine owning constraint (nil if standalone)
			var owningConstraint interface{}
			if oc, ok := idxMap["owningConstraint"].(string); ok && oc != "" {
				owningConstraint = oc
			}

			rows = append(rows, []interface{}{
				int64(i + 1),      // id
				name,              // name
				"ONLINE",          // state
				100.0,             // populationPercent
				idxType,           // type
				entityType,        // entityType
				labelsOrTypes,     // labelsOrTypes
				properties,        // properties
				"nornicdb+schema", // indexProvider
				owningConstraint,  // owningConstraint
				nil,               // lastRead
				int64(0),          // readCount
			})
		}
	}

	return &ExecuteResult{
		Columns: []string{"id", "name", "state", "populationPercent", "type", "entityType", "labelsOrTypes", "properties", "indexProvider", "owningConstraint", "lastRead", "readCount"},
		Rows:    rows,
	}, nil
}

// executeShowConstraints handles SHOW CONSTRAINTS command
func (e *StorageExecutor) executeShowConstraints(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if isCompositeRoot(e.storage) {
		return nil, fmt.Errorf("Neo.ClientError.Statement.NotAllowed: " +
			"SHOW CONSTRAINTS on composite databases requires a constituent target. " +
			"Use USE <composite>.<alias> SHOW CONSTRAINTS")
	}
	if isShowConstraintContractsCommand(cypher) {
		return e.executeShowConstraintContracts(ctx)
	}
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}

	if schema != nil {
		constraints := schema.GetAllConstraints()
		for i, constraint := range constraints {
			var ownedIndex interface{}
			if constraint.OwnedIndex != "" {
				ownedIndex = constraint.OwnedIndex
			}
			// Direction and maxCount for cardinality constraints.
			var direction, maxCount interface{}
			if constraint.Type == storage.ConstraintCardinality {
				direction = constraint.Direction
				maxCount = int64(constraint.MaxCount)
			}
			// Source/target labels and policy mode for policy constraints.
			var sourceLabel, targetLabel, policyMode interface{}
			if constraint.Type == storage.ConstraintPolicy {
				sourceLabel = constraint.SourceLabel
				targetLabel = constraint.TargetLabel
				policyMode = constraint.PolicyMode
			}
			rows = append(rows, []interface{}{
				int64(i + 1),
				constraint.Name,
				string(constraint.Type),
				string(constraint.EffectiveEntityType()),
				[]string{constraint.Label},
				constraint.Properties,
				ownedIndex,
				nil,
				direction,
				maxCount,
				sourceLabel,
				targetLabel,
				policyMode,
			})
		}

		offset := len(rows)
		for i, constraint := range schema.GetAllPropertyTypeConstraints() {
			rows = append(rows, []interface{}{
				int64(offset + i + 1),
				constraint.Name,
				string(storage.ConstraintPropertyType),
				string(constraint.EffectiveEntityType()),
				[]string{constraint.Label},
				[]string{constraint.Property},
				nil,
				string(constraint.ExpectedType),
				nil, nil, nil, nil, nil,
			})
		}
	}

	return &ExecuteResult{
		Columns: []string{"id", "name", "type", "entityType", "labelsOrTypes", "properties", "ownedIndex", "propertyType", "direction", "maxCount", "sourceLabel", "targetLabel", "policyMode"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) executeShowConstraintContracts(ctx context.Context) (*ExecuteResult, error) {
	schema := e.storage.GetSchema()
	rows := [][]interface{}{}
	if schema != nil {
		contracts := schema.GetAllConstraintContracts()
		for _, contract := range contracts {
			compiledCount := int64(0)
			runtimeCount := int64(0)
			for _, entry := range contract.Entries {
				if strings.HasPrefix(entry.Kind, "primitive-") {
					compiledCount++
				} else {
					runtimeCount++
				}
			}
			rows = append(rows, []interface{}{
				contract.Name,
				contract.TargetEntityType,
				contract.TargetLabelOrType,
				int64(len(contract.Entries)),
				compiledCount,
				runtimeCount,
				contract.Definition,
			})
		}
	}
	return &ExecuteResult{
		Columns: []string{"name", "targetEntityType", "targetLabelOrType", "entryCount", "compiledEntryCount", "runtimeEntryCount", "definition"},
		Rows:    rows,
	}, nil
}

// executeShowProcedures handles SHOW PROCEDURES command
func (e *StorageExecutor) executeShowProcedures(ctx context.Context, cypher string) (*ExecuteResult, error) {
	ensureBuiltInProceduresRegistered()
	registered := ListRegisteredProcedures()
	procedures := make([][]interface{}, 0, len(registered))
	for _, p := range registered {
		procedures = append(procedures, []interface{}{p.Name, p.Signature, p.Description, string(p.Mode), p.WorksOnSystem})
	}

	return &ExecuteResult{
		Columns: []string{"name", "signature", "description", "mode", "worksOnSystem"},
		Rows:    procedures,
	}, nil
}

// executeShowFunctions handles SHOW FUNCTIONS command
func (e *StorageExecutor) executeShowFunctions(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Return list of available functions
	functions := [][]interface{}{
		// Scalar functions
		{"id", "id(entity :: ANY) :: INTEGER", "Returns the id of a node or relationship", false, false, false},
		{"elementId", "elementId(entity :: ANY) :: STRING", "Returns the element id of a node or relationship", false, false, false},
		{"labels", "labels(node :: NODE) :: LIST<STRING>", "Returns labels of a node", false, false, false},
		{"type", "type(relationship :: RELATIONSHIP) :: STRING", "Returns the type of a relationship", false, false, false},
		{"keys", "keys(entity :: ANY) :: LIST<STRING>", "Returns the property keys of a node or relationship", false, false, false},
		{"properties", "properties(entity :: ANY) :: MAP", "Returns all properties of a node or relationship", false, false, false},
		{"coalesce", "coalesce(expression :: ANY...) :: ANY", "Returns first non-null value", false, false, false},
		{"head", "head(list :: LIST<ANY>) :: ANY", "Returns the first element of a list", false, false, false},
		{"last", "last(list :: LIST<ANY>) :: ANY", "Returns the last element of a list", false, false, false},
		{"tail", "tail(list :: LIST<ANY>) :: LIST<ANY>", "Returns all but the first element of a list", false, false, false},
		{"size", "size(list :: LIST<ANY>) :: INTEGER", "Returns the number of elements in a list", false, false, false},
		{"length", "length(path :: PATH) :: INTEGER", "Returns the length of a path", false, false, false},
		{"reverse", "reverse(original :: LIST<ANY> | STRING) :: LIST<ANY> | STRING", "Reverses a list or string", false, false, false},
		{"range", "range(start :: INTEGER, end :: INTEGER, step :: INTEGER = 1) :: LIST<INTEGER>", "Returns a list of integers", false, false, false},
		{"toString", "toString(expression :: ANY) :: STRING", "Converts expression to string", false, false, false},
		{"toInteger", "toInteger(expression :: ANY) :: INTEGER", "Converts expression to integer", false, false, false},
		{"toFloat", "toFloat(expression :: ANY) :: FLOAT", "Converts expression to float", false, false, false},
		{"toBoolean", "toBoolean(expression :: ANY) :: BOOLEAN", "Converts expression to boolean", false, false, false},
		{"toLower", "toLower(original :: STRING) :: STRING", "Converts string to lowercase", false, false, false},
		{"toUpper", "toUpper(original :: STRING) :: STRING", "Converts string to uppercase", false, false, false},
		{"trim", "trim(original :: STRING) :: STRING", "Trims whitespace from string", false, false, false},
		{"ltrim", "ltrim(original :: STRING) :: STRING", "Trims leading whitespace", false, false, false},
		{"rtrim", "rtrim(original :: STRING) :: STRING", "Trims trailing whitespace", false, false, false},
		{"replace", "replace(original :: STRING, search :: STRING, replace :: STRING) :: STRING", "Replaces all occurrences", false, false, false},
		{"split", "split(original :: STRING, splitDelimiter :: STRING) :: LIST<STRING>", "Splits string by delimiter", false, false, false},
		{"substring", "substring(original :: STRING, start :: INTEGER, length :: INTEGER = NULL) :: STRING", "Returns substring", false, false, false},
		{"left", "left(original :: STRING, length :: INTEGER) :: STRING", "Returns left part of string", false, false, false},
		{"right", "right(original :: STRING, length :: INTEGER) :: STRING", "Returns right part of string", false, false, false},
		// Math functions
		{"abs", "abs(expression :: NUMBER) :: NUMBER", "Returns absolute value", false, false, false},
		{"ceil", "ceil(expression :: FLOAT) :: INTEGER", "Returns ceiling value", false, false, false},
		{"floor", "floor(expression :: FLOAT) :: INTEGER", "Returns floor value", false, false, false},
		{"round", "round(expression :: FLOAT) :: INTEGER", "Rounds to nearest integer", false, false, false},
		{"sign", "sign(expression :: NUMBER) :: INTEGER", "Returns sign of number", false, false, false},
		{"sqrt", "sqrt(expression :: FLOAT) :: FLOAT", "Returns square root", false, false, false},
		{"rand", "rand() :: FLOAT", "Returns random float between 0 and 1", false, false, false},
		{"randomUUID", "randomUUID() :: STRING", "Returns a random UUID", false, false, false},
		{"sin", "sin(expression :: FLOAT) :: FLOAT", "Returns sine", false, false, false},
		{"cos", "cos(expression :: FLOAT) :: FLOAT", "Returns cosine", false, false, false},
		{"tan", "tan(expression :: FLOAT) :: FLOAT", "Returns tangent", false, false, false},
		{"log", "log(expression :: FLOAT) :: FLOAT", "Returns natural logarithm", false, false, false},
		{"log10", "log10(expression :: FLOAT) :: FLOAT", "Returns base-10 logarithm", false, false, false},
		{"exp", "exp(expression :: FLOAT) :: FLOAT", "Returns e raised to power", false, false, false},
		{"pi", "pi() :: FLOAT", "Returns pi constant", false, false, false},
		{"e", "e() :: FLOAT", "Returns Euler's number", false, false, false},
		// Temporal functions
		{"timestamp", "timestamp() :: INTEGER", "Returns current timestamp in milliseconds", false, false, false},
		{"datetime", "datetime(input :: ANY = NULL) :: DATETIME", "Creates a datetime", false, false, false},
		{"date", "date(input :: ANY = NULL) :: DATE", "Creates a date", false, false, false},
		{"time", "time(input :: ANY = NULL) :: TIME", "Creates a time", false, false, false},
		// Aggregation functions
		{"count", "count(expression :: ANY) :: INTEGER", "Returns count", true, false, false},
		{"sum", "sum(expression :: NUMBER) :: NUMBER", "Returns sum", true, false, false},
		{"avg", "avg(expression :: NUMBER) :: FLOAT", "Returns average", true, false, false},
		{"min", "min(expression :: ANY) :: ANY", "Returns minimum", true, false, false},
		{"max", "max(expression :: ANY) :: ANY", "Returns maximum", true, false, false},
		{"collect", "collect(expression :: ANY) :: LIST<ANY>", "Collects values into list", true, false, false},
		// Predicate functions
		{"exists", "exists(expression :: ANY) :: BOOLEAN", "Returns true if expression is not null", false, false, false},
		{"isEmpty", "isEmpty(list :: LIST<ANY> | MAP | STRING) :: BOOLEAN", "Returns true if empty", false, false, false},
		{"all", "all(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if all match", false, false, false},
		{"any", "any(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if any match", false, false, false},
		{"none", "none(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if none match", false, false, false},
		{"single", "single(variable IN list WHERE predicate) :: BOOLEAN", "Returns true if exactly one matches", false, false, false},
		// Spatial functions
		{"point", "point(input :: MAP) :: POINT", "Creates a point", false, false, false},
		{"distance", "distance(point1 :: POINT, point2 :: POINT) :: FLOAT", "Returns distance between points", false, false, false},
		{"polygon", "polygon(points :: LIST<POINT>) :: POLYGON", "Creates a polygon from a list of points", false, false, false},
		{"lineString", "lineString(points :: LIST<POINT>) :: LINESTRING", "Creates a lineString from a list of points", false, false, false},
		{"point.intersects", "point.intersects(point :: POINT, polygon :: POLYGON) :: BOOLEAN", "Checks if point intersects with polygon", false, false, false},
		{"point.contains", "point.contains(polygon :: POLYGON, point :: POINT) :: BOOLEAN", "Checks if polygon contains point", false, false, false},
		// Vector functions
		{"vector.similarity.cosine", "vector.similarity.cosine(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Cosine similarity", false, false, false},
		{"vector.similarity.euclidean", "vector.similarity.euclidean(vector1 :: LIST<FLOAT>, vector2 :: LIST<FLOAT>) :: FLOAT", "Euclidean similarity", false, false, false},
		// Kalman filter functions
		{"kalman.init", "kalman.init(config? :: MAP) :: STRING", "Create new Kalman filter state (basic scalar filter for noise smoothing)", false, false, false},
		{"kalman.process", "kalman.process(measurement :: FLOAT, state :: STRING, target? :: FLOAT) :: MAP", "Process measurement, returns {value, state}", false, false, false},
		{"kalman.predict", "kalman.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict state n steps into the future", false, false, false},
		{"kalman.state", "kalman.state(state :: STRING) :: FLOAT", "Get current state estimate from state JSON", false, false, false},
		{"kalman.reset", "kalman.reset(state :: STRING) :: STRING", "Reset filter state to initial values", false, false, false},
		{"kalman.velocity.init", "kalman.velocity.init(initialPos? :: FLOAT, initialVel? :: FLOAT) :: STRING", "Create 2-state Kalman filter (position + velocity for trend tracking)", false, false, false},
		{"kalman.velocity.process", "kalman.velocity.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, velocity, state}", false, false, false},
		{"kalman.velocity.predict", "kalman.velocity.predict(state :: STRING, steps :: INTEGER) :: FLOAT", "Predict position n steps into the future", false, false, false},
		{"kalman.adaptive.init", "kalman.adaptive.init(config? :: MAP) :: STRING", "Create adaptive Kalman filter (auto-switches between basic and velocity modes)", false, false, false},
		{"kalman.adaptive.process", "kalman.adaptive.process(measurement :: FLOAT, state :: STRING) :: MAP", "Process measurement, returns {value, mode, state}", false, false, false},
	}

	return &ExecuteResult{
		Columns: []string{"name", "signature", "description", "aggregating", "isBuiltIn", "argumentDescription"},
		Rows:    functions,
	}, nil
}

// executeShowDatabase handles SHOW DATABASE command (singular - shows current database)
func (e *StorageExecutor) executeShowDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	nodeCount, _ := e.storage.NodeCount()
	edgeCount, _ := e.storage.EdgeCount()

	// Try to get database name from context or use default
	dbName := "nornic" // Default fallback

	// Priority 1: Check context for :USE database command
	if useDB := GetUseDatabaseFromContext(ctx); useDB != "" {
		dbName = useDB
	} else if e.dbManager != nil {
		// Priority 2: Try to infer database name from storage (if it's a NamespacedEngine)
		if namespacedEngine, ok := e.storage.(interface{ Namespace() string }); ok {
			if namespace := namespacedEngine.Namespace(); namespace != "" {
				dbName = namespace
			}
		}
		// Priority 3: Use default database from dbManager
		// Note: DatabaseManagerInterface doesn't expose DefaultDatabaseName,
		// so we can't call it directly. The NamespacedEngine namespace should
		// already be set correctly by the server layer.
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"},
		Rows: [][]interface{}{
			{dbName, "standard", "read-write", "localhost:7687", "primary", true, "online", "online", "", true, true, []string{}},
		},
		Stats: &QueryStats{
			NodesCreated:         int(nodeCount),
			RelationshipsCreated: int(edgeCount),
		},
	}, nil
}

// executeShowDatabases handles SHOW DATABASES command (plural - lists all databases).
//
// Returns a list of all databases with their metadata including name, type, status,
// and whether they are the default database. This command requires DatabaseManager
// to be set via SetDatabaseManager().
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//	result, err := executor.Execute(ctx, "SHOW DATABASES", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//	for _, row := range result.Rows {
//		// emit "Database: %s (type: %s, status: %s)" for row[0], row[1], row[7]
//	}
//
// Returns Neo4j-compatible format with columns:
//   - name: Database name
//   - type: Database type (standard, system)
//   - access: Access mode (read-write)
//   - address: Server address
//   - role: Server role (primary)
//   - writer: Whether writes are allowed
//   - requestedStatus: Requested status
//   - currentStatus: Current status (online, offline)
//   - statusMessage: Status message
//   - default: Whether this is the default database
//   - home: Whether this is the home database
//   - constituents: Constituent databases (empty for single databases)
func (e *StorageExecutor) executeShowDatabases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW DATABASES requires multi-database support")
	}

	databases := e.dbManager.ListDatabases()
	rows := make([][]interface{}, 0, len(databases))

	for _, db := range databases {
		rows = append(rows, []interface{}{
			db.Name(),
			db.Type(),
			"read-write",
			"localhost:7687",
			"primary",
			true,
			db.Status(),
			db.Status(),
			"",
			db.IsDefault(),
			db.IsDefault(),
			[]string{},
		})
	}

	return &ExecuteResult{
		Columns: []string{"name", "type", "access", "address", "role", "writer", "requestedStatus", "currentStatus", "statusMessage", "default", "home", "constituents"},
		Rows:    rows,
	}, nil
}

// executeCreateDatabase handles CREATE DATABASE command.
//
// Creates a new database with the specified name. Supports optional IF NOT EXISTS
// clause to avoid errors when the database already exists. This command requires
// DatabaseManager to be set via SetDatabaseManager().
//
// Syntax:
//   - CREATE DATABASE name
//   - CREATE DATABASE name IF NOT EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Create database
//	result, err := executor.Execute(ctx, "CREATE DATABASE tenant_a", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Create with IF NOT EXISTS (idempotent)
//	result, err = executor.Execute(ctx, "CREATE DATABASE tenant_a IF NOT EXISTS", nil)
//
// Returns:
//   - Success: Result with database name in single row
//   - Error: If database already exists (unless IF NOT EXISTS is used)
//   - Error: If DatabaseManager is not configured
func (e *StorageExecutor) executeCreateDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	e.logger().Debug("executeCreateDatabase invoked",
		"subsystem", "create_database",
		"cypher", cypher)
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - CREATE DATABASE requires multi-database support")
	}

	// Find "CREATE DATABASE" keyword position (with flexible whitespace)
	createDbIdx := findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE")
	if createDbIdx == -1 {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax")
	}

	// Skip "CREATE" and whitespace to find "DATABASE"
	startPos := createDbIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax: database name expected")
	}

	// Find end of database name (whitespace, end of string, or "IF NOT EXISTS")
	// Check for "IF NOT EXISTS" first (with flexible whitespace)
	// This is a 3-word keyword: IF NOT EXISTS
	ifNotIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "NOT")
	var ifNotExistsIdx int = -1
	if ifNotIdx >= 0 {
		// Found "IF NOT" - check if "EXISTS" follows
		afterIfNot := startPos + ifNotIdx + len("IF")
		// Skip whitespace after "IF"
		for afterIfNot < len(cypher) && isWhitespace(cypher[afterIfNot]) {
			afterIfNot++
		}
		// Check for "NOT"
		if afterIfNot+len("NOT") <= len(cypher) && strings.EqualFold(cypher[afterIfNot:afterIfNot+len("NOT")], "NOT") {
			afterNot := afterIfNot + len("NOT")
			// Skip whitespace after "NOT"
			for afterNot < len(cypher) && isWhitespace(cypher[afterNot]) {
				afterNot++
			}
			// Check for "EXISTS"
			if afterNot+len("EXISTS") <= len(cypher) && strings.EqualFold(cypher[afterNot:afterNot+len("EXISTS")], "EXISTS") {
				// Found "IF NOT EXISTS" - database name ends before "IF"
				ifNotExistsIdx = ifNotIdx
			}
		}
	}
	var dbNameEnd int
	if ifNotExistsIdx >= 0 {
		// Database name ends before "IF NOT EXISTS"
		dbNameEnd = startPos + ifNotExistsIdx
	} else {
		// No IF NOT EXISTS - database name goes to end of query
		dbNameEnd = len(cypher)
	}

	// Extract database name (trim whitespace)
	dbName, err := unquoteBacktickIdentifier(strings.TrimSpace(cypher[startPos:dbNameEnd]))
	if err != nil {
		return nil, err
	}
	if dbName == "" {
		return nil, fmt.Errorf("invalid CREATE DATABASE syntax: database name cannot be empty")
	}

	// Validate database name (basic validation)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, fmt.Errorf("invalid database name: '%s' (cannot contain whitespace)", dbName)
	}

	// Check if already exists
	if e.dbManager.Exists(dbName) {
		if ifNotExistsIdx >= 0 {
			// IF NOT EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"name"},
				Rows:    [][]interface{}{{dbName}},
			}, nil
		}
		return nil, fmt.Errorf("database '%s' already exists", dbName)
	}

	// Create database
	err = e.dbManager.CreateDatabase(dbName)
	if err != nil {
		e.logger().Error("CreateDatabase failed",
			"subsystem", "create_database",
			"database", dbName,
			"error", err)
		return nil, fmt.Errorf("failed to create database '%s': %w", dbName, err)
	}
	e.logger().Info("CreateDatabase succeeded",
		"subsystem", "create_database",
		"database", dbName)

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeDropDatabase handles DROP DATABASE command.
//
// Deletes a database and all its data. Supports optional IF EXISTS clause to
// avoid errors when the database doesn't exist. This command requires
// DatabaseManager to be set via SetDatabaseManager().
//
// Syntax:
//   - DROP DATABASE name
//   - DROP DATABASE name IF EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Drop database
//	result, err := executor.Execute(ctx, "DROP DATABASE tenant_a", nil)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Drop with IF EXISTS (idempotent)
//	result, err = executor.Execute(ctx, "DROP DATABASE tenant_a IF EXISTS", nil)
//
// Warning: This operation permanently deletes all data in the database.
// The default and system databases cannot be dropped.
//
// Returns:
//   - Success: Result with database name in single row (empty if IF EXISTS and not found)
//   - Error: If database doesn't exist (unless IF EXISTS is used)
//   - Error: If DatabaseManager is not configured
func (e *StorageExecutor) executeDropDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - DROP DATABASE requires multi-database support")
	}

	// Find "DROP DATABASE" keyword position (with flexible whitespace)
	dropDbIdx := findMultiWordKeywordIndex(cypher, "DROP", "DATABASE")
	if dropDbIdx == -1 {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax")
	}

	// Skip "DROP" and whitespace to find "DATABASE"
	startPos := dropDbIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax: database name expected")
	}

	// Find end of database name (whitespace, end of string, or "IF EXISTS")
	// Check for "IF EXISTS" first (with flexible whitespace)
	ifExistsIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "EXISTS")
	var dbNameEnd int
	if ifExistsIdx >= 0 {
		dbNameEnd = startPos + ifExistsIdx
		ifExistsIdx = startPos + ifExistsIdx // Absolute position
	} else {
		// No IF EXISTS - database name goes to end of query
		dbNameEnd = len(cypher)
	}

	// Extract database name (trim whitespace)
	dbName, err := unquoteBacktickIdentifier(strings.TrimSpace(cypher[startPos:dbNameEnd]))
	if err != nil {
		return nil, err
	}
	if dbName == "" {
		return nil, fmt.Errorf("invalid DROP DATABASE syntax: database name cannot be empty")
	}

	// Validate database name (basic validation)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, fmt.Errorf("invalid database name: '%s' (cannot contain whitespace)", dbName)
	}

	// Check if exists
	if !e.dbManager.Exists(dbName) {
		if ifExistsIdx >= 0 {
			// IF EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"name"},
				Rows:    [][]interface{}{},
			}, nil
		}
		return nil, fmt.Errorf("database '%s' does not exist", dbName)
	}

	// Drop database
	err = e.dbManager.DropDatabase(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop database '%s': %w", dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"name"},
		Rows:    [][]interface{}{{dbName}},
	}, nil
}

// executeCreateAlias handles CREATE ALIAS command (Neo4j-compatible).
//
// Creates an alias for a database. Aliases allow referencing databases with
// alternative names, useful for database renaming, environment mapping, etc.
//
// Syntax:
//   - CREATE ALIAS alias_name FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Create alias
//	result, err := executor.Execute(ctx, "CREATE ALIAS main FOR DATABASE tenant_primary_2024", nil)
//
// Returns:
//   - Success: Result with alias name in single row
//   - Error: If alias already exists or database doesn't exist
func (e *StorageExecutor) executeCreateAlias(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - CREATE ALIAS requires multi-database support")
	}

	// Find "CREATE ALIAS" keyword position
	createAliasIdx := findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS")
	if createAliasIdx == -1 {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax")
	}

	// Skip "CREATE" and whitespace to find "ALIAS"
	startPos := createAliasIdx + len("CREATE")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIAS" and whitespace
	if startPos+len("ALIAS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIAS")], "ALIAS") {
		startPos += len("ALIAS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax: alias name expected")
	}

	// Find "FOR DATABASE" to separate alias name from database name
	forIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
	if forIdx == -1 {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax: FOR DATABASE expected")
	}

	// Extract alias name (trim all whitespace)
	aliasName := strings.TrimSpace(cypher[startPos : startPos+forIdx])
	// Remove all whitespace from alias name (handle cases where whitespace variations
	// might have left whitespace in the extracted name)
	aliasName = strings.ReplaceAll(aliasName, " ", "")
	aliasName = strings.ReplaceAll(aliasName, "\t", "")
	aliasName = strings.ReplaceAll(aliasName, "\n", "")
	aliasName = strings.ReplaceAll(aliasName, "\r", "")

	if aliasName == "" {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax: alias name cannot be empty")
	}

	// Validate alias name (should not contain whitespace after cleaning)
	if strings.ContainsAny(aliasName, " \t\n\r") {
		return nil, fmt.Errorf("invalid alias name: '%s' (cannot contain whitespace)", aliasName)
	}

	// Skip "FOR" and whitespace
	dbStartPos := startPos + forIdx + len("FOR")
	for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
		dbStartPos++
	}
	// Skip "DATABASE" and whitespace
	if dbStartPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbStartPos:dbStartPos+len("DATABASE")], "DATABASE") {
		dbStartPos += len("DATABASE")
		for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
			dbStartPos++
		}
	}

	if dbStartPos >= len(cypher) {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax: database name expected")
	}

	// Extract database name (rest of query, trim all whitespace)
	dbName := strings.TrimSpace(cypher[dbStartPos:])
	// Remove all whitespace from database name
	dbName = strings.ReplaceAll(dbName, " ", "")
	dbName = strings.ReplaceAll(dbName, "\t", "")
	dbName = strings.ReplaceAll(dbName, "\n", "")
	dbName = strings.ReplaceAll(dbName, "\r", "")

	if dbName == "" {
		return nil, fmt.Errorf("invalid CREATE ALIAS syntax: database name cannot be empty")
	}

	// Validate database name (should not contain whitespace after cleaning)
	if strings.ContainsAny(dbName, " \t\n\r") {
		return nil, fmt.Errorf("invalid database name: '%s' (cannot contain whitespace)", dbName)
	}

	// Create alias
	err := e.dbManager.CreateAlias(aliasName, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to create alias '%s' for database '%s': %w", aliasName, dbName, err)
	}

	return &ExecuteResult{
		Columns: []string{"alias"},
		Rows:    [][]interface{}{{aliasName}},
	}, nil
}

// executeDropAlias handles DROP ALIAS command (Neo4j-compatible).
//
// Removes an alias for a database.
//
// Syntax:
//   - DROP ALIAS alias_name
//   - DROP ALIAS alias_name IF EXISTS
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Drop alias
//	result, err := executor.Execute(ctx, "DROP ALIAS main", nil)
//
// Returns:
//   - Success: Result with alias name in single row (empty if IF EXISTS and not found)
//   - Error: If alias doesn't exist (unless IF EXISTS is used)
func (e *StorageExecutor) executeDropAlias(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - DROP ALIAS requires multi-database support")
	}

	// Find "DROP ALIAS" keyword position
	dropAliasIdx := findMultiWordKeywordIndex(cypher, "DROP", "ALIAS")
	if dropAliasIdx == -1 {
		return nil, fmt.Errorf("invalid DROP ALIAS syntax")
	}

	// Skip "DROP" and whitespace to find "ALIAS"
	startPos := dropAliasIdx + len("DROP")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIAS" and whitespace
	if startPos+len("ALIAS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIAS")], "ALIAS") {
		startPos += len("ALIAS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	if startPos >= len(cypher) {
		return nil, fmt.Errorf("invalid DROP ALIAS syntax: alias name expected")
	}

	// Find end of alias name (whitespace, end of string, or "IF EXISTS")
	ifExistsIdx := findMultiWordKeywordIndex(cypher[startPos:], "IF", "EXISTS")
	var aliasNameEnd int
	if ifExistsIdx >= 0 {
		aliasNameEnd = startPos + ifExistsIdx
	} else {
		aliasNameEnd = len(cypher)
	}

	// Extract alias name (trim all whitespace)
	aliasName := strings.TrimSpace(cypher[startPos:aliasNameEnd])
	// Remove all whitespace from alias name
	aliasName = strings.ReplaceAll(aliasName, " ", "")
	aliasName = strings.ReplaceAll(aliasName, "\t", "")
	aliasName = strings.ReplaceAll(aliasName, "\n", "")
	aliasName = strings.ReplaceAll(aliasName, "\r", "")

	if aliasName == "" {
		return nil, fmt.Errorf("invalid DROP ALIAS syntax: alias name cannot be empty")
	}

	// Validate alias name (should not contain whitespace after cleaning)
	if strings.ContainsAny(aliasName, " \t\n\r") {
		return nil, fmt.Errorf("invalid alias name: '%s' (cannot contain whitespace)", aliasName)
	}

	// Check if alias exists (by checking if it resolves)
	_, err := e.dbManager.ResolveDatabase(aliasName)
	if err != nil {
		if ifExistsIdx >= 0 {
			// IF EXISTS - return success with no error
			return &ExecuteResult{
				Columns: []string{"alias"},
				Rows:    [][]interface{}{},
			}, nil
		}
		return nil, fmt.Errorf("alias '%s' does not exist", aliasName)
	}

	// Drop alias
	err = e.dbManager.DropAlias(aliasName)
	if err != nil {
		return nil, fmt.Errorf("failed to drop alias '%s': %w", aliasName, err)
	}

	return &ExecuteResult{
		Columns: []string{"alias"},
		Rows:    [][]interface{}{{aliasName}},
	}, nil
}

// executeShowAliases handles SHOW ALIASES command (Neo4j-compatible).
//
// Lists all database aliases, optionally filtered by database.
//
// Syntax:
//   - SHOW ALIASES
//   - SHOW ALIASES FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// List all aliases
//	result, err := executor.Execute(ctx, "SHOW ALIASES", nil)
//
//	// List aliases for specific database
//	result, err = executor.Execute(ctx, "SHOW ALIASES FOR DATABASE tenant_a", nil)
//
// Returns:
//   - Success: Result with alias and database columns
func (e *StorageExecutor) executeShowAliases(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW ALIASES requires multi-database support")
	}

	// Find "SHOW ALIASES" keyword position
	showAliasesIdx := findMultiWordKeywordIndex(cypher, "SHOW", "ALIASES")
	if showAliasesIdx == -1 {
		return nil, fmt.Errorf("invalid SHOW ALIASES syntax")
	}

	// Skip "SHOW" and whitespace to find "ALIASES"
	startPos := showAliasesIdx + len("SHOW")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "ALIASES" and whitespace
	if startPos+len("ALIASES") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("ALIASES")], "ALIASES") {
		startPos += len("ALIASES")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	}

	// Check for "FOR DATABASE" clause
	var databaseName string
	if startPos < len(cypher) {
		forIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
		if forIdx >= 0 {
			// Extract database name
			dbStartPos := startPos + forIdx + len("FOR")
			for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
				dbStartPos++
			}
			// Skip "DATABASE" and whitespace
			if dbStartPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbStartPos:dbStartPos+len("DATABASE")], "DATABASE") {
				dbStartPos += len("DATABASE")
				for dbStartPos < len(cypher) && isWhitespace(cypher[dbStartPos]) {
					dbStartPos++
				}
			}
			if dbStartPos < len(cypher) {
				databaseName = strings.TrimSpace(cypher[dbStartPos:])
			}
		}
	}

	// List aliases
	aliases := e.dbManager.ListAliases(databaseName)

	// Format results
	rows := make([][]interface{}, 0, len(aliases))
	for alias, dbName := range aliases {
		rows = append(rows, []interface{}{alias, dbName})
	}

	return &ExecuteResult{
		Columns: []string{"alias", "database"},
		Rows:    rows,
	}, nil
}

// executeAlterDatabase handles ALTER DATABASE SET LIMIT command.
//
// Sets resource limits for a database. Supports setting individual limits
// or multiple limits in a single command.
//
// Syntax:
//   - ALTER DATABASE database_name SET LIMIT limit_name = value
//   - ALTER DATABASE database_name SET LIMIT limit_name1 = value1, limit_name2 = value2
//
// Supported limit names:
//   - max_nodes: Maximum number of nodes (int64)
//   - max_edges: Maximum number of edges (int64)
//   - max_bytes: Maximum storage size in bytes (int64)
//   - max_query_time: Maximum query execution time (duration string, e.g., "60s", "5m")
//   - max_results: Maximum number of query results (int64)
//   - max_concurrent_queries: Maximum concurrent queries (int)
//   - max_connections: Maximum connections (int)
//   - max_queries_per_second: Maximum queries per second (int)
//   - max_writes_per_second: Maximum writes per second (int)
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Set max nodes limit
//	result, err := executor.Execute(ctx, "ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000", nil)
//
//	// Set multiple limits
//	result, err = executor.Execute(ctx, "ALTER DATABASE tenant_a SET LIMIT max_nodes = 1000000, max_edges = 5000000", nil)
//
// Returns:
//   - Success: Result with database name
//   - Error: If database doesn't exist or syntax is invalid
func (e *StorageExecutor) executeAlterDatabase(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - ALTER DATABASE requires multi-database support")
	}

	// Find "ALTER DATABASE" keyword position
	alterDbIdx := findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE")
	if alterDbIdx == -1 {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax")
	}

	// Skip "ALTER" and whitespace to find "DATABASE"
	startPos := alterDbIdx + len("ALTER")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "DATABASE" and whitespace
	if startPos+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("DATABASE")], "DATABASE") {
		startPos += len("DATABASE")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax: DATABASE keyword expected")
	}

	// Find "SET LIMIT" keyword
	setLimitIdx := findMultiWordKeywordIndex(cypher[startPos:], "SET", "LIMIT")
	if setLimitIdx == -1 {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax: SET LIMIT clause expected")
	}

	// Extract database name (between "DATABASE" and "SET LIMIT")
	dbNameEnd := startPos + setLimitIdx
	databaseName := strings.TrimSpace(cypher[startPos:dbNameEnd])
	if databaseName == "" {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax: database name expected")
	}

	// Skip "SET LIMIT" and whitespace
	limitStartPos := startPos + setLimitIdx + len("SET")
	for limitStartPos < len(cypher) && isWhitespace(cypher[limitStartPos]) {
		limitStartPos++
	}
	if limitStartPos+len("LIMIT") <= len(cypher) && strings.EqualFold(cypher[limitStartPos:limitStartPos+len("LIMIT")], "LIMIT") {
		limitStartPos += len("LIMIT")
		for limitStartPos < len(cypher) && isWhitespace(cypher[limitStartPos]) {
			limitStartPos++
		}
	} else {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax: LIMIT keyword expected")
	}

	// Get existing limits or create new ones
	existingLimitsInterface, err := e.dbManager.GetDatabaseLimits(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", databaseName, err)
	}

	// Type assert to *multidb.Limits
	var existingLimits *multidb.Limits
	if existingLimitsInterface != nil {
		var ok bool
		existingLimits, ok = existingLimitsInterface.(*multidb.Limits)
		if !ok {
			return nil, fmt.Errorf("invalid limits type returned from database manager")
		}
	}

	// Create new limits if none exist, otherwise deep copy
	var limits *multidb.Limits
	if existingLimits == nil {
		limits = &multidb.Limits{}
	} else {
		// Deep copy to avoid modifying the original
		limits = &multidb.Limits{
			Storage: multidb.StorageLimits{
				MaxNodes: existingLimits.Storage.MaxNodes,
				MaxEdges: existingLimits.Storage.MaxEdges,
				MaxBytes: existingLimits.Storage.MaxBytes,
			},
			Query: multidb.QueryLimits{
				MaxQueryTime:         existingLimits.Query.MaxQueryTime,
				MaxResults:           existingLimits.Query.MaxResults,
				MaxConcurrentQueries: existingLimits.Query.MaxConcurrentQueries,
			},
			Connection: multidb.ConnectionLimits{
				MaxConnections: existingLimits.Connection.MaxConnections,
			},
			Rate: multidb.RateLimits{
				MaxQueriesPerSecond: existingLimits.Rate.MaxQueriesPerSecond,
				MaxWritesPerSecond:  existingLimits.Rate.MaxWritesPerSecond,
			},
		}
	}

	// Parse limit assignments: "limit_name = value, limit_name2 = value2"
	limitClause := strings.TrimSpace(cypher[limitStartPos:])
	if limitClause == "" {
		return nil, fmt.Errorf("invalid ALTER DATABASE syntax: limit assignment expected")
	}

	// Split by comma to handle multiple limits
	assignments := strings.Split(limitClause, ",")
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if assignment == "" {
			continue
		}

		// Parse "limit_name = value"
		parts := strings.SplitN(assignment, "=", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid limit assignment syntax: expected 'limit_name = value', got '%s'", assignment)
		}

		limitName := strings.TrimSpace(strings.ToLower(parts[0]))
		limitValue := strings.TrimSpace(parts[1])

		// Parse and set the limit based on name
		switch limitName {
		case "max_nodes":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid max_nodes value: %w", err)
			}
			limits.Storage.MaxNodes = val

		case "max_edges":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid max_edges value: %w", err)
			}
			limits.Storage.MaxEdges = val

		case "max_bytes":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid max_bytes value: %w", err)
			}
			limits.Storage.MaxBytes = val

		case "max_query_time":
			duration, err := time.ParseDuration(limitValue)
			if err != nil {
				return nil, fmt.Errorf("invalid max_query_time value (expected duration like '60s' or '5m'): %w", err)
			}
			limits.Query.MaxQueryTime = duration

		case "max_results":
			val, err := strconv.ParseInt(limitValue, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid max_results value: %w", err)
			}
			limits.Query.MaxResults = val

		case "max_concurrent_queries":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, fmt.Errorf("invalid max_concurrent_queries value: %w", err)
			}
			limits.Query.MaxConcurrentQueries = val

		case "max_connections":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, fmt.Errorf("invalid max_connections value: %w", err)
			}
			limits.Connection.MaxConnections = val

		case "max_queries_per_second":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, fmt.Errorf("invalid max_queries_per_second value: %w", err)
			}
			limits.Rate.MaxQueriesPerSecond = val

		case "max_writes_per_second":
			val, err := strconv.Atoi(limitValue)
			if err != nil {
				return nil, fmt.Errorf("invalid max_writes_per_second value: %w", err)
			}
			limits.Rate.MaxWritesPerSecond = val

		default:
			return nil, fmt.Errorf("unknown limit name: '%s' (supported: max_nodes, max_edges, max_bytes, max_query_time, max_results, max_concurrent_queries, max_connections, max_queries_per_second, max_writes_per_second)", limitName)
		}
	}

	// Update limits in database manager (pass as interface{} to match interface)
	err = e.dbManager.SetDatabaseLimits(databaseName, limits)
	if err != nil {
		return nil, fmt.Errorf("failed to set limits for database '%s': %w", databaseName, err)
	}

	return &ExecuteResult{
		Columns: []string{"database"},
		Rows:    [][]interface{}{{databaseName}},
	}, nil
}

// executeShowLimits handles SHOW LIMITS command.
//
// Lists resource limits for a database in Neo4j-compatible format.
//
// Syntax:
//   - SHOW LIMITS FOR DATABASE database_name
//
// Example:
//
//	executor := cypher.NewStorageExecutor(storage)
//	executor.SetDatabaseManager(dbManager)
//
//	// Show limits for database
//	result, err := executor.Execute(ctx, "SHOW LIMITS FOR DATABASE tenant_a", nil)
//
// Returns:
//   - Success: Result with limit information in Neo4j-compatible format
//   - Error: If database doesn't exist
func (e *StorageExecutor) executeShowLimits(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if e.dbManager == nil {
		return nil, fmt.Errorf("database manager not available - SHOW LIMITS requires multi-database support")
	}

	// Find "SHOW LIMITS" keyword position
	showLimitsIdx := findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS")
	if showLimitsIdx == -1 {
		return nil, fmt.Errorf("invalid SHOW LIMITS syntax")
	}

	// Skip "SHOW" and whitespace to find "LIMITS"
	startPos := showLimitsIdx + len("SHOW")
	for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
		startPos++
	}
	// Skip "LIMITS" and whitespace
	if startPos+len("LIMITS") <= len(cypher) && strings.EqualFold(cypher[startPos:startPos+len("LIMITS")], "LIMITS") {
		startPos += len("LIMITS")
		for startPos < len(cypher) && isWhitespace(cypher[startPos]) {
			startPos++
		}
	} else {
		return nil, fmt.Errorf("invalid SHOW LIMITS syntax: LIMITS keyword expected")
	}

	// Check for "FOR DATABASE" clause
	forDbIdx := findMultiWordKeywordIndex(cypher[startPos:], "FOR", "DATABASE")
	if forDbIdx == -1 {
		return nil, fmt.Errorf("invalid SHOW LIMITS syntax: FOR DATABASE clause expected")
	}

	// Skip "FOR" and whitespace
	dbNameStart := startPos + forDbIdx + len("FOR")
	for dbNameStart < len(cypher) && isWhitespace(cypher[dbNameStart]) {
		dbNameStart++
	}
	// Skip "DATABASE" and whitespace
	if dbNameStart+len("DATABASE") <= len(cypher) && strings.EqualFold(cypher[dbNameStart:dbNameStart+len("DATABASE")], "DATABASE") {
		dbNameStart += len("DATABASE")
		for dbNameStart < len(cypher) && isWhitespace(cypher[dbNameStart]) {
			dbNameStart++
		}
	} else {
		return nil, fmt.Errorf("invalid SHOW LIMITS syntax: DATABASE keyword expected")
	}

	// Extract database name (rest of query)
	databaseName := strings.TrimSpace(cypher[dbNameStart:])
	if databaseName == "" {
		return nil, fmt.Errorf("invalid SHOW LIMITS syntax: database name expected")
	}

	// Get limits from database manager
	limitsInterface, err := e.dbManager.GetDatabaseLimits(databaseName)
	if err != nil {
		return nil, fmt.Errorf("database '%s' not found: %w", databaseName, err)
	}

	// Type assert to *multidb.Limits
	var limits *multidb.Limits
	if limitsInterface != nil {
		var ok bool
		limits, ok = limitsInterface.(*multidb.Limits)
		if !ok {
			return nil, fmt.Errorf("invalid limits type returned from database manager")
		}
	}

	// If no limits set, return empty/unlimited values
	if limits == nil {
		limits = &multidb.Limits{}
	}

	// Format limits in Neo4j-compatible format
	// Neo4j returns: name, type, value, description
	rows := make([][]interface{}, 0)

	// Storage limits
	if limits.Storage.MaxNodes > 0 {
		rows = append(rows, []interface{}{databaseName, "max_nodes", limits.Storage.MaxNodes, "Maximum number of nodes"})
	}
	if limits.Storage.MaxEdges > 0 {
		rows = append(rows, []interface{}{databaseName, "max_edges", limits.Storage.MaxEdges, "Maximum number of edges"})
	}
	if limits.Storage.MaxBytes > 0 {
		rows = append(rows, []interface{}{databaseName, "max_bytes", limits.Storage.MaxBytes, "Maximum storage size in bytes"})
	}

	// Query limits
	if limits.Query.MaxQueryTime > 0 {
		rows = append(rows, []interface{}{databaseName, "max_query_time", limits.Query.MaxQueryTime.String(), "Maximum query execution time"})
	}
	if limits.Query.MaxResults > 0 {
		rows = append(rows, []interface{}{databaseName, "max_results", limits.Query.MaxResults, "Maximum number of query results"})
	}
	if limits.Query.MaxConcurrentQueries > 0 {
		rows = append(rows, []interface{}{databaseName, "max_concurrent_queries", limits.Query.MaxConcurrentQueries, "Maximum concurrent queries"})
	}

	// Connection limits
	if limits.Connection.MaxConnections > 0 {
		rows = append(rows, []interface{}{databaseName, "max_connections", limits.Connection.MaxConnections, "Maximum concurrent connections"})
	}

	// Rate limits
	if limits.Rate.MaxQueriesPerSecond > 0 {
		rows = append(rows, []interface{}{databaseName, "max_queries_per_second", limits.Rate.MaxQueriesPerSecond, "Maximum queries per second"})
	}
	if limits.Rate.MaxWritesPerSecond > 0 {
		rows = append(rows, []interface{}{databaseName, "max_writes_per_second", limits.Rate.MaxWritesPerSecond, "Maximum writes per second"})
	}

	// If no limits are set, return a single row indicating unlimited
	if len(rows) == 0 {
		rows = append(rows, []interface{}{databaseName, "unlimited", nil, "No limits configured (unlimited)"})
	}

	return &ExecuteResult{
		Columns: []string{"database", "limit", "value", "description"},
		Rows:    rows,
	}, nil
}

// truncateQuery truncates a query string to maxLen characters for error messages
func truncateQuery(query string, maxLen int) string {
	query = strings.TrimSpace(query)
	if len(query) <= maxLen {
		return query
	}
	return query[:maxLen] + "..."
}
