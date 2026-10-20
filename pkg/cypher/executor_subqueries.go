package cypher

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(cypher string) bool {
	if q := fabric.ReadQueryStructure(cypher); q.Err == nil {
		for _, c := range q.Clauses {
			if c.Kind == "CALL" && c.Nested != nil {
				return true
			}
		}
		return false
	}
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(cypher, callSubqueryRe)
}

// startsWithCallSubquery reports whether cypher begins with a CALL { } or
// CALL (vars) { } subquery clause (as opposed to a procedure CALL, or a
// subquery later in the statement).
func startsWithCallSubquery(cypher string) bool {
	if q := fabric.ReadQueryStructure(cypher); q.Err == nil {
		return q.Starts("CALL") && q.Clauses[0].Nested != nil
	}
	return callSubqueryAt(strings.TrimSpace(cypher), 0)
}

func appendCorrelatedBinding(result *ExecuteResult, name string, value interface{}) *ExecuteResult {
	if result == nil || name == "" {
		return result
	}
	for _, column := range result.Columns {
		if strings.EqualFold(strings.TrimSpace(column), name) {
			return result
		}
	}
	result.Columns = append(result.Columns, name)
	for index := range result.Rows {
		result.Rows[index] = append(result.Rows[index], value)
	}
	return result
}

func (e *StorageExecutor) resolveCorrelatedImportValue(ctx context.Context, outerPart, seedVar, seedID, importVar string, cache map[string]map[string]interface{}) (interface{}, bool, error) {
	if cache != nil {
		if byVar, ok := cache[seedID]; ok {
			if val, exists := byVar[importVar]; exists {
				return val, true, nil
			}
		}
	}

	outerQuery := strings.TrimSpace(outerPart) + " RETURN " + seedVar + ", " + importVar
	outerResult, err := e.executeInternal(ctx, outerQuery, nil)
	if err != nil {
		return nil, false, localizedError(localization.CypherSubqueriesCorrelatedImportFailed(importVar, err), err)
	}

	seedCol := -1
	importCol := -1
	for i, c := range outerResult.Columns {
		if strings.EqualFold(strings.TrimSpace(c), seedVar) {
			seedCol = i
		}
		if strings.EqualFold(strings.TrimSpace(c), importVar) {
			importCol = i
		}
	}
	if seedCol == -1 || importCol == -1 {
		return nil, false, nil
	}

	for _, row := range outerResult.Rows {
		if seedCol >= len(row) {
			continue
		}
		if !rowMatchesSeedID(row[seedCol], seedID) {
			continue
		}
		var val interface{}
		if importCol < len(row) {
			val = row[importCol]
		}
		if cache != nil {
			byVar, ok := cache[seedID]
			if !ok {
				byVar = make(map[string]interface{}, 4)
				cache[seedID] = byVar
			}
			byVar[importVar] = val
		}
		return val, true, nil
	}
	// Fallback for OPTIONAL MATCH imports: resolve the imported variable using
	// a seed-scoped OPTIONAL MATCH query. This preserves correlated semantics for
	// patterns like:
	//   MATCH (o) ... OPTIONAL MATCH (o)-[:R]->(t:Label {...}) WITH o,t CALL { ... }
	// where `t` can be null/non-null per seed.
	if !strings.EqualFold(importVar, seedVar) {
		if optional := fabric.ReadQueryStructure(outerPart).First("OPTIONAL MATCH"); optional != nil {
			if optional.Body != "" {
				requiredPart := "MATCH " + optional.Body
				seedScoped := fmt.Sprintf("MATCH (%s) WHERE id(%s) = %s %s RETURN %s", seedVar, seedVar, quoteCypherStringLiteral(seedID), requiredPart, importVar)
				fallbackRes, fallbackErr := e.executeInternal(ctx, seedScoped, nil)
				if fallbackErr != nil {
					return nil, false, localizedError(localization.CypherSubqueriesOptionalImportFallbackFailed(importVar, fallbackErr), fallbackErr)
				}
				if fallbackRes != nil && len(fallbackRes.Rows) > 0 {
					importCol := -1
					for i, c := range fallbackRes.Columns {
						if strings.EqualFold(strings.TrimSpace(c), importVar) {
							importCol = i
							break
						}
					}
					if importCol < 0 && len(fallbackRes.Columns) == 1 {
						importCol = 0
					}
					if importCol >= 0 && importCol < len(fallbackRes.Rows[0]) {
						val := fallbackRes.Rows[0][importCol]
						if cache != nil {
							byVar, ok := cache[seedID]
							if !ok {
								byVar = make(map[string]interface{}, 4)
								cache[seedID] = byVar
							}
							byVar[importVar] = val
						}
						return val, true, nil
					}
				}
			}
		}
	}

	return nil, false, nil
}

func rowMatchesSeedID(value interface{}, seedID string) bool {
	switch v := value.(type) {
	case *storage.Node:
		return v != nil && string(v.ID) == seedID
	case storage.Node:
		return string(v.ID) == seedID
	case map[string]interface{}:
		for _, key := range []string{"_nodeId", "id", "_id", "elementId"} {
			if raw, ok := v[key]; ok {
				if s, ok := raw.(string); ok && s != "" {
					if s == seedID {
						return true
					}
					if last := strings.LastIndex(s, ":"); last >= 0 && last+1 < len(s) && s[last+1:] == seedID {
						return true
					}
				}
			}
		}
	case string:
		if v == seedID {
			return true
		}
		if last := strings.LastIndex(v, ":"); last >= 0 && last+1 < len(v) && v[last+1:] == seedID {
			return true
		}
	}
	return false
}

func (e *StorageExecutor) normalizeUnionBranchColumns(query string, result *ExecuteResult) {
	if result == nil || len(result.Columns) > 0 {
		return
	}
	if inferred := e.inferTopLevelReturnColumns(query); len(inferred) > 0 {
		result.Columns = inferred
		return
	}
	if inferred := e.inferExplainColumns(query); len(inferred) > 0 {
		result.Columns = inferred
	}
}

// isCallSubqueryPureReturn checks whether innerBody is a pure RETURN projection
// with no MATCH/CALL/UNWIND/write clauses. Precomputed once per branch to avoid
// repeated keyword scans on every seed row.
func isCallSubqueryPureReturn(innerBody string) bool {
	q := fabric.ReadQueryStructure(innerBody)
	return q.Starts("RETURN") && len(q.Clauses) == 1
}

func (e *StorageExecutor) tryExecuteCorrelatedReturnProjection(ctx context.Context, seedRow []interface{}, seedCols []string, withVars []string, innerBody string) (*ExecuteResult, bool, error) {
	return e.tryExecuteCorrelatedReturnProjectionPreChecked(ctx, seedRow, seedCols, withVars, innerBody, false)
}

// tryExecuteCorrelatedReturnProjectionPreChecked is the same as tryExecuteCorrelatedReturnProjection
// but accepts a precomputed isPureReturn flag to skip redundant keyword scanning.
func (e *StorageExecutor) tryExecuteCorrelatedReturnProjectionPreChecked(ctx context.Context, seedRow []interface{}, seedCols []string, withVars []string, innerBody string, isPureReturn bool) (*ExecuteResult, bool, error) {
	if !isPureReturn {
		// Caller didn't precompute — do the check now.
		if !isCallSubqueryPureReturn(innerBody) {
			return nil, false, nil
		}
	}
	trimmed := strings.TrimSpace(innerBody)

	colIdx := make(map[string]int, len(seedCols))
	for i, c := range seedCols {
		colIdx[c] = i
	}

	row := make([]interface{}, 0, len(withVars))
	cols := make([]string, 0, len(withVars))
	for _, v := range withVars {
		idx, ok := colIdx[v]
		if !ok || idx < 0 || idx >= len(seedRow) {
			return nil, true, localizedError(localization.CypherSubqueriesWithImportUnknownVariable(v), nil)
		}
		cols = append(cols, v)
		row = append(row, seedRow[idx])
	}
	seedOnly := &ExecuteResult{
		Columns: cols,
		Rows:    [][]interface{}{row},
	}
	res, err := e.processCallSubqueryReturn(ctx, seedOnly, trimmed)
	if err != nil {
		return nil, true, err
	}
	return res, true, nil
}

// seedNodesFromOuterMatch executes the outer MATCH/WHERE segment through the normal
// execution pipeline (instead of manual scan/filter) so index/hot-path optimizations
// apply before correlated CALL {} expansion.
func (e *StorageExecutor) seedNodesFromOuterMatch(ctx context.Context, outerPart, variable string) ([]*storage.Node, error) {
	variable = strings.TrimSpace(variable)
	if variable == "" {
		return nil, localizedError(localization.CypherSubqueriesCorrelatedVariableEmpty(), nil)
	}

	trimmedOuter := strings.TrimSpace(outerPart)
	structure := fabric.ReadQueryStructure(trimmedOuter)

	// Fast path for simple seeded MATCH queries without relationship patterns:
	//   MATCH (v[:Label] {props}) [WHERE ...]
	// including ID/property indexed WHERE predicates.
	if structure.Starts("MATCH") && len(structure.Clauses) == 1 &&
		!containsRelationshipPattern(trimmedOuter) {
		matchBody := structure.Clauses[0].Pattern
		whereClause := structure.Clauses[0].Predicate

		np, parseErr := e.parseNodePattern(ctx, matchBody)
		if parseErr != nil {
			return nil, parseErr
		}
		if np.variable != "" && !isIdentifierReferenced(trimmedOuter, variable) {
			return nil, nil
		}
		if strings.EqualFold(strings.TrimSpace(np.variable), variable) {
			if whereClause != "" {
				// O(1) direct ID seek path for: MATCH (v) WHERE id(v)=...
				if nodes, ok, err := e.tryCollectNodesFromIDEquality(ctx, np, whereClause); ok || err != nil {
					return nodes, err
				}
				// O(k) batched ID seek path for: MATCH (v) WHERE id(v) IN $ids
				if params := getParamsFromContext(ctx); params != nil {
					if nodes, ok, err := e.tryCollectNodesFromIDIn(ctx, np, whereClause, params); ok || err != nil {
						return nodes, err
					}
				}
				// Indexed IN-list path for batched correlated lookups.
				if params := getParamsFromContext(ctx); params != nil {
					if nodes, ok, err := e.tryCollectNodesFromPropertyIndexIn(np, whereClause, params); ok || err != nil {
						return nodes, err
					}
				}
				// Indexed equality path.
				if nodes, ok, err := e.tryCollectNodesFromPropertyIndex(ctx, np, whereClause); ok || err != nil {
					return nodes, err
				}
			}

			// Label/property-only fast path.
			nodes, err := e.loadPatternNodes(ctx, np.labels, np.properties)
			if err != nil {
				return nil, err
			}
			if len(np.properties) == 0 {
				// If a non-indexable WHERE exists, defer to generic executor to preserve semantics.
				if whereClause == "" {
					return nodes, nil
				}
				seedQuery := strings.TrimSpace(outerPart) + " RETURN " + variable
				outerRes, err := e.executeInternal(ctx, seedQuery, nil)
				if err != nil {
					return nil, err
				}
				return e.extractSeedNodesFromResult(outerRes, variable)
			}
			filtered := make([]*storage.Node, 0, len(nodes))
			for _, n := range nodes {
				if n == nil {
					continue
				}
				if e.nodeMatchesProps(n, np.properties) {
					filtered = append(filtered, n)
				}
			}
			// Same rule: preserve non-indexable WHERE semantics through generic executor.
			if whereClause != "" {
				seedQuery := strings.TrimSpace(outerPart) + " RETURN " + variable
				outerRes, err := e.executeInternal(ctx, seedQuery, nil)
				if err != nil {
					return nil, err
				}
				return e.extractSeedNodesFromResult(outerRes, variable)
			}
			return filtered, nil
		}
	}

	seedQuery := trimmedOuter
	if structure.First("RETURN") == nil {
		seedQuery += " RETURN " + variable
	}
	outerRes, err := e.executeInternal(ctx, seedQuery, nil)
	if err != nil {
		return nil, err
	}
	return e.extractSeedNodesFromResult(outerRes, variable)
}

func (e *StorageExecutor) extractSeedNodesFromResult(outerRes *ExecuteResult, variable string) ([]*storage.Node, error) {
	if outerRes == nil || len(outerRes.Rows) == 0 {
		return []*storage.Node{}, nil
	}

	colIdx := -1
	for i, col := range outerRes.Columns {
		if strings.EqualFold(strings.TrimSpace(col), variable) {
			colIdx = i
			break
		}
	}
	if colIdx < 0 {
		return nil, localizedError(localization.CypherSubqueriesOuterMatchProjectionMissing(variable), nil)
	}

	seedNodes := make([]*storage.Node, 0, len(outerRes.Rows))
	for _, row := range outerRes.Rows {
		if colIdx >= len(row) {
			continue
		}
		val := row[colIdx]
		switch v := val.(type) {
		case *storage.Node:
			if v != nil {
				seedNodes = append(seedNodes, v)
			}
		case map[string]interface{}:
			if n := e.seedNodeFromMap(v); n != nil {
				seedNodes = append(seedNodes, n)
			}
		case string:
			if n := e.seedNodeFromIDString(v); n != nil {
				seedNodes = append(seedNodes, n)
			}
		case []interface{}:
			// Defensive: some execution paths can wrap a single projected value.
			if len(v) == 1 {
				switch wrapped := v[0].(type) {
				case *storage.Node:
					if wrapped != nil {
						seedNodes = append(seedNodes, wrapped)
					}
				case map[string]interface{}:
					if n := e.seedNodeFromMap(wrapped); n != nil {
						seedNodes = append(seedNodes, n)
					}
				case string:
					if n := e.seedNodeFromIDString(wrapped); n != nil {
						seedNodes = append(seedNodes, n)
					}
				}
			}
		}
	}
	return seedNodes, nil
}

func (e *StorageExecutor) seedNodeFromMap(m map[string]interface{}) *storage.Node {
	for _, key := range []string{"_nodeId", "id", "_id", "elementId"} {
		if raw, ok := m[key]; ok {
			if id, ok := raw.(string); ok && id != "" {
				if n := e.seedNodeFromIDString(id); n != nil {
					return n
				}
			}
		}
	}
	return nil
}

func (e *StorageExecutor) seedNodeFromIDString(id string) *storage.Node {
	id = strings.TrimSpace(id)
	if id == "" {
		return nil
	}
	// Direct NodeID lookup first.
	if n, err := e.getQueryNode(e.storage, storage.NodeID(id)); err == nil && n != nil {
		return n
	}
	// Handle elementId-style values: "<prefix>:<db>:<id>" -> "<id>"
	if last := strings.LastIndex(id, ":"); last >= 0 && last+1 < len(id) {
		idTail := id[last+1:]
		if n, err := e.getQueryNode(e.storage, storage.NodeID(idTail)); err == nil && n != nil {
			return n
		}
	}
	return nil
}

// parseCallSubquery extracts the body from CALL { ... } and any trailing clauses
// Returns: body, afterCall, inTransactions bool, batchSize int
func (e *StorageExecutor) parseCallSubquery(cypher string) (body, afterCall string, inTransactions bool, batchSize int) {
	batchSize = 1000 // Default batch size
	if q := fabric.ReadQueryStructure(cypher); q.Starts("CALL") && q.Clauses[0].Nested != nil {
		c := q.Clauses[0]
		if size, err := strconv.Atoi(c.BatchSize); err == nil && size > 0 {
			batchSize = size
		}
		body = c.Body
		if stripped, finishes := stripUnionBranchFinishes(body); finishes && strings.TrimSpace(stripped) != "" {
			body = stripped
		}
		return body, c.Remaining, c.InTransactions, batchSize
	}

	trimmed := strings.TrimSpace(cypher)

	// Find the opening brace
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", "", false, batchSize
	}

	// The matching closing brace; a brace in quoted text isn't one
	// (CALL () { RETURN '}' AS y } …).
	braceEnd := findMatchingDelimiter(trimmed, braceStart, '{', '}')
	if braceEnd < 0 {
		return "", "", false, batchSize
	}

	// Extract body (between braces)
	body = strings.TrimSpace(trimmed[braceStart+1 : braceEnd])
	if stripped, finishes := stripUnionBranchFinishes(body); finishes && strings.TrimSpace(stripped) != "" {
		body = stripped
	}

	// Get what's after the closing brace
	afterCall = strings.TrimSpace(trimmed[braceEnd+1:])

	// Check for IN TRANSACTIONS
	upperAfter := upperASCII(afterCall)
	if strings.HasPrefix(upperAfter, "IN TRANSACTIONS") {
		inTransactions = true
		afterTx := strings.TrimSpace(afterCall[15:])
		upperAfterTx := upperASCII(afterTx)

		// Check for OF n ROWS
		if strings.HasPrefix(upperAfterTx, "OF ") {
			// Parse batch size
			ofPart := afterTx[3:]
			// Find ROWS keyword
			rowsIdx := strings.Index(upperASCII(ofPart), " ROWS")
			if rowsIdx > 0 {
				sizeStr := strings.TrimSpace(ofPart[:rowsIdx])
				if size, err := strconv.Atoi(sizeStr); err == nil && size > 0 {
					batchSize = size
				}
				afterCall = strings.TrimSpace(ofPart[rowsIdx+5:])
			} else {
				afterCall = ""
			}
		} else {
			afterCall = afterTx
		}
	}

	return body, afterCall, inTransactions, batchSize
}

// callSubqueryHasScopeClause reports whether a CALL subquery declares its
// imports with a scope clause (CALL () { … }, CALL (x, y) { … }, CALL (*)
// { … }). Its body's leading WITH is then an ordinary projection, never an
// import list (parseLeadingWithImports): CALL () { WITH 1 AS x FINISH } is a
// unit subquery whose body is just that WITH (#907).
func callSubqueryHasScopeClause(cypher string) bool {
	return readCallSubqueryScope(cypher).scoped
}

type callSubqueryScopeClause struct {
	names      []string
	scoped     bool
	text       string
	bodyOffset int
	invalid    bool
}

// readCallSubqueryScope is the shared static/runtime owner of CALL's scope
// clause. scoped distinguishes CALL (*) from legacy CALL { ... }; a nil name
// list means all outer variables, and a non-nil empty list means CALL ().
// text and bodyOffset let validation use the same parsed clause. Legacy
// importing-WITH and implicit imports are resolved by their existing owners.
func readCallSubqueryScope(cypher string) callSubqueryScopeClause {
	// Fabric's maintained grammar owns source structure. The engine's scope
	// contract supplies import facts to static and runtime consumers alike.
	if query := fabric.ReadQueryStructure(cypher); query.Err == nil {
		if !query.Starts("CALL") {
			return callSubqueryScopeClause{}
		}
		clause := query.Clauses[0]
		scope := callSubqueryScopeClause{scoped: clause.Scoped, text: clause.ScopeText, bodyOffset: clause.BodyOffset}
		if clause.Scoped {
			if clause.ScopeNames == nil {
				scope.text = "*"
			} else {
				scope.names = append([]string{}, clause.ScopeNames...)
			}
		}
		return scope
	}
	trimmed := strings.TrimSpace(cypher)
	if findKeywordIndex(trimmed, "CALL") != 0 {
		return callSubqueryScopeClause{}
	}
	idx := skipSpaces(trimmed, len("CALL"))
	offset := strings.Index(cypher, trimmed)
	scope := callSubqueryScopeClause{bodyOffset: offset + idx}
	if idx >= len(trimmed) || trimmed[idx] != '(' {
		return scope
	}
	close := findMatchingCallParen(trimmed, idx)
	if close < 0 {
		return scope
	}
	scope.scoped = true
	scope.text = strings.TrimSpace(trimmed[idx+1 : close])
	scope.bodyOffset = offset + skipSpaces(trimmed, close+1)
	if scope.text == "*" {
		return scope
	}
	parts := splitTopLevelComma(scope.text)
	scope.names = make([]string, 0, len(parts))
	for _, part := range parts {
		if simpleSemanticIdentifier(part) == "" && scope.text != "" {
			scope.invalid = true
		}
		name := strings.Trim(strings.TrimSpace(part), "`")
		if isSimpleIdentifier(name) {
			scope.names = append(scope.names, name)
		}
	}
	return scope
}

// prefixUnionBranches puts prefix before body, or before each branch of a
// UNION / UNION ALL body: every branch of a CALL subquery imports the same
// outer variables.
func prefixUnionBranches(prefix, body string) string {
	branches, unionAll, mixed, ok := parseTopLevelUnionBranches(body)
	if !ok || mixed || len(branches) < 2 {
		return prefix + " " + body
	}
	separator := " UNION "
	if unionAll {
		separator = " UNION ALL "
	}
	for i, branch := range branches {
		branches[i] = prefix + " " + branch
	}
	return strings.Join(branches, separator)
}

// callSeedIsFirstNode reports whether a MATCH … CALL { … } needs only the
// MATCH's first node from the outer row: the pattern is that one node pattern,
// and every variable the subquery imports (CALL (x, …) or a leading WITH x, …)
// is that node's variable.
func callSeedIsFirstNode(pattern, variable string, callImportVars []string, subqueryBody string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern[0] != '(' || findMatchingDelimiter(pattern, 0, '(', ')') != len(pattern)-1 {
		return false
	}
	imports := callImportVars
	if withVars, _, hasWith, err := parseLeadingWithImports(subqueryBody); err == nil && hasWith {
		imports = append(append([]string(nil), imports...), withVars...)
	}
	for _, imported := range imports {
		if !strings.EqualFold(strings.TrimSpace(imported), variable) && strings.TrimSpace(imported) != "*" {
			return false
		}
	}
	return true
}

func importsVariable(vars []string, variable string) bool {
	for _, v := range vars {
		if strings.EqualFold(strings.TrimSpace(v), strings.TrimSpace(variable)) {
			return true
		}
	}
	return false
}

// executeCallInTransactions executes a CALL {} IN TRANSACTIONS query
// This batches operations for large datasets by processing results in separate transactions.
//
// The subquery is executed in batches where each batch is processed in its own transaction.
// This is useful for large imports/updates to avoid memory issues and provide transaction boundaries.
//
// Example:
//
//	CALL {
//	  MATCH (p:Person)
//	  SET p.processed = true
//	  RETURN p.name AS name
//	} IN TRANSACTIONS OF 2 ROWS
//
// This will process Person nodes in batches of 2, each batch in a separate transaction.
//
// Strategy:
//  1. First execute the subquery to determine the total number of rows (read-only)
//  2. If it contains write operations, process in batches by adding LIMIT/SKIP to the MATCH
//  3. Each batch is executed in its own transaction via executeWithImplicitTransaction
//
// rejectCallInTransactionsInExplicitTx enforces the Neo4j rule that
// CALL { ... } IN TRANSACTIONS cannot run inside an explicit transaction:
// its batches would execute inside the caller's transaction and survive a
// ROLLBACK (#648).
func (e *StorageExecutor) rejectCallInTransactionsInExplicitTx() error {
	if e.txContext != nil && e.txContext.active {
		if tx, ok := e.txContext.tx.(*storage.BadgerTransaction); ok && tx.OperationCount() > 0 {
			return &classifiedCypherError{
				cause:  errors.New("Expected transaction state to be empty when calling transactional subquery. (Transactions committed: 0)"),
				code:   "Neo.DatabaseError.Statement.ExecutionFailed",
				detail: "InvalidCallInTransactions",
			}
		}
		return newSemanticError("Neo.DatabaseError.Transaction.TransactionStartFailed", "InvalidCallInTransactions",
			"CALL { ... } IN TRANSACTIONS is not allowed inside an explicit transaction")
	}
	return nil
}

// makeSubqueryReadOnly converts a subquery with writes to a read-only version for row counting.
// This is used to determine how many batches we need before executing the actual writes.
// Returns empty string if conversion is not possible.
//
// The count query is the subquery's MATCH with RETURN 1: one row per matched
// row, the rows the batches page through. The subquery's own RETURN can't
// be kept, since it reads variables the dropped write clause binds
// (MATCH (s) CREATE (t) RETURN t.value).
func (e *StorageExecutor) makeSubqueryReadOnly(subquery string) string {
	return fabric.ReadOnlyQueryPrefix(subquery)
}

// addLimitSkipToSubquery adds LIMIT and SKIP clauses to a subquery for batching.
// For queries with MATCH followed by write operations (SET, CREATE, DELETE, MERGE),
// it adds LIMIT/SKIP after the MATCH clause to limit how many rows are processed.
// For other patterns, it adds LIMIT/SKIP before RETURN.
//
// This ensures that batching limits the number of matched rows processed, not just
// the number of returned rows.
func (e *StorageExecutor) addLimitSkipToSubquery(subquery string, limit, skip int) string {
	return fabric.QueryWithBatchWindow(subquery, limit, skip)
}

func detectReferencedCallSubquerySeedColumns(seedResult *ExecuteResult, subqueryBody string) []string {
	if seedResult == nil || len(seedResult.Columns) == 0 {
		return nil
	}
	referenced := make([]string, 0, len(seedResult.Columns))
	for _, col := range seedResult.Columns {
		name := strings.TrimSpace(col)
		if name == "" {
			continue
		}
		if isIdentifierReferenced(subqueryBody, name) {
			referenced = append(referenced, name)
		}
	}
	return referenced
}

// withImportClauseKeywords are the clauses that can follow a subquery's
// importing WITH list (parseLeadingWithImports).
var withImportClauseKeywords = []string{"WHERE", "OPTIONAL MATCH", "MATCH", "UNWIND", "MERGE", "CREATE", "SET", "DETACH DELETE", "DELETE", "REMOVE", "CALL", "RETURN", "WITH"}

// leadingWithClause returns the leading WITH clause of text, which starts
// with WITH (its WHERE, ORDER BY, SKIP and LIMIT included), and whether a
// clause follows it. Unlike splitPipelineClauses it reads no further than
// the next clause, in one pass, and it takes texts with procedure calls
// (WITH p CALL db.x() …).
func leadingWithClause(text string) (clause string, followed bool) {
	q := fabric.ReadQueryStructure(text)
	if !q.Starts("WITH") {
		return text, false
	}
	return q.Clauses[0].Text, len(q.Clauses) > 1
}

// isLeadingWithFollower reports whether word starts a clause that can follow
// a leading WITH.
func isLeadingWithFollower(word string) bool {
	switch len(word) {
	case 3:
		return equalFoldASCII(word, "set") || equalFoldASCII(word, "use")
	case 4:
		return equalFoldASCII(word, "call") || equalFoldASCII(word, "with") || equalFoldASCII(word, "load")
	case 5:
		return equalFoldASCII(word, "match") || equalFoldASCII(word, "merge") || equalFoldASCII(word, "union")
	case 6:
		return equalFoldASCII(word, "unwind") || equalFoldASCII(word, "create") || equalFoldASCII(word, "insert") ||
			equalFoldASCII(word, "delete") || equalFoldASCII(word, "detach") || equalFoldASCII(word, "remove") ||
			equalFoldASCII(word, "return") || equalFoldASCII(word, "finish")
	case 7:
		return equalFoldASCII(word, "foreach")
	case 8:
		return equalFoldASCII(word, "optional") || equalFoldASCII(word, "nodetach")
	}
	return false
}

// unscopedCallImports reads the importing WITHs of an unscoped CALL
// subquery's body: whether any UNION branch (or the body, without UNION)
// starts with an importing WITH (branchLeadingWithImport), and the
// variables those WITHs list. A branch that imports nothing adds none,
// whatever the other branches import (#907); it still reads outer variables
// through NornicDB's implicit import (pipelineApplyCallSubqueryWithMetadata).
// A CALL with a scope clause has no importing WITH
// (callSubqueryHasScopeClause).
//
// names is filled only when collectNames.
func unscopedCallImports(subqueryBody string, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	trimmed := strings.TrimSpace(subqueryBody)
	if indexASCIIFold(trimmed, "with") < 0 {
		return nil, false, nil
	}
	if indexASCIIFold(trimmed, "union") >= 0 {
		if branches, _, _, union := parseTopLevelUnionBranches(trimmed); union {
			for _, branch := range branches {
				branchNames, branchImports, err := branchLeadingWithImport(branch, outer, collectNames)
				if err != nil {
					return nil, false, err
				}
				if branchImports {
					importing = true
					names = append(names, branchNames...)
				}
			}
			return names, importing, nil
		}
	}
	return branchLeadingWithImport(trimmed, outer, collectNames)
}

// branchLeadingWithImport reports whether one branch of an unscoped CALL
// body starts with an importing WITH: WITH * or a WITH that reads a
// variable from outside (outer), and the variables it lists (not *). Any
// other leading WITH (WITH 1 AS x, WITH 2 AS i over an outer i) is an
// ordinary projection inside the subquery (#907). An importing WITH must
// be followed by a query clause: CALL { WITH x } imports and is an error.
func branchLeadingWithImport(branch string, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	trimmed := strings.TrimSpace(fabric.SemanticQuery(branch))
	if !startsWithKeywordFold(trimmed, "WITH") {
		return nil, false, nil
	}
	clause, followed := leadingWithClause(trimmed)
	projection, _ := projectionSemanticBodyAndTail(clause, "WITH")
	var buffer [8]string
	for _, item := range appendTopLevelComma(buffer[:0], projection) {
		expression, alias := parseProjectionExprAlias(item)
		expression = strings.TrimSpace(expression)
		if expression == "*" {
			importing = true
			continue
		}
		if collectNames {
			// The item's name: the variable itself for a plain reference.
			names = append(names, alias)
		} else if importing {
			break
		}
		if importing || expression == "" {
			continue
		}
		// A plain reference (the usual import) needs no expression scan.
		if !isCypherBacktickByte(expression[0]) && isSimpleIdentifier(expression) {
			importing = outer(expression)
			continue
		}
		for _, variable := range expressionFreeVariables(expression) {
			if outer(variable) {
				importing = true
				break
			}
		}
	}
	if !importing {
		return nil, false, nil
	}
	if !followed {
		return names, true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}
	return names, true, nil
}

// unscopedBranchLocals returns the outer variables (names) that one branch
// of an unscoped CALL body declares itself (branchDeclaresVariable): the
// branch doesn't receive their outer values through NornicDB's implicit
// import, so they are new variables there, as in Cypher (#907).
func unscopedBranchLocals(branch string, names []string) []string {
	var locals []string
	for _, name := range names {
		if branchDeclaresVariable(branch, name) {
			locals = append(locals, name)
		}
	}
	return locals
}

// branchDeclaresVariable reports whether a branch of an unscoped CALL body
// declares variable before it reads it: its first use is a node or
// relationship variable of a MATCH, OPTIONAL MATCH, CREATE or MERGE pattern
// (or the pattern's path name), or the name UNWIND … AS or WITH … AS binds.
// Cypher gives such a branch a new variable; NornicDB's implicit import of an
// outer variable applies only to a name the branch reads first (#907).
func branchDeclaresVariable(branch, variable string) bool {
	return fabric.BranchDeclaresVariable(branch, variable)
}

// patternPartEnd is where a MATCH, CREATE or MERGE clause's pattern ends:
// at its WHERE, or MERGE's ON CREATE / ON MATCH, else the clause's end.

// firstVariableOccurrence is the index of the first use of variable in text
// as a variable: a whole word outside string literals, not a property key
// (m.variable), a parameter ($variable) or a map key ({variable: …}).
// -1 when there is none.

// isPatternVariableAt reports whether the word at index is a pattern's node
// or relationship variable ((n:L), (n), -[r:T]->) or its path name (p = …).

// wordBefore is the identifier that ends right before index (spaces
// skipped), "" when another character comes first.
func wordBefore(text string, index int) string {
	end := index
	for end > 0 && isASCIISpace(text[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	return text[start:end]
}

// isPatternClauseKeyword reports whether word is a keyword a pattern
// follows: MATCH, CREATE, MERGE.
func isPatternClauseKeyword(word string) bool {
	return equalFoldASCII(word, "MATCH") || equalFoldASCII(word, "CREATE") || equalFoldASCII(word, "MERGE")
}

// precededByAS reports whether the word at index follows the keyword AS.

// previousNonSpace is the last non-space byte before index, 0 at the start.
func previousNonSpace(text string, index int) byte {
	for index > 0 {
		index--
		if !isASCIISpace(text[index]) {
			return text[index]
		}
	}
	return 0
}

// nextNonSpace is the first non-space byte at or after index, 0 at the end.
func nextNonSpace(text string, index int) byte {
	for ; index < len(text); index++ {
		if !isASCIISpace(text[index]) {
			return text[index]
		}
	}
	return 0
}

func parseLeadingWithImports(subqueryBody string) (withVars []string, innerBody string, hasWith bool, err error) {
	q := fabric.ReadQueryStructure(subqueryBody)
	if !q.Starts("WITH") {
		return nil, strings.TrimSpace(subqueryBody), false, nil
	}
	if len(q.Clauses) < 2 {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}
	c := q.Clauses[0]
	p := c.Projection
	if p == nil {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithImportsRequired(), nil)
	}
	if p.Star {
		withVars = append(withVars, "*")
	}
	for _, item := range p.Items {
		withVars = append(withVars, item.Column)
	}
	if len(withVars) == 0 {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithImportsRequired(), nil)
	}
	innerBody = c.Remaining
	if c.Predicate != "" || p.Distinct || p.Modifiers != "" {
		innerBody = c.Text + " " + innerBody
	}
	return withVars, innerBody, true, nil
}

// splitLeadingWhereNullGuard detects a leading correlated guard shape:
//
//	WITH <imports> WHERE <expr> <write/query-clause...>
//
// and returns (<expr>, <rest-after-where-expr>, true).
// It is intentionally conservative and only used by the correlated write branch
// fallback path in executeMatchWithCallSubquery.
func splitLeadingWhereNullGuard(query string) (whereExpr string, rest string, ok bool) {
	return fabric.LeadingWhereAndTail(query)
}

// evalWhereNullGuard evaluates narrow guard expressions used in correlated UNION
// write branches:
//
//	<var> IS NULL
//	<var> IS NOT NULL
//
// Returns (pass, handled). handled=false means expression is outside this narrow
// supported subset and caller should use the generic path.
func evalWhereNullGuard(whereExpr string, vars map[string]interface{}) (pass bool, handled bool) {
	name, not, ok := fabric.ExpressionNullGuard(whereExpr)
	if !ok {
		return false, false
	}
	value, exists := vars[name]
	isNull := !exists || value == nil
	if not {
		return !isNull, true
	}
	return isNull, true
}

// splitTopLevelUnionBranches splits a query by top-level UNION/UNION ALL separators.
// Returns branches, unionAllMode, ok.
func splitTopLevelUnionBranches(query string) ([]string, bool, bool) {
	branches, unionAll, mixed, ok := parseTopLevelUnionBranches(query)
	return branches, unionAll, ok && !mixed
}

// parseTopLevelUnionBranches preserves whether a composition mixes UNION and
// UNION ALL so callers can report the Cypher semantic error rather than
// treating the statement as an unsplittable query.
func parseTopLevelUnionBranches(query string) (branches []string, unionAllMode, mixed, ok bool) {
	if structure := fabric.ReadQueryStructure(query); structure.Err == nil {
		parts, distinct, found, err := fabric.SplitQueryUnion(query)
		if err != nil || !found {
			return nil, false, false, false
		}
		all, unique := false, false
		for _, d := range distinct {
			if d {
				unique = true
			} else {
				all = true
			}
		}
		return parts, !unique, all && unique, true
	}
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, false, false, false
	}

	type sep struct {
		pos int
		end int
		all bool
	}
	seps := make([]sep, 0)
	depthParen := 0
	depthBracket := 0
	depthBrace := 0

	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		switch cypherLexicalByte(ch) {
		case cypherQuoteClass:
			// A string or a backticked name (`union`) holds no UNION.
			i = skipCypherQuotedText(trimmed, i, ch) - 1
			continue
		case '(':
			depthParen++
			continue
		case ')':
			if depthParen > 0 {
				depthParen--
			}
			continue
		case '[':
			depthBracket++
			continue
		case ']':
			if depthBracket > 0 {
				depthBracket--
			}
			continue
		case '{':
			depthBrace++
			continue
		case '}':
			if depthBrace > 0 {
				depthBrace--
			}
			continue
		}

		if depthParen != 0 || depthBracket != 0 || depthBrace != 0 {
			continue
		}
		// A variable named union is a name, not the operator (#894).
		if !matchKeywordAt(trimmed, i, "UNION") || clauseKeywordUsedAsName(trimmed, i, i+len("UNION"), "UNION") {
			continue
		}
		j := skipSpaces(trimmed, i+len("UNION"))
		all := false
		if matchKeywordAt(trimmed, j, "ALL") {
			all = true
			j = skipSpaces(trimmed, j+len("ALL"))
		}
		seps = append(seps, sep{pos: i, end: j, all: all})
		i = j - 1
	}

	if len(seps) == 0 {
		return nil, false, false, false
	}

	unionAllMode = true
	hasDistinct := false
	hasAll := false
	for _, s := range seps {
		if s.all {
			hasAll = true
		} else {
			hasDistinct = true
			unionAllMode = false
		}
	}
	mixed = hasDistinct && hasAll

	branches = make([]string, 0, util.SafePreallocSum(len(seps), 1))
	start := 0
	for _, s := range seps {
		part := strings.TrimSpace(trimmed[start:s.pos])
		if part == "" {
			return nil, false, false, false
		}
		branches = append(branches, part)
		start = s.end
	}
	last := strings.TrimSpace(trimmed[start:])
	if last == "" {
		return nil, false, false, false
	}
	branches = append(branches, last)
	return branches, unionAllMode, mixed, true
}

func (e *StorageExecutor) refreshCallSeedNodeValues(ctx context.Context, seedRow []interface{}, colMap map[string]int, importVars []string, stats *QueryStats) ([]interface{}, error) {
	if !queryStatsMayContainNodeMutation(stats) {
		return seedRow, nil
	}

	importedNodeIDs := make(map[storage.EntityIdentity]struct{}, len(importVars))
	for _, variable := range importVars {
		index, ok := colMap[variable]
		if !ok || index < 0 || index >= len(seedRow) {
			continue
		}
		if node, ok := seedRow[index].(*storage.Node); ok && node != nil {
			importedNodeIDs[node.Identity()] = struct{}{}
		}
	}
	if len(importedNodeIDs) == 0 {
		return seedRow, nil
	}

	checkedIDs := make(map[storage.EntityIdentity]struct{}, len(importedNodeIDs))
	updatedNodes := make(map[storage.EntityIdentity]*storage.Node, len(importedNodeIDs))
	var refreshed []interface{}
	for index, value := range seedRow {
		node, ok := value.(*storage.Node)
		if !ok || node == nil {
			continue
		}
		if _, imported := importedNodeIDs[node.Identity()]; !imported {
			continue
		}
		if _, checked := checkedIDs[node.Identity()]; !checked {
			checkedIDs[node.Identity()] = struct{}{}
			current, err := e.refreshCallSeedNodeAfterWrite(ctx, node, stats)
			if err != nil {
				return nil, err
			}
			if current != nil {
				updatedNodes[node.Identity()] = current
			}
		}
		if current := updatedNodes[node.Identity()]; current != nil {
			if refreshed == nil {
				refreshed = append([]interface{}(nil), seedRow...)
			}
			refreshed[index] = current
		}
	}
	if refreshed == nil {
		return seedRow, nil
	}
	return refreshed, nil
}

func (e *StorageExecutor) refreshCallSeedNodeAfterWrite(ctx context.Context, node *storage.Node, stats *QueryStats) (*storage.Node, error) {
	if node == nil || !queryStatsMayContainNodeMutation(stats) {
		return node, nil
	}
	store := e.getStorage(ctx)
	lookup, valid := node.Identity().NodeLookupID(store)
	if !valid {
		return node, nil
	}
	current, err := e.getQueryNode(store, lookup)
	if errors.Is(err, storage.ErrNotFound) {
		return node, nil
	}
	if err != nil {
		return nil, err
	}
	if current == nil {
		return node, nil
	}
	return current, nil
}

func queryStatsMayContainNodeMutation(stats *QueryStats) bool {
	return stats != nil && (stats.PropertiesSet > 0 || stats.LabelsAdded > 0 || stats.LabelsRemoved > 0)
}

func (e *StorageExecutor) tryExecuteCorrelatedBatchedLookup(
	ctx context.Context,
	seedResult *ExecuteResult,
	innerBody string,
	importVars []string,
	colMap map[string]int,
) (*ExecuteResult, bool, error) {
	if seedResult == nil || len(seedResult.Rows) == 0 || len(importVars) != 1 {
		return nil, false, nil
	}

	importCol := strings.TrimSpace(importVars[0])
	if importCol == "" {
		return nil, false, nil
	}
	importIdx, ok := colMap[importCol]
	if !ok {
		return nil, false, nil
	}

	trimmed := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(innerBody), ";"))
	structure := fabric.ReadQueryStructure(trimmed)
	if structure.Err != nil || len(structure.Clauses) != 2 || !structure.Starts("MATCH") || structure.Clauses[1].Kind != "RETURN" {
		return nil, false, nil
	}
	matchPart := "MATCH " + structure.Clauses[0].Pattern
	wherePart := structure.Clauses[0].Predicate
	if matchPart == "" || wherePart == "" {
		return nil, false, nil
	}

	matchVar, matchProp, otherWhere, ok := extractCallSubqueryCorrelationWhere(wherePart, importCol)
	if !ok || matchVar == "" || matchProp == "" {
		return nil, false, nil
	}
	if sanitized, ok := sanitizeCallSubqueryOtherWhere(otherWhere, importCol); ok {
		otherWhere = sanitized
	} else {
		return nil, false, nil
	}

	resultProjection := structure.Clauses[1].Projection
	if resultProjection == nil || resultProjection.Star || resultProjection.Distinct || resultProjection.Modifiers != "" {
		// A window belongs to each correlated invocation, not the shared key
		// lookup. Let the ordinary CALL row stream execute that projection.
		return nil, false, nil
	}
	projection, modifiers := resultProjection.Text, resultProjection.Modifiers
	if projection == "" {
		return nil, false, nil
	}

	// Collect unique lookup keys from seed rows.
	keys := make([]interface{}, 0, len(seedResult.Rows))
	seenKeys := make(map[string]struct{}, len(seedResult.Rows))
	for _, row := range seedResult.Rows {
		if importIdx < 0 || importIdx >= len(row) {
			continue
		}
		k := row[importIdx]
		ks := callSubqueryLookupKeyString(k)
		if _, exists := seenKeys[ks]; exists {
			continue
		}
		seenKeys[ks] = struct{}{}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return &ExecuteResult{
			Columns: append([]string{}, seedResult.Columns...),
			Rows:    [][]interface{}{},
		}, true, nil
	}

	var rewritten strings.Builder
	rewritten.Grow(len(trimmed) + len(projection) + 128)
	rewritten.WriteString(matchPart)
	if strings.TrimSpace(otherWhere) != "" {
		rewritten.WriteString(" WHERE ")
		rewritten.WriteString(otherWhere)
		rewritten.WriteString(" AND ")
	} else {
		rewritten.WriteString(" WHERE ")
	}
	rewritten.WriteString(matchVar)
	rewritten.WriteString(".")
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" IN $__call_subquery_lookup_keys")
	rewritten.WriteString(" RETURN ")
	rewritten.WriteString(matchVar)
	rewritten.WriteString(".")
	rewritten.WriteString(matchProp)
	rewritten.WriteString(" AS __call_subquery_lookup_key, ")
	rewritten.WriteString(projection)
	if strings.TrimSpace(modifiers) != "" {
		rewritten.WriteString(" ")
		rewritten.WriteString(strings.TrimSpace(modifiers))
	}

	params := map[string]interface{}{
		"__call_subquery_lookup_keys": keys,
	}
	innerRes, err := e.executeInternal(ctx, rewritten.String(), params)
	if err != nil {
		return nil, true, localizedError(localization.CypherSubqueriesBatchedLookupFailed(err), err)
	}

	if innerRes == nil {
		return &ExecuteResult{
			Columns: append([]string{}, seedResult.Columns...),
			Rows:    [][]interface{}{},
		}, true, nil
	}
	if len(innerRes.Columns) == 0 || !strings.EqualFold(strings.TrimSpace(innerRes.Columns[0]), "__call_subquery_lookup_key") {
		return nil, true, localizedError(localization.CypherSubqueriesBatchedLookupColumnsUnexpected(fmt.Sprint(innerRes.Columns)), nil)
	}

	// Keep only inner columns not already present in seed columns and not join-key column.
	innerKeepIdx := make([]int, 0, len(innerRes.Columns))
	innerKeepCols := make([]string, 0, len(innerRes.Columns))
	for i, col := range innerRes.Columns {
		if i == 0 {
			continue
		}
		if _, exists := colMap[col]; exists {
			continue
		}
		innerKeepIdx = append(innerKeepIdx, i)
		innerKeepCols = append(innerKeepCols, col)
	}

	grouped := make(map[string][][]interface{}, len(innerRes.Rows))
	for _, r := range innerRes.Rows {
		if len(r) == 0 {
			continue
		}
		k := callSubqueryLookupKeyString(r[0])
		vals := make([]interface{}, 0, len(innerKeepIdx))
		for _, idx := range innerKeepIdx {
			if idx >= 0 && idx < len(r) {
				vals = append(vals, r[idx])
			} else {
				vals = append(vals, nil)
			}
		}
		grouped[k] = append(grouped[k], vals)
	}

	combinedCols := append([]string{}, seedResult.Columns...)
	combinedCols = append(combinedCols, innerKeepCols...)
	combinedRows := make([][]interface{}, 0, len(seedResult.Rows))
	for _, seedRow := range seedResult.Rows {
		if importIdx < 0 || importIdx >= len(seedRow) {
			continue
		}
		matches := grouped[callSubqueryLookupKeyString(seedRow[importIdx])]
		if len(matches) == 0 {
			continue
		}
		for _, m := range matches {
			joined := append([]interface{}{}, seedRow...)
			joined = append(joined, m...)
			combinedRows = append(combinedRows, joined)
		}
	}

	return &ExecuteResult{
		Columns: combinedCols,
		Rows:    combinedRows,
	}, true, nil
}

func callSubqueryLookupKeyString(v interface{}) string {
	switch x := v.(type) {
	case nil:
		return "<nil>"
	case string:
		return "s:" + normalizeCallSubqueryLookupString(x)
	case []byte:
		return "s:" + normalizeCallSubqueryLookupString(string(x))
	case int:
		return fmt.Sprintf("i:%d", x)
	case int64:
		return fmt.Sprintf("i64:%d", x)
	case float64:
		return fmt.Sprintf("f:%g", x)
	case bool:
		if x {
			return "b:1"
		}
		return "b:0"
	default:
		return fmt.Sprintf("%T:%v", v, v)
	}
}

func normalizeCallSubqueryLookupString(s string) string {
	s = strings.TrimSpace(s)
	return strings.Trim(s, `"`)
}

func extractCallSubqueryCorrelationWhere(whereClause, importCol string) (matchVar, matchProp, otherWhere string, ok bool) {
	terms := fabric.ExpressionAndTerms(whereClause)
	if len(terms) == 0 {
		return "", "", "", false
	}
	correlationIdx := -1
	for i, term := range terms {
		lhs, rhs, isEq := splitTopLevelEqualityCallSubquery(term)
		if !isEq {
			continue
		}
		leftVar, leftProp, leftOK := parseCallSubqueryVarProp(lhs)
		rightVar, rightProp, rightOK := parseCallSubqueryVarProp(rhs)
		switch {
		case leftOK && isSimpleIdentifier(strings.TrimSpace(rhs)) && strings.EqualFold(strings.TrimSpace(rhs), importCol):
			matchVar, matchProp = leftVar, leftProp
			correlationIdx = i
		case rightOK && isSimpleIdentifier(strings.TrimSpace(lhs)) && strings.EqualFold(strings.TrimSpace(lhs), importCol):
			matchVar, matchProp = rightVar, rightProp
			correlationIdx = i
		}
		if correlationIdx >= 0 {
			break
		}
	}
	if correlationIdx < 0 || matchVar == "" || matchProp == "" {
		return "", "", "", false
	}
	remaining := make([]string, 0, len(terms)-1)
	for i, term := range terms {
		if i == correlationIdx {
			continue
		}
		t := strings.TrimSpace(term)
		if t != "" {
			remaining = append(remaining, t)
		}
	}
	return matchVar, matchProp, strings.Join(remaining, " AND "), true
}

func parseCallSubqueryVarProp(expr string) (string, string, bool) {
	return fabric.ExpressionProperty(expr)
}

func sanitizeCallSubqueryOtherWhere(otherWhere string, importCol string) (string, bool) {
	if strings.TrimSpace(otherWhere) == "" {
		return "", true
	}
	terms := fabric.ExpressionAndTerms(otherWhere)
	if len(terms) == 0 {
		return "", true
	}
	kept := make([]string, 0, len(terms))
	for _, term := range terms {
		term = strings.TrimSpace(term)
		if term == "" {
			continue
		}
		if isCallSubqueryImportNotNullGuardTerm(term, importCol) {
			continue
		}
		if containsStandaloneIdentifier(term, importCol) {
			return "", false
		}
		kept = append(kept, term)
	}
	if len(kept) == 0 {
		return "", true
	}
	return strings.Join(kept, " AND "), true
}

func isCallSubqueryImportNotNullGuardTerm(term, importCol string) bool {
	return fabric.ExpressionNotNullGuard(term, importCol)
}

func splitTopLevelEqualityCallSubquery(expr string) (string, string, bool) {
	return fabric.ExpressionEquality(expr)
}

func splitTopLevelResultModifiersCallSubquery(returnPart string) (projection string, modifiers string) {
	p := fabric.ProjectionSyntax(returnPart)
	if p == nil {
		return strings.TrimSpace(returnPart), ""
	}
	return p.Text, p.Modifiers
}

func callSubqueryQueryIsWrite(query string) bool { return fabric.ReadQueryStructure(query).Writes() }

func isSimpleIdentifier(s string) bool {
	s = strings.TrimSpace(strings.Trim(s, "`"))
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if i == 0 {
			if !isIdentStartByte(ch) {
				return false
			}
			continue
		}
		if !isIdentByte(ch) {
			return false
		}
	}
	return true
}

func containsStandaloneIdentifier(expr, ident string) bool {
	return fabric.ExpressionHasVariable(expr, ident)
}

func crossJoinCallResults(left, right *ExecuteResult) *ExecuteResult {
	if left == nil {
		return right
	}
	if right == nil {
		return left
	}

	colMap := make(map[string]struct{}, len(left.Columns))
	for _, col := range left.Columns {
		colMap[col] = struct{}{}
	}
	combinedCols := append([]string{}, left.Columns...)
	innerUniqueIdx := make([]int, 0, len(right.Columns))
	for i, col := range right.Columns {
		if _, exists := colMap[col]; !exists {
			combinedCols = append(combinedCols, col)
			innerUniqueIdx = append(innerUniqueIdx, i)
		}
	}

	rows := make([][]interface{}, 0, util.SafePreallocProduct(len(left.Rows), len(right.Rows)))
	for _, lrow := range left.Rows {
		for _, rrow := range right.Rows {
			joined := append([]interface{}{}, lrow...)
			for _, idx := range innerUniqueIdx {
				if idx >= 0 && idx < len(rrow) {
					joined = append(joined, rrow[idx])
				} else {
					joined = append(joined, nil)
				}
			}
			rows = append(rows, joined)
		}
	}

	return &ExecuteResult{Columns: combinedCols, Rows: rows}
}

// processCallSubqueryReturn processes the RETURN clause after CALL {}
func (e *StorageExecutor) processCallSubqueryReturn(ctx context.Context, innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	if fabric.ReadQueryStructure(afterCall).First("RETURN") == nil {
		return innerResult, nil
	}
	rows := make([]pipelineRow, 0, len(innerResult.Rows))
	scope := make(map[string]struct{}, len(innerResult.Columns))
	for _, column := range innerResult.Columns {
		scope[column] = struct{}{}
	}
	for _, values := range innerResult.Rows {
		row := make(pipelineRow, len(innerResult.Columns))
		for index, column := range innerResult.Columns {
			if index < len(values) {
				row[column] = values[index]
			} else {
				row[column] = nil
			}
		}
		rows = append(rows, row)
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(afterCall)
	if !ok {
		return nil, localizedError(localization.CypherSubqueriesAfterCallClauseUnsupported(afterCall), nil)
	}
	result, handled, err := e.runPipelineClauses(ctx, rows, scope, clauses, clauses)
	if err != nil {
		return nil, err
	}
	if !handled || result == nil {
		return nil, localizedError(localization.CypherSubqueriesAfterCallClauseUnsupported("RETURN"), nil)
	}
	result.Stats = innerResult.Stats
	return result, nil
}

// applyResultModifiers applies ORDER BY, LIMIT, SKIP to a result
func (e *StorageExecutor) applyResultModifiers(ctx context.Context, result *ExecuteResult, modifiers string) (*ExecuteResult, error) {
	skip := 0
	if value, ok := e.parseIntModifier(ctx, modifiers, "SKIP"); ok {
		skip = value
	}
	limit := -1
	if value, ok := e.parseIntModifier(ctx, modifiers, "LIMIT"); ok {
		limit = value
	}
	return e.applyCallResultOrderWindow(ctx, result, parseOrderByTerms(modifiers), skip, limit)
}

// applyOrderByToResult applies ORDER BY to a result set
func (e *StorageExecutor) applyOrderByToResult(result *ExecuteResult, orderByClause string) *ExecuteResult {
	terms := parseOrderByTerms(orderByClause)
	if len(terms) == 0 {
		terms = parseOrderByClause(orderByClause)
	}
	_, _ = e.applyCallResultOrderWindow(context.Background(), result, terms, 0, -1)
	return result
}

func (e *StorageExecutor) applyCallResultOrderWindow(ctx context.Context, result *ExecuteResult, terms []orderByTerm, skip, limit int) (*ExecuteResult, error) {
	rows := make([]pipelineRow, 0, len(result.Rows))
	for _, values := range result.Rows {
		row := make(pipelineRow, len(result.Columns))
		for index, column := range result.Columns {
			if index < len(values) {
				row[column] = values[index]
			} else {
				row[column] = nil
			}
		}
		for _, term := range terms {
			parts := strings.SplitN(term.column, ".", 2)
			if len(parts) != 2 {
				continue
			}
			if value, ok := row[parts[0]].(map[string]interface{}); ok {
				if _, nested := value["properties"].(map[string]interface{}); nested {
					row[term.column] = extractPropertyFromValue(value, parts[1])
				}
			}
		}
		rows = append(rows, row)
	}
	if !e.orderPipelineRows(ctx, rows, terms) {
		if err := getExpressionFailure(ctx); err != nil {
			return nil, err
		}
	}
	rows = applyPipelineWindow(rows, skip, limit)
	result.Rows = make([][]interface{}, 0, len(rows))
	for _, row := range rows {
		values := make([]interface{}, len(result.Columns))
		for index, column := range result.Columns {
			values[index] = row[column]
		}
		result.Rows = append(result.Rows, values)
	}
	return result, nil
}

type orderByTerm struct {
	column     string
	descending bool
}

// parseOrderByTerms parses the ORDER BY of a projection's modifiers. Keywords
// inside braces (COLLECT { … ORDER BY … }, map literals) belong to nested
// expressions, never to the projection (#652, #547).
func parseOrderByTerms(modifiers string) []orderByTerm {
	if p := fabric.ResultModifierSyntax(modifiers); p != nil {
		var terms []orderByTerm
		for _, item := range p.Order {
			terms = append(terms, orderByTerm{column: item.Expression, descending: item.Desc})
		}
		return terms
	}
	orderByIndex := topLevelKeywordIndex(modifiers, "ORDER BY")
	if orderByIndex < 0 {
		return nil
	}
	return parseOrderByClause(modifiers[orderByIndex+len("ORDER BY"):])
}

// parseOrderByClause parses the sort items that follow ORDER BY. The list ends
// at SKIP / LIMIT, and at the WHERE that may follow ORDER BY in a WITH clause
// (WITH n ORDER BY n.x WHERE n.y > 0): that WHERE filters the sorted rows and
// is not part of the last sort item.
func parseOrderByClause(clause string) []orderByTerm {
	if items := fabric.OrderItemsSyntax(clause); items != nil {
		terms := make([]orderByTerm, 0, len(items))
		for _, item := range items {
			terms = append(terms, orderByTerm{column: item.Expression, descending: item.Desc})
		}
		return terms
	}
	clause = strings.TrimSpace(clause)
	end := len(clause)
	for _, keyword := range []string{"LIMIT", "SKIP"} {
		if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	if index := topLevelKeywordIndex(clause, "WHERE"); index >= 0 && index < end {
		end = index
	}
	clause = strings.TrimSpace(clause[:end])
	parts := splitTopLevelComma(clause)
	terms := make([]orderByTerm, 0, len(parts))
	for _, part := range parts {
		expression := strings.TrimSpace(part)
		fields := strings.Fields(expression)
		if len(fields) == 0 {
			continue
		}
		term := orderByTerm{column: expression}
		if len(fields) > 1 {
			direction := upperASCII(fields[len(fields)-1])
			switch direction {
			case "DESC", "DESCENDING":
				term.descending = true
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			case "ASC", "ASCENDING":
				term.column = strings.TrimSpace(expression[:strings.LastIndex(expression, fields[len(fields)-1])])
			}
		}
		terms = append(terms, term)
	}
	return terms
}

// compareValuesForSort compares two values for sorting, returns -1, 0, or 1
// extractPropertyFromValue extracts a named property from a value that may be a node map.
func extractPropertyFromValue(val interface{}, propName string) interface{} {
	property, _ := rowPropertyValue(val, propName)
	return property
}

func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		if cypherValueKindOf(b) == valueKindNull {
			return 0
		}
		return 1
	}
	if b == nil {
		if cypherValueKindOf(a) == valueKindNull {
			return 0
		}
		return -1
	}
	if comparison, temporal := compareTemporalOrdering(a, b); temporal {
		return comparison
	}
	if comparison, points := comparePointOrdering(a, b); points {
		return comparison
	}

	aRank := cypherSortRank(a)
	bRank := cypherSortRank(b)
	if aRank != bRank {
		if aRank < bRank {
			return -1
		}
		return 1
	}
	if left, isDuration := asCypherDuration(a); isDuration {
		if right, bothDurations := asCypherDuration(b); bothDurations {
			return compareDurationOrdering(left, right)
		}
	}

	if aList, ok := cypherSortList(a); ok {
		bList, _ := cypherSortList(b)
		for index := 0; index < len(aList) && index < len(bList); index++ {
			if comparison := compareValuesForSort(aList[index], bList[index]); comparison != 0 {
				return comparison
			}
		}
		return compareOrderedInts(len(aList), len(bList))
	}
	if comparison, exact := compareCypherNumbersExactly(a, b); exact {
		return comparison
	}
	if aNumber, ok := strictNumericValue(a); ok {
		bNumber, _ := strictNumericValue(b)
		if math.IsNaN(aNumber) && math.IsNaN(bNumber) {
			return 0
		}
		if aNumber < bNumber {
			return -1
		}
		if aNumber > bNumber {
			return 1
		}
		// -0.0 orders before 0.0 (Neo4j orders floats as Java's
		// Double.compare does), so max picks 0.0 and min -0.0.
		if _, leftFloat := a.(float64); leftFloat && aNumber == 0 {
			if _, rightFloat := b.(float64); rightFloat && math.Signbit(aNumber) != math.Signbit(bNumber) {
				if math.Signbit(aNumber) {
					return -1
				}
				return 1
			}
		}
		return 0
	}
	switch left := a.(type) {
	case CypherVector:
		return compareVectors(left, b.(CypherVector))
	case CypherUUID:
		return compareUUIDs(left, b.(CypherUUID))
	case string:
		right := b.(string)
		if left < right {
			return -1
		}
		if left > right {
			return 1
		}
		return 0
	case bool:
		right := b.(bool)
		if left == right {
			return 0
		}
		if !left {
			return -1
		}
		return 1
	}

	if comparison, _, structured := compareCypherStructuredValues(a, b, compareSortableValues); structured {
		return comparison
	}
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func compareOrderedInts(left, right int) int {
	if left < right {
		return -1
	}
	if left > right {
		return 1
	}
	return 0
}

func parseOrderByModifier(modifiers string) (column string, descending bool, ok bool) {
	terms := parseOrderByTerms(modifiers)
	if len(terms) == 0 {
		return "", false, false
	}
	return terms[0].column, terms[0].descending, terms[0].column != ""
}

// parseIntModifier returns the SKIP or LIMIT value in a RETURN/WITH modifier
// tail. A plain integer is read directly; anything else (1 + 1, $s + 1 after
// parameter substitution, toInteger('2')) is evaluated as a constant
// expression through evaluatePipelinePagination, the same evaluation WITH uses,
// so every route applies SKIP/LIMIT expressions instead of ignoring them.
// ok is false when the keyword is absent or the value is not a non-negative
// integer.
func (e *StorageExecutor) parseIntModifier(ctx context.Context, modifiers, keyword string) (value int, ok bool) {
	if p := fabric.ResultModifierSyntax(modifiers); p != nil {
		text := ""
		switch keyword {
		case "SKIP":
			text = p.Skip
		case "LIMIT":
			text = p.Limit
		}
		if text == "" {
			return 0, false
		}
		if n, err := strconv.Atoi(text); err == nil {
			return n, true
		}
		return e.evaluatePipelinePagination(ctx, text, nil)
	}
	idx := topLevelKeywordIndex(modifiers, keyword)
	if idx == -1 {
		return 0, false
	}
	kwPart := strings.TrimSpace(modifiers[idx+len(keyword):])
	nextKw := len(kwPart)
	for _, otherKeyword := range []string{"LIMIT", "SKIP", "ORDER BY"} {
		if otherKeyword == keyword {
			continue
		}
		if kidx := topLevelKeywordIndex(kwPart, otherKeyword); kidx != -1 && kidx < nextKw {
			nextKw = kidx
		}
	}
	vs := strings.TrimSpace(kwPart[:nextKw])
	if v, err := strconv.Atoi(vs); err == nil {
		return v, true
	}
	if vs == "" {
		return 0, false
	}
	return e.evaluatePipelinePagination(ctx, vs, nil)
}

func findColumnIndexByName(cols []string, name string) int {
	for i, col := range cols {
		if col == name {
			return i
		}
	}
	return -1
}

type rowRefForOrder struct {
	row []interface{}
	idx int
}

func compareRowRefsForOrder(a, b rowRefForOrder, colIdx int, descending bool) int {
	var av, bv interface{}
	if colIdx >= 0 && colIdx < len(a.row) {
		av = a.row[colIdx]
	}
	if colIdx >= 0 && colIdx < len(b.row) {
		bv = b.row[colIdx]
	}
	cmp := compareValuesForSort(av, bv)
	if descending {
		cmp = -cmp
	}
	if cmp != 0 {
		return cmp
	}
	// Stable tie-breaker by original position.
	if a.idx < b.idx {
		return -1
	}
	if a.idx > b.idx {
		return 1
	}
	return 0
}

type topKRowsHeap struct {
	items      []rowRefForOrder
	colIdx     int
	descending bool
}

func (h topKRowsHeap) Len() int { return len(h.items) }
func (h topKRowsHeap) Less(i, j int) bool {
	// Keep worst row at heap top.
	return compareRowRefsForOrder(h.items[i], h.items[j], h.colIdx, h.descending) > 0
}
func (h topKRowsHeap) Swap(i, j int) { h.items[i], h.items[j] = h.items[j], h.items[i] }
func (h *topKRowsHeap) Push(x interface{}) {
	h.items = append(h.items, x.(rowRefForOrder))
}
func (h *topKRowsHeap) Pop() interface{} {
	n := len(h.items)
	it := h.items[n-1]
	h.items = h.items[:n-1]
	return it
}

func selectTopKRowsForOrder(rows [][]interface{}, colIdx int, descending bool, k int) [][]interface{} {
	if k <= 0 || len(rows) == 0 {
		return [][]interface{}{}
	}
	if k >= len(rows) {
		out := make([][]interface{}, len(rows))
		copy(out, rows)
		sort.SliceStable(out, func(i, j int) bool {
			ri := rowRefForOrder{row: out[i], idx: i}
			rj := rowRefForOrder{row: out[j], idx: j}
			return compareRowRefsForOrder(ri, rj, colIdx, descending) < 0
		})
		return out
	}

	h := &topKRowsHeap{
		items:      make([]rowRefForOrder, 0, k),
		colIdx:     colIdx,
		descending: descending,
	}
	for i, row := range rows {
		ref := rowRefForOrder{row: row, idx: i}
		if h.Len() < k {
			heap.Push(h, ref)
			continue
		}
		// Replace current worst when new row is better.
		if compareRowRefsForOrder(ref, h.items[0], colIdx, descending) < 0 {
			h.items[0] = ref
			heap.Fix(h, 0)
		}
	}

	selected := make([]rowRefForOrder, len(h.items))
	copy(selected, h.items)
	sort.SliceStable(selected, func(i, j int) bool {
		return compareRowRefsForOrder(selected[i], selected[j], colIdx, descending) < 0
	})

	out := make([][]interface{}, 0, len(selected))
	for _, it := range selected {
		out = append(out, it.row)
	}
	return out
}

// Patterns compiled once (#591).
var (
	// callPropertyAccessPattern is a variable.property reference in a CALL.
	callPropertyAccessPattern = regexp.MustCompile(`(\w+)\.(\w+)`)
	// variableNullCheckPattern is "variable IS [NOT] NULL".
	variableNullCheckPattern = regexp.MustCompile(`(?i)^([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL$`)
)
