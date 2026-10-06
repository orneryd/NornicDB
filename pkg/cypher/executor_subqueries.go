package cypher

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(cypher, callSubqueryRe)
}

// startsWithCallSubquery reports whether cypher begins with a CALL { } or
// CALL (vars) { } subquery clause (as opposed to a procedure CALL, or a
// subquery later in the statement).
func startsWithCallSubquery(cypher string) bool {
	return callSubqueryAt(strings.TrimSpace(cypher), 0)
}

// substituteBoundVariablesInCall replaces node variable references in CALL
// statements with actual values.
//
// Example:
//
//	CALL db.index.vector.queryNodes('idx', 10, n.embedding)
//
// becomes:
//
//	CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, ...])
//
// substituteBoundVariablesInCall is the relationship-aware
// variant used by MATCH ... WITH r CALL ... pipelines.
func (e *StorageExecutor) substituteBoundVariablesInCall(callPart string, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge) string {
	result := callPart

	// Find all variable.property patterns in the CALL
	// Pattern: varName.propertyName (but not inside strings)
	// We need to be careful not to match patterns inside quoted strings
	matches := callPropertyAccessPattern.FindAllStringSubmatchIndex(callPart, -1)

	// Process matches in reverse order to maintain indices
	for i := len(matches) - 1; i >= 0; i-- {
		match := matches[i]
		startIdx := match[0]
		endIdx := match[1]
		varName := callPart[match[2]:match[3]]
		propName := callPart[match[4]:match[5]]

		// Check if this match is inside a quoted string (skip if so)
		beforeMatch := callPart[:startIdx]
		singleQuotes := strings.Count(beforeMatch, "'") - strings.Count(beforeMatch, "\\'")
		doubleQuotes := strings.Count(beforeMatch, "\"") - strings.Count(beforeMatch, "\\\"")
		if singleQuotes%2 != 0 || doubleQuotes%2 != 0 {
			// Inside a quoted string - skip
			continue
		}

		// Check if this variable is in our context
		var value interface{}
		found := false
		if node, exists := nodeContext[varName]; exists {
			// Evaluate the property access
			{
				// Regular property access — no special-casing for any property name.
				// Users can store embeddings in any property and create a vector index for it.
				if val, ok := node.Properties[propName]; ok {
					value = val
					found = true
				}
			}
		}
		if !found {
			if rel, exists := relContext[varName]; exists && rel != nil {
				if val, ok := rel.Properties[propName]; ok {
					value = val
					found = true
				}
			}
		}

		if found {
			// Replace the variable.property with the actual value
			if value != nil {
				replacement := callLiteralForBoundValue(value)
				// Replace from end to start to maintain indices
				result = result[:startIdx] + replacement + result[endIdx:]
			}
		}
	}

	// Procedures that accept a bound node variable as the first argument need
	// the variable rewritten to a concrete node identifier before executeCall.
	// Example:
	//   CALL db.create.setNodeVectorProperty(n, 'emb', [..])
	// -> CALL db.create.setNodeVectorProperty('node-id', 'emb', [..])
	upper := upperASCII(result)
	procIdx := strings.Index(upper, "DB.CREATE.SETNODEVECTORPROPERTY")
	if procIdx >= 0 {
		openParen := strings.Index(result[procIdx:], "(")
		if openParen >= 0 {
			openParen += procIdx
			closeParen := findMatchingCallParen(result, openParen)
			if closeParen > openParen {
				args := splitProcedureTopLevelComma(result[openParen+1 : closeParen])
				if len(args) > 0 {
					firstArg := strings.TrimSpace(args[0])
					if node, ok := nodeContext[firstArg]; ok && node != nil {
						args[0] = quoteCypherStringLiteral(string(node.ID))
						result = result[:openParen+1] + strings.Join(args, ", ") + result[closeParen:]
					}
				}
			}
		}
	}

	procIdx = strings.Index(upper, "DB.CREATE.SETRELATIONSHIPVECTORPROPERTY")
	if procIdx >= 0 {
		openParen := strings.Index(result[procIdx:], "(")
		if openParen >= 0 {
			openParen += procIdx
			closeParen := findMatchingCallParen(result, openParen)
			if closeParen > openParen {
				args := splitProcedureTopLevelComma(result[openParen+1 : closeParen])
				if len(args) > 0 {
					firstArg := strings.TrimSpace(args[0])
					if rel, ok := relContext[firstArg]; ok && rel != nil {
						args[0] = quoteCypherStringLiteral(string(rel.ID))
						result = result[:openParen+1] + strings.Join(args, ", ") + result[closeParen:]
					}
				}
			}
		}
	}

	return result
}

func callLiteralForBoundValue(value interface{}) string {
	switch v := value.(type) {
	case []float32:
		parts := make([]string, len(v))
		for i, f := range v {
			parts[i] = fmt.Sprintf("%g", f)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case []float64:
		parts := make([]string, len(v))
		for i, f := range v {
			parts[i] = fmt.Sprintf("%g", f)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case string:
		return quoteCypherStringLiteral(v)
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float32:
		return fmt.Sprintf("%g", v)
	case float64:
		return fmt.Sprintf("%g", v)
	case []interface{}:
		parts := make([]string, len(v))
		for i, item := range v {
			parts[i] = callLiteralForBoundValue(item)
		}
		return "[" + strings.Join(parts, ", ") + "]"
	case bool:
		if v {
			return "true"
		}
		return "false"
	default:
		return fmt.Sprintf("%v", v)
	}
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
		if optIdx := findMultiWordKeywordIndex(outerPart, "OPTIONAL", "MATCH"); optIdx >= 0 {
			optionalPart := strings.TrimSpace(outerPart[optIdx:])
			end := len(optionalPart)
			for _, kw := range []string{"WITH", "RETURN", "ORDER BY", "SKIP", "LIMIT"} {
				if idx := findKeywordIndex(optionalPart, kw); idx >= 0 && idx < end {
					end = idx
				}
			}
			optionalPart = strings.TrimSpace(optionalPart[:end])
			if strings.HasPrefix(upperASCII(optionalPart), "OPTIONAL MATCH") {
				requiredPart := optionalPart
				if len(requiredPart) >= len("OPTIONAL MATCH") {
					requiredPart = "MATCH" + requiredPart[len("OPTIONAL MATCH"):]
				}
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
	trimmed := strings.TrimSpace(innerBody)
	// Accept any valid RETURN clause start (space/newline/tab after RETURN),
	// not just a single-space prefix.
	if findKeywordIndex(trimmed, "RETURN") != 0 {
		return false
	}
	return findKeywordIndex(trimmed, "MATCH") < 0 &&
		findKeywordIndex(trimmed, "CALL") < 0 &&
		findKeywordIndex(trimmed, "UNWIND") < 0 &&
		findKeywordIndex(trimmed, "MERGE") < 0 &&
		findKeywordIndex(trimmed, "CREATE") < 0 &&
		findKeywordIndex(trimmed, "DELETE") < 0 &&
		findKeywordIndex(trimmed, "SET") < 0 &&
		findKeywordIndex(trimmed, "REMOVE") < 0 &&
		findKeywordIndex(trimmed, "WITH") < 0 &&
		findKeywordIndex(trimmed, "UNION") < 0
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
	hasOuterPipelineClauses := findKeywordIndex(trimmedOuter, "WITH") >= 0 ||
		findKeywordIndex(trimmedOuter, "RETURN") >= 0

	// Fast path for simple seeded MATCH queries without relationship patterns:
	//   MATCH (v[:Label] {props}) [WHERE ...]
	// including ID/property indexed WHERE predicates.
	if findKeywordIndex(trimmedOuter, "MATCH") == 0 &&
		!hasOuterPipelineClauses &&
		!strings.Contains(trimmedOuter, "-[") &&
		!strings.Contains(trimmedOuter, "--") {
		matchBody := strings.TrimSpace(trimmedOuter[len("MATCH"):])
		whereClause := ""
		if whereIdx := findKeywordIndex(matchBody, "WHERE"); whereIdx >= 0 {
			whereClause = strings.TrimSpace(matchBody[whereIdx+len("WHERE"):])
			// Keep WHERE-only predicate text for fast-path parsing. Trailing clauses
			// (WITH/RETURN/ORDER/SKIP/LIMIT) belong to the outer pipeline and must not
			// be interpreted as part of the predicate expression.
			whereEnd := len(whereClause)
			for _, kw := range []string{"WITH", "RETURN", "ORDER BY", "SKIP", "LIMIT"} {
				if idx := findKeywordIndex(whereClause, kw); idx >= 0 && idx < whereEnd {
					whereEnd = idx
				}
			}
			whereClause = strings.TrimSpace(whereClause[:whereEnd])
			matchBody = strings.TrimSpace(matchBody[:whereIdx])
		}

		np := e.parseNodePattern(ctx, matchBody)
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
					if nodes, ok, err := e.tryCollectNodesFromIDInParam(np, whereClause, params); ok || err != nil {
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
	if topLevelKeywordIndex(seedQuery, "RETURN") < 0 {
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
	if n, err := e.storage.GetNode(storage.NodeID(id)); err == nil && n != nil {
		return n
	}
	// Handle elementId-style values: "<prefix>:<db>:<id>" -> "<id>"
	if last := strings.LastIndex(id, ":"); last >= 0 && last+1 < len(id) {
		idTail := id[last+1:]
		if n, err := e.storage.GetNode(storage.NodeID(idTail)); err == nil && n != nil {
			return n
		}
	}
	return nil
}

// executeCallSubquery executes a CALL {} subquery
// Syntax: CALL { <subquery> } [IN TRANSACTIONS [OF n ROWS]]
// The subquery can contain MATCH, CREATE, RETURN, UNION, etc.
func (e *StorageExecutor) executeCallSubquery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if outcome := e.executePipeline(ctx, cypher); outcome.terminal() {
		return outcome.result, outcome.err
	}
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Extract the subquery body from CALL { ... }
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(cypher)
	if subqueryBody == "" {
		return nil, localizedError(localization.CypherSubqueriesCallBodyExpected(), nil)
	}
	if inTransactions {
		return nil, localizedError(localization.CypherSubqueriesTransactionBodyEmpty("CALL"), nil)
	}

	// Check if the subquery body starts with USE — this indicates a fabric
	// cross-database subquery (e.g. CALL { USE nornic.tr MATCH ... }).
	// Resolve the target database and execute against that engine.
	subqueryExecutor := e
	if use, useRemaining, hasUse, useErr := parseUseClause(subqueryBody, true); hasUse || useErr != nil {
		if useErr != nil {
			return nil, useErr
		}
		if dynErr := e.dynamicUseError(use); dynErr != nil {
			return nil, dynErr
		}
		useDB := use.Name
		if authErr := e.authorizeSelectedDatabase(ctx, useDB); authErr != nil {
			return nil, authErr
		}
		scopedExec, resolvedDB, scopeErr := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if scopeErr != nil {
			return nil, localizedError(localization.CypherSubqueriesUseDatabaseFailed(useDB, scopeErr), scopeErr)
		}
		subqueryExecutor = scopedExec
		subqueryBody = useRemaining
		ctx = withExecutionDatabase(ctx, resolvedDB)
	}

	// Execute the inner subquery
	var innerResult *ExecuteResult
	var err error

	if inTransactions {
		// Execute in batches (for large data operations)
		innerResult, err = subqueryExecutor.executeCallInTransactions(ctx, subqueryBody, batchSize)
	} else {
		innerResult, err = subqueryExecutor.executeInternal(ctx, subqueryBody, nil)
	}

	if err != nil {
		if inTransactions {
			return nil, err
		}
		return nil, localizedError(localization.CypherSubqueriesCallError(err), err)
	}

	// If there's something after CALL { }, process it (e.g., RETURN)
	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, innerResult, afterCall)
	}

	return innerResult, nil
}

// parseCallSubquery extracts the body from CALL { ... } and any trailing clauses
// Returns: body, afterCall, inTransactions bool, batchSize int
func (e *StorageExecutor) parseCallSubquery(cypher string) (body, afterCall string, inTransactions bool, batchSize int) {
	batchSize = 1000 // Default batch size

	trimmed := strings.TrimSpace(cypher)

	// Find the opening brace
	braceStart := strings.Index(trimmed, "{")
	if braceStart == -1 {
		return "", "", false, batchSize
	}

	// Find matching closing brace
	depth := 0
	braceEnd := -1
	for i := braceStart; i < len(trimmed); i++ {
		if trimmed[i] == '{' {
			depth++
		} else if trimmed[i] == '}' {
			depth--
			if depth == 0 {
				braceEnd = i
				break
			}
		}
	}

	if braceEnd == -1 {
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

func parseCallSubqueryImportVariables(cypher string) []string {
	trimmed := strings.TrimSpace(cypher)
	if findKeywordIndex(trimmed, "CALL") != 0 {
		return nil
	}
	idx := skipSpaces(trimmed, len("CALL"))
	if idx >= len(trimmed) || trimmed[idx] != '(' {
		return nil
	}
	close := findMatchingCallParen(trimmed, idx)
	if close < 0 {
		return nil
	}
	body := strings.TrimSpace(trimmed[idx+1 : close])
	if body == "" {
		return []string{}
	}
	if body == "*" {
		return nil
	}
	parts := splitProcedureTopLevelComma(body)
	vars := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		name = strings.Trim(name, "`")
		if !isSimpleIdentifier(name) {
			continue
		}
		vars = append(vars, name)
	}
	return vars
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

func (e *StorageExecutor) executeVariableScopeCallInTransactions(ctx context.Context, seedNodes []*storage.Node, seedVar, subqueryBody, afterCall string, batchSize int) (*ExecuteResult, error) {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if len(seedNodes) == 0 {
		empty := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: &QueryStats{}}
		if strings.TrimSpace(afterCall) != "" {
			return e.processAfterCallSubquery(ctx, empty, afterCall)
		}
		return empty, nil
	}

	if err := e.rejectCallInTransactionsInExplicitTx(); err != nil {
		return nil, err
	}
	withVars, innerBody, hasWith, err := parseLeadingWithImports(subqueryBody)
	if err != nil {
		return nil, err
	}
	if !hasWith || len(withVars) != 1 || !strings.EqualFold(strings.TrimSpace(withVars[0]), strings.TrimSpace(seedVar)) {
		return nil, localizedError(localization.CypherSubqueriesTransactionImportShapeUnsupported(seedVar), nil)
	}
	innerBody = strings.TrimSpace(innerBody)
	if innerBody == "" {
		return nil, localizedError(localization.CypherSubqueriesTransactionBodyEmpty(seedVar), nil)
	}

	combined := &ExecuteResult{Columns: []string{}, Rows: make([][]interface{}, 0), Stats: &QueryStats{}}
	for start := 0; start < len(seedNodes); start += batchSize {
		end := start + batchSize
		if end > len(seedNodes) {
			end = len(seedNodes)
		}
		ids := make([]string, 0, end-start)
		for _, node := range seedNodes[start:end] {
			if node == nil || node.ID == "" {
				continue
			}
			ids = append(ids, string(node.ID))
		}
		if len(ids) == 0 {
			continue
		}

		batchQuery := fmt.Sprintf("MATCH (%s) WHERE id(%s) IN $__call_in_tx_ids CALL (%s) { %s }", seedVar, seedVar, seedVar, innerBody)
		if strings.TrimSpace(afterCall) != "" {
			batchQuery += " RETURN *"
		}
		batchParams := map[string]interface{}{"__call_in_tx_ids": ids}
		if inherited := getParamsFromContext(ctx); inherited != nil {
			batchParams = make(map[string]interface{}, util.SafePreallocSum(len(inherited), 1))
			for key, value := range inherited {
				batchParams[key] = value
			}
			batchParams["__call_in_tx_ids"] = ids
		}
		batchCtx := withQueryParams(ctx, batchParams)
		batchResult, err := e.executeWithImplicitTransaction(batchCtx, batchQuery, upperASCII(batchQuery))
		if err != nil {
			return nil, localizedError(localization.CypherSubqueriesTransactionBatchFailed(seedVar, start/batchSize+1, err), err)
		}
		if batchResult == nil {
			continue
		}
		if len(combined.Columns) == 0 && len(batchResult.Columns) > 0 {
			combined.Columns = append([]string{}, batchResult.Columns...)
		}
		combined.Rows = append(combined.Rows, batchResult.Rows...)
		if batchResult.Stats != nil {
			addQueryStats(combined.Stats, batchResult.Stats)
		}
	}

	if strings.TrimSpace(afterCall) != "" {
		return e.processAfterCallSubquery(ctx, combined, afterCall)
	}
	// A statement that ends with the CALL has no columns and no rows, as in
	// Neo4j; its writes are counted (#507, #676).
	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: combined.Stats}, nil
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

func (e *StorageExecutor) executeCallInTransactions(ctx context.Context, subquery string, batchSize int) (*ExecuteResult, error) {
	if err := e.rejectCallInTransactionsInExplicitTx(); err != nil {
		return nil, err
	}
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Check if the subquery contains write operations (CREATE, SET, DELETE, MERGE)
	upperSubquery := upperASCII(subquery)
	hasWrites := strings.Contains(upperSubquery, "CREATE") ||
		strings.Contains(upperSubquery, "SET") ||
		strings.Contains(upperSubquery, "DELETE") ||
		strings.Contains(upperSubquery, "MERGE")

	if !hasWrites {
		// No write operations - execute once and return (no need for batching)
		result, err := e.executeInternal(ctx, subquery, nil)
		if err != nil {
			return nil, localizedError(localization.CypherSubqueriesExecutionFailed(err), err)
		}
		return result, nil
	}

	// For write operations, we need to batch the execution
	// Strategy: Add LIMIT/SKIP to the MATCH part (before write operations) to process in batches
	// We'll execute the subquery multiple times, each time with different LIMIT/SKIP values
	// Each execution will be in its own transaction

	// First, try to get a row count estimate by executing a read-only version
	// This helps us determine how many batches we need
	readOnlyQuery := e.makeSubqueryReadOnly(subquery)
	var totalRows int
	var resultColumns []string

	if readOnlyQuery != "" {
		// Execute the read-only count (no writes). An error only means no
		// estimate: it runs with its own expression-failure record, so it
		// doesn't become the statement's error. The columns come from the
		// batches, which run the subquery's own RETURN.
		probeCtx := context.WithValue(ctx, expressionFailureKey{}, &expressionFailure{})
		readOnlyResult, err := e.executeInternal(probeCtx, readOnlyQuery, nil)
		if err == nil && readOnlyResult != nil {
			totalRows = len(readOnlyResult.Rows)
		}
	}

	// If we couldn't get a row count, we'll need to process until we get no more results
	// This is less efficient but handles edge cases
	useIterativeBatching := totalRows == 0

	// Guard: write queries without a safely batchable MATCH row source (e.g. bare CREATE
	// or UNWIND-driven writes) cannot reliably make forward progress with our SKIP/LIMIT
	// pagination rewrite and may loop indefinitely. Execute once instead.
	if useIterativeBatching {
		hasBatchableSource := strings.Contains(upperSubquery, "MATCH ")
		if !hasBatchableSource {
			singleResult, err := e.executeWithImplicitTransaction(ctx, subquery, upperASCII(subquery))
			if err != nil {
				return nil, localizedError(localization.CypherSubqueriesFirstBatchFailed(err), err)
			}
			return singleResult, nil
		}
	}

	// Combined result
	combinedResult := &ExecuteResult{
		Columns: resultColumns,
		Rows:    make([][]interface{}, 0),
		Stats:   &QueryStats{},
	}

	if useIterativeBatching {
		// Iterative batching: process batches until we get no results
		batchNum := 0
		prevBatchSig := ""
		for {
			skip := batchNum * batchSize
			limit := batchSize

			// Create a modified subquery with LIMIT and SKIP to process this batch
			modifiedSubquery := e.addLimitSkipToSubquery(subquery, limit, skip)
			// If we cannot inject pagination once skip > 0, this query shape cannot
			// make forward progress in iterative mode. Stop after the first batch to
			// preserve correctness and avoid infinite re-processing.
			if skip > 0 && strings.TrimSpace(modifiedSubquery) == strings.TrimSpace(subquery) {
				break
			}

			// Execute this batch in its own transaction
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, upperASCII(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, localizedError(localization.CypherSubqueriesBatchFailed(batchNum+1, err), err)
			}

			// If no results, we're done
			if batchResult == nil || len(batchResult.Rows) == 0 {
				break
			}
			currBatchSig := fmt.Sprintf("%v", batchResult.Rows)
			if skip > 0 && prevBatchSig != "" && currBatchSig == prevBatchSig {
				break
			}
			prevBatchSig = currBatchSig

			// Set columns from first batch if not set
			if len(combinedResult.Columns) == 0 && len(batchResult.Columns) > 0 {
				combinedResult.Columns = batchResult.Columns
			}

			// Accumulate results
			combinedResult.Rows = append(combinedResult.Rows, batchResult.Rows...)
			if batchResult.Stats != nil {
				addQueryStats(combinedResult.Stats, batchResult.Stats)
			}

			// If we got fewer rows than the batch size, we're done
			if len(batchResult.Rows) < batchSize {
				break
			}

			batchNum++
		}
	} else {
		// Known row count: process exact number of batches
		// Calculate number of batches
		numBatches := (totalRows + batchSize - 1) / batchSize

		// Process each batch in a separate transaction
		for batchNum := 0; batchNum < numBatches; batchNum++ {
			skip := batchNum * batchSize
			limit := batchSize

			// Create a modified subquery with LIMIT and SKIP to process this batch
			modifiedSubquery := e.addLimitSkipToSubquery(subquery, limit, skip)

			// Execute this batch in its own transaction
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, upperASCII(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, localizedError(localization.CypherSubqueriesBatchProgressFailed(batchNum+1, numBatches, err), err)
			}

			// Set columns from first batch if not set
			if len(combinedResult.Columns) == 0 && batchResult != nil && len(batchResult.Columns) > 0 {
				combinedResult.Columns = batchResult.Columns
			}

			// Accumulate results
			if batchResult != nil {
				combinedResult.Rows = append(combinedResult.Rows, batchResult.Rows...)
				if batchResult.Stats != nil {
					addQueryStats(combinedResult.Stats, batchResult.Stats)
				}
			}
		}
	}

	return combinedResult, nil
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
	matchIdx := findKeywordIndex(subquery, "MATCH")
	setIdx := findKeywordIndex(subquery, "SET")
	returnIdx := findKeywordIndex(subquery, "RETURN")

	// MATCH ... SET ... RETURN
	if matchIdx >= 0 && setIdx > matchIdx && returnIdx > setIdx {
		return strings.TrimSpace(subquery[matchIdx:setIdx]) + " RETURN 1"
	}

	// MATCH ... CREATE ... RETURN
	createIdx := findKeywordIndex(subquery, "CREATE")
	if matchIdx >= 0 && createIdx > matchIdx && returnIdx > createIdx {
		return strings.TrimSpace(subquery[matchIdx:createIdx]) + " RETURN 1"
	}

	// If we can't convert, return empty string (caller will use iterative batching)
	return ""
}

// addLimitSkipToSubquery adds LIMIT and SKIP clauses to a subquery for batching.
// For queries with MATCH followed by write operations (SET, CREATE, DELETE, MERGE),
// it adds LIMIT/SKIP after the MATCH clause to limit how many rows are processed.
// For other patterns, it adds LIMIT/SKIP before RETURN.
//
// This ensures that batching limits the number of matched rows processed, not just
// the number of returned rows.
func (e *StorageExecutor) addLimitSkipToSubquery(subquery string, limit, skip int) string {
	// Check for MATCH ... SET/CREATE/DELETE/MERGE ... RETURN pattern
	// For these, we want to add LIMIT/SKIP after MATCH to limit how many rows are processed
	matchIdx := findKeywordIndex(subquery, "MATCH")
	if matchIdx >= 0 {
		// Find the first operation after MATCH (SET, CREATE, DELETE, MERGE, or RETURN)
		remaining := subquery[matchIdx+5:] // Skip "MATCH"
		setIdx := findKeywordIndex(remaining, "SET")
		createIdx := findKeywordIndex(remaining, "CREATE")
		deleteIdx := findKeywordIndex(remaining, "DELETE")
		mergeIdx := findKeywordIndex(remaining, "MERGE")
		returnIdx := findKeywordIndex(remaining, "RETURN")

		// Find the earliest operation after MATCH
		firstOpIdx := -1
		var firstOpName string
		if setIdx >= 0 && (firstOpIdx == -1 || setIdx < firstOpIdx) {
			firstOpIdx = setIdx
			firstOpName = "SET"
		}
		if createIdx >= 0 && (firstOpIdx == -1 || createIdx < firstOpIdx) {
			firstOpIdx = createIdx
			firstOpName = "CREATE"
		}
		if deleteIdx >= 0 && (firstOpIdx == -1 || deleteIdx < firstOpIdx) {
			firstOpIdx = deleteIdx
			firstOpName = "DELETE"
		}
		if mergeIdx >= 0 && (firstOpIdx == -1 || mergeIdx < firstOpIdx) {
			firstOpIdx = mergeIdx
			firstOpName = "MERGE"
		}
		if returnIdx >= 0 && (firstOpIdx == -1 || returnIdx < firstOpIdx) {
			firstOpIdx = returnIdx
			firstOpName = "RETURN"
		}

		if firstOpIdx > 0 {
			// We need to find where the MATCH clause ends
			// The MATCH clause can include WHERE, so we need to find the end of the pattern
			matchEnd := matchIdx + 5 + firstOpIdx // End of MATCH pattern, start of first operation

			// Check if there's a WHERE clause between MATCH and the first operation
			whereIdx := findKeywordIndex(subquery[matchIdx+5:matchIdx+5+firstOpIdx], "WHERE")
			if whereIdx >= 0 {
				// Find end of WHERE clause (before first operation)
				whereEnd := findKeywordIndex(subquery[matchIdx+5+whereIdx:matchIdx+5+firstOpIdx], firstOpName)
				if whereEnd > 0 {
					matchEnd = matchIdx + 5 + whereIdx + 5 + whereEnd // After WHERE clause
				}
			}

			// Extract the MATCH part
			matchPart := strings.TrimSpace(subquery[:matchEnd])
			afterOp := subquery[matchEnd:]

			// Extract variable name from MATCH pattern (e.g., "MATCH (s:Source)" -> "s")
			varNames := e.extractVariableNamesFromPattern(matchPart[5:]) // Skip "MATCH"
			varName := "n"                                               // Default fallback
			if len(varNames) > 0 {
				varName = varNames[0]
			}

			// Use WITH clause to apply LIMIT/SKIP (Cypher doesn't allow LIMIT directly after MATCH)
			// Format: MATCH ... WITH var SKIP n LIMIT m CREATE/SET...
			if skip > 0 {
				return matchPart + fmt.Sprintf(" WITH %s SKIP %d LIMIT %d ", varName, skip, limit) + afterOp
			}
			return matchPart + fmt.Sprintf(" WITH %s LIMIT %d ", varName, limit) + afterOp
		}
	}

	// Fallback: Add LIMIT/SKIP before RETURN (or at end if no RETURN)
	returnIdx := findKeywordIndex(subquery, "RETURN")
	if returnIdx == -1 {
		// No RETURN clause - append LIMIT/SKIP at the end
		if skip > 0 {
			return subquery + fmt.Sprintf(" SKIP %d LIMIT %d", skip, limit)
		}
		return subquery + fmt.Sprintf(" LIMIT %d", limit)
	}

	// Find where the RETURN clause starts in the original query
	returnPart := subquery[returnIdx:]

	// Check if LIMIT or SKIP already exists
	if strings.Contains(upperASCII(returnPart), "LIMIT") || strings.Contains(upperASCII(returnPart), "SKIP") {
		// LIMIT/SKIP already present - append (may cause issues but handles common cases)
		if skip > 0 {
			return subquery + fmt.Sprintf(" SKIP %d LIMIT %d", skip, limit)
		}
		return subquery + fmt.Sprintf(" LIMIT %d", limit)
	}

	// Insert SKIP and LIMIT before RETURN
	beforeReturn := strings.TrimSpace(subquery[:returnIdx])
	returnClause := subquery[returnIdx:]

	if skip > 0 {
		return beforeReturn + fmt.Sprintf(" SKIP %d LIMIT %d ", skip, limit) + returnClause
	}
	return beforeReturn + fmt.Sprintf(" LIMIT %d ", limit) + returnClause
}

// processAfterCallSubquery handles clauses after CALL { } like RETURN
func (e *StorageExecutor) processAfterCallSubquery(ctx context.Context, innerResult *ExecuteResult, afterCall string) (*ExecuteResult, error) {
	upperAfter := upperASCII(afterCall)

	// Handle chained CALL { } subqueries.
	if strings.HasPrefix(upperAfter, "CALL") && startsWithCallSubquery(afterCall) {
		return e.executeChainedCallSubquery(ctx, innerResult, afterCall)
	}

	// Handle RETURN clause
	if strings.HasPrefix(upperAfter, "RETURN ") {
		return e.processCallSubqueryReturn(ctx, innerResult, afterCall)
	}
	if strings.EqualFold(strings.TrimSpace(afterCall), "RETURN") {
		return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherMatchingReturnExpressionRequired())
	}

	// Handle ORDER BY (without RETURN means use inner result's columns)
	if strings.HasPrefix(upperAfter, "ORDER BY ") {
		return e.applyResultModifiers(ctx, innerResult, afterCall)
	}
	if clauses, ok := canExecuteAsPipeline(afterCall); ok {
		rows := make([]pipelineRow, 0, len(innerResult.Rows))
		scope := make(map[string]struct{}, len(innerResult.Columns))
		for _, column := range innerResult.Columns {
			scope[column] = struct{}{}
		}
		params := getParamsFromContext(ctx)
		for _, seed := range innerResult.Rows {
			row := make(pipelineRow, len(innerResult.Columns)+len(params))
			for index, column := range innerResult.Columns {
				if index < len(seed) {
					row[column] = seed[index]
				} else {
					row[column] = nil
				}
			}
			bindParameterRow(ctx, row)
			rows = append(rows, row)
		}
		result, handled, err := e.runPipelineClauses(ctx, rows, scope, clauses, clauses)
		if err != nil {
			return nil, err
		}
		if handled {
			result.Stats = mergeQueryStats(result.Stats, innerResult.Stats)
			return result, nil
		}
	}

	// Unsupported clause after CALL {}
	firstWord := strings.Split(upperAfter, " ")[0]
	err := localizedError(localization.CypherSubqueriesAfterCallClauseUnsupported(firstWord), nil)
	return nil, &classifiedCypherError{
		cause:  err,
		code:   "Neo.ClientError.Statement.SyntaxError",
		detail: "UnexpectedSyntax",
	}
}

// executeChainedCallSubquery runs a CALL { } subquery for the rows produced by
// the clauses before it (seedResult), then the clauses after it. A subquery
// that imports a seed column, or that writes, runs once per seed row (a unit
// subquery, one without RETURN, keeps each row); a read-only subquery that
// imports nothing runs once and its rows are joined to every seed row. A
// statement that ends with the subquery returns no columns and no rows, as in
// Neo4j; its counters are kept.
func (e *StorageExecutor) executeChainedCallSubquery(ctx context.Context, seedResult *ExecuteResult, callClause string) (*ExecuteResult, error) {
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(callClause)
	if subqueryBody == "" {
		return nil, localizedError(localization.CypherSubqueriesCallBodyExpected(), nil)
	}
	if inTransactions {
		return nil, localizedError(localization.CypherSubqueriesChainedTransactionsUnsupported(batchSize), nil)
	}
	seedRows := callPipelineRowsFromResult(ctx, seedResult)
	scope := make(map[string]struct{}, len(seedResult.Columns))
	for _, column := range seedResult.Columns {
		scope[column] = struct{}{}
	}
	callOnly := strings.TrimSpace(callClause)
	if strings.TrimSpace(afterCall) != "" {
		callOnly = strings.TrimSpace(strings.TrimSuffix(callOnly, afterCall))
	}
	metadata := &pipelineCallMetadata{scope: scope}
	pipelineRows, pipelineStats, handled, pipelineErr := e.pipelineApplyCallSubqueryWithMetadata(ctx, seedRows, callOnly, metadata)
	if pipelineErr != nil {
		return nil, pipelineErr
	}
	if handled {
		combined := callPipelineResultFromRows(pipelineRows, seedResult, pipelineStats, metadata.columns)
		if strings.TrimSpace(afterCall) != "" {
			return e.processAfterCallSubquery(ctx, combined, afterCall)
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: pipelineStats}, nil
	}

	use, bodyWithoutUse, hasUse, err := parseUseClause(subqueryBody, true)
	if err != nil {
		return nil, err
	}
	if err := e.dynamicUseError(use); err != nil {
		return nil, err
	}
	useDB := use.Name

	targetExec := e
	if hasUse {
		if err := e.authorizeSelectedDatabase(ctx, useDB); err != nil {
			return nil, err
		}
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, err
		}
		targetExec = scopedExec
		ctx = withExecutionDatabase(ctx, resolvedDB)
		subqueryBody = bodyWithoutUse
	}

	withVars, innerBody, hasWith, err := parseLeadingWithImports(subqueryBody)
	if err != nil {
		return nil, err
	}
	if scopedVars := parseCallSubqueryImportVariables(callClause); len(scopedVars) > 0 {
		withVars = scopedVars
		innerBody = subqueryBody
		hasWith = true
	}
	implicitImportVars := detectReferencedCallSubquerySeedColumns(seedResult, subqueryBody)

	// The statement's counters are those of the clauses before the CALL plus
	// everything the subquery wrote (#650).
	stats := mergeQueryStats(nil, seedResult.Stats)
	combined := &ExecuteResult{Columns: []string{}, Rows: make([][]interface{}, 0)}
	if hasWith {
		combined, err = targetExec.executeCorrelatedCallWithSeedRows(ctx, seedResult, innerBody, withVars)
		if err != nil {
			return nil, err
		}
		stats = mergeQueryStats(stats, combined.Stats)
	} else if len(implicitImportVars) > 0 {
		combined, err = targetExec.executeCorrelatedCallWithSeedRows(ctx, seedResult, subqueryBody, implicitImportVars)
		if err != nil {
			return nil, err
		}
		stats = mergeQueryStats(stats, combined.Stats)
	} else if callSubqueryQueryIsWrite(subqueryBody) {
		// Its writes happen once per incoming row, as in Neo4j.
		combined, err = targetExec.executeCorrelatedCallWithSeedRows(ctx, seedResult, subqueryBody, nil)
		if err != nil {
			return nil, err
		}
		stats = mergeQueryStats(stats, combined.Stats)
	} else {
		innerResult, err := targetExec.executeInternal(ctx, subqueryBody, nil)
		if err != nil {
			return nil, localizedError(localization.CypherSubqueriesCallError(err), err)
		}
		if innerResult != nil {
			stats = mergeQueryStats(stats, innerResult.Stats)
		}
		combined = crossJoinCallResults(seedResult, innerResult)
	}
	combined.Stats = stats

	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, combined, afterCall)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}, Stats: stats}, nil
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

func parseLeadingWithImports(subqueryBody string) (withVars []string, innerBody string, hasWith bool, err error) {
	trimmed := strings.TrimSpace(subqueryBody)
	if !hasPrefixFoldASCII(trimmed, "WITH ") {
		return nil, trimmed, false, nil
	}

	afterWith := strings.TrimSpace(trimmed[len("WITH "):])
	// The import list ends at the first clause keyword after it (one scan
	// for all of them); a keyword at the very start is part of the list.
	nextIdx := firstKeywordIndexFromDefault(afterWith, 0, withImportClauseKeywords...)
	if nextIdx == 0 {
		nextIdx = firstKeywordIndexFromDefault(afterWith, 1, withImportClauseKeywords...)
	}
	if nextIdx < 0 {
		nextIdx = len(afterWith)
	}

	if nextIdx == len(afterWith) {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}

	withExpr := strings.TrimSpace(afterWith[:nextIdx])
	innerBody = strings.TrimSpace(afterWith[nextIdx:])
	if nextIdx < len(afterWith) && findKeywordIndex(afterWith[nextIdx:], "WHERE") == 0 {
		// Preserve `WITH ... WHERE ...` as part of the inner body so correlated
		// branch semantics remain identical to openCypher behavior.
		innerBody = strings.TrimSpace("WITH " + withExpr + " " + afterWith[nextIdx:])
		if whereExpr, whereRest, ok := splitLeadingWhereNullGuard(innerBody); ok {
			innerBody = strings.TrimSpace("WITH " + withExpr + " WHERE " + whereExpr + " " + whereRest)
		}
	}
	if innerBody == "" {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithBodyEmpty(), nil)
	}

	parts := splitReturnExpressions(withExpr)
	withVars = make([]string, 0, len(parts))
	for _, part := range parts {
		expr := strings.TrimSpace(part)
		if expr == "" {
			continue
		}

		upperExpr := upperASCII(expr)
		if asIdx := strings.Index(upperExpr, " AS "); asIdx >= 0 {
			alias := strings.TrimSpace(expr[asIdx+4:])
			if alias == "" {
				return nil, "", true, localizedError(localization.CypherSubqueriesWithImportExpressionInvalid(expr), nil)
			}
			withVars = append(withVars, alias)
			continue
		}

		withVars = append(withVars, expr)
	}

	if len(withVars) == 0 {
		return nil, "", true, localizedError(localization.CypherSubqueriesWithImportsRequired(), nil)
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
	trimmed := strings.TrimSpace(query)
	var afterWhere string
	if strings.HasPrefix(upperASCII(trimmed), "WITH ") {
		afterWith := strings.TrimSpace(trimmed[len("WITH "):])
		whereIdx := findKeywordIndex(afterWith, "WHERE")
		if whereIdx <= 0 {
			return "", "", false
		}
		afterWhere = strings.TrimSpace(afterWith[whereIdx+len("WHERE"):])
	} else if strings.HasPrefix(upperASCII(trimmed), "WHERE ") {
		afterWhere = strings.TrimSpace(trimmed[len("WHERE "):])
	} else {
		return "", "", false
	}
	if afterWhere == "" {
		return "", "", false
	}

	nextIdx := len(afterWhere)
	clauseStarts := []int{
		findKeywordIndex(afterWhere, "SET"),
		findKeywordIndex(afterWhere, "CREATE"),
		findKeywordIndex(afterWhere, "MERGE"),
		findKeywordIndex(afterWhere, "DELETE"),
		findKeywordIndex(afterWhere, "REMOVE"),
		findKeywordIndex(afterWhere, "MATCH"),
		findMultiWordKeywordIndex(afterWhere, "OPTIONAL", "MATCH"),
		findKeywordIndex(afterWhere, "UNWIND"),
		findKeywordIndex(afterWhere, "CALL"),
		findKeywordIndex(afterWhere, "RETURN"),
		findKeywordIndex(afterWhere, "WITH"),
	}
	for _, idx := range clauseStarts {
		if idx > 0 && idx < nextIdx {
			nextIdx = idx
		}
	}
	if nextIdx == len(afterWhere) {
		return "", "", false
	}

	whereExpr = strings.TrimSpace(afterWhere[:nextIdx])
	rest = strings.TrimSpace(afterWhere[nextIdx:])
	if whereExpr == "" || rest == "" {
		return "", "", false
	}
	return whereExpr, rest, true
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
	expr := strings.TrimSpace(whereExpr)
	// Accept optional wrapping parentheses.
	for strings.HasPrefix(expr, "(") && strings.HasSuffix(expr, ")") {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == expr {
			break
		}
		expr = inner
	}

	// Case-insensitive single-variable null checks only.
	// Examples:
	//   t IS NULL
	//   t IS NOT NULL
	m := variableNullCheckPattern.FindStringSubmatch(expr)
	if len(m) != 3 {
		return false, false
	}

	varName := m[1]
	_, exists := vars[varName]
	isNull := !exists || vars[varName] == nil
	isNot := strings.TrimSpace(upperASCII(m[2])) == "NOT"
	if isNot {
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
		switch ch {
		case '\'', '"', '`':
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

func (e *StorageExecutor) executeCorrelatedCallWithSeedRows(ctx context.Context, seedResult *ExecuteResult, innerBody string, importVars []string) (*ExecuteResult, error) {
	colMap := make(map[string]int, len(seedResult.Columns))
	for i, col := range seedResult.Columns {
		colMap[col] = i
	}

	// Fast path: correlated equality lookups can be rewritten into a single batched
	// IN query and hash-joined back to seed rows, eliminating per-seed re-execution.
	if optimized, handled, err := e.tryExecuteCorrelatedBatchedLookup(ctx, seedResult, innerBody, importVars, colMap); handled || err != nil {
		return optimized, err
	}

	combinedCols := append([]string{}, seedResult.Columns...)
	combinedRows := make([][]interface{}, 0)
	var stats *QueryStats

	for _, seedRow := range seedResult.Rows {
		params := make(map[string]interface{}, len(importVars))
		correlatedBody := innerBody
		nodeBindClauses := make([]string, 0, len(importVars))
		nodeBindVars := make([]string, 0, len(importVars))
		var valueBindings []string
		for _, varName := range importVars {
			idx, ok := colMap[varName]
			if !ok {
				return nil, localizedError(localization.CypherSubqueriesWithImportUnknownVariable(varName), nil)
			}
			if idx < 0 || idx >= len(seedRow) {
				return nil, localizedError(localization.CypherSubqueriesSeedRowMissingVariable(varName), nil)
			}
			seedVal := seedRow[idx]
			// Only a node is bound as a node: a string, map or list import is
			// that value, even when it names a node's id (#648).
			if seedNode, isNode := seedVal.(*storage.Node); isNode && seedNode != nil {
				pname := "__seed_id_" + varName
				params[pname] = string(seedNode.ID)
				nodeBindClauses = append(nodeBindClauses, fmt.Sprintf("MATCH (%s) WHERE id(%s) = $%s", varName, varName, pname))
				nodeBindVars = append(nodeBindVars, varName)
				continue
			}
			// Any other value is a variable of the subquery too: bound by a
			// leading WITH, so it is the imported value wherever the body uses
			// it (SET t.p = …, t IS NULL, a map, a list), as in Neo4j (#648).
			pname := "__seed_value_" + varName
			params[pname] = seedVal
			valueBindings = append(valueBindings, "$"+pname+" AS "+varName)
		}
		if len(nodeBindClauses) > 0 || len(valueBindings) > 0 {
			// Imported nodes are bound by MATCH; a WITH carries them next to
			// the imported values only when there are values, so a body such
			// as MATCH … WITH p MERGE … keeps its shape otherwise.
			prefix := strings.Join(nodeBindClauses, " ")
			if len(valueBindings) > 0 {
				prefix = strings.TrimSpace(prefix + " WITH " + strings.Join(append(nodeBindVars, valueBindings...), ", "))
			}
			correlatedBody = prefixUnionBranches(prefix, correlatedBody)
		}

		innerRes, err := e.executeInternal(ctx, correlatedBody, params)
		if err != nil {
			return nil, localizedError(localization.CypherSubqueriesCallError(err), err)
		}
		seedRow, err = e.refreshCallSeedNodeValues(ctx, seedRow, colMap, importVars, innerRes.Stats)
		if err != nil {
			return nil, err
		}
		stats = mergeQueryStats(stats, innerRes.Stats)

		if len(innerRes.Rows) == 0 {
			// Unit subquery semantics: when a correlated subquery performs side effects
			// without RETURN, preserve the outer row.
			if len(innerRes.Columns) == 0 {
				combinedRows = append(combinedRows, append([]interface{}{}, seedRow...))
			}
			continue
		}

		innerUniqueIdx := make([]int, 0, len(innerRes.Columns))
		innerUniqueCols := make([]string, 0, len(innerRes.Columns))
		for i, col := range innerRes.Columns {
			if _, exists := colMap[col]; !exists {
				innerUniqueIdx = append(innerUniqueIdx, i)
				innerUniqueCols = append(innerUniqueCols, col)
			}
		}
		if len(combinedCols) == len(seedResult.Columns) && len(innerUniqueCols) > 0 {
			combinedCols = append(combinedCols, innerUniqueCols...)
		}

		for _, innerRow := range innerRes.Rows {
			joined := append([]interface{}{}, seedRow...)
			for _, idx := range innerUniqueIdx {
				if idx >= 0 && idx < len(innerRow) {
					joined = append(joined, innerRow[idx])
				} else {
					joined = append(joined, nil)
				}
			}
			combinedRows = append(combinedRows, joined)
		}
	}

	return &ExecuteResult{Columns: combinedCols, Rows: combinedRows, Stats: stats}, nil
}

func (e *StorageExecutor) refreshCallSeedNodeValues(ctx context.Context, seedRow []interface{}, colMap map[string]int, importVars []string, stats *QueryStats) ([]interface{}, error) {
	if !queryStatsMayContainNodeMutation(stats) {
		return seedRow, nil
	}

	importedNodeIDs := make(map[storage.NodeID]struct{}, len(importVars))
	for _, variable := range importVars {
		index, ok := colMap[variable]
		if !ok || index < 0 || index >= len(seedRow) {
			continue
		}
		if node, ok := seedRow[index].(*storage.Node); ok && node != nil {
			importedNodeIDs[node.ID] = struct{}{}
		}
	}
	if len(importedNodeIDs) == 0 {
		return seedRow, nil
	}

	checkedIDs := make(map[storage.NodeID]struct{}, len(importedNodeIDs))
	updatedNodes := make(map[storage.NodeID]*storage.Node, len(importedNodeIDs))
	var refreshed []interface{}
	for index, value := range seedRow {
		node, ok := value.(*storage.Node)
		if !ok || node == nil {
			continue
		}
		if _, imported := importedNodeIDs[node.ID]; !imported {
			continue
		}
		if _, checked := checkedIDs[node.ID]; !checked {
			checkedIDs[node.ID] = struct{}{}
			current, err := e.refreshCallSeedNodeAfterWrite(ctx, node, stats)
			if err != nil {
				return nil, err
			}
			if current != nil {
				updatedNodes[node.ID] = current
			}
		}
		if current := updatedNodes[node.ID]; current != nil {
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
	current, err := e.getStorage(ctx).GetNode(node.ID)
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
	if findKeywordIndex(trimmed, "MATCH") != 0 || !containsFold(trimmed, "RETURN ") || callSubqueryQueryIsWrite(trimmed) {
		return nil, false, nil
	}
	if containsFold(trimmed, "OPTIONAL MATCH") {
		return nil, false, nil
	}

	returnIdx := indexTopLevelKeywordCallSubquery(trimmed, "RETURN")
	if returnIdx < 0 {
		return nil, false, nil
	}
	beforeReturn := strings.TrimSpace(trimmed[:returnIdx])
	returnPart := strings.TrimSpace(trimmed[returnIdx+len("RETURN"):])
	if beforeReturn == "" || returnPart == "" {
		return nil, false, nil
	}

	matchPart := beforeReturn
	wherePart := ""
	if whereIdx := indexTopLevelKeywordCallSubquery(beforeReturn, "WHERE"); whereIdx >= 0 {
		matchPart = strings.TrimSpace(beforeReturn[:whereIdx])
		wherePart = strings.TrimSpace(beforeReturn[whereIdx+len("WHERE"):])
	}
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

	projection, modifiers := splitTopLevelResultModifiersCallSubquery(returnPart)
	projection = strings.TrimSpace(projection)
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
	terms := splitTopLevelAndConjuncts(whereClause)
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
	expr = strings.TrimSpace(expr)
	dot := strings.Index(expr, ".")
	if dot <= 0 || dot >= len(expr)-1 {
		return "", "", false
	}
	v := strings.Trim(expr[:dot], "`")
	p := strings.Trim(expr[dot+1:], "`")
	if !isSimpleIdentifier(v) || !isSimpleIdentifier(p) {
		return "", "", false
	}
	return v, p, true
}

func sanitizeCallSubqueryOtherWhere(otherWhere string, importCol string) (string, bool) {
	if strings.TrimSpace(otherWhere) == "" {
		return "", true
	}
	terms := splitTopLevelAndConjuncts(otherWhere)
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

func isCallSubqueryImportNotNullGuardTerm(term string, importCol string) bool {
	parts := strings.Fields(strings.TrimSpace(term))
	if len(parts) != 4 {
		return false
	}
	left := strings.TrimSpace(parts[0])
	left = strings.Trim(left, "`")
	if !strings.EqualFold(left, strings.TrimSpace(importCol)) {
		return false
	}
	return strings.EqualFold(parts[1], "IS") &&
		strings.EqualFold(parts[2], "NOT") &&
		strings.EqualFold(parts[3], "NULL")
}

func splitTopLevelEqualityCallSubquery(expr string) (lhs, rhs string, ok bool) {
	inSingle, inDouble, inBacktick := false, false, false
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		case '(':
			paren++
			continue
		case ')':
			if paren > 0 {
				paren--
			}
			continue
		case '[':
			bracket++
			continue
		case ']':
			if bracket > 0 {
				bracket--
			}
			continue
		case '{':
			brace++
			continue
		case '}':
			if brace > 0 {
				brace--
			}
			continue
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if ch == '=' {
			left := strings.TrimSpace(expr[:i])
			right := strings.TrimSpace(expr[i+1:])
			if left == "" || right == "" {
				return "", "", false
			}
			return left, right, true
		}
	}
	return "", "", false
}

func indexTopLevelKeywordCallSubquery(s string, keyword string) int {
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(s); i++ {
		ch := s[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
		case '"':
			inDouble = true
		case '`':
			inBacktick = true
		case '(':
			paren++
		case ')':
			if paren > 0 {
				paren--
			}
		case '[':
			bracket++
		case ']':
			if bracket > 0 {
				bracket--
			}
		case '{':
			brace++
		case '}':
			if brace > 0 {
				brace--
			}
		}
		if paren != 0 || bracket != 0 || brace != 0 {
			continue
		}
		if findKeywordIndex(s[i:], keyword) == 0 {
			return i
		}
	}
	return -1
}

func splitTopLevelResultModifiersCallSubquery(returnPart string) (projection string, modifiers string) {
	idx := indexTopLevelKeywordCallSubquery(returnPart, "ORDER BY")
	if idx < 0 {
		idx = indexTopLevelKeywordCallSubquery(returnPart, "SKIP")
	}
	if idx < 0 {
		idx = indexTopLevelKeywordCallSubquery(returnPart, "LIMIT")
	}
	if idx < 0 {
		return strings.TrimSpace(returnPart), ""
	}
	return strings.TrimSpace(returnPart[:idx]), strings.TrimSpace(returnPart[idx:])
}

func callSubqueryQueryIsWrite(query string) bool {
	// Single-pass: check each write keyword once with findKeywordIndex from position 0.
	// Previous implementation was O(n*m) — called findKeywordIndex from every byte offset.
	return findKeywordIndex(query, "CREATE") >= 0 ||
		findKeywordIndex(query, "MERGE") >= 0 ||
		findKeywordIndex(query, "DELETE") >= 0 ||
		findKeywordIndex(query, "SET") >= 0 ||
		findKeywordIndex(query, "REMOVE") >= 0
}

func isSimpleIdentifier(s string) bool {
	s = strings.TrimSpace(strings.Trim(s, "`"))
	if s == "" {
		return false
	}
	for i := 0; i < len(s); i++ {
		ch := s[i]
		if i == 0 {
			if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_') {
				return false
			}
			continue
		}
		if !((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || (ch >= '0' && ch <= '9') || ch == '_') {
			return false
		}
	}
	return true
}

func containsStandaloneIdentifier(expr, ident string) bool {
	ident = strings.TrimSpace(strings.Trim(ident, "`"))
	if ident == "" {
		return false
	}
	isWord := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
	}
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(expr); i++ {
		ch := expr[i]
		switch {
		case inSingle:
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			if ch == '`' {
				inBacktick = false
			}
			continue
		}
		switch ch {
		case '\'':
			inSingle = true
			continue
		case '"':
			inDouble = true
			continue
		case '`':
			inBacktick = true
			continue
		}
		if i+len(ident) > len(expr) {
			break
		}
		if !strings.EqualFold(expr[i:i+len(ident)], ident) {
			continue
		}
		if i > 0 {
			prev := expr[i-1]
			if isWord(prev) || prev == '.' {
				continue
			}
		}
		if i+len(ident) < len(expr) {
			next := expr[i+len(ident)]
			if isWord(next) || next == '.' {
				continue
			}
		}
		return true
	}
	return false
}

// replaceIdentifierOutsideQuotes replaces identifier tokens that are not part of
// a dotted access chain (e.g. preserves tt.translationId when replacing translationId).
// expandMapMemberAccess rewrites every occurrence of `<ident>.<key>`
// inside query into the Cypher literal of mapVal[key], leaving every
// other use of <ident> alone for the caller's standalone-replacement
// step. This is the per-token analog of property access on a bound
// map value: WITH $m AS m ... m.name must evaluate to mapVal["name"],
// not stringify into "{...}.name". When key is not present in mapVal,
// the access expands to `null` (Cypher semantics for missing map keys).
//
// The scan respects token boundaries: a previous-character word /
// underscore / dot disqualifies the match, so identifiers that happen
// to share a suffix (e.g. `prefix_m.name` or `obj.m.name`) are left
// alone.
func expandMapMemberAccess(query, ident string, mapVal map[string]interface{}) string {
	if ident == "" || query == "" || len(mapVal) == 0 {
		return query
	}
	isWord := func(b byte) bool {
		return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
	}

	var out strings.Builder
	out.Grow(len(query))
	i := 0
	for i < len(query) {
		j := strings.Index(query[i:], ident)
		if j < 0 {
			out.WriteString(query[i:])
			break
		}
		j += i
		k := j + len(ident)

		prevWord := j > 0 && (isWord(query[j-1]) || query[j-1] == '.')
		if prevWord || k >= len(query) {
			out.WriteString(query[i:k])
			i = k
			continue
		}
		var key string
		nextPos := k
		if query[k] == '.' {
			// Read the property key (longest run of word characters after `.`).
			keyStart := k + 1
			keyEnd := keyStart
			for keyEnd < len(query) && isWord(query[keyEnd]) {
				keyEnd++
			}
			if keyEnd == keyStart {
				// `.` followed by non-word: not a property access, skip.
				out.WriteString(query[i:k])
				i = k
				continue
			}
			key = query[keyStart:keyEnd]
			nextPos = keyEnd
		} else if query[k] == '[' {
			// Bracket form: ident['key'] or ident["key"].
			pos := k + 1
			for pos < len(query) && isWhitespace(query[pos]) {
				pos++
			}
			if pos >= len(query) || (query[pos] != '\'' && query[pos] != '"') {
				out.WriteString(query[i:k])
				i = k
				continue
			}
			quote := query[pos]
			pos++
			keyStart := pos
			for pos < len(query) && query[pos] != quote {
				pos++
			}
			if pos >= len(query) {
				out.WriteString(query[i:k])
				i = k
				continue
			}
			key = query[keyStart:pos]
			pos++ // close quote
			for pos < len(query) && isWhitespace(query[pos]) {
				pos++
			}
			if pos >= len(query) || query[pos] != ']' {
				out.WriteString(query[i:k])
				i = k
				continue
			}
			nextPos = pos + 1
		} else {
			// Not a member-access; leave for standalone replacement.
			out.WriteString(query[i:k])
			i = k
			continue
		}
		out.WriteString(query[i:j])
		propVal, ok := mapVal[key]
		if !ok {
			out.WriteString("null")
		} else {
			out.WriteString(valueToCypherLiteral(propVal))
		}
		i = nextPos
	}

	return out.String()
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
	if findKeywordIndex(afterCall, "RETURN") == -1 {
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
	clauses := []pipelineClause{{kind: pipelineClauseReturn, text: afterCall}}
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
	switch entity := val.(type) {
	case *storage.Node:
		if entity != nil {
			return entity.Properties[propName]
		}
	case *storage.Edge:
		if entity != nil {
			return entity.Properties[propName]
		}
	}
	if m, ok := val.(map[string]interface{}); ok {
		if pv, exists := m[propName]; exists {
			return pv
		}
		// Check nested properties map
		if props, ok := m["properties"].(map[string]interface{}); ok {
			if pv, exists := props[propName]; exists {
				return pv
			}
		}
	}
	return nil
}

func compareValuesForSort(a, b interface{}) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return 1
	}
	if b == nil {
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
		return 0
	}
	switch left := a.(type) {
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

	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

// cypherSortRank implements the openCypher comparability order used by ORDER
// BY: maps, nodes, relationships, lists, paths, strings, booleans, numbers,
// NaN, and null. Null is handled before this function.
// cypherSortRank is a value's position in Neo4j's orderability across types:
// map < node < relationship < list < path < point < zoned datetime < local
// datetime < date < zoned time < local time < duration < string < boolean <
// number < NaN (#817, #837).
func cypherSortRank(value interface{}) int {
	if _, ok := pointValue(value); ok {
		return 5
	}
	if kind, _, ok := temporalOrderParts(value); ok {
		return temporalSortRanks[kind]
	}
	switch value.(type) {
	case *storage.Node:
		return 1
	case *storage.Edge:
		return 2
	case PathResult, *PathResult:
		return 4
	case CypherDuration, *CypherDuration:
		return 11
	case string:
		return 12
	case bool:
		return 13
	}
	if object, isMap := toStringAnyMap(value); isMap {
		if _, isPath := object["_pathResult"]; isPath {
			return 4
		}
	}
	if number, ok := strictNumericValue(value); ok {
		if math.IsNaN(number) {
			return 15
		}
		return 14
	}
	typeOf := reflect.TypeOf(value)
	if typeOf != nil {
		switch typeOf.Kind() {
		case reflect.Map:
			return 0
		case reflect.Slice, reflect.Array:
			return 3
		}
	}
	return 20
}

// temporalSortRanks are the temporal kinds' positions in cypherSortRank.
var temporalSortRanks = map[string]int{"datetime": 6, "localdatetime": 7, "date": 8, "time": 9, "localtime": 10}

func cypherSortList(value interface{}) ([]interface{}, bool) {
	typeOf := reflect.TypeOf(value)
	if typeOf == nil || (typeOf.Kind() != reflect.Slice && typeOf.Kind() != reflect.Array) {
		return nil, false
	}
	return toAnySlice(value), true
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
	idx := findKeywordIndex(modifiers, keyword)
	if idx == -1 {
		return 0, false
	}
	kwPart := strings.TrimSpace(modifiers[idx+len(keyword):])
	nextKw := len(kwPart)
	for _, otherKeyword := range []string{"LIMIT", "SKIP", "ORDER BY"} {
		if otherKeyword == keyword {
			continue
		}
		if kidx := findKeywordIndex(kwPart, otherKeyword); kidx != -1 && kidx < nextKw {
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
