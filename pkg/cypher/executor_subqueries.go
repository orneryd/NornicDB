package cypher

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
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
func isCallSubquery(ctx context.Context, cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(ctx, cypher, callSubqueryRe)
}

// startsWithCallSubquery reports whether cypher begins with a CALL { } or
// CALL (vars) { } subquery clause (as opposed to a procedure CALL, or a
// subquery later in the statement).
func startsWithCallSubquery(ctx context.Context, cypher string) bool {
	return callSubqueryAt(ctx, strings.TrimSpace(cypher), 0)
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
		!containsRelationshipPattern(trimmedOuter) && patternMapsAreConstant(trimmedOuter) {
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

		np, parseErr := readNodePattern(ctx, matchBody, patternPropertyKeys)
		if parseErr != nil {
			return nil, parseErr
		}
		if np.variable != "" && !isIdentifierReferenced(trimmedOuter, variable) {
			return nil, nil
		}
		if strings.EqualFold(strings.TrimSpace(np.variable), variable) {
			rowCtx, prepared, prepareErr := e.bindPatternProperties(ctx, matchBody, nil)
			if prepareErr != nil {
				return nil, prepareErr
			}
			ctx, matchBody = rowCtx, prepared
			np, parseErr = e.parseNodePattern(ctx, matchBody)
			if parseErr != nil {
				return nil, parseErr
			}
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
				seedQuery := "MATCH " + matchBody + " WHERE " + whereClause + " RETURN " + variable
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
				seedQuery := "MATCH " + matchBody + " WHERE " + whereClause + " RETURN " + variable
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

// parseCallSubquery is the compatibility adapter for body/plan consumers. The
// shared grammar owns the body boundary and the complete transactional spec.
func (e *StorageExecutor) parseCallSubquery(ctx context.Context, cypher string) (body, afterCall string, inTransactions bool, batch *transactionBatchSpec) {
	call, err := readCallClause(ctx, cypher)
	if err != nil {
		return "", "", false, nil
	}
	body = call.Body
	if stripped, finishes := stripUnionBranchFinishes(ctx, body); finishes && strings.TrimSpace(stripped) != "" {
		body = stripped
	}
	return body, call.After, call.Batch != nil, call.Batch
}

// callSubqueryHasScopeClause reports whether a CALL subquery declares its
// imports with a scope clause (CALL () { … }, CALL (x, y) { … }, CALL (*)
// { … }). Its body's leading WITH is then an ordinary projection, never an
// import list (parseLeadingWithImports): CALL () { WITH 1 AS x FINISH } is a
// unit subquery whose body is just that WITH (#907).
func callSubqueryHasScopeClause(ctx context.Context, cypher string) bool {
	return readCallSubqueryScope(ctx, cypher).scoped
}

// parseCallSubqueryImportVariables is the legacy names-only view. Scope and
// result-schema consumers use StatementFacts, which preserves syntax errors.
func parseCallSubqueryImportVariables(ctx context.Context, cypher string) ([]string, error) {
	scope := readCallSubqueryScope(ctx, cypher)
	if scope.invalid {
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", "CALL imports must contain only variable names")
	}
	return scope.names, nil
}

// prefixUnionBranches puts prefix before body, or before each branch of a
// UNION / UNION ALL body: every branch of a CALL subquery imports the same
// outer variables.
func prefixUnionBranches(ctx context.Context, prefix, body string) string {
	branches, unionAll, mixed, ok := parseTopLevelUnionBranches(ctx, body)
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
	if pattern == "" || pattern[0] != '(' || cyphertext.FindMatchingDelimiter(pattern, 0, '(', ')', cyphertext.OpeningDelimiter) != len(pattern)-1 {
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
func (e *StorageExecutor) addLimitSkipToSubquery(ctx context.Context, subquery string, limit, skip int) string {
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
			varNames := e.extractVariableNamesFromPattern(ctx, matchPatternStructure(matchPart[5:])) // Skip "MATCH"
			varName := "n"                                                                           // Default fallback
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
	depth := 0
	for index := len("WITH"); index < len(text); index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		case '(', '[', '{':
			depth++
			continue
		case ')', ']', '}':
			if depth > 0 {
				depth--
			}
			continue
		}
		if depth > 0 || !cyphertext.NameStartAt(text, index) || cyphertext.NamePartAt(text, index-1) {
			continue
		}
		end := index + 1
		end = cyphertext.NamePartEnd(text, end)
		previous := text[index-1]
		if previous != '.' && previous != '$' && previous != ':' && isLeadingWithFollower(text[index:end]) &&
			!(equalFoldASCII(text[index:end], "with") && isOperatorWith(text, index)) {
			return text[:index], true
		}
		index = end - 1
	}
	return text, false
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
func unscopedCallImports(ctx context.Context, source string, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	body := readQueryBody(ctx, source, 0, len(source))
	for _, branch := range body.branches {
		next, imports, failure := branchSyntaxImports(branch, outer, collectNames)
		if failure != nil {
			return nil, false, failure
		}
		if imports {
			importing = true
			names = append(names, next...)
		}
	}
	return names, importing, nil
}

func branchLeadingWithImport(ctx context.Context, source string, outer func(string) bool, collectNames bool) ([]string, bool, error) {
	body := readQueryBody(ctx, source, 0, len(source))
	if len(body.branches) == 0 {
		return nil, false, nil
	}
	return branchSyntaxImports(body.branches[0], outer, collectNames)
}

func branchSyntaxImports(branch queryBodyBranch, outer func(string) bool, collectNames bool) (names []string, importing bool, err error) {
	if len(branch.clauses) == 0 || branch.clauses[0].kind != pipelineClauseWith {
		return nil, false, nil
	}
	plan := projectionPlanFromSyntax(branch.clauses[0].syntax)
	importing = plan.star
	if plan.star {
		plan = plan.withStarExpanded(nil)
	}
	for _, item := range plan.projections {
		if collectNames {
			names = append(names, item.alias)
		}
		for _, name := range expressionFreeVariables(item.expr) {
			if outer(name) {
				importing = true
			}
		}
	}
	if !importing {
		return nil, false, nil
	}
	if len(branch.clauses) == 1 && branch.useStart < 0 && !branch.finished {
		return names, true, localizedError(localization.CypherSubqueriesWithQueryClauseRequired(), nil)
	}
	return names, true, nil
}

// unscopedBranchLocals returns the outer variables (names) that one branch
// of an unscoped CALL body declares itself (branchDeclaresVariable): the
// branch doesn't receive their outer values through NornicDB's implicit
// import, so they are new variables there, as in Cypher (#907).
func unscopedBranchLocals(ctx context.Context, branch string, names []string) []string {
	var locals []string
	for _, name := range names {
		if branchDeclaresVariable(ctx, branch, name) {
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
func branchDeclaresVariable(ctx context.Context, branch, variable string) bool {
	clauses, ok := splitPipelineClauses(ctx, strings.TrimSpace(branch))
	if !ok {
		return false
	}
	for _, clause := range clauses {
		index := firstVariableOccurrence(clause.text, variable)
		if index < 0 {
			continue
		}
		switch clause.kind {
		case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
			return index < patternPartEnd(clause) && isPatternVariableAt(clause.text, index, len(variable))
		case pipelineClauseUnwind, pipelineClauseWith:
			return precededByAS(clause.text, index)
		}
		return false
	}
	return false
}

// patternPartEnd is where a MATCH, CREATE or MERGE clause's pattern ends:
// at its WHERE, or MERGE's ON CREATE / ON MATCH, else the clause's end.
func patternPartEnd(clause pipelineClause) int {
	end := len(clause.text)
	for _, keyword := range [...]string{"WHERE", "ON"} {
		if index := topLevelKeywordIndex(clause.text, keyword); index > 0 && index < end {
			end = index
		}
	}
	return end
}

// firstVariableOccurrence is the index of the first use of variable in text
// as a variable: a whole word outside string literals, not a property key
// (m.variable), a parameter ($variable) or a map key ({variable: …}).
// -1 when there is none.
func firstVariableOccurrence(text, variable string) int {
	for index := 0; index+len(variable) <= len(text); index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		}
		if text[index:index+len(variable)] != variable {
			continue
		}
		end := index + len(variable)
		if (index > 0 && cyphertext.NamePartAt(text, index-1)) || (end < len(text) && cyphertext.NamePartAt(text, end)) {
			continue
		}
		previous, next := previousNonSpace(text, index), nextNonSpace(text, end)
		if previous == '.' || previous == '$' || ((previous == '{' || previous == ',') && next == ':') {
			continue
		}
		return index
	}
	return -1
}

// isPatternVariableAt reports whether the word at index is a pattern's node
// or relationship variable ((n:L), (n), -[r:T]->) or its path name (p = …).
func isPatternVariableAt(text string, index, length int) bool {
	next := nextNonSpace(text, index+length)
	switch previousNonSpace(text, index) {
	case '(':
		// Not a function call's argument (size(n)): a name before the
		// parenthesis is a function, unless it is the clause keyword
		// (MATCH (n), OPTIONAL MATCH (n), CREATE (n), MERGE (n)).
		opening := strings.LastIndexByte(text[:index], '(')
		if word := wordBefore(text, opening); word != "" && !isPatternClauseKeyword(word) {
			return false
		}
		return next == ':' || next == ')' || next == '{'
	case '[':
		opening := strings.LastIndexByte(text[:index], '[')
		if before := previousNonSpace(text, opening); before != '-' && before != '<' {
			return false
		}
		return next == ':' || next == ']' || next == '{' || next == '*'
	}
	return next == '=' && nextNonSpace(text, strings.IndexByte(text[index:], '=')+index+1) == '('
}

// wordBefore is the identifier that ends right before index (spaces
// skipped), "" when another character comes first.
func wordBefore(text string, index int) string {
	end := index
	for end > 0 && cyphertext.IsWhitespaceAt(text, end-1) {
		end--
	}
	start := end
	for start > 0 && cyphertext.NamePartAt(text, start-1) {
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
func precededByAS(text string, index int) bool {
	end := index
	for end > 0 && cyphertext.IsWhitespaceAt(text, end-1) {
		end--
	}
	return end >= 3 && equalFoldASCII(text[end-2:end], "AS") && !cyphertext.NamePartAt(text, end-3)
}

// previousNonSpace is the last non-space byte before index, 0 at the start.
func previousNonSpace(text string, index int) byte {
	for index > 0 {
		index--
		if !cyphertext.IsWhitespaceAt(text, index) {
			return text[index]
		}
	}
	return 0
}

// nextNonSpace is the first non-space byte at or after index, 0 at the end.
func nextNonSpace(text string, index int) byte {
	for ; index < len(text); index++ {
		if !cyphertext.IsWhitespaceAt(text, index) {
			return text[index]
		}
	}
	return 0
}

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
	for strings.HasPrefix(expr, "(") && cyphertext.FindMatchingDelimiter(expr, 0, '(', ')', cyphertext.OpeningDelimiter) == len(expr)-1 {
		inner := strings.TrimSpace(expr[1 : len(expr)-1])
		if inner == expr {
			break
		}
		expr = inner
	}

	name, present := cyphertext.ScanIdentifier(expr, 0)
	if !present {
		return false, false
	}
	index, unclosed := cyphertext.GapEnd(expr, name.End)
	if unclosed >= 0 {
		return false, false
	}
	next := keywordEndAt(expr, index, "IS")
	if next == index {
		return false, false
	}
	index = next
	index, unclosed = cyphertext.GapEnd(expr, index)
	if unclosed >= 0 {
		return false, false
	}
	isNot := false
	if end := keywordEndAt(expr, index, "NOT"); end > index {
		isNot = true
		index, unclosed = cyphertext.GapEnd(expr, end)
	}
	if unclosed >= 0 {
		return false, false
	}
	next = keywordEndAt(expr, index, "NULL")
	if next == index {
		return false, false
	}
	index = next
	index, unclosed = cyphertext.GapEnd(expr, index)
	if unclosed >= 0 || index != len(expr) {
		return false, false
	}
	value, exists := vars[name.Value]
	isNull := !exists || value == nil

	if isNot {
		return !isNull, true
	}
	return isNull, true
}

// parseTopLevelUnionBranches preserves whether a composition mixes UNION and
// UNION ALL so callers can report the Cypher semantic error rather than
// treating the statement as an unsplittable query.
func parseTopLevelUnionBranches(ctx context.Context, query string) (branches []string, unionAllMode, mixed, ok bool) {
	body := readQueryBody(ctx, query, 0, len(query))
	if body.status == queryBodyDeclined || len(body.branches) < 2 {
		return nil, false, false, false
	}
	unionAllMode = true
	for i, branch := range body.branches {
		branches = append(branches, query[branch.start:branch.end])
		if i > 0 && !branch.unionAll {
			unionAllMode = false
		}
	}
	return branches, unionAllMode, body.status == queryBodyMixedUnion, true
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
	current, err := e.getQueryNode(e.getStorage(ctx), node.ID)
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

	matchVar, matchProp, otherWhere, ok := cyphertext.ExtractCorrelationWhere(wherePart, importCol, splitTopLevelAndConjuncts, splitTopLevelEqualityCallSubquery, parseCallSubqueryVarProp, isSimpleIdentifier)
	if !ok || matchVar == "" || matchProp == "" {
		return nil, false, nil
	}
	if sanitized, ok := cyphertext.FilterImportConjuncts(otherWhere, importCol, cyphertext.IdentifierUnqualifiedFold, splitTopLevelAndConjuncts); ok {
		otherWhere = sanitized
	} else {
		return nil, false, nil
	}

	projection, modifiers := splitTopLevelResultModifiersCallSubquery(returnPart)
	projection = strings.TrimSpace(projection)
	if projection == "" {
		return nil, false, nil
	}
	if _, distinct := cutDistinct(projection); distinct || projection == "*" || strings.TrimSpace(modifiers) != "" {
		// A window belongs to each correlated invocation, not the shared key
		// lookup. Let the ordinary CALL row stream execute that projection.
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

func splitTopLevelEqualityCallSubquery(expr string) (lhs, rhs string, ok bool) {
	paren, bracket, brace := 0, 0, 0
	for i := 0; i < len(expr); i++ {
		if end, quoted := skipCypherLexicalToken(expr, i); quoted {
			i = end - 1
			continue
		}
		ch := expr[i]
		switch ch {
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
	for i := 0; i < len(s); i++ {
		if end, quoted := skipCypherLexicalToken(s, i); quoted {
			i = end - 1
			continue
		}
		ch := s[i]
		switch ch {
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
	return cyphertext.IsIdentifier(strings.TrimSpace(strings.Trim(s, "`")))
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
	return parseOrderByClause(modifiers[keywordEndAt(modifiers, orderByIndex, "ORDER BY"):])
}

// parseOrderByClause parses the sort items that follow ORDER BY. The list ends
// at SKIP / LIMIT, and at the WHERE that may follow ORDER BY in a WITH clause
// (WITH n ORDER BY n.x WHERE n.y > 0): that WHERE filters the sorted rows and
// is not part of the last sort item. A word at the start is the first sort
// item, even one named like those keywords (ORDER BY skip{.a}).
func parseOrderByClause(clause string) []orderByTerm {
	clause = strings.TrimSpace(clause)
	end := len(clause)
	keywordAfterFirstItem := func(keyword string) int {
		index := topLevelKeywordIndex(clause, keyword)
		if index == 0 {
			opts := defaultKeywordScanOpts()
			opts.SkipBraces = true
			index = keywordIndexFrom(clause, keyword, len(keyword), opts)
		}
		return index
	}
	for _, keyword := range []string{"LIMIT", "SKIP"} {
		if index := keywordAfterFirstItem(keyword); index >= 0 && index < end {
			end = index
		}
	}
	if index := keywordAfterFirstItem("WHERE"); index >= 0 && index < end {
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
)
