// Cypher clause implementations for NornicDB.
// This file contains implementations for WITH, UNWIND, UNION, OPTIONAL MATCH,
// FOREACH, and LOAD CSV clauses.

package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// findStandaloneWithIndex finds the index of a standalone "WITH" keyword
// that is NOT part of "STARTS WITH" or "ENDS WITH".
// Returns -1 if not found.
func findStandaloneWithIndex(s string) int {
	opts := defaultKeywordScanOpts()

	searchFrom := 0
	for {
		absolutePos := keywordIndexFrom(s, "WITH", searchFrom, opts)
		if absolutePos == -1 {
			return -1
		}
		if !prevWordEqualsIgnoreCase(s, absolutePos, "STARTS") && !prevWordEqualsIgnoreCase(s, absolutePos, "ENDS") {
			return absolutePos
		}
		searchFrom = absolutePos + 4
	}
}

func prevWordEqualsIgnoreCase(s string, pos int, word string) bool {
	if pos <= 0 {
		return false
	}
	i := pos - 1
	for i >= 0 && isASCIISpace(s[i]) {
		i--
	}
	if i < 0 {
		return false
	}
	end := i + 1
	for i >= 0 && isIdentByte(s[i]) {
		i--
	}
	start := i + 1
	if end-start != len(word) {
		return false
	}
	for j := 0; j < len(word); j++ {
		if asciiUpper(s[start+j]) != asciiUpper(word[j]) {
			return false
		}
	}
	return true
}

// findKeywordNotInBrackets finds the index of a keyword that is NOT inside brackets [] or parentheses ()
// This is used to avoid matching keywords inside list comprehensions like [x IN list WHERE x > 2]
// The keyword should be in the format " KEYWORD " with leading/trailing spaces.
// This function normalizes whitespace (tabs, newlines) to match.
func findKeywordNotInBrackets(s string, keyword string) int {
	opts := defaultKeywordScanOpts()
	opts.SkipBraces = false
	opts.Boundary = keywordBoundaryWhitespace

	keywordCore := strings.TrimSpace(keyword)
	if keywordCore == "" {
		return -1
	}
	return keywordIndexFrom(s, keywordCore, 0, opts)
}

// isWhitespace returns true if the rune is a whitespace character
func isWhitespace(ch byte) bool {
	return isASCIISpace(ch)
}

// ========================================
// WITH Clause
// ========================================

// executeWith handles WITH clause - intermediate result projection
func (e *StorageExecutor) executeWith(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	withIdx := findKeywordIndex(cypher, "WITH")
	if withIdx == -1 {
		return nil, fmt.Errorf("WITH clause not found in query: %q", truncateQuery(cypher, 80))
	}

	remainderStart := withIdx + 4
	// Skip all whitespace (spaces, tabs, newlines)
	for remainderStart < len(cypher) && isWhitespace(cypher[remainderStart]) {
		remainderStart++
	}

	// Use findKeywordIndex which handles whitespace/newlines properly
	nextClauseKeywords := []string{
		"MATCH", "OPTIONAL MATCH", "WHERE", "RETURN",
		"CREATE", "MERGE", "DELETE", "DETACH DELETE", "SET", "REMOVE",
		"UNWIND", "CALL", "FOREACH",
		"ORDER", "SKIP", "LIMIT",
	}
	nextClauseIdx := len(cypher)
	for _, keyword := range nextClauseKeywords {
		idx := findKeywordIndex(cypher[remainderStart:], keyword)
		if idx >= 0 && remainderStart+idx < nextClauseIdx {
			nextClauseIdx = remainderStart + idx
		}
	}

	withExpr := strings.TrimSpace(cypher[remainderStart:nextClauseIdx])
	boundVars := make(map[string]interface{})

	items := e.splitWithItems(withExpr)
	columns := make([]string, 0)
	values := make([]interface{}, 0)

	for _, item := range items {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}

		upperItem := strings.ToUpper(item)
		asIdx := strings.Index(upperItem, " AS ")
		var alias string
		var expr string
		if asIdx > 0 {
			expr = strings.TrimSpace(item[:asIdx])
			alias = strings.TrimSpace(item[asIdx+4:])
		} else {
			expr = item
			alias = item
		}

		trimmedExpr := strings.TrimSpace(expr)
		var val interface{}
		if strings.HasPrefix(trimmedExpr, "{") && strings.HasSuffix(trimmedExpr, "}") {
			val = e.evaluateMapLiteral(ctx, trimmedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		} else {
			val = e.evaluateExpressionWithContext(ctx, trimmedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		}
		boundVars[alias] = val
		columns = append(columns, alias)
		values = append(values, val)
	}

	if nextClauseIdx < len(cypher) {
		remainder := strings.TrimSpace(cypher[nextClauseIdx:])

		// If it's a RETURN clause, evaluate it with the bound variables
		if strings.HasPrefix(strings.ToUpper(remainder), "RETURN") {
			returnExpr := strings.TrimSpace(remainder[6:])

			// Parse return items
			returnItems := e.parseReturnItems(returnExpr)
			returnColumns := make([]string, len(returnItems))
			returnValues := make([]interface{}, len(returnItems))

			for i, item := range returnItems {
				if item.alias != "" {
					returnColumns[i] = item.alias
				} else {
					returnColumns[i] = item.expr
				}

				// First check if it's a direct reference to a bound variable
				if val, ok := boundVars[item.expr]; ok {
					returnValues[i] = val
				} else {
					// Substitute bound variables in the expression
					expr := item.expr
					for varName, varVal := range boundVars {
						// Replace the variable name in the expression
						// Handle list comprehension: [x IN varName WHERE ...] -> [x IN [1,2,3] WHERE ...]
						if strings.Contains(expr, varName) {
							// Convert value to string representation
							var replacement string
							switch v := varVal.(type) {
							case []interface{}:
								parts := make([]string, len(v))
								for j, elem := range v {
									switch e := elem.(type) {
									case string:
										parts[j] = fmt.Sprintf("'%s'", e)
									default:
										parts[j] = fmt.Sprintf("%v", e)
									}
								}
								replacement = "[" + strings.Join(parts, ", ") + "]"
							case string:
								replacement = fmt.Sprintf("'%s'", v)
							default:
								replacement = fmt.Sprintf("%v", v)
							}
							expr = strings.ReplaceAll(expr, varName, replacement)
						}
					}
					returnValues[i] = e.evaluateExpressionWithContext(ctx, expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
				}
			}

			return &ExecuteResult{
				Columns: returnColumns,
				Rows:    [][]interface{}{returnValues},
			}, nil
		}

		// Substitute bound variables into remainder before delegating
		// e.g., WITH [[1,2],[3,4]] AS matrix UNWIND matrix ... -> UNWIND [[1,2],[3,4]] ...
		substitutedRemainder := remainder
		for varName, varVal := range boundVars {
			switch v := varVal.(type) {
			case []interface{}:
				parts := make([]string, len(v))
				for j, elem := range v {
					switch e := elem.(type) {
					case []interface{}:
						innerParts := make([]string, len(e))
						for k, innerElem := range e {
							switch ie := innerElem.(type) {
							case string:
								innerParts[k] = fmt.Sprintf("'%s'", ie)
							default:
								innerParts[k] = fmt.Sprintf("%v", ie)
							}
						}
						parts[j] = "[" + strings.Join(innerParts, ", ") + "]"
					case string:
						parts[j] = fmt.Sprintf("'%s'", e)
					default:
						parts[j] = fmt.Sprintf("%v", e)
					}
				}
				replacement := "[" + strings.Join(parts, ", ") + "]"
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, replacement)
			case string:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, fmt.Sprintf("'%s'", v))
			case map[string]interface{}:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, mapToCypherLiteral(v))
			case map[interface{}]interface{}:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, mapToCypherLiteral(normalizeInterfaceMap(v)))
			case nil:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, "null")
			default:
				substitutedRemainder = replaceStandaloneCypherIdentifier(substitutedRemainder, varName, fmt.Sprintf("%v", v))
			}
		}
		return e.executeInternal(ctx, substitutedRemainder, nil)
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

func normalizeInterfaceMap(input map[interface{}]interface{}) map[string]interface{} {
	output := make(map[string]interface{}, len(input))
	for k, v := range input {
		key := fmt.Sprintf("%v", k)
		output[key] = v
	}
	return output
}

func mapToCypherLiteral(m map[string]interface{}) string {
	var parts []string
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s: %s", k, valueToCypherLiteral(v)))
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

func valueToCypherLiteral(v interface{}) string {
	switch val := v.(type) {
	case string:
		return fmt.Sprintf("'%s'", val)
	case bool:
		if val {
			return "true"
		}
		return "false"
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return fmt.Sprintf("%v", val)
	case map[string]interface{}:
		return mapToCypherLiteral(val)
	case map[interface{}]interface{}:
		return mapToCypherLiteral(normalizeInterfaceMap(val))
	case []interface{}:
		items := make([]string, len(val))
		for i, item := range val {
			items[i] = valueToCypherLiteral(item)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case nil:
		return "null"
	default:
		return fmt.Sprintf("%v", val)
	}
}

// splitWithItems splits WITH expressions respecting nested brackets and quotes
func (e *StorageExecutor) splitWithItems(expr string) []string {
	var items []string
	var current strings.Builder
	depth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, c := range expr {
		switch {
		case c == '\'' || c == '"':
			if !inQuote {
				inQuote = true
				quoteChar = c
			} else if c == quoteChar {
				inQuote = false
			}
			current.WriteRune(c)
		case c == '(' || c == '[' || c == '{':
			if !inQuote {
				depth++
			}
			current.WriteRune(c)
		case c == ')' || c == ']' || c == '}':
			if !inQuote {
				depth--
			}
			current.WriteRune(c)
		case c == ',' && depth == 0 && !inQuote:
			items = append(items, current.String())
			current.Reset()
		default:
			current.WriteRune(c)
		}
	}
	if current.Len() > 0 {
		items = append(items, current.String())
	}
	return items
}

// ========================================
// UNWIND Clause
// ========================================

// executeUnwind handles UNWIND clause - list expansion
func (e *StorageExecutor) executeUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Check for double UNWIND - handle by recursively processing
	firstUnwind := findKeywordIndex(cypher, "UNWIND")
	if firstUnwind >= 0 {
		// Find the AS clause for the first UNWIND
		afterFirstUnwind := upper[firstUnwind+6:]
		firstAsIdx := strings.Index(afterFirstUnwind, " AS ")
		if firstAsIdx >= 0 {
			// Find where the variable ends (next space or UNWIND)
			varStart := firstAsIdx + 4
			restAfterAs := strings.TrimSpace(afterFirstUnwind[varStart:])
			varEndIdx := strings.IndexAny(restAfterAs, " \t\n")
			if varEndIdx > 0 {
				restAfterVar := strings.TrimSpace(restAfterAs[varEndIdx:])
				// Check if there's another UNWIND
				if strings.HasPrefix(strings.ToUpper(restAfterVar), "UNWIND") {
					// Handle double UNWIND by unwinding the first list and processing second UNWIND for each
					return e.executeDoubleUnwind(ctx, cypher)
				}
			}
		}
	}

	// Check for unsupported map keys() function
	if strings.Contains(upper, "KEYS(") && strings.Contains(upper, "UNWIND") {
		return nil, fmt.Errorf("keys() function with UNWIND is not supported in this context")
	}

	unwindIdx := findKeywordIndex(cypher, "UNWIND")
	if unwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found in query: %q", truncateQuery(cypher, 80))
	}

	afterUnwind := cypher[unwindIdx+6:]
	asRelIdx := findKeywordNotInBrackets(afterUnwind, " AS ")
	if asRelIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
	}

	asIdx := unwindIdx + 6 + asRelIdx
	listExpr := strings.TrimSpace(cypher[unwindIdx+6 : asIdx])

	remainderStart := asIdx + len("AS")
	for remainderStart < len(cypher) && isASCIISpace(cypher[remainderStart]) {
		remainderStart++
	}
	remainder := strings.TrimSpace(cypher[remainderStart:])
	spaceIdx := strings.IndexAny(remainder, " \t\r\n")
	var variable string
	var restQuery string
	if spaceIdx > 0 {
		variable = strings.TrimSpace(remainder[:spaceIdx])
		restQuery = strings.TrimSpace(remainder[spaceIdx:])
	} else {
		variable = strings.TrimSpace(remainder)
		restQuery = ""
	}

	params := getParamsFromContext(ctx)
	unwindParamName := ""
	var list interface{}
	if strings.HasPrefix(strings.TrimSpace(listExpr), "$") {
		paramName := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(listExpr), "$"))
		if paramName == "" {
			return nil, fmt.Errorf("UNWIND requires a valid parameter name after $")
		}
		unwindParamName = paramName
		if params == nil {
			return nil, fmt.Errorf("UNWIND parameter $%s requires parameters to be provided", paramName)
		}
		paramValue, exists := params[paramName]
		if !exists {
			return nil, fmt.Errorf("UNWIND parameter $%s not found in provided parameters", paramName)
		}
		list = paramValue
	} else {
		listExprEval := listExpr
		if params != nil {
			listExprEval = e.substituteParams(listExprEval, params)
		}
		list = e.evaluateExpressionWithContext(ctx, listExprEval, make(map[string]*storage.Node), make(map[string]*storage.Edge))
	}

	var items []interface{}
	switch v := list.(type) {
	case nil:
		// UNWIND null produces no rows (Neo4j compatible)
		items = []interface{}{}
	case []interface{}:
		items = v
	case []string:
		items = make([]interface{}, len(v))
		for i, s := range v {
			items[i] = s
		}
	case []int64:
		items = make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
	case []float64:
		items = make([]interface{}, len(v))
		for i, n := range v {
			items[i] = n
		}
	case []map[string]interface{}:
		items = make([]interface{}, len(v))
		for i := range v {
			items[i] = v[i]
		}
	default:
		rv := reflect.ValueOf(list)
		if rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) {
			items = make([]interface{}, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				items[i] = rv.Index(i).Interface()
			}
		} else {
			// Single value gets wrapped in a list
			items = []interface{}{list}
		}
	}

	// Handle UNWIND ... CREATE/MERGE/MATCH ... mutation patterns.
	if restQuery != "" {
		trimmedRest := strings.TrimSpace(restQuery)
		upperRest := strings.ToUpper(trimmedRest)
		matchStartedMutation := (strings.HasPrefix(upperRest, "MATCH") || strings.HasPrefix(upperRest, "OPTIONAL MATCH")) &&
			(findKeywordIndexInContext(trimmedRest, "MERGE") >= 0 ||
				findKeywordIndexInContext(trimmedRest, "CREATE") >= 0 ||
				findKeywordIndexInContext(trimmedRest, "SET") >= 0)
		if matchStartedMutation {
			// Bulk-seed fast path: UNWIND $rows MATCH (...) [MATCH (...)...]
			// CREATE (...) [CREATE (...)...] without RETURN / WITH / nested
			// UNWIND. Parses the mutation body once, then runs each row
			// directly against storage (no per-row Cypher re-parse).
			if strings.HasPrefix(upperRest, "MATCH") {
				if fast, ok, err := e.executeUnwindMultiMatchCreateBatch(ctx, variable, items, restQuery); ok {
					return fast, err
				}
			}
			if strings.HasPrefix(upperRest, "MATCH") {
				if fast, ok, err := e.executeUnwindFixedChainLinkBatch(ctx, variable, items, restQuery); ok {
					return fast, err
				}
			}
			if fast, ok, err := e.executeUnwindCompoundMutationBatch(ctx, variable, unwindParamName, items, restQuery); ok {
				return fast, err
			}
			returnIdx := findKeywordIndex(restQuery, "RETURN")
			var mutationPart, returnPart string
			if returnIdx > 0 {
				mutationPart = strings.TrimSpace(restQuery[:returnIdx])
				returnPart = strings.TrimSpace(restQuery[returnIdx:])
			} else {
				mutationPart = restQuery
				returnPart = ""
			}
			if startsWithKeywordFold(strings.TrimSpace(mutationPart), "MATCH") ||
				startsWithKeywordFold(strings.TrimSpace(mutationPart), "OPTIONAL MATCH") {
				if fast, ok, err := e.executeUnwindMergeChainBatch(ctx, variable, items, mutationPart, returnPart); ok {
					return fast, err
				}
			}
		}
		if strings.HasPrefix(upperRest, "CREATE") ||
			strings.HasPrefix(upperRest, "MERGE") ||
			(strings.HasPrefix(upperRest, "OPTIONAL MATCH") && matchStartedMutation) {
			if fast, ok, err := e.executeUnwindCompoundMutationBatch(ctx, variable, unwindParamName, items, restQuery); ok {
				return fast, err
			}

			result := &ExecuteResult{
				Columns: []string{},
				Rows:    [][]interface{}{},
				Stats:   &QueryStats{},
			}

			// Split mutation and RETURN parts
			returnIdx := findKeywordIndex(restQuery, "RETURN")
			var mutationPart, returnPart string
			if returnIdx > 0 {
				mutationPart = strings.TrimSpace(restQuery[:returnIdx])
				returnPart = strings.TrimSpace(restQuery[returnIdx:])
			} else {
				mutationPart = restQuery
				returnPart = ""
			}

			// Hot path: UNWIND row [MATCH ...] MERGE (n:Label {k: row.x})
			// [SET n.p = row.y | SET n += row.props] [MERGE relationship] RETURN count(n).
			// Execute as a single batched pass to avoid per-item MERGE query re-parsing/scans.
			if startsWithKeywordFold(strings.TrimSpace(mutationPart), "MERGE") {
				if fast, ok, err := e.executeUnwindMergeChainBatch(ctx, variable, items, mutationPart, returnPart); ok {
					return fast, err
				}
			}

			// Execute mutation for each unwound item
			for _, item := range items {
				// Reconstruct full query with RETURN.
				fullQuery := mutationPart
				if returnPart != "" {
					fullQuery += " " + returnPart
				}

				// For mutation paths that depend on map sources or preserve row alias semantics
				// in WITH pipelines, execute with per-item parameters instead of literal substitution.
				// This preserves complex map/string payloads and avoids corrupting clauses like
				// `WITH cc, row, csByID` when `row` is a map.
				// Also covers mutations that pass `row` through a WITH clause to a
				// nested UNWIND (UNWIND row.xs AS x) — literal-substituting `row`
				// with a map literal breaks the parser, and substituting it with
				// `{}` loses the property list the inner UNWIND needs.
				trimmedMutation := strings.TrimSpace(mutationPart)
				upperMutation := strings.ToUpper(trimmedMutation)
				useParamExecution := strings.Contains(mutationPart, "+=") ||
					strings.HasPrefix(upperMutation, "OPTIONAL MATCH") ||
					(findKeywordIndexInContext(mutationPart, "WITH") >= 0 &&
						findKeywordIndexInContext(mutationPart, "UNWIND") >= 0)

				var mutationResult *ExecuteResult
				var err error
				if useParamExecution {
					callParams := make(map[string]interface{}, len(params)+1)
					for k, v := range params {
						callParams[k] = v
					}
					callParams[variable] = item
					if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(mutationPart)), "OPTIONAL MATCH") &&
						findKeywordIndexInContext(mutationPart, "MERGE") > 0 {
						substitutedMutation := e.replaceVariableInMutationQuery(mutationPart, variable, item)
						substitutedFull := substitutedMutation
						if returnPart != "" {
							substitutedFull += " " + returnPart
						}
						mutationResult, err = e.executeCompoundMatchMerge(context.WithValue(ctx, paramsKey, params), substitutedFull)
					} else {
						mutationResult, err = e.executeInternal(ctx, fullQuery, callParams)
					}
				} else {
					// Replace variable references ONLY in the mutation clause
					mutationQuerySubstituted := e.replaceVariableInMutationQuery(mutationPart, variable, item)
					substitutedFull := mutationQuerySubstituted
					if returnPart != "" {
						substitutedFull += " " + returnPart
					}
					trimmed := strings.TrimSpace(substitutedFull)
					// Route MERGE-heavy mutation chains through context-aware MERGE execution.
					// This avoids brittle top-level MERGE parsing for shapes like:
					// MERGE (...) MERGE (...) SET ... MERGE (...) ...
					if strings.HasPrefix(strings.ToUpper(trimmed), "MERGE ") {
						complexMergeChain := findKeywordIndexInContext(trimmed, "FOREACH") > 0 || findKeywordIndexInContext(trimmed, "WITH") > 0
						// Queries containing MATCH after MERGE (e.g. MERGE ... MATCH ... MERGE rel)
						// are better handled by the regular executor route so MATCH bindings are
						// preserved for downstream relationship merges.
						if findKeywordIndexInContext(trimmed, "MATCH") > 0 || complexMergeChain {
							mutationResult, err = e.executeInternal(ctx, substitutedFull, params)
						} else {
							mutationResult, err = e.executeMergeWithContext(ctx, trimmed, make(map[string]*storage.Node), make(map[string]*storage.Edge))
						}
					} else {
						mutationResult, err = e.executeInternal(ctx, substitutedFull, params)
					}
				}
				if err != nil {
					return nil, fmt.Errorf("UNWIND mutation failed: %w", err)
				}

				// Accumulate stats when available. Some execution paths can return a
				// nil Stats pointer even when the side-effect succeeded.
				if mutationResult != nil && mutationResult.Stats != nil {
					result.Stats.NodesCreated += mutationResult.Stats.NodesCreated
					result.Stats.RelationshipsCreated += mutationResult.Stats.RelationshipsCreated
				}

				// If there's a RETURN clause, collect the result rows
				if mutationResult != nil && returnPart != "" && len(mutationResult.Rows) > 0 {
					// First iteration: set columns
					if len(result.Columns) == 0 {
						result.Columns = mutationResult.Columns
					}
					// Append all rows from this per-item execution
					result.Rows = append(result.Rows, mutationResult.Rows...)
				}
			}

			return result, nil
		}
	}

	// Handle UNWIND ... WITH collect(DISTINCT var.prop) AS alias RETURN alias
	// for batched key extraction pipelines used by Fabric APPLY execution.
	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "WITH ") {
		if res, ok := e.executeUnwindWithCollectProjection(variable, items, restQuery); ok {
			return res, nil
		}
	}

	// Handle UNWIND ... MATCH ... RETURN ... by evaluating MATCH per unwound value
	// and combining results. This avoids silently returning only unwound values when
	// a trailing MATCH pipeline is present.
	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "MATCH ") {
		returnIdx := findKeywordIndex(restQuery, "RETURN")
		mutationPart := restQuery
		returnPart := ""
		if returnIdx > 0 {
			mutationPart = strings.TrimSpace(restQuery[:returnIdx])
			returnPart = strings.TrimSpace(restQuery[returnIdx:])
		}
		if fast, ok, err := e.executeUnwindMergeChainBatch(ctx, variable, items, mutationPart, returnPart); ok {
			return fast, err
		}
		if fast, ok, err := e.executeUnwindFixedChainLinkBatch(ctx, variable, items, restQuery); ok {
			return fast, err
		}
		normalizedRestQuery := normalizeMultiMatchWhereClauses(restQuery)
		// Fast path: set-based rewrite for correlated MATCH pipelines that return
		// COUNT(...). This avoids per-item subquery execution in UNWIND loops.
		if canApplySetBasedUnwindRewrite(normalizedRestQuery, items) {
			if rewritten, ok := rewriteUnwindCorrelationToIn(normalizedRestQuery, variable, "__unwind_items"); ok {
				rewritten = rewriteTopLevelMultiMatchToCartesianMatch(rewritten)
				callParams := make(map[string]interface{}, len(params)+1)
				for k, v := range params {
					callParams[k] = v
				}
				callParams["__unwind_items"] = items
				rewrittenResult, err := e.Execute(ctx, rewritten, callParams)
				if err == nil {
					return rewrittenResult, nil
				}
			}
		}

		returnItems := []returnItem{}
		if retIdx := findKeywordIndex(normalizedRestQuery, "RETURN"); retIdx > 0 {
			returnClause := strings.TrimSpace(normalizedRestQuery[retIdx+6:])
			returnEnd := len(returnClause)
			for _, keyword := range []string{"ORDER", "SKIP", "LIMIT"} {
				if idx := findKeywordIndexInContext(returnClause, keyword); idx >= 0 && idx < returnEnd {
					returnEnd = idx
				}
			}
			returnClause = strings.TrimSpace(returnClause[:returnEnd])
			returnItems = e.parseReturnItems(returnClause)
		}

		aggregateCountOnly := len(returnItems) == 1 && isAggregateFuncName(returnItems[0].expr, "count")
		var aggregatedCount int64
		result := &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}

		for _, item := range items {
			substitutedQuery := e.replaceVariableInQuery(normalizedRestQuery, variable, item)
			subResult, err := e.Execute(ctx, substitutedQuery, params)
			if err != nil {
				return nil, fmt.Errorf("UNWIND MATCH failed: %w", err)
			}
			if subResult == nil {
				continue
			}
			if len(result.Columns) == 0 {
				result.Columns = append([]string(nil), subResult.Columns...)
			}
			if aggregateCountOnly {
				if len(subResult.Rows) == 0 || len(subResult.Rows[0]) == 0 {
					continue
				}
				switch v := subResult.Rows[0][0].(type) {
				case int64:
					aggregatedCount += v
				case int:
					aggregatedCount += int64(v)
				case float64:
					aggregatedCount += int64(v)
				}
				continue
			}
			result.Rows = append(result.Rows, subResult.Rows...)
		}

		if aggregateCountOnly {
			if len(result.Columns) == 0 {
				if returnItems[0].alias != "" {
					result.Columns = []string{returnItems[0].alias}
				} else {
					result.Columns = []string{returnItems[0].expr}
				}
			}
			result.Rows = [][]interface{}{{aggregatedCount}}
		}
		return result, nil
	}

	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "RETURN") {
		returnClause := strings.TrimSpace(restQuery[6:])
		returnItems := e.parseReturnItems(returnClause)

		// Check if any return items are aggregation functions
		hasAggregation := false
		for _, item := range returnItems {
			upperExpr := strings.ToUpper(item.expr)
			if strings.HasPrefix(upperExpr, "SUM(") || strings.HasPrefix(upperExpr, "COUNT(") ||
				strings.HasPrefix(upperExpr, "AVG(") || strings.HasPrefix(upperExpr, "MIN(") ||
				strings.HasPrefix(upperExpr, "MAX(") || strings.HasPrefix(upperExpr, "COLLECT(") {
				hasAggregation = true
				break
			}
		}

		if hasAggregation {
			// Aggregate across all unwound items
			result := &ExecuteResult{
				Columns: make([]string, len(returnItems)),
				Rows:    [][]interface{}{make([]interface{}, len(returnItems))},
			}

			for i, item := range returnItems {
				if item.alias != "" {
					result.Columns[i] = item.alias
				} else {
					result.Columns[i] = item.expr
				}

				upperExpr := strings.ToUpper(item.expr)
				switch {
				case strings.HasPrefix(upperExpr, "SUM("):
					inner := item.expr[4 : len(item.expr)-1]
					var sum float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								sum += n
							}
						}
					}
					result.Rows[0][i] = int64(sum) // Return as int64 for integer sums
				case strings.HasPrefix(upperExpr, "COUNT("):
					result.Rows[0][i] = int64(len(items))
				case strings.HasPrefix(upperExpr, "AVG("):
					inner := item.expr[4 : len(item.expr)-1]
					var sum float64
					var count int
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								sum += n
								count++
							}
						}
					}
					if count > 0 {
						result.Rows[0][i] = sum / float64(count)
					} else {
						result.Rows[0][i] = nil
					}
				case strings.HasPrefix(upperExpr, "MIN("):
					inner := item.expr[4 : len(item.expr)-1]
					var min *float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								if min == nil || n < *min {
									min = &n
								}
							}
						}
					}
					if min != nil {
						result.Rows[0][i] = *min
					}
				case strings.HasPrefix(upperExpr, "MAX("):
					inner := item.expr[4 : len(item.expr)-1]
					var max *float64
					for _, it := range items {
						if inner == variable {
							if n, ok := toFloat64(it); ok {
								if max == nil || n > *max {
									max = &n
								}
							}
						}
					}
					if max != nil {
						result.Rows[0][i] = *max
					}
				case strings.HasPrefix(upperExpr, "COLLECT("):
					inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
					collected := make([]interface{}, 0, len(items))
					for _, it := range items {
						if inner == variable {
							collected = append(collected, it)
						}
					}
					// Apply suffix (e.g., [..10] for slicing) if present
					if suffix != "" {
						result.Rows[0][i] = e.applyArraySuffix(collected, suffix)
					} else {
						result.Rows[0][i] = collected
					}
				}
			}
			return result, nil
		}

		// No aggregation - return individual rows
		result := &ExecuteResult{
			Columns: make([]string, len(returnItems)),
			Rows:    make([][]interface{}, 0, len(items)),
		}
		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}
		for _, item := range items {
			row := make([]interface{}, len(returnItems))
			rowValues := map[string]interface{}{variable: item}
			for i, ri := range returnItems {
				row[i] = e.evaluateExpressionFromValues(ri.expr, rowValues)
			}
			result.Rows = append(result.Rows, row)
		}
		return result, nil
	}

	result := &ExecuteResult{
		Columns: []string{variable},
		Rows:    make([][]interface{}, 0, len(items)),
	}
	for _, item := range items {
		result.Rows = append(result.Rows, []interface{}{item})
	}
	return result, nil
}

type unwindSimpleSetAssignment struct {
	prop     string
	expr     string
	mergeMap bool
}

type unwindMergeChainNodePlan struct {
	mergeVar            string
	labels              []string
	matchAssignments    []unwindSimpleSetAssignment
	setAssignments      []unwindSimpleSetAssignment
	onCreateAssignments []unwindSimpleSetAssignment
	onMatchAssignments  []unwindSimpleSetAssignment
}

type unwindMergeChainLookupPlan struct {
	varName          string
	labels           []string
	matchAssignments []unwindSimpleSetAssignment
	setAssignments   []unwindSimpleSetAssignment
	optional         bool
	anyLabel         bool
}

type unwindMergeChainWithAssignment struct {
	alias string
	expr  string
}

type unwindMergeChainWithPlan struct {
	assignments []unwindMergeChainWithAssignment
}

type unwindMergeChainWherePlan struct {
	clause string
}

type unwindMergeChainRelationshipPlan struct {
	fromVar        string
	toVar          string
	relVar         string
	relType        string
	setAssignments []unwindSimpleSetAssignment
}

type unwindMergeChainStep struct {
	node         *unwindMergeChainNodePlan
	lookup       *unwindMergeChainLookupPlan
	with         *unwindMergeChainWithPlan
	where        *unwindMergeChainWherePlan
	relationship *unwindMergeChainRelationshipPlan
}

type unwindMergeChainPlan struct {
	supported bool
	simple    bool
	steps     []unwindMergeChainStep
}

func parseUnwindSimpleMergeMatchAssignments(raw string) ([]unwindSimpleSetAssignment, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, false
	}
	parts := splitTopLevelComma(trimmed)
	out := make([]unwindSimpleSetAssignment, 0, len(parts))
	for _, part := range parts {
		entry := strings.TrimSpace(part)
		if entry == "" {
			continue
		}
		colonIdx := strings.Index(entry, ":")
		if colonIdx <= 0 || colonIdx == len(entry)-1 {
			return nil, false
		}
		prop := strings.TrimSpace(entry[:colonIdx])
		expr := strings.TrimSpace(entry[colonIdx+1:])
		if prop == "" || expr == "" {
			return nil, false
		}
		out = append(out, unwindSimpleSetAssignment{prop: prop, expr: expr})
	}
	if len(out) == 0 {
		return nil, false
	}
	return out, true
}

func parseUnwindSimpleSetAssignments(raw, mergeVar string) ([]unwindSimpleSetAssignment, bool) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, true
	}
	assignments := splitTopLevelComma(trimmed)
	out := make([]unwindSimpleSetAssignment, 0, len(assignments))
	for _, assignment := range assignments {
		a := strings.TrimSpace(assignment)
		if a == "" {
			continue
		}
		if plusEqIdx := strings.Index(a, "+="); plusEqIdx > 0 {
			left := strings.TrimSpace(a[:plusEqIdx])
			right := strings.TrimSpace(a[plusEqIdx+2:])
			if left != mergeVar || right == "" {
				return nil, false
			}
			out = append(out, unwindSimpleSetAssignment{
				expr:     right,
				mergeMap: true,
			})
			continue
		}
		eqIdx := strings.Index(a, "=")
		if eqIdx <= 0 {
			return nil, false
		}
		left := strings.TrimSpace(a[:eqIdx])
		right := strings.TrimSpace(a[eqIdx+1:])
		dotIdx := strings.Index(left, ".")
		if dotIdx <= 0 || dotIdx == len(left)-1 {
			return nil, false
		}
		leftVar := strings.TrimSpace(left[:dotIdx])
		prop := strings.TrimSpace(left[dotIdx+1:])
		if leftVar != mergeVar || prop == "" {
			return nil, false
		}
		out = append(out, unwindSimpleSetAssignment{
			prop: prop,
			expr: right,
		})
	}
	return out, true
}

func parseSimpleUnwindMergeClause(mergePart string) (string, []string, []unwindSimpleSetAssignment, bool) {
	return parseUnwindNodePatternClause(mergePart, "MERGE")
}

func parseUnwindNodePatternClause(clause string, keyword string) (string, []string, []unwindSimpleSetAssignment, bool) {
	varName, labels, assignments, _, ok := parseUnwindNodePatternClauseInternal(clause, keyword, false)
	return varName, labels, assignments, ok
}

func parseUnwindNodePatternClauseInternal(clause string, keyword string, allowAlternatives bool) (string, []string, []unwindSimpleSetAssignment, bool, bool) {
	trimmed := strings.TrimSpace(clause)
	if !startsWithKeywordFold(trimmed, keyword) {
		return "", nil, nil, false, false
	}
	rest := strings.TrimSpace(trimmed[len(keyword):])
	if !strings.HasPrefix(rest, "(") {
		return "", nil, nil, false, false
	}
	parenEnd := findMatchingParen(rest, 0)
	if parenEnd < 0 || strings.TrimSpace(rest[parenEnd+1:]) != "" {
		return "", nil, nil, false, false
	}
	nodePattern := strings.TrimSpace(rest[1:parenEnd])
	braceStart := strings.Index(nodePattern, "{")
	if braceStart < 0 {
		return "", nil, nil, false, false
	}
	exec := &StorageExecutor{}
	braceEnd := exec.findMatchingBrace(nodePattern, braceStart)
	if braceEnd < 0 || strings.TrimSpace(nodePattern[braceEnd+1:]) != "" {
		return "", nil, nil, false, false
	}
	head := strings.TrimSpace(nodePattern[:braceStart])
	propMap := strings.TrimSpace(nodePattern[braceStart+1 : braceEnd])
	if head == "" || propMap == "" {
		return "", nil, nil, false, false
	}
	parts := strings.Split(head, ":")
	if len(parts) < 2 {
		return "", nil, nil, false, false
	}
	mergeVar := strings.TrimSpace(parts[0])
	if !isSimpleIdentifier(mergeVar) {
		return "", nil, nil, false, false
	}
	labels := make([]string, 0, len(parts)-1)
	anyLabel := false
	for i := 1; i < len(parts); i++ {
		label := strings.TrimSpace(parts[i])
		if strings.Contains(label, "|") {
			if !allowAlternatives || anyLabel || len(parts) != 2 {
				return "", nil, nil, false, false
			}
			for _, alternative := range strings.Split(label, "|") {
				alternative = strings.TrimSpace(alternative)
				if !isSimpleIdentifier(alternative) {
					return "", nil, nil, false, false
				}
				labels = append(labels, alternative)
			}
			anyLabel = true
			continue
		}
		if !isSimpleIdentifier(label) {
			return "", nil, nil, false, false
		}
		labels = append(labels, label)
	}
	if len(labels) == 0 {
		return "", nil, nil, false, false
	}
	assignments, ok := parseUnwindSimpleMergeMatchAssignments(propMap)
	if !ok {
		return "", nil, nil, false, false
	}
	return mergeVar, labels, assignments, anyLabel, true
}

func parseUnwindLookupClause(clause string) (unwindMergeChainLookupPlan, bool) {
	optional := startsWithKeywordFold(strings.TrimSpace(clause), "OPTIONAL MATCH")
	keyword := "MATCH"
	if optional {
		keyword = "OPTIONAL MATCH"
	}
	varName, labels, assignments, anyLabel, ok := parseUnwindNodePatternClauseInternal(clause, keyword, true)
	if !ok {
		return unwindMergeChainLookupPlan{}, false
	}
	return unwindMergeChainLookupPlan{
		varName:          varName,
		labels:           labels,
		matchAssignments: assignments,
		optional:         optional,
		anyLabel:         anyLabel,
	}, true
}

func unwindLookupSetHasUniqueAnchor(schema *storage.SchemaManager, lookupPlan unwindMergeChainLookupPlan) bool {
	if schema == nil || lookupPlan.anyLabel || len(lookupPlan.labels) != 1 || len(lookupPlan.matchAssignments) == 0 {
		return false
	}
	matchedProps := make(map[string]struct{}, len(lookupPlan.matchAssignments))
	for _, assignment := range lookupPlan.matchAssignments {
		matchedProps[assignment.prop] = struct{}{}
	}
	for _, constraint := range schema.GetConstraintsForLabels([]string{lookupPlan.labels[0]}) {
		if constraint.EffectiveEntityType() != storage.ConstraintEntityNode {
			continue
		}
		if constraint.Type != storage.ConstraintUnique && constraint.Type != storage.ConstraintNodeKey {
			continue
		}
		if len(constraint.Properties) == 0 {
			continue
		}
		allConstraintPropsMatched := true
		for _, prop := range constraint.Properties {
			if _, ok := matchedProps[prop]; !ok {
				allConstraintPropsMatched = false
				break
			}
		}
		if allConstraintPropsMatched {
			return true
		}
	}
	return false
}

func unwindMergeChainMatchSetLookupsAreUnique(store storage.Engine, plan unwindMergeChainPlan) bool {
	schema := store.GetSchema()
	for _, step := range plan.steps {
		if step.lookup == nil || len(step.lookup.setAssignments) == 0 {
			continue
		}
		if !unwindLookupSetHasUniqueAnchor(schema, *step.lookup) {
			return false
		}
	}
	return true
}

func unwindMergeLabelsKey(labels []string) string {
	if len(labels) == 0 {
		return ""
	}
	sorted := append([]string(nil), labels...)
	sort.Strings(sorted)
	return strings.Join(sorted, ":")
}

func parseUnwindWithClause(clause string) (unwindMergeChainWithPlan, bool) {
	trimmed := strings.TrimSpace(clause)
	if !startsWithKeywordFold(trimmed, "WITH") {
		return unwindMergeChainWithPlan{}, false
	}
	body := strings.TrimSpace(trimmed[len("WITH"):])
	if body == "" {
		return unwindMergeChainWithPlan{}, false
	}
	parts := splitTopLevelComma(body)
	plan := unwindMergeChainWithPlan{}
	for _, part := range parts {
		item := strings.TrimSpace(part)
		if item == "" {
			continue
		}
		if isSimpleIdentifier(item) {
			continue
		}
		asIdx := findKeywordIndexInContext(item, "AS")
		if asIdx <= 0 {
			return unwindMergeChainWithPlan{}, false
		}
		expr := strings.TrimSpace(item[:asIdx])
		alias := strings.TrimSpace(item[asIdx+2:])
		if expr == "" || !isSimpleIdentifier(alias) {
			return unwindMergeChainWithPlan{}, false
		}
		plan.assignments = append(plan.assignments, unwindMergeChainWithAssignment{alias: alias, expr: expr})
	}
	return plan, true
}

func parseUnwindWhereClause(clause string) (unwindMergeChainWherePlan, bool) {
	trimmed := strings.TrimSpace(clause)
	if !startsWithKeywordFold(trimmed, "WHERE") {
		return unwindMergeChainWherePlan{}, false
	}
	body := strings.TrimSpace(trimmed[len("WHERE"):])
	if body == "" {
		return unwindMergeChainWherePlan{}, false
	}
	return unwindMergeChainWherePlan{clause: body}, true
}

func (e *StorageExecutor) cachedUnwindMergeChainPlan(mutationPart string) unwindMergeChainPlan {
	key := strings.TrimSpace(mutationPart)
	cache := e.unwindMergeChainPlanCache
	if cache == nil {
		cache = &unwindMergeChainPlanCache{plans: make(map[string]unwindMergeChainPlan, 128)}
		e.unwindMergeChainPlanCache = cache
	}
	cache.mu.RLock()
	if plan, ok := cache.plans[key]; ok {
		cache.mu.RUnlock()
		return plan
	}
	cache.mu.RUnlock()

	plan := parseUnwindMergeChainPattern(key)
	cache.mu.Lock()
	cache.plans[key] = plan
	cache.mu.Unlock()
	return plan
}

func isComparableInterfaceValue(v interface{}) bool {
	if v == nil {
		return true
	}
	t := reflect.TypeOf(v)
	return t.Comparable()
}

func parseSimpleCountReturn(returnPart, mergeVar string) (alias string, ok bool) {
	r := strings.TrimSpace(returnPart)
	if r == "" {
		return "", true
	}
	if !startsWithKeywordFold(r, "RETURN") {
		return "", false
	}
	body := strings.TrimSpace(r[len("RETURN "):])
	asIdx := findKeywordIndexInContext(body, "AS")
	if asIdx <= 0 {
		return "", false
	}
	expr := strings.TrimSpace(body[:asIdx])
	alias = strings.TrimSpace(body[asIdx+2:])
	expected := "count(" + mergeVar + ")"
	if !strings.EqualFold(expr, expected) {
		return "", false
	}
	if alias == "" {
		alias = "count(" + mergeVar + ")"
	}
	return alias, true
}

func parseUnwindBatchCountReturn(returnPart string) (alias string, ok bool) {
	r := strings.TrimSpace(returnPart)
	if r == "" {
		return "", true
	}
	if !startsWithKeywordFold(r, "RETURN") {
		return "", false
	}
	body := strings.TrimSpace(r[len("RETURN "):])
	asIdx := findKeywordIndexInContext(body, "AS")
	if asIdx <= 0 {
		return "", false
	}
	expr := strings.TrimSpace(body[:asIdx])
	alias = strings.TrimSpace(body[asIdx+2:])
	if alias == "" {
		return "", false
	}
	upperExpr := strings.ToUpper(strings.ReplaceAll(expr, " ", ""))
	if !strings.HasPrefix(upperExpr, "COUNT(") || !strings.HasSuffix(upperExpr, ")") {
		return "", false
	}
	inner := strings.TrimSpace(expr[len("count(") : len(expr)-1])
	if inner != "*" && !isSimpleIdentifier(inner) {
		return "", false
	}
	return alias, true
}

func splitUnwindMergeChainClauses(input string) ([]string, bool) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "" {
		return nil, false
	}
	keywords := []string{"ON CREATE SET", "ON MATCH SET", "OPTIONAL MATCH", "MATCH", "MERGE", "WITH", "WHERE", "SET"}
	var clauses []string
	for trimmed != "" {
		keyword := ""
		for _, candidate := range keywords {
			if startsWithKeywordFold(trimmed, candidate) {
				keyword = candidate
				break
			}
		}
		if keyword == "" {
			return nil, false
		}
		next := len(trimmed)
		for _, candidate := range keywords {
			if idx := findKeywordIndexInContext(trimmed[len(keyword):], candidate); idx >= 0 {
				pos := len(keyword) + idx
				if pos < next {
					next = pos
				}
			}
		}
		clauses = append(clauses, strings.TrimSpace(trimmed[:next]))
		trimmed = strings.TrimSpace(trimmed[next:])
	}
	return clauses, len(clauses) > 0
}

func parseUnwindMergeRelationshipClause(clause string) (unwindMergeChainRelationshipPlan, bool) {
	trimmed := strings.TrimSpace(clause)
	if !startsWithKeywordFold(trimmed, "MERGE") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	rest := strings.TrimSpace(trimmed[len("MERGE"):])
	if !strings.HasPrefix(rest, "(") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	fromEnd := findMatchingParen(rest, 0)
	if fromEnd <= 1 {
		return unwindMergeChainRelationshipPlan{}, false
	}
	fromVar := strings.TrimSpace(rest[1:fromEnd])
	afterFrom := strings.TrimSpace(rest[fromEnd+1:])
	if !isSimpleIdentifier(fromVar) || !strings.HasPrefix(afterFrom, "-") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	openBracket := strings.Index(afterFrom, "[")
	closeBracket := strings.Index(afterFrom, "]")
	if openBracket < 0 || closeBracket <= openBracket {
		return unwindMergeChainRelationshipPlan{}, false
	}
	relInner := strings.TrimSpace(afterFrom[openBracket+1 : closeBracket])
	var relVar string
	var relType string
	switch {
	case strings.HasPrefix(relInner, ":"):
		relType = strings.TrimSpace(relInner[1:])
	default:
		colonIdx := strings.Index(relInner, ":")
		if colonIdx <= 0 || colonIdx == len(relInner)-1 {
			return unwindMergeChainRelationshipPlan{}, false
		}
		relVar = strings.TrimSpace(relInner[:colonIdx])
		relType = strings.TrimSpace(relInner[colonIdx+1:])
		if !isSimpleIdentifier(relVar) {
			return unwindMergeChainRelationshipPlan{}, false
		}
	}
	if relType == "" {
		return unwindMergeChainRelationshipPlan{}, false
	}
	afterRel := strings.TrimSpace(afterFrom[closeBracket+1:])
	if !strings.HasPrefix(afterRel, "->") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	afterArrow := strings.TrimSpace(afterRel[2:])
	if !strings.HasPrefix(afterArrow, "(") {
		return unwindMergeChainRelationshipPlan{}, false
	}
	toEnd := findMatchingParen(afterArrow, 0)
	if toEnd != len(afterArrow)-1 {
		return unwindMergeChainRelationshipPlan{}, false
	}
	toVar := strings.TrimSpace(afterArrow[1:toEnd])
	if !isSimpleIdentifier(relType) || !isSimpleIdentifier(toVar) {
		return unwindMergeChainRelationshipPlan{}, false
	}
	return unwindMergeChainRelationshipPlan{fromVar: fromVar, toVar: toVar, relVar: relVar, relType: relType}, true
}

func splitUnwindCompoundMutationStages(restQuery, unwindParamName, unwindVar string) ([]string, bool) {
	trimmed := strings.TrimSpace(restQuery)
	if trimmed == "" || unwindParamName == "" || unwindVar == "" {
		return nil, false
	}
	skipSpace := func(pos int) int {
		for pos < len(trimmed) && isASCIISpace(trimmed[pos]) {
			pos++
		}
		return pos
	}

	stages := make([]string, 0, 4)
	start := 0
	searchFrom := 0
	for {
		withIdx := keywordIndexFrom(trimmed, "WITH", searchFrom, defaultKeywordScanOpts())
		if withIdx < 0 {
			break
		}

		pos := skipSpace(withIdx + len("WITH"))
		if pos >= len(trimmed) || trimmed[pos] != '$' {
			searchFrom = withIdx + len("WITH")
			continue
		}
		paramName, trailing, ok := parseIdentifierToken(trimmed[pos+1:])
		if !ok || !strings.EqualFold(paramName, unwindParamName) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos := pos + 1 + len(paramName)
		_ = trailing

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "AS") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("AS"))
		aliasOne, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(aliasOne)

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "UNWIND") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("UNWIND"))
		aliasTwo, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok || !strings.EqualFold(aliasOne, aliasTwo) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(aliasTwo)

		pos = skipSpace(nextPos)
		if !startsWithKeywordFold(trimmed[pos:], "AS") {
			searchFrom = withIdx + len("WITH")
			continue
		}
		pos = skipSpace(pos + len("AS"))
		stageVar, _, ok := parseIdentifierToken(trimmed[pos:])
		if !ok || !strings.EqualFold(stageVar, unwindVar) {
			searchFrom = withIdx + len("WITH")
			continue
		}
		nextPos = pos + len(stageVar)

		stage := strings.TrimSpace(trimmed[start:withIdx])
		if stage == "" {
			return nil, false
		}
		stages = append(stages, stage)
		start = nextPos
		searchFrom = nextPos
	}
	if len(stages) == 0 {
		return nil, false
	}
	last := strings.TrimSpace(trimmed[start:])
	if last == "" {
		return nil, false
	}
	stages = append(stages, last)
	return stages, len(stages) > 1
}

func (e *StorageExecutor) executeUnwindCompoundMutationBatch(ctx context.Context, unwindVar, unwindParamName string, items []interface{}, restQuery string) (*ExecuteResult, bool, error) {
	stages, ok := splitUnwindCompoundMutationStages(restQuery, unwindParamName, unwindVar)
	if !ok {
		return nil, false, nil
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	for _, stage := range stages {
		fast, supported, err := e.executeUnwindMergeChainBatch(ctx, unwindVar, items, stage, "")
		if err != nil {
			return nil, true, err
		}
		if !supported {
			return nil, false, nil
		}
		if fast != nil && fast.Stats != nil {
			result.Stats.NodesCreated += fast.Stats.NodesCreated
			result.Stats.RelationshipsCreated += fast.Stats.RelationshipsCreated
		}
	}
	e.markCompoundQueryFastPathUsed()
	return result, true, nil
}

func parseUnwindMergeChainPattern(mutationPart string) unwindMergeChainPlan {
	plan := unwindMergeChainPlan{supported: false}
	mutation := strings.TrimSpace(mutationPart)
	if !startsWithKeywordFold(mutation, "MERGE") && !startsWithKeywordFold(mutation, "MATCH") && !startsWithKeywordFold(mutation, "OPTIONAL MATCH") {
		return plan
	}
	if findKeywordIndexInContext(mutation, "CALL") >= 0 ||
		findKeywordIndexInContext(mutation, "DELETE") >= 0 ||
		findKeywordIndexInContext(mutation, "DETACH") >= 0 ||
		findKeywordIndexInContext(mutation, "REMOVE") >= 0 ||
		findKeywordIndexInContext(mutation, "FOREACH") >= 0 ||
		findKeywordIndexInContext(mutation, "UNWIND") >= 0 ||
		findKeywordIndexInContext(mutation, "RETURN") >= 0 {
		return plan
	}
	clauses, ok := splitUnwindMergeChainClauses(mutation)
	if !ok || len(clauses) == 0 {
		return plan
	}
	boundVars := make(map[string]struct{})
	hasMutation := false
	for i := 0; i < len(clauses); i++ {
		clause := clauses[i]
		if !startsWithKeywordFold(clause, "MERGE") {
			if lookupPlan, ok := parseUnwindLookupClause(clause); ok {
				for i+1 < len(clauses) && startsWithKeywordFold(clauses[i+1], "SET") {
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(clauses[i+1][len("SET"):]), lookupPlan.varName)
					if !ok {
						return unwindMergeChainPlan{}
					}
					lookupPlan.setAssignments = append(lookupPlan.setAssignments, parsed...)
					hasMutation = true
					i++
				}
				plan.steps = append(plan.steps, unwindMergeChainStep{lookup: &lookupPlan})
				boundVars[lookupPlan.varName] = struct{}{}
				continue
			}
			if withPlan, ok := parseUnwindWithClause(clause); ok {
				for _, assignment := range withPlan.assignments {
					boundVars[assignment.alias] = struct{}{}
				}
				plan.steps = append(plan.steps, unwindMergeChainStep{with: &withPlan})
				continue
			}
			if wherePlan, ok := parseUnwindWhereClause(clause); ok {
				plan.steps = append(plan.steps, unwindMergeChainStep{where: &wherePlan})
				continue
			}
			return unwindMergeChainPlan{}
		}
		mergeVar, labels, assignments, ok := parseSimpleUnwindMergeClause(clause)
		if ok {
			nodePlan := &unwindMergeChainNodePlan{
				mergeVar:         mergeVar,
				labels:           labels,
				matchAssignments: assignments,
			}
			for i+1 < len(clauses) {
				nextClause := clauses[i+1]
				switch {
				case startsWithKeywordFold(nextClause, "SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.setAssignments = append(nodePlan.setAssignments, parsed...)
					i++
				case startsWithKeywordFold(nextClause, "ON CREATE SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("ON CREATE SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.onCreateAssignments = append(nodePlan.onCreateAssignments, parsed...)
					i++
				case startsWithKeywordFold(nextClause, "ON MATCH SET"):
					parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("ON MATCH SET"):]), mergeVar)
					if !ok {
						return unwindMergeChainPlan{}
					}
					nodePlan.onMatchAssignments = append(nodePlan.onMatchAssignments, parsed...)
					i++
				default:
					goto nodeDone
				}
			}
		nodeDone:
			plan.steps = append(plan.steps, unwindMergeChainStep{node: nodePlan})
			boundVars[mergeVar] = struct{}{}
			hasMutation = true
			continue
		}
		relPlan, ok := parseUnwindMergeRelationshipClause(clause)
		if !ok {
			return unwindMergeChainPlan{}
		}
		for i+1 < len(clauses) {
			nextClause := clauses[i+1]
			if !startsWithKeywordFold(nextClause, "SET") || relPlan.relVar == "" {
				break
			}
			parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(nextClause[len("SET"):]), relPlan.relVar)
			if !ok {
				return unwindMergeChainPlan{}
			}
			relPlan.setAssignments = append(relPlan.setAssignments, parsed...)
			i++
		}
		if _, exists := boundVars[relPlan.fromVar]; !exists {
			return unwindMergeChainPlan{}
		}
		if _, exists := boundVars[relPlan.toVar]; !exists {
			return unwindMergeChainPlan{}
		}
		for i+1 < len(clauses) && startsWithKeywordFold(clauses[i+1], "SET") {
			if relPlan.relVar == "" {
				return unwindMergeChainPlan{}
			}
			parsed, ok := parseUnwindSimpleSetAssignments(strings.TrimSpace(clauses[i+1][len("SET"):]), relPlan.relVar)
			if !ok {
				return unwindMergeChainPlan{}
			}
			relPlan.setAssignments = append(relPlan.setAssignments, parsed...)
			i++
		}
		plan.steps = append(plan.steps, unwindMergeChainStep{relationship: &relPlan})
		hasMutation = true
	}
	if len(plan.steps) == 0 || !hasMutation {
		return unwindMergeChainPlan{}
	}
	plan.simple = len(plan.steps) == 1 && plan.steps[0].node != nil
	plan.supported = true
	return plan
}

func canonicalUnwindMergeValue(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := make([]interface{}, 0, len(keys))
		for _, k := range keys {
			out = append(out, []interface{}{k, canonicalUnwindMergeValue(val[k])})
		}
		return map[string]interface{}{"type": "map", "entries": out}
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, item := range val {
			out[i] = canonicalUnwindMergeValue(item)
		}
		return map[string]interface{}{"type": "list", "items": out}
	case string:
		return map[string]interface{}{"type": "string", "value": val}
	case bool:
		return map[string]interface{}{"type": "bool", "value": val}
	case int:
		return map[string]interface{}{"type": "int", "value": strconv.FormatInt(int64(val), 10)}
	case int8:
		return map[string]interface{}{"type": "int8", "value": strconv.FormatInt(int64(val), 10)}
	case int16:
		return map[string]interface{}{"type": "int16", "value": strconv.FormatInt(int64(val), 10)}
	case int32:
		return map[string]interface{}{"type": "int32", "value": strconv.FormatInt(int64(val), 10)}
	case int64:
		return map[string]interface{}{"type": "int64", "value": strconv.FormatInt(val, 10)}
	case uint:
		return map[string]interface{}{"type": "uint", "value": strconv.FormatUint(uint64(val), 10)}
	case uint8:
		return map[string]interface{}{"type": "uint8", "value": strconv.FormatUint(uint64(val), 10)}
	case uint16:
		return map[string]interface{}{"type": "uint16", "value": strconv.FormatUint(uint64(val), 10)}
	case uint32:
		return map[string]interface{}{"type": "uint32", "value": strconv.FormatUint(uint64(val), 10)}
	case uint64:
		return map[string]interface{}{"type": "uint64", "value": strconv.FormatUint(val, 10)}
	case float32:
		return map[string]interface{}{"type": "float32", "value": strconv.FormatFloat(float64(val), 'g', -1, 32)}
	case float64:
		return map[string]interface{}{"type": "float64", "value": strconv.FormatFloat(val, 'g', -1, 64)}
	case nil:
		return map[string]interface{}{"type": "nil"}
	default:
		rv := reflect.ValueOf(v)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			out := make([]interface{}, rv.Len())
			for i := 0; i < rv.Len(); i++ {
				out[i] = canonicalUnwindMergeValue(rv.Index(i).Interface())
			}
			return map[string]interface{}{"type": rv.Type().String(), "items": out}
		}
		return map[string]interface{}{"type": fmt.Sprintf("%T", v), "value": fmt.Sprintf("%#v", v)}
	}
}

func unwindMergeKey(label string, props map[string]interface{}) string {
	propNames := mergePropertyNamesSorted(props)
	entries := make([]interface{}, 0, len(propNames))
	for _, prop := range propNames {
		entries = append(entries, []interface{}{prop, canonicalUnwindMergeValue(props[prop])})
	}
	encoded, err := json.Marshal(map[string]interface{}{
		"label":   label,
		"entries": entries,
	})
	if err != nil {
		return fmt.Sprintf("%s|%v", label, propNames)
	}
	return string(encoded)
}

func applyUnwindMergeChainSetAssignment(
	node *storage.Node,
	assignment unwindSimpleSetAssignment,
	rowValues map[string]interface{},
	resolveValue func(string, map[string]interface{}) interface{},
) (bool, error) {
	if node.Properties == nil {
		node.Properties = make(map[string]interface{})
	}
	if assignment.mergeMap {
		value := resolveValue(assignment.expr, rowValues)
		props, err := normalizePropsMap(value, fmt.Sprintf("variable %s", assignment.expr))
		if err != nil {
			return false, err
		}
		changed := false
		for prop, val := range props {
			if cur, exists := node.Properties[prop]; !exists || !reflect.DeepEqual(cur, val) {
				node.Properties[prop] = val
				changed = true
			}
		}
		return changed, nil
	}
	val := resolveValue(assignment.expr, rowValues)
	if cur, exists := node.Properties[assignment.prop]; !exists || !reflect.DeepEqual(cur, val) {
		node.Properties[assignment.prop] = val
		return true, nil
	}
	return false, nil
}

func applyUnwindMergeChainEdgeSetAssignment(
	edge *storage.Edge,
	assignment unwindSimpleSetAssignment,
	rowValues map[string]interface{},
	resolveValue func(string, map[string]interface{}) interface{},
) (bool, error) {
	if edge.Properties == nil {
		edge.Properties = make(map[string]interface{})
	}
	if assignment.mergeMap {
		value := resolveValue(assignment.expr, rowValues)
		props, err := normalizePropsMap(value, fmt.Sprintf("variable %s", assignment.expr))
		if err != nil {
			return false, err
		}
		changed := false
		for prop, val := range props {
			if cur, exists := edge.Properties[prop]; !exists || !reflect.DeepEqual(cur, val) {
				edge.Properties[prop] = val
				changed = true
			}
		}
		return changed, nil
	}
	val := resolveValue(assignment.expr, rowValues)
	if cur, exists := edge.Properties[assignment.prop]; !exists || !reflect.DeepEqual(cur, val) {
		edge.Properties[assignment.prop] = val
		return true, nil
	}
	return false, nil
}

func (e *StorageExecutor) executeUnwindMergeChainBatch(ctx context.Context, unwindVar string, items []interface{}, mutationPart, returnPart string) (*ExecuteResult, bool, error) {
	plan := e.cachedUnwindMergeChainPlan(mutationPart)
	if !plan.supported {
		return nil, false, nil
	}
	countAlias, ok := parseUnwindBatchCountReturn(returnPart)
	if !ok {
		return nil, false, nil
	}
	e.markUnwindMergeChainBatchUsed()
	if plan.simple {
		e.markUnwindSimpleMergeBatchUsed()
	}

	store := e.getStorage(ctx)
	if !unwindMergeChainMatchSetLookupsAreUnique(store, plan) {
		return nil, false, nil
	}
	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}
	if countAlias != "" {
		result.Columns = []string{countAlias}
	}

	lookupCache := make(map[string]*storage.Node)
	lookupKnown := make(map[string]bool)
	// Relationships touching nodes created inside this batch cannot exist in
	// committed storage yet, but duplicate input rows must still reuse any edge
	// created earlier in the same batch.
	batchCreatedNodes := make(map[storage.NodeID]struct{})
	relationshipCache := make(map[string]*storage.Edge)
	relationshipKnown := make(map[string]bool)
	notified := make(map[string]struct{})
	params := getParamsFromContext(ctx)
	resolveBatchValue := func(expr string, values map[string]interface{}) interface{} {
		trimmed := strings.TrimSpace(expr)
		if params != nil {
			if strings.HasPrefix(trimmed, "$") && len(trimmed) > 1 && isSimpleIdentifier(trimmed[1:]) {
				if val, ok := params[trimmed[1:]]; ok {
					return val
				}
			}
			if strings.Contains(trimmed, "$") {
				trimmed = strings.TrimSpace(e.substituteParams(trimmed, params))
			}
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "COALESCE(") && strings.HasSuffix(trimmed, ")") {
			nodeMap := make(map[string]*storage.Node)
			for key, raw := range values {
				if node, ok := raw.(*storage.Node); ok && node != nil {
					nodeMap[key] = node
				}
			}
			return e.evaluateCoalesceInContext(trimmed, nodeMap, nil, values)
		}
		val := e.evaluateExpressionFromValues(trimmed, values)
		if literal, ok := val.(string); ok && literal == trimmed {
			return e.parseValue(ctx, trimmed)
		}
		return val
	}
	resolveWithValue := func(expr string, values map[string]interface{}) interface{} {
		return resolveBatchValue(expr, values)
	}
	notifyOnce := func(nodeID storage.NodeID) {
		key := string(nodeID)
		if key == "" {
			return
		}
		if _, exists := notified[key]; exists {
			return
		}
		notified[key] = struct{}{}
		e.notifyNodeMutated(key)
	}
	applyRelationshipAssignments := func(edge *storage.Edge, assignments []unwindSimpleSetAssignment, values map[string]interface{}) bool {
		if edge.Properties == nil {
			edge.Properties = map[string]interface{}{}
		}
		needsUpdate := false
		for _, assignment := range assignments {
			val := resolveBatchValue(assignment.expr, values)
			if cur, exists := edge.Properties[assignment.prop]; !exists || !reflect.DeepEqual(canonicalUnwindMergeValue(cur), canonicalUnwindMergeValue(val)) {
				edge.Properties[assignment.prop] = val
				needsUpdate = true
			}
		}
		return needsUpdate
	}
	createRelationship := func(edge *storage.Edge) (*storage.Edge, bool, error) {
		const maxCreateAttempts = 3
		for attempt := 0; attempt < maxCreateAttempts; attempt++ {
			if err := store.CreateEdge(edge); err != nil {
				if err != storage.ErrAlreadyExists {
					return nil, false, fmt.Errorf("UNWIND MERGE chain relationship create failed: %w", err)
				}
				if existing := store.GetEdgeBetween(edge.StartNode, edge.EndNode, edge.Type); existing != nil {
					return existing, false, nil
				}
				edge.ID = storage.EdgeID(e.generateID())
				continue
			}
			return edge, true, nil
		}
		return nil, false, fmt.Errorf("UNWIND MERGE chain relationship create failed after %d edge ID collisions", maxCreateAttempts)
	}
	relationshipKey := func(startID, endID storage.NodeID, edgeType string) string {
		return string(startID) + "\x00" + edgeType + "\x00" + string(endID)
	}
	// findRelationship prefers the batch-local relationship cache and only
	// falls back to committed storage when both endpoints predate this batch.
	findRelationship := func(fromNode, toNode *storage.Node, edgeType string) (*storage.Edge, string) {
		key := relationshipKey(fromNode.ID, toNode.ID, edgeType)
		if relationshipKnown[key] {
			return relationshipCache[key], key
		}
		_, fromCreated := batchCreatedNodes[fromNode.ID]
		_, toCreated := batchCreatedNodes[toNode.ID]
		if fromCreated || toCreated {
			relationshipKnown[key] = true
			return nil, key
		}
		edge := store.GetEdgeBetween(fromNode.ID, toNode.ID, edgeType)
		relationshipCache[key] = edge
		relationshipKnown[key] = true
		return edge, key
	}

	processedRows := 0
	for _, item := range items {
		rowValues := map[string]interface{}{unwindVar: item}
		skipRow := false
		for _, step := range plan.steps {
			if step.node != nil {
				nodePlan := step.node
				matchProps := make(map[string]interface{}, len(nodePlan.matchAssignments))
				for _, assignment := range nodePlan.matchAssignments {
					matchProps[assignment.prop] = resolveBatchValue(assignment.expr, rowValues)
				}
				lookupKey := unwindMergeKey(unwindMergeLabelsKey(nodePlan.labels), matchProps)
				node := lookupCache[lookupKey]
				if !lookupKnown[lookupKey] {
					var err error
					node, err = e.findMergeNode(store, nodePlan.labels, matchProps)
					if err != nil {
						return nil, true, err
					}
					lookupCache[lookupKey] = node
					lookupKnown[lookupKey] = true
				}
				if node == nil {
					node = &storage.Node{
						ID:         storage.NodeID(e.generateID()),
						Labels:     append([]string(nil), nodePlan.labels...),
						Properties: cloneNodePropertiesMap(matchProps),
					}
					for _, assignment := range nodePlan.setAssignments {
						if _, err := applyUnwindMergeChainSetAssignment(node, assignment, rowValues, resolveBatchValue); err != nil {
							return nil, true, err
						}
					}
					for _, assignment := range nodePlan.onCreateAssignments {
						if _, err := applyUnwindMergeChainSetAssignment(node, assignment, rowValues, resolveBatchValue); err != nil {
							return nil, true, err
						}
					}
					actualID, err := store.CreateNode(node)
					if err != nil {
						return nil, true, fmt.Errorf("UNWIND MERGE chain create failed: %w", err)
					}
					node.ID = actualID
					batchCreatedNodes[node.ID] = struct{}{}
					lookupCache[lookupKey] = node
					e.cacheMergeNode(nodePlan.labels, matchProps, node)
					result.Stats.NodesCreated++
					notifyOnce(node.ID)
				} else {
					needsUpdate := false
					for _, assignment := range nodePlan.setAssignments {
						changed, err := applyUnwindMergeChainSetAssignment(node, assignment, rowValues, resolveBatchValue)
						if err != nil {
							return nil, true, err
						}
						if changed {
							needsUpdate = true
						}
					}
					for _, assignment := range nodePlan.onMatchAssignments {
						changed, err := applyUnwindMergeChainSetAssignment(node, assignment, rowValues, resolveBatchValue)
						if err != nil {
							return nil, true, err
						}
						if changed {
							needsUpdate = true
						}
					}
					if needsUpdate {
						if err := store.UpdateNode(node); err != nil {
							return nil, true, fmt.Errorf("UNWIND MERGE chain update failed: %w", err)
						}
						e.cacheMergeNode(nodePlan.labels, matchProps, node)
						notifyOnce(node.ID)
					}
				}
				rowValues[nodePlan.mergeVar] = node
				continue
			}

			if step.lookup != nil {
				lookupPlan := step.lookup
				matchProps := make(map[string]interface{}, len(lookupPlan.matchAssignments))
				for _, assignment := range lookupPlan.matchAssignments {
					matchProps[assignment.prop] = resolveBatchValue(assignment.expr, rowValues)
				}
				lookupKey := unwindMergeKey(unwindMergeLabelsKey(lookupPlan.labels), matchProps)
				node := lookupCache[lookupKey]
				if !lookupKnown[lookupKey] {
					var err error
					if lookupPlan.anyLabel {
						node, err = e.findMergeNodeAnyLabel(store, lookupPlan.labels, matchProps)
					} else {
						node, err = e.findMergeNode(store, lookupPlan.labels, matchProps)
					}
					if err != nil {
						return nil, true, err
					}
					lookupCache[lookupKey] = node
					lookupKnown[lookupKey] = true
				}
				if node == nil && !lookupPlan.optional {
					skipRow = true
					break
				}
				if node == nil {
					rowValues[lookupPlan.varName] = nil
				} else {
					needsUpdate := false
					for _, assignment := range lookupPlan.setAssignments {
						changed, err := applyUnwindMergeChainSetAssignment(node, assignment, rowValues, resolveBatchValue)
						if err != nil {
							return nil, true, err
						}
						if changed {
							needsUpdate = true
						}
					}
					if needsUpdate {
						if err := store.UpdateNode(node); err != nil {
							return nil, true, fmt.Errorf("UNWIND MATCH chain update failed: %w", err)
						}
						if !lookupPlan.anyLabel {
							e.cacheMergeNode(lookupPlan.labels, matchProps, node)
						}
						notifyOnce(node.ID)
					}
					rowValues[lookupPlan.varName] = node
				}
				continue
			}

			if step.with != nil {
				for _, assignment := range step.with.assignments {
					rowValues[assignment.alias] = resolveWithValue(assignment.expr, rowValues)
				}
				continue
			}

			if step.where != nil {
				if !e.evaluateWithWhereCondition(ctx, step.where.clause, rowValues) {
					skipRow = true
					break
				}
				continue
			}

			relPlan := step.relationship
			fromNode, _ := rowValues[relPlan.fromVar].(*storage.Node)
			toNode, _ := rowValues[relPlan.toVar].(*storage.Node)
			if fromNode == nil || toNode == nil {
				skipRow = true
				break
			}
			edge, relKey := findRelationship(fromNode, toNode, relPlan.relType)
			relationshipChanged := false
			if edge == nil {
				edge = &storage.Edge{
					ID:         storage.EdgeID(e.generateID()),
					Type:       relPlan.relType,
					StartNode:  fromNode.ID,
					EndNode:    toNode.ID,
					Properties: map[string]interface{}{},
				}
				applyRelationshipAssignments(edge, relPlan.setAssignments, rowValues)
				createdEdge, created, err := createRelationship(edge)
				if err != nil {
					return nil, true, err
				}
				edge = createdEdge
				relationshipCache[relKey] = edge
				relationshipKnown[relKey] = true
				if created {
					result.Stats.RelationshipsCreated++
					relationshipChanged = true
				} else if applyRelationshipAssignments(createdEdge, relPlan.setAssignments, rowValues) {
					if err := store.UpdateEdge(createdEdge); err != nil {
						return nil, true, fmt.Errorf("UNWIND MERGE chain relationship update failed: %w", err)
					}
					relationshipCache[relKey] = createdEdge
					relationshipChanged = true
				}
			} else if applyRelationshipAssignments(edge, relPlan.setAssignments, rowValues) {
				if err := store.UpdateEdge(edge); err != nil {
					return nil, true, fmt.Errorf("UNWIND MERGE chain relationship update failed: %w", err)
				}
				relationshipCache[relKey] = edge
				relationshipChanged = true
			}
			if relationshipChanged {
				notifyOnce(fromNode.ID)
				notifyOnce(toNode.ID)
			}
			if relPlan.relVar != "" {
				rowValues[relPlan.relVar] = edge
			}
		}
		if skipRow {
			continue
		}
		processedRows++
	}

	if countAlias != "" {
		result.Rows = [][]interface{}{{int64(processedRows)}}
	}
	return result, true, nil
}

func (e *StorageExecutor) executeUnwindFixedChainLinkBatch(ctx context.Context, unwindVar string, items []interface{}, restQuery string) (*ExecuteResult, bool, error) {
	returnIdx := findKeywordIndex(restQuery, "RETURN")
	if returnIdx <= 0 {
		return nil, false, nil
	}
	mutationPart := strings.TrimSpace(restQuery[:returnIdx])
	returnPart := strings.TrimSpace(restQuery[returnIdx:])
	countAlias, ok := parseSimpleCountReturn(returnPart, "o")
	if !ok {
		return nil, false, nil
	}

	normalized := strings.Join(strings.Fields(mutationPart), " ")

	type rootSpec struct {
		varName  string
		label    string
		byID     bool
		propName string
		rowField string
	}
	type hopSpec struct {
		label      string
		propName   string
		rowField   string
		depthByVar map[string]int
	}
	parseRowFieldRef := func(expr string) (string, string, bool) {
		trimmed := strings.TrimSpace(expr)
		dotIdx := strings.Index(trimmed, ".")
		if dotIdx <= 0 || dotIdx == len(trimmed)-1 {
			return "", "", false
		}
		base := strings.TrimSpace(trimmed[:dotIdx])
		field := strings.TrimSpace(trimmed[dotIdx+1:])
		if !isSimpleIdentifier(base) || !isSimpleIdentifier(field) {
			return "", "", false
		}
		return base, field, true
	}
	parseIdentifierNodePattern := func(pattern string) (string, string, string, string, bool) {
		trimmed := strings.TrimSpace(pattern)
		if strings.HasPrefix(trimmed, "(") && strings.HasSuffix(trimmed, ")") {
			trimmed = strings.TrimSpace(trimmed[1 : len(trimmed)-1])
		}
		braceIdx := strings.Index(trimmed, "{")
		head := trimmed
		props := ""
		if braceIdx >= 0 {
			closeIdx := e.findMatchingBrace(trimmed, braceIdx)
			if closeIdx != len(trimmed)-1 {
				return "", "", "", "", false
			}
			head = strings.TrimSpace(trimmed[:braceIdx])
			props = strings.TrimSpace(trimmed[braceIdx+1 : closeIdx])
		}
		parts := strings.Split(head, ":")
		if len(parts) != 2 {
			return "", "", "", "", false
		}
		varName := strings.TrimSpace(parts[0])
		label := strings.TrimSpace(parts[1])
		if !isSimpleIdentifier(varName) || !isSimpleIdentifier(label) {
			return "", "", "", "", false
		}
		if props == "" {
			return varName, label, "", "", true
		}
		pairs := splitTopLevelComma(props)
		if len(pairs) != 1 {
			return "", "", "", "", false
		}
		pair := strings.TrimSpace(pairs[0])
		colonIdx := strings.Index(pair, ":")
		if colonIdx <= 0 || colonIdx == len(pair)-1 {
			return "", "", "", "", false
		}
		propName := strings.TrimSpace(pair[:colonIdx])
		propExpr := strings.TrimSpace(pair[colonIdx+1:])
		if !isSimpleIdentifier(propName) || propExpr == "" {
			return "", "", "", "", false
		}
		return varName, label, propName, propExpr, true
	}
	parseRowFieldDepthExpr := func(expr string) (string, string, int, bool) {
		parts := strings.SplitN(strings.TrimSpace(expr), "+", 2)
		if len(parts) != 2 {
			return "", "", 0, false
		}
		rowVar, rowField, ok := parseRowFieldRef(parts[0])
		if !ok {
			return "", "", 0, false
		}
		suffix := strings.TrimSpace(parts[1])
		if len(suffix) < 4 {
			return "", "", 0, false
		}
		if (suffix[0] != '\'' || suffix[len(suffix)-1] != '\'') && (suffix[0] != '"' || suffix[len(suffix)-1] != '"') {
			return "", "", 0, false
		}
		literal := suffix[1 : len(suffix)-1]
		if !strings.HasPrefix(literal, ":") {
			return "", "", 0, false
		}
		depth, err := strconv.Atoi(strings.TrimSpace(literal[1:]))
		if err != nil || depth <= 0 {
			return "", "", 0, false
		}
		return rowVar, rowField, depth, true
	}
	parseMergeClause := func(clause string) (string, string, string, bool) {
		trimmed := strings.TrimSpace(clause)
		if !strings.HasPrefix(strings.ToUpper(trimmed), "MERGE ") {
			return "", "", "", false
		}
		body := strings.TrimSpace(trimmed[len("MERGE "):])
		if !strings.HasPrefix(body, "(") {
			return "", "", "", false
		}
		fromEnd := findMatchingParen(body, 0)
		if fromEnd <= 1 {
			return "", "", "", false
		}
		fromVar := strings.TrimSpace(body[1:fromEnd])
		rest := strings.TrimSpace(body[fromEnd+1:])
		if !isSimpleIdentifier(fromVar) || !strings.HasPrefix(rest, "-") {
			return "", "", "", false
		}
		openBracket := strings.Index(rest, "[")
		closeBracket := strings.Index(rest, "]")
		if openBracket < 0 || closeBracket <= openBracket {
			return "", "", "", false
		}
		relInner := strings.TrimSpace(rest[openBracket+1 : closeBracket])
		if !strings.HasPrefix(relInner, ":") {
			return "", "", "", false
		}
		relType := strings.TrimSpace(relInner[1:])
		afterRel := strings.TrimSpace(rest[closeBracket+1:])
		if !strings.HasPrefix(afterRel, "->") {
			return "", "", "", false
		}
		afterArrow := strings.TrimSpace(afterRel[2:])
		if !strings.HasPrefix(afterArrow, "(") {
			return "", "", "", false
		}
		toEnd := findMatchingParen(afterArrow, 0)
		if toEnd != len(afterArrow)-1 {
			return "", "", "", false
		}
		toVar := strings.TrimSpace(afterArrow[1:toEnd])
		if !isSimpleIdentifier(relType) || !isSimpleIdentifier(toVar) {
			return "", "", "", false
		}
		return strings.ToLower(fromVar), relType, strings.ToLower(toVar), true
	}
	splitMutationClauses := func(input string) ([]string, bool) {
		trimmed := strings.TrimSpace(input)
		if trimmed == "" {
			return nil, false
		}
		var clauses []string
		for trimmed != "" {
			upper := strings.ToUpper(trimmed)
			var keyword string
			switch {
			case strings.HasPrefix(upper, "MATCH "):
				keyword = "MATCH"
			case strings.HasPrefix(upper, "MERGE "):
				keyword = "MERGE"
			default:
				return nil, false
			}
			next := len(trimmed)
			for _, candidate := range []string{"MATCH", "MERGE"} {
				if idx := findKeywordIndexInContext(trimmed[len(keyword):], candidate); idx >= 0 {
					pos := len(keyword) + idx
					if pos < next {
						next = pos
					}
				}
			}
			clauses = append(clauses, strings.TrimSpace(trimmed[:next]))
			trimmed = strings.TrimSpace(trimmed[next:])
		}
		return clauses, true
	}
	mutationClauses, ok := splitMutationClauses(normalized)
	if !ok || len(mutationClauses) < 3 {
		return nil, false, nil
	}
	parseRootSpec := func(clause string) (rootSpec, bool) {
		trimmed := strings.TrimSpace(clause)
		if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
			return rootSpec{}, false
		}
		body := strings.TrimSpace(trimmed[len("MATCH "):])
		if !strings.HasPrefix(body, "(") {
			return rootSpec{}, false
		}
		closeIdx := findMatchingParen(body, 0)
		if closeIdx < 0 {
			return rootSpec{}, false
		}
		varName, label, propName, propExpr, ok := parseIdentifierNodePattern(body[:closeIdx+1])
		if !ok {
			return rootSpec{}, false
		}
		rest := strings.TrimSpace(body[closeIdx+1:])
		var out rootSpec
		out.varName = strings.ToLower(varName)
		out.label = label
		if propName != "" {
			rowVar, rowField, ok := parseRowFieldRef(propExpr)
			if !ok || !strings.EqualFold(rowVar, unwindVar) {
				return rootSpec{}, false
			}
			out.propName = propName
			out.rowField = rowField
			return out, rest == ""
		}
		if rest == "" || !strings.HasPrefix(strings.ToUpper(rest), "WHERE ") {
			return rootSpec{}, false
		}
		whereExpr := strings.TrimSpace(rest[len("WHERE "):])
		parts := strings.SplitN(whereExpr, "=", 2)
		if len(parts) != 2 {
			return rootSpec{}, false
		}
		left := strings.TrimSpace(parts[0])
		right := strings.TrimSpace(parts[1])
		lowerLeft := strings.ToLower(left)
		if !strings.HasPrefix(lowerLeft, "elementid(") || !strings.HasSuffix(left, ")") {
			return rootSpec{}, false
		}
		whereVar := strings.TrimSpace(left[len("elementId(") : len(left)-1])
		rowVar, rowField, ok := parseRowFieldRef(right)
		if !ok || !strings.EqualFold(whereVar, varName) || !strings.EqualFold(rowVar, unwindVar) {
			return rootSpec{}, false
		}
		out.byID = true
		out.rowField = rowField
		return out, true
	}
	root, ok := parseRootSpec(mutationClauses[0])
	if !ok {
		return nil, false, nil
	}
	parseHopSpec := func(clauses []string) (hopSpec, bool) {
		var out hopSpec
		out.depthByVar = make(map[string]int)
		seenDepth := make(map[int]bool)
		for _, clause := range clauses {
			trimmed := strings.TrimSpace(clause)
			if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
				return hopSpec{}, false
			}
			body := strings.TrimSpace(trimmed[len("MATCH "):])
			if strings.Contains(strings.ToUpper(body), " WHERE ") {
				return hopSpec{}, false
			}
			varName, label, propName, propExpr, ok := parseIdentifierNodePattern(body)
			if !ok || propName == "" {
				return hopSpec{}, false
			}
			rowVar, rowField, depth, ok := parseRowFieldDepthExpr(propExpr)
			if !ok || !strings.EqualFold(rowVar, unwindVar) {
				return hopSpec{}, false
			}
			if out.label == "" {
				out.label = label
				out.propName = propName
				out.rowField = rowField
			}
			if !strings.EqualFold(out.label, label) || !strings.EqualFold(out.propName, propName) || !strings.EqualFold(out.rowField, rowField) {
				return hopSpec{}, false
			}
			lowerVar := strings.ToLower(varName)
			if prev, exists := out.depthByVar[lowerVar]; exists && prev != depth {
				return hopSpec{}, false
			}
			out.depthByVar[lowerVar] = depth
			seenDepth[depth] = true
		}
		if len(out.depthByVar) == 0 {
			return hopSpec{}, false
		}
		for i := 1; i <= len(seenDepth); i++ {
			if !seenDepth[i] {
				return hopSpec{}, false
			}
		}
		return out, true
	}
	firstMergeIdx := -1
	for idx, clause := range mutationClauses {
		if strings.HasPrefix(strings.ToUpper(clause), "MERGE ") {
			firstMergeIdx = idx
			break
		}
	}
	if firstMergeIdx <= 1 || firstMergeIdx >= len(mutationClauses) {
		return nil, false, nil
	}
	hop, ok := parseHopSpec(mutationClauses[1:firstMergeIdx])
	if !ok {
		return nil, false, nil
	}
	relType := ""
	nextByFrom := make(map[string]string)
	for _, clause := range mutationClauses[firstMergeIdx:] {
		from, rel, to, ok := parseMergeClause(clause)
		if !ok {
			return nil, false, nil
		}
		if relType == "" {
			relType = rel
		}
		if !strings.EqualFold(rel, relType) {
			return nil, false, nil
		}
		if prev, exists := nextByFrom[from]; exists && prev != to {
			return nil, false, nil
		}
		nextByFrom[from] = to
	}
	firstHopVar, ok := nextByFrom[root.varName]
	if !ok {
		return nil, false, nil
	}
	chainVars := make([]string, 0, len(hop.depthByVar))
	seenVar := make(map[string]bool)
	cur := firstHopVar
	for {
		if seenVar[cur] {
			return nil, false, nil
		}
		seenVar[cur] = true
		chainVars = append(chainVars, cur)
		next, ok := nextByFrom[cur]
		if !ok {
			break
		}
		cur = next
	}
	if len(chainVars) == 0 || len(chainVars) != len(hop.depthByVar) {
		return nil, false, nil
	}
	for _, v := range chainVars {
		if _, ok := hop.depthByVar[v]; !ok {
			return nil, false, nil
		}
	}

	store := e.getStorage(ctx)
	result := &ExecuteResult{
		Columns: []string{countAlias},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	originals, err := store.GetNodesByLabel(root.label)
	if err != nil {
		return nil, true, err
	}
	originalByProp := make(map[string]*storage.Node, len(originals))
	originalByID := make(map[string]*storage.Node, len(originals))
	for _, n := range originals {
		if n == nil {
			continue
		}
		originalByID[string(n.ID)] = n
		if !root.byID {
			key := fmt.Sprintf("%v", n.Properties[root.propName])
			if key == "" {
				continue
			}
			originalByProp[key] = n
		}
	}

	hops, err := store.GetNodesByLabel(hop.label)
	if err != nil {
		return nil, true, err
	}
	hopByID := make(map[string]*storage.Node, len(hops))
	for _, n := range hops {
		if n == nil {
			continue
		}
		key := fmt.Sprintf("%v", n.Properties[hop.propName])
		if key == "" {
			continue
		}
		hopByID[key] = n
	}

	notified := make(map[string]struct{})
	notifyOnce := func(nodeID storage.NodeID) {
		key := string(nodeID)
		if key == "" {
			return
		}
		if _, exists := notified[key]; exists {
			return
		}
		notified[key] = struct{}{}
		e.notifyNodeMutated(key)
	}

	mergeHop := func(from, to *storage.Node) error {
		if from == nil || to == nil {
			return nil
		}
		if store.GetEdgeBetween(from.ID, to.ID, relType) != nil {
			return nil
		}
		edge := &storage.Edge{
			ID:         storage.EdgeID(e.generateID()),
			Type:       relType,
			StartNode:  from.ID,
			EndNode:    to.ID,
			Properties: map[string]interface{}{},
		}
		if createErr := store.CreateEdge(edge); createErr != nil {
			if createErr != storage.ErrAlreadyExists {
				return createErr
			}
		} else {
			result.Stats.RelationshipsCreated++
			notifyOnce(from.ID)
			notifyOnce(to.ID)
		}
		return nil
	}

	var matchedCount int64
	for _, item := range items {
		rowMap, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		var o *storage.Node
		var hopPrefix string
		if root.byID {
			rawRootID := rowMap[root.rowField]
			rootID, _ := normalizeNodeIDValue(rawRootID).(string)
			rootID = strings.TrimSpace(rootID)
			if rootID == "" {
				continue
			}
			o = originalByID[rootID]
		} else {
			rootKey := fmt.Sprintf("%v", rowMap[root.rowField])
			if rootKey == "" {
				continue
			}
			o = originalByProp[rootKey]
		}
		if o == nil {
			continue
		}
		hopPrefix = fmt.Sprintf("%v", rowMap[hop.rowField])
		if hopPrefix == "" {
			continue
		}
		chainNodes := make([]*storage.Node, 0, len(chainVars))
		missing := false
		for _, v := range chainVars {
			depth := hop.depthByVar[v]
			n := hopByID[fmt.Sprintf("%s:%d", hopPrefix, depth)]
			if n == nil {
				missing = true
				break
			}
			chainNodes = append(chainNodes, n)
		}
		if missing || len(chainNodes) == 0 {
			continue
		}

		if err := mergeHop(o, chainNodes[0]); err != nil {
			return nil, true, fmt.Errorf("UNWIND fixed-chain merge failed: %w", err)
		}
		for i := 0; i < len(chainNodes)-1; i++ {
			if err := mergeHop(chainNodes[i], chainNodes[i+1]); err != nil {
				return nil, true, fmt.Errorf("UNWIND fixed-chain merge failed: %w", err)
			}
		}
		matchedCount++
	}
	e.markUnwindFixedChainLinkBatchUsed()
	result.Rows = [][]interface{}{{matchedCount}}
	return result, true, nil
}

func rewriteUnwindCorrelationToIn(query string, variable string, paramName string) (string, bool) {
	if strings.TrimSpace(query) == "" || strings.TrimSpace(variable) == "" || strings.TrimSpace(paramName) == "" {
		return "", false
	}
	// Preserve join correlation semantics:
	//   a.prop = unwindVar AND b.prop = unwindVar
	// => a.prop IN $items AND b.prop = a.prop
	// so we do not produce cross-key cartesian joins.
	type equalityMatch struct {
		start int
		end   int
		lhs   string
	}
	matches := make([]equalityMatch, 0, 2)
	inSingle := false
	inDouble := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDouble && (i == 0 || query[i-1] != '\\') {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle && (i == 0 || query[i-1] != '\\') {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble || ch != '=' {
			continue
		}

		lhsEnd := i
		for lhsEnd > 0 && isASCIISpace(query[lhsEnd-1]) {
			lhsEnd--
		}
		lhsStart := lhsEnd
		for lhsStart > 0 {
			prev := query[lhsStart-1]
			if isIdentByte(prev) || prev == '.' {
				lhsStart--
				continue
			}
			break
		}
		lhs := strings.TrimSpace(query[lhsStart:lhsEnd])
		if lhs == "" || !isSimplePropertyReference(lhs) {
			continue
		}

		rhsStart := i + 1
		for rhsStart < len(query) && isASCIISpace(query[rhsStart]) {
			rhsStart++
		}
		if rhsStart+len(variable) > len(query) || !strings.EqualFold(query[rhsStart:rhsStart+len(variable)], variable) {
			continue
		}
		rhsEnd := rhsStart + len(variable)
		if rhsEnd < len(query) && isIdentByte(query[rhsEnd]) {
			continue
		}
		matches = append(matches, equalityMatch{start: lhsStart, end: rhsEnd, lhs: lhs})
		i = rhsEnd - 1
	}
	if len(matches) == 0 {
		return "", false
	}
	firstExpr := matches[0].lhs
	if firstExpr == "" {
		return "", false
	}

	var b strings.Builder
	cursor := 0
	for i, m := range matches {
		b.WriteString(query[cursor:m.start])
		if i == 0 {
			b.WriteString(m.lhs)
			b.WriteString(" IN $")
			b.WriteString(paramName)
		} else {
			b.WriteString(m.lhs)
			b.WriteString(" = ")
			b.WriteString(firstExpr)
		}
		cursor = m.end
	}
	b.WriteString(query[cursor:])
	return b.String(), true
}

func isSimplePropertyReference(expr string) bool {
	parts := strings.Split(expr, ".")
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if !isSimpleIdentifier(strings.TrimSpace(part)) {
			return false
		}
	}
	return true
}

func rewriteTopLevelMultiMatchToCartesianMatch(query string) string {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
		return query
	}
	returnIdx := findKeywordIndex(trimmed, "RETURN")
	if returnIdx <= 0 {
		return query
	}
	body := strings.TrimSpace(trimmed[:returnIdx])
	tail := strings.TrimSpace(trimmed[returnIdx:])
	whereIdx := findKeywordIndex(body, "WHERE")
	if whereIdx <= 0 {
		return query
	}
	patterns := strings.TrimSpace(body[:whereIdx])
	whereClause := strings.TrimSpace(body[whereIdx+len("WHERE"):])
	if patterns == "" || whereClause == "" {
		return query
	}

	upperPatterns := strings.ToUpper(patterns)
	if strings.Count(upperPatterns, "MATCH ") != 2 {
		return query
	}
	first := strings.TrimSpace(patterns[len("MATCH "):])
	secondIdx := findKeywordIndex(first, "MATCH")
	if secondIdx <= 0 {
		return query
	}
	left := strings.TrimSpace(first[:secondIdx])
	right := strings.TrimSpace(first[secondIdx+len("MATCH"):])
	if left == "" || right == "" {
		return query
	}
	return "MATCH " + left + ", " + right + " WHERE " + whereClause + " " + tail
}

func canApplySetBasedUnwindRewrite(query string, items []interface{}) bool {
	if strings.TrimSpace(query) == "" || len(items) == 0 {
		return false
	}
	upper := strings.ToUpper(query)
	// Keep rewrite on read-only MATCH ... RETURN count(...) pipelines.
	// Mutation clauses with correlated values (SET += row.props, MERGE/CREATE/DELETE/REMOVE)
	// must execute per-row to preserve semantics.
	if findKeywordIndex(query, "CREATE") >= 0 ||
		findKeywordIndex(query, "MERGE") >= 0 ||
		findKeywordIndex(query, "SET") >= 0 ||
		findKeywordIndex(query, "DELETE") >= 0 ||
		findKeywordIndex(query, "REMOVE") >= 0 {
		return false
	}
	if !strings.Contains(upper, "RETURN") || !strings.Contains(upper, "COUNT(") {
		return false
	}
	// Rewrites should preserve semantics. We only apply when unwind items are
	// distinct comparable values so IN-list matching does not collapse duplicates.
	seen := map[interface{}]struct{}{}
	for _, it := range items {
		if it == nil {
			if _, exists := seen[nil]; exists {
				return false
			}
			seen[nil] = struct{}{}
			continue
		}
		rv := reflect.ValueOf(it)
		if !rv.IsValid() || !rv.Type().Comparable() {
			return false
		}
		if _, exists := seen[it]; exists {
			return false
		}
		seen[it] = struct{}{}
	}
	return true
}

// normalizeMultiMatchWhereClauses rewrites top-level two-MATCH forms that place
// WHERE between MATCH clauses into a single terminal WHERE:
//  1. MATCH A WHERE wa MATCH B RETURN ...
//     -> MATCH A MATCH B WHERE wa RETURN ...
//  2. MATCH A WHERE wa MATCH B WHERE wb RETURN ...
//     -> MATCH A MATCH B WHERE wa AND wb RETURN ...
func normalizeMultiMatchWhereClauses(query string) string {
	trimmed := strings.TrimSpace(query)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "MATCH ") {
		return query
	}
	// Only normalize chained required MATCH clauses. Queries containing
	// OPTIONAL MATCH have different left-join semantics and must not be
	// rewritten into multi-MATCH WHERE forms.
	if findKeywordIndex(trimmed, "OPTIONAL MATCH") >= 0 {
		return query
	}

	returnIdx := findKeywordIndex(trimmed, "RETURN")
	if returnIdx <= 0 {
		return query
	}
	mainPart := strings.TrimSpace(trimmed[:returnIdx])
	tailPart := strings.TrimSpace(trimmed[returnIdx:])

	searchFrom := len("MATCH")
	secondMatchIdx := -1
	if searchFrom < len(mainPart) {
		if rel := findKeywordIndex(mainPart[searchFrom:], "MATCH"); rel >= 0 {
			secondMatchIdx = searchFrom + rel
		}
	}
	if secondMatchIdx <= 0 {
		return query
	}
	left := strings.TrimSpace(mainPart[:secondMatchIdx])
	right := strings.TrimSpace(mainPart[secondMatchIdx+len("MATCH"):])
	if !strings.HasPrefix(strings.ToUpper(left), "MATCH ") {
		return query
	}

	leftWhereIdx := findKeywordIndex(left, "WHERE")
	if leftWhereIdx <= 0 {
		return query
	}
	rightWhereIdx := findKeywordIndex(right, "WHERE")

	leftPattern := strings.TrimSpace(left[len("MATCH "):leftWhereIdx])
	leftWhere := strings.TrimSpace(left[leftWhereIdx+len("WHERE"):])
	rightPattern := strings.TrimSpace(right)
	rightWhere := ""
	if rightWhereIdx > 0 {
		rightPattern = strings.TrimSpace(right[:rightWhereIdx])
		rightWhere = strings.TrimSpace(right[rightWhereIdx+len("WHERE"):])
	}

	if leftPattern == "" || rightPattern == "" || leftWhere == "" {
		return query
	}

	var b strings.Builder
	b.WriteString("MATCH ")
	b.WriteString(leftPattern)
	b.WriteString(" MATCH ")
	b.WriteString(rightPattern)
	b.WriteString(" WHERE ")
	b.WriteString(leftWhere)
	if rightWhere != "" {
		b.WriteString(" AND ")
		b.WriteString(rightWhere)
	}
	b.WriteString(" ")
	b.WriteString(tailPart)
	return b.String()
}

type unwindCollectDistinctProjectionPlan struct {
	srcVar string
	prop   string
	alias  string
}

func (e *StorageExecutor) parseUnwindCollectDistinctProjection(restQuery string) (unwindCollectDistinctProjectionPlan, bool) {
	trimmed := strings.TrimSpace(restQuery)
	if !startsWithKeywordFold(trimmed, "WITH") {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	returnIdx := findKeywordIndexInContext(trimmed, "RETURN")
	if returnIdx <= 0 {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	withClause := strings.TrimSpace(trimmed[len("WITH"):returnIdx])
	returnClause := strings.TrimSpace(trimmed[returnIdx+len("RETURN"):])
	if withClause == "" || returnClause == "" {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	withItems := e.splitWithItems(withClause)
	if len(withItems) != 1 {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	expr, alias := parseProjectionExprAlias(strings.TrimSpace(withItems[0]))
	if expr == "" || alias == "" {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	agg := ParseAggregation(expr)
	if agg == nil || !strings.EqualFold(agg.Function, "COLLECT") || !agg.Distinct || agg.IsStar || agg.Variable == "" || agg.Property == "" {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	returnItems := e.parseReturnItems(returnClause)
	if len(returnItems) != 1 || !strings.EqualFold(strings.TrimSpace(returnItems[0].expr), alias) || returnItems[0].alias != "" {
		return unwindCollectDistinctProjectionPlan{}, false
	}
	return unwindCollectDistinctProjectionPlan{srcVar: agg.Variable, prop: agg.Property, alias: alias}, true
}

func (e *StorageExecutor) executeUnwindWithCollectProjection(unwindVar string, items []interface{}, restQuery string) (*ExecuteResult, bool) {
	plan, ok := e.parseUnwindCollectDistinctProjection(restQuery)
	if !ok {
		return nil, false
	}
	if !strings.EqualFold(plan.srcVar, unwindVar) {
		return nil, false
	}

	seen := map[interface{}]struct{}{}
	values := make([]interface{}, 0, len(items))
	for _, it := range items {
		var v interface{}
		switch row := it.(type) {
		case map[string]interface{}:
			v = row[plan.prop]
		case map[interface{}]interface{}:
			v = row[plan.prop]
		default:
			continue
		}
		if _, exists := seen[v]; exists {
			continue
		}
		seen[v] = struct{}{}
		values = append(values, v)
	}

	return &ExecuteResult{
		Columns: []string{plan.alias},
		Rows:    [][]interface{}{{values}},
	}, true
}

// executeDoubleUnwind handles double UNWIND clauses like:
// UNWIND [[1,2],[3,4]] AS pair UNWIND pair AS num RETURN num
func (e *StorageExecutor) executeDoubleUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse first UNWIND
	firstUnwindIdx := findKeywordIndex(cypher, "UNWIND")
	if firstUnwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found")
	}

	afterFirst := cypher[firstUnwindIdx+6:]
	firstAsIdx := findKeywordNotInBrackets(afterFirst, " AS ")
	if firstAsIdx == -1 {
		return nil, fmt.Errorf("first UNWIND requires AS clause")
	}

	firstListExpr := strings.TrimSpace(afterFirst[:firstAsIdx])
	afterFirstAsStart := firstAsIdx + len("AS")
	for afterFirstAsStart < len(afterFirst) && isASCIISpace(afterFirst[afterFirstAsStart]) {
		afterFirstAsStart++
	}
	afterFirstAs := strings.TrimSpace(afterFirst[afterFirstAsStart:])

	// Get first variable name
	varEndIdx := strings.IndexAny(afterFirstAs, " \t\n")
	if varEndIdx == -1 {
		return nil, fmt.Errorf("malformed double UNWIND")
	}
	firstVar := afterFirstAs[:varEndIdx]
	restQuery := strings.TrimSpace(afterFirstAs[varEndIdx:])

	// Parse second UNWIND
	if !strings.HasPrefix(strings.ToUpper(restQuery), "UNWIND") {
		return nil, fmt.Errorf("expected second UNWIND")
	}

	afterSecond := restQuery[6:]
	secondAsIdx := findKeywordNotInBrackets(afterSecond, " AS ")
	if secondAsIdx == -1 {
		return nil, fmt.Errorf("second UNWIND requires AS clause")
	}

	secondListExpr := strings.TrimSpace(afterSecond[:secondAsIdx])
	afterSecondAsStart := secondAsIdx + len("AS")
	for afterSecondAsStart < len(afterSecond) && isASCIISpace(afterSecond[afterSecondAsStart]) {
		afterSecondAsStart++
	}
	afterSecondAs := strings.TrimSpace(afterSecond[afterSecondAsStart:])

	var secondVar, finalRest string
	varEndIdx2 := strings.IndexAny(afterSecondAs, " \t\n")
	if varEndIdx2 == -1 {
		secondVar = afterSecondAs
		finalRest = ""
	} else {
		secondVar = afterSecondAs[:varEndIdx2]
		finalRest = strings.TrimSpace(afterSecondAs[varEndIdx2:])
	}

	// Evaluate the first list
	firstList := e.evaluateExpressionWithContext(ctx, firstListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

	var outerItems []interface{}
	switch v := firstList.(type) {
	case []interface{}:
		outerItems = v
	case nil:
		outerItems = []interface{}{}
	default:
		outerItems = []interface{}{firstList}
	}

	// Collect all paired items (outer, inner) for cartesian or nested product
	type pairedItem struct {
		outer interface{}
		inner interface{}
	}
	var allPairedItems []pairedItem

	// Fast path: independent second UNWIND expression can be evaluated once and reused.
	secondDependsOnFirst := replaceIdentifierOutsideQuotes(secondListExpr, firstVar, "__nornic_probe__") != secondListExpr
	var independentInnerList interface{}
	if !secondDependsOnFirst && secondListExpr != firstVar {
		independentInnerList = e.evaluateExpressionWithContext(ctx, secondListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
	}

	for _, outerItem := range outerItems {
		// The second UNWIND expression should reference the first variable
		// If secondListExpr == firstVar, use outerItem directly (nested case)
		var innerList interface{}
		if secondListExpr == firstVar {
			innerList = outerItem
		} else if secondDependsOnFirst {
			evaluatedExpr := e.replaceVariableInQuery(secondListExpr, firstVar, outerItem)
			innerList = e.evaluateExpressionWithContext(ctx, evaluatedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
		} else {
			innerList = independentInnerList
		}

		switch inner := innerList.(type) {
		case []interface{}:
			for _, innerItem := range inner {
				allPairedItems = append(allPairedItems, pairedItem{outer: outerItem, inner: innerItem})
			}
		case nil:
			// Skip
		default:
			allPairedItems = append(allPairedItems, pairedItem{outer: outerItem, inner: innerList})
		}
	}

	// Process RETURN clause
	if strings.HasPrefix(strings.ToUpper(finalRest), "RETURN") {
		returnClause := strings.TrimSpace(finalRest[6:])
		returnItems := e.parseReturnItems(returnClause)

		result := &ExecuteResult{
			Columns: make([]string, len(returnItems)),
			Rows:    make([][]interface{}, 0, len(allPairedItems)),
		}

		for i, item := range returnItems {
			if item.alias != "" {
				result.Columns[i] = item.alias
			} else {
				result.Columns[i] = item.expr
			}
		}

		for _, paired := range allPairedItems {
			row := make([]interface{}, len(returnItems))
			for i, item := range returnItems {
				if item.expr == secondVar {
					row[i] = paired.inner
				} else if item.expr == firstVar {
					row[i] = paired.outer
				} else {
					evaluatedExpr := e.replaceVariableInQuery(item.expr, firstVar, paired.outer)
					evaluatedExpr = e.replaceVariableInQuery(evaluatedExpr, secondVar, paired.inner)
					row[i] = e.evaluateExpressionWithContext(ctx, evaluatedExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
				}
			}
			result.Rows = append(result.Rows, row)
		}

		return result, nil
	}

	// Default: return all paired items (inner values only)
	result := &ExecuteResult{
		Columns: []string{secondVar},
		Rows:    make([][]interface{}, len(allPairedItems)),
	}
	for i, paired := range allPairedItems {
		result.Rows[i] = []interface{}{paired.inner}
	}
	return result, nil
}

// ========================================
// UNION Clause
// ========================================

// executeUnion handles UNION / UNION ALL
// Supports both single UNION (query1 UNION query2) and chained UNIONs (query1 UNION query2 UNION query3 ...)
// Handles UNION with flexible whitespace (spaces, newlines, tabs)
func (e *StorageExecutor) executeUnion(ctx context.Context, cypher string, unionAll bool) (*ExecuteResult, error) {
	queries, splitAll, ok := splitTopLevelUnionBranches(cypher)
	if !ok || len(queries) < 2 {
		return nil, fmt.Errorf("UNION clause not found in query: %q", truncateQuery(cypher, 80))
	}
	if unionAll != splitAll {
		if unionAll {
			return nil, fmt.Errorf("UNION ALL clause not found in query: %q", truncateQuery(cypher, 80))
		}
		return nil, fmt.Errorf("UNION clause not found in query: %q", truncateQuery(cypher, 80))
	}

	// Execute all queries and combine results
	var combinedResult *ExecuteResult
	seen := make(map[string]bool) // For UNION (distinct) deduplication

	for i, query := range queries {
		result, err := e.executeInternal(ctx, query, nil)
		if err != nil {
			return nil, fmt.Errorf("error in UNION query %d (%q): %w", i+1, truncateQuery(query, 50), err)
		}
		// Some execution branches can return empty column metadata when no rows are produced,
		// even though the query has an explicit RETURN/YIELD projection. For UNION semantics we
		// must validate/provide branch column shapes deterministically.
		if len(result.Columns) == 0 {
			result.Columns = e.inferExplainColumns(query)
			if len(result.Columns) == 0 {
				// UNION branch execution can legitimately return zero rows; still preserve
				// projected column shape from the branch RETURN clause.
				result.Columns = e.inferTopLevelReturnColumns(query)
			}
		}

		if combinedResult == nil {
			// First query - initialize result
			combinedResult = &ExecuteResult{
				Columns: result.Columns,
				Rows:    make([][]interface{}, 0),
			}
		} else {
			// Validate column count matches
			if len(combinedResult.Columns) != len(result.Columns) {
				return nil, fmt.Errorf("UNION queries must return the same number of columns (got %d and %d)", len(combinedResult.Columns), len(result.Columns))
			}
		}

		// Add rows from this query
		if unionAll {
			// UNION ALL - include all rows
			combinedResult.Rows = append(combinedResult.Rows, result.Rows...)
		} else {
			// UNION (distinct) - deduplicate rows
			for _, row := range result.Rows {
				key := fmt.Sprintf("%v", row)
				if !seen[key] {
					combinedResult.Rows = append(combinedResult.Rows, row)
					seen[key] = true
				}
			}
		}
	}

	if combinedResult == nil {
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return combinedResult, nil
}

// ========================================
// OPTIONAL MATCH Clause
// ========================================

// executeOptionalMatch handles OPTIONAL MATCH - returns null for non-matches
func (e *StorageExecutor) executeOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)
	optMatchIdx := strings.Index(upper, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found in query: %q", truncateQuery(cypher, 80))
	}

	modifiedQuery := cypher[:optMatchIdx] + "MATCH" + cypher[optMatchIdx+14:]

	result, err := e.executeMatch(ctx, modifiedQuery)

	// Handle error case - return result with null values
	if err != nil {
		// Default to a single null row if we can't determine columns
		return &ExecuteResult{
			Columns: []string{"result"},
			Rows:    [][]interface{}{{nil}},
		}, nil
	}

	// Handle empty result - return null row preserving columns
	if len(result.Rows) == 0 {
		nullRow := make([]interface{}, len(result.Columns))
		for i := range nullRow {
			nullRow[i] = nil
		}
		return &ExecuteResult{
			Columns: result.Columns,
			Rows:    [][]interface{}{nullRow},
		}, nil
	}

	return result, nil
}

// joinedRow represents a row from a left outer join between MATCH and OPTIONAL MATCH
type joinedRow struct {
	initialNode  *storage.Node
	relatedNode  *storage.Node
	relationship *storage.Edge
}

// optionalRelPattern holds parsed relationship info for OPTIONAL MATCH
type optionalRelPattern struct {
	sourceVar    string
	relType      string
	relVar       string
	targetVar    string
	targetLabels []string
	targetProps  map[string]interface{}
	direction    string // "out", "in", "both"
}

// optionalRelResult holds a node and its connecting edge for OPTIONAL MATCH
type optionalRelResult struct {
	node *storage.Node
	edge *storage.Edge
}

// executeCompoundMatchOptionalMatch handles MATCH ... OPTIONAL MATCH ... WITH ... RETURN queries
// This implements left outer join semantics for relationship traversals with aggregation support
func (e *StorageExecutor) executeCompoundMatchOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
	originalCypher := cypher
	params := getParamsFromContext(ctx)
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find OPTIONAL MATCH position
	optMatchIdx := findKeywordIndex(cypher, "OPTIONAL MATCH")
	if optMatchIdx == -1 {
		return nil, fmt.Errorf("OPTIONAL MATCH not found in compound query: %q", truncateQuery(cypher, 80))
	}

	// Find WITH or RETURN after OPTIONAL MATCH
	remainingAfterOptMatch := cypher[optMatchIdx+14:] // Skip "OPTIONAL MATCH"
	withIdx := findKeywordIndex(remainingAfterOptMatch, "WITH")
	returnIdx := findKeywordIndex(remainingAfterOptMatch, "RETURN")

	// Determine where OPTIONAL MATCH pattern ends
	optMatchEndIdx := len(remainingAfterOptMatch)
	if withIdx > 0 && (returnIdx == -1 || withIdx < returnIdx) {
		optMatchEndIdx = withIdx
	} else if returnIdx > 0 {
		optMatchEndIdx = returnIdx
	}

	optMatchPattern := strings.TrimSpace(remainingAfterOptMatch[:optMatchEndIdx])
	restOfQuery := ""
	if optMatchEndIdx < len(remainingAfterOptMatch) {
		restOfQuery = strings.TrimSpace(remainingAfterOptMatch[optMatchEndIdx:])
	}

	// Parse the initial MATCH clause section (everything between MATCH and OPTIONAL MATCH)
	// This may contain: node pattern, WHERE clause, and WITH DISTINCT
	initialSection := strings.TrimSpace(cypher[5:optMatchIdx]) // Get original case, skip "MATCH"
	originalOptMatchIdx := findKeywordIndex(originalCypher, "OPTIONAL MATCH")
	initialSectionRaw := initialSection
	if originalOptMatchIdx > 5 {
		initialSectionRaw = strings.TrimSpace(originalCypher[5:originalOptMatchIdx])
	}

	// Extract WHERE clause if present (between node pattern and WITH DISTINCT/OPTIONAL MATCH)
	var whereClause string
	whereIdx := findKeywordIndex(initialSection, "WHERE")

	// Find standalone WITH (not part of "STARTS WITH" or "ENDS WITH")
	firstWithIdx := findStandaloneWithIndex(initialSection)

	// Determine the node pattern end
	nodePatternEnd := len(initialSection)
	if whereIdx > 0 {
		nodePatternEnd = whereIdx
	} else if firstWithIdx > 0 {
		nodePatternEnd = firstWithIdx
	}

	nodePatternStr := strings.TrimSpace(initialSection[:nodePatternEnd])
	nodePattern := e.parseNodePattern(ctx, nodePatternStr)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern from MATCH clause: %q", truncateQuery(nodePatternStr, 50))
	}

	// Extract WHERE clause content if present
	if whereIdx > 0 {
		whereEnd := len(initialSection)
		if firstWithIdx > whereIdx {
			whereEnd = firstWithIdx
		}
		whereClause = strings.TrimSpace(initialSection[whereIdx+5 : whereEnd]) // Skip "WHERE"
	}
	rawWhereClause := ""
	if rawWhereIdx := findKeywordIndex(initialSectionRaw, "WHERE"); rawWhereIdx > 0 {
		rawWhereEnd := len(initialSectionRaw)
		if rawWithIdx := findStandaloneWithIndex(initialSectionRaw); rawWithIdx > rawWhereIdx {
			rawWhereEnd = rawWithIdx
		}
		rawWhereClause = strings.TrimSpace(initialSectionRaw[rawWhereIdx+5 : rawWhereEnd])
	}

	// Collect initial MATCH nodes with index-backed candidates when possible.
	initialNodes, err := e.collectOptionalMatchInitialNodes(ctx, nodePattern, whereClause, rawWhereClause, params)
	if err != nil {
		return nil, fmt.Errorf("failed to get initial nodes: %w", err)
	}

	// Parse the OPTIONAL MATCH relationship pattern
	relPattern := e.parseOptionalRelPattern(ctx, optMatchPattern)

	// Fast path: OPTIONAL MATCH incoming count aggregation (Northwind-style).
	// Avoid building joinedRows and per-node edge scans.
	if res, ok, err := e.tryFastCompoundOptionalMatchCount(initialNodes, nodePattern, relPattern, restOfQuery); ok || err != nil {
		if err != nil {
			return nil, err
		}
		return res, nil
	}

	// Build result rows - this is left outer join semantics
	var joinedRows []joinedRow

	for _, node := range initialNodes {
		// Try to find related nodes via the relationship
		relatedNodes := e.findOptionalRelatedNodes(ctx, node, optMatchPattern, relPattern)

		if len(relatedNodes) == 0 {
			// No match - add row with null for the optional part (left outer join)
			joinedRows = append(joinedRows, joinedRow{
				initialNode:  node,
				relatedNode:  nil,
				relationship: nil,
			})
		} else {
			// Add a row for each match
			for _, related := range relatedNodes {
				joinedRows = append(joinedRows, joinedRow{
					initialNode:  node,
					relatedNode:  related.node,
					relationship: related.edge,
				})
			}
		}
	}

	// Now process WITH and RETURN clauses
	if strings.HasPrefix(strings.ToUpper(restOfQuery), "WITH") {
		optMatchAfterWith := findKeywordIndex(restOfQuery, "OPTIONAL MATCH")
		returnAfterWith := findKeywordIndex(restOfQuery, "RETURN")
		if optMatchAfterWith > 0 && (returnAfterWith == -1 || optMatchAfterWith < returnAfterWith) {
			return e.executeJoinedRowsWithOptionalMatch(ctx, joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
		}
		return e.processWithAggregation(ctx, joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
	}

	if strings.HasPrefix(strings.ToUpper(restOfQuery), "RETURN") {
		return e.buildJoinedResult(ctx, joinedRows, nodePattern.variable, relPattern.targetVar, relPattern.relVar, restOfQuery)
	}

	// No WITH or RETURN, just return count
	return &ExecuteResult{
		Columns: []string{"matched"},
		Rows:    [][]interface{}{{int64(len(joinedRows))}},
	}, nil
}

func (e *StorageExecutor) collectOptionalMatchInitialNodes(
	ctx context.Context,
	nodePattern nodePatternInfo,
	whereClause string,
	rawWhereClause string,
	params map[string]interface{},
) ([]*storage.Node, error) {
	var (
		nodes             []*storage.Node
		err               error
		usedPropertyIndex bool
	)

	if rawWhereClause != "" {
		inWherePart := rawWhereClause
		if candidates, used, idxErr := e.tryCollectNodesFromIDEqualityParam(ctx, nodePattern, inWherePart, params); idxErr == nil && used {
			nodes = candidates
			usedPropertyIndex = true
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromIDInParam(nodePattern, inWherePart, params); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexInOrParam(nodePattern, inWherePart, params); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexOrEquality(ctx, nodePattern, inWherePart, params); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexIn(nodePattern, inWherePart, params); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
	}

	if !usedPropertyIndex && whereClause != "" {
		if candidates, used, idxErr := e.tryCollectNodesFromIDEquality(ctx, nodePattern, whereClause); idxErr == nil && used {
			nodes = candidates
			usedPropertyIndex = true
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexInLiteral(ctx, nodePattern, whereClause); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndex(ctx, nodePattern, whereClause); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
		if !usedPropertyIndex {
			if candidates, used, idxErr := e.tryCollectNodesFromPropertyIndexNotNull(nodePattern, whereClause); idxErr == nil && used {
				nodes = candidates
				usedPropertyIndex = true
			}
		}
	}

	if !usedPropertyIndex {
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nodePattern.labels)
		if err != nil {
			return nil, err
		}
	}
	if usedPropertyIndex && len(nodes) == 0 && whereClause != "" {
		// Fail-open on possible stale index metadata/candidates.
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nodePattern.labels)
		if err != nil {
			return nil, err
		}
	}

	// Filter by pattern properties if any.
	if len(nodePattern.properties) > 0 {
		filtered := make([]*storage.Node, 0, len(nodes))
		for _, node := range nodes {
			match := true
			for k, v := range nodePattern.properties {
				if node.Properties[k] != v {
					match = false
					break
				}
			}
			if match {
				filtered = append(filtered, node)
			}
		}
		nodes = filtered
	}

	// Apply WHERE filtering for full semantic correctness.
	if whereClause != "" {
		nodes = e.filterNodes(ctx, nodes, nodePattern.variable, whereClause)
	}
	return nodes, nil
}

// parseOptionalRelPattern parses patterns like (a)-[r:TYPE]->(b:Label)
func (e *StorageExecutor) parseOptionalRelPattern(ctx context.Context, pattern string) optionalRelPattern {
	result := optionalRelPattern{
		direction:   "out",
		targetProps: make(map[string]interface{}),
	}
	pattern = strings.TrimSpace(pattern)

	// Check direction
	if strings.Contains(pattern, "<-") {
		result.direction = "in"
	} else if strings.Contains(pattern, "->") {
		result.direction = "out"
	} else if strings.Contains(pattern, "-") {
		result.direction = "both"
	}

	// Extract source variable
	if idx := strings.Index(pattern, "("); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], ")")
		if endIdx > 0 {
			sourceStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(sourceStr, ":"); colonIdx > 0 {
				result.sourceVar = strings.TrimSpace(sourceStr[:colonIdx])
			} else {
				result.sourceVar = strings.TrimSpace(sourceStr)
			}
		}
	}

	// Extract relationship type and variable
	if idx := strings.Index(pattern, "["); idx >= 0 {
		endIdx := strings.Index(pattern[idx:], "]")
		if endIdx > 0 {
			relStr := pattern[idx+1 : idx+endIdx]
			if colonIdx := strings.Index(relStr, ":"); colonIdx >= 0 {
				result.relVar = strings.TrimSpace(relStr[:colonIdx])
				result.relType = strings.TrimSpace(relStr[colonIdx+1:])
			} else {
				result.relVar = strings.TrimSpace(relStr)
			}
		}
	}

	// Extract target
	relEnd := strings.Index(pattern, "]")
	if relEnd > 0 {
		remaining := pattern[relEnd+1:]
		if idx := strings.Index(remaining, "("); idx >= 0 {
			endIdx := strings.Index(remaining[idx:], ")")
			if endIdx > 0 {
				targetStr := remaining[idx+1 : idx+endIdx]
				targetInfo := e.parseNodePattern(ctx, "("+targetStr+")")
				if targetInfo.variable != "" {
					result.targetVar = targetInfo.variable
				}
				if len(targetInfo.labels) > 0 {
					result.targetLabels = append([]string(nil), targetInfo.labels...)
				}
				if len(targetInfo.properties) > 0 {
					result.targetProps = targetInfo.properties
				}
			}
		}
	}

	return result
}

// findRelatedNodes finds nodes connected via the specified relationship pattern
func (e *StorageExecutor) findRelatedNodes(sourceNode *storage.Node, pattern optionalRelPattern) []optionalRelResult {
	var results []optionalRelResult
	var edges []*storage.Edge

	// Get edges based on direction
	switch pattern.direction {
	case "out":
		outEdges, err := e.storage.GetOutgoingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = outEdges
	case "in":
		inEdges, err := e.storage.GetIncomingEdges(sourceNode.ID)
		if err != nil {
			return results
		}
		edges = inEdges
	case "both":
		outEdges, _ := e.storage.GetOutgoingEdges(sourceNode.ID)
		inEdges, _ := e.storage.GetIncomingEdges(sourceNode.ID)
		edges = append(outEdges, inEdges...)
	}

	for _, edge := range edges {
		// Check relationship type if specified
		if pattern.relType != "" && edge.Type != pattern.relType {
			continue
		}

		// Determine target node ID
		var targetNodeID storage.NodeID
		if edge.StartNode == sourceNode.ID {
			targetNodeID = edge.EndNode
		} else {
			targetNodeID = edge.StartNode
		}

		// Get the target node
		targetNode, err := e.storage.GetNode(targetNodeID)
		if err != nil || targetNode == nil {
			continue
		}

		// Check target labels if specified.
		if len(pattern.targetLabels) > 0 && !mergeNodeHasLabels(targetNode, pattern.targetLabels) {
			continue
		}

		// Check target properties if specified
		if len(pattern.targetProps) > 0 {
			match := true
			for k, expected := range pattern.targetProps {
				actual, ok := targetNode.Properties[k]
				if !ok || !e.compareEqual(actual, expected) {
					match = false
					break
				}
			}
			if !match {
				continue
			}
		}

		results = append(results, optionalRelResult{node: targetNode, edge: edge})
	}

	return results
}

func (e *StorageExecutor) findOptionalRelatedNodes(ctx context.Context, sourceNode *storage.Node, patternText string, pattern optionalRelPattern) []optionalRelResult {
	if strings.Contains(patternText, "*") {
		traversal := e.parseTraversalPattern(ctx, patternText)
		if traversal == nil {
			return nil
		}
		paths := e.traverseFromNode(ctx, sourceNode, traversal)
		results := make([]optionalRelResult, 0, len(paths))
		seen := make(map[string]bool)
		for _, path := range paths {
			if len(path.Nodes) == 0 {
				continue
			}
			node := path.Nodes[len(path.Nodes)-1]
			var edge *storage.Edge
			if len(path.Relationships) > 0 {
				edge = path.Relationships[0]
			}
			key := string(node.ID)
			if edge != nil {
				key += ":" + string(edge.ID)
			}
			if !seen[key] {
				seen[key] = true
				results = append(results, optionalRelResult{node: node, edge: edge})
			}
		}
		return results
	}

	return e.findRelatedNodes(sourceNode, pattern)
}

// processWithAggregation handles WITH clauses with aggregation functions
// It finds the WITH clause that contains aggregations and processes them
// Also evaluates CASE WHEN expressions in WITH clauses
func buildJoinedEvaluationContext(row joinedRow, sourceVar, targetVar, relVar string) (map[string]*storage.Node, map[string]*storage.Edge) {
	nodeCtx := make(map[string]*storage.Node, 2)
	if sourceVar != "" {
		nodeCtx[sourceVar] = row.initialNode
	}
	if targetVar != "" {
		nodeCtx[targetVar] = row.relatedNode
	}
	relCtx := make(map[string]*storage.Edge, 1)
	if relVar != "" {
		relCtx[relVar] = row.relationship
	}
	return nodeCtx, relCtx
}

func stripTrailingReturnClauses(returnClause string) string {
	returnEnd := len(returnClause)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(returnClause, keyword); idx >= 0 && idx < returnEnd {
			returnEnd = idx
		}
	}
	return strings.TrimSpace(returnClause[:returnEnd])
}

func joinedValueKey(val interface{}) string {
	switch v := val.(type) {
	case *storage.Node:
		if v == nil {
			return "node:nil"
		}
		return "node:" + string(v.ID)
	case *storage.Edge:
		if v == nil {
			return "edge:nil"
		}
		return "edge:" + string(v.ID)
	default:
		return fmt.Sprintf("%#v", val)
	}
}

func (e *StorageExecutor) executeJoinedRowsWithOptionalMatch(ctx context.Context, rows []joinedRow, sourceVar, targetVar, relVar, query string) (*ExecuteResult, error) {
	withIdx := findKeywordIndex(query, "WITH")
	optMatchIdx := findKeywordIndex(query, "OPTIONAL MATCH")
	returnIdx := findKeywordIndex(query, "RETURN")
	if withIdx == -1 || optMatchIdx == -1 || returnIdx == -1 {
		return nil, fmt.Errorf("WITH, OPTIONAL MATCH, and RETURN clauses required")
	}

	withSection := strings.TrimSpace(query[withIdx+4 : optMatchIdx])
	postWithWhere := ""
	withClause := withSection
	if postWhereIdx := findKeywordIndex(withSection, "WHERE"); postWhereIdx > 0 {
		withClause = strings.TrimSpace(withSection[:postWhereIdx])
		postWithWhere = strings.TrimSpace(withSection[postWhereIdx+5:])
	}

	distinct := false
	if strings.HasPrefix(strings.ToUpper(withClause), "DISTINCT ") {
		distinct = true
		withClause = strings.TrimSpace(withClause[9:])
	}

	withItems := e.splitWithItems(withClause)
	type computedRow struct {
		values map[string]interface{}
	}
	computedRows := make([]computedRow, 0, len(rows))
	withAliases := make([]string, 0, len(withItems))

	for _, row := range rows {
		nodeCtx, relCtx := buildJoinedEvaluationContext(row, sourceVar, targetVar, relVar)
		values := make(map[string]interface{}, len(withItems))
		for _, item := range withItems {
			item = strings.TrimSpace(item)
			if item == "" {
				continue
			}
			upperItem := strings.ToUpper(item)
			alias := item
			expr := item
			if asIdx := strings.Index(upperItem, " AS "); asIdx > 0 {
				expr = strings.TrimSpace(item[:asIdx])
				alias = strings.TrimSpace(item[asIdx+4:])
			}
			values[alias] = e.evaluateExpressionWithContext(ctx, expr, nodeCtx, relCtx)
			if len(computedRows) == 0 {
				withAliases = append(withAliases, alias)
			}
		}
		computedRows = append(computedRows, computedRow{values: values})
	}

	if distinct {
		seen := make(map[string]bool)
		unique := make([]computedRow, 0, len(computedRows))
		for _, row := range computedRows {
			parts := make([]string, 0, len(withAliases))
			for _, alias := range withAliases {
				parts = append(parts, alias+"="+joinedValueKey(row.values[alias]))
			}
			key := strings.Join(parts, "|")
			if !seen[key] {
				seen[key] = true
				unique = append(unique, row)
			}
		}
		computedRows = unique
	}

	if postWithWhere != "" {
		filtered := make([]computedRow, 0, len(computedRows))
		for _, row := range computedRows {
			if e.evaluateWithWhereCondition(ctx, postWithWhere, row.values) {
				filtered = append(filtered, row)
			}
		}
		computedRows = filtered
	}

	optMatchPattern := strings.TrimSpace(query[optMatchIdx+14 : returnIdx])
	optMatchWhereClause := ""
	if optMatchWhereIdx := findKeywordIndex(optMatchPattern, "WHERE"); optMatchWhereIdx > 0 {
		optMatchWhereClause = strings.TrimSpace(optMatchPattern[optMatchWhereIdx+5:])
		optMatchPattern = strings.TrimSpace(optMatchPattern[:optMatchWhereIdx])
	}
	relPattern := e.parseOptionalRelPattern(ctx, optMatchPattern)

	type optionalRow struct {
		computedValues map[string]interface{}
		relatedNode    *storage.Node
		relationship   *storage.Edge
	}
	joinedOptionalRows := make([]optionalRow, 0, len(computedRows))
	for _, row := range computedRows {
		sourceNode, _ := row.values[relPattern.sourceVar].(*storage.Node)
		if sourceNode == nil {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
			continue
		}
		relatedNodes := e.findOptionalRelatedNodes(ctx, sourceNode, optMatchPattern, relPattern)
		if len(relatedNodes) == 0 {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
			continue
		}
		addedAny := false
		for _, related := range relatedNodes {
			if optMatchWhereClause != "" && related.node != nil && !e.nodeMatchesWhereClause(ctx, related.node, optMatchWhereClause, relPattern.targetVar) {
				continue
			}
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values, relatedNode: related.node, relationship: related.edge})
			addedAny = true
		}
		if !addedAny {
			joinedOptionalRows = append(joinedOptionalRows, optionalRow{computedValues: row.values})
		}
	}

	returnClause := stripTrailingReturnClauses(strings.TrimSpace(query[returnIdx+6:]))
	returnItems := e.parseReturnItems(returnClause)
	result := &ExecuteResult{Columns: make([]string, len(returnItems)), Rows: make([][]interface{}, 0, len(joinedOptionalRows))}
	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, row := range joinedOptionalRows {
		resultRow := make([]interface{}, len(returnItems))
		nodeMap := make(map[string]*storage.Node)
		edgeMap := make(map[string]*storage.Edge)
		for varName, val := range row.computedValues {
			if node, ok := val.(*storage.Node); ok {
				nodeMap[varName] = node
			}
			if edge, ok := val.(*storage.Edge); ok {
				edgeMap[varName] = edge
			}
		}
		if relPattern.targetVar != "" {
			nodeMap[relPattern.targetVar] = row.relatedNode
		}
		if relPattern.relVar != "" {
			edgeMap[relPattern.relVar] = row.relationship
		}
		for i, item := range returnItems {
			if val, ok := row.computedValues[item.expr]; ok {
				resultRow[i] = val
				continue
			}
			resultRow[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeMap, edgeMap)
		}
		result.Rows = append(result.Rows, resultRow)
	}

	orderByIdx := findKeywordIndex(query, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5
		for orderStart < len(query) && isWhitespace(query[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(query) && strings.EqualFold(query[orderStart:orderStart+2], "BY") {
			orderStart += 2
		}
		orderPart := query[orderStart:]
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	skipIdx := findKeywordIndex(query, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(query[skipIdx+4:])
		skipPart = strings.Fields(skipPart)[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	limitIdx := findKeywordIndex(query, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(query[limitIdx+5:])
		limitPart = strings.Fields(limitPart)[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	if skip > 0 || limit >= 0 {
		startIdx := skip
		if startIdx > len(result.Rows) {
			startIdx = len(result.Rows)
		}
		endIdx := len(result.Rows)
		if limit >= 0 && startIdx+limit < endIdx {
			endIdx = startIdx + limit
		}
		result.Rows = result.Rows[startIdx:endIdx]
	}

	return result, nil
}

func (e *StorageExecutor) processWithAggregation(ctx context.Context, rows []joinedRow, sourceVar, targetVar, relVar, restOfQuery string) (*ExecuteResult, error) {
	// Find RETURN clause
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required after WITH")
	}

	// First, check for CASE WHEN expressions in the first WITH clause and evaluate them
	// This computes values like: WITH f, c, CASE WHEN c IS NOT NULL THEN 1 ELSE 0 END as hasChunk
	computedValues := make(map[int]map[string]interface{}) // row index -> computed values
	firstWithIdx := findKeywordIndex(restOfQuery, "WITH")
	if firstWithIdx >= 0 {
		// Find where first WITH ends (at next WITH, RETURN, or end)
		firstWithEnd := returnIdx
		nextWithIdx := findKeywordIndex(restOfQuery[firstWithIdx+4:], "WITH")
		if nextWithIdx > 0 {
			firstWithEnd = firstWithIdx + 4 + nextWithIdx
		}

		firstWithClause := strings.TrimSpace(restOfQuery[firstWithIdx+4 : firstWithEnd])
		withItems := e.splitWithItems(firstWithClause)

		// Check if any item is a CASE expression
		for _, item := range withItems {
			item = strings.TrimSpace(item)
			upperItem := strings.ToUpper(item)
			asIdx := strings.Index(upperItem, " AS ")
			if asIdx > 0 {
				expr := strings.TrimSpace(item[:asIdx])
				alias := strings.TrimSpace(item[asIdx+4:])

				if isCaseExpression(expr) {
					// Evaluate CASE for each row
					for rowIdx, r := range rows {
						if computedValues[rowIdx] == nil {
							computedValues[rowIdx] = make(map[string]interface{})
						}
						nodeMap, relMap := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
						computedValues[rowIdx][alias] = e.evaluateCaseExpression(ctx, expr, nodeMap, relMap)
					}
				}
			}
		}
	}

	// Find the WITH clause that contains the aggregations
	// This handles cases like: WITH f, c, CASE... WITH COUNT(f)... RETURN...
	// We need to find the WITH that has COUNT/SUM/COLLECT etc.
	aggregationWithStart := -1
	aggregationWithEnd := returnIdx

	// Look for WITH clauses between start and RETURN
	queryBeforeReturn := restOfQuery[:returnIdx]
	withIdx := 0
	for {
		nextWithIdx := findKeywordIndex(queryBeforeReturn[withIdx:], "WITH")
		if nextWithIdx == -1 {
			break
		}
		absWithIdx := withIdx + nextWithIdx
		// Check if this WITH clause contains aggregation functions
		nextClauseEnd := len(queryBeforeReturn)
		followingWithIdx := findKeywordIndex(queryBeforeReturn[absWithIdx+4:], "WITH")
		if followingWithIdx > 0 {
			nextClauseEnd = absWithIdx + 4 + followingWithIdx
		}
		withContent := queryBeforeReturn[absWithIdx:nextClauseEnd]
		upperWithContent := strings.ToUpper(withContent)
		if strings.Contains(upperWithContent, "COUNT(") ||
			strings.Contains(upperWithContent, "SUM(") ||
			strings.Contains(upperWithContent, "COLLECT(") {
			aggregationWithStart = absWithIdx
			aggregationWithEnd = nextClauseEnd
			break
		}
		withIdx = absWithIdx + 4
	}

	// Parse the aggregation items from the WITH clause that contains them
	var returnItems []returnItem
	if aggregationWithStart >= 0 {
		withClause := strings.TrimSpace(restOfQuery[aggregationWithStart+4 : aggregationWithEnd])
		returnItems = e.parseReturnItems(withClause)
	} else {
		// No aggregation WITH found, use RETURN clause items
		returnClause := stripTrailingReturnClauses(strings.TrimSpace(restOfQuery[returnIdx+6:]))
		returnItems = e.parseReturnItems(returnClause)
	}

	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    [][]interface{}{},
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	row := make([]interface{}, len(returnItems))

	for i, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)

		switch {
		case strings.HasPrefix(upperExpr, "COUNT(DISTINCT "):
			inner := item.expr[15 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.initialNode != nil {
						seen[r.initialNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				seen := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.relatedNode != nil {
						seen[r.relatedNode.ID] = true
					}
				}
				row[i] = int64(len(seen))
			} else {
				row[i] = int64(0)
			}

		case strings.HasPrefix(upperExpr, "COUNT("):
			inner := item.expr[6 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)

			if inner == "*" {
				row[i] = int64(len(rows))
			} else if isCaseExpression(inner) {
				// COUNT(CASE WHEN condition THEN 1 END) - count only non-NULL results
				count := int64(0)
				for _, r := range rows {
					nodeMap, relMap := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					result := e.evaluateCaseExpression(ctx, inner, nodeMap, relMap)
					// count() only counts non-NULL values
					if result != nil {
						count++
					}
				}
				row[i] = count
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(sourceVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.initialNode != nil {
						count++
					}
				}
				row[i] = count
			} else if strings.HasPrefix(strings.ToUpper(inner), strings.ToUpper(targetVar)) {
				count := int64(0)
				for _, r := range rows {
					if r.relatedNode != nil {
						count++
					}
				}
				row[i] = count
			} else {
				row[i] = int64(len(rows))
			}

		case strings.HasPrefix(upperExpr, "SUM("):
			// Handle arithmetic of sum terms: SUM(x) + SUM(y) - SUM(z)
			if (strings.Contains(upperExpr, "+") || strings.Contains(upperExpr, "-")) && strings.Contains(upperExpr, "SUM(") {
				total := float64(0)
				lastOp := byte('+')
				start := 0
				parenDepth := 0
				inSingleQuote := false
				inDoubleQuote := false

				evalTerm := func(term string) (float64, bool, error) {
					inner, _, ok := extractFuncArgsWithSuffix(strings.TrimSpace(term), "sum")
					if !ok {
						return 0, false, fmt.Errorf("unsupported SUM arithmetic term: %s", strings.TrimSpace(term))
					}
					sumVal := float64(0)
					hasNonNull := false
					for rowIdx, r := range rows {
						var val interface{}
						if cv, ok := computedValues[rowIdx]; ok {
							if computed, exists := cv[strings.TrimSpace(inner)]; exists {
								val = computed
							}
						}
						if val == nil {
							nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
							val = e.evaluateExpressionWithContext(ctx, strings.TrimSpace(inner), nodeCtx, relCtx)
						}
						if val == nil {
							continue // SUM ignores NULLs
						}
						num, ok := toFloat64(val)
						if !ok {
							return 0, false, fmt.Errorf("SUM() requires numeric values, got %T in expression %q", val, strings.TrimSpace(inner))
						}
						hasNonNull = true
						sumVal += num
					}
					return sumVal, hasNonNull, nil
				}

				for idx := 0; idx < len(item.expr); idx++ {
					ch := item.expr[idx]
					if ch == '\'' && !inDoubleQuote {
						inSingleQuote = !inSingleQuote
					} else if ch == '"' && !inSingleQuote {
						inDoubleQuote = !inDoubleQuote
					}
					if inSingleQuote || inDoubleQuote {
						continue
					}
					if ch == '(' {
						parenDepth++
						continue
					}
					if ch == ')' && parenDepth > 0 {
						parenDepth--
						continue
					}
					if parenDepth == 0 && (ch == '+' || ch == '-') {
						term := strings.TrimSpace(item.expr[start:idx])
						if term != "" {
							termValue, _, err := evalTerm(term)
							if err != nil {
								return nil, err
							}
							if lastOp == '+' {
								total += termValue
							} else {
								total -= termValue
							}
						}
						lastOp = ch
						start = idx + 1
					}
				}
				lastTerm := strings.TrimSpace(item.expr[start:])
				if lastTerm != "" {
					termValue, _, err := evalTerm(lastTerm)
					if err != nil {
						return nil, err
					}
					if lastOp == '+' {
						total += termValue
					} else {
						total -= termValue
					}
				}
				row[i] = total
				break
			}

			inner := item.expr[4 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			sumVal := float64(0)
			hasNonNull := false
			for rowIdx, r := range rows {
				var val interface{}

				// Prefer computed aliases from preceding WITH.
				if cv, ok := computedValues[rowIdx]; ok {
					if computed, exists := cv[inner]; exists {
						val = computed
					}
				}

				if val == nil {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val = e.evaluateExpressionWithContext(ctx, inner, nodeCtx, relCtx)
				}

				if val == nil {
					continue // SUM ignores NULLs
				}
				num, ok := toFloat64(val)
				if !ok {
					return nil, fmt.Errorf("SUM() requires numeric values, got %T in expression %q", val, inner)
				}
				hasNonNull = true
				sumVal += num
			}
			if hasNonNull {
				row[i] = sumVal
			} else {
				row[i] = nil
			}

		case strings.HasPrefix(upperExpr, "COLLECT(DISTINCT "):
			// COLLECT(DISTINCT expression) - may have suffix like [..10]
			inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
			// Skip "DISTINCT " prefix
			if strings.HasPrefix(strings.ToUpper(inner), "DISTINCT ") {
				inner = strings.TrimSpace(inner[9:])
			}
			seen := make(map[string]bool) // Use string key for map comparison
			var collected []interface{}

			// Check if inner is simple property access or general expression
			if strings.Contains(inner, ".") && !strings.HasPrefix(inner, "{") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							key := fmt.Sprintf("%v", val)
							if !seen[key] {
								seen[key] = true
								collected = append(collected, val)
							}
						}
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT(DISTINCT { key: value })
				for _, r := range rows {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val := e.evaluateExpressionWithContext(ctx, inner, nodeCtx, relCtx)
					if val != nil {
						key := fmt.Sprintf("%v", val)
						if !seen[key] {
							seen[key] = true
							collected = append(collected, val)
						}
					}
				}
			}
			// Apply suffix (e.g., [..10] for slicing) if present
			if suffix != "" {
				row[i] = e.applyArraySuffix(collected, suffix)
			} else {
				row[i] = collected
			}

		case strings.HasPrefix(upperExpr, "COLLECT("):
			// COLLECT(expression) - may have suffix like [..10]
			inner, suffix, _ := extractFuncArgsWithSuffix(item.expr, "collect")
			var collected []interface{}

			// Check if inner is simple property access or general expression
			if strings.Contains(inner, ".") && !strings.HasPrefix(inner, "{") {
				parts := strings.SplitN(inner, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						if val, ok := node.Properties[propName]; ok {
							collected = append(collected, val)
						}
					}
				}
			} else {
				// General expression (e.g., map literal): COLLECT({ key: value })
				for _, r := range rows {
					nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
					val := e.evaluateExpressionWithContext(ctx, inner, nodeCtx, relCtx)
					if val != nil {
						collected = append(collected, val)
					}
				}
			}
			// Apply suffix (e.g., [..10] for slicing) if present
			if suffix != "" {
				row[i] = e.applyArraySuffix(collected, suffix)
			} else {
				row[i] = collected
			}

		default:
			expr := strings.TrimSpace(item.expr)
			found := false

			// Prefer previously computed WITH aliases/values for this row set.
			for rowIdx := range rows {
				if cv, ok := computedValues[rowIdx]; ok {
					if val, exists := cv[expr]; exists {
						row[i] = val
						found = true
						break
					}
				}
			}
			if found {
				break
			}

			// Bare variable projections from WITH (e.g., RETURN o, t, r).
			if strings.EqualFold(expr, sourceVar) {
				for _, r := range rows {
					if r.initialNode != nil {
						row[i] = r.initialNode
						found = true
						break
					}
				}
				if !found {
					row[i] = nil
				}
				break
			}
			if strings.EqualFold(expr, targetVar) {
				for _, r := range rows {
					if r.relatedNode != nil {
						row[i] = r.relatedNode
						found = true
						break
					}
				}
				if !found {
					row[i] = nil
				}
				break
			}
			if strings.EqualFold(expr, relVar) {
				for _, r := range rows {
					if r.relationship != nil {
						row[i] = r.relationship
						found = true
						break
					}
				}
				if !found {
					row[i] = nil
				}
				break
			}

			if strings.Contains(expr, ".") {
				// Handle simple property access: seed.name, connected.property, etc.
				parts := strings.SplitN(expr, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				// Get value from first row (for aggregated queries, all rows have the same source node)
				for _, r := range rows {
					var node *storage.Node
					if strings.EqualFold(varName, sourceVar) {
						node = r.initialNode
					} else if strings.EqualFold(varName, targetVar) {
						node = r.relatedNode
					}
					if node != nil {
						row[i] = node.Properties[propName]
						break // Use first non-nil value
					}
				}
			} else {
				// Try evaluating a non-aggregate expression with first available row context.
				if len(rows) > 0 {
					nodeCtx, relCtx := buildJoinedEvaluationContext(rows[0], sourceVar, targetVar, relVar)
					row[i] = e.evaluateExpressionWithContext(ctx, expr, nodeCtx, relCtx)
				}
			}
		}
	}

	result.Rows = append(result.Rows, row)
	return result, nil
}

// buildJoinedResult builds a result from joined rows for simple RETURN
// If RETURN contains aggregation functions, delegates to processWithAggregation
func (e *StorageExecutor) buildJoinedResult(ctx context.Context, rows []joinedRow, sourceVar, targetVar, relVar, restOfQuery string) (*ExecuteResult, error) {
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required")
	}

	returnClause := stripTrailingReturnClauses(strings.TrimSpace(restOfQuery[returnIdx+6:]))
	returnItems := e.parseReturnItems(returnClause)

	// Check if any return item is an aggregation function
	hasAggregation := false
	for _, item := range returnItems {
		upperExpr := strings.ToUpper(item.expr)
		if strings.HasPrefix(upperExpr, "COUNT(") ||
			strings.HasPrefix(upperExpr, "SUM(") ||
			strings.HasPrefix(upperExpr, "AVG(") ||
			strings.HasPrefix(upperExpr, "MIN(") ||
			strings.HasPrefix(upperExpr, "MAX(") ||
			strings.HasPrefix(upperExpr, "COLLECT(") {
			hasAggregation = true
			break
		}
	}

	// If there's an aggregation, delegate to processWithAggregation
	if hasAggregation {
		if grouped, ok := e.tryBuildJoinedGroupedCollectResult(ctx, rows, sourceVar, targetVar, relVar, returnItems); ok {
			return grouped, nil
		}
		return e.processWithAggregation(ctx, rows, sourceVar, targetVar, relVar, restOfQuery)
	}

	result := &ExecuteResult{
		Columns: make([]string, len(returnItems)),
		Rows:    make([][]interface{}, 0, len(rows)),
	}

	for i, item := range returnItems {
		if item.alias != "" {
			result.Columns[i] = item.alias
		} else {
			result.Columns[i] = item.expr
		}
	}

	for _, joinedRow := range rows {
		row := make([]interface{}, len(returnItems))
		nodeCtx, relCtx := buildJoinedEvaluationContext(joinedRow, sourceVar, targetVar, relVar)
		for i, item := range returnItems {
			row[i] = e.evaluateExpressionWithContext(ctx, item.expr, nodeCtx, relCtx)
		}
		result.Rows = append(result.Rows, row)
	}

	orderByIdx := findKeywordIndex(restOfQuery, "ORDER")
	if orderByIdx > 0 {
		orderStart := orderByIdx + 5
		for orderStart < len(restOfQuery) && isWhitespace(restOfQuery[orderStart]) {
			orderStart++
		}
		if orderStart+2 <= len(restOfQuery) && strings.EqualFold(restOfQuery[orderStart:orderStart+2], "BY") {
			orderStart += 2
		}
		orderPart := restOfQuery[orderStart:]
		endIdx := len(orderPart)
		for _, kw := range []string{"SKIP", "LIMIT"} {
			if idx := findKeywordIndex(orderPart, kw); idx >= 0 && idx < endIdx {
				endIdx = idx
			}
		}
		orderExpr := strings.TrimSpace(orderPart[:endIdx])
		result.Rows = e.orderResultRows(result.Rows, result.Columns, orderExpr)
	}

	skipIdx := findKeywordIndex(restOfQuery, "SKIP")
	skip := 0
	if skipIdx > 0 {
		skipPart := strings.TrimSpace(restOfQuery[skipIdx+4:])
		skipPart = strings.Fields(skipPart)[0]
		if s, err := strconv.Atoi(skipPart); err == nil {
			skip = s
		}
	}

	limitIdx := findKeywordIndex(restOfQuery, "LIMIT")
	limit := -1
	if limitIdx > 0 {
		limitPart := strings.TrimSpace(restOfQuery[limitIdx+5:])
		limitPart = strings.Fields(limitPart)[0]
		if l, err := strconv.Atoi(limitPart); err == nil {
			limit = l
		}
	}

	if skip > 0 || limit >= 0 {
		startIdx := skip
		if startIdx > len(result.Rows) {
			startIdx = len(result.Rows)
		}
		endIdx := len(result.Rows)
		if limit >= 0 && startIdx+limit < endIdx {
			endIdx = startIdx + limit
		}
		result.Rows = result.Rows[startIdx:endIdx]
	}

	return result, nil
}

// tryBuildJoinedGroupedCollectResult implements Cypher grouping semantics for
// joined-row pipelines that include COLLECT(...) alongside non-aggregate return
// expressions. It returns (result, true) when handled, otherwise (nil, false).
func (e *StorageExecutor) tryBuildJoinedGroupedCollectResult(ctx context.Context, rows []joinedRow, sourceVar, targetVar, relVar string, returnItems []returnItem) (*ExecuteResult, bool) {
	if len(returnItems) == 0 {
		return nil, false
	}
	isAggExpr := func(expr string) bool {
		return isAggregateFuncName(expr, "count") ||
			isAggregateFuncName(expr, "sum") ||
			isAggregateFuncName(expr, "avg") ||
			isAggregateFuncName(expr, "min") ||
			isAggregateFuncName(expr, "max") ||
			isAggregateFuncName(expr, "collect")
	}

	hasNonAgg := false
	for _, item := range returnItems {
		expr := strings.TrimSpace(item.expr)
		if !isAggregateFuncName(expr, "collect") {
			hasNonAgg = true
			continue
		}
	}
	if !hasNonAgg {
		return nil, false
	}
	for _, item := range returnItems {
		expr := strings.TrimSpace(item.expr)
		if isAggExpr(expr) && !isAggregateFuncName(expr, "collect") {
			return nil, false
		}
	}

	type grouped struct {
		keyVals []interface{}
		rows    []joinedRow
	}
	groups := make(map[string]*grouped)
	order := make([]string, 0)

	nonAggIndexes := make([]int, 0)
	for i, item := range returnItems {
		if !isAggExpr(strings.TrimSpace(item.expr)) {
			nonAggIndexes = append(nonAggIndexes, i)
		}
	}

	for _, jr := range rows {
		nodeCtx, relCtx := buildJoinedEvaluationContext(jr, sourceVar, targetVar, relVar)
		keyVals := make([]interface{}, len(nonAggIndexes))
		for ki, itemIdx := range nonAggIndexes {
			keyVals[ki] = e.evaluateExpressionWithContext(ctx, strings.TrimSpace(returnItems[itemIdx].expr), nodeCtx, relCtx)
		}
		key := fmt.Sprintf("%#v", keyVals)
		g, ok := groups[key]
		if !ok {
			g = &grouped{keyVals: keyVals, rows: make([]joinedRow, 0, 1)}
			groups[key] = g
			order = append(order, key)
		}
		g.rows = append(g.rows, jr)
	}

	res := &ExecuteResult{Columns: make([]string, len(returnItems)), Rows: make([][]interface{}, 0, len(order))}
	for i, item := range returnItems {
		if item.alias != "" {
			res.Columns[i] = item.alias
		} else {
			res.Columns[i] = item.expr
		}
	}

	nonAggPos := make(map[int]int, len(nonAggIndexes))
	for pos, idx := range nonAggIndexes {
		nonAggPos[idx] = pos
	}

	for _, key := range order {
		g := groups[key]
		row := make([]interface{}, len(returnItems))
		for i, item := range returnItems {
			expr := strings.TrimSpace(item.expr)
			if pos, ok := nonAggPos[i]; ok {
				row[i] = g.keyVals[pos]
				continue
			}

			// COLLECT and COLLECT(DISTINCT) over grouped rows.
			inner, suffix, _ := extractFuncArgsWithSuffix(expr, "collect")
			distinct := strings.HasPrefix(strings.ToUpper(strings.TrimSpace(inner)), "DISTINCT ")
			if distinct {
				inner = strings.TrimSpace(inner[len("DISTINCT "):])
			}
			collected := make([]interface{}, 0, len(g.rows))
			seen := map[string]struct{}{}
			for _, r := range g.rows {
				nodeCtx, relCtx := buildJoinedEvaluationContext(r, sourceVar, targetVar, relVar)
				normalizedInner := strings.TrimSpace(inner)
				if strings.HasPrefix(strings.ToUpper(normalizedInner), "CASE ") && !strings.HasSuffix(strings.ToUpper(normalizedInner), " END") {
					normalizedInner = normalizedInner + " END"
				}
				var val interface{}
				if isCaseExpression(normalizedInner) {
					val = e.evaluateCaseExpression(ctx, normalizedInner, nodeCtx, relCtx)
				} else {
					val = e.evaluateExpressionWithContext(ctx, normalizedInner, nodeCtx, relCtx)
				}
				if val == nil {
					continue // collect(null) ignores nulls (Neo4j compatible)
				}
				if distinct {
					h := fmt.Sprintf("%#v", val)
					if _, exists := seen[h]; exists {
						continue
					}
					seen[h] = struct{}{}
				}
				collected = append(collected, val)
			}
			if suffix != "" {
				row[i] = e.applyArraySuffix(collected, suffix)
			} else {
				row[i] = collected
			}
		}
		res.Rows = append(res.Rows, row)
	}

	return res, true
}

// ========================================
// FOREACH Clause
// ========================================

// executeForeach handles FOREACH clause - iterate and perform updates
func (e *StorageExecutor) executeForeach(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return e.executeForeachWithContext(ctx, cypher, make(map[string]*storage.Node), make(map[string]*storage.Edge))
}

// executeForeachWithContext executes a FOREACH clause with access to existing variable bindings.
//
// This is required for Neo4j-compatible patterns like:
//
//	OPTIONAL MATCH (a:TypeA {name: 'A1'})
//	FOREACH (x IN CASE WHEN a IS NOT NULL THEN [1] ELSE [] END | MERGE (e)-[:REL]->(a))
func (e *StorageExecutor) executeForeachWithContext(ctx context.Context, cypher string, nodeCtx map[string]*storage.Node, relCtx map[string]*storage.Edge) (*ExecuteResult, error) {
	foreachIdx := findKeywordIndex(cypher, "FOREACH")
	if foreachIdx == -1 {
		return nil, fmt.Errorf("FOREACH clause not found in query: %q", truncateQuery(cypher, 80))
	}

	parenStart := strings.Index(cypher[foreachIdx:], "(")
	if parenStart == -1 {
		return nil, fmt.Errorf("FOREACH requires parentheses (e.g., FOREACH (x IN list | SET ...))")
	}
	parenStart += foreachIdx

	depth := 1
	parenEnd := parenStart + 1
	for parenEnd < len(cypher) && depth > 0 {
		if cypher[parenEnd] == '(' {
			depth++
		} else if cypher[parenEnd] == ')' {
			depth--
		}
		parenEnd++
	}
	if depth != 0 {
		return nil, fmt.Errorf("FOREACH requires balanced parentheses")
	}

	inner := strings.TrimSpace(cypher[parenStart+1 : parenEnd-1])

	inIdx := strings.Index(strings.ToUpper(inner), " IN ")
	if inIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires IN clause (e.g., FOREACH (x IN list | SET ...))")
	}

	variable := strings.TrimSpace(inner[:inIdx])
	remainder := strings.TrimSpace(inner[inIdx+4:])

	pipeIdx := strings.Index(remainder, "|")
	if pipeIdx == -1 {
		return nil, fmt.Errorf("FOREACH requires | separator")
	}

	listExpr := strings.TrimSpace(remainder[:pipeIdx])
	updateClause := strings.TrimSpace(remainder[pipeIdx+1:])

	list := e.evaluateExpressionWithContext(ctx, listExpr, nodeCtx, relCtx)

	var items []interface{}
	switch v := list.(type) {
	case []interface{}:
		items = v
	case nil:
		items = nil
	default:
		items = []interface{}{list}
	}

	result := &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats:   &QueryStats{},
	}

	for _, item := range items {
		substituted := strings.TrimSpace(e.replaceVariableInQuery(updateClause, variable, item))
		if substituted == "" {
			continue
		}

		upper := strings.ToUpper(substituted)
		var updateResult *ExecuteResult
		var err error

		switch {
		case strings.HasPrefix(upper, "MERGE"):
			updateResult, err = e.executeMergeWithContext(ctx, substituted, nodeCtx, relCtx)
		default:
			// Fallback: execute as standalone clause.
			// This supports simple CREATE/SET/REMOVE updates that don't depend on external bindings.
			updateResult, err = e.executeInternal(ctx, substituted, nil)
		}

		if err != nil {
			return nil, err
		}

		if updateResult != nil && updateResult.Stats != nil {
			result.Stats.NodesCreated += updateResult.Stats.NodesCreated
			result.Stats.PropertiesSet += updateResult.Stats.PropertiesSet
			result.Stats.RelationshipsCreated += updateResult.Stats.RelationshipsCreated
		}
	}

	// Support continuation after FOREACH, e.g.:
	// FOREACH (...) RETURN ...
	trailing := strings.TrimSpace(cypher[parenEnd:])
	if trailing != "" {
		after, err := e.executeInternal(ctx, trailing, nil)
		if err != nil {
			return nil, err
		}
		if after != nil {
			if after.Stats == nil {
				after.Stats = &QueryStats{}
			}
			after.Stats.NodesCreated += result.Stats.NodesCreated
			after.Stats.PropertiesSet += result.Stats.PropertiesSet
			after.Stats.RelationshipsCreated += result.Stats.RelationshipsCreated
			return after, nil
		}
	}

	return result, nil
}

// ========================================
// LOAD CSV Clause
// ========================================

// executeLoadCSV handles LOAD CSV clause
func (e *StorageExecutor) executeLoadCSV(ctx context.Context, cypher string) (*ExecuteResult, error) {
	return nil, fmt.Errorf("LOAD CSV is not supported in NornicDB embedded mode")
}

// ========================================
// Helper Functions
// ========================================

// replaceVariableInQuery replaces all occurrences of a variable with its value in a query.
func (e *StorageExecutor) replaceVariableInQuery(query string, variable string, value interface{}) string {
	result := query

	// Handle property access patterns first (variable.property)
	// For maps, replace variable.key with the actual value.
	if valueMap, ok := toStringAnyMap(value); ok {
		// Find all property access patterns
		for key, propVal := range valueMap {
			propValStr := e.valueToLiteral(propVal)
			pattern := variable + "." + key
			result = strings.ReplaceAll(result, pattern, propValStr)
			backtickedPattern := variable + ".`" + strings.ReplaceAll(key, "`", "``") + "`"
			result = strings.ReplaceAll(result, backtickedPattern, propValStr)
		}
		// Also handle standalone variable references (e.g. SET n = row).
		value = valueMap
	}

	// Convert to a Cypher literal so maps/lists remain valid expressions.
	valueStr := e.valueToLiteral(value)
	// Guard against corrupting clauses like `WITH o, row` or
	// `UNWIND row.xs AS x` where bare `row` must survive substitution —
	// injecting a full map literal breaks the parser because `{...}` is
	// parsed as a node-property map.
	if _, ok := toStringAnyMap(value); ok {
		if findKeywordIndexInContext(query, "WITH") >= 0 ||
			findKeywordIndexInContext(query, "UNWIND") >= 0 {
			valueStr = "{}"
		}
	}
	return replaceIdentifierOutsideQuotes(result, variable, valueStr)
}

// replaceVariableInMutationQuery substitutes UNWIND row variables in mutation queries.
// For map-shaped rows, standalone variable tokens inside WITH pipelines are collapsed to
// "{}" to avoid parser ambiguity from large inline map literals, while row.property tokens
// are still replaced with their concrete values.
func (e *StorageExecutor) replaceVariableInMutationQuery(query string, variable string, value interface{}) string {
	result := query
	valueStr := e.valueToLiteral(value)

	if valueMap, ok := toStringAnyMap(value); ok {
		for key, propVal := range valueMap {
			propValStr := e.valueToLiteral(propVal)
			pattern := variable + "." + key
			result = strings.ReplaceAll(result, pattern, propValStr)
			backtickedPattern := variable + ".`" + strings.ReplaceAll(key, "`", "``") + "`"
			result = strings.ReplaceAll(result, backtickedPattern, propValStr)
		}
		// When the mutation contains WITH (carrying the row forward) or a
		// nested UNWIND over a row property, substituting bare `row` with the
		// full map literal corrupts the query shape. Use an empty-map
		// placeholder instead — the WITH/UNWIND bindings are recomputed by
		// the executor anyway.
		if findKeywordIndexInContext(query, "WITH") >= 0 ||
			findKeywordIndexInContext(query, "UNWIND") >= 0 {
			valueStr = "{}"
		}
	}

	return replaceIdentifierOutsideQuotes(result, variable, valueStr)
}

func replaceIdentifierOutsideQuotes(input string, ident string, replacement string) string {
	if ident == "" {
		return input
	}
	var b strings.Builder
	b.Grow(len(input) + 16)

	inSingle := false
	inDouble := false
	inBacktick := false
	for i := 0; i < len(input); {
		ch := input[i]
		switch {
		case inSingle:
			b.WriteByte(ch)
			i++
			if ch == '\'' {
				inSingle = false
			}
			continue
		case inDouble:
			b.WriteByte(ch)
			i++
			if ch == '"' {
				inDouble = false
			}
			continue
		case inBacktick:
			b.WriteByte(ch)
			i++
			if ch == '`' {
				inBacktick = false
			}
			continue
		}

		if ch == '\'' {
			inSingle = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '"' {
			inDouble = true
			b.WriteByte(ch)
			i++
			continue
		}
		if ch == '`' {
			inBacktick = true
			b.WriteByte(ch)
			i++
			continue
		}

		if !isIdentByte(ch) {
			b.WriteByte(ch)
			i++
			continue
		}
		start := i
		for i < len(input) && isIdentByte(input[i]) {
			i++
		}
		token := input[start:i]
		if token == ident && shouldReplaceIdentifierToken(input, start, i) {
			b.WriteString(replacement)
		} else {
			b.WriteString(token)
		}
	}
	return b.String()
}

func shouldReplaceIdentifierToken(input string, tokenStart int, tokenEnd int) bool {
	// Do not replace property tokens (n.name) or map keys ({name: ...}).
	prev := prevNonSpaceByte(input, tokenStart)
	if prev == '.' {
		return false
	}
	next := nextNonSpaceByte(input, tokenEnd)
	if next == ':' {
		return false
	}
	return true
}

func prevNonSpaceByte(s string, pos int) byte {
	for i := pos - 1; i >= 0; i-- {
		if !isASCIISpace(s[i]) {
			return s[i]
		}
	}
	return 0
}

func nextNonSpaceByte(s string, pos int) byte {
	for i := pos; i < len(s); i++ {
		if !isASCIISpace(s[i]) {
			return s[i]
		}
	}
	return 0
}

func toStringAnyMap(value interface{}) (map[string]interface{}, bool) {
	if m, ok := value.(map[string]interface{}); ok {
		return m, true
	}
	if m, ok := value.(map[interface{}]interface{}); ok {
		out := make(map[string]interface{}, len(m))
		for k, v := range m {
			ks, ok := k.(string)
			if !ok {
				return nil, false
			}
			out[ks] = v
		}
		return out, true
	}
	return nil, false
}
