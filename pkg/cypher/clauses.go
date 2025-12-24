// Cypher clause implementations for NornicDB.
// This file contains implementations for WITH, UNWIND, UNION, OPTIONAL MATCH,
// FOREACH, and LOAD CSV clauses.

package cypher

import (
	"context"
	"fmt"
	"regexp"
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

	upper := strings.ToUpper(cypher)

	withIdx := strings.Index(upper, "WITH")
	if withIdx == -1 {
		return nil, fmt.Errorf("WITH clause not found in query: %q", truncateQuery(cypher, 80))
	}

	remainderStart := withIdx + 4
	// Skip all whitespace (spaces, tabs, newlines)
	for remainderStart < len(cypher) && isWhitespace(cypher[remainderStart]) {
		remainderStart++
	}

	// Use findKeywordIndex which handles whitespace/newlines properly
	nextClauseKeywords := []string{"MATCH", "WHERE", "RETURN", "CREATE", "MERGE", "DELETE", "SET", "UNWIND", "ORDER", "SKIP", "LIMIT"}
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

		val := e.evaluateExpressionWithContext(expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
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
					returnValues[i] = e.evaluateExpressionWithContext(expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
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
				substitutedRemainder = strings.ReplaceAll(substitutedRemainder, varName, replacement)
			case string:
				substitutedRemainder = strings.ReplaceAll(substitutedRemainder, varName, fmt.Sprintf("'%s'", v))
			case nil:
				substitutedRemainder = strings.ReplaceAll(substitutedRemainder, varName, "null")
			default:
				substitutedRemainder = strings.ReplaceAll(substitutedRemainder, varName, fmt.Sprintf("%v", v))
			}
		}
		return e.executeInternal(ctx, substitutedRemainder, nil)
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
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
	// Substitute parameters AFTER routing to avoid keyword detection issues
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	upper := strings.ToUpper(cypher)

	// Check for double UNWIND - handle by recursively processing
	firstUnwind := strings.Index(upper, "UNWIND")
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

	unwindIdx := strings.Index(upper, "UNWIND")
	if unwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found in query: %q", truncateQuery(cypher, 80))
	}

	asIdx := strings.Index(upper, " AS ")
	if asIdx == -1 {
		return nil, fmt.Errorf("UNWIND requires AS clause (e.g., UNWIND [1,2,3] AS x)")
	}

	listExpr := strings.TrimSpace(cypher[unwindIdx+6 : asIdx])

	remainder := strings.TrimSpace(cypher[asIdx+4:])
	spaceIdx := strings.IndexAny(remainder, " \t\n")
	var variable string
	var restQuery string
	if spaceIdx > 0 {
		variable = remainder[:spaceIdx]
		restQuery = strings.TrimSpace(remainder[spaceIdx:])
	} else {
		variable = remainder
		restQuery = ""
	}

	list := e.evaluateExpressionWithContext(listExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

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
	default:
		// Single value gets wrapped in a list
		items = []interface{}{list}
	}

	// Handle UNWIND ... CREATE ... pattern
	if restQuery != "" && strings.HasPrefix(strings.ToUpper(restQuery), "CREATE") {
		result := &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Stats:   &QueryStats{},
		}

		// Split CREATE and RETURN parts
		returnIdx := findKeywordIndex(restQuery, "RETURN")
		var createPart, returnPart string
		if returnIdx > 0 {
			createPart = strings.TrimSpace(restQuery[:returnIdx])
			returnPart = strings.TrimSpace(restQuery[returnIdx:])
		} else {
			createPart = restQuery
			returnPart = ""
		}

		// Execute CREATE for each unwound item
		for _, item := range items {
			// Replace variable references ONLY in the CREATE clause
			createQuerySubstituted := replaceVariableInQuery(createPart, variable, item)

			// Reconstruct full query with RETURN
			fullQuery := createQuerySubstituted
			if returnPart != "" {
				fullQuery += " " + returnPart
			}

			// Execute the CREATE (with RETURN if present)
			createResult, err := e.executeCreate(ctx, fullQuery)
			if err != nil {
				return nil, fmt.Errorf("UNWIND CREATE failed: %w", err)
			}

			// Accumulate stats
			result.Stats.NodesCreated += createResult.Stats.NodesCreated
			result.Stats.RelationshipsCreated += createResult.Stats.RelationshipsCreated

			// If there's a RETURN clause, collect the result rows
			if returnPart != "" && len(createResult.Rows) > 0 {
				// First iteration: set columns
				if len(result.Columns) == 0 {
					result.Columns = createResult.Columns
				}
				// Append all rows from this CREATE execution
				result.Rows = append(result.Rows, createResult.Rows...)
			}
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
			for i, ri := range returnItems {
				if ri.expr == variable {
					row[i] = item
				}
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

// executeDoubleUnwind handles double UNWIND clauses like:
// UNWIND [[1,2],[3,4]] AS pair UNWIND pair AS num RETURN num
func (e *StorageExecutor) executeDoubleUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Check for dependent range expressions (range(1, i) where i is from first UNWIND)
	if containsOutsideStrings(upper, "RANGE(") {
		// Find if second UNWIND uses range with first variable
		firstAsIdx := findKeywordIndex(cypher, "AS")
		if firstAsIdx >= 0 {
			afterAs := strings.TrimSpace(cypher[firstAsIdx+2:])
			varEnd := strings.IndexAny(afterAs, " \t\n")
			if varEnd > 0 {
				firstVar := strings.ToUpper(afterAs[:varEnd])
				restQuery := strings.ToUpper(afterAs[varEnd:])
				// Check if range() contains the first variable
				if containsOutsideStrings(restQuery, "RANGE(") && containsOutsideStrings(restQuery, firstVar) {
					return nil, fmt.Errorf("dependent range in double UNWIND is not supported")
				}
			}
		}
	}

	// Parse first UNWIND
	firstUnwindIdx := strings.Index(upper, "UNWIND")
	if firstUnwindIdx == -1 {
		return nil, fmt.Errorf("UNWIND clause not found")
	}

	afterFirst := cypher[firstUnwindIdx+6:]
	firstAsIdx := strings.Index(strings.ToUpper(afterFirst), " AS ")
	if firstAsIdx == -1 {
		return nil, fmt.Errorf("first UNWIND requires AS clause")
	}

	firstListExpr := strings.TrimSpace(afterFirst[:firstAsIdx])
	afterFirstAs := strings.TrimSpace(afterFirst[firstAsIdx+4:])

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
	secondAsIdx := strings.Index(strings.ToUpper(afterSecond), " AS ")
	if secondAsIdx == -1 {
		return nil, fmt.Errorf("second UNWIND requires AS clause")
	}

	secondListExpr := strings.TrimSpace(afterSecond[:secondAsIdx])
	afterSecondAs := strings.TrimSpace(afterSecond[secondAsIdx+4:])

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
	firstList := e.evaluateExpressionWithContext(firstListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))

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

	for _, outerItem := range outerItems {
		// The second UNWIND expression should reference the first variable
		// If secondListExpr == firstVar, use outerItem directly (nested case)
		var innerList interface{}
		if secondListExpr == firstVar {
			innerList = outerItem
		} else {
			// Cartesian product - evaluate second list independently
			innerList = e.evaluateExpressionWithContext(secondListExpr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
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
					row[i] = e.evaluateExpressionWithContext(item.expr, make(map[string]*storage.Node), make(map[string]*storage.Edge))
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
	// Normalize whitespace for easier parsing (preserve structure but make UNION detection easier)
	// Replace newlines/tabs with spaces, then normalize multiple spaces to single space
	normalized := regexp.MustCompile(`\s+`).ReplaceAllString(cypher, " ")
	upper := strings.ToUpper(normalized)

	var separatorPattern *regexp.Regexp
	if unionAll {
		// Match "UNION ALL" with flexible whitespace
		separatorPattern = regexp.MustCompile(`(?i)\s+UNION\s+ALL\s+`)
	} else {
		// Match "UNION" with flexible whitespace (but not "UNION ALL")
		// We'll check manually to avoid matching "UNION ALL"
		separatorPattern = regexp.MustCompile(`(?i)\s+UNION\s+`)
	}

	// Find all UNION occurrences (handle chained UNIONs)
	var queries []string
	remaining := normalized
	lastIndex := 0

	for {
		matches := separatorPattern.FindStringIndex(upper[lastIndex:])
		if matches == nil {
			// No more UNIONs - add remaining query
			if strings.TrimSpace(remaining[lastIndex:]) != "" {
				queries = append(queries, strings.TrimSpace(remaining[lastIndex:]))
			}
			break
		}

		// Extract query before UNION
		unionStart := lastIndex + matches[0]
		unionEnd := lastIndex + matches[1]

		// For UNION (not UNION ALL), check if this is actually "UNION ALL"
		if !unionAll {
			// Check if the next characters after "UNION" are "ALL"
			if unionEnd < len(upper) {
				afterUnion := strings.TrimSpace(upper[unionEnd:])
				if strings.HasPrefix(afterUnion, "ALL") {
					// This is "UNION ALL", skip it (we're looking for plain UNION)
					// Find the end of "ALL"
					allEnd := unionEnd
					for allEnd < len(upper) && (upper[allEnd] == ' ' || upper[allEnd] == 'A' || upper[allEnd] == 'L') {
						if allEnd+1 < len(upper) && upper[allEnd] == 'L' && upper[allEnd+1] == 'L' {
							allEnd += 2
							break
						}
						allEnd++
					}
					lastIndex = allEnd
					continue
				}
			}
		}
		query := strings.TrimSpace(remaining[lastIndex:unionStart])
		if query != "" {
			queries = append(queries, query)
		}

		// Move past this UNION
		lastIndex = unionEnd
	}

	if len(queries) < 2 {
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
	sourceVar   string
	relType     string
	relVar      string
	targetVar   string
	targetLabel string
	direction   string // "out", "in", "both"
}

// optionalRelResult holds a node and its connecting edge for OPTIONAL MATCH
type optionalRelResult struct {
	node *storage.Node
	edge *storage.Edge
}

// executeCompoundMatchOptionalMatch handles MATCH ... OPTIONAL MATCH ... WITH ... RETURN queries
// This implements left outer join semantics for relationship traversals with aggregation support
func (e *StorageExecutor) executeCompoundMatchOptionalMatch(ctx context.Context, cypher string) (*ExecuteResult, error) {
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
	nodePattern := e.parseNodePattern(nodePatternStr)
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

	// Get all nodes matching the initial pattern
	var initialNodes []*storage.Node
	var err error
	if len(nodePattern.labels) > 0 {
		initialNodes, err = e.storage.GetNodesByLabel(nodePattern.labels[0])
	} else {
		initialNodes, err = e.storage.AllNodes()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get initial nodes: %w", err)
	}

	// Filter by properties if any
	if len(nodePattern.properties) > 0 {
		filtered := make([]*storage.Node, 0)
		for _, node := range initialNodes {
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
		initialNodes = filtered
	}

	// Apply WHERE clause filtering if present
	if whereClause != "" {
		initialNodes = e.filterNodes(initialNodes, nodePattern.variable, whereClause)
	}

	// Parse the OPTIONAL MATCH relationship pattern
	relPattern := e.parseOptionalRelPattern(optMatchPattern)

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
		relatedNodes := e.findRelatedNodes(node, relPattern)

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
		return e.processWithAggregation(joinedRows, nodePattern.variable, relPattern.targetVar, restOfQuery)
	}

	if strings.HasPrefix(strings.ToUpper(restOfQuery), "RETURN") {
		return e.buildJoinedResult(joinedRows, nodePattern.variable, relPattern.targetVar, restOfQuery)
	}

	// No WITH or RETURN, just return count
	return &ExecuteResult{
		Columns: []string{"matched"},
		Rows:    [][]interface{}{{int64(len(joinedRows))}},
	}, nil
}

// parseOptionalRelPattern parses patterns like (a)-[r:TYPE]->(b:Label)
func (e *StorageExecutor) parseOptionalRelPattern(pattern string) optionalRelPattern {
	result := optionalRelPattern{direction: "out"}
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
				if colonIdx := strings.Index(targetStr, ":"); colonIdx >= 0 {
					result.targetVar = strings.TrimSpace(targetStr[:colonIdx])
					result.targetLabel = strings.TrimSpace(targetStr[colonIdx+1:])
				} else {
					result.targetVar = strings.TrimSpace(targetStr)
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

		// Check target label if specified
		if pattern.targetLabel != "" {
			hasLabel := false
			for _, label := range targetNode.Labels {
				if label == pattern.targetLabel {
					hasLabel = true
					break
				}
			}
			if !hasLabel {
				continue
			}
		}

		results = append(results, optionalRelResult{node: targetNode, edge: edge})
	}

	return results
}

// processWithAggregation handles WITH clauses with aggregation functions
// It finds the WITH clause that contains aggregations and processes them
// Also evaluates CASE WHEN expressions in WITH clauses
func (e *StorageExecutor) processWithAggregation(rows []joinedRow, sourceVar, targetVar, restOfQuery string) (*ExecuteResult, error) {
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
						nodeMap := make(map[string]*storage.Node)
						if r.initialNode != nil {
							nodeMap[sourceVar] = r.initialNode
						}
						if r.relatedNode != nil {
							nodeMap[targetVar] = r.relatedNode
						}
						computedValues[rowIdx][alias] = e.evaluateCaseExpression(expr, nodeMap, nil)
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
		returnClause := strings.TrimSpace(restOfQuery[returnIdx+6:])
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
					nodeMap := make(map[string]*storage.Node)
					if r.initialNode != nil {
						nodeMap[sourceVar] = r.initialNode
					}
					if r.relatedNode != nil {
						nodeMap[targetVar] = r.relatedNode
					}
					result := e.evaluateCaseExpression(inner, nodeMap, nil)
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
			inner := item.expr[4 : len(item.expr)-1]
			inner = strings.TrimSpace(inner)
			sum := float64(0)

			// First check if inner refers to a computed value (from CASE WHEN)
			hasComputedValues := false
			for rowIdx := range rows {
				if cv, ok := computedValues[rowIdx]; ok {
					if val, exists := cv[inner]; exists {
						hasComputedValues = true
						if num, ok := toFloat64(val); ok {
							sum += num
						}
					}
				}
			}

			if !hasComputedValues {
				// Fall back to embedding check
				if strings.Contains(strings.ToUpper(inner), "EMBEDDING") {
					for _, r := range rows {
						if r.relatedNode != nil {
							if _, hasEmb := r.relatedNode.Properties["embedding"]; hasEmb {
								sum++
							}
						}
						if r.initialNode != nil {
							if _, hasEmb := r.initialNode.Properties["embedding"]; hasEmb {
								sum++
							}
						}
					}
				}
			}
			row[i] = sum

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
					nodeCtx := make(map[string]*storage.Node)
					relCtx := make(map[string]*storage.Edge)
					if r.initialNode != nil {
						nodeCtx[sourceVar] = r.initialNode
					}
					if r.relatedNode != nil {
						nodeCtx[targetVar] = r.relatedNode
					}
					if r.relationship != nil {
						relCtx["r"] = r.relationship
					}
					val := e.evaluateExpressionWithContext(inner, nodeCtx, relCtx)
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
					nodeCtx := make(map[string]*storage.Node)
					relCtx := make(map[string]*storage.Edge)
					if r.initialNode != nil {
						nodeCtx[sourceVar] = r.initialNode
					}
					if r.relatedNode != nil {
						nodeCtx[targetVar] = r.relatedNode
					}
					if r.relationship != nil {
						relCtx["r"] = r.relationship
					}
					val := e.evaluateExpressionWithContext(inner, nodeCtx, relCtx)
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
			// Check for arithmetic expressions: SUM(...) + SUM(...)
			if strings.Contains(upperExpr, "+") && strings.Contains(upperExpr, "SUM(") {
				// Handle SUM(x) + SUM(y) patterns used in VSCode stats query
				// The CASE WHEN computed values check for embedding IS NOT NULL
				// So SUM(chunkHasEmbedding) + SUM(fileHasEmbedding) counts embeddings
				sum := int64(0)

				// Count chunk embeddings (non-null)
				seenChunks := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.relatedNode != nil && !seenChunks[r.relatedNode.ID] {
						if _, hasEmb := r.relatedNode.Properties["embedding"]; hasEmb {
							seenChunks[r.relatedNode.ID] = true
							sum++
						}
					}
				}

				// Count file embeddings (non-null)
				seenFiles := make(map[storage.NodeID]bool)
				for _, r := range rows {
					if r.initialNode != nil && !seenFiles[r.initialNode.ID] {
						if _, hasEmb := r.initialNode.Properties["embedding"]; hasEmb {
							seenFiles[r.initialNode.ID] = true
							sum++
						}
					}
				}

				row[i] = sum
			} else if strings.Contains(item.expr, ".") {
				// Handle simple property access: seed.name, connected.property, etc.
				parts := strings.SplitN(item.expr, ".", 2)
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
				row[i] = nil
			}
		}
	}

	result.Rows = append(result.Rows, row)
	return result, nil
}

// buildJoinedResult builds a result from joined rows for simple RETURN
// If RETURN contains aggregation functions, delegates to processWithAggregation
func (e *StorageExecutor) buildJoinedResult(rows []joinedRow, sourceVar, targetVar, restOfQuery string) (*ExecuteResult, error) {
	returnIdx := findKeywordIndex(restOfQuery, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause required")
	}

	returnClause := strings.TrimSpace(restOfQuery[returnIdx+6:])
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
		return e.processWithAggregation(rows, sourceVar, targetVar, restOfQuery)
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
		for i, item := range returnItems {
			if strings.Contains(item.expr, ".") {
				parts := strings.SplitN(item.expr, ".", 2)
				varName := strings.TrimSpace(parts[0])
				propName := strings.TrimSpace(parts[1])

				var node *storage.Node
				if strings.EqualFold(varName, sourceVar) {
					node = joinedRow.initialNode
				} else if strings.EqualFold(varName, targetVar) {
					node = joinedRow.relatedNode
				}
				if node != nil {
					row[i] = node.Properties[propName]
				}
			} else if strings.EqualFold(item.expr, sourceVar) {
				row[i] = joinedRow.initialNode
			} else if strings.EqualFold(item.expr, targetVar) {
				row[i] = joinedRow.relatedNode
			}
		}
		result.Rows = append(result.Rows, row)
	}

	return result, nil
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

	list := e.evaluateExpressionWithContext(listExpr, nodeCtx, relCtx)

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
		substituted := strings.TrimSpace(replaceVariableInQuery(updateClause, variable, item))
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

// replaceVariableInQuery replaces all occurrences of a variable with its value in a query
func replaceVariableInQuery(query string, variable string, value interface{}) string {
	result := query

	// Handle property access patterns first (variable.property)
	// For maps, replace variable.key with the actual value
	if valueMap, ok := value.(map[string]interface{}); ok {
		// Find all property access patterns
		for key, propVal := range valueMap {
			pattern := variable + "." + key
			var propValStr string
			switch pv := propVal.(type) {
			case string:
				escaped := strings.ReplaceAll(pv, "'", "\\'")
				propValStr = fmt.Sprintf("'%s'", escaped)
			case int, int64:
				propValStr = fmt.Sprintf("%d", pv)
			case float64:
				propValStr = fmt.Sprintf("%f", pv)
			case bool:
				if pv {
					propValStr = "true"
				} else {
					propValStr = "false"
				}
			default:
				propValStr = fmt.Sprintf("%v", pv)
			}
			result = strings.ReplaceAll(result, pattern, propValStr)
		}
		// Don't replace standalone variable for maps - already handled property access
		return result
	}

	// For simple values, convert to string representation
	var valueStr string
	switch v := value.(type) {
	case string:
		// Escape single quotes in string values
		escaped := strings.ReplaceAll(v, "'", "\\'")
		valueStr = fmt.Sprintf("'%s'", escaped)
	case int, int64, int32:
		valueStr = fmt.Sprintf("%d", v)
	case float64, float32:
		valueStr = fmt.Sprintf("%f", v)
	case bool:
		if v {
			valueStr = "true"
		} else {
			valueStr = "false"
		}
	default:
		valueStr = fmt.Sprintf("%v", v)
	}

	// Replace standalone variable references
	// Need to be careful not to replace variable names that are part of other identifiers
	// Use regex to match word boundaries for more accurate replacement
	words := strings.Split(result, " ")
	for i, word := range words {
		// Remove common punctuation for comparison - include braces for property patterns like {name: name}
		trimmed := strings.TrimRight(word, ",;)}]")
		trimmed = strings.TrimLeft(trimmed, "({[")
		if trimmed == variable {
			// Replace the variable but preserve surrounding punctuation
			prefix := ""
			suffix := ""
			// Find where the variable actually starts/ends in the word
			varIdx := strings.Index(word, variable)
			if varIdx > 0 {
				prefix = word[:varIdx]
			}
			if varIdx >= 0 && varIdx+len(variable) < len(word) {
				suffix = word[varIdx+len(variable):]
			}
			words[i] = prefix + valueStr + suffix
		}
	}
	result = strings.Join(words, " ")

	return result
}
