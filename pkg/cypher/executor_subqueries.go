package cypher

import (
	"container/heap"
	"context"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// ===== CALL {} Subquery Support (Neo4j 4.0+) =====

// isCallSubquery detects if a query is a CALL {} subquery vs CALL procedure()
// CALL {} subqueries have "CALL" followed by optional whitespace and "{"
// CALL procedures have "CALL procedure.name()"
func isCallSubquery(cypher string) bool {
	// Use regex for flexible whitespace matching: CALL followed by optional whitespace and {
	return hasSubqueryPattern(cypher, callSubqueryRe)
}

// executeMatchWithCallProcedure handles MATCH ... CALL procedure() ... queries
// This allows procedure calls to use bound variables from the MATCH clause
// Example: MATCH (n:Node {id: 'n1'}) CALL db.index.vector.queryNodes('idx', 10, n.embedding) YIELD node, score
func (e *StorageExecutor) executeMatchWithCallProcedure(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters first
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find CALL position
	callIdx := findKeywordIndex(cypher, "CALL")
	if callIdx == -1 {
		return nil, fmt.Errorf("CALL not found in query")
	}

	// Extract the MATCH part (before CALL)
	matchPart := strings.TrimSpace(cypher[:callIdx])
	matchIdx := findKeywordIndex(matchPart, "MATCH")
	if matchIdx == -1 {
		return nil, fmt.Errorf("MATCH not found before CALL")
	}

	// Extract the CALL part and everything after
	callPart := strings.TrimSpace(cypher[callIdx:])

	// Execute MATCH to get bound variables
	// We'll execute a modified MATCH query that returns all bound variables
	matchPattern := strings.TrimSpace(matchPart[matchIdx+5:]) // Skip "MATCH"

	// Parse WHERE clause if present
	whereIdx := findKeywordIndex(matchPattern, "WHERE")
	var whereClause string
	patternOnly := matchPattern
	if whereIdx > 0 {
		patternOnly = strings.TrimSpace(matchPattern[:whereIdx])
		whereClause = strings.TrimSpace(matchPattern[whereIdx+5:])
	}

	// Parse node pattern to get variable name
	nodePattern := e.parseNodePattern(ctx, patternOnly)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern: %s", patternOnly)
	}

	hasOuterPipelineClauses := findKeywordIndex(matchPart, "WITH") >= 0 ||
		findKeywordIndex(matchPart, "RETURN") >= 0 ||
		findKeywordIndex(matchPart, "ORDER BY") >= 0 ||
		findKeywordIndex(matchPart, "SKIP") >= 0 ||
		findKeywordIndex(matchPart, "LIMIT") >= 0

	var (
		nodes []*storage.Node
		err   error
	)
	if !hasOuterPipelineClauses {
		// Preserve id/property/index seed fast paths for simple MATCH ... CALL procedure forms.
		nodes, err = e.seedNodesFromOuterMatch(ctx, matchPart, nodePattern.variable)
		if err != nil {
			return nil, fmt.Errorf("failed to evaluate outer MATCH bindings: %w", err)
		}
	} else {
		// For pre-CALL WITH/ORDER/LIMIT query shapes, keep manual node correlation.
		// seedNodesFromOuterMatch appends RETURN <seedVar> and is not equivalent for
		// WITH-aliased pre-call pipelines.
		nodes, err = e.loadNodesWithTemporalViewport(ctx, nodePattern.labels)
		if err != nil {
			return nil, fmt.Errorf("failed to get nodes: %w", err)
		}
		if len(nodePattern.properties) > 0 {
			nodes = e.filterNodesByProperties(nodes, nodePattern.properties)
		}
		if whereClause != "" {
			var filtered []*storage.Node
			for _, node := range nodes {
				if e.evaluateWhere(ctx, node, nodePattern.variable, whereClause) {
					filtered = append(filtered, node)
				}
			}
			nodes = filtered
		}
	}

	// If no nodes match, return empty result
	if len(nodes) == 0 {
		// Determine result columns from YIELD clause or procedure type
		var columns []string
		yield := parseYieldClause(callPart)
		if yield != nil && len(yield.items) > 0 {
			// Extract column names from yield items (use alias if present, otherwise name)
			columns = make([]string, len(yield.items))
			for i, item := range yield.items {
				if item.alias != "" {
					columns[i] = item.alias
				} else {
					columns[i] = item.name
				}
			}
		} else {
			// Default columns for vector queries
			if strings.Contains(strings.ToUpper(callPart), "QUERYNODES") {
				columns = []string{"node", "score"}
			} else if strings.Contains(strings.ToUpper(callPart), "QUERYRELATIONSHIPS") {
				columns = []string{"relationship", "score"}
			} else {
				columns = []string{} // Empty if unknown
			}
		}
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{},
		}, nil
	}

	// For each matching node, evaluate the CALL with bound variables
	var allResults []*ExecuteResult
	for _, node := range nodes {
		// Create variable context for this node
		nodeContext := map[string]*storage.Node{
			nodePattern.variable: node,
		}

		// Evaluate variable references in the CALL statement
		// Replace patterns like "n.embedding" with actual values
		evaluatedCall := e.substituteBoundVariablesInCall(callPart, nodeContext)

		// Execute the CALL with evaluated values
		result, err := e.executeCall(ctx, evaluatedCall)
		if err != nil {
			return nil, fmt.Errorf("failed to execute CALL for node %s: %w", node.ID, err)
		}
		if result != nil {
			allResults = append(allResults, result)
		}
	}

	// Combine results from all nodes
	if len(allResults) == 0 {
		// Determine result columns from YIELD clause or procedure type
		var columns []string
		yield := parseYieldClause(callPart)
		if yield != nil && len(yield.items) > 0 {
			// Extract column names from yield items (use alias if present, otherwise name)
			columns = make([]string, len(yield.items))
			for i, item := range yield.items {
				if item.alias != "" {
					columns[i] = item.alias
				} else {
					columns[i] = item.name
				}
			}
		} else {
			if strings.Contains(strings.ToUpper(callPart), "QUERYNODES") {
				columns = []string{"node", "score"}
			} else if strings.Contains(strings.ToUpper(callPart), "QUERYRELATIONSHIPS") {
				columns = []string{"relationship", "score"}
			} else {
				columns = []string{} // Empty if unknown
			}
		}
		return &ExecuteResult{
			Columns: columns,
			Rows:    [][]interface{}{},
		}, nil
	}

	// Merge all results
	combined := allResults[0]
	for i := 1; i < len(allResults); i++ {
		combined.Rows = append(combined.Rows, allResults[i].Rows...)
	}

	return combined, nil
}

// substituteBoundVariablesInCall replaces variable references in CALL statements with actual values
// Example: "CALL db.index.vector.queryNodes('idx', 10, n.embedding)" -> "CALL db.index.vector.queryNodes('idx', 10, [0.1, 0.2, ...])"
// This handles variable.property patterns like n.embedding, n.id, etc.
func (e *StorageExecutor) substituteBoundVariablesInCall(callPart string, nodeContext map[string]*storage.Node) string {
	result := callPart

	// Find all variable.property patterns in the CALL
	// Pattern: varName.propertyName (but not inside strings)
	// We need to be careful not to match patterns inside quoted strings
	varPattern := regexp.MustCompile(`(\w+)\.(\w+)`)
	matches := varPattern.FindAllStringSubmatchIndex(callPart, -1)

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
		if node, exists := nodeContext[varName]; exists {
			// Evaluate the property access
			var value interface{}
			{
				// Regular property access — no special-casing for any property name.
				// Users can store embeddings in any property and create a vector index for it.
				if val, ok := node.Properties[propName]; ok {
					value = val
				}
			}

			// Replace the variable.property with the actual value
			if value != nil {
				var replacement string
				switch v := value.(type) {
				case []float32:
					// Format as vector array
					parts := make([]string, len(v))
					for i, f := range v {
						parts[i] = fmt.Sprintf("%g", f)
					}
					replacement = "[" + strings.Join(parts, ", ") + "]"
				case []float64:
					// Format as vector array
					parts := make([]string, len(v))
					for i, f := range v {
						parts[i] = fmt.Sprintf("%g", f)
					}
					replacement = "[" + strings.Join(parts, ", ") + "]"
				case string:
					replacement = fmt.Sprintf("'%s'", v)
				case int, int64:
					replacement = fmt.Sprintf("%d", v)
				case float32, float64:
					replacement = fmt.Sprintf("%g", v)
				case []interface{}:
					// Format as array (common for embedding vectors from Cypher parameters)
					parts := make([]string, len(v))
					for i, item := range v {
						switch iv := item.(type) {
						case float64:
							parts[i] = fmt.Sprintf("%g", iv)
						case float32:
							parts[i] = fmt.Sprintf("%g", iv)
						case int:
							parts[i] = fmt.Sprintf("%d", iv)
						case int64:
							parts[i] = fmt.Sprintf("%d", iv)
						default:
							parts[i] = fmt.Sprintf("%v", iv)
						}
					}
					replacement = "[" + strings.Join(parts, ", ") + "]"
				case bool:
					if v {
						replacement = "true"
					} else {
						replacement = "false"
					}
				default:
					// For complex types, try to convert to string representation
					replacement = fmt.Sprintf("%v", v)
				}
				// Replace from end to start to maintain indices
				result = result[:startIdx] + replacement + result[endIdx:]
			}
		}
	}

	return result
}

// executeMatchWithCallSubquery handles MATCH ... WHERE ... CALL { WITH var ... } ... RETURN queries
// This is a correlated subquery where the CALL {} references variables from the outer MATCH
func (e *StorageExecutor) executeMatchWithCallSubquery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Find CALL position
	callIdx := findKeywordIndex(cypher, "CALL")
	if callIdx == -1 {
		return nil, fmt.Errorf("CALL not found in query")
	}

	// Extract the outer MATCH + WHERE part (before CALL)
	outerPart := strings.TrimSpace(cypher[:callIdx])

	// Parse the outer MATCH to get seed variable name.
	// Then execute the outer query through the normal executor path so index seeks,
	// ORDER/LIMIT planning, and predicate optimizations are preserved.
	matchIdx := findKeywordIndex(outerPart, "MATCH")
	if matchIdx == -1 {
		return nil, fmt.Errorf("MATCH not found before CALL")
	}

	// Extract the pattern segment (up to optional WHERE/WITH/RETURN/ORDER/SKIP/LIMIT)
	// so we can parse the correlated variable name from the node pattern while still
	// executing the full outer segment (including WITH/LIMIT) for seed extraction.
	matchPart := strings.TrimSpace(outerPart[matchIdx+5:]) // Skip "MATCH"
	nodePatternStr := matchPart
	patternEnd := len(matchPart)
	for _, kw := range []string{"WHERE", "WITH", "RETURN", "ORDER BY", "SKIP", "LIMIT"} {
		if idx := findKeywordIndex(matchPart, kw); idx >= 0 && idx < patternEnd {
			patternEnd = idx
		}
	}
	nodePatternStr = strings.TrimSpace(matchPart[:patternEnd])

	// Parse node pattern to get variable name
	nodePattern := e.parseNodePattern(ctx, nodePatternStr)
	if nodePattern.variable == "" {
		return nil, fmt.Errorf("could not parse node pattern: %s", nodePatternStr)
	}

	seedNodes, err := e.seedNodesFromOuterMatch(ctx, outerPart, nodePattern.variable)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate outer MATCH seeds: %w", err)
	}

	// Parse the CALL {} subquery and what comes after
	callPart := strings.TrimSpace(cypher[callIdx:])
	subqueryBody, afterCall, _, _ := e.parseCallSubquery(callPart)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body")
	}

	if len(seedNodes) == 0 {
		// No seed nodes matched. Preserve projected column semantics from trailing
		// clauses (e.g. RETURN after CALL {}) instead of returning internal defaults.
		empty := &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
		if strings.TrimSpace(afterCall) != "" {
			return e.processAfterCallSubquery(ctx, empty, afterCall)
		}
		return empty, nil
	}

	// Handle USE clause inside CALL subquery body — resolve the target database
	// and switch the executor before processing WITH/MATCH.
	subqueryExecutor := e
	if useDB, useRemaining, hasUse, useErr := parseLeadingUseClause(subqueryBody); hasUse || useErr != nil {
		if useErr != nil {
			return nil, fmt.Errorf("CALL subquery USE clause error: %w", useErr)
		}
		scopedExec, resolvedDB, scopeErr := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if scopeErr != nil {
			return nil, fmt.Errorf("CALL subquery USE %s failed: %w", useDB, scopeErr)
		}
		subqueryExecutor = scopedExec
		subqueryBody = useRemaining
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
	}
	// Check if subquery starts with "WITH <variable>" - this imports outer context
	upperBody := strings.ToUpper(strings.TrimSpace(subqueryBody))
	if !strings.HasPrefix(upperBody, "WITH ") {
		// No WITH clause - execute as standalone subquery for each seed
		return subqueryExecutor.executeCallSubquery(ctx, "CALL { "+subqueryBody+" }")
	}

	// Find where the WITH imports end (at the next query clause).
	// This must include write clauses (CREATE/MERGE/SET/DELETE/REMOVE) for
	// unit subqueries like:
	//   CALL { WITH p CREATE ... }
	// otherwise restOfSubquery becomes empty and side effects are skipped.
	withEndIdx := len(subqueryBody)
	clauseStarts := []int{
		findKeywordIndex(subqueryBody, "WHERE"),
		findMultiWordKeywordIndex(subqueryBody, "OPTIONAL", "MATCH"),
		findKeywordIndex(subqueryBody, "MATCH"),
		findKeywordIndex(subqueryBody, "UNWIND"),
		findKeywordIndex(subqueryBody, "MERGE"),
		findKeywordIndex(subqueryBody, "CREATE"),
		findKeywordIndex(subqueryBody, "SET"),
		findKeywordIndex(subqueryBody, "DELETE"),
		findKeywordIndex(subqueryBody, "REMOVE"),
		findKeywordIndex(subqueryBody, "CALL"),
		findKeywordIndex(subqueryBody, "RETURN"),
		findKeywordIndex(subqueryBody, "WITH"),
	}
	for _, idx := range clauseStarts {
		if idx > 0 && idx < withEndIdx {
			withEndIdx = idx
		}
	}

	// Parse UNION branches once (if present) so correlated execution only binds rows.
	// This removes repeated branch parsing overhead per seed row.
	hasUnion := findKeywordIndex(subqueryBody, "UNION ALL") >= 0 || findKeywordIndex(subqueryBody, "UNION") >= 0
	type parsedUnionBranch struct {
		withVars      []string
		innerBody     string
		hasWith       bool
		isPureReturn  bool // precomputed: innerBody is just RETURN (no MATCH/CALL/etc.)
		dependsOnSeed bool // precomputed: branch body references WITH-imported vars
		isWrite       bool // precomputed: branch contains write keywords
	}
	var (
		unionModeAll bool
		unionParsed  []parsedUnionBranch
	)
	if hasUnion {
		branches, modeAll, splitOK := splitTopLevelUnionBranches(subqueryBody)
		if !splitOK {
			return nil, fmt.Errorf("failed to parse UNION branches in correlated CALL subquery")
		}
		unionModeAll = modeAll
		unionParsed = make([]parsedUnionBranch, 0, len(branches))
		for i, branch := range branches {
			trimmedBranch := strings.TrimSpace(branch)
			if trimmedBranch == "" {
				continue
			}
			withVars, innerBody, hasWith, parseErr := parseLeadingWithImports(trimmedBranch)
			if parseErr != nil {
				return nil, fmt.Errorf("failed to parse UNION branch %d WITH imports: %w", i+1, parseErr)
			}
			pb := parsedUnionBranch{
				withVars:  withVars,
				innerBody: innerBody,
				hasWith:   hasWith,
			}
			// Precompute per-branch metadata once to avoid repeated keyword scans per seed row.
			if hasWith {
				pb.isWrite = callSubqueryQueryIsWrite(innerBody)
				for _, varName := range withVars {
					if isIdentifierReferenced(innerBody, varName) {
						pb.dependsOnSeed = true
						break
					}
				}
				// Defensive correctness: branches like "WITH o,t WHERE ... SET ... RETURN ..."
				// are always correlated even if identifier detection misses a tokenization edge.
				if !pb.dependsOnSeed && findKeywordIndex(strings.TrimSpace(innerBody), "WHERE") == 0 {
					pb.dependsOnSeed = true
				}
				pb.isPureReturn = isCallSubqueryPureReturn(innerBody)
			}
			unionParsed = append(unionParsed, pb)
		}
	}

	// Precompute static UNION branches (branches whose WITH-imported vars are not
	// referenced in branch body) once and reuse across all seeds.
	type cachedUnionBranch struct {
		result *ExecuteResult
		valid  bool
	}
	staticBranchCache := make([]cachedUnionBranch, len(unionParsed))
	if hasUnion {
		for i, branch := range unionParsed {
			if !branch.hasWith || len(branch.withVars) == 0 {
				continue
			}
			// Use precomputed flags instead of re-scanning keywords.
			if branch.dependsOnSeed || branch.isWrite {
				continue
			}
			branchResult, err := subqueryExecutor.executeInternal(ctx, branch.innerBody, nil)
			if err != nil {
				return nil, fmt.Errorf("failed static UNION subquery branch %d: %w", i+1, err)
			}
			staticBranchCache[i] = cachedUnionBranch{result: branchResult, valid: true}
		}
	}

	// Execute the subquery for each seed node
	var combinedResult *ExecuteResult
	correlatedImportCache := make(map[string]map[string]interface{}, 32)

	// Use a unique parameter name to avoid collision with user-provided parameters
	seedIDParamName := "__internal_seed_id"

	for _, seedNode := range seedNodes {
		seedID := string(seedNode.ID)

		// Transform the inner query to bind the seed variable properly
		// Original: "WITH seed MATCH path = (seed)-[r*1..2]-(connected) RETURN seed, collect(...)"
		// We need to replace the WITH clause with an explicit seed binding
		// SECURITY: Use parameterized query to prevent Cypher injection

		// Extract the rest after WITH clause (starts with MATCH or RETURN)
		restOfSubquery := strings.TrimSpace(subqueryBody[withEndIdx:])

		// Create parameters map with the seed ID (safe from injection)
		subqueryParams := map[string]interface{}{
			seedIDParamName: seedID,
		}

		// UNION subqueries with WITH-imported outer variables are executed branch-by-branch
		// using correlated seed rows to preserve CALL/YIELD tail semantics.
		if hasUnion {
			var perSeed *ExecuteResult
			var seen map[string]bool
			if !unionModeAll {
				seen = make(map[string]bool)
			}
			for i, branch := range unionParsed {
				var branchResult *ExecuteResult
				if staticBranchCache[i].valid {
					branchResult = staticBranchCache[i].result
				} else if branch.hasWith {
					// Fast path for simple correlated projection branch:
					// WITH seedVar RETURN ...
					// Uses precomputed isPureReturn flag to skip redundant keyword scans.
					seedCols := []string{nodePattern.variable}
					seedRow := []interface{}{seedNode}
					if branch.isPureReturn {
						if fastRes, ok, ferr := e.tryExecuteCorrelatedReturnProjectionPreChecked(ctx, seedRow, seedCols, branch.withVars, branch.innerBody, true); ok || ferr != nil {
							if ferr != nil {
								return nil, ferr
							}
							branchResult = fastRes
						}
					} else {
						fastRes, ok, ferr := e.tryExecuteCorrelatedReturnProjection(ctx, seedRow, seedCols, branch.withVars, branch.innerBody)
						if !(ok || ferr != nil) {
							// no-op
						} else {
							if ferr != nil {
								return nil, ferr
							}
							branchResult = fastRes
						}
					}
					if branchResult == nil {
						branchBody := branch.innerBody
						branchParams := map[string]interface{}{}
						bindClauses := make([]string, 0, len(branch.withVars))
						withItems := make([]string, 0, len(branch.withVars))
						resolvedVars := make(map[string]interface{}, len(branch.withVars))
						for _, varName := range branch.withVars {
							if varName == nodePattern.variable {
								pname := "__seed_id_" + varName
								branchParams[pname] = seedID
								resolvedVars[varName] = seedNode
								bindClauses = append(bindClauses, fmt.Sprintf("MATCH (%s) WHERE id(%s) = $%s", varName, varName, pname))
								withItems = append(withItems, varName)
								continue
							}

							// Additional WITH imports (for example OPTIONAL MATCH values)
							// are resolved from the outer query and bound as parameters.
							resolvedValue, _, resolveErr := e.resolveCorrelatedImportValue(ctx, outerPart, nodePattern.variable, seedID, varName, correlatedImportCache)
							if resolveErr != nil {
								return nil, resolveErr
							}
							if resolvedNode, ok := resolvedValue.(*storage.Node); ok && resolvedNode != nil {
								pname := "__seed_id_" + varName
								branchParams[pname] = string(resolvedNode.ID)
								resolvedVars[varName] = resolvedNode
								bindClauses = append(bindClauses, fmt.Sprintf("MATCH (%s) WHERE id(%s) = $%s", varName, varName, pname))
								withItems = append(withItems, varName)
								continue
							}
							pname := "__seed_val_" + varName
							branchParams[pname] = resolvedValue
							resolvedVars[varName] = resolvedValue
							withItems = append(withItems, fmt.Sprintf("$%s AS %s", pname, varName))
						}
						// For correlated write branches with leading WHERE null-guards, avoid
						// MATCH ... WITH ... WHERE ... CREATE/SET parser edge cases by
						// evaluating the null predicate locally and running a direct rewritten query.
						if whereExpr, whereRest, ok := splitLeadingWhereNullGuard(branchBody); ok && callSubqueryQueryIsWrite(whereRest) {
							if pass, handled := evalWhereNullGuard(whereExpr, resolvedVars); handled {
								if !pass {
									branchResult = &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
								} else {
									rewrite := whereRest
									matchPrefix := make([]string, 0, len(resolvedVars))
									for varName, val := range resolvedVars {
										n, ok := val.(*storage.Node)
										if !ok || n == nil {
											continue
										}
										if !isIdentifierReferenced(rewrite, varName) {
											continue
										}
										matchPrefix = append(matchPrefix, fmt.Sprintf("MATCH (%s) WHERE id(%s) = '%s'", varName, varName, n.ID))
									}
									if len(matchPrefix) > 0 {
										rewrite = strings.Join(matchPrefix, " ") + " " + rewrite
									}
									branchResult, err = subqueryExecutor.executeInternal(ctx, rewrite, nil)
								}
							}
						}
						if branchResult != nil {
							// handled by rewritten correlated branch path
						} else {
							if len(bindClauses) > 0 || len(withItems) > 0 {
								prefix := ""
								if len(bindClauses) > 0 {
									prefix = strings.Join(bindClauses, " ") + " "
								}
								if len(withItems) > 0 {
									prefix += "WITH " + strings.Join(withItems, ", ") + " "
								}
								branchBody = prefix + branchBody
							}
							branchResult, err = subqueryExecutor.executeInternal(ctx, branchBody, branchParams)
						}
					}
				} else {
					branchResult, err = subqueryExecutor.executeInternal(ctx, branch.innerBody, nil)
				}
				if err != nil {
					return nil, fmt.Errorf("failed correlated UNION subquery branch %d for seed %s: %w", i+1, seedID, err)
				}
				e.normalizeUnionBranchColumns(branch.innerBody, branchResult)

				if perSeed == nil {
					perSeed = &ExecuteResult{
						Columns: append([]string{}, branchResult.Columns...),
						Rows:    make([][]interface{}, 0),
					}
				} else if len(perSeed.Columns) != len(branchResult.Columns) {
					return nil, fmt.Errorf("UNION queries must return the same number of columns (got %d and %d)", len(perSeed.Columns), len(branchResult.Columns))
				}

				if unionModeAll {
					perSeed.Rows = append(perSeed.Rows, branchResult.Rows...)
					continue
				}
				for _, row := range branchResult.Rows {
					key := callSubqueryRowDedupKey(row)
					if !seen[key] {
						perSeed.Rows = append(perSeed.Rows, row)
						seen[key] = true
					}
				}
			}

			if perSeed == nil {
				perSeed = &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}
			}

			if combinedResult == nil {
				combinedResult = &ExecuteResult{
					Columns: perSeed.Columns,
					Rows:    make([][]interface{}, 0),
				}
			}

			if len(combinedResult.Columns) != len(perSeed.Columns) {
				return nil, fmt.Errorf("UNION queries must return the same number of columns (got %d and %d)", len(combinedResult.Columns), len(perSeed.Columns))
			}
			combinedResult.Rows = append(combinedResult.Rows, perSeed.Rows...)
			continue
		}

		// If the rest starts with MATCH, we need to handle the path pattern
		// Replace "MATCH path = (seed)" with "MATCH path = (seed) WHERE id(seed) = $param"
		if strings.HasPrefix(strings.ToUpper(restOfSubquery), "MATCH") {
			// Find the existing WHERE or RETURN to know where to inject our filter
			matchPart := restOfSubquery[5:] // Skip "MATCH"
			returnIdx := findKeywordIndex(matchPart, "RETURN")

			if returnIdx > 0 {
				patternPart := strings.TrimSpace(matchPart[:returnIdx])
				returnPart := matchPart[returnIdx:]

				// Check if there's already a WHERE clause in patternPart
				whereIdx := findKeywordNotInBrackets(strings.ToUpper(patternPart), " WHERE ")
				var substitutedBody string
				seedFilter := "id(" + nodePattern.variable + ") = $" + seedIDParamName

				if whereIdx > 0 {
					// There's already a WHERE clause - append with AND
					beforeWhere := patternPart[:whereIdx]
					afterWhere := patternPart[whereIdx+7:] // Skip " WHERE "
					substitutedBody = "MATCH " + beforeWhere + " WHERE " + seedFilter + " AND " + afterWhere + " " + returnPart
				} else {
					// No existing WHERE - add one
					substitutedBody = "MATCH " + patternPart + " WHERE " + seedFilter + " " + returnPart
				}

				// Execute the substituted subquery with parameters
				innerResult, err := subqueryExecutor.executeInternal(ctx, substitutedBody, subqueryParams)
				if err != nil {
					// Log but continue with other seeds
					continue
				}

				if combinedResult == nil {
					combinedResult = &ExecuteResult{
						Columns: innerResult.Columns,
						Rows:    make([][]interface{}, 0),
					}
				}

				// Add rows from this seed's result
				combinedResult.Rows = append(combinedResult.Rows, innerResult.Rows...)
				continue
			}
		}

		// Fallback correlated execution (parameterized - safe from injection).
		// For write tails (notably MERGE), avoid inserting an extra WITH between
		// MATCH and write clauses: current compound MATCH/MERGE handling expects
		// `MATCH ... MERGE ...`, while `MATCH ... WITH ... MERGE ...` can skip writes.
		substitutedBody := ""
		upperRest := strings.ToUpper(strings.TrimSpace(restOfSubquery))
		if strings.HasPrefix(upperRest, "MERGE ") ||
			strings.HasPrefix(upperRest, "CREATE ") ||
			strings.HasPrefix(upperRest, "SET ") ||
			strings.HasPrefix(upperRest, "DELETE ") ||
			strings.HasPrefix(upperRest, "REMOVE ") {
			substitutedBody = "MATCH (" + nodePattern.variable + ") WHERE id(" + nodePattern.variable + ") = $" + seedIDParamName + " " + restOfSubquery
		} else {
			substitutedBody = "MATCH (" + nodePattern.variable + ") WHERE id(" + nodePattern.variable + ") = $" + seedIDParamName + " WITH " + nodePattern.variable + " " + restOfSubquery
		}

		// Execute the substituted subquery with parameters
		innerResult, err := subqueryExecutor.executeInternal(ctx, substitutedBody, subqueryParams)
		if err != nil {
			// Log but continue with other seeds
			continue
		}

		if combinedResult == nil {
			combinedResult = &ExecuteResult{
				Columns: innerResult.Columns,
				Rows:    make([][]interface{}, 0),
			}
		}

		// Add rows from this seed's result, injecting the seed node for variables that reference it
		for _, row := range innerResult.Rows {
			newRow := make([]interface{}, len(row))
			copy(newRow, row)

			// Find columns that match the seed variable and inject the seed node
			// The variable name from outer MATCH should match a column in the inner RETURN
			seedVarLower := strings.ToLower(nodePattern.variable)
			for colIdx, colName := range innerResult.Columns {
				colNameLower := strings.ToLower(strings.TrimSpace(colName))
				if colNameLower == seedVarLower && newRow[colIdx] == nil {
					// Inject the seed node as a map representation
					newRow[colIdx] = seedNode
				}
			}
			combinedResult.Rows = append(combinedResult.Rows, newRow)
		}

		// If inner query returned 0 rows but we have a seed, create a row with just the seed
		if len(innerResult.Rows) == 0 {
			// Create a row with the seed node and empty neighbors
			emptyRow := make([]interface{}, len(innerResult.Columns))
			for colIdx, colName := range innerResult.Columns {
				if strings.EqualFold(colName, nodePattern.variable) {
					emptyRow[colIdx] = seedNode
				}
			}
			combinedResult.Rows = append(combinedResult.Rows, emptyRow)
		}
	}

	if combinedResult == nil {
		combinedResult = &ExecuteResult{
			Columns: []string{},
			Rows:    [][]interface{}{},
		}
	}

	// If there's something after CALL { }, process it (e.g., RETURN)
	if afterCall != "" {
		res, err := e.processAfterCallSubquery(ctx, combinedResult, afterCall)
		return res, err
	}

	return combinedResult, nil
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
		return nil, false, fmt.Errorf("failed to resolve correlated import %s: %w", importVar, err)
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
	if len(outerResult.Rows) == 1 && importCol >= 0 && importCol < len(outerResult.Rows[0]) {
		val := outerResult.Rows[0][importCol]
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
			if strings.HasPrefix(strings.ToUpper(optionalPart), "OPTIONAL MATCH") {
				requiredPart := optionalPart
				if len(requiredPart) >= len("OPTIONAL MATCH") {
					requiredPart = "MATCH" + requiredPart[len("OPTIONAL MATCH"):]
				}
				seedScoped := fmt.Sprintf("MATCH (%s) WHERE id(%s) = '%s' %s RETURN %s", seedVar, seedVar, seedID, requiredPart, importVar)
				fallbackRes, fallbackErr := e.executeInternal(ctx, seedScoped, nil)
				if fallbackErr != nil {
					return nil, false, fmt.Errorf("failed optional import fallback for %s: %w", importVar, fallbackErr)
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
			return nil, true, fmt.Errorf("CALL subquery WITH imports unknown variable: %s", v)
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

func callSubqueryRowDedupKey(row []interface{}) string {
	if len(row) == 0 {
		return "<empty>"
	}
	var b strings.Builder
	b.Grow(len(row) * 16)
	for i, v := range row {
		if i > 0 {
			b.WriteByte('|')
		}
		switch x := v.(type) {
		case nil:
			b.WriteString("n:")
		case string:
			b.WriteString("s:")
			b.WriteString(x)
		case []byte:
			b.WriteString("b:")
			b.Write(x)
		case int:
			b.WriteString("i:")
			b.WriteString(strconv.Itoa(x))
		case int64:
			b.WriteString("i64:")
			b.WriteString(strconv.FormatInt(x, 10))
		case float64:
			b.WriteString("f:")
			b.WriteString(strconv.FormatFloat(x, 'g', -1, 64))
		case float32:
			b.WriteString("f32:")
			b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
		case bool:
			if x {
				b.WriteString("t:1")
			} else {
				b.WriteString("t:0")
			}
		default:
			b.WriteString(fmt.Sprintf("%T:%v", v, v))
		}
	}
	return b.String()
}

// seedNodesFromOuterMatch executes the outer MATCH/WHERE segment through the normal
// execution pipeline (instead of manual scan/filter) so index/hot-path optimizations
// apply before correlated CALL {} expansion.
func (e *StorageExecutor) seedNodesFromOuterMatch(ctx context.Context, outerPart, variable string) ([]*storage.Node, error) {
	variable = strings.TrimSpace(variable)
	if variable == "" {
		return nil, fmt.Errorf("empty correlated variable")
	}

	trimmedOuter := strings.TrimSpace(outerPart)
	hasOuterPipelineClauses := findKeywordIndex(trimmedOuter, "WITH") >= 0 ||
		findKeywordIndex(trimmedOuter, "RETURN") >= 0 ||
		findKeywordIndex(trimmedOuter, "ORDER BY") >= 0 ||
		findKeywordIndex(trimmedOuter, "SKIP") >= 0 ||
		findKeywordIndex(trimmedOuter, "LIMIT") >= 0

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
			var nodes []*storage.Node
			var err error
			if len(np.labels) > 0 {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, np.labels)
				if err != nil {
					return nil, err
				}
			} else {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, nil)
				if err != nil {
					return nil, err
				}
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

	// Fast path for simple node-seed queries:
	//   MATCH (v:Label {k: val})
	// Avoids full executeInternal round-trip for seed extraction in correlated CALL.
	if findKeywordIndex(trimmedOuter, "MATCH") == 0 &&
		!hasOuterPipelineClauses &&
		findKeywordIndex(trimmedOuter, "WHERE") == -1 &&
		!strings.Contains(trimmedOuter, "-[") &&
		!strings.Contains(trimmedOuter, "--") {
		matchBody := strings.TrimSpace(trimmedOuter[len("MATCH"):])
		np := e.parseNodePattern(ctx, matchBody)
		if strings.EqualFold(strings.TrimSpace(np.variable), variable) {
			var nodes []*storage.Node
			var err error
			if len(np.labels) > 0 {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, np.labels)
				if err != nil {
					return nil, err
				}
			} else {
				nodes, err = e.loadNodesWithTemporalViewport(ctx, nil)
				if err != nil {
					return nil, err
				}
			}
			if len(np.properties) == 0 {
				return nodes, nil
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
			return filtered, nil
		}
	}

	seedQuery := strings.TrimSpace(outerPart) + " RETURN " + variable
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
		return nil, fmt.Errorf("outer MATCH did not project correlated variable %q", variable)
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
	// Substitute parameters
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	// Extract the subquery body from CALL { ... }
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(cypher)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body (expected CALL { <query> })")
	}

	// Check if the subquery body starts with USE — this indicates a fabric
	// cross-database subquery (e.g. CALL { USE nornic.tr MATCH ... }).
	// Resolve the target database and execute against that engine.
	subqueryExecutor := e
	if useDB, useRemaining, hasUse, useErr := parseLeadingUseClause(subqueryBody); hasUse || useErr != nil {
		if useErr != nil {
			return nil, fmt.Errorf("CALL subquery USE clause error: %w", useErr)
		}
		scopedExec, resolvedDB, scopeErr := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if scopeErr != nil {
			return nil, fmt.Errorf("CALL subquery USE %s failed: %w", useDB, scopeErr)
		}
		subqueryExecutor = scopedExec
		subqueryBody = useRemaining
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
	}

	// Execute the inner subquery
	var innerResult *ExecuteResult
	var err error

	if inTransactions {
		// Execute in batches (for large data operations)
		innerResult, err = subqueryExecutor.executeCallInTransactions(ctx, subqueryBody, batchSize)
	} else {
		// Check if subquery contains UNION - route to executeUnion if so
		// This must be checked before calling Execute, as Execute routes based on first keyword
		if findKeywordIndex(subqueryBody, "UNION ALL") >= 0 {
			innerResult, err = subqueryExecutor.executeUnion(ctx, subqueryBody, true)
		} else if findKeywordIndex(subqueryBody, "UNION") >= 0 {
			innerResult, err = subqueryExecutor.executeUnion(ctx, subqueryBody, false)
		} else {
			// Execute as single query
			innerResult, err = subqueryExecutor.executeInternal(ctx, subqueryBody, nil)
		}
	}

	if err != nil {
		return nil, fmt.Errorf("CALL subquery error: %w", err)
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

	// Get what's after the closing brace
	afterCall = strings.TrimSpace(trimmed[braceEnd+1:])

	// Check for IN TRANSACTIONS
	upperAfter := strings.ToUpper(afterCall)
	if strings.HasPrefix(upperAfter, "IN TRANSACTIONS") {
		inTransactions = true
		afterTx := strings.TrimSpace(afterCall[15:])
		upperAfterTx := strings.ToUpper(afterTx)

		// Check for OF n ROWS
		if strings.HasPrefix(upperAfterTx, "OF ") {
			// Parse batch size
			ofPart := afterTx[3:]
			// Find ROWS keyword
			rowsIdx := strings.Index(strings.ToUpper(ofPart), " ROWS")
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
func (e *StorageExecutor) executeCallInTransactions(ctx context.Context, subquery string, batchSize int) (*ExecuteResult, error) {
	if batchSize <= 0 {
		batchSize = 1000 // Default batch size
	}

	// Check if the subquery contains write operations (CREATE, SET, DELETE, MERGE)
	upperSubquery := strings.ToUpper(subquery)
	hasWrites := strings.Contains(upperSubquery, "CREATE") ||
		strings.Contains(upperSubquery, "SET") ||
		strings.Contains(upperSubquery, "DELETE") ||
		strings.Contains(upperSubquery, "MERGE")

	if !hasWrites {
		// No write operations - execute once and return (no need for batching)
		result, err := e.executeInternal(ctx, subquery, nil)
		if err != nil {
			return nil, fmt.Errorf("subquery execution failed: %w", err)
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
		// Execute read-only version to get row count (doesn't perform writes)
		readOnlyResult, err := e.executeInternal(ctx, readOnlyQuery, nil)
		if err == nil && readOnlyResult != nil {
			totalRows = len(readOnlyResult.Rows)
			resultColumns = readOnlyResult.Columns
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
			singleResult, err := e.executeWithImplicitTransaction(ctx, subquery, strings.ToUpper(subquery))
			if err != nil {
				return nil, fmt.Errorf("batch 1 failed: %w", err)
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
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, strings.ToUpper(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, fmt.Errorf("batch %d failed: %w", batchNum+1, err)
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
				combinedResult.Stats.NodesCreated += batchResult.Stats.NodesCreated
				combinedResult.Stats.NodesDeleted += batchResult.Stats.NodesDeleted
				combinedResult.Stats.RelationshipsCreated += batchResult.Stats.RelationshipsCreated
				combinedResult.Stats.RelationshipsDeleted += batchResult.Stats.RelationshipsDeleted
				combinedResult.Stats.PropertiesSet += batchResult.Stats.PropertiesSet
				combinedResult.Stats.LabelsAdded += batchResult.Stats.LabelsAdded
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
			batchResult, err := e.executeWithImplicitTransaction(ctx, modifiedSubquery, strings.ToUpper(modifiedSubquery))
			if err != nil {
				// On error, stop processing and return error
				return nil, fmt.Errorf("batch %d/%d failed: %w", batchNum+1, numBatches, err)
			}

			// Set columns from first batch if not set
			if len(combinedResult.Columns) == 0 && batchResult != nil && len(batchResult.Columns) > 0 {
				combinedResult.Columns = batchResult.Columns
			}

			// Accumulate results
			if batchResult != nil {
				combinedResult.Rows = append(combinedResult.Rows, batchResult.Rows...)
				if batchResult.Stats != nil {
					combinedResult.Stats.NodesCreated += batchResult.Stats.NodesCreated
					combinedResult.Stats.NodesDeleted += batchResult.Stats.NodesDeleted
					combinedResult.Stats.RelationshipsCreated += batchResult.Stats.RelationshipsCreated
					combinedResult.Stats.RelationshipsDeleted += batchResult.Stats.RelationshipsDeleted
					combinedResult.Stats.PropertiesSet += batchResult.Stats.PropertiesSet
					combinedResult.Stats.LabelsAdded += batchResult.Stats.LabelsAdded
				}
			}
		}
	}

	return combinedResult, nil
}

// makeSubqueryReadOnly converts a subquery with writes to a read-only version for row counting.
// This is used to determine how many batches we need before executing the actual writes.
// Returns empty string if conversion is not possible.
func (e *StorageExecutor) makeSubqueryReadOnly(subquery string) string {
	// Simple strategy: Replace write operations with RETURN of matched entities
	// This works for common patterns like "MATCH ... SET ... RETURN"

	// Check for MATCH ... SET ... RETURN pattern
	matchIdx := findKeywordIndex(subquery, "MATCH")
	setIdx := findKeywordIndex(subquery, "SET")
	returnIdx := findKeywordIndex(subquery, "RETURN")

	if matchIdx >= 0 && setIdx > matchIdx && returnIdx > setIdx {
		// Extract MATCH and RETURN parts, skip SET
		matchPart := strings.TrimSpace(subquery[matchIdx:setIdx])
		returnPart := strings.TrimSpace(subquery[returnIdx:])
		return matchPart + " " + returnPart
	}

	// Check for MATCH ... CREATE ... RETURN pattern
	createIdx := findKeywordIndex(subquery, "CREATE")
	if matchIdx >= 0 && createIdx > matchIdx && returnIdx > createIdx {
		// Extract MATCH and RETURN parts, skip CREATE
		matchPart := strings.TrimSpace(subquery[matchIdx:createIdx])
		returnPart := strings.TrimSpace(subquery[returnIdx:])
		return matchPart + " " + returnPart
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
	if strings.Contains(strings.ToUpper(returnPart), "LIMIT") || strings.Contains(strings.ToUpper(returnPart), "SKIP") {
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
	upperAfter := strings.ToUpper(afterCall)

	// Handle chained CALL { } subqueries.
	if strings.HasPrefix(upperAfter, "CALL") && isCallSubquery(afterCall) {
		return e.executeChainedCallSubquery(ctx, innerResult, afterCall)
	}

	// Handle RETURN clause
	if strings.HasPrefix(upperAfter, "RETURN ") {
		return e.processCallSubqueryReturn(ctx, innerResult, afterCall)
	}

	// Handle ORDER BY (without RETURN means use inner result's columns)
	if strings.HasPrefix(upperAfter, "ORDER BY ") {
		result := e.applyOrderByToResult(innerResult, afterCall)
		// Check for LIMIT/SKIP after ORDER BY
		return e.applyResultModifiers(result, afterCall)
	}

	// Unsupported clause after CALL {}
	firstWord := strings.Split(upperAfter, " ")[0]
	return nil, fmt.Errorf("unsupported clause after CALL {}: %s (supported: RETURN, ORDER BY, SKIP, LIMIT)", firstWord)
}

func (e *StorageExecutor) executeChainedCallSubquery(ctx context.Context, seedResult *ExecuteResult, callClause string) (*ExecuteResult, error) {
	subqueryBody, afterCall, inTransactions, batchSize := e.parseCallSubquery(callClause)
	if subqueryBody == "" {
		return nil, fmt.Errorf("invalid CALL {} subquery: empty body (expected CALL { <query> })")
	}

	if inTransactions {
		return nil, fmt.Errorf("CALL {} IN TRANSACTIONS is not supported in chained CALL subqueries (batchSize=%d)", batchSize)
	}

	useDB, bodyWithoutUse, hasUse, err := parseLeadingUseClause(subqueryBody)
	if err != nil {
		return nil, err
	}

	targetExec := e
	if hasUse {
		scopedExec, resolvedDB, err := e.scopedExecutorForUse(useDB, GetAuthTokenFromContext(ctx))
		if err != nil {
			return nil, err
		}
		targetExec = scopedExec
		ctx = context.WithValue(ctx, ctxKeyUseDatabase, resolvedDB)
		subqueryBody = bodyWithoutUse
	}

	withVars, innerBody, hasWith, err := parseLeadingWithImports(subqueryBody)
	if err != nil {
		return nil, err
	}

	combined := &ExecuteResult{Columns: []string{}, Rows: make([][]interface{}, 0)}
	if hasWith {
		combined, err = targetExec.executeCorrelatedCallWithSeedRows(ctx, seedResult, innerBody, withVars)
		if err != nil {
			return nil, err
		}
	} else {
		innerResult, err := targetExec.executeInternal(ctx, subqueryBody, nil)
		if err != nil {
			return nil, fmt.Errorf("CALL subquery error: %w", err)
		}
		combined = crossJoinCallResults(seedResult, innerResult)
	}

	if afterCall != "" {
		return e.processAfterCallSubquery(ctx, combined, afterCall)
	}

	return combined, nil
}

func parseLeadingWithImports(subqueryBody string) (withVars []string, innerBody string, hasWith bool, err error) {
	trimmed := strings.TrimSpace(subqueryBody)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "WITH ") {
		return nil, trimmed, false, nil
	}

	afterWith := strings.TrimSpace(trimmed[len("WITH "):])
	nextIdx := len(afterWith)
	clauseStarts := []int{
		findKeywordIndex(afterWith, "WHERE"),
		findMultiWordKeywordIndex(afterWith, "OPTIONAL", "MATCH"),
		findKeywordIndex(afterWith, "MATCH"),
		findKeywordIndex(afterWith, "UNWIND"),
		findKeywordIndex(afterWith, "MERGE"),
		findKeywordIndex(afterWith, "CREATE"),
		findKeywordIndex(afterWith, "CALL"),
		findKeywordIndex(afterWith, "RETURN"),
		findKeywordIndex(afterWith, "WITH"),
	}
	for _, idx := range clauseStarts {
		if idx > 0 && idx < nextIdx {
			nextIdx = idx
		}
	}

	if nextIdx == len(afterWith) {
		return nil, "", true, fmt.Errorf("invalid CALL {} subquery: WITH must be followed by a query clause")
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
		return nil, "", true, fmt.Errorf("invalid CALL {} subquery: empty query body after WITH")
	}

	parts := splitReturnExpressions(withExpr)
	withVars = make([]string, 0, len(parts))
	for _, part := range parts {
		expr := strings.TrimSpace(part)
		if expr == "" {
			continue
		}

		upperExpr := strings.ToUpper(expr)
		if asIdx := strings.Index(upperExpr, " AS "); asIdx >= 0 {
			alias := strings.TrimSpace(expr[asIdx+4:])
			if alias == "" {
				return nil, "", true, fmt.Errorf("invalid WITH import expression: %q", expr)
			}
			withVars = append(withVars, alias)
			continue
		}

		withVars = append(withVars, expr)
	}

	if len(withVars) == 0 {
		return nil, "", true, fmt.Errorf("invalid CALL {} subquery: WITH clause does not import variables")
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
	if strings.HasPrefix(strings.ToUpper(trimmed), "WITH ") {
		afterWith := strings.TrimSpace(trimmed[len("WITH "):])
		whereIdx := findKeywordIndex(afterWith, "WHERE")
		if whereIdx <= 0 {
			return "", "", false
		}
		afterWhere = strings.TrimSpace(afterWith[whereIdx+len("WHERE"):])
	} else if strings.HasPrefix(strings.ToUpper(trimmed), "WHERE ") {
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
	re := regexp.MustCompile(`(?i)^([A-Za-z_][A-Za-z0-9_]*)\s+IS\s+(NOT\s+)?NULL$`)
	m := re.FindStringSubmatch(expr)
	if len(m) != 3 {
		return false, false
	}

	varName := m[1]
	_, exists := vars[varName]
	isNull := !exists || vars[varName] == nil
	isNot := strings.TrimSpace(strings.ToUpper(m[2])) == "NOT"
	if isNot {
		return !isNull, true
	}
	return isNull, true
}

// splitTopLevelUnionBranches splits a query by top-level UNION/UNION ALL separators.
// Returns branches, unionAllMode, ok.
func splitTopLevelUnionBranches(query string) ([]string, bool, bool) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return nil, false, false
	}

	type sep struct {
		pos int
		end int
		all bool
	}
	seps := make([]sep, 0)
	inSingle := false
	inDouble := false
	depthParen := 0
	depthBracket := 0
	depthBrace := 0

	for i := 0; i < len(trimmed); i++ {
		ch := trimmed[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			continue
		}
		if inSingle || inDouble {
			continue
		}
		switch ch {
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
		if !matchKeywordAt(trimmed, i, "UNION") {
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
		return nil, false, false
	}

	unionAllMode := true
	for _, s := range seps {
		if !s.all {
			unionAllMode = false
			break
		}
	}

	branches := make([]string, 0, len(seps)+1)
	start := 0
	for _, s := range seps {
		part := strings.TrimSpace(trimmed[start:s.pos])
		if part == "" {
			return nil, false, false
		}
		branches = append(branches, part)
		start = s.end
	}
	last := strings.TrimSpace(trimmed[start:])
	if last == "" {
		return nil, false, false
	}
	branches = append(branches, last)
	return branches, unionAllMode, true
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

	for _, seedRow := range seedResult.Rows {
		params := make(map[string]interface{}, len(importVars))
		correlatedBody := innerBody
		nodeBindClauses := make([]string, 0, len(importVars))
		nodeBindVars := make([]string, 0, len(importVars))
		for _, varName := range importVars {
			idx, ok := colMap[varName]
			if !ok {
				return nil, fmt.Errorf("CALL subquery WITH imports unknown variable: %s", varName)
			}
			if idx < 0 || idx >= len(seedRow) {
				return nil, fmt.Errorf("CALL subquery seed row missing variable: %s", varName)
			}
			seedVal := seedRow[idx]
			seedNode := (*storage.Node)(nil)
			switch v := seedVal.(type) {
			case *storage.Node:
				if v != nil {
					seedNode = v
				}
			case map[string]interface{}:
				seedNode = e.seedNodeFromMap(v)
			case string:
				seedNode = e.seedNodeFromIDString(v)
			case []interface{}:
				if len(v) == 1 {
					switch wrapped := v[0].(type) {
					case *storage.Node:
						if wrapped != nil {
							seedNode = wrapped
						}
					case map[string]interface{}:
						seedNode = e.seedNodeFromMap(wrapped)
					case string:
						seedNode = e.seedNodeFromIDString(wrapped)
					}
				}
			}
			if seedNode != nil {
				pname := "__seed_id_" + varName
				params[pname] = string(seedNode.ID)
				nodeBindClauses = append(nodeBindClauses, fmt.Sprintf("MATCH (%s) WHERE id(%s) = $%s", varName, varName, pname))
				nodeBindVars = append(nodeBindVars, varName)
				continue
			}
			params[varName] = seedVal
			correlatedBody = replaceStandaloneCypherIdentifier(correlatedBody, varName, "$"+varName)
		}
		if len(nodeBindClauses) > 0 {
			prefix := strings.Join(nodeBindClauses, " ")
			// Keep imported node variables in scope directly from MATCH bindings.
			// Inserting an extra WITH here breaks valid MERGE tails such as:
			//   MATCH ... WITH p MERGE ...
			// under the current compound MATCH/MERGE execution path.
			// The MATCH bindings already provide the imported variables.
			_ = nodeBindVars
			correlatedBody = prefix + " " + correlatedBody
		}

		innerRes, err := e.executeInternal(ctx, correlatedBody, params)
		if err != nil {
			return nil, fmt.Errorf("CALL subquery error: %w", err)
		}

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

	return &ExecuteResult{Columns: combinedCols, Rows: combinedRows}, nil
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
		return nil, true, fmt.Errorf("batched correlated CALL lookup failed: %w", err)
	}

	if innerRes == nil {
		return &ExecuteResult{
			Columns: append([]string{}, seedResult.Columns...),
			Rows:    [][]interface{}{},
		}, true, nil
	}
	if len(innerRes.Columns) == 0 || !strings.EqualFold(strings.TrimSpace(innerRes.Columns[0]), "__call_subquery_lookup_key") {
		return nil, true, fmt.Errorf("batched correlated CALL lookup produced unexpected columns: %v", innerRes.Columns)
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
	terms := splitTopLevelAndCallSubquery(whereClause)
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
	terms := splitTopLevelAndCallSubquery(otherWhere)
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

func splitTopLevelAndCallSubquery(whereClause string) []string {
	parts := make([]string, 0, 4)
	start := 0
	paren, bracket, brace := 0, 0, 0
	inSingle, inDouble, inBacktick := false, false, false
	for i := 0; i < len(whereClause); i++ {
		ch := whereClause[i]
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
		if findKeywordIndex(whereClause[i:], "AND") == 0 {
			parts = append(parts, strings.TrimSpace(whereClause[start:i]))
			i += len("AND") - 1
			start = i + 1
		}
	}
	parts = append(parts, strings.TrimSpace(whereClause[start:]))
	return parts
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

// replaceStandaloneCypherIdentifier replaces identifier tokens that are not part of
// a dotted access chain (e.g. preserves tt.translationId when replacing translationId).
func replaceStandaloneCypherIdentifier(query, ident, replacement string) string {
	if ident == "" || query == "" || ident == replacement {
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

		prevWord := j > 0 && isWord(query[j-1])
		nextWord := k < len(query) && isWord(query[k])
		dotted := j > 0 && query[j-1] == '.'

		out.WriteString(query[i:j])
		if !prevWord && !nextWord && !dotted {
			out.WriteString(replacement)
		} else {
			out.WriteString(query[j:k])
		}
		i = k
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

	rows := make([][]interface{}, 0, len(left.Rows)*len(right.Rows))
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
	// Parse RETURN expressions
	returnIdx := findKeywordIndex(afterCall, "RETURN")
	if returnIdx == -1 {
		return innerResult, nil
	}

	returnClause := strings.TrimSpace(afterCall[returnIdx+6:])

	// Check for ORDER BY, LIMIT, SKIP using top-level keyword scanning so
	// multiline RETURN clauses are split deterministically.
	modifierIdx := len(returnClause)
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(returnClause, kw); idx >= 0 && idx < modifierIdx {
			modifierIdx = idx
		}
	}

	returnExprs := strings.TrimSpace(returnClause[:modifierIdx])
	modifierClause := ""
	if modifierIdx < len(returnClause) {
		modifierClause = returnClause[modifierIdx:]
	}

	// Parse return expressions
	parts := splitReturnExpressions(returnExprs)

	// Build column mapping from inner result
	colMap := make(map[string]int)
	for i, col := range innerResult.Columns {
		colMap[col] = i
	}

	// Check if RETURN clause has aggregation functions
	hasAggregation := false
	for _, part := range parts {
		if containsAggregateFunc(part) {
			hasAggregation = true
			break
		}
	}

	if hasAggregation {
		// Handle aggregation - aggregate all rows into one
		newColumns := make([]string, len(parts))
		resultRow := make([]interface{}, len(parts))

		for i, part := range parts {
			part = strings.TrimSpace(part)

			// Check for alias
			alias := part
			expr := part
			upperPart := strings.ToUpper(part)
			if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
				alias = strings.TrimSpace(part[asIdx+4:])
				expr = strings.TrimSpace(part[:asIdx])
			}

			newColumns[i] = alias

			if containsAggregateFunc(expr) {
				// Handle aggregation functions
				inner := extractFuncInner(expr)

				if isAggregateFuncName(expr, "collect") {
					// Handle COLLECT (with or without DISTINCT)
					upperInner := strings.ToUpper(inner)
					isDistinct := strings.HasPrefix(upperInner, "DISTINCT ")
					collectExpr := inner
					if isDistinct {
						collectExpr = strings.TrimSpace(inner[9:])
					}

					seen := make(map[string]bool)
					var collected []interface{}
					for _, row := range innerResult.Rows {
						// Build a values map from the row
						values := make(map[string]interface{})
						for j, col := range innerResult.Columns {
							if j < len(row) {
								values[col] = row[j]
							}
						}
						val := e.evaluateExpressionFromValues(collectExpr, values)
						if isDistinct {
							key := fmt.Sprintf("%v", val)
							if !seen[key] {
								seen[key] = true
								collected = append(collected, val)
							}
						} else {
							collected = append(collected, val)
						}
					}
					resultRow[i] = collected
				} else if isAggregateFuncName(expr, "count") {
					if inner == "*" {
						resultRow[i] = int64(len(innerResult.Rows))
					} else {
						count := int64(0)
						for _, row := range innerResult.Rows {
							if idx, ok := colMap[inner]; ok && idx < len(row) && row[idx] != nil {
								count++
							}
						}
						resultRow[i] = count
					}
				} else if isAggregateFuncName(expr, "sum") {
					sum := float64(0)
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							if num, ok := toFloat64(row[idx]); ok {
								sum += num
							}
						}
					}
					resultRow[i] = sum
				} else if isAggregateFuncName(expr, "avg") {
					sum := float64(0)
					count := 0
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							if num, ok := toFloat64(row[idx]); ok {
								sum += num
								count++
							}
						}
					}
					if count > 0 {
						resultRow[i] = sum / float64(count)
					}
				} else if isAggregateFuncName(expr, "min") {
					var minVal interface{}
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							val := row[idx]
							if val != nil && (minVal == nil || e.compareOrderValues(val, minVal) < 0) {
								minVal = val
							}
						}
					}
					resultRow[i] = minVal
				} else if isAggregateFuncName(expr, "max") {
					var maxVal interface{}
					for _, row := range innerResult.Rows {
						if idx, ok := colMap[inner]; ok && idx < len(row) {
							val := row[idx]
							if val != nil && (maxVal == nil || e.compareOrderValues(val, maxVal) > 0) {
								maxVal = val
							}
						}
					}
					resultRow[i] = maxVal
				}
			} else {
				// Non-aggregated column - use value from first row
				if len(innerResult.Rows) > 0 {
					if idx, ok := colMap[expr]; ok && idx < len(innerResult.Rows[0]) {
						resultRow[i] = innerResult.Rows[0][idx]
					}
				}
			}
		}

		result := &ExecuteResult{
			Columns: newColumns,
			Rows:    [][]interface{}{resultRow},
			Stats:   innerResult.Stats,
		}

		// Apply modifiers (ORDER BY, LIMIT, SKIP)
		if modifierClause != "" {
			return e.applyResultModifiers(result, modifierClause)
		}
		return result, nil
	}

	// No aggregation - Project columns
	type returnProjection struct {
		alias string
		expr  string
		idx   int
	}
	projections := make([]returnProjection, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)

		// Check for alias
		alias := part
		expr := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			expr = strings.TrimSpace(part[:asIdx])
		}

		idx := -1
		if colIdx, ok := colMap[expr]; ok {
			idx = colIdx
		}
		projections = append(projections, returnProjection{
			alias: alias,
			expr:  expr,
			idx:   idx,
		})
	}

	// Project rows
	newColumns := make([]string, len(projections))
	for i := range projections {
		newColumns[i] = projections[i].alias
	}
	newRows := make([][]interface{}, 0, len(innerResult.Rows))
	for _, row := range innerResult.Rows {
		// Build expression yield-context once per row for computed projections.
		yieldCtx := make(map[string]interface{}, len(innerResult.Columns))
		for i, col := range innerResult.Columns {
			if i < len(row) {
				yieldCtx[col] = row[i]
			}
		}

		newRow := make([]interface{}, len(projections))
		for i, p := range projections {
			if p.idx >= 0 && p.idx < len(row) {
				newRow[i] = row[p.idx]
				continue
			}
			newRow[i] = e.evaluateReturnExprInContext(ctx, p.expr, yieldCtx)
		}
		newRows = append(newRows, newRow)
	}

	result := &ExecuteResult{
		Columns: newColumns,
		Rows:    newRows,
		Stats:   innerResult.Stats,
	}

	// Apply modifiers (ORDER BY, LIMIT, SKIP)
	if modifierClause != "" {
		return e.applyResultModifiers(result, modifierClause)
	}

	return result, nil
}

// applyResultModifiers applies ORDER BY, LIMIT, SKIP to a result
func (e *StorageExecutor) applyResultModifiers(result *ExecuteResult, modifiers string) (*ExecuteResult, error) {
	orderByCol, orderByDesc, hasOrderBy := parseOrderByModifier(modifiers)
	skip, hasSkip := parseIntModifier(modifiers, "SKIP")
	limit, hasLimit := parseIntModifier(modifiers, "LIMIT")

	if hasOrderBy && hasLimit && limit >= 0 {
		if colIdx := findColumnIndexByName(result.Columns, orderByCol); colIdx >= 0 {
			k := limit
			if hasSkip && skip > 0 {
				k += skip
			}
			switch {
			case k <= 0:
				result.Rows = [][]interface{}{}
				return result, nil
			case k < len(result.Rows):
				result.Rows = selectTopKRowsForOrder(result.Rows, colIdx, orderByDesc, k)
			default:
				result = e.applyOrderByToResult(result, modifiers)
			}
		} else {
			// Preserve prior behavior for unknown ORDER BY columns.
			result = e.applyOrderByToResult(result, modifiers)
		}
	} else if hasOrderBy {
		result = e.applyOrderByToResult(result, modifiers)
	}

	// Apply SKIP after ORDER BY/TOP-K.
	if hasSkip && skip > 0 {
		if skip < len(result.Rows) {
			result.Rows = result.Rows[skip:]
		} else {
			result.Rows = [][]interface{}{}
		}
	}

	// Apply LIMIT after ORDER BY/TOP-K.
	if hasLimit && limit >= 0 {
		if limit < len(result.Rows) {
			result.Rows = result.Rows[:limit]
		}
	}

	return result, nil
}

// applyOrderByToResult applies ORDER BY to a result set
func (e *StorageExecutor) applyOrderByToResult(result *ExecuteResult, orderByClause string) *ExecuteResult {
	// Parse ORDER BY column [DESC|ASC]
	clause := strings.TrimSpace(orderByClause)
	if idx := findKeywordIndex(clause, "ORDER BY"); idx != -1 {
		clause = strings.TrimSpace(clause[idx+8:])
	}

	// Find end of ORDER BY (before LIMIT, SKIP)
	endIdx := len(clause)
	for _, kw := range []string{" LIMIT", " SKIP"} {
		if idx := strings.Index(strings.ToUpper(clause), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}
	clause = strings.TrimSpace(clause[:endIdx])

	// Parse column and direction
	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return result
	}

	colName := parts[0]
	descending := false
	if len(parts) > 1 && strings.ToUpper(parts[1]) == "DESC" {
		descending = true
	}

	// Find column index
	colIdx := -1
	for i, col := range result.Columns {
		if col == colName {
			colIdx = i
			break
		}
	}

	// If the ORDER BY column is a dotted property (e.g. t.createdAt) and not directly
	// projected, look for the variable name (e.g. "t") in the result columns and
	// extract the property from the returned node map.
	propName := ""
	if colIdx == -1 && strings.Contains(colName, ".") {
		parts := strings.SplitN(colName, ".", 2)
		varName, prop := parts[0], parts[1]
		for i, col := range result.Columns {
			if col == varName {
				colIdx = i
				propName = prop
				break
			}
		}
	}

	if colIdx == -1 {
		return result
	}

	// Sort rows
	sort.SliceStable(result.Rows, func(i, j int) bool {
		vi := result.Rows[i][colIdx]
		vj := result.Rows[j][colIdx]
		// Extract property from node map if ORDER BY uses var.property on a returned node
		if propName != "" {
			vi = extractPropertyFromValue(vi, propName)
			vj = extractPropertyFromValue(vj, propName)
		}
		cmp := compareValuesForSort(vi, vj)
		if descending {
			return cmp > 0
		}
		return cmp < 0
	})

	return result
}

// compareValuesForSort compares two values for sorting, returns -1, 0, or 1
// extractPropertyFromValue extracts a named property from a value that may be a node map.
func extractPropertyFromValue(val interface{}, propName string) interface{} {
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
		return -1
	}
	if b == nil {
		return 1
	}

	// Try numeric comparison
	switch va := a.(type) {
	case int:
		if vb, ok := b.(int); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case int64:
		if vb, ok := b.(int64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case float64:
		if vb, ok := b.(float64); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	case string:
		if vb, ok := b.(string); ok {
			if va < vb {
				return -1
			} else if va > vb {
				return 1
			}
			return 0
		}
	}

	// Fallback to string comparison
	sa := fmt.Sprintf("%v", a)
	sb := fmt.Sprintf("%v", b)
	if sa < sb {
		return -1
	} else if sa > sb {
		return 1
	}
	return 0
}

func parseOrderByModifier(modifiers string) (column string, descending bool, ok bool) {
	orderByIdx := findKeywordIndex(modifiers, "ORDER BY")
	if orderByIdx == -1 {
		return "", false, false
	}
	clause := strings.TrimSpace(modifiers[orderByIdx+8:])
	// Find end of ORDER BY (before LIMIT, SKIP)
	endIdx := len(clause)
	for _, kw := range []string{" LIMIT", " SKIP"} {
		if idx := strings.Index(strings.ToUpper(clause), kw); idx != -1 && idx < endIdx {
			endIdx = idx
		}
	}
	clause = strings.TrimSpace(clause[:endIdx])
	parts := strings.Fields(clause)
	if len(parts) == 0 {
		return "", false, false
	}
	col := strings.TrimSuffix(strings.TrimSpace(parts[0]), ",")
	descTok := ""
	if len(parts) > 1 {
		descTok = strings.TrimSuffix(strings.TrimSpace(parts[1]), ",")
	}
	desc := strings.EqualFold(descTok, "DESC")
	return col, desc, col != ""
}

func parseIntModifier(modifiers, keyword string) (value int, ok bool) {
	idx := findKeywordIndex(modifiers, keyword)
	if idx == -1 {
		return 0, false
	}
	kwPart := strings.TrimSpace(modifiers[idx+len(keyword):])
	nextKw := len(kwPart)
	for _, kw := range []string{" LIMIT", " SKIP", " ORDER"} {
		if kw == " "+keyword {
			continue
		}
		if kidx := strings.Index(strings.ToUpper(kwPart), kw); kidx != -1 && kidx < nextKw {
			nextKw = kidx
		}
	}
	vs := strings.TrimSpace(kwPart[:nextKw])
	v, err := strconv.Atoi(vs)
	if err != nil {
		return 0, false
	}
	return v, true
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
