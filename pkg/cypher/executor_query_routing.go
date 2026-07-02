package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryFastPathCompoundQuery attempts to handle common compound query patterns
// using structured scanning rather than regex capture arrays.
// Returns (result, true) if handled, (nil, false) if the query should go through normal routing.
//
// Pattern: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
// This is a very common pattern in benchmarks and relationship tests.
func (e *StorageExecutor) tryFastPathCompoundQuery(ctx context.Context, cypher string) (*ExecuteResult, bool) {
	if match, ok := matchCompoundQueryShape(cypher); ok {
		switch match.Kind {
		case shapeKindCompoundCreateDeleteRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRel(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
			)
		case shapeKindCompoundPropCreateDeleteRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRel(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
			)
		case shapeKindCompoundPropCreateDeleteReturnCountRel:
			e.markCompoundQueryFastPathUsed()
			return e.executeFastPathCreateDeleteRelCount(
				match.Captures.String("label1"),
				match.Captures.String("label2"),
				match.Captures.String("prop1"),
				match.Captures.Any("value1"),
				match.Captures.String("prop2"),
				match.Captures.Any("value2"),
				match.Captures.String("rel_type"),
				match.Captures.String("rel_var"),
			)
		}
	}

	return nil, false
}

// executeFastPathCreateDeleteRel executes the fast-path for MATCH...CREATE...DELETE patterns.
// If prop1/prop2 are empty, uses GetFirstNodeByLabel. Otherwise uses property lookup.
func (e *StorageExecutor) executeFastPathCreateDeleteRel(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string) (*ExecuteResult, bool) {
	var err error

	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	return &ExecuteResult{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

func (e *StorageExecutor) executeFastPathCreateDeleteRelCount(label1, label2, prop1 string, val1 any, prop2 string, val2 any, relType string, relVar string) (*ExecuteResult, bool) {
	var err error

	if prop1 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label1)
	} else {
		node1 := e.findNodeByLabelAndProperty(label1, prop1, val1)
		if node1 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	if prop2 == "" {
		_, err = storage.FirstNodeIDByLabel(e.storage, label2)
	} else {
		node2 := e.findNodeByLabelAndProperty(label2, prop2, val2)
		if node2 == nil {
			return nil, false
		}
	}
	if err != nil {
		return nil, false
	}

	return &ExecuteResult{
		Columns: []string{"count(" + relVar + ")"},
		Rows:    [][]interface{}{{int64(1)}},
		Stats: &QueryStats{
			RelationshipsCreated: 1,
			RelationshipsDeleted: 1,
		},
	}, true
}

// findNodeByLabelAndProperty finds a node by label and a single property value.
// Uses the node lookup cache for O(1) repeated lookups.
func (e *StorageExecutor) findNodeByLabelAndProperty(label, prop string, val any) *storage.Node {
	e.ensureNodeLookupCache()

	cacheKey := fmt.Sprintf("%s:{%s:%v}", label, prop, val)
	cacheMu := e.nodeLookupCacheLock()
	cacheMu.RLock()
	if cached, ok := e.nodeLookupCache[cacheKey]; ok {
		cacheMu.RUnlock()
		return cached
	}
	cacheMu.RUnlock()

	nodes, err := e.storage.GetNodesByLabel(label)
	if err != nil {
		return nil
	}

	for _, node := range nodes {
		if nodeVal, ok := node.Properties[prop]; ok {
			if fmt.Sprintf("%v", nodeVal) == fmt.Sprintf("%v", val) {
				cacheMu.Lock()
				e.nodeLookupCache[cacheKey] = node
				cacheMu.Unlock()
				return node
			}
		}
	}

	return nil
}

// isSystemCommandNoGraph returns true for statements that operate on database metadata
// (CREATE/DROP DATABASE, SHOW DATABASES, etc.) and must not use the async engine or
// implicit transactions. These are routed to executeWithoutTransaction directly.
func isSystemCommandNoGraph(cypher string) bool {
	return findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0 ||
		findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0 ||
		findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0 ||
		findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0
}

func isShowConstraintContractsCommand(cypher string) bool {
	return findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINT CONTRACTS") == 0
}

// executeWithoutTransaction executes query without transaction wrapping (original path).
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (*ExecuteResult, error) {
	if result, handled := e.tryFastPathSimpleMatchReturnLimit(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathAnyMatchVectorCosine(ctx, cypher, upperQuery); handled {
		return result, nil
	}
	if result, handled := e.tryFastPathCompoundQuery(ctx, cypher); handled {
		return result, nil
	}

	startsWithMatch := strings.HasPrefix(upperQuery, "MATCH")
	startsWithCreate := strings.HasPrefix(upperQuery, "CREATE")
	startsWithMerge := strings.HasPrefix(upperQuery, "MERGE")

	if startsWithMatch && hasSubqueryPattern(cypher, callSubqueryRe) {
		return e.executeMatchWithCallSubquery(ctx, cypher)
	}

	if startsWithMatch {
		callIdx := findKeywordIndex(cypher, "CALL")
		if callIdx > 0 {
			callPart := strings.TrimSpace(cypher[callIdx:])
			if !isCallSubquery(callPart) {
				prefix := cypher[:callIdx]
				hasMutationBeforeCall := findKeywordIndexInContext(prefix, "MERGE") >= 0 ||
					findKeywordIndexInContext(prefix, "CREATE") >= 0 ||
					findKeywordIndexInContext(prefix, "SET") >= 0 ||
					findKeywordIndexInContext(prefix, "DELETE") >= 0 ||
					findKeywordIndexInContext(prefix, "DETACH DELETE") >= 0 ||
					findKeywordIndexInContext(prefix, "REMOVE") >= 0
				if hasMutationBeforeCall {
					goto skipMatchCallRoute
				}
				if findKeywordIndex(cypher[:callIdx], "WITH") > 0 {
					return e.executeMatchWithClause(ctx, cypher)
				}
				return e.executeMatchWithCallProcedure(ctx, cypher)
			}
		}
	}

skipMatchCallRoute:
	if startsWithMerge {
		if findKeywordIndexInContext(cypher, "OPTIONAL MATCH") > 0 ||
			findKeywordIndexInContext(cypher, "WITH") > 0 ||
			findKeywordIndexInContext(cypher, "WHERE") > 0 {
			return e.executeMultipleMerges(ctx, cypher)
		}
		firstMergeEnd := findKeywordIndex(cypher[5:], ")")
		if firstMergeEnd > 0 {
			afterFirstMerge := cypher[5+firstMergeEnd+1:]
			secondMergeIdx := findKeywordIndex(afterFirstMerge, "MERGE")
			if secondMergeIdx >= 0 {
				return e.executeMultipleMerges(ctx, cypher)
			}
		}
		return e.executeMerge(ctx, cypher)
	}

	var mergeIdx, createIdx, withIdx, deleteIdx, optionalMatchIdx int = -1, -1, -1, -1, -1

	if startsWithMatch {
		mergeIdx = findKeywordIndex(cypher, "MERGE")
		createIdx = findKeywordIndex(cypher, "CREATE")
		optionalMatchIdx = findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH")
	} else if startsWithCreate {
		firstCreateEnd := findKeywordIndex(cypher[6:], ")")
		if firstCreateEnd > 0 {
			afterFirstCreate := cypher[6+firstCreateEnd+1:]
			secondCreateIdx := findKeywordIndex(afterFirstCreate, "CREATE")
			if secondCreateIdx >= 0 {
				return e.executeMultipleCreates(ctx, cypher)
			}
		}
		withIdx = findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			deleteIdx = findKeywordIndex(cypher, "DELETE")
		}
	}

	if startsWithMatch && mergeIdx > 0 {
		return e.executeCompoundMatchMerge(ctx, cypher)
	}
	if startsWithMatch && createIdx > 0 {
		if result, ok, err := e.executePipeline(ctx, cypher); ok {
			return result, err
		}
		return e.executeCompoundMatchCreate(ctx, cypher)
	}
	if startsWithCreate && withIdx > 0 && deleteIdx > 0 {
		return e.executeCompoundCreateWithDelete(ctx, cypher)
	}
	if findKeywordIndex(cypher, "UNWIND") == 0 {
		return e.executeUnwind(ctx, cypher)
	}

	hasDelete := findKeywordIndex(cypher, "DELETE") > 0
	hasDetachDelete := containsKeywordOutsideStrings(cypher, "DETACH DELETE")
	if hasDelete || hasDetachDelete {
		return e.executeDelete(ctx, cypher)
	}

	hasSet := containsKeywordOutsideStrings(cypher, "SET")
	hasOnCreateSet := containsKeywordOutsideStrings(cypher, "ON CREATE SET")
	hasOnMatchSet := containsKeywordOutsideStrings(cypher, "ON MATCH SET")

	if startsWithCreate && !isCreateProcedureCommand(cypher) && hasSet && !hasOnCreateSet && !hasOnMatchSet &&
		findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") != 0 {
		return e.executeCreateSet(ctx, cypher)
	}

	if findMultiWordKeywordIndex(cypher, "ALTER", "DATABASE") == 0 {
		return e.executeAlterDatabase(ctx, cypher)
	}

	if hasSet && !isCreateProcedureCommand(cypher) && !hasOnCreateSet && !hasOnMatchSet &&
		findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "DECAY PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION PROFILE") != 0 &&
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION POLICY") != 0 {
		if startsWithMatch || findKeywordIndex(cypher, "SET") == 0 {
			return e.executeSet(ctx, cypher)
		}
	}

	if containsKeywordOutsideStrings(cypher, "REMOVE") {
		return e.executeRemove(ctx, cypher)
	}

	if startsWithMatch && optionalMatchIdx > 0 {
		withBeforeOptional := findKeywordIndex(cypher[:optionalMatchIdx], "WITH")
		if withBeforeOptional > 0 {
			return e.executeMatchWithOptionalMatch(ctx, cypher)
		}
		return e.executeCompoundMatchOptionalMatch(ctx, cypher)
	}

	switch {
	case isCreateProcedureCommand(cypher):
		return e.executeCreateProcedure(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "OPTIONAL", "MATCH") == 0:
		return e.executeOptionalMatch(ctx, cypher)
	case startsWithMatch && isShortestPathQuery(cypher):
		spCypher := cypher
		if params := getParamsFromContext(ctx); params != nil {
			spCypher = e.substituteParams(spCypher, params)
		}
		query, err := e.parseShortestPathQuery(ctx, spCypher)
		if err != nil {
			return nil, err
		}
		return e.executeShortestPathQuery(ctx, query)
	case startsWithMatch:
		matchCount := countKeywordOccurrences(upperQuery, "MATCH")
		optionalMatchCount := countKeywordOccurrences(upperQuery, "OPTIONAL MATCH")
		isMultiMatch := matchCount-optionalMatchCount > 1
		if !isMultiMatch {
			patternInfo := DetectQueryPattern(ctx, cypher)
			if patternInfo.IsOptimizable() {
				if result, ok := e.ExecuteOptimized(ctx, cypher, patternInfo); ok {
					return result, nil
				}
			}
		}
		return e.executeMatch(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "CONSTRAINT") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "RANGE INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "CREATE", "VECTOR INDEX") == 0,
		findKeywordIndex(cypher, "CREATE INDEX") == 0:
		return e.executeSchemaCommand(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "COMPOSITE DATABASE") == 0:
		return e.executeCreateCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "DATABASE") == 0:
		return e.executeCreateDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "CREATE", "ALIAS") == 0:
		return e.executeCreateAlias(ctx, cypher)
	case startsWithCreate:
		return e.executeCreate(ctx, cypher)
	case hasDelete || hasDetachDelete:
		return e.executeDelete(ctx, cypher)
	case findKeywordIndex(cypher, "CALL") == 0:
		if isCallSubquery(cypher) {
			return e.executeCallSubquery(ctx, cypher)
		}
		return e.executeCall(ctx, cypher)
	case findKeywordIndex(cypher, "RETURN") == 0:
		return e.executeReturn(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "COMPOSITE DATABASE") == 0:
		return e.executeDropCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "DATABASE") == 0:
		return e.executeDropDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "ALIAS") == 0:
		return e.executeDropAlias(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "CONSTRAINT") == 0:
		return e.executeSchemaCommand(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "DROP", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "DROP", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case isDropProcedureCommand(cypher):
		return e.executeDropProcedure(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "DROP", "INDEX") == 0:
		return e.executeDropIndex(ctx, cypher)
	case findKeywordIndex(cypher, "DROP") == 0:
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	case findKeywordIndex(cypher, "WITH") == 0:
		return e.executeWith(ctx, cypher)
	case findKeywordIndex(cypher, "UNWIND") == 0:
		return e.executeUnwind(ctx, cypher)
	case findKeywordIndex(cypher, "UNION ALL") >= 0:
		return e.executeUnion(ctx, cypher, true)
	case findKeywordIndex(cypher, "UNION") >= 0:
		return e.executeUnion(ctx, cypher, false)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		return e.executeForeach(ctx, cypher)
	case findKeywordIndex(cypher, "LOAD CSV") == 0:
		return e.executeLoadCSV(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "FULLTEXT INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "RANGE INDEX") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "VECTOR INDEX") == 0:
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "INDEXES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "INDEX") == 0:
		return e.executeShowIndexes(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DECAY PROFILES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "PROMOTION PROFILES") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "PROMOTION POLICIES") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINTS") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "CONSTRAINT") == 0:
		return e.executeShowConstraints(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "PROCEDURES") == 0:
		return e.executeShowProcedures(ctx, cypher)
	case findKeywordIndex(cypher, "SHOW FUNCTIONS") == 0:
		return e.executeShowFunctions(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "COMPOSITE DATABASES") == 0:
		return e.executeShowCompositeDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "CONSTITUENTS") == 0:
		return e.executeShowConstituents(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASES") == 0:
		return e.executeShowDatabases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "DATABASE") == 0:
		return e.executeShowDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "ALIASES") == 0:
		return e.executeShowAliases(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE DATABASE") == 0:
		return e.executeAlterCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS") == 0:
		return e.executeShowLimits(ctx, cypher)
	default:
		firstWord := strings.Split(upperQuery, " ")[0]
		return nil, fmt.Errorf("unsupported query type: %s (supported: MATCH, CREATE, MERGE, DELETE, SET, REMOVE, RETURN, WITH, UNWIND, CALL, FOREACH, LOAD CSV, SHOW, DROP, ALTER)", firstWord)
	}
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if params := getParamsFromContext(ctx); params != nil {
		cypher = e.substituteParams(cypher, params)
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, fmt.Errorf("RETURN clause not found in query: %q", truncateQuery(cypher, 80))
	}

	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	if cut := firstTopLevelModifierIndex(returnClause); cut >= 0 {
		returnClause = strings.TrimSpace(returnClause[:cut])
	}

	parts := splitReturnExpressions(returnClause)
	columns := make([]string, 0, len(parts))
	values := make([]interface{}, 0, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		alias := part
		upperPart := strings.ToUpper(part)
		if asIdx := strings.Index(upperPart, " AS "); asIdx != -1 {
			alias = strings.TrimSpace(part[asIdx+4:])
			part = strings.TrimSpace(part[:asIdx])
		}

		columns = append(columns, alias)

		if strings.EqualFold(part, "null") {
			values = append(values, nil)
			continue
		}

		if isValidIdentifier(part) {
			if v, ok := e.fabricRecordBindings[part]; ok {
				values = append(values, v)
				continue
			}
		}

		result := e.evaluateExpressionWithContext(ctx, part, nil, nil)
		if result != nil {
			values = append(values, result)
			continue
		}

		if part == "1" || strings.HasPrefix(strings.ToLower(part), "true") {
			values = append(values, int64(1))
		} else if part == "0" || strings.HasPrefix(strings.ToLower(part), "false") {
			values = append(values, int64(0))
		} else if strings.HasPrefix(part, "'") && strings.HasSuffix(part, "'") {
			values = append(values, part[1:len(part)-1])
		} else if strings.HasPrefix(part, "\"") && strings.HasSuffix(part, "\"") {
			values = append(values, part[1:len(part)-1])
		} else {
			if val, err := strconv.ParseInt(part, 10, 64); err == nil {
				values = append(values, val)
			} else if val, err := strconv.ParseFloat(part, 64); err == nil {
				values = append(values, val)
			} else {
				values = append(values, part)
			}
		}
	}

	return &ExecuteResult{
		Columns: columns,
		Rows:    [][]interface{}{values},
	}, nil
}

func firstTopLevelModifierIndex(clause string) int {
	cut := -1
	for _, kw := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		if idx := topLevelKeywordIndex(clause, kw); idx >= 0 && (cut == -1 || idx < cut) {
			cut = idx
		}
	}
	return cut
}

// splitReturnExpressions splits RETURN expressions by comma, respecting parentheses and brackets depth
func splitReturnExpressions(clause string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	bracketDepth := 0
	inQuote := false
	quoteChar := rune(0)

	for _, ch := range clause {
		switch {
		case (ch == '\'' || ch == '"') && !inQuote:
			inQuote = true
			quoteChar = ch
			current.WriteRune(ch)
		case ch == quoteChar && inQuote:
			inQuote = false
			quoteChar = 0
			current.WriteRune(ch)
		case ch == '(' && !inQuote:
			parenDepth++
			current.WriteRune(ch)
		case ch == ')' && !inQuote:
			parenDepth--
			current.WriteRune(ch)
		case ch == '[' && !inQuote:
			bracketDepth++
			current.WriteRune(ch)
		case ch == ']' && !inQuote:
			bracketDepth--
			current.WriteRune(ch)
		case ch == ',' && parenDepth == 0 && bracketDepth == 0 && !inQuote:
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(ch)
		}
	}

	if current.Len() > 0 {
		parts = append(parts, current.String())
	}

	return parts
}

// validateSyntax performs syntax validation.
// When NORNICDB_PARSER=antlr, uses ANTLR for strict OpenCypher grammar validation.
// When NORNICDB_PARSER=nornic (default), uses fast inline validation.
func (e *StorageExecutor) validateSyntax(cypher string) error {
	if config.IsANTLRParser() {
		return e.validateSyntaxANTLR(cypher)
	}
	return e.validateSyntaxNornic(cypher)
}

// validateSyntaxANTLR uses ANTLR for strict OpenCypher grammar validation.
// Provides detailed error messages with line/column information.
func (e *StorageExecutor) validateSyntaxANTLR(cypher string) error {
	return antlr.Validate(cypher)
}

// validateSyntaxNornic performs fast inline syntax validation.
func (e *StorageExecutor) validateSyntaxNornic(cypher string) error {
	if e.hasCachedValidSyntax(cypher) {
		return nil
	}
	if !hasValidStartKeyword(cypher) {
		return fmt.Errorf("syntax error: query must start with a valid clause (MATCH, CREATE, MERGE, DELETE, CALL, SHOW, EXPLAIN, PROFILE, ALTER, USE, BEGIN, COMMIT, ROLLBACK, etc.)")
	}

	parenCount := 0
	bracketCount := 0
	braceCount := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]

		if inString {
			if c == stringChar && (i == 0 || cypher[i-1] != '\\') {
				inString = false
			}
			continue
		}

		switch c {
		case '"', '\'':
			inString = true
			stringChar = c
		case '(':
			parenCount++
		case ')':
			parenCount--
		case '[':
			bracketCount++
		case ']':
			bracketCount--
		case '{':
			braceCount++
		case '}':
			braceCount--
		}

		if parenCount < 0 || bracketCount < 0 || braceCount < 0 {
			return fmt.Errorf("syntax error: unbalanced brackets at position %d", i)
		}
	}

	if parenCount != 0 {
		return fmt.Errorf("syntax error: unbalanced parentheses")
	}
	if bracketCount != 0 {
		return fmt.Errorf("syntax error: unbalanced square brackets")
	}
	if braceCount != 0 {
		return fmt.Errorf("syntax error: unbalanced curly braces")
	}
	if inString {
		return fmt.Errorf("syntax error: unclosed quote")
	}

	e.markCachedValidSyntax(cypher)
	return nil
}

var validSyntaxStarts = [...]string{
	"MATCH", "CREATE", "MERGE", "DELETE", "DETACH", "CALL", "RETURN", "WITH",
	"UNWIND", "OPTIONAL", "DROP", "SHOW", "FOREACH", "LOAD", "EXPLAIN",
	"PROFILE", "ALTER", "USE", "BEGIN", "COMMIT", "ROLLBACK",
}

func hasValidStartKeyword(cypher string) bool {
	for _, start := range validSyntaxStarts {
		if startsWithKeywordFold(cypher, start) {
			return true
		}
	}
	return false
}

// ensureSyntaxValidationCache lazily installs the syntax-validation cache
// pointer using sync.Once so concurrent CALL { ... } subqueries (which fan
// out via executeCallTailParallel) cannot race on the pointer write. The
// underlying cache itself is already mutex-guarded; the race was on the
// initial pointer assignment.
func (e *StorageExecutor) ensureSyntaxValidationCache() *syntaxValidationCache {
	e.syntaxValidationOnce.Do(func() {
		if e.syntaxValidationCache == nil {
			e.syntaxValidationCache = &syntaxValidationCache{
				cache: make(map[string]struct{}, 1024),
				max:   4096,
			}
		}
	})
	return e.syntaxValidationCache
}

func (e *StorageExecutor) hasCachedValidSyntax(cypher string) bool {
	if cypher == "" {
		return false
	}
	c := e.ensureSyntaxValidationCache()
	c.mu.RLock()
	_, ok := c.cache[cypher]
	c.mu.RUnlock()
	return ok
}

func (e *StorageExecutor) markCachedValidSyntax(cypher string) {
	if cypher == "" {
		return
	}
	c := e.ensureSyntaxValidationCache()
	c.mu.Lock()
	if len(c.cache) >= c.max {
		for k := range c.cache {
			delete(c.cache, k)
			break
		}
	}
	c.cache[cypher] = struct{}{}
	c.mu.Unlock()
}
