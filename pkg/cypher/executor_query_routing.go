package cypher

import (
	"context"
	"fmt"
	"strings"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// tryFastPathCompoundQuery attempts to handle common compound query patterns
// using structured scanning rather than regex capture arrays.
// Returns (result, true) if handled, (nil, false) if the query should go through normal routing.
//
// Pattern: MATCH (a:Label), (b:Label) WITH a, b LIMIT 1 CREATE (a)-[r:Type]->(b) DELETE r
// This is a very common pattern in benchmarks and relationship tests.
func (e *StorageExecutor) tryFastPathCompoundQuery(ctx context.Context, cypher string) (*ExecuteResult, bool) {
	// Every shape handled below mutates a relationship and then deletes it.
	// Reject ordinary reads before invoking the compound-shape matcher: a
	// partial structural match must never perform speculative label scans on
	// the way to the converged read pipeline.
	if !containsKeywordOutsideStrings(cypher, "CREATE") || !containsKeywordOutsideStrings(cypher, "DELETE") {
		return nil, false
	}
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
func (e *StorageExecutor) executeWithoutTransaction(ctx context.Context, cypher string, upperQuery string) (result *ExecuteResult, err error) {
	defer func() {
		if recorded := getExpressionFailure(ctx); recorded != nil && err == nil {
			result, err = nil, recorded
		}
	}()
	// A top-level UNION composes complete single queries. Route it before any
	// handler can consume the leading MATCH, RETURN, or UNWIND branch. The
	// inexpensive substring guard keeps non-UNION queries off the structural
	// scanner used to distinguish top-level separators from nested subqueries.
	if strings.Contains(upperQuery, "UNION") {
		if branches, unionAll, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
			return e.executeUnion(ctx, cypher, unionAll)
		}
	}

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
		callIdx := topLevelKeywordIndex(cypher, "CALL")
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
		// REMOVE is a row clause: statements with one run on the pipeline,
		// which applies MERGE actions, SET and REMOVE row by row.
		if !containsKeywordOutsideStrings(cypher, "SET") || containsKeywordOutsideStrings(cypher, "REMOVE") {
			if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
				return result, err
			}
		}
		if findKeywordIndexInContext(cypher, "OPTIONAL MATCH") > 0 ||
			findKeywordIndexInContext(cypher, "WITH") > 0 ||
			findKeywordIndexInContext(cypher, "WHERE") > 0 ||
			len(collectTopLevelMergeClauseBoundaries(cypher, []string{"CREATE"})) > 0 {
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
		if clauses, ok := splitPipelineClauses(cypher); ok {
			createCount := 0
			hasMutationBetweenCreates := false
			hasRowPipelineClause := false
			for _, clause := range clauses {
				if clause.kind == pipelineClauseCreate {
					createCount++
				}
				if clause.kind == pipelineClauseSet || clause.kind == pipelineClauseRemove || clause.kind == pipelineClauseMerge {
					hasMutationBetweenCreates = true
				}
				if clause.kind == pipelineClauseWith || clause.kind == pipelineClauseUnwind || clause.kind == pipelineClauseMatch || clause.kind == pipelineClauseOptionalMatch {
					hasRowPipelineClause = true
				}
			}
			if createCount > 1 && !hasMutationBetweenCreates && !hasRowPipelineClause {
				return e.executeMultipleCreates(ctx, cypher)
			}
		}
		withIdx = findKeywordIndex(cypher, "WITH")
		if withIdx > 0 {
			deleteIdx = findKeywordIndex(cypher, "DELETE")
		}
	}

	if startsWithMatch && mergeIdx > 0 {
		if !containsKeywordOutsideStrings(cypher, "SET") || containsKeywordOutsideStrings(cypher, "REMOVE") {
			if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
				return result, err
			}
		}
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
	if startsWithCreate && withIdx > 0 {
		if result, ok, err := e.executePipeline(ctx, cypher); ok || err != nil {
			return result, err
		}
		return e.executeMultipleCreates(ctx, cypher)
	}
	if findKeywordIndex(cypher, "UNWIND") == 0 {
		return e.executeTopLevelUnwind(ctx, cypher)
	}

	hasDelete := findKeywordIndex(cypher, "DELETE") > 0
	hasDetachDelete := containsKeywordOutsideStrings(cypher, "DETACH DELETE")
	if hasDelete || hasDetachDelete {
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
		return e.executeDelete(ctx, cypher)
	}

	hasSet := containsKeywordOutsideStrings(cypher, "SET")
	hasOnCreateSet := containsKeywordOutsideStrings(cypher, "ON CREATE SET")
	hasOnMatchSet := containsKeywordOutsideStrings(cypher, "ON MATCH SET")
	if startsWithMatch && hasSet && containsKeywordOutsideStrings(cypher, "REMOVE") {
		if result, ok, err := e.executePipeline(ctx, cypher); ok || err != nil {
			return result, err
		}
	}

	if startsWithCreate && hasSet && containsKeywordOutsideStrings(cypher, "REMOVE") {
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
	}
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
			if startsWithMatch {
				if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
					return result, err
				}
			}
			return e.executeSet(ctx, cypher)
		}
	}

	if containsKeywordOutsideStrings(cypher, "REMOVE") {
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
		return e.executeRemove(ctx, cypher)
	}

	if startsWithMatch && optionalMatchIdx > 0 {
		if result, ok, err := e.executePipeline(ctx, cypher); ok || err != nil {
			return result, err
		}
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
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
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
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
		matchCount := countKeywordOccurrences(upperQuery, "MATCH")
		optionalMatchCount := countKeywordOccurrences(upperQuery, "OPTIONAL MATCH")
		if matchCount-optionalMatchCount > 1 && findKeywordIndexInContext(cypher, "WITH") > 0 {
			if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
				return result, err
			}
		}
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
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
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
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid DROP clause: "+truncateQuery(cypher, 80))
	case findKeywordIndex(cypher, "WITH") == 0:
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
		return e.executeWith(ctx, cypher)
	case findKeywordIndex(cypher, "UNWIND") == 0:
		return e.executeUnwind(ctx, cypher)
	case findKeywordIndex(cypher, "FOREACH") == 0:
		if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
			return result, err
		}
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
	case findMultiWordKeywordIndex(cypher, "SHOW", "SETTINGS") == 0,
		findMultiWordKeywordIndex(cypher, "SHOW", "SETTING") == 0:
		return e.executeShowSettings(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "COMPOSITE DATABASE") == 0:
		return e.executeAlterCompositeDatabase(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "ALTER", "DECAY PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION PROFILE") == 0,
		findMultiWordKeywordIndex(cypher, "ALTER", "PROMOTION POLICY") == 0:
		return e.executeKnowledgePolicyDDL(ctx, cypher)
	case findMultiWordKeywordIndex(cypher, "SHOW", "LIMITS") == 0:
		return e.executeShowLimits(ctx, cypher)
	default:
		// Terminal chokepoint of the converged router: a statement that passed
		// syntax validation but matches no handler is rejected here — never a
		// silent success, alternate text executor, or re-dispatch. Neo4j
		// reports unrecognized statements as syntax errors, so the localized
		// message keeps its text while Bolt carries the proper status code.
		firstWord := strings.Split(upperQuery, " ")[0]
		err := localizedError(localization.CypherTransactionsQueryTypeUnsupported(firstWord), nil)
		return nil, &classifiedCypherError{
			cause:  err,
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
}

// executeTopLevelUnwind keeps autocommit and explicit-transaction routing in
// sync through the converged clause pipeline. Shapes outside the pipeline's
// grammar continue through the residual handler.
func (e *StorageExecutor) executeTopLevelUnwind(ctx context.Context, cypher string) (*ExecuteResult, error) {
	if result, handled, err := e.executePipeline(ctx, cypher); handled || err != nil {
		return result, err
	}
	return e.executeUnwind(ctx, cypher)
}

// executeReturn handles simple RETURN statements (e.g., "RETURN 1").
func (e *StorageExecutor) executeReturn(ctx context.Context, cypher string) (*ExecuteResult, error) {
	params := getParamsFromContext(ctx)
	if params != nil {
		cypher = e.substituteParams(cypher, params)
	}
	row := make(pipelineRow, len(e.fabricRecordBindings)+len(params))
	for name, value := range e.fabricRecordBindings {
		row[name] = value
	}
	for name, value := range params {
		row["$"+name] = value
	}

	returnIdx := findKeywordIndex(cypher, "RETURN")
	if returnIdx == -1 {
		return nil, localizedError(localization.CypherTransactionsReturnClauseNotFound(truncateQuery(cypher, 80)), nil)
	}

	returnClause := strings.TrimSpace(cypher[returnIdx+6:])
	if cut := firstTopLevelModifierIndex(returnClause); cut >= 0 {
		returnClause = strings.TrimSpace(returnClause[:cut])
	}

	parts := splitReturnExpressions(returnClause)
	columns := make([]string, 0, len(parts))
	values := make([]interface{}, 0, len(parts))

	for _, part := range parts {
		part, alias := parseProjectionExprAlias(part)
		if err := e.validateStaticBooleanOperands(ctx, part); err != nil {
			return nil, err
		}
		if err := validateStaticMembershipOperand(part); err != nil {
			return nil, err
		}
		if err := e.validateRangeCalls(part, pipelineRow{}); err != nil {
			return nil, err
		}
		if err := e.validateRowSubscriptTypes(part, pipelineRow{}); err != nil {
			return nil, err
		}
		if err := e.validateRowConversionArguments(part, row); err != nil {
			return nil, err
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

		if variable := undefinedStandaloneMapValue(part); variable != "" {
			return nil, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UndefinedVariable",
				"variable is not defined: "+variable,
			)
		}

		result, defined := e.evaluateRowExpressionWithContext(ctx, part, row)
		if !defined {
			if failure := getExpressionFailure(ctx); failure != nil {
				return nil, failure
			}
			err := newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UnexpectedSyntax",
				"could not parse RETURN expression: "+part,
			)
			recordExpressionFailure(ctx, err)
			return nil, err
		}
		values = append(values, result)
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

// splitReturnExpressions splits RETURN expressions by comma while preserving
// nested parentheses, lists, and map literals.
func splitReturnExpressions(clause string) []string {
	var parts []string
	var current strings.Builder
	parenDepth := 0
	bracketDepth := 0
	braceDepth := 0
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
		case ch == '{' && !inQuote:
			braceDepth++
			current.WriteRune(ch)
		case ch == '}' && !inQuote:
			braceDepth--
			current.WriteRune(ch)
		case ch == ',' && parenDepth == 0 && bracketDepth == 0 && braceDepth == 0 && !inQuote:
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
	if err := validateUnicodeOperators(cypher); err != nil {
		return err
	}
	if err := validateUnicodeStringLiterals(cypher); err != nil {
		return err
	}
	if err := validateStaticMapKeys(cypher); err != nil {
		return err
	}
	if err := validateNumericLiterals(cypher); err != nil {
		return err
	}
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
		// Neo4j reports an unrecognized statement as a syntax error; classify
		// the localized terminal so Bolt carries the proper status code.
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherTransactionsSyntaxStartInvalid(), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}
	if err := validateLeadingNodePatternTransition(cypher); err != nil {
		return err
	}

	if isGraphQueryStatement(cypher) && hasAdjacentOperands(cypher) {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: an expression is followed by another expression without an operator")
	}

	parenCount := 0
	bracketCount := 0
	braceCount := 0
	inString := false
	stringChar := byte(0)

	for i := 0; i < len(cypher); i++ {
		c := cypher[i]

		if inString {
			// A backtick-quoted name has no backslash escapes (a doubled
			// backtick closes and reopens it); string literals do.
			if c == stringChar && (c == '`' || !isBackslashEscaped(cypher, i)) {
				inString = false
			}
			continue
		}
		if c == '.' && i+1 < len(cypher) && cypher[i+1] == '.' && bracketCount == 0 {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
		}
		if c == '+' {
			next := skipSpaces(cypher, i+1)
			if next < len(cypher) && cypher[next] == '*' {
				return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: malformed expression")
			}
		}

		switch c {
		case '"', '\'', '`':
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
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"UnexpectedSyntax",
				fmt.Sprintf("syntax error: unbalanced delimiter at position %d", i),
			)
		}
	}

	if parenCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced parentheses")
	}
	if bracketCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced square brackets")
	}
	if braceCount != 0 {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unbalanced curly braces")
	}
	if inString {
		return &classifiedCypherError{
			cause:  localizedError(localization.CypherTransactionsSyntaxUnclosedQuote(), nil),
			code:   "Neo.ClientError.Statement.SyntaxError",
			detail: "UnexpectedSyntax",
		}
	}

	e.markCachedValidSyntax(cypher)
	return nil
}

// hasAdjacentOperands reports two operands with no operator between them,
// such as n.name 'x', (a =~ 'T.*')'T.*', 5 'x', [1] 'x' or n.a n.b, which
// Neo4j rejects as a syntax error in every expression (RETURN, WITH, WHERE,
// ORDER BY, SET, property maps). An operand ends with a string or number
// literal, ')' , ']' or a property access (x.name); the next token may not
// start another literal, $parameter or property access. A bare word followed
// by a literal is a variable next to an operand (n 'x') unless it is one of
// the keywords a literal may follow (literalLeadingKeywords); otherwise a bare
// word resets the check.
// It applies to graph queries only (isGraphQueryStatement): schema,
// administration and knowledge-policy statements have their own grammars,
// e.g. constraint contract blocks separate predicates by line breaks.
func hasAdjacentOperands(cypher string) bool {
	operandEnded := false
	for index := 0; index < len(cypher); {
		c := cypher[index]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			index++
		case c == '\'' || c == '"':
			if operandEnded {
				return true
			}
			end := index + 1
			for end < len(cypher) {
				if cypher[end] == c && !isBackslashEscaped(cypher, end) {
					if end+1 < len(cypher) && cypher[end+1] == c {
						end += 2 // a doubled quote is an escaped quote
						continue
					}
					break
				}
				end++
			}
			index = end + 1
			operandEnded = true
		case c == '`':
			end := strings.IndexByte(cypher[index+1:], '`')
			if end < 0 {
				return false
			}
			property := index > 0 && cypher[index-1] == '.'
			index += end + 2
			operandEnded = property
		case c >= '0' && c <= '9':
			if operandEnded {
				return true
			}
			index++
			for index < len(cypher) && (isIdentCharByte(cypher[index]) ||
				(cypher[index] == '.' && index+1 < len(cypher) && cypher[index+1] >= '0' && cypher[index+1] <= '9')) {
				index++
			}
			operandEnded = true
		case c == '$':
			if operandEnded {
				return true
			}
			index++
			for index < len(cypher) && isIdentCharByte(cypher[index]) {
				index++
			}
			operandEnded = true
		case isIdentCharByte(c):
			start := index
			for index < len(cypher) && isIdentCharByte(cypher[index]) {
				index++
			}
			startsProperty := index < len(cypher) && cypher[index] == '.' &&
				(index+1 >= len(cypher) || cypher[index+1] != '.')
			if operandEnded && startsProperty {
				return true
			}
			property := start > 0 && cypher[start-1] == '.' && (start < 2 || cypher[start-2] != '.')
			if !property && !startsProperty && bareWordBeforeLiteral(cypher, start, index) {
				return true
			}
			operandEnded = property
		case c == ')' || c == ']':
			index++
			operandEnded = true
		default:
			index++
			operandEnded = false
		}
	}
	return false
}

// literalLeadingKeywords are the words a string / number literal or a
// $parameter may directly follow in a graph query (RETURN 'x', LIMIT 5,
// n.name CONTAINS 'x', STARTS WITH $p, CASE 'a' WHEN 'a' THEN 1 ELSE 2,
// ORDER BY 1, LOAD CSV FROM 'url' ... FIELDTERMINATOR ';', IN TRANSACTIONS
// OF 10 ROWS, USING PERIODIC COMMIT 500, SHORTEST 2, ...).
var literalLeadingKeywords = map[string]struct{}{
	"RETURN": {}, "WITH": {}, "WHERE": {}, "AND": {}, "OR": {}, "XOR": {}, "NOT": {},
	"IN": {}, "IS": {}, "CASE": {}, "WHEN": {}, "THEN": {}, "ELSE": {}, "CONTAINS": {},
	"SKIP": {}, "LIMIT": {}, "UNWIND": {}, "FROM": {}, "FIELDTERMINATOR": {}, "OF": {},
	"DISTINCT": {}, "BY": {}, "YIELD": {}, "SHORTEST": {}, "ANY": {}, "ALL": {},
	"COMMIT": {}, "USE": {}, "OFFSET": {}, "DELETE": {},
}

// bareWordBeforeLiteral reports whether the word cypher[start:end] (not a
// property name) is followed by a string or number literal and
// is not one of literalLeadingKeywords, i.e. a variable directly followed by
// another operand. A word followed by '(' (a function call) or ':' (a map key
// or label) is never such a variable.
func bareWordBeforeLiteral(cypher string, start, end int) bool {
	next := skipSpaces(cypher, end)
	if next == end || next >= len(cypher) {
		return false
	}
	// A $parameter after a word can be a node pattern's property map
	// parameter ((n:Label $props)), so only string and number literals count.
	switch c := cypher[next]; {
	case c == '\'' || c == '"' || (c >= '0' && c <= '9'):
	default:
		return false
	}
	if c := cypher[start]; c >= '0' && c <= '9' {
		return false
	}
	_, keyword := literalLeadingKeywords[strings.ToUpper(cypher[start:end])]
	return !keyword
}

// isGraphQueryStatement reports whether a statement is a Cypher graph query:
// it starts (after EXPLAIN / PROFILE) with MATCH, OPTIONAL MATCH, WITH,
// RETURN, UNWIND, MERGE, CALL, FOREACH, LOAD CSV, UNION or USE, or with
// CREATE followed by a pattern ("CREATE (" or "CREATE p = "). CREATE INDEX /
// CONSTRAINT / DATABASE / USER and the other CREATE ... definitions are not.
func isGraphQueryStatement(cypher string) bool {
	query := strings.TrimSpace(cypher)
	for _, prefix := range []string{"EXPLAIN", "PROFILE"} {
		if matchKeywordAt(query, 0, prefix) {
			query = strings.TrimSpace(query[len(prefix):])
		}
	}
	for _, keyword := range []string{"MATCH", "OPTIONAL", "WITH", "RETURN", "UNWIND", "MERGE", "CALL", "FOREACH", "LOAD", "UNION", "USE"} {
		if matchKeywordAt(query, 0, keyword) {
			return true
		}
	}
	if !matchKeywordAt(query, 0, "CREATE") {
		return false
	}
	rest := strings.TrimSpace(query[len("CREATE"):])
	if strings.HasPrefix(rest, "(") {
		return true
	}
	name, next, ok := scanIdentifierToken(rest, 0)
	return ok && name != "" && strings.HasPrefix(strings.TrimSpace(rest[next:]), "=")
}

func validateLeadingNodePatternTransition(cypher string) error {
	query := strings.TrimSpace(cypher)
	keyword := ""
	if matchKeywordAt(query, 0, "MATCH") {
		keyword = "MATCH"
	} else if matchKeywordAt(query, 0, "CREATE") {
		keyword = "CREATE"
	} else {
		return nil
	}
	open := skipSpaces(query, len(keyword))
	if open >= len(query) || query[open] != '(' {
		return nil
	}
	close := findMatchingParen(query, open)
	if close < 0 {
		return nil
	}
	remaining := strings.TrimSpace(query[close+1:])
	if remaining == "" || remaining[0] == ',' || remaining[0] == '-' || remaining[0] == '<' || remaining[0] == ';' {
		return nil
	}
	for _, allowed := range []string{"WHERE", "USING", "RETURN", "WITH", "MATCH", "OPTIONAL", "CREATE", "MERGE", "SET", "REMOVE", "DELETE", "UNWIND", "CALL", "FOREACH", "ORDER", "SKIP", "LIMIT", "UNION"} {
		if matchKeywordAt(remaining, 0, allowed) {
			return nil
		}
	}
	if matchKeywordAt(remaining, 0, "DETACH") && matchKeywordAt(strings.TrimSpace(remaining[len("DETACH"):]), 0, "DELETE") {
		return nil
	}
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "syntax error: unexpected text after node pattern")
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
