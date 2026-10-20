package cypher

// Pipeline mutation owns row-scoped DELETE, SET, CREATE, MERGE and FOREACH writes.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/pkg/util"
)

// pipelineApplyDelete collects every entity target before validation and
// mutation, preserving statement atomicity while retaining input rows for
// subsequent WITH and RETURN clauses.
func (e *StorageExecutor) pipelineApplyDelete(ctx context.Context, rows []pipelineRow, scope map[string]struct{}, clause string) (*QueryStats, bool, error) {
	targets, detach, ok := deleteClauseTargets(clause)
	if !ok || len(targets) == 0 {
		return nil, false, nil
	}
	for _, expression := range targets {
		if hasTopLevelDeleteLabelQualifier(expression) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidDelete",
				"DELETE accepts nodes, relationships, and paths, not labels or relationship types",
			)
		}
		references := expressionFreeVariables(expression)
		for _, name := range references {
			if _, bound := scope[name]; !bound {
				return nil, true, deleteUndefinedVariableError(name)
			}
		}
		// A row-dependent target is evaluated with its row below, never with
		// an empty planning scope (id(n) is not a constant).
		if len(references) > 0 {
			continue
		}
		if value, evaluated, err := e.evaluateRowValue(strings.TrimSpace(expression), pipelineRow{}); err != nil {
			return nil, true, err
		} else if evaluated && !isDeleteTargetValue(value) {
			return nil, true, newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
			)
		}
	}
	projected := &ExecuteResult{Columns: targets, Rows: make([][]interface{}, 0, len(rows))}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		values := make([]interface{}, 0, len(targets))
		for _, expression := range targets {
			value, ok, err := e.evaluateRowValue(strings.TrimSpace(expression), row)
			if err != nil {
				return nil, true, err
			}
			if !ok {
				return nil, true, deleteUndefinedVariableError(expression)
			}
			if !isDeleteTargetValue(value) {
				return nil, true, newSemanticError(
					"Neo.ClientError.Statement.TypeError",
					"InvalidArgumentType",
					fmt.Sprintf("DELETE expression %q does not evaluate to a node, relationship, or path", strings.TrimSpace(expression)),
				)
			}
			values = append(values, value)
		}
		projected.Rows = append(projected.Rows, values)
	}
	nodeIDs, edgeIDs := collectDeleteMutationTargets(projected)
	store := e.getStorage(ctx)
	// A node relationships still connect is deleted for now and checked at
	// commit, when the transaction can hold it (connectedNodeDeletes); a
	// store without transactions checks here.
	var connected map[storage.NodeID]struct{}
	if !detach {
		if _, deferred := store.(storage.ConnectedNodeDeleter); deferred {
			var err error
			if connected, err = connectedDeleteTargets(store, nodeIDs, edgeIDs); err != nil {
				return nil, true, err
			}
		} else if err := validateNoResidualRelationships(store, nodeIDs, edgeIDs); err != nil {
			return nil, true, err
		}
	}

	// Rows are deleted as they stream, so an entity an earlier row of the
	// statement deleted reaches storage again: it is already gone
	// (storage.ErrNotFound) and counts once, as in Neo4j (#827).
	deletedEdges := make(map[storage.EdgeID]struct{}, len(edgeIDs))
	for _, edgeID := range edgeIDs {
		if err := store.DeleteEdge(edgeID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, true, err
		}
		deletedEdges[edgeID] = struct{}{}
	}
	if detach {
		for _, nodeID := range nodeIDs {
			incident, err := connectingRelationships(store, nodeID)
			if err != nil {
				return nil, true, err
			}
			for _, edge := range incident {
				deletedEdges[edge.ID] = struct{}{}
			}
		}
	}
	stats := &QueryStats{RelationshipsDeleted: len(deletedEdges)}
	for _, nodeID := range nodeIDs {
		deleteNode := store.DeleteNode
		if _, stillConnected := connected[nodeID]; stillConnected {
			deleteNode = store.(storage.ConnectedNodeDeleter).DeleteConnectedNode
		}
		if err := deleteNode(nodeID); err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				continue
			}
			return nil, true, err
		}
		stats.NodesDeleted++
		e.removeNodeFromSearch(string(nodeID))
	}
	// Incident relationships of detached nodes were deleted with them.
	for edgeID := range deletedEdges {
		e.notifyEdgeMutated(string(edgeID))
	}
	deleted := deletedEntitiesOf(ctx)
	if deleted == nil {
		deleted = &deletedEntities{}
	}
	deleted.add(nodeIDs, deletedEdges)
	e.replaceDeletedEntityViews(rows, deleted)
	return stats, true, nil
}

// pipelineApplySet mutates entities already bound in each pipeline row. Scalar
// and map bindings are attached as typed context values so assignments such as
// SET target = row retain their original Go/Cypher types.
func (e *StorageExecutor) pipelineApplySet(ctx context.Context, rows []pipelineRow, clause string) (*QueryStats, bool, error) {
	// body may chain SET clauses (SET a SET b); the applicators keep their
	// boundaries, which properties_set depends on (setWrites).
	body := strings.TrimSpace(clause[len("SET"):])
	assignments := splitSetAssignments(collapseChainedSetClauses(body))
	if body == "" || len(assignments) == 0 {
		return nil, false, nil
	}

	store := e.getStorage(ctx)
	stats := &QueryStats{}
	if err := validatePipelineSetAssignments(assignments); err != nil {
		return nil, true, err
	}
	simpleTarget, simpleProperty, simpleExpression, simplePropertyAssignment := pipelineSimplePropertyAssignment(assignments)
	targets := pipelineSetTargetVariables(assignments)
	var runBuffer [4]setRun
	runs := appendSetClauseRuns(runBuffer[:0], body, assignments)
	var stateBuffer [4]setEntityState
	wrapSetError := func(state setEntityState, err error) error {
		return fmt.Errorf("SET %s: %w", pipelineSetOperation(state.variable, assignments), err)
	}
	if len(targets) == 0 {
		return nil, false, nil
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		for _, target := range targets {
			value, bound := row[target]
			if !bound || value == nil {
				continue
			}
			switch value.(type) {
			case *storage.Node, *storage.Edge:
			default:
				return nil, true, newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidType", fmt.Sprintf("Type mismatch: expected Node or Relationship but was %s", cypherTypeName(value)))
			}
		}
	}
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, true, err
		}
		rowCtx, nodes, rels := e.pipelineRowWriteContext(ctx, row)
		evalNodes := nodes
		if simplePropertyAssignment {
			if node := nodes[simpleTarget]; node != nil {
				value, err := e.setPropertyValue(rowCtx, simpleExpression, evalNodes, rels)
				if err != nil {
					return nil, true, err
				}
				before := cloneStringAnyMap(node.Properties)
				written := simplePropertyWrites(node.Properties, simpleProperty, value)
				setNodeProperty(node, simpleProperty, value)
				if err := e.persistSetEntities(store, []setEntityState{{variable: simpleTarget, node: node, properties: before, labels: node.Labels, written: written}}, stats, wrapSetError); err != nil {
					return nil, true, err
				}
				continue
			}
			if relationship := rels[simpleTarget]; relationship != nil {
				value, err := e.setPropertyValue(rowCtx, simpleExpression, evalNodes, rels)
				if err != nil {
					return nil, true, err
				}
				before := cloneStringAnyMap(relationship.Properties)
				written := simplePropertyWrites(relationship.Properties, simpleProperty, value)
				setRelationshipProperty(relationship, simpleProperty, value)
				if err := e.persistSetEntities(store, []setEntityState{{variable: simpleTarget, relationship: relationship, properties: before, written: written}}, stats, wrapSetError); err != nil {
					return nil, true, err
				}
				continue
			}
		}
		states, handled, err := e.applySetRuns(rowCtx, runs, evalNodes, rels, func(variable string) bool {
			_, bound := row[variable]
			return bound
		}, stateBuffer[:0])
		if !handled || err != nil {
			return nil, handled, err
		}
		if err := e.persistSetEntities(store, states, stats, wrapSetError); err != nil {
			return nil, true, err
		}
	}
	return stats, true, nil
}

// pipelineSimplePropertyAssignment recognizes the single x.p = <expr> SET
// (the common case) so pipelineApplySet can skip re-splitting per row.
func pipelineSimplePropertyAssignment(assignments []string) (target, property, expression string, ok bool) {
	if len(assignments) != 1 {
		return "", "", "", false
	}
	target, property, operator, expression := splitSetAssignment(assignments[0])
	if operator != "=" || property == "" || expression == "" {
		return "", "", "", false
	}
	return target, property, expression, true
}

// validatePipelineSetAssignments statically checks SET assignment shapes for
// every route (it runs from validateSetClauseScope before execution): x = v,
// x.p = v, x[key] = v, x += map and x:L1:$(expr) with a bound-identifier
// target, a non-empty right-hand side, a parseable inline map for += and a
// valid label chain (setLabelChainItems). The source type of x = and x += is checked after the
// variables (setSourceTypeError, validateSetClauseScope). Forms are split by
// splitSetAssignment, the splitter the applicators use.
func validatePipelineSetAssignments(assignments []string) error {
	for _, raw := range assignments {
		assignment := strings.TrimSpace(raw)
		if assignment == "" {
			return localizedError(localization.CypherMutationsSetAssignmentRequired(), nil)
		}
		target, _, operator, right := splitSetAssignment(assignment)
		if operator == "" || !isValidIdentifier(target) || right == "" {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid SET assignment: "+assignment)
		}
		if right == "$" {
			return localizedError(localization.CypherMutationsSetAssignmentParameterNameRequired(), nil)
		}
		switch operator {
		case "+=":
			if strings.HasPrefix(right, "{") {
				if _, err := parseSetMergeMapExpressionsStrict(right); err != nil {
					return localizedError(localization.CypherMutationsSetMergeParseFailed(err), err)
				}
			}
		case ":":
			// Static labels are checked here; a $(expr) item's value is read
			// at run time (chainLabelNames).
			if _, err := setLabelChainItems(right); err != nil {
				return err
			}
		}
	}
	return nil
}

func pipelineSetOperation(variable string, assignments []string) string {
	for _, assignment := range assignments {
		assignment = strings.TrimSpace(assignment)
		if strings.HasPrefix(assignment, variable+" +=") || strings.HasPrefix(assignment, variable+"+=") {
			return variable + " +="
		}
		if strings.HasPrefix(assignment, variable+" =") || strings.HasPrefix(assignment, variable+"=") {
			return variable + " ="
		}
		if strings.HasPrefix(assignment, variable+".") {
			return variable + ".property ="
		}
	}
	return variable
}

func addedLabelCount(before, after []string) int {
	known := make(map[string]struct{}, len(before))
	for _, label := range before {
		known[label] = struct{}{}
	}
	added := 0
	for _, label := range after {
		if _, exists := known[label]; !exists {
			added++
		}
	}
	return added
}

// pipelineSetTargetVariables lists the variables a SET clause writes, in
// first-appearance order, using the shared splitSetAssignment.
func pipelineSetTargetVariables(assignments []string) []string {
	seen := make(map[string]struct{})
	var targets []string
	for _, assignment := range assignments {
		target, _, operator, _ := splitSetAssignment(assignment)
		if operator == "" || !isValidIdentifier(target) {
			continue
		}
		if _, exists := seen[target]; exists {
			continue
		}
		seen[target] = struct{}{}
		targets = append(targets, target)
	}
	return targets
}

// changedPropertyCount is the SET counting rule: the number of properties
// added, replaced with a different value or removed between before and after.
func changedPropertyCount(before, after map[string]interface{}) int {
	changed := 0
	for key, value := range after {
		if old, exists := before[key]; !exists || !samePropertyValue(old, value) {
			changed++
		}
	}
	for key := range before {
		if _, exists := after[key]; !exists {
			changed++
		}
	}
	return changed
}

// samePropertyValue is reflect.DeepEqual for stored property values, with the
// common scalar types compared directly.
func samePropertyValue(a, b interface{}) bool {
	switch av := a.(type) {
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case int64:
		bv, ok := b.(int64)
		return ok && av == bv
	case float64:
		bv, ok := b.(float64)
		return ok && av == bv
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	}
	return reflect.DeepEqual(a, b)
}

func (e *StorageExecutor) tryExecutePipelineCreatePlan(ctx context.Context, clauses, originalClauses []pipelineClause) (*ExecuteResult, bool, error) {
	end := 0
	for end < len(clauses) && clauses[end].kind == pipelineClauseCreate {
		end++
	}
	if end == 0 {
		return nil, false, nil
	}
	for index := end; index < len(clauses); index++ {
		if clauses[index].kind != pipelineClauseSet &&
			!(clauses[index].kind == pipelineClauseReturn && index == len(clauses)-1) {
			return nil, false, nil
		}
	}
	initial := pipelineRow{}
	for name, value := range e.fabricRecordBindings {
		initial[name] = value
	}
	for name, value := range valueBindingsFromContext(ctx) {
		initial[name] = value
	}
	bindParameterRow(ctx, initial)
	rows, result, _, err := e.pipelineApplyCreateClauses(ctx, []pipelineRow{initial}, clauses[:end])
	if err != nil {
		return nil, true, err
	}
	for index := end; index < len(clauses); index++ {
		if clauses[index].kind == pipelineClauseSet {
			stats, _, err := e.pipelineApplySet(ctx, rows, clauses[index].text)
			if err != nil {
				return nil, true, err
			}
			addQueryStats(result.Stats, stats)
			continue
		}
		projected, err := e.projectMergeReturn(ctx, rows, originalClauses[index].text)
		if err != nil {
			return nil, true, err
		}
		result.Columns, result.Rows = projected.Columns, projected.Rows
	}
	return result, true, nil
}

type pipelineIndependentCreateBatchKey struct{}

func (e *StorageExecutor) pipelineApplyCreateClauses(ctx context.Context, rows []pipelineRow, clauses []pipelineClause) ([]pipelineRow, *ExecuteResult, bool, error) {
	created := &ExecuteResult{Stats: &QueryStats{}}
	if independent, _ := ctx.Value(pipelineIndependentCreateBatchKey{}).(bool); independent && len(rows) > 1 {
		return e.pipelineCreateSource(ctx, pipelineRowsSource(rows), clauses)
	}
	var out []pipelineRow
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, false, err
		}
		newRow, err := e.pipelineCreateRow(ctx, row, clauses, created)
		if err != nil {
			return nil, nil, true, err
		}
		out = append(out, newRow)
	}
	return out, created, true, nil
}

func (e *StorageExecutor) pipelineCreateRow(ctx context.Context, row pipelineRow, clauses []pipelineClause, created *ExecuteResult) (pipelineRow, error) {
	plan := acquireCreatePlan()
	defer plan.release()
	newRow, err := e.pipelinePlanCreateRow(ctx, row, clauses, plan)
	if err != nil {
		return nil, err
	}
	if err := e.applyCreatePlan(ctx, plan, created); err != nil {
		return nil, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
	}
	return newRow, nil
}

func (e *StorageExecutor) pipelinePlanCreateRow(ctx context.Context, row pipelineRow, clauses []pipelineClause, plan *createPlan) (pipelineRow, error) {
	nodes, edges := plan.bindings()
	newRow := make(pipelineRow, len(row))
	for name, value := range row {
		newRow[name] = value
		switch typed := value.(type) {
		case *storage.Node:
			if typed != nil {
				nodes[name] = typed
			}
		case *storage.Edge:
			if typed != nil {
				edges[name] = typed
			}
		}
	}
	for _, clause := range clauses {
		rowCtx := withValueBindings(ctx, newRow)
		pattern := pipelineClauseBody(clause.text, "CREATE")
		dynamic := hasDynamicToken(pattern)
		if dynamic {
			// Labels and types whose values depend on the row.
			resolved, err := e.resolveRowDynamicTokens(rowCtx, pattern, nodes, edges)
			if err != nil {
				return nil, err
			}
			pattern = resolved
		}
		var paths map[string]PathResult
		planned := false
		if !dynamic {
			// Parse-once template: the clause's pattern structure is fixed
			// per statement; only the property expressions are per-row. The
			// template is parsed from the clause text, so a clause with
			// row-dependent dynamic labels or types keeps the general route,
			// which plans the resolved pattern instead.
			if template := e.pipelineCreateTemplateFor(clause.text); template != nil && template.usable {
				var err error
				paths, planned, err = template.plan(ctx, e, newRow, nodes, edges, plan)
				if err != nil {
					if failure := getExpressionFailure(rowCtx); failure != nil {
						return nil, failure
					}
					return nil, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
				}
			}
		}
		if !planned {
			var err error
			paths, err = e.planCreatePatterns(rowCtx, pattern, nodes, edges, plan)
			if err != nil {
				if failure := getExpressionFailure(rowCtx); failure != nil {
					return nil, failure
				}
				return nil, localizedError(localization.CypherInvariantsPipelineCreateFailed(err), err)
			}
		}
		for name, node := range nodes {
			newRow[name] = node
		}
		for name, edge := range edges {
			newRow[name] = edge
		}
		for name, path := range paths {
			newRow[name] = e.pathToMap(path)
		}
	}
	return newRow, nil
}

// pipelineApplyMerge executes one MERGE per input row while retaining the row
// bindings for subsequent clauses. This preserves Cypher's row-at-a-time
// mutation semantics after UNWIND/WITH without duplicating MERGE behavior.
func (e *StorageExecutor) pipelineApplyMerge(ctx context.Context, rows []pipelineRow, clause string) ([]pipelineRow, *QueryStats, error) {
	if body := strings.TrimSpace(clause); hasDynamicToken(body) && startsWithKeywordFold(body, "MERGE") {
		if actions := splitMergeClauseActions(strings.TrimSpace(body[len("MERGE"):])); hasDynamicToken(actions.pattern) {
			return e.pipelineApplyRowDynamicMerge(ctx, rows, clause, actions.pattern)
		}
	}
	stats := &QueryStats{}
	out := make([]pipelineRow, 0, len(rows))
	// The relationship lookups of this MERGE over its rows share one read
	// per endpoint pair (relationshipMergeIdentityCache); one row looks up
	// directly.
	var identities *relationshipMergeIdentityCache
	if len(rows) > 1 {
		identities = newRelationshipMergeIdentityCache()
	}
	template := e.pipelineMergeTemplateFor(clause)
	// The clause's pattern and actions (each action clause a SET of its own,
	// in order) are the same for every row.
	clauseBody := strings.TrimSpace(clause)
	isMerge := startsWithKeywordFold(clauseBody, "MERGE")
	var clausePattern, onCreateSet, onMatchSet string
	multiRelationship := false
	if isMerge {
		parts := splitMergeClauseActions(strings.TrimSpace(clauseBody[len("MERGE"):]))
		clausePattern, onCreateSet, onMatchSet = parts.pattern, parts.onCreate.setText(), parts.onMatch.setText()
		multiRelationship = e.isMultiRelationshipPattern(clausePattern)
	}
	// Templates contain syntax only; row values stay in each prepared context.
	// Reuse the statement template until preparation changes that syntax.
	preparedTemplate, templatePattern := template, clausePattern
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		ctx := withValueBindings(ctx, row)
		preparedPattern := clausePattern
		rowTemplate := template
		if isMerge && (!containsRelationshipPattern(clausePattern) || template.usable && pipelineBoundMergeEndpoints(template, row)) {
			var prepareErr error
			ctx, preparedPattern, prepareErr = e.preparePatternRow(ctx, clausePattern, row)
			if prepareErr != nil {
				return nil, nil, prepareErr
			}
			if template.usable {
				if preparedPattern != templatePattern {
					preparedTemplate = e.pipelineMergeTemplateFor("MERGE " + preparedPattern)
					templatePattern = preparedPattern
				}
				rowTemplate = preparedTemplate
			}
		}
		// The clause parsed once: a row whose relationship already exists
		// is its matches, without rendering and parsing the row's text.
		pattern, startNode, endNode, templated, templateErr := rowTemplate.pattern(ctx, e, row)
		if templateErr != nil {
			return nil, nil, templateErr
		}
		if templated {
			matches, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, pattern, startNode, endNode)
			if findErr != nil {
				return nil, nil, findErr
			}
			if len(matches) > 0 {
				out = append(out, e.mergeRelationshipRows(row, nil, nil, pattern, startNode, endNode, matches)...)
				continue
			}
		}
		nodeContext := make(map[string]*storage.Node)
		relContext := make(map[string]*storage.Edge)
		for name, value := range row {
			switch typed := value.(type) {
			case *storage.Node:
				nodeContext[name] = typed
			case *storage.Edge:
				relContext[name] = typed
			}
		}
		var relationshipPattern *mergeRelationshipPattern
		var nodePathVariable string
		var nodePathBinding string
		mergePattern := clauseBody
		if isMerge {
			mergePattern = preparedPattern
			// A whole-path MERGE: the pattern's matches with the row's
			// bindings, every one a row, or the whole pattern created,
			// reusing the bound nodes, as in Neo4j. Every relationship
			// pattern with an endpoint the row doesn't bind takes it (#509:
			// two matching paths are two rows); one with both endpoints
			// bound keeps the relationship route below.
			mergeWholePath := func() error {
				produced, pathStats, created, err := e.pipelineMergePath(ctx, row, mergePattern, nodeContext, relContext)
				if err != nil {
					return err
				}
				addQueryStats(stats, pathStats)
				actions := onMatchSet
				if created {
					actions = onCreateSet
				}
				if err := e.applyMergeActions(ctx, produced, actions, stats); err != nil {
					return err
				}
				// It may have written between any pair of nodes.
				identities.reset()
				out = append(out, produced...)
				return nil
			}
			if multiRelationship {
				if err := mergeWholePath(); err != nil {
					return nil, nil, err
				}
				continue
			}
			if open, _ := firstRelationshipBracket(mergePattern); open >= 0 {
				shape, shapeErr := e.parseMergeRelationshipShape(mergePattern)
				if shapeErr != nil {
					return nil, nil, shapeErr
				}
				start := nodePatternStructure(shape.startContent).variable
				end := nodePatternStructure(shape.endContent).variable
				for _, variable := range [...]string{start, end} {
					if missingRelationshipEndpoint(row, variable) {
						return nil, nil, relationshipEndpointMissingError(variable)
					}
				}
				if nodeContext[start] == nil || nodeContext[end] == nil {
					if err := mergeWholePath(); err != nil {
						return nil, nil, err
					}
					continue
				}
				var parseErr error
				ctx, mergePattern, parseErr = e.preparePatternRow(ctx, mergePattern, row)
				if parseErr != nil {
					return nil, nil, parseErr
				}
				relationshipPattern, parseErr = e.parseMergeRelationshipPattern(ctx, mergePattern, nodeContext, relContext)
				if parseErr != nil {
					return nil, nil, parseErr
				}
			} else if nodePathVariable = extractPathAssignmentVariable(mergePattern); nodePathVariable != "" {
				mergePattern = strings.TrimSpace(mergePattern[strings.Index(mergePattern, "=")+1:])
				nodePathBinding = e.extractVarName(mergePattern)
			}
		}
		if relationshipPattern == nil {
			nodePattern := mergePattern
			variable, labels, properties, parseErr := e.parseMergeNodePattern(ctx, nodePattern, nodeContext, relContext)
			if parseErr == nil {
				_, alreadyBound := nodeContext[variable]
				if variable == "" || !alreadyBound {
					if err := prepareMergeKeys(ctx, e.getStorage(ctx), labels, properties); err != nil {
						return nil, nil, err
					}
					matches, scanned, findErr := e.findMergeNodesScanned(e.getStorage(ctx), labels, properties)
					if findErr != nil {
						return nil, nil, findErr
					}
					if len(matches) > 0 {
						matchedRows := make([]pipelineRow, 0, len(matches))
						for _, node := range matches {
							expanded := make(pipelineRow, util.SafePreallocSum(len(row), 2))
							for name, value := range row {
								expanded[name] = value
							}
							if variable != "" {
								expanded[variable] = node
							}
							if nodePathVariable != "" {
								path := PathResult{Nodes: []*storage.Node{node}}
								expanded[nodePathVariable] = e.pathToMap(path)
							}
							matchedRows = append(matchedRows, expanded)
						}
						// ON MATCH SET applies to every matched row, through
						// the shared SET applicator.
						if err := e.applyMergeActions(ctx, matchedRows, onMatchSet, stats); err != nil {
							return nil, nil, err
						}
						out = append(out, matchedRows...)
						continue
					}
					if scanned {
						ctx = withMergeNodeAbsent(ctx)
					}
				}
			}
		}
		if relationshipPattern != nil {
			// Bound endpoints: every existing relationship that matches the
			// pattern is a row, and ON MATCH SET applies to each, as in
			// Neo4j. One lookup decides; only a pattern with no match reaches
			// the create path below.
			startNode := nodeContext[relationshipPattern.startVariable]
			endNode := nodeContext[relationshipPattern.endVariable]
			if startNode != nil && endNode != nil {
				matches, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, relationshipPattern, startNode, endNode)
				if findErr != nil {
					return nil, nil, findErr
				}
				if len(matches) > 0 {
					matchedRows := e.mergeRelationshipRows(row, nodeContext, relContext, relationshipPattern, startNode, endNode, matches)
					if onMatchSet != "" {
						// It can change identity values.
						identities.reset()
					}
					if err := e.applyMergeActions(ctx, matchedRows, onMatchSet, stats); err != nil {
						return nil, nil, err
					}
					out = append(out, matchedRows...)
					continue
				}
			}
		}
		boundEndpoints := relationshipPattern != nil &&
			nodeContext[relationshipPattern.startVariable] != nil && nodeContext[relationshipPattern.endVariable] != nil
		merged, err := e.executeMergeWithContext(ctx, "MERGE "+mergePattern, nodeContext, relContext)
		if err != nil {
			return nil, nil, err
		}
		if relationshipPattern != nil && !boundEndpoints {
			// The MERGE bound or created endpoints itself: it may have
			// written between any pair.
			identities.reset()
		}
		patternCreated := false
		if merged != nil && merged.Stats != nil {
			addQueryStats(stats, merged.Stats)
			patternCreated = merged.Stats.NodesCreated > 0 || merged.Stats.RelationshipsCreated > 0
		}
		// The MERGE created its pattern (ON CREATE) or found it (ON MATCH).
		actions := onMatchSet
		if patternCreated {
			actions = onCreateSet
		}
		var produced []pipelineRow
		newRow := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeContext), len(relContext)))
		for name, value := range row {
			newRow[name] = value
		}
		for name, node := range nodeContext {
			newRow[name] = node
		}
		for name, relationship := range relContext {
			newRow[name] = relationship
		}
		if nodePathVariable != "" {
			if node := nodeContext[nodePathBinding]; node != nil {
				path := PathResult{Nodes: []*storage.Node{node}}
				newRow[nodePathVariable] = e.pathToMap(path)
			}
		}
		if relationshipPattern != nil {
			// The create path made the relationship (and any endpoint the
			// pattern didn't bind): its row carries it.
			startNode := nodeContext[relationshipPattern.startVariable]
			endNode := nodeContext[relationshipPattern.endVariable]
			if startNode != nil && endNode != nil {
				created := relContext[relationshipPattern.relVariable]
				var matches []*storage.Edge
				if created != nil && relationshipPattern.relVariable != "" {
					matches = []*storage.Edge{created}
					if onCreateSet == "" {
						identities.created(created)
					} else {
						identities.reset()
					}
				} else {
					identities.reset()
					found, findErr := findParsedMergeRelationships(e.getStorage(ctx), identities, relationshipPattern, startNode, endNode)
					if findErr != nil {
						return nil, nil, findErr
					}
					matches = found
				}
				if len(matches) > 0 {
					produced = e.mergeRelationshipRows(newRow, nil, nil, relationshipPattern, startNode, endNode, matches)
				}
			}
		}
		if produced == nil {
			produced = []pipelineRow{newRow}
		}
		if actions != "" {
			// It can change identity values.
			identities.reset()
		}
		if err := e.applyMergeActions(ctx, produced, actions, stats); err != nil {
			return nil, nil, err
		}
		out = append(out, produced...)
	}
	return out, stats, nil
}

// pipelineApplyRowDynamicMerge applies a MERGE whose pattern has labels or
// types whose values depend on the row: each row merges the pattern with
// them resolved (resolveRowDynamicTokens), as a MERGE of its own, in order,
// so a row sees what the rows before it merged.
func (e *StorageExecutor) pipelineApplyRowDynamicMerge(ctx context.Context, rows []pipelineRow, clause, pattern string) ([]pipelineRow, *QueryStats, error) {
	stats := &QueryStats{}
	out := make([]pipelineRow, 0, len(rows))
	at := strings.Index(clause, pattern)
	for _, row := range rows {
		rowCtx, nodes, rels := e.pipelineRowWriteContext(ctx, row)
		resolved, err := e.resolveRowDynamicTokens(rowCtx, pattern, nodes, rels)
		if err != nil {
			return nil, nil, err
		}
		produced, rowStats, err := e.pipelineApplyMerge(ctx, []pipelineRow{row}, clause[:at]+resolved+clause[at+len(pattern):])
		if err != nil {
			return nil, nil, err
		}
		addQueryStats(stats, rowStats)
		out = append(out, produced...)
	}
	return out, stats, nil
}

// mergeRelationshipRows returns one row per relationship a MERGE matched or
// created: row with the MERGE's node and relationship bindings, the pattern's
// relationship variable and its path variable.
func (e *StorageExecutor) mergeRelationshipRows(row pipelineRow, nodeContext map[string]*storage.Node, relContext map[string]*storage.Edge, pattern *mergeRelationshipPattern, startNode, endNode *storage.Node, relationships []*storage.Edge) []pipelineRow {
	rows := make([]pipelineRow, 0, len(relationships))
	for _, relationship := range relationships {
		expanded := make(pipelineRow, util.SafePreallocSum(len(row), len(nodeContext), len(relContext), 2))
		for name, value := range row {
			expanded[name] = value
		}
		for name, node := range nodeContext {
			expanded[name] = node
		}
		for name, edge := range relContext {
			expanded[name] = edge
		}
		if pattern.relVariable != "" {
			expanded[pattern.relVariable] = relationship
		}
		if pattern.pathVariable != "" {
			path := PathResult{Nodes: []*storage.Node{startNode, endNode}, Relationships: []*storage.Edge{relationship}, Length: 1}
			expanded[pattern.pathVariable] = e.pathToMap(path)
		}
		rows = append(rows, expanded)
	}
	return rows
}

// containsRemoveClauseAnywhere reports a REMOVE keyword outside strings,
// including one nested in a FOREACH body or subquery.
func containsRemoveClauseAnywhere(cypher string) bool {
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	return keywordIndexFrom(cypher, "REMOVE", 0, opts) >= 0
}

func (e *StorageExecutor) pipelineApplyForeach(ctx context.Context, rows []pipelineRow, clause string) (*QueryStats, error) {
	invalid := func() error {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidForeach", "invalid or unsupported FOREACH update")
	}
	variable, listExpr, updates, err := parsePipelineForeach(clause)
	if err != nil {
		return nil, err
	}
	stats := &QueryStats{}
	for _, row := range rows {
		items, ok := e.evaluateListForPipelineWithContext(ctx, listExpr, row)
		if !ok {
			return nil, invalid()
		}
		for _, item := range items {
			// The loop is a mutation boundary: a cancellation between items
			// stops the remaining writes (the MERGE branch probes too).
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			child := make(pipelineRow, len(row)+1)
			for name, value := range row {
				child[name] = value
			}
			child[variable] = item
			scope := make(map[string]struct{}, len(child))
			for name := range child {
				scope[name] = struct{}{}
			}
			result, handled, err := e.runPipelineClauseRows(ctx, []pipelineRow{child}, scope, updates, updates, nil)
			if err != nil {
				return nil, err
			}
			if !handled {
				return nil, invalid()
			}
			addQueryStats(stats, result.Stats)
		}
	}
	return stats, nil
}

// setSourceTypeError rejects SET x = <source> and SET x += <source>
// (property "") whose source has a static type other than a map, node or
// relationship: a literal, or a variable or parameter whose type types /
// params give (propertyAccessExpressionType, the owner of static expression
// types). Neo4j 5.26 types it before the statement runs ("Type mismatch:
// expected Map, Node or Relationship"). A source of unknown type, null
// included, is checked when the SET runs (setPropertyMapValue), as in Neo4j
// (#907).
func setSourceTypeError(property, operator, source string, types map[string]string, params map[string]interface{}) error {
	if property != "" || (operator != "=" && operator != "+=") {
		return nil
	}
	switch typeName := propertyAccessExpressionType(source, types, params); typeName {
	case "", "Map", "Node", "Relationship":
		return nil
	default:
		return typeNameMismatchError("Map, Node or Relationship", typeName)
	}
}

// setClauseSourceTypeError is setSourceTypeError for the assignments of a
// SET clause whose source is a variable or parameter, with the static types
// of the variables in scope. Literal sources are checked with each item's
// scope check (validateSetClauseScope), in item order, as Neo4j reports them.
func setClauseSourceTypeError(clause string, types map[string]string, params map[string]interface{}) error {
	body := strings.TrimSpace(clause)
	if startsWithKeywordFold(body, "SET") {
		body = body[len("SET"):]
	}
	for _, assignment := range splitSetAssignments(body) {
		_, property, operator, source := splitSetAssignment(assignment)
		if staticLiteralTypeName(source) != "" {
			continue
		}
		if err := setSourceTypeError(property, operator, source, types, params); err != nil {
			return err
		}
	}
	return nil
}
