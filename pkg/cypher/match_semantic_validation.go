package cypher

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

type matchBindingKind uint8

const (
	matchBindingUnknown matchBindingKind = iota
	matchBindingNode
	matchBindingRelationship
	matchBindingRelationshipList
	matchBindingNodeList
	matchBindingPath
	matchBindingValue
)

type matchSemanticScope map[string]matchBindingKind

// validateMatchSemanticScopes applies entity-type rules before physical query
// routing. MATCH variables may be reused only when their binding kind remains
// stable; a node name cannot already denote a relationship, path, or scalar.
// A statement that passes is cached by its text and language version,
// unless a Fabric APPLY binds variables for it: then the result depends on
// the bound record. cypher25 holds a Cypher 25 statement to Neo4j 2026.09's
// compile-time type rules (staticTypeScope.cypher25).
func (e *StorageExecutor) validateMatchSemanticScopes(cypher string, cypher25 bool) error {
	cacheable := len(e.fabricRecordBindings) == 0
	key := semanticCacheKey(cypher, cypher25)
	if cacheable && e.matchSemanticValidationCache.contains(key) {
		return nil
	}
	if err := checkedWithoutUnaryPlus(cypher, cypher25, func(text string) error {
		return e.validateMatchSemanticScopesUncached(text, cypher25)
	}); err != nil {
		return err
	}
	if cacheable {
		e.matchSemanticValidationCache.add(key)
	}
	return nil
}

// semanticCacheKey is a validation cache's key for a statement: its text,
// marked for a Cypher 25 statement, whose checks differ.
func semanticCacheKey(cypher string, cypher25 bool) string {
	if cypher25 {
		return "cypher25:" + cypher
	}
	return cypher
}

func (e *StorageExecutor) validateMatchSemanticScopesUncached(cypher string, cypher25 bool) error {
	if branches, _, _, ok := parseTopLevelUnionBranches(cypher); ok && len(branches) > 1 {
		for _, branch := range branches {
			if err := e.validateMatchSemanticScopes(branch, cypher25); err != nil {
				return err
			}
		}
		return nil
	}

	if isSchemaCommandStatement(cypher) {
		return nil
	}
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(cypher)
	if !ok {
		return nil
	}
	if err := validateQueryBodyComposition(cypher); err != nil {
		return err
	}
	// types is the one static type of every variable (#1055).
	types := make(typeScope, len(e.fabricRecordBindings))
	// A Fabric APPLY binds its input record's variables for the statement.
	for name := range e.fabricRecordBindings {
		types[name] = staticOperand{}
	}
	_, _, err := e.semanticScopeWalk(clauses, types, cypher25, subqueryScope{})
	return err
}

// subqueryScope is what a subquery body's walk (semanticScopeWalk) sees of
// the query around it: visible, the variables the whole body reads (after a
// WITH in it too), and outer, whether declaring one of them again with
// WITH … AS or UNWIND … AS is an error (redeclarationError). The zero value
// is a statement's own walk.
type subqueryScope struct {
	visible typeScope
	// outer: an EXISTS / COUNT / COLLECT body, which may not declare the
	// enclosing query's variables again. A CALL (…) body may declare its
	// imports again, as a top-level WITH may: the new value holds in the
	// body and the enclosing query keeps its own. Neo4j 5.26.30 rejects
	// that ("is shadowing an imported variable"); NornicDB keeps it as an
	// extension (#907).
	outer bool
}

// callBodyReturnStarError is Neo4j's error for RETURN * in a CALL
// subquery's body that declares no variable of its own: the variables it
// imports (CALL (*), CALL (x), or NornicDB's implicit imports of a CALL
// { … }) are the enclosing query's, so RETURN * has none to return, even
// beside other items (RETURN *, 2 AS y). nil outside a CALL body, or when
// the body binds a variable (WITH 1 AS y, UNWIND, MATCH (n)).
func callBodyReturnStarError(scope matchSemanticScope, clause string, enclosing subqueryScope) error {
	if enclosing.outer || enclosing.visible == nil {
		return nil
	}
	if !returnsStar(clause) {
		return nil
	}
	for name := range scope {
		if _, imported := enclosing.visible[name]; !imported {
			return nil
		}
	}
	return &classifiedCypherError{
		cause:  localizedError(localization.CypherMatchingReturnStarNoVariables(), nil),
		code:   "Neo.ClientError.Statement.SyntaxError",
		detail: "NoVariablesInScope",
	}
}

// validateQueryBodyComposition is Neo4j 5.26's rules for what a query body
// holds, applied to every body in cypher, at any depth and in any container
// (forEachQueryBody). An EXISTS, COUNT or COLLECT body holds no update
// clause, in a CALL body nested in it too ("A Count Expression cannot
// contain any updates"). Each UNION branch of a CALL body ends with RETURN,
// FINISH, an update clause, a unit subquery call (one whose body returns
// nothing) or a procedure call. A branch ending with a CALL that returns
// columns, or with MATCH, WITH or UNWIND, is "Query cannot conclude with
// CALL" (CALL { CALL { RETURN 'x' AS y } } RETURN *), also inside a body
// that ends with FINISH or inside EXISTS, COUNT and COLLECT, whose own
// bodies conclude as their forms do. It reads the text, so an old-style body
// (whose walk errors NornicDB's implicit imports keep) is held to it too;
// EXPLAIN reads it as well. A statement ending with a CALL that returns
// columns stays NornicDB's extension.
func validateQueryBodyComposition(cypher string) error {
	return forEachQueryBody(cypher, queryBodyContainerKeywords[:], func(container string, start, end int) error {
		if container != "CALL" {
			if queryBodyUpdates(cypher, start, end) {
				return &classifiedCypherError{
					cause:  localizedError(expressionBodyUpdatesMessages[container](), nil),
					code:   "Neo.ClientError.Statement.SyntaxError",
					detail: "InvalidClauseComposition",
				}
			}
			return nil
		}
		for _, branch := range readQueryBody(cypher, start, end).branches {
			if keyword := unconcludedClause(cypher, branch); keyword != "" {
				return &classifiedCypherError{
					cause:  localizedError(localization.CypherMatchingQueryCannotConclude(keyword), nil),
					code:   "Neo.ClientError.Statement.SyntaxError",
					detail: "InvalidClauseComposition",
				}
			}
		}
		return nil
	})
}

// expressionBodyUpdatesMessages is the error for an update in each
// expression container's body.
var expressionBodyUpdatesMessages = map[string]func() localization.Message{
	"EXISTS":  localization.CypherMatchingExistsExpressionUpdates,
	"COUNT":   localization.CypherMatchingCountExpressionUpdates,
	"COLLECT": localization.CypherMatchingCollectExpressionUpdates,
}

// updatingClauses are the clause kinds that update the graph.
var updatingClauses = map[pipelineClauseKind]bool{
	pipelineClauseCreate: true, pipelineClauseMerge: true, pipelineClauseDelete: true,
	pipelineClauseSet: true, pipelineClauseRemove: true, pipelineClauseForeach: true,
}

// queryBodyUpdates reports whether the query body source[start:end] holds
// an update clause, in any branch (readQueryBody) or in a CALL body at any
// depth in it. A declined branch holds no clause it can read.
func queryBodyUpdates(source string, start, end int) bool {
	for _, branch := range readQueryBody(source, start, end).branches {
		for _, clause := range branch.clauses {
			if updatingClauses[clause.kind] {
				return true
			}
			if from, to, ok := callClauseBody(clause); ok && queryBodyUpdates(source, from, to) {
				return true
			}
		}
	}
	return false
}

// queryBodyEndings is how each clause kind ends a query body: the keyword
// Neo4j names when it can't ("Query cannot conclude with MATCH"), or "" when
// it concludes the body (RETURN, an update clause, a procedure call). A CALL
// subquery concludes only as a unit subquery (callSubqueryReturnsColumns).
// Every clause kind is listed (TestQueryBodyEndingsCoverEveryClauseKind).
var queryBodyEndings = map[pipelineClauseKind]string{
	pipelineClauseMatch: "MATCH", pipelineClauseOptionalMatch: "MATCH", pipelineClauseWith: "WITH",
	pipelineClauseUnwind: "UNWIND", pipelineClauseLet: "LET", pipelineClauseFilter: "FILTER",
	pipelineClauseCallSubquery: "CALL",
	pipelineClauseReturn:       "", pipelineClauseCreate: "", pipelineClauseMerge: "", pipelineClauseDelete: "",
	pipelineClauseSet: "", pipelineClauseRemove: "", pipelineClauseForeach: "", pipelineClauseCall: "",
}

// unconcludedClause is the keyword of a query body branch's last clause when
// it can't conclude the body (queryBodyEndings), or "" when it can (FINISH
// too) or the reader declined it. source is the text the branch was read
// from.
func unconcludedClause(source string, branch queryBodyBranch) string {
	if branch.finished || len(branch.clauses) == 0 {
		return ""
	}
	last := branch.clauses[len(branch.clauses)-1]
	if last.kind == pipelineClauseCallSubquery && !callSubqueryReturnsColumns(source, last) {
		return ""
	}
	return queryBodyEndings[last.kind]
}

// callSubqueryReturnsColumns reports whether a CALL subquery clause returns
// columns: a branch of its body, as written, ends with RETURN. source is
// the text the clause was read from.
func callSubqueryReturnsColumns(source string, clause queryBodyClause) bool {
	start, end, ok := callClauseBody(clause)
	if !ok {
		return false
	}
	for _, branch := range readQueryBody(source, start, end).branches {
		if !branch.finished && len(branch.clauses) > 0 && branch.clauses[len(branch.clauses)-1].kind == pipelineClauseReturn {
			return true
		}
	}
	return false
}

// returnsStar reads the canonical projection fact, including comments and DISTINCT.
func returnsStar(clause string) bool {
	node, found := canonicalQueryClause(clause)
	return found && projectionPlanFromSyntax(node).star
}

// redeclarationError is the error for a WITH … AS or UNWIND … AS that
// declares name again in an expression subquery's body that sees it from
// the enclosing query: Neo4j's "is shadowing a variable with the same name
// from the outer scope". nil when the body may.
func (enclosing subqueryScope) redeclarationError(name string) error {
	if !enclosing.outer {
		return nil
	}
	if _, visible := enclosing.visible[name]; !visible {
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
		localization.CypherCoreVariableDeclaredInOuterScope(name))
}

// semanticScopeWalk checks clauses in order against the variables' static
// types (types, the one type scope of #1055), updating them clause by
// clause, and returns them after the last clause; complete is false when the
// walk stopped before the end because a clause (YIELD *, RETURN *) names
// nothing statically. A CALL subquery's returned columns are typed by
// walking its body the same way.
// enclosing, for a subquery body, is what it sees of the query around it
// (subqueryScope).
func (e *StorageExecutor) semanticScopeWalk(clauses []pipelineClause, types typeScope, cypher25 bool, enclosing subqueryScope) (typeScope, bool, error) {
	var scope matchSemanticScope
	// valueTypes holds the static types of variables bound to a literal by
	// WITH … AS or UNWIND, for the function argument checks.
	var valueTypes map[string]string
	// valueMembers holds the member types of variables a WITH bound to a
	// map literal, in a Cypher 25 statement (projectStaticValueMembers);
	// every other binding of a name drops its entry.
	var valueMembers map[string]map[string]staticOperand
	returnSeen := false
	// unwound names the typed variables whose type came from UNWIND, which
	// Neo4j checks against a procedure's parameters only when the call runs.
	var unwound map[string]struct{}
	// view is the static view of types; stale after a clause changes them.
	var view staticTypeScope
	stale := true
	for _, clause := range clauses {
		if err := expressionSubqueryClauseBindingError(clause, enclosing); err != nil {
			return types, false, err
		}
		if returnSeen {
			// RETURN is the terminal clause: a clause after it is never a
			// legal statement (RETURN 1 AS x RETURN 2 AS y is Neo4j's
			// "Invalid input 'RETURN'").
			token := strings.Fields(clause.text)[0]
			return types, false, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInput(token))
		}
		if clause.kind == pipelineClauseReturn {
			returnSeen = true
		}
		if stale {
			view = types.static(cypher25)
		}
		scope, valueTypes, valueMembers = view.kinds, view.values, view.members
		// Every clause but RETURN and DELETE may change types.
		stale = true
		if clause.kind != pipelineClauseMatch && clause.kind != pipelineClauseOptionalMatch {
			if err := validateShortestCallBindings(clause.text, scope); err != nil {
				return types, false, err
			}
		}
		var projectedTypes typeScope
		if clause.kind == pipelineClauseWith {
			projectedTypes = projectTypeScopeIn(types, view, clause.text)
			// The projection reads the incoming variables; WITH … WHERE
			// reads the projected ones.
			projection := clause.text
			if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
				projection = clause.text[:where]
				if err := graphListOperandTypeError(clause.text[where:], projectedTypes.kinds()); err != nil {
					return types, false, err
				}
			}
			if err := graphListOperandTypeError(projection, scope); err != nil {
				return types, false, err
			}
		}
		// A clause's EXISTS / COUNT / COLLECT subqueries see its variables, and
		// a WITH's or RETURN's projected ones (WITH i.n AS k WHERE COUNT { … k … });
		// MATCH and procedure CALL are checked once their outputs bind;
		// a CALL subquery's body starts from its imports.
		if clause.kind != pipelineClauseMatch && clause.kind != pipelineClauseOptionalMatch && clause.kind != pipelineClauseCall && clause.kind != pipelineClauseCallSubquery && (mayContainSubqueryExpression(clause.text) || containsRelationshipPattern(clause.text)) {
			if err := e.validateClauseSubqueries(clause, types, view, projectedTypes, cypher25); err != nil {
				return types, false, err
			}
		}
		switch clause.kind {
		case pipelineClauseLet, pipelineClauseFilter:
			if err := e.validateSharedClause(scope, valueTypes, clause, cypher25); err != nil {
				return types, false, err
			}
			if clause.kind == pipelineClauseLet {
				projections, err := parsePipelineLet(clause.text)
				if err != nil {
					return types, false, err
				}
				for _, projection := range projections {
					if _, exists := scope[projection.alias]; exists {
						return types, false, newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							fmt.Sprintf("variable %s is already declared", projection.alias))
					}
					input := types.static(cypher25)
					input.complete = false
					types[projection.alias] = projectedOperand(types, input, projection.expression)
					scope[projection.alias] = types[projection.alias].bindingKind()
				}
			}
		case pipelineClauseCallSubquery:
			body, _, _, _ := e.parseCallSubquery(clause.text)
			branches := []string{body}
			if unionBranches, _, _, union := parseTopLevelUnionBranches(body); union && len(unionBranches) > 0 {
				branches = unionBranches
			}
			// The body is a query of its own, walked from the variables it
			// imports; its subqueries see its scope.
			returned, err := e.callSubqueryReturnTypes(clause.text, branches, types, cypher25)
			if err != nil {
				return types, false, err
			}
			if returnIndex := topLevelKeywordIndex(branches[0], "RETURN"); returnIndex >= 0 {
				columns := pipelineReturnSourceColumns(branches[0][returnIndex:])
				if columns == nil || slices.Contains(columns, "*") {
					// RETURN * (which names no column of its own) returns
					// every variable of the body. When the
					// body's variables are typed, the statement goes on
					// with them; otherwise, as after YIELD *, the rest of
					// it isn't checked (#907: CALL { UNWIND [2] AS b
					// RETURN * } RETURN b).
					if returned == nil {
						return types, false, nil
					}
					for name, operand := range returned {
						if _, bound := scope[name]; !bound {
							types[name] = operand
							scope[name] = operand.bindingKind()
						}
					}
					continue
				}
				for _, name := range columns {
					// A returned column is a new variable of the enclosing
					// query: one it already binds is declared twice
					// (Neo4j 5.26.30's VariableAlreadyBound), unless every
					// branch returns that outer variable itself, unchanged
					// (callSubqueryReturnsOuterUnchanged).
					if _, bound := scope[name]; bound && !callSubqueryReturnsOuterUnchanged(branches, name) {
						return types, false, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							localization.CypherCoreVariableDeclaredInOuterScope(name))
					}
					types[name] = returned[name]
					scope[name] = types[name].bindingKind()
				}
			}
		case pipelineClauseCall:
			// CALL proc() YIELD x: a yielded name must be new (Neo4j:
			// VariableAlreadyBound). YIELD * names nothing statically, so
			// the rest of the statement isn't checked.
			if err := staticProcedureArgumentTypeError(clause.text, staticTypeScope{kinds: scope, values: valueTypes, derived: view.derived, complete: true}, unwound); err != nil {
				return types, false, err
			}
			yield := parseYieldClause(clause.text)
			if yield == nil || yield.yieldAll {
				if mayContainSubqueryExpression(clause.text) || containsRelationshipPattern(clause.text) {
					if err := e.validateClauseSubqueries(clause, types, view, nil, cypher25); err != nil {
						return types, false, err
					}
				}
				return types, false, nil
			}
			outputTypes := procedureOutputTypes(clause.text)
			for _, item := range yield.items {
				name := item.column()
				if _, bound := types[name]; bound {
					return types, false, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
						localization.CypherCoreProcedureOutputShadowsVariable(name))
				}
				types[name] = staticOperand{}
				if typeName := outputTypes[item.name]; typeName != "" {
					// A procedure's declared output type.
					types[name] = provenOperand(typeName)
				}
			}
			// Like WITH, the invocation reads the incoming scope and the
			// predicate reads it extended by the projected (yielded) names.
			if mayContainSubqueryExpression(clause.text) || containsRelationshipPattern(clause.text) {
				if err := e.validateClauseSubqueries(clause, types, view, types, cypher25); err != nil {
					return types, false, err
				}
			}
			if err := e.validateStaticClauseTypes(clause, types.static(cypher25)); err != nil {
				return types, false, err
			}
		case pipelineClauseMatch, pipelineClauseOptionalMatch:
			if err := e.validateMatchClauseBindings(scope, clause.text); err != nil {
				return types, false, err
			}
			// A quantified group's predicates see one iteration's node and
			// relationship; every check after them reads the clause without
			// them, where the group's variables are lists.
			masked, err := e.validateQuantifiedGroupPredicates(clause.text, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25})
			if err != nil {
				return types, false, err
			}
			clause.text = masked
			// The clause's WHERE subqueries see its own bindings.
			if err := e.validateSubqueryPatternBindings(staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}, clause.text); err != nil {
				return types, false, err
			}
			types.adoptKinds(scope)
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
		case pipelineClauseWith:
			if containsMalformedCreateClauseToken(clause.text) {
				return types, false, nil
			}
			if err := validateWithOrderBySemanticScope(scope, clause.text, enclosing.visible); err != nil {
				return types, false, err
			}
			// The WHERE after WITH reads the projected scope merged over the
			// incoming one, as at runtime (aliases shadow same-named inputs).
			if where := topLevelKeywordIndex(clause.text, "WHERE"); where >= 0 {
				projected := projectedTypes.kinds()
				whereScope := make(matchSemanticScope, len(scope)+len(projected))
				for name, kind := range scope {
					whereScope[name] = kind
				}
				for name, kind := range projected {
					whereScope[name] = kind
				}
				if err := e.validateMatchWhereSimpleOperands(whereScope, clause.text[where+len("WHERE"):]); err != nil {
					return types, false, err
				}
			}
			// The projection sees the incoming variables; WHERE / ORDER BY
			// after it see the projected ones.
			projection, rest := splitWithProjection(clause.text)
			projectionBody, _ := cutDistinct(strings.TrimSpace(projection[len("WITH"):]))
			for _, raw := range splitTopLevelComma(projectionBody) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
				if err := projectionItemTermError(expression); err != nil {
					return types, false, err
				}
				if err := projectionAliasError(raw); err != nil {
					return types, false, err
				}
				if expression != "*" {
					if err := undefinedExpressionVariable(scope, expression); err != nil {
						return types, false, err
					}
				}
			}
			input := staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}
			if err := validateStaticFunctionVariables(projection, input); err != nil {
				return types, false, err
			}
			if err := e.validateStaticOperatorTypes(clause, input, func() staticTypeScope { return projectionAliasScope(input, clause.text) }, nil); err != nil {
				return types, false, err
			}
			// Retain each import's current operand after a rebinding, using
			// the same rule that carries runtime values across WITH.
			carryStaticSubqueryImports(projectedTypes, types, enclosing.visible, projectionBody)
			types = projectedTypes
			unwound = projectUnwoundValues(unwound, clause.text)
			projected := types.static(cypher25)
			scope, valueTypes, valueMembers = projected.kinds, projected.values, projected.members
			view, stale = projected, false
			if err := forEachProjectedTailExpression(projection, rest, func(expression string) error {
				return validateStaticFunctionVariables(expression, projected)
			}); err != nil {
				return types, false, err
			}
		case pipelineClauseUnwind:
			if err := validateUnwindAlias(clause.text); err != nil {
				return types, false, err
			}
			if err := undefinedExpressionVariable(scope, unwindSourceExpression(clause.text)); err != nil {
				return types, false, err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
			if alias := unwindBindingName(clause.text); alias != "" {
				// FOR is new syntax: unlike the pre-existing permissive UNWIND,
				// it must not redeclare an already-bound variable, as in Neo4j.
				if startsWithKeywordFold(clause.text, "FOR") {
					if _, bound := scope[alias]; bound {
						return types, false, newSemanticError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
							fmt.Sprintf("variable %s is already declared", alias))
					}
				}
				input := view
				input.complete = false
				// Neo4j types an element of a list UNWIND binds only when a
				// procedure call runs; UNWIND 5 binds 5 statically.
				source := projectedOperand(types, input, unwindSourceExpression(clause.text))
				types[alias] = unwoundOperand(types, input, clause.text)
				scope[alias] = types[alias].bindingKind()
				delete(unwound, alias)
				if types[alias].typeName() != "" && !source.excludes(isListTypeName) {
					if unwound == nil {
						unwound = make(map[string]struct{})
					}
					unwound[alias] = struct{}{}
				}
			}
		case pipelineClauseReturn:
			stale = false
			if err := validateReturnSemanticScopeIn(scope, clause.text, enclosing.visible); err != nil {
				return types, false, err
			}
			if err := callBodyReturnStarError(scope, clause.text, enclosing); err != nil {
				return types, false, err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
		case pipelineClauseDelete:
			stale = false
			if err := deleteTargetTypeError(clause.text, scope); err != nil {
				return types, false, err
			}
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
		case pipelineClauseCreate, pipelineClauseMerge:
			addMatchPatternBindingKinds(scope, clause.text)
			types.adoptKinds(scope)
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
		default:
			if err := e.validateStaticClauseTypes(clause, staticTypeScope{kinds: scope, values: valueTypes, members: valueMembers, derived: view.derived, complete: true, cypher25: cypher25}); err != nil {
				return types, false, err
			}
		}
		if clause.kind != pipelineClauseWith {
			// Every other clause reads its own and the incoming bindings.
			if err := graphListOperandTypeError(clause.text, types.kinds()); err != nil {
				return types, false, err
			}
		}
	}
	return types, true, nil
}

func validateWithOrderBySemanticScope(input matchSemanticScope, clause string, retained typeScope) error {
	body := strings.TrimSpace(clause[len("WITH"):])
	orderIndex := topLevelKeywordIndex(body, "ORDER BY")
	if orderIndex < 0 {
		return nil
	}
	orderBody := strings.TrimSpace(body[keywordEndAt(body, orderIndex, "ORDER BY"):])
	projectionBody := strings.TrimSpace(body[:orderIndex])
	hasProjectionAggregate := false
	for _, item := range splitTopLevelComma(projectionBody) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(item))
		if containsAggregateFunc(expression) {
			hasProjectionAggregate = true
			break
		}
	}
	if err := validateReturnOrderBySemanticScopeIn("RETURN "+body, retained); err != nil {
		return err
	}
	for _, term := range parseOrderByClause(orderBody) {
		if containsAggregateFunc(term.column) && !hasProjectionAggregate {
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidAggregation",
				"ORDER BY cannot introduce an aggregate after a non-aggregating WITH",
			)
		}
	}
	return validateOrderByReferences(input, projectMatchSemanticScope(input, clause), orderBody)
}

// validateOrderByReferences checks that every variable an ORDER BY of a WITH
// or RETURN reads is in the clause's incoming scope or one it projects, as
// Neo4j does ("Variable `y` not defined"). orderBody is the text after ORDER
// BY; its SKIP / LIMIT are ignored.
func validateOrderByReferences(input, output matchSemanticScope, orderBody string) error {
	for _, keyword := range []string{"SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(orderBody, keyword); index >= 0 {
			orderBody = strings.TrimSpace(orderBody[:index])
		}
	}
	for _, term := range parseOrderByClause(orderBody) {
		for _, reference := range semanticFreeReferences(term.column) {
			base := strings.SplitN(reference, ".", 2)[0]
			if _, available := input[base]; available {
				continue
			}
			if _, projected := output[base]; projected {
				continue
			}
			return createUndefinedVariableError(base)
		}
	}
	return nil
}

func validateReturnSemanticScope(scope matchSemanticScope, clause string) error {
	return validateReturnSemanticScopeIn(scope, clause, nil)
}

// validateReturnSemanticScopeIn is validateReturnSemanticScope in an EXISTS /
// COUNT / COLLECT body, whose enclosing query's variables (outer) are
// constants there, not grouping keys: COLLECT { … RETURN count(*) + g }.
func validateReturnSemanticScopeIn(scope matchSemanticScope, clause string, outer typeScope) error {
	if err := validateReturnOrderBySemanticScopeIn(clause, outer); err != nil {
		return err
	}
	body := strings.TrimSpace(clause[len("RETURN"):])
	if err := validateReturnAggregationSemanticsIn(body, outer); err != nil {
		return err
	}
	// A dangling modifier keyword (RETURN 1 ORDER BY / SKIP / LIMIT) and an
	// empty projection (RETURN, WITH 1 AS x RETURN) are Neo4j syntax errors.
	// The clause is scanned with its keyword, which tells a keyword-named
	// first item from a clause (RETURN skip[0], #894).
	clauseText := strings.TrimSpace(clause)
	projectionEnd := len(clauseText)
	for _, keyword := range []string{"ORDER BY", "SKIP", "LIMIT"} {
		index := topLevelKeywordIndex(clauseText, keyword)
		if index < len("RETURN") || index >= projectionEnd {
			continue
		}
		if strings.TrimSpace(clauseText[index+len(keyword):]) == "" {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(""))
		}
		projectionEnd = index
	}
	projectionText := strings.TrimSpace(clauseText[len("RETURN"):projectionEnd])
	body, _ = cutDistinct(projectionText)
	if body == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherMatchingReturnExpressionRequired())
	}
	if orderIndex := topLevelKeywordIndex(clauseText, "ORDER BY"); orderIndex >= len("RETURN") {
		projection := strings.TrimSpace(clauseText[len("RETURN"):orderIndex])
		if err := validateOrderByReferences(scope, projectMatchSemanticScope(scope, "WITH "+projection), strings.TrimSpace(clauseText[keywordEndAt(clauseText, orderIndex, "ORDER BY"):])); err != nil {
			return err
		}
	}
	for _, raw := range splitTopLevelComma(body) {
		expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
		if err := projectionItemTermError(expression); err != nil {
			return err
		}
		if err := undefinedExpressionVariable(scope, expression); err != nil {
			return err
		}
		if expression == "*" {
			if len(scope) == 0 {
				return &classifiedCypherError{
					cause:  localizedError(localization.CypherMatchingReturnStarNoVariables(), nil),
					code:   "Neo.ClientError.Statement.SyntaxError",
					detail: "NoVariablesInScope",
				}
			}
			continue
		}
		if _, literal := parseLiteralValueFromComputedRow(expression); literal {
			continue
		}
		variable := simpleSemanticIdentifier(expression)
		if variable == "" {
			// m.val: the property chain's variable must be bound.
			if base, _, chain := rowPropertyChainShape(expression); chain {
				variable = base
			}
		}
		// null.a is a property of null (null), not of a variable.
		if variable != "" && !isLiteralKeyword(variable) {
			if _, found := scope[variable]; !found {
				return createUndefinedVariableError(variable)
			}
		}
	}
	return nil
}

// undefinedExpressionVariable is "Variable `x` not defined" for the first
// variable expression reads that scope doesn't bind: anywhere in it,
// inside a CASE, a list, a map projection or a comprehension's WHERE and
// projection included, as in Neo4j (#907). A comprehension's or a
// reduce's own variable is bound inside it (expressionFreeVariables).
func undefinedExpressionVariable(scope matchSemanticScope, expression string) error {
	// A literal, a variable or a property chain reads at most its base
	// variable: checked without the scanner.
	if _, literal := parseLiteralValueFromComputedRow(expression); literal {
		return nil
	}
	variable := simpleSemanticIdentifier(expression)
	if variable == "" {
		if base, _, chain := rowPropertyChainShape(expression); chain {
			variable = base
		}
	}
	if variable != "" {
		if _, found := scope[variable]; !found && !isLiteralKeyword(variable) {
			return createUndefinedVariableError(variable)
		}
		return nil
	}
	// expressionFreeVariables leaves out true, false, null and the other
	// literal words.
	for _, name := range expressionFreeVariables(expression) {
		if _, found := scope[name]; !found {
			return createUndefinedVariableError(name)
		}
	}
	return nil
}

func (e *StorageExecutor) validateMatchClauseBindings(scope matchSemanticScope, clause string) error {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"OPTIONAL MATCH", "MATCH"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	whereClause := ""
	// A WHERE inside parentheses is a quantified path pattern's own.
	if where := topLevelKeywordIndex(pattern, "WHERE"); where >= 0 {
		whereClause = strings.TrimSpace(pattern[where+len("WHERE"):])
	}
	// A quantified group's WHERE is an expression, not pattern structure.
	pattern = strings.TrimSpace(matchPatternStructure(pattern))
	if mergePatternUsesParameterPredicate(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidParameterUse",
			"MATCH pattern predicates must use a property map, not a parameter map",
		)
	}
	if invalidRelationshipPattern(pattern) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidRelationshipPattern",
			"invalid variable-length relationship pattern",
		)
	}

	variableLengthRelationships := variableLengthRelationshipVariableSet(pattern)
	groupNodes, groupRelationships := quantifiedGroupVariables(pattern)
	for variable := range groupNodes {
		if _, bound := scope[variable]; bound {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherMatchingQuantifiedPathVariableBound(variable))
		}
	}
	for variable := range groupRelationships {
		if _, bound := scope[variable]; bound {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherMatchingQuantifiedPathVariableBound(variable))
		}
	}
	for _, patternPart := range splitTopLevelComma(pattern) {
		patternPart = strings.TrimSpace(patternPart)
		pathVariable := extractPathAssignmentVariable(patternPart)
		entityPattern := patternPart
		if pathVariable != "" {
			if equals := strings.Index(patternPart, "="); equals >= 0 {
				entityPattern = strings.TrimSpace(patternPart[equals+1:])
			}
			if _, alreadyBound := scope[pathVariable]; alreadyBound {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			scope[pathVariable] = matchBindingPath
		}

		if _, _, _, selected := splitPathSelectorTerm(whereClause); !selected {
			if err := validateShortestCallBindings(entityPattern, scope); err != nil {
				return err
			}
		}
		for _, variable := range extractNodeVariables(entityPattern) {
			if variable == pathVariable {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			kind := matchBindingNode
			if _, grouped := groupNodes[variable]; grouped {
				kind = matchBindingNodeList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
				return err
			}
		}
		for _, variable := range extractRelationshipVariables(entityPattern) {
			if variable == pathVariable {
				return newSemanticError(
					"Neo.ClientError.Statement.SyntaxError",
					"VariableAlreadyBound",
					fmt.Sprintf("path variable %s is already bound", pathVariable),
				)
			}
			kind := matchBindingRelationship
			if _, variableLength := variableLengthRelationships[variable]; variableLength {
				kind = matchBindingRelationshipList
			}
			if _, grouped := groupRelationships[variable]; grouped {
				kind = matchBindingRelationshipList
			}
			if err := bindMatchSemanticKind(scope, variable, kind); err != nil {
				return err
			}
		}

	}
	return e.validateMatchWhereSimpleOperands(scope, whereClause)
}

func (e *StorageExecutor) validateMatchWhereSimpleOperands(scope matchSemanticScope, whereClause string) error {
	whereClause = strings.TrimSpace(maskSubqueryBodies(whereClause))
	if whereClause == "" {
		return nil
	}
	switch whereClause[0] {
	case '=', '<', '>', '~':
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression(string(whereClause[0])))
	case '!':
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInputExpectedExpression("!"))
	}
	// A clause keyword read as a variable followed by another term with no
	// operator between them is Neo4j's Invalid input error (WHERE RETURN n:
	// return is the variable, n is invalid — the #740 reading).
	if err := projectionItemTermError(whereClause); err != nil {
		return err
	}
	if containsAggregateFunc(whereClause) {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"InvalidAggregation",
			"aggregate expressions are not allowed in WHERE",
		)
	}
	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		if left, right, found := splitByOperatorOutsideCase(whereClause, operator, true, true); found {
			if err := e.validateMatchWhereSimpleOperands(scope, left); err != nil {
				return err
			}
			return e.validateMatchWhereSimpleOperands(scope, right)
		}
	}
	if err := e.validateWherePatternExpressionScope(scope, whereClause); err != nil {
		return err
	}
	if operands, _, comparison := splitComparisonChain(whereClause); comparison {
		for _, operand := range operands {
			operand = strings.TrimSpace(operand)
			// A clause keyword read as a variable followed by another term with
			// no operator between them is Neo4j's Invalid input error
			// (WHERE n.x = RETURN n: return is the variable, n is invalid).
			if err := projectionItemTermError(operand); err != nil {
				return err
			}
			if variable, _, propertyAccess := parseVarPropertyRef(operand); propertyAccess {
				switch scope[normalizeProjectionColumnName(variable)] {
				case matchBindingPath, matchBindingRelationshipList, matchBindingNodeList:
					return newSemanticError(
						"Neo.ClientError.Statement.SyntaxError",
						"InvalidArgumentType",
						fmt.Sprintf("property access is not supported on %s", variable),
					)
				}
			}
			if _, literal := parseLiteralValueFromComputedRow(operand); literal {
				continue
			}
			identifier := simpleSemanticIdentifier(operand)
			if identifier == "" {
				continue
			}
			_, inScope := scope[identifier]
			_, externallyBound := e.fabricRecordBindings[identifier]
			if !inScope && !externallyBound {
				return createUndefinedVariableError(identifier)
			}
		}
	}
	for _, operator := range []string{" STARTS WITH ", " ENDS WITH ", " CONTAINS ", " NOT IN ", " IN ", "=~"} {
		left, right, found := splitByOperatorOutsideCase(whereClause, operator, true, true)
		if !found {
			continue
		}
		left = strings.TrimSpace(left)
		// A clause keyword read as a variable on the operator's right followed
		// by another term is Invalid input (WHERE n.x IN RETURN n).
		if err := projectionItemTermError(strings.TrimSpace(right)); err != nil {
			return err
		}
		if simpleSemanticIdentifier(left) == left {
			if _, exists := scope[left]; !exists {
				return createUndefinedVariableError(left)
			}
		}
	}
	return nil
}

func (e *StorageExecutor) validateWherePatternExpressionScope(scope matchSemanticScope, expression string) error {
	expression = strings.TrimSpace(expression)
	if inner, enclosed := stripEnclosingExpressionParentheses(expression); enclosed {
		// (true), (false) and (null) are parenthesised literals, not node
		// patterns (#878 writes an element's own predicate in parentheses).
		if variable := simpleSemanticIdentifier(inner); variable != "" && !isLiteralKeyword(variable) {
			if _, inScope := scope[variable]; !inScope {
				if _, externallyBound := e.fabricRecordBindings[variable]; !externallyBound {
					return createUndefinedVariableError(variable)
				}
			}
			return newSemanticError(
				"Neo.ClientError.Statement.SyntaxError",
				"InvalidArgumentType",
				"a node pattern is not a boolean predicate",
			)
		}
	}
	// A pattern comprehension binds its own variables (#1109); only a
	// pattern expression may not.
	expression = maskPatternComprehensions(expression)
	if !containsRelationshipPattern(expression) {
		return nil
	}
	for _, variable := range append(extractNodeVariables(expression), extractRelationshipVariables(expression)...) {
		if _, inScope := scope[variable]; inScope {
			continue
		}
		if _, externallyBound := e.fabricRecordBindings[variable]; externallyBound {
			continue
		}
		return createUndefinedVariableError(variable)
	}
	return nil
}

// maskPatternComprehensions blanks, keeping byte offsets, the inside of every
// pattern comprehension in expression ([(a)-->(b) WHERE … | b.x]), whose
// pattern binds variables of its own. Quoted text and relationship brackets
// (-[r]->) are not comprehensions.
func maskPatternComprehensions(expression string) string {
	if strings.IndexByte(expression, '[') < 0 {
		return expression
	}
	var masked []byte
	for index := 0; index < len(expression); index++ {
		switch character := expression[index]; cypherLexicalByte(character) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(expression, index, character) - 1
			continue
		case '[':
		default:
			continue
		}
		if index > 0 && expression[index-1] == '-' {
			continue
		}
		close := findMatchingBracket(expression, index)
		if close < 0 {
			return expression
		}
		if _, _, comprehension := splitPatternComprehension(expression[index : close+1]); comprehension {
			if masked == nil {
				masked = []byte(expression)
			}
			for position := index + 1; position < close; position++ {
				masked[position] = ' '
			}
			index = close
		}
	}
	if masked == nil {
		return expression
	}
	return string(masked)
}

// maskSubqueryBodies replaces each classified subquery operand with a literal
// of its result kind, preserving byte offsets. Leaving EXISTS/COUNT/COLLECT
// before an erased body makes later readers mistake it for a pattern variable
// or keyword-named map projection. Classify the original body before masking;
// quoted text and actual map projections keep their meaning.
func maskSubqueryBodies(expression string) string {
	subqueries := findSubqueryExpressions(expression)
	if len(subqueries) == 0 {
		return expression
	}
	masked := []byte(expression)
	for _, subquery := range subqueries {
		for position := subquery.start; position < subquery.end; position++ {
			masked[position] = ' '
		}
		literal := "true"
		switch subquery.kind {
		case "COUNT":
			literal = "0"
		case "COLLECT":
			literal = "[]"
		}
		copy(masked[subquery.start:], literal)
	}
	return string(masked)
}

// validateSubqueryPatternBindings checks every EXISTS / COUNT / COLLECT
// subquery in text as a query of its own, walked by the one clause walk
// (semanticScopeWalk) from scope, the variables the subquery sees at its
// position (a local of an enclosing expression typed as that local;
// withLocalTypes). The walk binds the body's MATCH and WITH variables and
// checks its nested subqueries from them in turn: a pattern can't use an
// outer variable as another kind (COUNT { (n)-[s]->() } with s a string is
// Neo4j's "s defined with conflicting type", #1055). A local the body's own
// clauses declare (a comprehension's, a quantifier's, reduce's, a named
// path; not a nested subquery's or CALL's) may not redeclare a variable it
// sees: Neo4j's "is shadowing a variable with the same name from the outer
// scope". At the top level and in CALL { } such a local shadows instead.
func (e *StorageExecutor) validateSubqueryPatternBindings(scope staticTypeScope, text string) error {
	if err := validateExpressionPatternBindings(scope, text); err != nil {
		return err
	}
	if !mayContainSubqueryExpression(text) {
		return nil
	}
	for _, subquery := range findSubqueryExpressions(text) {
		local := withPatternComprehensionBindings(withLocalTypes(scope, text, subquery.start), text, subquery.start)
		for name := range expressionLocalBindings(maskNestedScopeBodies(subquery.body)) {
			if local.bound(name) {
				return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound",
					localization.CypherCoreVariableDeclaredInOuterScope(name))
			}
		}
		body := strings.TrimSpace(subquery.body)
		if strings.HasPrefix(body, "(") {
			body = "MATCH " + body
		}
		// Each UNION branch is a query of its own.
		branches := []string{body}
		if unionBranches, _, _, union := parseTopLevelUnionBranches(body); union && len(unionBranches) > 0 {
			branches = unionBranches
		}
		visible := typeScopeOf(local)
		for _, branch := range branches {
			clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
			if !ok {
				continue
			}
			if _, _, err := e.semanticScopeWalk(clauses, maps.Clone(visible), scope.cypher25, subqueryScope{visible: visible, outer: true}); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateExpressionPatternBindings applies the shared pattern-kind check
// to expression chains with the local scope at their actual occurrence.
func validateExpressionPatternBindings(scope staticTypeScope, text string) error {
	if !containsRelationshipPattern(text) {
		return nil
	}
	start := 0
	// MATCH's own pattern was already checked and bound; its predicate
	// contains the expression patterns checked here.
	if startsWithKeywordFold(text, "MATCH") || startsWithKeywordFold(text, "OPTIONAL MATCH") {
		start = topLevelKeywordIndex(text, "WHERE")
		if start < 0 {
			return nil
		}
	}
	structure := maskNestedScopeBodies(text)
	local := func(at int) staticTypeScope {
		return withPatternComprehensionBindings(withLocalTypes(scope, text, at), text, at)
	}
	for index := start; index < len(structure); index++ {
		if isCypherQuoteByte(structure[index]) {
			index = skipCypherQuotedText(structure, index, structure[index]) - 1
			continue
		}
		if structure[index] == '[' {
			close := findMatchingBracket(structure, index)
			if close < 0 {
				break
			}
			if pattern, _, comprehension := splitPatternComprehension(structure[index : close+1]); comprehension {
				// The head is checked before its new locals are introduced;
				// enclosing comprehensions still supply their own local types.
				if err := validateBoundPatternKinds(local(index-1).kinds, pattern); err != nil {
					return err
				}
			}
		}
		if structure[index] != '(' {
			continue
		}
		end, chain := relationshipChainEnd(structure, index, len(structure))
		if !chain {
			continue
		}
		if err := validateBoundPatternKinds(local(index).kinds, structure[index:end]); err != nil {
			return err
		}
		index = end - 1
	}
	return nil
}

func validateBoundPatternKinds(scope matchSemanticScope, pattern string) error {
	pattern = matchPatternStructure(pattern)
	kinds := maps.Clone(scope)
	if kinds == nil {
		kinds = make(matchSemanticScope)
	}
	for _, variable := range extractNodeVariables(pattern) {
		if err := bindMatchSemanticKind(kinds, variable, matchBindingNode); err != nil {
			return err
		}
	}
	lists := variableLengthRelationshipVariableSet(pattern)
	for _, variable := range extractRelationshipVariables(pattern) {
		kind := matchBindingRelationship
		if _, list := lists[variable]; list {
			kind = matchBindingRelationshipList
		}
		if err := bindMatchSemanticKind(kinds, variable, kind); err != nil {
			return err
		}
	}
	return nil
}

// maskNestedScopeBodies blanks the bodies of the EXISTS / COUNT / COLLECT
// and CALL subqueries in text, which start scopes of their own.
func maskNestedScopeBodies(text string) string {
	masked := []byte(maskSubqueryBodies(text))
	for index := 0; index < len(text); index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		}
		if !matchKeywordAt(text, index, "CALL") {
			continue
		}
		open := skipSpaces(text, index+len("CALL"))
		if open < len(text) && text[open] == '(' {
			if close := findMatchingParen(text, open); close >= 0 {
				open = skipSpaces(text, close+1)
			}
		}
		if open >= len(text) || text[open] != '{' {
			continue
		}
		close := findMatchingDelimiter(text, open, '{', '}')
		if close < 0 {
			break
		}
		for position := open + 1; position < close; position++ {
			masked[position] = ' '
		}
		index = close
	}
	return string(masked)
}

func bindMatchSemanticKind(scope matchSemanticScope, variable string, kind matchBindingKind) error {
	variable = normalizeProjectionColumnName(strings.TrimSpace(variable))
	if variable == "" {
		return nil
	}
	if existing, found := scope[variable]; found && existing != matchBindingUnknown && existing != kind {
		return newSemanticError(
			"Neo.ClientError.Statement.SyntaxError",
			"VariableTypeConflict",
			fmt.Sprintf("variable %s is already bound to a different entity type", variable),
		)
	}
	scope[variable] = kind
	return nil
}

// schemaCommandKeywords follow CREATE or DROP in a schema command.
var schemaCommandKeywords = []string{"CONSTRAINT", "INDEX", "FULLTEXT", "VECTOR", "RANGE", "TEXT", "POINT", "LOOKUP", "BTREE", "OR REPLACE"}

// isSchemaCommandStatement reports whether cypher is a schema command
// (CREATE / DROP CONSTRAINT, INDEX, …): its patterns and expressions declare a
// schema rule over a pattern (FOR ()-[r:T]-() REQUIRE …) and bind no clause
// variables.
func isSchemaCommandStatement(cypher string) bool {
	trimmed := strings.TrimSpace(cypher)
	for _, command := range []string{"CREATE", "DROP"} {
		if !startsWithKeywordFold(trimmed, command) {
			continue
		}
		rest := strings.TrimSpace(trimmed[len(command):])
		for _, keyword := range schemaCommandKeywords {
			if startsWithKeywordFold(rest, keyword) {
				return true
			}
		}
	}
	return false
}

// validateStaticClauseTypes applies the compile-time type checks of one
// clause: function arguments (validateStaticFunctionVariables) and operators
// (validateStaticOperatorTypes).
func (e *StorageExecutor) validateStaticClauseTypes(clause pipelineClause, scope staticTypeScope) error {
	if err := validateStaticPropertySubscripts(clause.text, scope); err != nil {
		return err
	}
	if err := staticWriteTokenError(clause, scope); err != nil {
		return err
	}
	if len(scope.values) > 0 {
		if err := staticListOperandTypeError(clause.text, scope); err != nil {
			return err
		}
	}
	var projectedScope func() staticTypeScope
	if clause.kind == pipelineClauseReturn {
		projection, rest := splitWithProjection(clause.text)
		var projected *staticTypeScope
		projectedScope = func() staticTypeScope {
			if projected == nil {
				built := projectionAliasScope(scope, clause.text)
				projected = &built
			}
			return *projected
		}
		if err := validateStaticFunctionVariables(projection, scope); err != nil {
			return err
		}
		if err := forEachProjectedTailExpression(projection, rest, func(expression string) error {
			return validateStaticFunctionVariablesIn(expression, projectedScope)
		}); err != nil {
			return err
		}
	} else if err := validateStaticFunctionVariables(clause.text, scope); err != nil {
		return err
	}
	return e.validateStaticOperatorTypes(clause, scope, projectedScope, nil)
}

// validateQuantifiedGroupPredicates checks each quantified group's
// predicates (its own WHERE and its elements' inline WHERE) in a MATCH
// clause where the group's variables are one node or relationship each (an
// iteration's), not the lists the group binds after it: in
// (()-[r WHERE m[r.id] IS NULL]->())+ r.id is a relationship's property and
// type(r) takes a relationship. Every expression check a clause's
// expressions get runs on them with that scope: subquery patterns,
// subscripts, dynamic labels and types, list operands, function arguments
// and operators. It returns
// the clause with those predicates blanked, for the checks of the rest of
// the clause.
func (e *StorageExecutor) validateQuantifiedGroupPredicates(clause string, scope staticTypeScope) (string, error) {
	groups := quantifiedGroups(clause)
	if len(groups) == 0 {
		return clause, nil
	}
	for _, group := range groups {
		predicates := group.predicateTexts(clause)
		if len(predicates) == 0 {
			continue
		}
		kinds := make(matchSemanticScope, len(scope.kinds)+len(group.nodes)+len(group.relationships))
		for name, kind := range scope.kinds {
			kinds[name] = kind
		}
		for _, variable := range group.nodes {
			kinds[variable] = matchBindingNode
		}
		for _, variable := range group.relationships {
			kinds[variable] = matchBindingRelationship
		}
		iteration := scope
		iteration.kinds = kinds
		for _, predicate := range predicates {
			if err := e.validateSubqueryPatternBindings(iteration, predicate); err != nil {
				return "", err
			}
			if err := validateStaticPropertySubscripts(predicate, iteration); err != nil {
				return "", err
			}
			// A dynamic label or type of the group, in its pattern
			// ((x:$(l))) or a predicate (x:$(l)), is a predicate call
			// here (labelExpression.predicate).
			if err := staticPatternTokenError(pipelineClause{kind: pipelineClauseMatch, text: predicate}, iteration); err != nil {
				return "", err
			}
			if err := staticListOperandTypeError(predicate, iteration); err != nil {
				return "", err
			}
			if err := validateStaticFunctionVariables(predicate, iteration); err != nil {
				return "", err
			}
			// Neo4j types the operators of a group predicate but doesn't
			// require a boolean result (WHERE r.kind + 1 matches nothing).
			if _, err := (&staticOperatorChecker{scope: iteration}).check(predicate); err != nil {
				return "", err
			}
		}
	}
	return maskQuantifiedGroupPredicates(clause), nil
}

// projectionAliasScope is the scope of the WHERE / ORDER BY after a RETURN or
// WITH projection: the incoming variables, with each projection alias
// replacing the variable of the same name (RETURN n.num AS n ORDER BY n + 2
// sees n as a number, not the node).
func projectionAliasScope(input staticTypeScope, clause string) staticTypeScope {
	body := strings.TrimSpace(clause)
	for _, keyword := range []string{"RETURN", "WITH"} {
		if startsWithKeywordFold(body, keyword) {
			body = strings.TrimSpace(body[len(keyword):])
			break
		}
	}
	types := typeScopeOf(input)
	for name, operand := range projectTypeScopeIn(types, input, "WITH "+body) {
		types[name] = operand
	}
	scope := types.static(input.cypher25)
	scope.complete = input.complete
	return scope
}

// projectionItemTermError is Neo4j's SyntaxError for a projection item that
// is a clause-keyword name followed by another term with no operator between
// them (WITH RETURN n: return is a variable, as Neo4j reads it (#740), and n
// is "Invalid input"). Before, the keyword was taken for a clause.
func projectionItemTermError(expression string) error {
	expression = strings.TrimSpace(expression)
	name, next, ok := scanIdentifierToken(expression, 0)
	if !ok || !isNameableClauseKeyword(name) {
		return nil
	}
	rest := strings.TrimSpace(expression[next:])
	if rest == "" || rest == expression[next:] {
		return nil // the name alone, or followed directly by an operator
	}
	if c := rest[0]; isIdentByte(c) || isCypherQuoteByte(c) || c == '$' {
		token, _, _ := scanIdentifierToken(rest, 0)
		if token == "" {
			token = rest[:1]
		}
		if !isExpressionContinuationWord(token) {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
				localization.CypherCoreInvalidInputExpectedExpression(token))
		}
	}
	return nil
}

// projectionAliasError is Neo4j's SyntaxError for a projection item whose
// alias, as written, is not one name (WITH n AS return n: the n after the
// alias is "Invalid input"). A backtick-quoted alias is one name.
func projectionAliasError(item string) error {
	// The alias follows the item's own AS (projectionAliasIndex), so an alias
	// named as is a name (WITH n AS as, #894).
	as := projectionAliasIndex(item)
	if as < 0 {
		return nil
	}
	alias := strings.TrimSpace(item[as+len("AS"):])
	if alias == "" {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
			localization.CypherCoreInvalidInput(""))
	}
	_, next, ok := scanSymbolicName(alias, 0)
	if !ok || next == len(alias) {
		return nil
	}
	rest := strings.TrimSpace(alias[next:])
	token, _, _ := scanIdentifierToken(rest, 0)
	if token == "" {
		token = rest[:1]
	}
	return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax",
		localization.CypherCoreInvalidInputExpectedExpression(token))
}

// isExpressionContinuationWord reports whether word, after an operand,
// continues the expression (an operator word: AND, IS NULL, STARTS WITH, …).
func isExpressionContinuationWord(word string) bool {
	for _, operator := range [...]string{"AND", "OR", "XOR", "IS", "IN", "STARTS", "ENDS", "CONTAINS", "NOT"} {
		if strings.EqualFold(word, operator) {
			return true
		}
	}
	return false
}

// splitWithProjection splits a WITH clause into its projection and the WHERE /
// ORDER BY / SKIP / LIMIT that follow it.
func splitWithProjection(clause string) (projection, rest string) {
	end := len(clause)
	for _, keyword := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if index := topLevelKeywordIndex(clause, keyword); index >= 0 && index < end {
			end = index
		}
	}
	return clause[:end], clause[end:]
}

// unwindSourceExpression is the list expression of an UNWIND clause.
func unwindSourceExpression(clause string) string {
	expression, _, _ := parsePipelineIteration(clause)
	return expression
}

func coalesceSemanticKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	name, inner, ok := parseFunctionCallWS(expression)
	if !ok || !strings.EqualFold(name, "coalesce") {
		return matchBindingUnknown, false
	}
	kind := matchBindingUnknown
	for _, argument := range splitTopLevelComma(inner) {
		variable := simpleSemanticIdentifier(argument)
		argumentKind, found := scope[variable]
		if variable == "" || !found || argumentKind == matchBindingUnknown {
			return matchBindingUnknown, false
		}
		if kind != matchBindingUnknown && kind != argumentKind {
			return matchBindingUnknown, false
		}
		kind = argumentKind
	}
	return kind, kind != matchBindingUnknown
}

func variableLengthRelationshipVariableSet(pattern string) map[string]struct{} {
	result := make(map[string]struct{})
	for open, end := nextRelationshipBracket(pattern, 0); open >= 0; open, end = nextRelationshipBracket(pattern, end+1) {
		declaration := relationshipDeclarationHead(pattern[open+1 : end])
		if !declaration.hasLength {
			continue
		}
		if name, _, ok := scanIdentifierToken(declaration.variable, 0); ok {
			result[name] = struct{}{}
		}
	}
	return result
}

// entityListLiteralKind is the kind of a list literal of node variables
// ([a, b] is a List<Node>) or of relationship variables; ok is false for any
// other expression.
func entityListLiteralKind(expression string, scope matchSemanticScope) (matchBindingKind, bool) {
	expression = strings.TrimSpace(expression)
	if !isListLiteral(expression) {
		return matchBindingUnknown, false
	}
	items := splitTopLevelComma(expression[1 : len(expression)-1])
	if len(items) == 0 {
		return matchBindingUnknown, false
	}
	element := matchBindingUnknown
	for _, item := range items {
		name := simpleSemanticIdentifier(item)
		kind := scope[name]
		if name == "" || (kind != matchBindingNode && kind != matchBindingRelationship) || (element != matchBindingUnknown && kind != element) {
			return matchBindingUnknown, false
		}
		element = kind
	}
	if element == matchBindingNode {
		return matchBindingNodeList, true
	}
	return matchBindingRelationshipList, true
}

// invalidRelationshipPattern reports a relationship bracket that is not closed
// or whose range is malformed. Only relationship brackets are read: a '[' in
// a property map ($ids[i * 20 + 1], $ws[0..2]) is a subscript or a slice.
func invalidRelationshipPattern(pattern string) bool {
	for open, end := scanRelationshipBracket(pattern, 0); open >= 0; open, end = scanRelationshipBracket(pattern, end+1) {
		if end < 0 {
			return true
		}
		// Quoted names and values can hold * and .. (#879), and so can
		// property values and dynamic types: the declaration reader skips
		// them.
		if relationshipDeclarationHead(pattern[open+1 : end]).lengthInvalid {
			return true
		}
	}
	return false
}

// simpleSemanticIdentifier returns the variable an expression consists of,
// plain or backtick-quoted (`x y`), unquoted; "" for any other expression.
func simpleSemanticIdentifier(expression string) string {
	expression = lexicalExpressionText(expression)
	// A backtick-quoted variable (`my x`, `$p`) is a variable whatever its
	// characters: it isn't a parameter, and scope checks must see it.
	if isBacktickQuotedName(expression) {
		return normalizeProjectionColumnName(expression)
	}
	name, next, ok := scanSymbolicName(expression, 0)
	if !ok || strings.TrimSpace(expression[next:]) != "" {
		return ""
	}
	return normalizeProjectionColumnName(name)
}

func addMatchPatternBindingKinds(scope matchSemanticScope, clause string) {
	pattern := strings.TrimSpace(clause)
	for _, keyword := range []string{"CREATE", "MERGE"} {
		if startsWithKeywordFold(pattern, keyword) {
			pattern = strings.TrimSpace(pattern[len(keyword):])
			break
		}
	}
	// The pattern binds what its structure names: (l) in a WHERE is no node.
	pattern = matchPatternStructure(pattern)
	for _, variable := range extractNodeVariables(pattern) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingNode
		}
	}
	for _, variable := range extractRelationshipVariables(pattern) {
		if _, found := scope[variable]; !found {
			scope[variable] = matchBindingRelationship
		}
	}
	for _, patternPart := range splitTopLevelComma(pattern) {
		if variable := extractPathAssignmentVariable(strings.TrimSpace(patternPart)); variable != "" {
			if _, found := scope[variable]; !found {
				scope[variable] = matchBindingPath
			}
		}
	}
}

// deleteTargetTypeError rejects a DELETE target whose type, known before the
// statement runs, is a property of a node or relationship (n.p, a stored
// value): Neo4j 5.26's "Type mismatch: expected Node, Path or Relationship"
// (#907). DELETE removes entities, and a property is removed with REMOVE;
// NornicDB used to accept the statement and delete nothing. A map's key (m.k)
// has no static type. null is a valid target that deletes nothing.
//
// A list of nodes or relationships (the variable of a variable-length
// relationship, MATCH ()-[x*]->() DELETE x) is a NornicDB extension: Neo4j
// 5.26 rejects it, NornicDB deletes each entity in it, as before (at the
// project owner's direction).
func deleteTargetTypeError(clause string, scope matchSemanticScope) error {
	targets, _, _ := deleteClauseTargets(clause)
	for _, expression := range targets {
		expression = strings.TrimSpace(expression)
		if variable, _, property := parseVarPropertyRef(expression); property && isEntityPropertyAccess(expression, variable) {
			switch scope[normalizeProjectionColumnName(variable)] {
			case matchBindingNode, matchBindingRelationship:
				return typeNameMismatchError("Node, Path or Relationship", "a property value")
			}
		}
	}
	return nil
}

// isEntityPropertyAccess reports whether expression, which parseVarPropertyRef
// read as variable.key, is exactly that, with nothing after the key (no
// subscript or further access).
func isEntityPropertyAccess(expression, variable string) bool {
	rest := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(expression), variable))
	key := strings.TrimSpace(strings.TrimPrefix(rest, "."))
	return isValidIdentifier(key) || isBacktickQuotedName(key)
}

// callSubqueryReturnsOuterUnchanged reports whether every branch of a CALL
// subquery returns the enclosing query's variable name as itself (RETURN
// name, or name AS name) without declaring name again in its body. Neo4j
// rejects even that as VariableAlreadyBound; NornicDB accepts it as an
// extension, since the column is the outer value and nothing is ambiguous
// (#907; the implicit import is the same kind of extension). A branch that
// rebinds name (WITH … AS name, UNWIND … AS name, YIELD name, a nested CALL
// returning it) or returns another value under it is rejected.
func callSubqueryReturnsOuterUnchanged(branches []string, name string) bool {
	for _, branch := range branches {
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
		if !ok || len(clauses) == 0 || clauses[len(clauses)-1].kind != pipelineClauseReturn {
			return false
		}
		for _, clause := range clauses {
			switch clause.kind {
			case pipelineClauseWith, pipelineClauseReturn:
				keyword := "WITH"
				if clause.kind == pipelineClauseReturn {
					keyword = "RETURN"
				}
				items, _ := projectionSemanticBodyAndTail(clause.text, keyword)
				items, _ = cutDistinct(strings.TrimSpace(items))
				for _, raw := range splitTopLevelComma(items) {
					expression, alias := parseProjectionExprAlias(strings.TrimSpace(raw))
					if normalizeProjectionColumnName(alias) == name && normalizeProjectionColumnName(expression) != name {
						return false
					}
				}
			case pipelineClauseUnwind:
				if unwindBindingName(clause.text) == name {
					return false
				}
			case pipelineClauseCallSubquery:
				inner, _, _, _ := (&StorageExecutor{}).parseCallSubquery(clause.text)
				if returnIndex := topLevelKeywordIndex(inner, "RETURN"); returnIndex >= 0 {
					for _, column := range pipelineReturnSourceColumns(inner[returnIndex:]) {
						if column == name {
							return false
						}
					}
				}
			case pipelineClauseCall:
				if yield := parseYieldClause(clause.text); yield != nil {
					if yield.yieldAll {
						return false
					}
					for _, item := range yield.items {
						if item.name == name || item.alias == name {
							return false
						}
					}
				}
			}
		}
	}
	return true
}

// withPatternComprehensionBindings is scope with the variables of every
// pattern comprehension around offset at of text that scope doesn't bind:
// in [(n)-->(x) WHERE EXISTS { … x … } | x] the subquery sees x. A name
// scope binds is the outer variable the pattern reads.
func withPatternComprehensionBindings(scope staticTypeScope, text string, at int) staticTypeScope {
	var added matchSemanticScope
	for open := 0; open < at; open++ {
		switch c := text[open]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			open = skipCypherQuotedText(text, open, c) - 1
			continue
		case '[':
		default:
			continue
		}
		close := findMatchingDelimiter(text, open, '[', ']')
		if close < at {
			continue
		}
		pattern, _, comprehension := splitPatternComprehension(text[open : close+1])
		if !comprehension {
			continue
		}
		bindings := make(matchSemanticScope)
		addMatchPatternBindingKinds(bindings, pattern)
		for name, kind := range bindings {
			if scope.bound(name) {
				continue
			}
			if added == nil {
				added = make(matchSemanticScope)
			}
			added[name] = kind
		}
	}
	if added == nil {
		return scope
	}
	local := scope
	local.kinds = maps.Clone(scope.kinds)
	if local.kinds == nil {
		local.kinds = make(matchSemanticScope)
	}
	maps.Copy(local.kinds, added)
	return local
}

// validateClauseSubqueries checks a clause's EXISTS / COUNT / COLLECT
// subqueries (validateSubqueryPatternBindings) with the variables each
// sees: a WITH's or RETURN's projection reads the incoming ones, its WHERE
// and ORDER BY the projected ones over them (WITH i.n AS k WHERE
// COUNT { … k … }). CALL's invocation reads the incoming scope, while
// YIELD's WHERE reads the yielded names over it through the same path.
// Any other clause's subqueries read the incoming scope.
func (e *StorageExecutor) validateClauseSubqueries(clause pipelineClause, types typeScope, view staticTypeScope, projected typeScope, cypher25 bool) error {
	switch clause.kind {
	case pipelineClauseWith, pipelineClauseCall:
	case pipelineClauseReturn:
		projected = projectTypeScopeIn(types, view, "WITH "+strings.TrimSpace(strings.TrimSpace(clause.text)[len("RETURN"):]))
	default:
		return e.validateSubqueryPatternBindings(view, clause.text)
	}
	projection, rest := splitWithProjection(clause.text)
	if err := e.validateSubqueryPatternBindings(view, projection); err != nil {
		return err
	}
	if !mayContainSubqueryExpression(rest) && !containsRelationshipPattern(rest) {
		return nil
	}
	merged := typeScopeOf(view)
	for name, operand := range projected {
		merged[name] = operand
	}
	tail := merged.static(cypher25)
	tail.params = view.params
	return e.validateSubqueryPatternBindings(tail, rest)
}
