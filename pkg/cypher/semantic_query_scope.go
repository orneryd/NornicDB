package cypher

import (
	"context"
	"maps"
	"slices"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// semanticDependencyScope observes reads in semanticScopeWalk. active names
// still denote the input binding; a declaration or a dropping WITH removes
// that provenance. Nested walks share reads, never their mutable local scope.
type semanticDependencyScope struct {
	active   map[string]bool
	reads    map[string]bool
	touched  map[string]bool
	parent   *semanticDependencyScope
	complete bool
	implicit bool
}

func newSemanticDependencies(input typeScope) *semanticDependencyScope {
	proof := &semanticDependencyScope{active: make(map[string]bool, len(input)), reads: make(map[string]bool), touched: make(map[string]bool), complete: true}
	for name := range input {
		proof.active[name] = true
	}
	return proof
}

func (proof *semanticDependencyScope) nested() *semanticDependencyScope {
	if proof == nil {
		return nil
	}
	return &semanticDependencyScope{active: maps.Clone(proof.active), reads: proof.reads, touched: make(map[string]bool), parent: proof, complete: proof.complete, implicit: proof.implicit}
}

func (proof *semanticDependencyScope) read(name string) {
	if proof != nil && proof.active[name] {
		proof.reads[name] = true
		proof.touched[name] = true
		proof.parent.read(name)
	}
}

func (proof *semanticDependencyScope) expression(expression string) {
	if proof == nil {
		return
	}
	for _, name := range expressionFreeVariables(expression) {
		proof.read(name)
	}
}

// semanticQueryScope is the shared walk's static result. Physical consumers
// use imports and projection clauses from this result; rows do not establish
// which names exist. A partial walk never proves an input can be omitted.
type semanticQueryScope struct {
	query     string
	execution string
	clauses   []pipelineClause
	branches  []*semanticQueryScope
	columns   []string
	returned  typeScope
	unchanged map[string]bool
	imports   typeScope
	globals   typeScope
	locals    map[string]bool
	complete  bool
	wildcard  bool
	proof     *semanticDependencyScope
}

type semanticSubqueryGlobalsKey struct{}

// Runtime names have unknown static types. Their models are immutable after
// the shared walk, independent of row values and storage contents. Typed
// compilation still calls the walk directly. Only completely proved models
// enter this bounded cache; dynamic schemas keep the general path.
type semanticRuntimeScopeKey struct {
	kind, query, tail, names string
	cypher25 bool
}

type semanticRuntimeScopeValue struct {
	model *semanticQueryScope
	dependent bool
}

var semanticRuntimeScopes = newBoundedCache[semanticRuntimeScopeKey, semanticRuntimeScopeValue](512, func(key semanticRuntimeScopeKey) int {
	return boundedCacheStringShardIndex(key.query) ^ boundedCacheStringShardIndex(key.tail)
})

func semanticScopeNames(names []string) string {
	sort.Strings(names)
	if len(names) == 1 { return names[0] }
	return strings.Join(names, "\x00")
}

func semanticRuntimeInput(names []string) typeScope {
	input := make(typeScope, len(names))
	for _, name := range names { input[name] = staticOperand{} }
	return input
}

func (e *StorageExecutor) runtimeScalarQueryScope(ctx context.Context, kind, body string, values map[string]interface{}) (*semanticQueryScope, error) {
	var buffer [8]string
	names := buffer[:0]
	for name := range values {
		if !strings.HasPrefix(name, "$") && !strings.HasPrefix(name, "\x00") { names = append(names, name) }
	}
	key := semanticRuntimeScopeKey{kind: kind, query: body, names: semanticScopeNames(names), cypher25: cypherVersionFromContext(ctx) == "25"}
	if cached, found := semanticRuntimeScopes.get(key); found { return cached.model, nil }
	model, err := e.scalarQueryScope(kind, body, semanticRuntimeInput(names), nil, key.cypher25)
	if err == nil && model.complete && model.proof.complete { semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model}) }
	return model, err
}

func (e *StorageExecutor) runtimeCallQueryScope(ctx context.Context, clause string, input typeScope) (*semanticQueryScope, error) {
	var buffer [8]string
	names := buffer[:0]
	for name := range input { names = append(names, name) }
	key := semanticRuntimeScopeKey{kind: "CALL", query: clause, names: semanticScopeNames(names), cypher25: cypherVersionFromContext(ctx) == "25"}
	if cached, found := semanticRuntimeScopes.get(key); found { return cached.model, nil }
	model, err := e.callQueryScope(clause, input, nil, key.cypher25)
	if err == nil && model != nil && model.complete && model.proof.complete { semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model}) }
	return model, err
}

func semanticDependencyKey(clauses []pipelineClause, names []string) semanticRuntimeScopeKey {
	key := semanticRuntimeScopeKey{kind: "QUERY", names: semanticScopeNames(names)}
	switch len(clauses) {
	case 1:
		key.query = clauses[0].text
	case 2:
		key.query, key.tail = clauses[0].text, clauses[1].text
	default:
		texts := make([]string, len(clauses))
		for index, clause := range clauses { texts[index] = clause.text }
		key.query = strings.Join(texts, " ")
	}
	return key
}

func semanticRuntimeDependencies(clauses []pipelineClause, names []string, cypher25 bool) bool {
	if len(names) == 0 { return false }
	if len(clauses) == 0 { return true }
	key := semanticDependencyKey(clauses, names)
	key.cypher25 = cypher25
	if cached, found := semanticRuntimeScopes.get(key); found { return cached.dependent }
	proof, complete := semanticRuntimeInput(names).pipelineDependenciesIn(clauses, cypher25)
	dependent := !complete || len(proof.reads) != 0
	// An incomplete analysis can only cache the conservative answer. A
	// cached omission always came from the same complete semantic walk.
	semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{dependent: dependent})
	return dependent
}

func (model *semanticQueryScope) executionQuery() string {
	if !model.complete {
		return model.query
	}
	if model.execution != "" { return model.execution }
	parts := make([]string, len(model.branches))
	for index, branch := range model.branches {
		texts := make([]string, len(branch.clauses))
		for offset, clause := range branch.clauses {
			texts[offset] = clause.text
		}
		parts[index] = strings.Join(texts, " ")
	}
	separator := " UNION "
	if _, all, _, union := parseTopLevelUnionBranches(model.query); union && all {
		separator = " UNION ALL "
	}
	return strings.Join(parts, separator)
}

func withSemanticSubqueryGlobals(ctx context.Context, values map[string]interface{}) context.Context {
	globals := make(pipelineRow)
	for name, value := range values {
		if !strings.HasPrefix(name, "$") && !strings.HasPrefix(name, "\x00") {
			globals[name] = value
		}
	}
	return context.WithValue(ctx, semanticSubqueryGlobalsKey{}, globals)
}

func scopeVisibleTypes(local, enclosing typeScope) typeScope {
	if len(enclosing) == 0 {
		return local
	}
	visible := maps.Clone(enclosing)
	maps.Copy(visible, local)
	return visible
}

// walkQueryScope keeps UNION branch entry scopes independent and validates
// their static projections, including branches which will produce no rows.
func (e *StorageExecutor) walkQueryScope(query string, initial typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	model := &semanticQueryScope{query: query, complete: true, proof: enclosing.dependencies}
	branches := []string{query}
	if parts, _, _, union := parseTopLevelUnionBranches(query); union {
		branches = parts
	}
	for _, branch := range branches {
		part := &semanticQueryScope{query: branch, locals: make(map[string]bool)}
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
		if !ok || len(clauses) == 0 {
			model.complete = false
			continue
		}
		part.clauses = slices.Clone(clauses)
		if last := clauses[len(clauses)-1]; last.kind == pipelineClauseReturn {
			part.wildcard = slices.Contains(pipelineReturnSourceColumns(last.text), "*")
		}
		frame := enclosing
		frame.visible = maps.Clone(enclosing.visible)
		frame.dependencies = enclosing.dependencies.nested()
		if frame.dependencies != nil && frame.dependencies.implicit && clauses[0].kind == pipelineClauseWith {
			for name := range projectTypeScope(initial, cypher25, clauses[0].text) {
				delete(frame.visible, name)
			}
		}
		frame.result = part
		part.proof = frame.dependencies
		_, complete, err := e.semanticScopeWalk(clauses, maps.Clone(initial), cypher25, frame)
		if err != nil {
			return model, err
		}
		part.complete = complete
		part.globals = frame.visible
		if part.proof != nil && !part.proof.complete {
			enclosing.dependencies.complete = false
		}
		model.complete = model.complete && part.complete
		if len(model.branches) == 0 {
			model.columns, model.returned, model.unchanged = part.columns, part.returned, part.unchanged
		} else if part.complete && !sameSemanticColumns(model.columns, part.columns) {
			if len(model.columns) == len(part.columns) {
				return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "DifferentColumnsInUnion", localization.CypherResidualUnionColumnNamesMismatch())
			}
			return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnionColumnMismatch",
				localization.CypherResidualUnionColumnCountMismatch(len(model.columns), len(part.columns)))
		}
		model.wildcard = model.wildcard || part.wildcard
		if len(model.branches) > 0 {
			for name := range model.unchanged {
				model.unchanged[name] = model.unchanged[name] && part.unchanged[name]
			}
		}
		model.branches = append(model.branches, part)
	}
	if len(model.branches) == 1 {
		model.clauses = model.branches[0].clauses
	}
	if enclosing.dependencies != nil {
		enclosing.dependencies.complete = enclosing.dependencies.complete && model.complete
	}
	model.execution = model.executionQuery()
	return model, nil
}

func sameSemanticColumns(left, right []string) bool {
	_, same := unionColumnOrder(left, right)
	return same
}

// observeScopeClause is part of the semantic walk, using its current binding
// scope and the canonical expression/projection readers. It does not infer
// scope from occurrence searches or from a row execution.
func (enclosing subqueryScope) observeScopeClause(clause pipelineClause, types typeScope) {
	proof := enclosing.dependencies
	if proof == nil {
		return
	}
	switch clause.kind {
	case pipelineClauseWith, pipelineClauseReturn:
		keyword := "WITH"
		if clause.kind == pipelineClauseReturn {
			keyword = "RETURN"
		}
		projection, tail := projectionSemanticBodyAndTail(clause.text, keyword)
		projection, _ = cutDistinct(projection)
		for _, raw := range splitTopLevelComma(projection) {
			expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
			if expression == "*" {
				for name := range types {
					proof.read(name)
				}
			} else {
				proof.expression(expression)
			}
		}
		proof.expression(tail)
	case pipelineClauseMatch, pipelineClauseOptionalMatch:
		proof.expression(clause.text)
	case pipelineClauseUnwind:
		proof.expression(unwindSourceExpression(clause.text))
	case pipelineClauseCallSubquery:
		// Its recursively walked model reports imports to this frame.
	default:
		// The shared walker may validate more clauses than an input-omission
		// proof understands. Such a clause keeps the general execution path.
		proof.complete = false
	}
}

func (enclosing subqueryScope) declarePatternLocals(types typeScope, clause pipelineClause) {
	proof := enclosing.dependencies
	if proof == nil || !proof.implicit {
		return
	}
	bindings := make(matchSemanticScope)
	addMatchPatternBindingKinds(bindings, clause.text[:patternPartEnd(clause)])
	for name := range bindings {
		if proof.active[name] && !proof.touched[name] {
			delete(types, name)
			delete(proof.active, name)
			delete(enclosing.visible, name)
			if enclosing.result != nil {
				enclosing.result.locals[name] = true
			}
		}
	}
}

func (enclosing subqueryScope) projectedScope(clause pipelineClause, local typeScope, view staticTypeScope) error {
	if clause.kind != pipelineClauseReturn {
		return nil
	}
	projection, tail := projectionSemanticBodyAndTail(clause.text, "RETURN")
	body, distinct := cutDistinct(projection)
	var expanded []string
	wildcard := false
	for _, raw := range splitTopLevelComma(body) {
		if strings.TrimSpace(raw) != "*" {
			expanded = append(expanded, raw)
			continue
		}
		wildcard = true
		if len(local) == 0 {
			return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "NoVariablesInScope", localization.CypherMatchingReturnStarNoVariables())
		}
		names := make([]string, 0, len(local))
		for name := range local {
			names = append(names, name)
		}
		sort.Strings(names)
		expanded = append(expanded, names...)
	}
	result := enclosing.result
	if result == nil {
		return nil
	}
	result.wildcard = wildcard
	result.returned = projectLocalTypeScope(local, scopeVisibleTypes(local, enclosing.visible), view, clause.text)
	result.unchanged = make(map[string]bool)
	local.forEachProjectedBinding(clause.text, func(name, expression string, inherited bool) {
		result.columns = append(result.columns, name)
		if enclosing.dependencies != nil && enclosing.dependencies.active[name] && simpleSemanticIdentifier(unwrapOuterParens(expression)) == name {
			result.unchanged[name] = true
		}
	})
	if wildcard {
		keyword := "RETURN "
		if distinct {
			keyword = "RETURN DISTINCT "
		}
		for index := range result.clauses {
			if result.clauses[index].kind == pipelineClauseReturn {
				result.clauses[index].text = keyword + strings.Join(expanded, ", ") + " " + tail
			}
		}
	}
	return nil
}

func (e *StorageExecutor) scalarQueryScope(kind, body string, visible typeScope, parent *semanticDependencyScope, cypher25 bool) (*semanticQueryScope, error) {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "(") {
		body = "MATCH " + body
	}
	proof := parent.nested()
	if proof == nil {
		proof = newSemanticDependencies(visible)
	}
	proof.implicit = false
	model, err := e.walkQueryScope(body, typeScope{}, subqueryScope{visible: visible, outer: true, dependencies: proof}, cypher25)
	if err != nil {
		return model, err
	}
	if kind == "COLLECT" && (model.wildcard || model.complete && len(model.columns) != 1) {
		return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery", localization.CypherSubqueriesCollectSingleColumn(len(model.columns)))
	}
	return model, nil
}

// callQueryScope is the single CALL import/branch scope contract. Scoped
// imports are named globals, outside the body's wildcard scope. Legacy WITH
// imports and #907 implicit imports enter the ordinary current scope instead.
func (e *StorageExecutor) callQueryScope(call string, outer typeScope, parent *semanticDependencyScope, cypher25 bool) (*semanticQueryScope, error) {
	body, _, _, _ := e.parseCallSubquery(call)
	if _, _, err := readCallScopeClause(call); err != nil {
		return nil, err
	}
	imports, scoped, ok := callSubqueryImports(call, outer)
	if !ok {
		return nil, nil
	}
	if scoped {
		for _, name := range parseCallSubqueryImportVariables(call) {
			if simpleSemanticIdentifier(name) == "" {
				return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(name))
			}
			if _, exists := outer[name]; !exists {
				return nil, createUndefinedVariableError(name)
			}
		}
	}
	if !scoped {
		branches := []string{body}
		if parts, _, _, union := parseTopLevelUnionBranches(body); union {
			branches = parts
		}
		for _, branch := range branches {
			clauses, parsed := splitPipelineClausesAllowingProcedureCalls(branch)
			if !parsed || len(clauses) == 0 || clauses[0].kind != pipelineClauseWith {
				continue
			}
			projection, tail := projectionSemanticBodyAndTail(clauses[0].text, "WITH")
			importing := false
			for _, raw := range splitTopLevelComma(projection) {
				expression, _ := parseProjectionExprAlias(strings.TrimSpace(raw))
				if expression == "*" {
					importing = true
				}
				for _, name := range expressionFreeVariables(expression) {
					if _, exists := outer[name]; exists {
						importing = true
					}
				}
			}
			if importing && (strings.TrimSpace(tail) != "" || topLevelKeywordIndex(projection, "AS") >= 0) {
				return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(projection))
			}
			if importing && len(clauses) == 1 {
				return nil, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSubqueriesWithQueryClauseRequired())
			}
		}
	}
	proof := newSemanticDependencies(imports)
	proof.implicit = !scoped
	frame := subqueryScope{dependencies: proof, visible: imports}
	initial := imports
	if scoped {
		frame.visible = imports
		initial = typeScope{}
	}
	model, err := e.walkQueryScope(body, initial, frame, cypher25)
	if err != nil {
		return model, err
	}
	model.imports = make(typeScope)
	for _, name := range model.columns {
		if _, bound := outer[name]; bound && !model.unchanged[name] {
			return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "VariableAlreadyBound", localization.CypherCoreVariableDeclaredInOuterScope(name))
		}
	}
	for name, operand := range imports {
		if scoped || !model.complete || !proof.complete || proof.reads[name] {
			model.imports[name] = operand
			parent.read(name)
		}
	}
	if parent != nil && (!model.complete || !proof.complete) {
		parent.complete = false
	}
	return model, nil
}

// readCallScopeClause is the import-header reader for validation, semantic
// scope construction and execution. nil names denotes (*); an empty non-nil
// list denotes (). Unscoped CALL has scoped == false.
func readCallScopeClause(call string) (names []string, scoped bool, err error) {
	parsed := readCallSubqueryScope(call)
	if parsed.invalid {
		return nil, parsed.scoped, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(parsed.text))
	}
	return parsed.names, parsed.scoped, nil
}
