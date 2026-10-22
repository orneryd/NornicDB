package cypher

import (
	"bytes"
	"context"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync/atomic"

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

func (proof *semanticDependencyScope) incomplete() {
	if proof != nil {
		proof.complete = false
		proof.parent.incomplete()
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

	// Runtime instantiation uses only these local names, never their types
	// or literal values. stableNames excludes value-named projections from
	// literal-normalized metadata reuse.
	projectionLocals typeScope
	stableNames      bool
}

type semanticSubqueryGlobalsKey struct{}

// Runtime names have unknown static types. Their models are immutable after
// the shared walk, independent of row values and storage contents. Typed
// compilation still calls the walk directly. Completely proved CALL facts with
// stable binding names may share the existing literal-normalized compile key,
// but each execution binds its own source text. Value-named columns and dynamic
// schemas retain the exact-text or general path.
type semanticRuntimeScopeKind uint8

const (
	semanticRuntimeQuery semanticRuntimeScopeKind = iota
	semanticRuntimeCall
	semanticRuntimeExists
	semanticRuntimeCount
	semanticRuntimeCollect
)

type semanticRuntimeScopeKey struct {
	query, tail, third, names string
	kind                      semanticRuntimeScopeKind
	cypher25                  bool
}

// A compiled expression retains one complete, immutable scope proof. Its
// schema guard compares every row key, never row values or storage results.
// The enclosing expression caches bound the number of these entries; wide
// or changing schemas continue through the shared runtime cache.
type semanticScalarScopeCache struct {
	entry atomic.Pointer[semanticScalarScopeEntry]
}

type semanticScalarScopeEntry struct {
	kind, body string
	cypher25   bool
	names      []string
	model      *semanticQueryScope
}

func (cache *semanticScalarScopeCache) get(kind, body string, cypher25 bool, values map[string]interface{}) *semanticQueryScope {
	if cache == nil {
		return nil
	}
	entry := cache.entry.Load()
	if entry == nil || entry.kind != kind || entry.body != body || entry.cypher25 != cypher25 || len(entry.names) != len(values) {
		return nil
	}
	for _, name := range entry.names {
		if _, found := values[name]; !found {
			return nil
		}
	}
	return entry.model
}

func (cache *semanticScalarScopeCache) put(kind, body string, cypher25 bool, values map[string]interface{}, model *semanticQueryScope) {
	if cache == nil || len(values) > 32 {
		return
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	cache.entry.Store(&semanticScalarScopeEntry{kind: kind, body: body, cypher25: cypher25, names: names, model: model})
}

type semanticRuntimeScopeValue struct {
	model      *semanticQueryScope
	dependency *semanticRuntimeDependencyProof
}

// The same immutable proof backs the bounded cache and its last-read memo.
// Repeated reads compare the complete key instead of hashing its query and
// schema again. A different query, schema or language mode still uses the
// bounded cache and the same semantic walk; no row values are retained.
type semanticRuntimeDependencyProof struct {
	key       semanticRuntimeScopeKey
	dependent bool
}

var semanticLastRuntimeDependency atomic.Pointer[semanticRuntimeDependencyProof]

var semanticRuntimeScopes = newBoundedCache[semanticRuntimeScopeKey, semanticRuntimeScopeValue](512, func(key semanticRuntimeScopeKey) int {
	return boundedCacheStringShardIndex(key.query) ^ boundedCacheStringShardIndex(key.tail)
})

func semanticScopeNames(names []string) string {
	if len(names) == 1 {
		return names[0]
	}
	sort.Strings(names)
	return strings.Join(names, "\x00")
}

func semanticRuntimeInput(names []string) typeScope {
	input := make(typeScope, len(names))
	for _, name := range names {
		input[name] = staticOperand{}
	}
	return input
}

func (e *StorageExecutor) runtimeScalarQueryScope(ctx context.Context, kind, body string, values map[string]interface{}, cache *semanticScalarScopeCache) (*semanticQueryScope, error) {
	cypher25 := cypherVersionFromContext(ctx) == "25"
	if model := cache.get(kind, body, cypher25, values); model != nil {
		return model, nil
	}
	var scopeKind semanticRuntimeScopeKind
	switch kind {
	case "EXISTS":
		scopeKind = semanticRuntimeExists
	case "COUNT":
		scopeKind = semanticRuntimeCount
	case "COLLECT":
		scopeKind = semanticRuntimeCollect
	}
	var buffer [8]string
	names := buffer[:0]
	for name := range values {
		if semanticBindingName(name) {
			names = append(names, name)
		}
	}
	key := semanticRuntimeScopeKey{kind: scopeKind, query: body, names: semanticScopeNames(names), cypher25: cypher25}
	if scopeKind != semanticRuntimeQuery {
		if cached, found := semanticRuntimeScopes.get(key); found {
			cache.put(kind, body, cypher25, values, cached.model)
			return cached.model, nil
		}
	}
	model, err := e.scalarQueryScope(kind, body, semanticRuntimeInput(names), nil, key.cypher25)
	if err == nil && model.complete && model.proof.complete && scopeKind != semanticRuntimeQuery {
		semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model})
		cache.put(kind, body, cypher25, values, model)
	}
	return model, err
}

func (e *StorageExecutor) runtimeCallQueryScope(ctx context.Context, clause, body string, names []string) (*semanticQueryScope, error) {
	key := semanticRuntimeScopeKey{kind: semanticRuntimeCall, query: clause, names: semanticScopeNames(names), cypher25: cypherVersionFromContext(ctx) == "25"}
	if cached, found := semanticRuntimeScopes.get(key); found {
		return cached.model, nil
	}
	return e.runtimeCallQueryScopeMiss(key, clause, body, names)
}

func (e *StorageExecutor) runtimeCallQueryScopeMiss(key semanticRuntimeScopeKey, clause, body string, names []string) (*semanticQueryScope, error) {
	cached, canonical, normalized, found := semanticCanonicalCallScope(key)
	if found {
		// As in the existing compile-verdict cache, pagination remains
		// value-dependent and is checked on the current source.
		if containsFold(body, "SKIP") || containsFold(body, "LIMIT") {
			if err := e.validateStaticPaginationExpressions(body); err != nil {
				return nil, err
			}
		}
		// Cache only immutable scope facts. The CALL operator already
		// owns this request's parsed body and binds its own clauses.
		semanticRuntimeScopes.put(key, cached)
		return cached.model, nil
	}
	model, err := e.callQueryScope(clause, semanticRuntimeInput(names), subqueryScope{}, key.cypher25)
	if err == nil && model != nil && model.complete && model.proof.complete {
		semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model})
		if model.stableNames && normalized {
			key.query = canonical
			semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model})
		}
	}
	return model, err
}

// Canonical metadata is looked up through the same literal reader used by
// compile validation. Only misses retain a normalized string. Exact-query
// entries are still populated by the caller, preserving repeated-query hits.
func semanticCanonicalCallScope(key semanticRuntimeScopeKey) (semanticRuntimeScopeValue, string, bool, bool) {
	var buffer [1024]byte
	length := 0
	overflow := false
	changed := writeNormalizedLiteralText(key.query, func(part string) {
		if overflow || len(part) > len(buffer)-length {
			overflow = true
			return
		}
		length += copy(buffer[length:], part)
	})
	if !changed {
		return semanticRuntimeScopeValue{}, "", false, false
	}
	if overflow {
		canonical, _ := normalizeLiteralText(key.query)
		key.query = canonical
		value, found := semanticRuntimeScopes.get(key)
		return value, canonical, true, found
	}
	// A non-escaping byte-to-string conversion can still allocate for longer
	// text. Read the bounded shard with borrowed bytes, retaining a canonical
	// string only when the metadata is absent.
	lookup := key
	lookup.query = ""
	query := buffer[:length]
	shard := boundedCacheBytesShardIndex(query) ^ boundedCacheStringShardIndex(lookup.tail)
	value, found := semanticRuntimeScopes.getMatchingFromShard(shard, func(candidate semanticRuntimeScopeKey) bool {
		canonical := candidate.query
		candidate.query = ""
		return candidate == lookup && bytes.Equal(query, []byte(canonical))
	})
	if found {
		return value, "", true, true
	}
	return value, string(buffer[:length]), true, false
}

func semanticDependencyKey(clauses []pipelineClause, names []string) semanticRuntimeScopeKey {
	key := semanticRuntimeScopeKey{kind: semanticRuntimeQuery, names: semanticScopeNames(names)}
	switch len(clauses) {
	case 1:
		key.query = clauses[0].text
	case 2:
		key.query, key.tail = clauses[0].text, clauses[1].text
	case 3:
		key.query, key.tail, key.third = clauses[0].text, clauses[1].text, clauses[2].text
	default:
		texts := make([]string, len(clauses))
		for index, clause := range clauses {
			texts[index] = clause.text
		}
		key.query = strings.Join(texts, " ")
	}
	return key
}

func semanticRuntimeDependencies(clauses []pipelineClause, names []string, cypher25 bool) bool {
	if len(names) == 0 {
		return false
	}
	if len(clauses) == 0 {
		return true
	}
	key := semanticDependencyKey(clauses, names)
	key.cypher25 = cypher25
	if proof := semanticLastRuntimeDependency.Load(); proof != nil && proof.key == key {
		return proof.dependent
	}
	if cached, found := semanticRuntimeScopes.get(key); found {
		semanticLastRuntimeDependency.Store(cached.dependency)
		return cached.dependency.dependent
	}
	proof, complete := semanticRuntimeInput(names).pipelineDependenciesIn(clauses, cypher25)
	dependent := !complete || len(proof.reads) != 0
	// An incomplete analysis can only cache the conservative answer. A
	// cached omission always came from the same complete semantic walk.
	entry := &semanticRuntimeDependencyProof{key: key, dependent: dependent}
	semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{dependency: entry})
	semanticLastRuntimeDependency.Store(entry)
	return dependent
}

func (model *semanticQueryScope) executionQuery() string {
	if !model.complete {
		return model.query
	}
	if !model.wildcard {
		return model.query
	}
	if model.execution != "" {
		return model.execution
	}
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

// runtimeClauses binds immutable scope facts to the current branch program.
// The caller owns clauses; no cached expression from a literal-normalized
// sibling may execute here. Only wildcard projections need a source rewrite.
func (model *semanticQueryScope) runtimeClauses(query string, clauses []pipelineClause) []pipelineClause {
	if !model.complete {
		return clauses
	}
	if model.query == query {
		return model.clauses
	}
	if model.wildcard {
		for offset, clause := range clauses {
			if clause.kind != pipelineClauseReturn {
				continue
			}
			var expanded []string
			model.projectionLocals.forEachProjectedBinding(clause.text, func(name, expression, text string, inherited bool) {
				expanded = append(expanded, text)
			})
			clauses[offset].text = expandedSemanticReturn(clause.text, expanded)
		}
	}
	return clauses
}

// selected is the fallback executor's emitted seed scope. Its carried globals
// must name exactly those bindings; nil denotes the pipeline's complete row.
func withSemanticSubqueryGlobals(ctx context.Context, values map[string]interface{}, selected []string) context.Context {
	globals := make(pipelineRow)
	if selected == nil {
		for name, value := range values {
			if semanticBindingName(name) {
				globals[name] = value
			}
		}
	} else {
		for _, name := range selected {
			globals[name] = values[name]
		}
	}
	return context.WithValue(ctx, semanticSubqueryGlobalsKey{}, globals)
}

func semanticBindingName(name string) bool {
	return !strings.HasPrefix(name, "$") && !strings.HasPrefix(name, "\x00")
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
	model := &semanticQueryScope{query: query, complete: true, stableNames: true, proof: enclosing.dependencies}
	branches := []string{query}
	if parts, _, _, union := parseTopLevelUnionBranches(query); union {
		branches = parts
	}
	for _, branch := range branches {
		part := &semanticQueryScope{query: branch, stableNames: true, locals: make(map[string]bool)}
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(branch)
		if !ok || len(clauses) == 0 {
			model.complete = false
			continue
		}
		part.clauses = clauses
		if last := clauses[len(clauses)-1]; last.kind == pipelineClauseReturn {
			part.wildcard = typeScope(nil).forEachProjectedBinding(last.text, func(string, string, string, bool) {})
		}
		frame := enclosing
		frame.visible = maps.Clone(enclosing.visible)
		frame.dependencies = enclosing.dependencies.nested()
		entry := initial
		if frame.dependencies != nil && frame.dependencies.implicit && clauses[0].kind == pipelineClauseWith {
			if imports, importing := callWithImports(clauses[0].text, enclosing.callLocals, initial); importing {
				projection, tail := projectionSemanticBodyAndTail(clauses[0].text, "WITH")
				if strings.TrimSpace(tail) != "" || topLevelKeywordIndex(projection, "AS") >= 0 {
					return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(projection))
				}
				if len(clauses) == 1 {
					return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", localization.CypherSubqueriesWithQueryClauseRequired())
				}
				// An importing WITH binds ordinary locals. Its star expands
				// the parent's current scope, while explicit names may also
				// read named ancestors. Selected names follow later WITH
				// horizons. Only unselected current-query locals become
				// #907 implicit globals; ancestor-only names must be named
				// explicitly and cannot enter through WITH *.
				entry = imports
				for name := range frame.visible {
					_, local := enclosing.callLocals[name]
					_, selected := imports[name]
					if !local || selected {
						delete(frame.visible, name)
					}
				}
			}
		}
		frame.result = part
		part.proof = frame.dependencies
		_, complete, err := e.semanticScopeWalk(clauses, maps.Clone(entry), cypher25, frame)
		if err != nil {
			return model, err
		}
		part.complete = complete
		part.globals = frame.visible
		if part.proof != nil && !part.proof.complete {
			enclosing.dependencies.incomplete()
		}
		model.complete = model.complete && part.complete
		model.stableNames = model.stableNames && part.stableNames
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
	if !model.complete {
		enclosing.dependencies.incomplete()
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
		_, tail := projectionSemanticBodyAndTail(clause.text, keyword)
		types.forEachProjectedBinding(clause.text, func(name, expression, text string, inherited bool) {
			if inherited {
				proof.read(name)
			} else {
				proof.expression(expression)
			}
		})
		if strings.TrimSpace(tail) != "" {
			proof.projectionTail(types, clause.text).expression(tail)
		}
	case pipelineClauseMatch, pipelineClauseOptionalMatch:
		proof.expression(clause.text)
	case pipelineClauseUnwind:
		proof.expression(unwindSourceExpression(clause.text))
	case pipelineClauseCallSubquery:
		// Its recursively walked model reports imports to this frame.
	default:
		// The shared walker may validate more clauses than an input-omission
		// proof understands. Such a clause keeps the general execution path.
		proof.incomplete()
	}
}

// Projection expressions read the incoming bindings. Their tails read aliases
// over those bindings, so a new alias cannot consume a same-named ancestor.
// Nested tail subqueries use this same provenance view.
func (proof *semanticDependencyScope) projectionTail(types typeScope, clause string) *semanticDependencyScope {
	if proof == nil {
		return nil
	}
	tail := proof.nested()
	types.forEachProjectedBinding(clause, func(name, expression, text string, inherited bool) {
		if !inherited && projectedSourceVariable(expression) != name {
			delete(tail.active, name)
		}
	})
	return tail
}

func (enclosing subqueryScope) bindPatternScope(types typeScope, clause pipelineClause) {
	proof := enclosing.dependencies
	if proof == nil {
		return
	}
	bindings := make(matchSemanticScope)
	addMatchPatternBindingKinds(bindings, clause.text[:patternPartEnd(clause)])
	for name := range bindings {
		if proof.implicit && proof.active[name] && !proof.touched[name] {
			delete(types, name)
			delete(proof.active, name)
			delete(enclosing.visible, name)
			if enclosing.result != nil {
				enclosing.result.locals[name] = true
			}
		} else {
			proof.read(name)
		}
	}
}

func (enclosing subqueryScope) projectedScope(clause pipelineClause, local typeScope, view staticTypeScope) error {
	if clause.kind != pipelineClauseReturn {
		return nil
	}
	result := enclosing.result
	var named typeScope
	if result != nil {
		result.projectionLocals = local
		result.returned = make(typeScope)
		result.unchanged = make(map[string]bool)
		named = scopeVisibleTypes(local, enclosing.visible)
		view.complete = false
	}
	wildcard := local.forEachProjectedBinding(clause.text, func(name, expression, text string, inherited bool) {
		if result == nil {
			return
		}
		result.columns = append(result.columns, name)
		if simpleSemanticIdentifier(name) == "" {
			if _, changed := normalizeLiteralText(name); changed {
				result.stableNames = false
			}
		}
		result.returned[name] = projectedBindingOperand(local, named, view, name, expression, inherited)
		if enclosing.dependencies != nil && enclosing.dependencies.active[name] && projectedSourceVariable(expression) == name {
			result.unchanged[name] = true
		}
	})
	if wildcard && len(local) == 0 {
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "NoVariablesInScope", localization.CypherMatchingReturnStarNoVariables())
	}
	if result == nil {
		return nil
	}
	result.wildcard = wildcard
	if wildcard {
		var expanded []string
		local.forEachProjectedBinding(clause.text, func(name, expression, text string, inherited bool) {
			expanded = append(expanded, text)
		})
		result.clauses = slices.Clone(result.clauses)
		for index := range result.clauses {
			if result.clauses[index].kind == pipelineClauseReturn {
				result.clauses[index].text = expandedSemanticReturn(clause.text, expanded)
			}
		}
	}
	return nil
}

func expandedSemanticReturn(clause string, expanded []string) string {
	_, tail := projectionSemanticBodyAndTail(clause, "RETURN")
	keyword := "RETURN "
	if _, distinct := cutDistinct(pipelineClauseBody(clause, "RETURN")); distinct {
		keyword = "RETURN DISTINCT "
	}
	return keyword + strings.Join(expanded, ", ") + " " + tail
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
	model, err := e.walkQueryScope(body, typeScope{}, subqueryScope{visible: visible, initial: visible, outer: true, dependencies: proof}, cypher25)
	if err != nil {
		return model, err
	}
	if kind == "COLLECT" && (model.wildcard || model.complete && len(model.columns) != 1) {
		return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery", localization.CypherSubqueriesCollectSingleColumn(len(model.columns)))
	}
	return model, nil
}

// callWithImports resolves legacy WITH's ordinary locals. Unlike CALL(*), its
// star does not include a CALL frame's global imports. Explicit expressions may
// read named ancestors. A constant-only WITH is the existing implicit-import
// extension, rather than an importing clause.
func callWithImports(clause string, local, visible typeScope) (typeScope, bool) {
	imports := make(typeScope)
	importing := local.forEachProjectedBinding(clause, func(name, expression, text string, inherited bool) {
		if inherited {
			imports[name] = local[name]
			return
		}
		for _, source := range expressionFreeVariables(expression) {
			if operand, exists := visible[source]; exists {
				imports[source] = operand
			}
		}
	})
	return imports, importing || len(imports) != 0
}

// callQueryScope is the single CALL import/branch scope contract. Scoped
// imports are named globals, outside the body's wildcard scope. Legacy WITH
// imports and #907 implicit imports enter the ordinary current scope instead.
func (e *StorageExecutor) callQueryScope(call string, local typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	body, _, _, _ := e.parseCallSubquery(call)
	if _, _, err := readCallScopeClause(call); err != nil {
		return nil, err
	}
	outer := scopeVisibleTypes(local, enclosing.visible)
	header := outer
	if enclosing.outer {
		// Scalar ancestors are available by name, but are outside the
		// scalar query's current scope. A CALL frame's global imports
		// do belong to its query scope and are included by CALL(*).
		header = local
	}
	imports, scoped, ok := callSubqueryImports(call, header, enclosing.visible)
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
	proof := newSemanticDependencies(imports)
	proof.implicit = !scoped
	frame := subqueryScope{dependencies: proof, visible: imports, initial: imports, callLocals: local}
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
			enclosing.dependencies.read(name)
		}
	}
	if !model.complete || !proof.complete {
		enclosing.dependencies.incomplete()
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
