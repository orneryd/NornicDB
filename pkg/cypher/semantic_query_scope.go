package cypher

import (
	"bytes"
	"context"
	"maps"
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
	parent   *semanticDependencyScope
	complete bool
}

func newSemanticDependencies(input typeScope) *semanticDependencyScope {
	proof := &semanticDependencyScope{active: make(map[string]bool, len(input)), reads: make(map[string]bool), complete: true}
	for name := range input {
		proof.active[name] = true
	}
	return proof
}

func (proof *semanticDependencyScope) nested() *semanticDependencyScope {
	if proof == nil {
		return nil
	}
	return &semanticDependencyScope{active: maps.Clone(proof.active), reads: make(map[string]bool), parent: proof, complete: proof.complete}
}

func (proof *semanticDependencyScope) read(name string) {
	if proof != nil && proof.active[name] {
		proof.reads[name] = true
		proof.parent.read(name)
	}
}

func (proof *semanticDependencyScope) expression(ctx context.Context, expression string) {
	if proof == nil {
		return
	}
	for _, name := range expressionFreeVariables(ctx, expression) {
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
	clauses   []pipelineClause
	branches  []*semanticQueryScope
	columns   []string
	returned  typeScope
	unchanged map[string]bool
	imports   typeScope
	bindings  *semanticBodyScope
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
	parent                    *semanticBodyScope
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
	parent     *semanticBodyScope
}

func (cache *semanticScalarScopeCache) get(kind, body string, cypher25 bool, values map[string]interface{}, parent *semanticBodyScope) *semanticQueryScope {
	if cache == nil {
		return nil
	}
	entry := cache.entry.Load()
	if entry == nil || entry.kind != kind || entry.body != body || entry.cypher25 != cypher25 || entry.parent != parent || len(entry.names) != len(values) {
		return nil
	}
	for _, name := range entry.names {
		if _, found := values[name]; !found {
			return nil
		}
	}
	return entry.model
}

func (cache *semanticScalarScopeCache) put(kind, body string, cypher25 bool, values map[string]interface{}, model *semanticQueryScope, parent *semanticBodyScope) {
	if cache == nil || len(values) > 32 {
		return
	}
	names := make([]string, 0, len(values))
	for name := range values {
		names = append(names, name)
	}
	cache.entry.Store(&semanticScalarScopeEntry{kind: kind, body: body, cypher25: cypher25, names: names, model: model, parent: parent})
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
	parent := semanticBodyFromContext(ctx)
	if model := cache.get(kind, body, cypher25, values, parent); model != nil {
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
	key := semanticRuntimeScopeKey{kind: scopeKind, query: body, names: semanticScopeNames(names), cypher25: cypher25, parent: parent}
	if scopeKind != semanticRuntimeQuery {
		if cached, found := semanticRuntimeScopes.get(key); found {
			cache.put(kind, body, cypher25, values, cached.model, parent)
			return cached.model, nil
		}
	}
	writtenNames, _ := ctx.Value(quotedVariablesKey{}).(*quotedVariableNames)
	if err := e.validateDuplicateReturnColumnName(ctx, body, writtenNames); err != nil {
		return nil, err
	}
	model, err := e.scalarQueryScope(ctx, kind, body, semanticRuntimeInput(names), nil, parent, key.cypher25)
	if err == nil && model.complete && model.proof.complete && scopeKind != semanticRuntimeQuery {
		semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{model: model})
		cache.put(kind, body, cypher25, values, model, parent)
	}
	return model, err
}

func (e *StorageExecutor) runtimeCallQueryScope(ctx context.Context, clause, body string, names []string) (*semanticQueryScope, error) {
	key := semanticRuntimeScopeKey{kind: semanticRuntimeCall, query: clause, names: semanticScopeNames(names), cypher25: cypherVersionFromContext(ctx) == "25", parent: semanticBodyFromContext(ctx)}
	if cached, found := semanticRuntimeScopes.get(key); found {
		return cached.model, nil
	}
	return e.runtimeCallQueryScopeMiss(ctx, key, clause, body, names)
}

func (e *StorageExecutor) runtimeCallQueryScopeMiss(ctx context.Context, key semanticRuntimeScopeKey, clause, body string, names []string) (*semanticQueryScope, error) {
	cached, canonical, normalized, found := semanticCanonicalCallScope(key)
	if found {
		// As in the existing compile-verdict cache, pagination remains
		// value-dependent and is checked on the current source.
		if containsFold(body, "SKIP") || containsFold(body, "LIMIT") {
			if err := e.validateStaticPaginationExpressions(ctx, extractPaginationExpressions(ctx, body)...); err != nil {
				return nil, err
			}
		}
		// Cache only immutable scope facts. The CALL operator already
		// owns this request's parsed body and binds its own clauses.
		semanticRuntimeScopes.put(key, cached)
		return cached.model, nil
	}
	// Syntax uses the original projection spelling; the cached scope below
	// uses canonical binding identities. Neither decision depends on rows.
	writtenNames, _ := ctx.Value(quotedVariablesKey{}).(*quotedVariableNames)
	if err := e.validateDuplicateReturnColumnName(ctx, clause, writtenNames); err != nil {
		return nil, err
	}
	input := key.parent.runtimeCurrent(names)
	frame := subqueryScope{bindings: key.parent}
	if key.parent != nil {
		frame.outer = key.parent.scalar
	}
	model, err := e.callQueryScope(ctx, clause, input, frame, key.cypher25)
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

func semanticRuntimeDependencies(ctx context.Context, clauses []pipelineClause, names []string, cypher25 bool, parent ...*semanticBodyScope) bool {
	if len(names) == 0 {
		return false
	}
	if len(clauses) == 0 {
		return true
	}
	key := semanticDependencyKey(clauses, names)
	key.cypher25 = cypher25
	if len(parent) != 0 {
		key.parent = parent[0]
	}
	if proof := semanticLastRuntimeDependency.Load(); proof != nil && proof.key == key {
		return proof.dependent
	}
	if cached, found := semanticRuntimeScopes.get(key); found {
		semanticLastRuntimeDependency.Store(cached.dependency)
		return cached.dependency.dependent
	}
	proof, complete := semanticRuntimeInput(names).pipelineDependenciesIn(ctx, clauses, cypher25, key.parent)
	dependent := !complete || len(proof.reads) != 0
	// An incomplete analysis can only cache the conservative answer. A
	// cached omission always came from the same complete semantic walk.
	entry := &semanticRuntimeDependencyProof{key: key, dependent: dependent}
	semanticRuntimeScopes.put(key, semanticRuntimeScopeValue{dependency: entry})
	semanticLastRuntimeDependency.Store(entry)
	return dependent
}

func (model *semanticQueryScope) executionQuery() string {
	return model.query
}

// Canonical clauses retain their current source and syntax identity. Scope
// snapshots attach to those clauses; cached rewritten bodies never execute.
func (model *semanticQueryScope) runtimeClauses(query string, clauses []pipelineClause) []pipelineClause {
	if model.complete {
		for index := range clauses {
			if index < len(model.clauses) {
				clauses[index].bindings = model.clauses[index].bindings
			}
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
func (e *StorageExecutor) walkQueryScope(ctx context.Context, query string, initial typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	model := &semanticQueryScope{query: query, complete: true, stableNames: true, proof: enclosing.dependencies}
	branches := []string{query}
	if parts, _, _, union := parseTopLevelUnionBranches(ctx, query); union {
		branches = parts
	}
	for branchIndex, branch := range branches {
		part := &semanticQueryScope{query: branch, stableNames: true, locals: make(map[string]bool)}
		clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, branch)
		if !ok || len(clauses) == 0 {
			model.complete = false
			continue
		}
		part.clauses = clauses
		if last := clauses[len(clauses)-1]; last.kind == pipelineClauseReturn {
			part.wildcard = typeScope(nil).forEachProjectedBinding(ctx, last.text, func(string, string, string, bool) {})
		}
		frame := enclosing
		frame.bindings = enclosing.bindings.clone(initial)
		frame.dependencies = enclosing.dependencies.nested()
		entry, entryErr := frame.bindings.branchEntry(ctx, clauses, branchIndex, initial)
		if entryErr != nil {
			return model, entryErr
		}
		frame.result = part
		part.proof = frame.dependencies
		_, complete, err := e.semanticScopeWalk(ctx, clauses, maps.Clone(entry), cypher25, frame)
		if err != nil {
			return model, err
		}
		part.complete = complete
		part.bindings = frame.bindings
		if part.proof != nil && !part.proof.complete {
			enclosing.dependencies.incomplete()
		}
		model.complete = model.complete && part.complete
		model.stableNames = model.stableNames && part.stableNames
		if len(model.branches) == 0 {
			model.columns, model.returned, model.unchanged = part.columns, maps.Clone(part.returned), part.unchanged
		} else if part.complete && !sameSemanticColumns(model.columns, part.columns) {
			if len(model.columns) == len(part.columns) {
				return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "DifferentColumnsInUnion", localization.CypherResidualUnionColumnNamesMismatch())
			}
			return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "UnionColumnMismatch",
				localization.CypherResidualUnionColumnCountMismatch(len(model.columns), len(part.columns)))
		}
		if len(model.branches) > 0 {
			for name, operand := range model.returned {
				model.returned[name] = joinedOperand(operand, part.returned[name])
			}
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
	return model, nil
}

func sameSemanticColumns(left, right []string) bool {
	_, same := unionColumnOrder(left, right)
	return same
}

// observeScopeClause is part of the semantic walk, using its current binding
// scope and the canonical expression/projection readers. It does not infer
// scope from occurrence searches or from a row execution.
func (enclosing subqueryScope) observeScopeClause(ctx context.Context, clause pipelineClause, types typeScope) {
	proof := enclosing.dependencies
	if proof == nil && enclosing.bindings == nil {
		return
	}
	enclosing.bindings.readClause(ctx, clause, types, proof)
	switch clause.kind {
	case pipelineClauseWith, pipelineClauseReturn:
		enclosing.bindings.projection(types).forEachProjectedBinding(ctx, clause.text, func(name, expression, text string, inherited bool) {
			if inherited {
				proof.read(name)
			}
		})
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseUnwind, pipelineClauseCallSubquery, pipelineClauseLet:
	default:
		proof.incomplete()
	}
}

// Projection expressions read the incoming bindings. Their tails read aliases
// over those bindings, so a new alias cannot consume a same-named ancestor.
// Nested tail subqueries use this same provenance view.
func (proof *semanticDependencyScope) projectionTail(ctx context.Context, types typeScope, clause string) *semanticDependencyScope {
	if proof == nil {
		return nil
	}
	tail := proof.nested()
	types.forEachProjectedBinding(ctx, clause, func(name, expression, text string, inherited bool) {
		if !inherited && projectedSourceVariable(expression) != name {
			delete(tail.active, name)
		}
	})
	return tail
}

// bindPatternScope captures declarations before predicate masking. The same
// names establish provenance and carry validated kinds to the next clause.
func (enclosing subqueryScope) bindPatternScope(ctx context.Context, types typeScope, clause pipelineClause) matchSemanticScope {
	switch clause.kind {
	case pipelineClauseMatch, pipelineClauseOptionalMatch, pipelineClauseCreate, pipelineClauseMerge:
	default:
		return nil
	}
	bindings := make(matchSemanticScope)
	addMatchPatternBindingKinds(ctx, bindings, clause.text[:patternPartEnd(clause)])
	for name, kind := range bindings {
		enclosing.bindings.pattern(name, kind, types, enclosing.dependencies, enclosing.result)
	}
	return bindings
}

func (enclosing subqueryScope) projectedScope(ctx context.Context, clause pipelineClause, local typeScope, view staticTypeScope) error {
	if clause.kind != pipelineClauseReturn {
		return nil
	}
	result := enclosing.result
	var named typeScope
	if result != nil {
		result.projectionLocals = local
		result.returned = make(typeScope)
		result.unchanged = make(map[string]bool)
		named = enclosing.bindings.visible(local)
		view.complete = false
	}
	projection := enclosing.bindings.projection(local)
	wildcard := projection.forEachProjectedBinding(ctx, clause.text, func(name, expression, text string, inherited bool) {
		if result == nil {
			return
		}
		result.columns = append(result.columns, name)
		if simpleSemanticIdentifier(name) == "" {
			if _, changed := normalizeLiteralText(name); changed {
				result.stableNames = false
			}
		}
		result.returned[name] = projectedBindingOperand(ctx, projection, named, view, name, expression, inherited)
		if enclosing.dependencies != nil && enclosing.dependencies.active[name] && projectedSourceVariable(expression) == name {
			result.unchanged[name] = true
		}
	})
	if err := returnStarScopeError(wildcard, len(local)); err != nil {
		return err
	}
	if result == nil {
		return nil
	}
	result.wildcard = wildcard
	return nil
}

func (e *StorageExecutor) scalarQueryScope(ctx context.Context, kind, body string, visible typeScope, parent *semanticDependencyScope, bindingParent *semanticBodyScope, cypher25 bool) (*semanticQueryScope, error) {
	body = strings.TrimSpace(body)
	if strings.HasPrefix(body, "(") {
		body = "MATCH " + body
	}
	proof := parent.nested()
	if proof == nil {
		proof = newSemanticDependencies(visible)
	}
	frame := bindingParent.scalarChild(visible)
	model, err := e.walkQueryScope(ctx, body, typeScope{}, subqueryScope{bindings: frame, outer: true, dependencies: proof}, cypher25)
	if err != nil {
		return model, err
	}
	model.imports = make(typeScope)
	for name, operand := range visible {
		if !model.complete || !proof.complete || proof.reads[name] {
			model.imports[name] = operand
		}
	}
	if kind == "COLLECT" && (model.wildcard || model.complete && len(model.columns) != 1) {
		return model, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidCollectSubquery", localization.CypherSubqueriesCollectSingleColumn(len(model.columns)))
	}
	return model, nil
}

// scalarInput uses the same proof as the semantic frame. An unused weak
// ancestor must not turn a scalar body's fresh MATCH into a bound-node read.
func (model *semanticQueryScope) scalarInput(values map[string]interface{}) map[string]interface{} {
	if !model.complete || !model.proof.complete {
		return values
	}
	for name := range values {
		if semanticBindingName(name) {
			if _, used := model.imports[name]; !used {
				selected := make(map[string]interface{}, len(model.imports))
				for key, value := range values {
					if _, used := model.imports[key]; used || !semanticBindingName(key) {
						selected[key] = value
					}
				}
				return selected
			}
		}
	}
	return values
}

// callWithImports resolves legacy WITH's ordinary locals. Unlike CALL(*), its
// star does not include a CALL frame's global imports. Explicit expressions may
// read named ancestors. A constant-only WITH is the existing implicit-import
// extension, rather than an importing clause.
func callWithImports(ctx context.Context, clause string, local, visible typeScope) (typeScope, bool) {
	imports := make(typeScope)
	importing := local.forEachProjectedBinding(ctx, clause, func(name, expression, text string, inherited bool) {
		if inherited {
			imports[name] = local[name]
			return
		}
		for _, source := range expressionFreeVariables(ctx, expression) {
			if operand, exists := visible[source]; exists {
				imports[source] = operand
			}
		}
	})
	return imports, importing || len(imports) != 0
}

// callQueryScope is the single CALL import/branch scope contract. Scoped
// imports remain visible across WITH and enter its projection scope on lookup.
// #907 implicit imports are tagged weak and can be shadowed by declarations.
func (e *StorageExecutor) callQueryScope(ctx context.Context, call string, local typeScope, enclosing subqueryScope, cypher25 bool) (*semanticQueryScope, error) {
	names, scoped, scopeErr := readCallScopeClause(ctx, call)
	if scopeErr != nil {
		return nil, scopeErr
	}
	if !startsWithCallSubquery(ctx, call) {
		return nil, nil
	}
	body, _, _, _ := e.parseCallSubquery(ctx, call)
	if body == "" {
		return nil, nil
	}
	imports, bindings, err := callBodyScope(names, scoped, local, enclosing.bindings, enclosing.outer)
	if err != nil {
		return nil, err
	}
	proof := newSemanticDependencies(imports)
	initial := bindings.entry
	frame := subqueryScope{dependencies: proof, bindings: bindings}
	model, err := e.walkQueryScope(ctx, body, initial, frame, cypher25)
	if err != nil {
		return model, err
	}
	model.imports = make(typeScope)
	strongOuter := enclosing.bindings.strong(local)
	for _, name := range model.columns {
		if _, bound := strongOuter[name]; bound && !model.unchanged[name] {
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
func readCallScopeClause(ctx context.Context, call string) (names []string, scoped bool, err error) {
	parsed := readCallSubqueryScope(ctx, call)
	if parsed.invalid {
		return nil, parsed.scoped, localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidVariableImport", localization.CypherSubqueriesWithImportExpressionInvalid(parsed.text))
	}
	return parsed.names, parsed.scoped, nil
}
