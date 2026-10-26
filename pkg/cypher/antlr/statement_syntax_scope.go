// Author: Codex R17 implementation agent, 26 October 2026.
package antlr

import (
	"context"
	"strings"
	"sync"
)

// WithStatementSyntax retains the existing grammar products for one execution.
// Derived inspection windows share their CST; the process cache's capacity does
// not control their lifetime. At most the original and normalized statement
// products per dialect are retained. Independently parsed adapter fragments
// are not added. Indexes and zero-copy views are bounded by those products.
// For example, a CALL reader can inspect its already parsed clause and suffix
// while the enclosing statement continues to own all node and token storage.
func WithStatementSyntax(ctx context.Context, sources ...string) context.Context {
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.Lock()
		same := len(sources) == 0 || sources[0] == scope.original || sources[0] == scope.canonical
		if same && len(sources) > 1 && scope.canonical != sources[1] {
			scope.replaceCurrentLocked(sources[1])
		}
		scope.Unlock()
		if same {
			return ctx
		}
	}
	scope := &statementSyntaxScope{}
	if len(sources) > 0 {
		scope.original = sources[0]
	}
	if len(sources) > 1 {
		scope.canonical = sources[1]
	}
	return context.WithValue(ctx, statementSyntaxScopeKey{}, scope)
}

// Rewrites may finish after an earlier spelling was already inspected. Keep
// the original and replace the previous current product, including its views.
// Published syntax values remain immutable; obsolete products get no additional
// lifetime from this execution scope.
func (scope *statementSyntaxScope) replaceCurrentLocked(source string) {
	originals := make(map[statementSyntaxKey]*statementSyntaxData)
	for key, data := range scope.products {
		if key.source == scope.original && key.entry == StatementEntry {
			originals[key] = data
		}
	}
	scope.products, scope.windows, scope.calls, scope.indexed = nil, nil, nil, nil
	scope.canonical = source
	for key, data := range originals {
		scope.retainLocked(key, data)
	}
}

type statementSyntaxScopeKey struct{}
type syntaxInspectionKey struct {
	source   string
	cypher25 bool
	entry    SyntaxEntry
}
type syntaxInspection struct {
	data             *statementSyntaxData
	node, start, end int
	ambiguous        bool
}
type statementSyntaxScope struct {
	sync.Mutex
	original, canonical string
	prepared            bool
	products            map[statementSyntaxKey]*statementSyntaxData
	windows, calls      map[syntaxInspectionKey]syntaxInspection
	indexed             map[*statementSyntaxData]bool
}

// Seed lazily from an already cached original, including when normalization's
// own cache avoided parsing it this time. A result-cache hit need not allocate
// syntax indexes. No new parse is performed here.
func (scope *statementSyntaxScope) prepare() {
	scope.Lock()
	if scope.prepared {
		scope.Unlock()
		return
	}
	scope.prepared = true
	original := scope.original
	scope.Unlock()
	statementSyntaxCache.RLock()
	data := statementSyntaxCache.entries[statementSyntaxKey{source: original, entry: StatementEntry}]
	if data == nil {
		data = statementSyntaxCache.entries[statementSyntaxKey{source: original, entry: fragmentEntry}]
	}
	statementSyntaxCache.RUnlock()
	if data != nil {
		scope.retain(statementSyntaxKey{source: original, entry: StatementEntry}, data)
	}
}

// Establish the declared whole statement before inspecting its fragments.
// A failed admission is left to the caller's existing error owner; partial
// products are never retained. Recursion ends at that exact whole-source entry.
func (scope *statementSyntaxScope) ensureRoot(ctx context.Context, source string, cypher25 bool) {
	scope.prepare()
	scope.Lock()
	root := scope.canonical
	if root == "" {
		root = scope.original
	}
	retained := scope.products[statementSyntaxKey{source: root, entry: StatementEntry, cypher25: cypher25}] != nil
	scope.Unlock()
	if root != "" && root != source && !retained {
		_, _ = ParseSyntax(ctx, root, StatementEntry, cypher25)
	}
}

func statementSyntaxScopeFrom(ctx context.Context) *statementSyntaxScope {
	scope, _ := ctx.Value(statementSyntaxScopeKey{}).(*statementSyntaxScope)
	return scope
}

func (scope *statementSyntaxScope) product(key statementSyntaxKey) *statementSyntaxData {
	scope.prepare()
	scope.Lock()
	defer scope.Unlock()
	if data := scope.products[key]; data != nil {
		return data
	}
	if key.entry == StatementEntry || key.entry == fragmentEntry {
		return nil
	}
	view := scope.windows[syntaxInspectionKey{key.source, key.cypher25, key.entry}]
	if view.data == nil || view.ambiguous {
		return nil
	}
	data := view.rebase(key.source)
	scope.products[key] = data
	return data
}

// retain indexes only grammar-produced extents. It does not search input text
// to invent a source association. Ambiguous equal-text occurrences remain
// standalone entry requests until their caller carries a particular node.
func (scope *statementSyntaxScope) retain(key statementSyntaxKey, data *statementSyntaxData) *statementSyntaxData {
	scope.Lock()
	defer scope.Unlock()
	return scope.retainLocked(key, data)
}

func (scope *statementSyntaxScope) retainLocked(key statementSyntaxKey, data *statementSyntaxData) *statementSyntaxData {
	if key.entry != StatementEntry && key.entry != fragmentEntry || data.nodes[data.root].rule != CypherParserRULE_script {
		return data
	}
	if scope.original == "" {
		scope.original = key.source
	}
	if key.source != scope.original && key.source != scope.canonical {
		return data
	}
	// Concurrent cache misses cannot replace this execution's chosen product
	// or add another tree after process-cache eviction.
	if prior := scope.products[statementSyntaxKey{source: key.source, entry: StatementEntry, cypher25: key.cypher25}]; prior != nil {
		return prior
	}
	if scope.products == nil {
		scope.products = make(map[statementSyntaxKey]*statementSyntaxData)
		scope.windows = make(map[syntaxInspectionKey]syntaxInspection)
		scope.calls = make(map[syntaxInspectionKey]syntaxInspection)
		scope.indexed = make(map[*statementSyntaxData]bool)
	}
	scope.products[key] = data
	if data.nodes[data.root].rule == CypherParserRULE_script && (key.entry == StatementEntry || key.entry == fragmentEntry) {
		scope.products[statementSyntaxKey{key.source, StatementEntry, key.cypher25}] = data
		scope.products[statementSyntaxKey{key.source, fragmentEntry, key.cypher25}] = data
	}
	if scope.indexed[data] {
		return data
	}
	scope.indexed[data] = true
	for id, node := range data.nodes {
		if node.rule < 0 || node.start < 0 || node.end > len(data.source) || node.end <= node.start {
			continue
		}
		view := syntaxInspection{data: data, node: id, start: node.start, end: node.end}
		scope.put(scope.windows, syntaxInspectionKey{data.source[node.start:node.end], key.cypher25, fragmentEntry}, view)
		entry, exact := inspectionEntry(node.rule)
		if exact {
			scope.put(scope.windows, syntaxInspectionKey{data.source[node.start:node.end], key.cypher25, entry}, view)
		}
		if node.rule != CypherParserRULE_callSubquery && node.rule != CypherParserRULE_queryCallSt && node.rule != CypherParserRULE_standaloneCall {
			continue
		}
		// These are known complete query suffixes, produced by their actual
		// enclosing grammar nodes, rather than arbitrary matching substrings.
		for parent := id; parent >= 0; parent = data.nodes[parent].parent {
			container := data.nodes[parent]
			switch container.rule {
			case CypherParserRULE_callSubquery:
				if parent != id {
					continue
				}
			case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery, CypherParserRULE_singleQuery, CypherParserRULE_subqueryBody, CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall:
			default:
				continue
			}
			end := container.end
			if parent == data.root {
				end = len(data.source)
			}
			if end < node.end || end > len(data.source) {
				continue
			}
			text := strings.TrimSpace(data.source[node.start:end])
			call := view
			call.end = node.start + len(text)
			scope.put(scope.calls, syntaxInspectionKey{text, key.cypher25, fragmentEntry}, call)
		}
	}
	return data
}

func inspectionEntry(rule int) (SyntaxEntry, bool) {
	switch rule {
	case CypherParserRULE_callSubquery:
		return CallEntry, true
	case CypherParserRULE_subqueryBody:
		return ClauseEntry, true
	case CypherParserRULE_expression:
		return ExpressionEntry, true
	case CypherParserRULE_pattern:
		return PatternEntry, true
	}
	return 0, false
}

func (scope *statementSyntaxScope) put(index map[syntaxInspectionKey]syntaxInspection, key syntaxInspectionKey, view syntaxInspection) {
	if prior, found := index[key]; found {
		// A separately parsed adapter product cannot replace the original
		// statement's source association or make its unique window ambiguous.
		// Repeated occurrences within that original product still decline.
		if prior.data != view.data {
			return
		}
		// Several grammar containers can describe the same extent. Preserve
		// its outermost node; distinct occurrences must not alias by value.
		if prior.start != view.start || prior.end != view.end {
			prior.ambiguous = true
			index[key] = prior
		}
		return
	}
	index[key] = view
}

func (view syntaxInspection) rebase(source string) *statementSyntaxData {
	data := *view.data
	data.source, data.offset, data.root = source, view.start, view.node
	data.bytes = nil
	data.firstToken = 0
	for data.firstToken < len(data.tokens) && data.tokens[data.firstToken].start < view.start {
		data.firstToken++
	}
	data.afterToken = data.firstToken
	for data.afterToken < len(data.tokens) && data.tokens[data.afterToken].end <= view.end {
		data.afterToken++
	}
	return &data
}

// InspectSyntax is the compatibility inspection entry. Execution readers
// consume grammar-owned windows; a genuinely standalone source uses the same
// parser owner. Inspection is not statement admission or scope binding.
func InspectSyntax(ctx context.Context, source string, cypher25 bool) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25)
		scope.Lock()
		view := scope.windows[syntaxInspectionKey{source, cypher25, fragmentEntry}]
		scope.Unlock()
		if view.data != nil && !view.ambiguous {
			return StatementSyntax{view.rebase(source)}, nil
		}
	}
	return ParseSyntax(ctx, source, fragmentEntry, cypher25)
}

// QuerySyntax carries an already admitted query window into scope compilation.
// Node identities remain those of the statement that supplied its execution
// clauses. A standalone request still uses the full statement admission entry.
func QuerySyntax(ctx context.Context, source string, cypher25 bool) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25)
		scope.Lock()
		view := scope.windows[syntaxInspectionKey{source, cypher25, fragmentEntry}]
		scope.Unlock()
		if view.data != nil && !view.ambiguous {
			switch view.data.nodes[view.node].rule {
			case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery,
				CypherParserRULE_subqueryBody, CypherParserRULE_singleQuery,
				CypherParserRULE_singlePartQ, CypherParserRULE_multiPartQ,
				CypherParserRULE_readingStatement, CypherParserRULE_updatingStatement,
				CypherParserRULE_matchSt, CypherParserRULE_unwindSt, CypherParserRULE_callSubquery,
				CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall,
				CypherParserRULE_withSt, CypherParserRULE_returnSt, CypherParserRULE_foreachSt:
				return StatementSyntax{view.rebase(source)}, nil
			}
		}
	}
	return ParseSyntax(ctx, source, StatementEntry, cypher25)
}

// LeadingCallSyntax selects only a source-leading CALL from a retained known
// query window. The returned node's source includes its suffix, so DescribeCall
// keeps the original continuation while reading the existing CALL subtree.
func LeadingCallSyntax(ctx context.Context, source string, cypher25 bool) (SyntaxNode, error) {
	if err := ctx.Err(); err != nil {
		return SyntaxNode{}, err
	}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25)
		scope.Lock()
		view := scope.calls[syntaxInspectionKey{source, cypher25, fragmentEntry}]
		scope.Unlock()
		if view.data != nil && !view.ambiguous {
			return StatementSyntax{view.rebase(source)}.Root(), nil
		}
	}
	syntax, err := InspectSyntax(ctx, source, cypher25)
	if !syntax.Valid() {
		return SyntaxNode{}, err
	}
	call, _ := syntax.LeadingCall()
	return call, err
}
