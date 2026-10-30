// Author: Codex R17 implementation agent, 27 October 2026.
package antlr

import (
	"context"
	"sort"
	"sync"
)

// WithStatementSyntax retains the existing grammar products for one execution.
// Original and current sources each retain at most one product per explicit
// stage and dialect (eight roots total). Adapter fragments add no roots. Views
// share those roots' storage. Compatibility fragment requests resolve only
// their own extents without materializing an index. No context, parameters
// or principal are retained by a grammar product.
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

// Keep the original root and its shared syntax storage intact. Only the
// replaced current root loses this scope's reference; published views remain
// immutable. Clear the removed slice tail so it cannot extend that lifetime.
func (scope *statementSyntaxScope) replaceCurrentLocked(source string) {
	kept := scope.roots[:0]
	for _, root := range scope.roots {
		if root.key.source == scope.original {
			kept = append(kept, root)
		}
	}
	clear(scope.roots[len(kept):])
	scope.roots = kept
	scope.canonical = source
}

type statementSyntaxScopeKey struct{}
type syntaxInspectionKey struct {
	source string
	entry  SyntaxEntry
}
type syntaxInspection struct {
	data                                 *statementSyntaxData
	node, start, end, codeStart, codeEnd int
	ambiguous                            bool
}
type retainedSyntaxRoot struct {
	key  statementSyntaxKey
	data *statementSyntaxData
}
type statementSyntaxScope struct {
	sync.Mutex
	original, canonical string
	prepared            [2][2]bool // explicit source stage, dialect
	roots               []*retainedSyntaxRoot
}

func statementSyntaxScopeFrom(ctx context.Context) *statementSyntaxScope {
	scope, _ := ctx.Value(statementSyntaxScopeKey{}).(*statementSyntaxScope)
	return scope
}

// A cache hit adopts a root without scanning its nodes or allocating indexes.
func (scope *statementSyntaxScope) prepare(cypher25 bool, stage SourceStage) {
	if stage != RawSource && stage != AcceptedSource {
		return
	}
	dialect := 0
	if cypher25 {
		dialect = 1
	}
	scope.Lock()
	if scope.prepared[stage][dialect] {
		scope.Unlock()
		return
	}
	scope.prepared[stage][dialect] = true
	original := scope.original
	scope.Unlock()
	key := statementSyntaxKey{source: original, entry: StatementEntry, cypher25: cypher25, stage: stage}
	statementSyntaxCache.RLock()
	data := statementSyntaxCache.entries[key]
	if data == nil {
		key.entry = fragmentEntry
		data = statementSyntaxCache.entries[key]
	}
	statementSyntaxCache.RUnlock()
	if data != nil {
		scope.retain(key, data)
	}
}

func (scope *statementSyntaxScope) rootLocked(key statementSyntaxKey) *retainedSyntaxRoot {
	for _, root := range scope.roots {
		if root.key.source == key.source && root.key.stage == key.stage && root.key.cypher25 == key.cypher25 {
			return root
		}
	}
	return nil
}

// Establish the declared current root before requesting an adapter window.
// A direct root request ends recursion and needs no fragment index.
func (scope *statementSyntaxScope) ensureRoot(ctx context.Context, source string, cypher25 bool, stage SourceStage) {
	scope.prepare(cypher25, stage)
	scope.Lock()
	root := scope.canonical
	if root == "" {
		root = scope.original
	}
	retained := scope.rootLocked(statementSyntaxKey{source: root, cypher25: cypher25, stage: stage}) != nil
	scope.Unlock()
	if root != "" && root != source && !retained {
		_, _ = ParseSyntax(ctx, root, fragmentEntry, cypher25, stage)
	}
}

func (scope *statementSyntaxScope) product(key statementSyntaxKey) *statementSyntaxData {
	scope.prepare(key.cypher25, key.stage)
	scope.Lock()
	defer scope.Unlock()
	if root := scope.rootLocked(key); root != nil {
		if key.entry == fragmentEntry || key.entry == StatementEntry && root.data.nodes[root.data.root].rule == CypherParserRULE_script {
			return root.data
		}
	}
	if key.entry == StatementEntry {
		return nil
	}
	// The declared whole source is parsed/adopted directly. Never build the
	// original's full-window index just to discover a rewritten whole root.
	if key.entry == fragmentEntry && (key.source == scope.original || key.source == scope.canonical) {
		return nil
	}
	return scope.windowLocked(key, false)
}

func (scope *statementSyntaxScope) windowLocked(key statementSyntaxKey, call bool) *statementSyntaxData {
	for _, root := range scope.roots {
		if root.key.stage != key.stage || root.key.cypher25 != key.cypher25 {
			continue
		}
		view, exists := root.inspection(syntaxInspectionKey{key.source, key.entry}, call)
		if !exists {
			continue
		}
		// A repeated original occurrence cannot be resolved by a later root.
		if view.ambiguous {
			return nil
		}
		return view.rebase()
	}
	return nil
}

func (scope *statementSyntaxScope) retain(key statementSyntaxKey, data *statementSyntaxData) *statementSyntaxData {
	scope.Lock()
	defer scope.Unlock()
	rule := data.nodes[data.root].rule
	if (key.entry != StatementEntry && key.entry != fragmentEntry) || (rule != CypherParserRULE_script && rule != CypherParserRULE_subqueryBody) {
		return data
	}
	if scope.original == "" {
		scope.original = key.source
	}
	if key.source != scope.original && key.source != scope.canonical {
		return data
	}
	if prior := scope.rootLocked(key); prior != nil {
		if prior.data.nodes[prior.data.root].rule == CypherParserRULE_script || rule != CypherParserRULE_script {
			return prior.data
		}
		// Admission promotes only this root. Every other root remains intact.
		prior.data = data
		return data
	}
	scope.roots = append(scope.roots, &retainedSyntaxRoot{key: key, data: data})
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

func (view syntaxInspection) rebase() *statementSyntaxData {
	data := *view.data
	data.offset, data.sourceEnd, data.root = view.start, view.end, view.node
	data.inputOffset = view.codeStart
	data.firstToken = sort.Search(len(data.tokens), func(i int) bool { return data.tokens[i].start >= view.codeStart })
	data.afterToken = data.firstToken + sort.Search(len(data.tokens)-data.firstToken, func(i int) bool { return data.tokens[data.firstToken+i].end > view.codeEnd })
	// Effects belong to the selected subtree, not its enclosing statement.
	data.effects = (StatementSyntax{&data}).collectEffects()
	return &data
}

// InspectSyntax consumes a retained grammar window, or the same parser owner
// for standalone input. Stage is explicit; inspection never admits a statement.
func InspectSyntax(ctx context.Context, source string, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	return ParseSyntax(ctx, source, fragmentEntry, cypher25, stage)
}

// QuerySyntax carries query node identities into bound compilation. Explicit
// statement validation retains its separate Script admission contract.
func QuerySyntax(ctx context.Context, source string, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	syntax, err := InspectSyntax(ctx, source, cypher25, stage)
	if err == nil {
		switch syntax.Root().Rule() {
		case CypherParserRULE_script, CypherParserRULE_query, CypherParserRULE_regularQuery,
			CypherParserRULE_subqueryBody, CypherParserRULE_singleQuery, CypherParserRULE_singlePartQ, CypherParserRULE_multiPartQ,
			CypherParserRULE_readingStatement, CypherParserRULE_updatingStatement, CypherParserRULE_matchSt,
			CypherParserRULE_unwindSt, CypherParserRULE_callSubquery, CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall,
			CypherParserRULE_withSt, CypherParserRULE_returnSt, CypherParserRULE_foreachSt:
			return syntax, nil
		}
	}
	return ParseSyntax(ctx, source, StatementEntry, cypher25, stage)
}

// LeadingCallSyntax selects a source-leading CALL with its known continuation.
// Whole roots already supply nodes; only a suffix request needs a reverse index.
func LeadingCallSyntax(ctx context.Context, source string, cypher25 bool, stage SourceStage) (SyntaxNode, error) {
	if err := ctx.Err(); err != nil {
		return SyntaxNode{}, err
	}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25, stage)
		key := statementSyntaxKey{source: source, entry: fragmentEntry, cypher25: cypher25, stage: stage}
		scope.Lock()
		root := scope.rootLocked(key)
		var data *statementSyntaxData
		if root != nil {
			data = root.data
		} else {
			data = scope.windowLocked(key, true)
		}
		scope.Unlock()
		if data != nil {
			syntax := StatementSyntax{data}
			syntax.recordCredentials(ctx)
			if root == nil {
				return syntax.Root(), nil
			}
			call, _ := syntax.LeadingCall()
			return call, nil
		}
	}
	syntax, err := InspectSyntax(ctx, source, cypher25, stage)
	if !syntax.Valid() {
		return SyntaxNode{}, err
	}
	call, _ := syntax.LeadingCall()
	return call, err
}
