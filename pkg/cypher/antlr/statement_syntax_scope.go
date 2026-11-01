// Author: Codex R17 implementation lane, 1 November 2026.
package antlr

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sort"
	"sync"
)

// WithStatementSyntax declares one execution's source. Internal source windows
// keep this declaration; only a separate ingress creates a separate owner.
// WithNewStatementSyntax is the explicit client/program ingress. Internal
// windows use WithStatementSyntax and cannot create another parsing owner.
func WithNewStatementSyntax(ctx context.Context, sources ...string) context.Context {
	return newStatementSyntaxScope(ctx, true, sources...)
}

// StatementSyntaxIngress marks a new client or generated participant input.
// Frontend adapters establish its accepted source before execution retains a
// syntax product; an enclosing execution's tree cannot admit that new input.
func StatementSyntaxIngress(ctx context.Context) context.Context {
	return context.WithValue(ctx, statementSyntaxScopeKey{}, (*statementSyntaxScope)(nil))
}

// WithExecutionStatementSyntax retains the accepted execution input. Routed
// windows keep their existing owner; a new ingress gets one full-statement
// owner, rather than the standalone fragment adapter's ingress.
func WithExecutionStatementSyntax(ctx context.Context, sources ...string) context.Context {
	if statementSyntaxScopeFrom(ctx) == nil {
		return newStatementSyntaxScope(ctx, true, sources...)
	}
	return WithStatementSyntax(ctx, sources...)
}
func newStatementSyntaxScope(ctx context.Context, full bool, sources ...string) context.Context {
	scope := &statementSyntaxScope{fullStatement: full}
	if len(sources) > 0 {
		scope.original = sources[0]
	}
	if len(sources) > 1 {
		scope.canonical = sources[1]
	}
	if len(sources) > 0 {
		text, stage := sources[0], RawSource
		if len(sources) > 1 {
			text, stage = sources[1], AcceptedSource
		}
		if view, ok := cyphertext.RetainedSource(ctx, text, stage); ok {
			scope.source = view
		}
	}
	return context.WithValue(ctx, statementSyntaxScopeKey{}, scope)
}
func WithStatementSyntax(ctx context.Context, sources ...string) context.Context {
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.Lock()
		if len(sources) > 1 {
			scope.canonical = sources[1]
			if view, ok := cyphertext.RetainedSource(ctx, sources[1], AcceptedSource); ok {
				scope.source = view
			}
		}
		scope.Unlock()
		return ctx
	}
	return newStatementSyntaxScope(ctx, false, sources...)
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
	parse               sync.Mutex
	original, canonical string
	roots               []*retainedSyntaxRoot // zero or one; windows share its node/token arrays
	acquired            bool
	source              *cyphertext.Source
	fullStatement       bool
	failure             error
	presentedSource     *cyphertext.Source
	presentation        *retainedSyntaxRoot
}

func statementSyntaxScopeFrom(ctx context.Context) *statementSyntaxScope {
	scope, _ := ctx.Value(statementSyntaxScopeKey{}).(*statementSyntaxScope)
	return scope
}

// acquire serializes acquisition for the execution. Both successful and failed
// admission products belong to this one owner; a consumer never retries a parse.
func (scope *statementSyntaxScope) acquire(ctx context.Context, key statementSyntaxKey) error {
	scope.parse.Lock()
	defer scope.parse.Unlock()
	scope.Lock()
	if scope.acquired {
		err := scope.failure
		scope.Unlock()
		return err
	}
	entry := fragmentEntry
	if scope.fullStatement {
		entry = StatementEntry
	}
	source := scope.canonical
	if source == "" {
		source = scope.original
	}
	if source == "" {
		source = key.source
		scope.original = source
		if !scope.fullStatement {
			entry = key.entry
		}
	}
	projection := scope.source
	if projection == nil || projection.Text != source {
		projection = cyphertext.NewSource(ctx, source, key.stage)
	}
	scope.Unlock()
	syntax, err := parseCachedSource(ctx, projection, entry, key.cypher25)
	scope.Lock()
	defer scope.Unlock()
	scope.acquired = true
	scope.failure = err
	if syntax.Valid() {
		scope.roots = []*retainedSyntaxRoot{{key: statementSyntaxKey{source: source, entry: entry, cypher25: key.cypher25, stage: key.stage}, data: syntax.data}}
	}
	return err
}
func stageRoot(root *retainedSyntaxRoot, stage SourceStage) *retainedSyntaxRoot {
	if stage == AcceptedSource && root.data.view.Original != root.data.source {
		data := syntaxSourceView(root.data, &cyphertext.Source{Original: root.data.source, Text: root.data.source})
		data.submitted = root.data.submitted
		if data.submitted == nil {
			data.submitted = root.data.view
		}
		return &retainedSyntaxRoot{key: root.key, data: data}
	}
	return root
}

// presentationLocked projects the one CST through the source owner's retained
// transformations. A window selects its original grammar occurrence; it never
// admits new syntax or inherits the enclosing statement's effects.
func (scope *statementSyntaxScope) presentationLocked(source *cyphertext.Source) *retainedSyntaxRoot {
	if len(scope.roots) == 0 || source == nil {
		return nil
	}
	if scope.presentedSource == source {
		return scope.presentation
	}
	scope.presentedSource, scope.presentation = source, nil
	root := stageRoot(scope.roots[0], AcceptedSource)
	bridge, extent, ok := source.ViewFrom(root.data.source)
	if !ok || bridge.Original == root.data.view.Original {
		return nil
	}
	start, end, ok := root.liveWindow(extent.Start, extent.End)
	if !ok {
		return nil
	}
	wholeStart, wholeEnd, _ := root.liveWindow(0, len(root.data.source))
	data := root.data
	if start != wholeStart || end != wholeEnd {
		view, found := root.inspectionAt(data.root, start, end, fragmentEntry, false)
		if !found {
			return nil
		}
		data = view.rebase()
	}
	projected := *data
	projected.view, projected.offset, projected.sourceEnd = bridge, 0, len(source.Text)
	projected.inputOffset = extent.Start
	scope.presentation = &retainedSyntaxRoot{key: root.key, data: &projected}
	return scope.presentation
}
func (scope *statementSyntaxScope) rootLocked(key statementSyntaxKey, source *cyphertext.Source) *retainedSyntaxRoot {
	if len(scope.roots) == 0 {
		return nil
	}
	if root := scope.presentationLocked(source); root != nil && key.source == root.data.view.Original {
		return root
	}
	root := stageRoot(scope.roots[0], key.stage)
	if key.source == root.data.view.Original {
		return root
	}
	return nil
}
func (scope *statementSyntaxScope) product(key statementSyntaxKey, source *cyphertext.Source) *statementSyntaxData {
	scope.Lock()
	defer scope.Unlock()
	if root := scope.rootLocked(key, source); root != nil {
		if key.entry == root.key.entry || key.entry == fragmentEntry || key.entry == StatementEntry && root.data.nodes[root.data.root].rule == CypherParserRULE_script {
			return root.data
		}
	}
	return scope.windowLocked(key, source, false)
}
func (scope *statementSyntaxScope) windowLocked(key statementSyntaxKey, source *cyphertext.Source, call bool) *statementSyntaxData {
	if len(scope.roots) == 0 {
		return nil
	}
	if root := scope.presentationLocked(source); root != nil {
		if view, found := root.inspection(syntaxInspectionKey{key.source, key.entry}, call); found {
			if view.ambiguous {
				return nil
			}
			return view.rebase()
		}
	}
	view, found := stageRoot(scope.roots[0], key.stage).inspection(syntaxInspectionKey{key.source, key.entry}, call)
	if !found || view.ambiguous {
		return nil
	}
	return view.rebase()
}
func (scope *statementSyntaxScope) read(ctx context.Context, key statementSyntaxKey, call bool) (StatementSyntax, error) {
	if err := syntaxStageError(ctx, key.source, key.stage); err != nil {
		return StatementSyntax{}, err
	}
	if err := scope.acquire(ctx, key); err != nil {
		scope.Lock()
		var data *statementSyntaxData
		if len(scope.roots) > 0 {
			data = scope.roots[0].data
		}
		scope.Unlock()
		return StatementSyntax{data}, err
	}
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	source, known := cyphertext.RetainedSource(ctx, key.source, key.stage)
	if !known {
		scope.Lock()
		source = scope.source
		scope.Unlock()
	}
	if !call {
		if data := scope.product(key, source); data != nil {
			return StatementSyntax{data}, nil
		}
	}
	if call {
		scope.Lock()
		root := scope.rootLocked(key, source)
		var data *statementSyntaxData
		if root != nil {
			data = root.data
		} else {
			data = scope.windowLocked(key, source, true)
		}
		scope.Unlock()
		if data != nil {
			return StatementSyntax{data}, nil
		}
	}
	return StatementSyntax{}, unretainedSyntaxError()
}
func unretainedSyntaxError() error {
	return errors.New("syntax consumer requested a window outside its admitted statement")
}
func inspectionEntry(rule int) (SyntaxEntry, bool) {
	switch rule {
	case CypherParserRULE_scopeList:
		return ScopeListEntry, true
	case CypherParserRULE_useClause:
		return UseEntry, true
	case CypherParserRULE_callSubquery:
		return CallEntry, true
	case CypherParserRULE_subqueryBody:
		return ClauseEntry, true
	case CypherParserRULE_expression:
		return ExpressionEntry, true
	case CypherParserRULE_pattern, CypherParserRULE_patternPart:
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

// InspectSyntax reads an admitted window, or admits a standalone adapter input.
func InspectSyntax(ctx context.Context, source Statement, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	return ParseSyntax(ctx, source, fragmentEntry, cypher25, stage)
}
func QuerySyntax(ctx context.Context, source Statement, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	// Standalone query admission has a distinct statement ingress. Inside an
	// execution this entry still reads its existing owner, without retrying
	// an overlapping fragment when the statement is rejected.
	if statementSyntaxScopeFrom(ctx) != nil {
		return InspectSyntax(ctx, source, cypher25, stage)
	}
	return ParseSyntax(ctx, source, StatementEntry, cypher25, stage)
}
func LeadingCallSyntax(ctx context.Context, statement Statement, cypher25 bool, stage SourceStage) (SyntaxNode, error) {
	if err := ctx.Err(); err != nil {
		return SyntaxNode{}, err
	}
	var syntax StatementSyntax
	var err error
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		syntax, err = scope.read(ctx, statementSyntaxKey{source: statement.text, entry: fragmentEntry, cypher25: cypher25, stage: stage}, true)
	} else {
		syntax, err = InspectSyntax(ctx, statement, cypher25, stage)
	}
	if err != nil {
		return SyntaxNode{}, err
	}
	syntax.recordCredentials(ctx)
	if node := syntax.Root(); node.Rule() == CypherParserRULE_callSubquery || node.Rule() == CypherParserRULE_queryCallSt || node.Rule() == CypherParserRULE_standaloneCall {
		return node, nil
	}
	call, _ := syntax.LeadingCall()
	return call, nil
}
