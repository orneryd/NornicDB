// Author: R17 implementation lane, 1 November 2026.
package antlr

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"sync"
)

type syntaxOwnerKey struct{}

// syntaxExecution retains one grammar acquisition for each distinct program.
// Its lock serializes acquisition, including failed parses and derived windows.
type syntaxExecution struct {
	sync.Mutex
	programs map[string]*syntaxProgram
}
type syntaxProgram struct {
	binding  *syntaxOwner
	syntax   StatementSyntax
	failure  error
	acquired bool
}
type syntaxOwner struct {
	execution *syntaxExecution
	program   *syntaxProgram
	sync.Mutex
	source      string
	prepared    *cyphertext.Source
	inputSource *cyphertext.Source
	syntax      StatementSyntax
	failure     error
	acquired    bool
	views       map[statementSyntaxKey]StatementSyntax
	presented   *cyphertext.Source
	projection  StatementSyntax
}

// WithNewSyntaxExecution starts the registry for one public statement execution.
func WithNewSyntaxExecution(ctx context.Context, source string) context.Context {
	ctx = context.WithValue(ctx, syntaxOwnerKey{}, (*syntaxOwner)(nil))
	return WithSyntaxOwner(ctx, source)
}

// WithSyntaxOwner declares a program while sharing the execution's registry.
// Byte-identical declarations select the same retained acquisition.
func WithSyntaxOwner(ctx context.Context, source string) context.Context {
	prepared, ok := cyphertext.RetainedSource(ctx, source, AcceptedSource)
	if !ok {
		prepared = cyphertext.NewSource(ctx, source, AcceptedSource)
	}
	return withSyntaxProgram(ctx, prepared)
}

func withSyntaxProgram(ctx context.Context, source *cyphertext.Source) context.Context {
	previous, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	execution := &syntaxExecution{programs: make(map[string]*syntaxProgram)}
	if previous != nil {
		execution = previous.execution
	}
	execution.Lock()
	program := execution.programs[source.Text]
	if program == nil {
		program = &syntaxProgram{}
		execution.programs[source.Text] = program
	}
	binding := &syntaxOwner{execution: execution, program: program, source: source.Text, prepared: source}
	if program.binding == nil {
		program.binding = binding
	}
	execution.Unlock()
	return context.WithValue(ctx, syntaxOwnerKey{}, binding)
}

// WithSyntaxProgram selects the distinct text produced by frontend lowering.
// Ordinary consumer windows retain their existing owner instead.
func WithSyntaxProgram(ctx context.Context, source *cyphertext.Source) context.Context {
	if !HasSyntaxOwner(ctx) || source == nil {
		return ctx
	}
	owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	if owner.source == source.Text {
		return ctx
	}
	return withSyntaxProgram(ctx, source)
}

// declaredProgram selects an explicitly prepared program at a legacy text-only
// boundary. It cannot manufacture a new ingress on an ordinary adapter miss.
func (owner *syntaxOwner) declaredProgram(text string) *syntaxOwner {
	if text == owner.source {
		return owner
	}
	owner.execution.Lock()
	defer owner.execution.Unlock()
	if program := owner.execution.programs[text]; program != nil {
		return program.binding
	}
	return nil
}

// HasSyntaxOwner distinguishes an admitted execution from a standalone adapter.
func HasSyntaxOwner(ctx context.Context) bool {
	owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	return owner != nil
}

// EnsureSyntaxOwner preserves an internal continuation's owner. It is not a
// fallback: only a path with no declared statement acquires its own ingress.
func EnsureSyntaxOwner(ctx context.Context, source string) context.Context {
	if owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner); owner != nil {
		return ctx
	}
	return WithSyntaxOwner(ctx, source)
}

// acquire serializes registry access so concurrent consumers cannot parse the
// same text twice. Complete windows of an acquired program reuse its arena.
func (owner *syntaxOwner) acquire(ctx context.Context, cypher25 bool) {
	execution := owner.execution
	execution.Lock()
	defer execution.Unlock()
	program := owner.program
	if !program.acquired {
		for _, retained := range execution.programs {
			if !retained.acquired || retained.failure != nil || !retained.syntax.Valid() {
				continue
			}
			if derived, ok := retained.syntax.compatibilityText(owner.prepared.Text, StatementEntry); ok {
				program.syntax = derived
				break
			}
		}
		if !program.syntax.Valid() {
			program.syntax, program.failure = parseSyntaxStandalone(ctx, owner.prepared.Text, StatementEntry, cypher25, AcceptedSource)
		}
		program.acquired = true
	}
	owner.syntax, owner.failure, owner.acquired = program.syntax, program.failure, true
	owner.inputSource = owner.prepared
	if owner.syntax.Valid() {
		data := owner.syntax.data
		bridge := cyphertext.NewSource(ctx, data.view.Original, AcceptedSource).Window(data.viewStart, data.viewEnd)
		if inverse, _, ok := bridge.ViewFrom(data.view.Original); ok {
			if frame, ok := data.view.Reframe(inverse); ok {
				// Execution facts use this program's accepted coordinates. Mapping
				// accepted -> original -> accepted loses inserted lowering tokens.
				local := *data
				local.view = frame
				local.viewStart, local.viewEnd = 0, len(owner.prepared.Text)
				owner.syntax = StatementSyntax{&local}
				if original, ok := frame.Reframe(owner.prepared); ok {
					owner.inputSource = original
				}
			}
		}
	}
}

func (owner *syntaxOwner) read(ctx context.Context, text string, entry SyntaxEntry, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	owner.Lock()
	defer owner.Unlock()
	if !owner.acquired {
		owner.acquire(ctx, cypher25)
	}
	if !owner.syntax.Valid() {
		return owner.syntax, owner.failure
	}
	key := statementSyntaxKey{source: text, entry: entry, cypher25: cypher25, stage: stage}
	if view, ok := owner.views[key]; ok {
		view.recordCredentials(ctx)
		return view, nil
	}
	syntax := owner.syntax
	if source := cyphertext.CurrentSource(ctx); source != nil {
		if source != owner.presented {
			owner.presented = source
			owner.projection = StatementSyntax{}
			if projected, ok := owner.presentTo(syntax, source); ok {
				if window, ok := projected.CompatibilityView(0, len(source.Text), FragmentEntry); ok {
					owner.projection = window
				}
			}
		}
		if owner.projection.Valid() {
			syntax = owner.projection
		}
		if stage == RawSource && text == owner.inputSource.Original {
			data := *owner.syntax.data
			data.view = owner.inputSource
			data.viewStart = 0
			data.viewEnd = len(owner.inputSource.Original)
			data.projected = false
			syntax = StatementSyntax{&data}
		}
	}
	remember := func(view StatementSyntax) StatementSyntax {
		if owner.views == nil {
			owner.views = make(map[statementSyntaxKey]StatementSyntax)
		}
		owner.views[key] = view
		view.recordCredentials(ctx)
		return view
	}
	if text == syntax.Source() && (entry == FragmentEntry || entry == StatementEntry && syntax.Root().Rule() == CypherParserRULE_script) {
		if owner.failure != nil {
			syntax.recordCredentials(ctx)
			if local := syntax.compatibilityError(); local != nil {
				return syntax, local
			}
			return syntax, owner.failure
		}
		return remember(syntax), nil
	}
	if derived, ok := syntax.compatibilityText(text, entry); ok {
		if err := derived.compatibilityError(); err != nil {
			return derived, err
		}
		return remember(derived), nil
	}
	if owner.inputSource != nil && owner.inputSource.Text == owner.syntax.data.source {
		data := *owner.syntax.data
		data.view = owner.inputSource
		data.viewStart = 0
		data.viewEnd = len(owner.inputSource.Original)
		data.projected = true
		ingress := StatementSyntax{&data}
		if text == ingress.Source() && (entry == StatementEntry || entry == FragmentEntry) {
			if owner.failure != nil {
				return ingress, ingress.compatibilityError()
			}
			return remember(ingress), nil
		}
		if derived, ok := ingress.compatibilityText(text, entry); ok {
			if err := derived.compatibilityError(); err != nil {
				return derived, err
			}
			return remember(derived), nil
		}
	}
	// Earlier source stages remain valid views of the same parser arena.
	if derived, ok := owner.syntax.compatibilityText(text, entry); ok {
		if err := derived.compatibilityError(); err != nil {
			return derived, err
		}
		return remember(derived), nil
	}
	if derived, ok := syntax.compatibilityCode(text, entry); ok {
		if err := derived.compatibilityError(); err != nil {
			return derived, err
		}
		return remember(derived), nil
	}
	if owner.failure != nil {
		return owner.syntax, owner.failure
	}
	return StatementSyntax{}, errors.New("syntax compatibility adapter requested a non-derived statement window")
}

// CallScopeSyntax keeps the legacy list adapter's contract while reading a
// real CALL's retained scope. A standalone list has a distinct grammar ingress;
// an owned list never manufactures a second CALL statement for parsing.
func CallScopeSyntax(ctx context.Context, list string, cypher25 bool) (SyntaxNode, error) {
	owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	if owner == nil {
		syntax, err := parseSyntaxStandalone(ctx, "CALL ("+list+") { RETURN 0 }", CallEntry, cypher25, AcceptedSource)
		if err != nil {
			return SyntaxNode{}, err
		}
		return syntax.Root(), nil
	}
	text := owner.source
	if source := cyphertext.CurrentSource(ctx); source != nil {
		if _, _, ok := source.ViewFrom(owner.source); ok {
			text = source.Text
		}
	}
	syntax, err := owner.read(ctx, text, StatementEntry, cypher25, AcceptedSource)
	if !syntax.Valid() {
		return SyntaxNode{}, err
	}
	var found SyntaxNode
	syntax.Walk(func(node SyntaxNode) func() {
		if found.Valid() || node.Rule() != CypherParserRULE_callSubquery {
			return nil
		}
		open, a := node.DirectToken(CypherLexerLPAREN)
		close, b := node.DirectToken(CypherLexerRPAREN)
		if !a || !b {
			return nil
		}
		_, start := open.Span()
		end, _ := close.Span()
		if start <= end && node.Source()[start:end] == list {
			found = node
		}
		return nil
	})
	if found.Valid() {
		return found, nil
	}
	if err != nil {
		return SyntaxNode{}, err
	}
	return SyntaxNode{}, errors.New("CALL scope list is not a derived view of its statement")
}

// ProjectionClauseSyntax serves the legacy RETURN-item adapter without parsing
// its manufactured RETURN prefix. The source projection supplies that prefix's
// local coordinates; all expression and alias nodes remain the real nodes.
func ProjectionClauseSyntax(ctx context.Context, item string, cypher25 bool) (SyntaxNode, error) {
	owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	if owner == nil {
		syntax, err := parseSyntaxStandalone(ctx, "RETURN "+item, FragmentEntry, cypher25, AcceptedSource)
		if err != nil {
			return SyntaxNode{}, err
		}
		var found SyntaxNode
		syntax.Walk(func(node SyntaxNode) func() {
			if !found.Valid() && node.Rule() == CypherParserRULE_returnSt {
				found = node
			}
			return nil
		})
		return found, nil
	}
	wholeCtx := cyphertext.WithSourceView(ctx, owner.prepared)
	syntax, err := owner.read(wholeCtx, owner.prepared.Text, StatementEntry, cypher25, AcceptedSource)
	if !syntax.Valid() {
		return SyntaxNode{}, err
	}
	if current := cyphertext.CurrentSource(ctx); current != nil {
		if projected, ok := owner.presentTo(syntax, current); ok {
			syntax = projected
		}
	}
	var found SyntaxNode
	for _, rule := range []int{CypherParserRULE_projectionBody, CypherParserRULE_projectionItems, CypherParserRULE_projectionItem, CypherParserRULE_expression} {
		for id, itemData := range syntax.data.nodes {
			if itemData.rule != rule {
				continue
			}
			node := SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}
			a, b := node.Span()
			if a < 0 || b < a || b > len(syntax.Source()) {
				continue
			}
			if node.Text() == item || node.Code() == item {
				found = node
				break
			}
		}
		if found.Valid() {
			break
		}
	}
	if !found.Valid() && item == "*" {
		// A leading star is a projectionItems terminal, not an expression or
		// projectionItem. Keep that original grammar role for RETURN *, ... .
		for id, data := range syntax.data.nodes {
			if data.rule != CypherParserRULE_projectionItems {
				continue
			}
			items := SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}
			for i := 0; i < items.ChildCount(); i++ {
				star := items.Child(i)
				if !star.IsToken(CypherLexerMULT) {
					continue
				}
				a, b := star.Span()
				if a >= 0 && b <= len(syntax.Source()) && star.Text() == item {
					found = star
					break
				}
			}
			if found.Valid() {
				break
			}
		}
	}
	if !found.Valid() {
		return SyntaxNode{}, errors.New("projection input is not a derived view of its statement")
	}
	var source *cyphertext.Source
	if found.Text() == item {
		start, end := found.Span()
		source = cyphertext.NewSource(ctx, syntax.data.view.Original, AcceptedSource).Window(syntax.data.viewStart+start, syntax.data.viewStart+end)
	} else {
		source = found.CodeSource()
	}
	source = source.Rewrite("RETURN "+item, []cyphertext.SourceEdit{{Original: cyphertext.Span{Start: 0, End: 0}, Accepted: cyphertext.Span{Start: 0, End: 7}}})
	presented, ok := syntax.present(source)
	if !ok {
		return SyntaxNode{}, errors.New("projection input has no retained source frame")
	}
	data := *presented.data
	data.containers = append([]syntaxNodeData(nil), data.containers...)
	data.parents = make(map[int]int)
	selected := found.id
	rule := found.Rule()
	if found.IsToken(CypherLexerMULT) {
		selected = data.container(CypherParserRULE_projectionItems, []int{selected})
		rule = CypherParserRULE_projectionItems
	}
	switch rule {
	case CypherParserRULE_expression:
		selected = data.container(CypherParserRULE_projectionItem, []int{selected})
		fallthrough
	case CypherParserRULE_projectionItem:
		selected = data.container(CypherParserRULE_projectionItems, []int{selected})
		fallthrough
	case CypherParserRULE_projectionItems:
		selected = data.container(CypherParserRULE_projectionBody, []int{selected})
	}
	clause := data.container(CypherParserRULE_returnSt, []int{selected})
	data.spans = map[int]cyphertext.Span{clause: {Start: 0, End: len(source.Text)}}
	data.root = clause
	itemData := data.node(found.id)
	data.firstToken = itemData.firstToken
	data.afterToken = itemData.afterToken
	if err := (StatementSyntax{&data}).compatibilityError(); err != nil {
		return SyntaxNode{}, err
	}
	return SyntaxNode{syntaxView[syntaxNode]{&data, clause}}, nil
}

func (owner *syntaxOwner) presentTo(syntax StatementSyntax, source *cyphertext.Source) (StatementSyntax, bool) {
	if source == nil || !syntax.Valid() {
		return StatementSyntax{}, false
	}
	// Derive directly from accepted program coordinates. The original-input
	// association is retained separately for restoration and credentials.
	bridge, _, ok := source.ViewFrom(owner.source)
	if !ok {
		return StatementSyntax{}, false
	}
	frame, ok := owner.syntax.data.view.Reframe(bridge)
	if !ok {
		return StatementSyntax{}, false
	}
	data := *syntax.data
	data.view = frame
	data.viewStart, data.viewEnd = 0, len(source.Text)
	data.projected = true
	return StatementSyntax{&data}, true
}

// RetainedSyntaxSource exposes the association of an already-derived adapter
// input. The inverse is built exclusively from the source owner's recorded
// edits/windows; rebinding can retain it without finding another occurrence.
func RetainedSyntaxSource(ctx context.Context, text string) (*cyphertext.Source, bool) {
	owner, _ := ctx.Value(syntaxOwnerKey{}).(*syntaxOwner)
	if owner == nil {
		return nil, false
	}
	owner.Lock()
	defer owner.Unlock()
	if owner.inputSource == nil {
		return nil, false
	}
	for _, syntax := range owner.views {
		if syntax.Source() != text {
			continue
		}
		inverse, _, ok := syntax.data.view.ViewFrom(syntax.data.view.Original)
		if !ok {
			continue
		}
		inverse = inverse.Window(syntax.data.viewStart, syntax.data.viewEnd)
		if source, ok := inverse.Reframe(owner.inputSource); ok {
			return source, true
		}
	}
	return nil, false
}
