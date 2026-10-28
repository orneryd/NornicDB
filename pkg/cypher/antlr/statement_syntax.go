package antlr

import (
	"context"
	"github.com/orneryd/nornicdb/internal/contextslot"
	"strings"
	"sync"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// SyntaxEntry selects an entry in the same grammar. Fragments use their own
// entry only when no enclosing statement product is available.
type SyntaxEntry uint8

const (
	StatementEntry SyntaxEntry = iota
	CallEntry
	ClauseEntry
	ExpressionEntry
	// FragmentEntry is the scope-independent inspection view used when a
	// compatibility caller supplies a statement, body or expression alone.
	FragmentEntry
	PatternEntry
	// UseEntry reads only the leading reference; semantic routing validates its follower.
	UseEntry
)

// StatementSyntax retains one source's complete concrete syntax and all lexer
// channels. Its views cannot mutate the cached tree, tokens or source map.
type StatementSyntax struct{ data *statementSyntaxData }

// Valid reports whether the reader retained a syntax product. An error may
// accompany a partial product for diagnostic framing; it is never cached or
// accepted by statement compilation.
func (syntax StatementSyntax) Valid() bool { return syntax.data != nil }

// LeadingCall returns the CALL clause that starts this source window. A later
// CALL, or one inside a procedure argument, can never supply the frame.
func (syntax StatementSyntax) LeadingCall() (SyntaxNode, bool) {
	if !syntax.Valid() {
		return SyntaxNode{}, false
	}
	first := -1
	for _, token := range syntax.data.tokens {
		if token.channel == 0 && token.kind != -1 {
			first = token.start
			break
		}
	}
	var found SyntaxNode
	syntax.Walk(func(node SyntaxNode) func() {
		if found.Valid() {
			return nil
		}
		switch node.Rule() {
		case CypherParserRULE_callSubquery, CypherParserRULE_queryCallSt, CypherParserRULE_standaloneCall:
			start, _ := node.Span()
			if start == first {
				found = node
			}
		}
		return nil
	})
	return found, found.Valid()
}

// syntaxNode and syntaxToken close the node/token kind sets of syntaxView.
// They are unexported, so only a closed type switch can choose the arena that
// owns a view's id.
type syntaxNode struct{}
type syntaxToken struct{}

// syntaxView is the single two-word identity shared by nodes and tokens: the
// retained statement product and a stable index into its node or token arena.
type syntaxView[nodeKind syntaxNode | syntaxToken] struct {
	data *statementSyntaxData
	id   int
}

// SyntaxNode is an immutable node identity within a StatementSyntax.
type SyntaxNode struct{ syntaxView[syntaxNode] }

// SyntaxToken is an immutable lexer-token identity within a StatementSyntax.
type SyntaxToken struct{ syntaxView[syntaxToken] }

// Span selects the owned node/token arena and maps its half-open interval
// through the retained source-coordinate owner once.
func (view syntaxView[nodeKind]) Span() (int, int) {
	var member nodeKind
	var accepted cyphertext.Span
	switch any(member).(type) {
	case syntaxNode:
		item := view.data.nodes[view.id]
		accepted = cyphertext.Span{Start: item.start, End: item.end}
	case syntaxToken:
		item := view.data.tokens[view.id]
		accepted = cyphertext.Span{Start: item.start, End: item.end}
	}
	span, _ := view.data.view.OriginalSpan(accepted, cyphertext.SourceExtent)
	return span.Start, span.End
}

// Text returns the selected node or token's exact original source bytes.
func (view syntaxView[nodeKind]) Text() string {
	start, end := view.Span()
	return view.data.view.Original[start:end]
}

// SyntaxVisitor enters a node and optionally restores its state on exit.
// Every visitor sees every child. A visitor cannot prune traversal.
type SyntaxVisitor func(SyntaxNode) func()

type statementSyntaxData struct {
	source      string
	view        *cyphertext.Source
	effects     statementEffectsData
	bytes       []int // rune boundary to original UTF-8 byte boundary
	nodes       []syntaxNodeData
	tokens      []syntaxTokenData
	credentials []cyphertext.CredentialSpan
	useError    *UseReferenceError
	diagnostics []localization.Message // Structural grammar diagnostics; failed products are never cached.
}

type syntaxNodeData struct {
	rule, parent           int
	start, end             int
	firstToken, afterToken int
	children               []int
	name                   string
}

type syntaxTokenData struct {
	kind, channel int
	start, end    int
}

type statementSyntaxKey struct {
	source   string
	entry    SyntaxEntry
	cypher25 bool
	stage    cyphertext.SourceStage
}

var statementSyntaxCache = struct {
	sync.RWMutex
	entries map[statementSyntaxKey]*statementSyntaxData
}{entries: make(map[statementSyntaxKey]*statementSyntaxData)}

const statementSyntaxCacheLimit = 4096

// ParseSyntax uses the existing grammar lexer/parser owner once and retains its
// complete product. Source stage, dialect and entry are immutable cache identity;
// incoming scopes, procedure signatures and values belong to bound compilation.
func ParseSyntax(ctx context.Context, source string, entry SyntaxEntry, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	key := statementSyntaxKey{source: source, entry: entry, cypher25: cypher25, stage: stage}
	statementSyntaxCache.RLock()
	data := statementSyntaxCache.entries[key]
	if data == nil && entry == FragmentEntry {
		for _, complete := range []SyntaxEntry{StatementEntry} {
			if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: complete, cypher25: cypher25, stage: stage}]; prior != nil {
				data = prior
				break
			}
		}
	}
	if data == nil && entry == StatementEntry {
		if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: FragmentEntry, cypher25: cypher25, stage: stage}]; prior != nil && prior.nodes[0].rule == CypherParserRULE_script {
			data = prior
		}
	}
	statementSyntaxCache.RUnlock()
	if data != nil {
		syntax := StatementSyntax{data}
		syntax.recordCredentials(ctx)
		return syntax, nil
	}
	projection := cyphertext.NewSource(ctx, source, stage)
	if projection.Err != nil {
		return StatementSyntax{}, projection.Err
	}
	data, err := parseStatementSyntax(ctx, projection, entry, nil)
	if err != nil {
		syntax := StatementSyntax{data}
		syntax.recordCredentials(ctx)
		return syntax, err
	}
	statementSyntaxCache.Lock()
	if prior := statementSyntaxCache.entries[key]; prior != nil {
		data = prior
	} else {
		if len(statementSyntaxCache.entries) >= statementSyntaxCacheLimit {
			clear(statementSyntaxCache.entries)
		}
		statementSyntaxCache.entries[key] = data
		if data.nodes[0].rule == CypherParserRULE_script && (entry == StatementEntry || entry == FragmentEntry) {
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: StatementEntry, cypher25: cypher25, stage: stage}] = data
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: FragmentEntry, cypher25: cypher25, stage: stage}] = data
		}
	}
	statementSyntaxCache.Unlock()
	syntax := StatementSyntax{data}
	syntax.recordCredentials(ctx)
	return syntax, nil
}

// parseStatementSyntax relocates readBatchTree's lexer/parser entry owner.
// Token filling uses that same lexer; retaining trivia does not lex again.
func parseStatementSyntax(ctx context.Context, projection *cyphertext.Source, entry SyntaxEntry, observe func(antlr4.Tree)) (data *statementSyntaxData, err error) {
	defer recoverPredictionCancellation(&err)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if projection.Err != nil {
		return nil, projection.Err
	}
	source := projection.Text
	if cyphertext.HasAdjacentStringLiterals(source) {
		message := localization.CypherANTLRParseSyntaxError("adjacent string literals")
		return nil, localization.NewLocalizedError(string(message.ID), message, nil)
	}
	inputView, input := sourceInputStream(ctx, projection)
	lexer := NewCypherLexer(input)
	listener := &parseErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)
	tokens := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	var stream antlr4.TokenStream = tokens
	if ctx.Done() != nil {
		stream = &contextTokenStream{TokenStream: tokens, ctx: ctx}
	}
	parser := NewCypherParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)
	prediction := acquirePrediction()
	usePrediction(parser, prediction)
	defer func() { parser.Interpreter = nil; prediction.release() }()
	var tree antlr4.Tree
	switch entry {
	case UseEntry:
		prefix := parser.UseReferencePrefix()
		tree = prefix.UseClause()
	case CallEntry:
		tree = parser.CallSubquery()
	case ClauseEntry:
		tree = parser.SubqueryBody()
	case ExpressionEntry:
		tree = parser.Expression()
	case FragmentEntry:
		// The lexer identifies statement entries before prediction. Choosing
		// Script directly avoids the overlapping script/body/expression
		// alternatives for a complete query, while retaining the same grammar.
		switch stream.LA(1) {
		case CypherLexerMATCH, CypherLexerOPTIONAL, CypherLexerRETURN, CypherLexerWITH,
			CypherLexerUNWIND, CypherLexerCALL, CypherLexerCREATE, CypherLexerMERGE,
			CypherLexerDELETE, CypherLexerDETACH, CypherLexerSET, CypherLexerREMOVE,
			CypherLexerFOREACH, CypherLexerLET, CypherLexerFILTER, CypherLexerFOR,
			CypherLexerUSE, CypherLexerSHOW, CypherLexerEXPLAIN, CypherLexerPROFILE,
			CypherLexerCYPHER, CypherLexerFINISH:
			tree = parser.Script()
			break
		}
		if tree != nil {
			break
		}
		fragment := parser.SyntaxFragment()
		tree = fragment
		switch {
		case fragment.Script() != nil:
			tree = fragment.Script()
		case fragment.SubqueryBody() != nil:
			tree = fragment.SubqueryBody()
		case fragment.Expression() != nil:
			tree = fragment.Expression()
		}
	case PatternEntry:
		tree = parser.Pattern()
	default:
		tree = parser.Script()
	}
	if (entry == ClauseEntry || entry == ExpressionEntry || entry == PatternEntry) && stream.LA(1) != antlr4.TokenEOF {
		trailing := stream.LT(1)
		listener.errors = append(listener.errors, localization.CypherANTLRUnexpectedInput(trailing.GetLine(), trailing.GetColumn()))
	}
	tokens.Fill()
	for i, message := range listener.errors {
		line, lineOK := message.Data["Line"].(int)
		column, columnOK := message.Data["Column"].(int)
		if lineOK && columnOK {
			if originalLine, originalColumn, ok := projection.OriginalPosition(line, column); ok {
				listener.errors[i] = localization.CypherANTLRUnexpectedInput(originalLine, originalColumn)
			}
		}
	}
	var syntaxErr error
	if len(listener.errors) != 0 {
		message := localization.CypherANTLRParseSyntaxError(strings.Join(listener.fallbacks(), "; "), listener.errors...)
		syntaxErr = localization.NewLocalizedError(string(message.ID), message, nil)
	}
	if observe != nil {
		observe(tree)
	}
	data = &statementSyntaxData{source: source, view: projection, diagnostics: append([]localization.Message(nil), listener.errors...)}
	data.bytes = inputView.boundaries
	if listener.useError != nil {
		copy := *listener.useError
		copy.Offset, _ = data.byteSpan(copy.Offset, copy.Offset)
		copy.Offset, _ = projection.OriginalOffset(copy.Offset, true, cyphertext.SourceExtent)
		data.useError = &copy
	}

	for _, token := range tokens.GetAllTokens() {
		start, end := data.byteSpan(token.GetStart(), token.GetStop()+1)
		data.tokens = append(data.tokens, syntaxTokenData{kind: token.GetTokenType(), channel: token.GetChannel(), start: start, end: end})
	}
	parent := -1
	walkSyntax(tree, func(node antlr4.Tree) []antlr4.Tree { return node.GetChildren() }, func(node antlr4.Tree) func() {
		id := len(data.nodes)
		item := syntaxNodeData{rule: -1, parent: parent, firstToken: -1, afterToken: -1}
		switch parsed := node.(type) {
		case antlr4.ParserRuleContext:
			item.rule = parsed.GetRuleIndex()
			if first, last := parsed.GetStart(), parsed.GetStop(); first != nil && last != nil {
				item.start, item.end = data.byteSpan(first.GetStart(), last.GetStop()+1)
				item.firstToken, item.afterToken = first.GetTokenIndex(), last.GetTokenIndex()+1
			}
			if item.rule == CypherParserRULE_symbol {
				item.name = cyphertext.SymbolicNameValue(source[item.start:item.end])
			}
		case antlr4.TerminalNode:
			token := parsed.GetSymbol()
			item.start, item.end = data.byteSpan(token.GetStart(), token.GetStop()+1)
			item.firstToken, item.afterToken = token.GetTokenIndex(), token.GetTokenIndex()+1
		}
		if item.rule == CypherParserRULE_credentialValue || item.rule == CypherParserRULE_credentialURL {
			raw := source[item.start:item.end]
			value, quoted := cyphertext.DecodeString(raw, cyphertext.AcceptedSource)
			if !quoted {
				value = cyphertext.SymbolicNameValue(raw)
			}
			data.credentials = append(data.credentials, cyphertext.CredentialSpan{Start: item.start, End: item.end, Raw: raw, Value: value, Password: item.rule == CypherParserRULE_credentialValue})
		}
		data.nodes = append(data.nodes, item)
		if parent >= 0 {
			data.nodes[parent].children = append(data.nodes[parent].children, id)
		}
		previous := parent
		parent = id
		return func() { parent = previous }
	})
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if syntaxErr == nil {
		data.effects = (StatementSyntax{data}).collectEffects()
	}
	return data, syntaxErr
}

func (data *statementSyntaxData) byteSpan(start, end int) (int, int) {
	if data.bytes == nil {
		start = max(0, min(start, len(data.source)))
		end = max(start, min(end, len(data.source)))
		return start, end
	}
	last := len(data.bytes) - 1
	start = max(0, min(start, last))
	end = max(start, min(end, last))
	return data.bytes[start], data.bytes[end]
}

// walkSyntax is the generic traversal relocated from walkCypherSyntax. Scope,
// graph-effect and placement consumers use this one non-pruning algorithm.
func walkSyntax[T any](node T, children func(T) []T, enter func(T) func()) {
	leave := enter(node)
	for _, child := range children(node) {
		walkSyntax(child, children, enter)
	}
	if leave != nil {
		leave()
	}
}

// Source returns the original source, including comments and quoted spellings.
func (syntax StatementSyntax) Source() string { return syntax.data.view.Original }

// Root is the root of this grammar entry. Node IDs are stable within the source.
func (syntax StatementSyntax) Root() SyntaxNode {
	return SyntaxNode{syntaxView[syntaxNode]{syntax.data, 0}}
}

// TokenCount includes hidden whitespace, comment channels and EOF.
func (syntax StatementSyntax) TokenCount() int { return len(syntax.data.tokens) }

// Token returns an immutable token view, not a lexer token pointer.
func (syntax StatementSyntax) Token(index int) SyntaxToken {
	return SyntaxToken{syntaxView[syntaxToken]{syntax.data, index}}
}

// Window selects a retained rule by its live-token interval. Whitespace and
// comments around a braced body do not change its identity or original spans.
// A query view may omit only its grammar-owned preamble/prefix children.
func (syntax StatementSyntax) Window(start, end int, rules ...int) (SyntaxNode, bool) {
	first, after := -1, -1
	for index, token := range syntax.data.tokens {
		if token.channel != 0 || token.kind == -1 || token.start < start || token.end > end {
			continue
		}
		if first < 0 {
			first = index
		}
		after = index + 1
	}
	for _, rule := range rules {
		for id, item := range syntax.data.nodes {
			if item.rule != rule {
				continue
			}
			if first < 0 {
				if item.start == item.end && item.start >= start && item.end <= end {
					return SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}, true
				}
				continue
			}
			if item.firstToken == first && item.afterToken == after {
				return SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}, true
			}
			if rule == CypherParserRULE_query && item.afterToken == after {
				for _, childID := range item.children {
					child := syntax.data.nodes[childID]
					if child.rule == CypherParserRULE_queryPrefix || child.rule == CypherParserRULE_cypherPreamble {
						continue
					}
					if child.firstToken == first {
						return SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}, true
					}
					break
				}
			}
		}
	}
	return SyntaxNode{}, false
}

// Walk composes visitors in one traversal, restoring their state in reverse
// order. An effect-cache hit cannot skip a separately supplied binding visitor.
func (syntax StatementSyntax) Walk(visitors ...SyntaxVisitor) {
	syntax.Root().Walk(visitors...)
}

// Walk visits this existing subtree without parsing a fragment or changing its
// original-source spans. All consumers use the same non-pruning traversal.
func (root SyntaxNode) Walk(visitors ...SyntaxVisitor) {
	var local [4]func()
	leaves := local[:]
	if len(visitors) > len(local) {
		leaves = make([]func(), len(visitors))
	}
	for i, visitor := range visitors {
		leaves[i] = visitor(root)
	}
	for _, child := range root.data.nodes[root.id].children {
		SyntaxNode{syntaxView[syntaxNode]{root.data, child}}.Walk(visitors...)
	}
	for i := len(visitors) - 1; i >= 0; i-- {
		if leaves[i] != nil {
			leaves[i]()
		}
	}
}

// ID returns this node's stable identity in its statement product.
func (node SyntaxNode) ID() int { return node.id }

// Source is the unmodified statement shared by every node in this product.
func (node SyntaxNode) Source() string { return node.data.view.Original }

// Valid reports whether a structural lookup found a node.
func (node SyntaxNode) Valid() bool { return node.data != nil && node.id >= 0 }

// Rule is a generated CypherParserRULE value, or -1 for a terminal.
func (node SyntaxNode) Rule() int { return node.data.nodes[node.id].rule }

// TokenRange returns the half-open interval in the one retained token stream.
func (node SyntaxNode) TokenRange() (int, int) {
	item := node.data.nodes[node.id]
	return item.firstToken, item.afterToken
}

// Code removes comments using the retained lexer channels, preserving gaps
// between tokens. It never scans quotes, splits an item list or lexes again.
func (node SyntaxNode) Code() string {
	item := node.data.nodes[node.id]
	start, end := item.start, item.end
	first, after := node.TokenRange()
	var builder strings.Builder
	last := start
	for index := first; index >= 0 && index < after; index++ {
		token := node.data.tokens[index]
		if token.kind != CypherLexerCOMMENT && token.kind != CypherLexerLINE_COMMENT {
			continue
		}
		builder.WriteString(node.data.source[last:token.start])
		builder.WriteByte(' ')
		last = token.end
	}
	if last == start {
		return node.data.source[start:end]
	}
	builder.WriteString(node.data.source[last:end])
	return builder.String()
}

// Name returns a symbol's decoded identity. Its spelling is available in Text.
func (node SyntaxNode) Name() (string, bool) {
	item := node.data.nodes[node.id]
	return item.name, item.rule == CypherParserRULE_symbol
}

// Parent returns the enclosing CST node, if this is not the root.
func (node SyntaxNode) Parent() (SyntaxNode, bool) {
	parent := node.data.nodes[node.id].parent
	return SyntaxNode{syntaxView[syntaxNode]{node.data, parent}}, parent >= 0
}

// ChildCount is the number of direct CST children, including terminals.
func (node SyntaxNode) ChildCount() int { return len(node.data.nodes[node.id].children) }

// Child returns a direct immutable child view.
func (node SyntaxNode) Child(index int) SyntaxNode {
	return SyntaxNode{syntaxView[syntaxNode]{node.data, node.data.nodes[node.id].children[index]}}
}

// ChildRule returns a direct rule child, or an absent view. It never searches
// another clause or a nested query for a missing part of the current clause.
func (node SyntaxNode) ChildRule(rule int) SyntaxNode {
	if !node.Valid() {
		return SyntaxNode{}
	}
	for _, id := range node.data.nodes[node.id].children {
		if node.data.nodes[id].rule == rule {
			return SyntaxNode{syntaxView[syntaxNode]{node.data, id}}
		}
	}
	return SyntaxNode{}
}

// DirectToken selects a terminal owned by this rule, excluding nested rules.
func (node SyntaxNode) DirectToken(kind int) (SyntaxToken, bool) {
	for _, id := range node.data.nodes[node.id].children {
		child := node.data.nodes[id]
		if child.rule < 0 && child.firstToken >= 0 && node.data.tokens[child.firstToken].kind == kind {
			return SyntaxToken{syntaxView[syntaxToken]{node.data, child.firstToken}}, true
		}
	}
	return SyntaxToken{}, false
}

// IsToken reports a terminal's kind; rule nodes and absent views are false.
func (node SyntaxNode) IsToken(kind int) bool {
	if !node.Valid() {
		return false
	}
	item := node.data.nodes[node.id]
	return item.rule < 0 && item.firstToken >= 0 && node.data.tokens[item.firstToken].kind == kind
}

// QualifiedName decodes the direct symbol components of an invocation or USE
// name. Comments and token gaps never become part of an identity.
func (node SyntaxNode) QualifiedName() string {
	var parts []string
	for _, id := range node.data.nodes[node.id].children {
		item := node.data.nodes[id]
		if item.rule == CypherParserRULE_symbol {
			parts = append(parts, item.name)
		}
	}
	return strings.Join(parts, ".")
}

// Kind is the generated lexer token kind, including EOF.
func (token SyntaxToken) Kind() int { return token.data.tokens[token.id].kind }

// Channel is the lexer channel; comments and whitespace are retained.
func (token SyntaxToken) Channel() int { return token.data.tokens[token.id].channel }

func (syntax StatementSyntax) recordCredentials(ctx context.Context) {
	set := contextslot.Get[cyphertext.CredentialSet](ctx)
	if set == nil || !syntax.Valid() {
		return
	}
	source := syntax.data.view
	if retained, ok := cyphertext.RetainedSource(ctx, syntax.data.source, cyphertext.AcceptedSource); ok {
		source = retained
	}
	for _, span := range syntax.data.credentials {
		set.RecordSource(source, span.Start, span.End, span.Value, span.Password)
	}
}
