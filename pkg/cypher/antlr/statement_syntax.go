// Author of the R17 prediction correction: Codex, 26 October 2026.
package antlr

import (
	"context"
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
	// fragmentEntry is private: consumers inspect retained syntax through
	// InspectSyntax or LeadingCallSyntax rather than requesting another parse.
	fragmentEntry
	PatternEntry
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
	for index := syntax.data.firstToken; index < syntax.data.afterToken; index++ {
		token := syntax.data.tokens[index]
		if token.channel == 0 && token.kind != -1 {
			first = token.start - syntax.data.offset
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

// SyntaxNode is an immutable node identity within a StatementSyntax.
type SyntaxNode struct {
	data *statementSyntaxData
	id   int
}

// SyntaxToken is an immutable lexer-token identity within a StatementSyntax.
type SyntaxToken struct {
	data  *statementSyntaxData
	index int
}

// SyntaxOrigin distinguishes node identities carried by retained windows.
// Standalone parses have source-local node numbers and use the zero origin.
// A window's node numbers still belong to its enclosing retained product.
type SyntaxOrigin struct {
	tree         *syntaxNodeData
	root, offset int
}

func (data *statementSyntaxData) origin() SyntaxOrigin {
	if data.root == 0 && data.offset == 0 {
		return SyntaxOrigin{}
	}
	return SyntaxOrigin{tree: &data.nodes[0], root: data.root, offset: data.offset}
}

// Origin is the identity namespace used by this source window's node IDs.
func (syntax StatementSyntax) Origin() SyntaxOrigin { return syntax.data.origin() }

// Origin is the identity namespace used by this node's source window.
func (node SyntaxNode) Origin() SyntaxOrigin { return node.data.origin() }

// SyntaxVisitor enters a node and optionally restores its state on exit.
// Every visitor sees every child. A visitor cannot prune traversal.
type SyntaxVisitor func(SyntaxNode) func()

type statementSyntaxData struct {
	source      string
	offset      int // original CST coordinates minus this view's source origin
	root        int // retained node selected by a view; zero for a complete parse
	firstToken  int
	afterToken  int
	bytes       []int // rune boundary to original UTF-8 byte boundary
	nodes       []syntaxNodeData
	tokens      []syntaxTokenData
	diagnostics []string // Raw grammar diagnostics; failed products are never cached.
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
}

var statementSyntaxCache = struct {
	sync.RWMutex
	entries map[statementSyntaxKey]*statementSyntaxData
}{entries: make(map[statementSyntaxKey]*statementSyntaxData)}

const statementSyntaxCacheLimit = 4096

// ParseSyntax uses the existing grammar lexer/parser owner once and retains its
// complete product. Dialect and entry are part of the immutable cache identity;
// incoming scopes, procedure signatures and values belong to bound compilation.
func ParseSyntax(ctx context.Context, source string, entry SyntaxEntry, cypher25 bool) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	key := statementSyntaxKey{source: source, entry: entry, cypher25: cypher25}
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25)
		if data := scope.product(key); data != nil {
			return StatementSyntax{data}, nil
		}
	}
	syntax, err := parseCachedSyntax(ctx, source, entry, cypher25)
	if err == nil {
		if scope := statementSyntaxScopeFrom(ctx); scope != nil {
			syntax.data = scope.retain(key, syntax.data)
		}
	}
	return syntax, err
}

func parseCachedSyntax(ctx context.Context, source string, entry SyntaxEntry, cypher25 bool) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	key := statementSyntaxKey{source: source, entry: entry, cypher25: cypher25}
	statementSyntaxCache.RLock()
	data := statementSyntaxCache.entries[key]
	if data == nil && entry == fragmentEntry {
		for _, complete := range []SyntaxEntry{StatementEntry} {
			if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: complete, cypher25: cypher25}]; prior != nil {
				data = prior
				break
			}
		}
	}
	if data == nil && entry == StatementEntry {
		if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: fragmentEntry, cypher25: cypher25}]; prior != nil && prior.nodes[0].rule == CypherParserRULE_script {
			data = prior
		}
	}
	statementSyntaxCache.RUnlock()
	if data != nil {
		return StatementSyntax{data}, nil
	}
	data, err := parseStatementSyntax(ctx, source, entry, nil)
	if err != nil {
		return StatementSyntax{data}, err
	}
	statementSyntaxCache.Lock()
	if prior := statementSyntaxCache.entries[key]; prior != nil {
		data = prior
	} else {
		if len(statementSyntaxCache.entries) >= statementSyntaxCacheLimit {
			clear(statementSyntaxCache.entries)
		}
		statementSyntaxCache.entries[key] = data
		if data.nodes[0].rule == CypherParserRULE_script && (entry == StatementEntry || entry == fragmentEntry) {
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: StatementEntry, cypher25: cypher25}] = data
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: fragmentEntry, cypher25: cypher25}] = data
		}
	}
	statementSyntaxCache.Unlock()
	return StatementSyntax{data}, nil
}

// parseStatementSyntax relocates readBatchTree's lexer/parser entry owner.
// Token filling uses that same lexer; retaining trivia does not lex again.
func parseStatementSyntax(ctx context.Context, source string, entry SyntaxEntry, observe func(antlr4.Tree)) (data *statementSyntaxData, err error) {
	return parseStatementSyntaxPrediction(ctx, source, entry, observe, antlr4.PredictionModeSLL)
}

// Every syntax entry shares the same prediction policy. SLL produces the
// retained tree on the ordinary path. An unsuccessful attempt is discarded and
// the original source is read with LL, preserving full-context diagnostics and
// ensuring lexer errors are not lost by rewinding an already-filled stream.
func parseStatementSyntaxPrediction(ctx context.Context, source string, entry SyntaxEntry, observe func(antlr4.Tree), mode int) (data *statementSyntaxData, err error) {
	defer recoverPredictionCancellation(&err)
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	// Both attempts belong to one parse and one prediction lease. Releasing
	// SLL before retry can rotate its warm generation and make LL rebuild all
	// the same prediction states. The budget owner releases only after retry.
	prediction := acquirePrediction()
	defer prediction.release()
	return parseStatementSyntaxAttempt(ctx, source, entry, observe, mode, prediction)
}

func parseStatementSyntaxAttempt(ctx context.Context, source string, entry SyntaxEntry, observe func(antlr4.Tree), mode int, prediction *parserPrediction) (data *statementSyntaxData, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			if _, retry := failure.(syntaxPredictionRetry); retry {
				data, err = parseStatementSyntaxAttempt(ctx, source, entry, observe, antlr4.PredictionModeLL, prediction)
				return
			}
			panic(failure)
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	lexer := NewCypherLexer(contextInputStream(ctx, source))
	listener := &parseErrorListener{}
	var errors antlr4.ErrorListener = listener
	if mode == antlr4.PredictionModeSLL {
		errors = &syntaxPredictionListener{parseErrorListener: listener}
	}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(errors)
	tokens := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	var stream antlr4.TokenStream = tokens
	if ctx.Done() != nil {
		stream = &contextTokenStream{TokenStream: tokens, ctx: ctx}
	}
	parser := NewCypherParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(errors)
	usePrediction(parser, prediction)
	defer func() { parser.Interpreter = nil }()
	parser.Interpreter.SetPredictionMode(mode)
	var tree antlr4.Tree
	switch entry {
	case CallEntry:
		tree = parser.CallSubquery()
	case ClauseEntry:
		tree = parser.SubqueryBody()
	case ExpressionEntry:
		tree = parser.Expression()
	case fragmentEntry:
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
		listener.errors = append(listener.errors, "unexpected trailing input")
	}
	tokens.Fill()
	if len(listener.errors) != 0 && mode == antlr4.PredictionModeSLL && ctx.Err() == nil {
		return parseStatementSyntaxAttempt(ctx, source, entry, observe, antlr4.PredictionModeLL, prediction)
	}
	var syntaxErr error
	if len(listener.errors) != 0 {
		message := localization.CypherANTLRParseSyntaxError(strings.Join(listener.errors, "; "))
		syntaxErr = localization.NewLocalizedError(string(message.ID), message, nil)
	}
	if observe != nil {
		observe(tree)
	}
	data = &statementSyntaxData{source: source, diagnostics: append([]string(nil), listener.errors...)}
	for offset := range source {
		data.bytes = append(data.bytes, offset)
	}
	data.bytes = append(data.bytes, len(source))
	for _, token := range tokens.GetAllTokens() {
		start, end := data.byteSpan(token.GetStart(), token.GetStop()+1)
		data.tokens = append(data.tokens, syntaxTokenData{kind: token.GetTokenType(), channel: token.GetChannel(), start: start, end: end})
	}
	data.afterToken = len(data.tokens)
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
	return data, syntaxErr
}

// A fast attempt has no diagnostic or recovery contract. Stop at its first
// error; the full-context attempt alone recovers and publishes diagnostics.
type syntaxPredictionRetry struct{}

type syntaxPredictionListener struct{ *parseErrorListener }

func (*syntaxPredictionListener) SyntaxError(antlr4.Recognizer, interface{}, int, int, string, antlr4.RecognitionException) {
	panic(syntaxPredictionRetry{})
}

func (data *statementSyntaxData) byteSpan(start, end int) (int, int) {
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
func (syntax StatementSyntax) Source() string { return syntax.data.source }

// Root is the root of this grammar entry. Node IDs are stable within the source.
func (syntax StatementSyntax) Root() SyntaxNode { return SyntaxNode{syntax.data, syntax.data.root} }

// TokenCount includes hidden whitespace, comment channels and EOF.
func (syntax StatementSyntax) TokenCount() int {
	return syntax.data.afterToken - syntax.data.firstToken
}

// Token returns an immutable token view, not a lexer token pointer.
func (syntax StatementSyntax) Token(index int) SyntaxToken {
	return SyntaxToken{syntax.data, syntax.data.firstToken + index}
}

// Window selects a retained rule by its live-token interval. Whitespace and
// comments around a braced body do not change its identity or original spans.
// A query view may omit only its grammar-owned preamble/prefix children.
func (syntax StatementSyntax) Window(start, end int, rules ...int) (SyntaxNode, bool) {
	first, after := -1, -1
	for index := syntax.data.firstToken; index < syntax.data.afterToken; index++ {
		token := syntax.data.tokens[index]
		if token.channel != 0 || token.kind == -1 || token.start < start+syntax.data.offset || token.end > end+syntax.data.offset {
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
				if item.start == item.end && item.start >= start+syntax.data.offset && item.end <= end+syntax.data.offset {
					return SyntaxNode{syntax.data, id}, true
				}
				continue
			}
			if item.firstToken == first && item.afterToken == after {
				return SyntaxNode{syntax.data, id}, true
			}
			if rule == CypherParserRULE_query && item.afterToken == after {
				for _, childID := range item.children {
					child := syntax.data.nodes[childID]
					if child.rule == CypherParserRULE_queryPrefix || child.rule == CypherParserRULE_cypherPreamble {
						continue
					}
					if child.firstToken == first {
						return SyntaxNode{syntax.data, id}, true
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
		(SyntaxNode{root.data, child}).Walk(visitors...)
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
func (node SyntaxNode) Source() string { return node.data.source }

// Valid reports whether a structural lookup found a node.
func (node SyntaxNode) Valid() bool { return node.data != nil && node.id >= 0 }

// Rule is a generated CypherParserRULE value, or -1 for a terminal.
func (node SyntaxNode) Rule() int { return node.data.nodes[node.id].rule }

// Span returns the original, half-open UTF-8 byte interval.
func (node SyntaxNode) Span() (int, int) {
	item := node.data.nodes[node.id]
	return item.start - node.data.offset, item.end - node.data.offset
}

// TokenRange returns the half-open interval in the one retained token stream.
func (node SyntaxNode) TokenRange() (int, int) {
	item := node.data.nodes[node.id]
	return item.firstToken - node.data.firstToken, item.afterToken - node.data.firstToken
}

// Text returns original text. Expression evaluation may request Code instead.
func (node SyntaxNode) Text() string {
	start, end := node.Span()
	return node.data.source[start:end]
}

// Code removes comments using the retained lexer channels, preserving gaps
// between tokens. It never scans quotes, splits an item list or lexes again.
func (node SyntaxNode) Code() string {
	start, end := node.Span()
	first, after := node.TokenRange()
	var builder strings.Builder
	last := start
	for index := first; index >= 0 && index < after; index++ {
		token := node.data.tokens[node.data.firstToken+index]
		if token.kind != CypherLexerCOMMENT && token.kind != CypherLexerLINE_COMMENT {
			continue
		}
		builder.WriteString(node.data.source[last : token.start-node.data.offset])
		builder.WriteByte(' ')
		last = token.end - node.data.offset
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
	if node.id == node.data.root {
		return SyntaxNode{}, false
	}
	parent := node.data.nodes[node.id].parent
	return SyntaxNode{node.data, parent}, parent >= 0
}

// ChildCount is the number of direct CST children, including terminals.
func (node SyntaxNode) ChildCount() int { return len(node.data.nodes[node.id].children) }

// Child returns a direct immutable child view.
func (node SyntaxNode) Child(index int) SyntaxNode {
	return SyntaxNode{node.data, node.data.nodes[node.id].children[index]}
}

// ChildRule returns a direct rule child, or an absent view. It never searches
// another clause or a nested query for a missing part of the current clause.
func (node SyntaxNode) ChildRule(rule int) SyntaxNode {
	if !node.Valid() {
		return SyntaxNode{}
	}
	for _, id := range node.data.nodes[node.id].children {
		if node.data.nodes[id].rule == rule {
			return SyntaxNode{node.data, id}
		}
	}
	return SyntaxNode{}
}

// DirectToken selects a terminal owned by this rule, excluding nested rules.
func (node SyntaxNode) DirectToken(kind int) (SyntaxToken, bool) {
	for _, id := range node.data.nodes[node.id].children {
		child := node.data.nodes[id]
		if child.rule < 0 && child.firstToken >= 0 && node.data.tokens[child.firstToken].kind == kind {
			return SyntaxToken{node.data, child.firstToken}, true
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
func (token SyntaxToken) Kind() int { return token.data.tokens[token.index].kind }

// Channel is the lexer channel; comments and whitespace are retained.
func (token SyntaxToken) Channel() int { return token.data.tokens[token.index].channel }

// Span returns this token's original, half-open UTF-8 byte interval.
func (token SyntaxToken) Span() (int, int) {
	item := token.data.tokens[token.index]
	return item.start - token.data.offset, item.end - token.data.offset
}

// Text returns the token's original source, without decoding or rewriting it.
func (token SyntaxToken) Text() string {
	start, end := token.Span()
	return token.data.source[start:end]
}
