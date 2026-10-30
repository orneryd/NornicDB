// Author of retained-input changes: Codex R17 implementation agent, 29 October 2026.
// Author of the R17 prediction correction: Codex, 26 October 2026.
package antlr

import (
	"context"
	"strings"
	"sync"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/internal/contextslot"
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
	// UseEntry reads only the leading reference; semantic routing validates its follower.
	UseEntry
)

// FragmentEntry preserves the release's explicit-stage inspection API. The
// execution readers use InspectSyntax so they consume the retained owner.
const FragmentEntry = fragmentEntry

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
			first, _ = (SyntaxToken{syntaxView[syntaxToken]{syntax.data, index}}).Span()
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

// inputSpan selects the absolute interval in the one retained parser input.
func (view syntaxView[nodeKind]) inputSpan() (int, int) {
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
	return accepted.Start, accepted.End
}

// InputSpan is the UTF-8 interval in this retained parser-input window. It
// preserves the accepted coordinate frame when a source adapter reads a known
// suffix, without searching for its value in the enclosing input.
func (view syntaxView[nodeKind]) InputSpan() (int, int) {
	start, end := view.inputSpan()
	return start - view.data.inputOffset, end - view.data.inputOffset
}

// Span maps the shared node/token input interval into its original source window.
func (view syntaxView[nodeKind]) Span() (int, int) {
	start, end := view.inputSpan()
	span, _ := view.data.view.OriginalSpan(cyphertext.Span{Start: start, End: end}, cyphertext.SourceExtent)
	return span.Start - view.data.offset, span.End - view.data.offset
}

// Text returns the selected node or token's exact original source bytes.
func (view syntaxView[nodeKind]) Text() string {
	start, end := view.Span()
	return view.data.view.Original[view.data.offset+start : view.data.offset+end]
}

// SyntaxOrigin distinguishes node identities carried by retained windows and
// body inspection entries, stages and dialects. Separately produced node IDs
// cannot alias a different admitted Script's IDs.
// A window's node numbers still belong to its enclosing retained product.
type SyntaxOrigin struct {
	tree         *syntaxNodeData
	root, offset int
}

func (data *statementSyntaxData) origin() SyntaxOrigin {
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
	view        *cyphertext.Source
	effects     statementEffectsData
	offset      int // original-source origin of this retained window
	sourceEnd   int // original-source end of this retained window
	inputOffset int // accepted-input start of this retained window
	root        int // retained node selected by a view; zero for a complete parse
	firstToken  int
	afterToken  int
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
	if scope := statementSyntaxScopeFrom(ctx); scope != nil {
		scope.ensureRoot(ctx, source, cypher25, stage)
		if data := scope.product(key); data != nil {
			syntax := StatementSyntax{data}
			syntax.recordCredentials(ctx)
			return syntax, nil
		}
	}
	syntax, err := parseCachedSyntax(ctx, source, entry, cypher25, stage)
	if err == nil {
		if scope := statementSyntaxScopeFrom(ctx); scope != nil {
			syntax.data = scope.retain(key, syntax.data)
		}
	}
	syntax.recordCredentials(ctx)
	return syntax, err
}

func parseCachedSyntax(ctx context.Context, source string, entry SyntaxEntry, cypher25 bool, stage SourceStage) (StatementSyntax, error) {
	if err := ctx.Err(); err != nil {
		return StatementSyntax{}, err
	}
	key := statementSyntaxKey{source: source, entry: entry, cypher25: cypher25, stage: stage}
	statementSyntaxCache.RLock()
	data := statementSyntaxCache.entries[key]
	if data == nil && entry == fragmentEntry {
		for _, complete := range []SyntaxEntry{StatementEntry} {
			if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: complete, cypher25: cypher25, stage: stage}]; prior != nil {
				data = prior
				break
			}
		}
	}
	if data == nil && entry == StatementEntry {
		if prior := statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: fragmentEntry, cypher25: cypher25, stage: stage}]; prior != nil && prior.nodes[0].rule == CypherParserRULE_script {
			data = prior
		}
	}
	statementSyntaxCache.RUnlock()
	if data != nil {
		return StatementSyntax{data}, nil
	}
	projection := cyphertext.NewSource(ctx, source, stage)
	if projection.Err != nil {
		return StatementSyntax{}, projection.Err
	}
	data, err := parseStatementSyntax(ctx, projection, entry, nil)
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
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: StatementEntry, cypher25: cypher25, stage: stage}] = data
			statementSyntaxCache.entries[statementSyntaxKey{source: source, entry: fragmentEntry, cypher25: cypher25, stage: stage}] = data
		}
	}
	statementSyntaxCache.Unlock()
	return StatementSyntax{data}, nil
}

// parseStatementSyntax relocates readBatchTree's lexer/parser entry owner.
// Token filling uses that same lexer; retaining trivia does not lex again.
func parseStatementSyntax(ctx context.Context, projection *cyphertext.Source, entry SyntaxEntry, observe func(antlr4.Tree)) (data *statementSyntaxData, err error) {
	return parseStatementSyntaxPredictionPolicy(ctx, projection, entry, observe, antlr4.PredictionModeSLL, true)
}

// Keep an independent prediction-mode reference for the corpus parity check.
func parseStatementSyntaxPrediction(ctx context.Context, source string, entry SyntaxEntry, observe func(antlr4.Tree), mode int) (data *statementSyntaxData, err error) {
	return parseStatementSyntaxPredictionPolicy(ctx, cyphertext.NewSource(ctx, source, RawSource), entry, observe, mode, false)
}

// Every syntax entry shares this policy. SLL produces the retained tree,
// using full context within CASE result expressions. An unsuccessful attempt
// is discarded and the original source is read with LL, preserving diagnostics
// and lexer errors rather than rewinding an already-filled stream.
func parseStatementSyntaxPredictionPolicy(ctx context.Context, projection *cyphertext.Source, entry SyntaxEntry, observe func(antlr4.Tree), mode int, caseResults bool) (data *statementSyntaxData, err error) {
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
	// Both attempts belong to one parse and one prediction lease. Releasing
	// SLL before retry can rotate its warm generation and make LL rebuild all
	// the same prediction states. The budget owner releases only after retry.
	prediction := acquirePrediction()
	defer prediction.release()
	return parseStatementSyntaxAttempt(ctx, projection, entry, observe, mode, prediction, caseResults)
}

func parseStatementSyntaxAttempt(ctx context.Context, projection *cyphertext.Source, entry SyntaxEntry, observe func(antlr4.Tree), mode int, prediction *parserPrediction, caseResults bool) (data *statementSyntaxData, err error) {
	defer func() {
		if failure := recover(); failure != nil {
			if _, retry := failure.(syntaxPredictionRetry); retry {
				data, err = parseStatementSyntaxAttempt(ctx, projection, entry, observe, antlr4.PredictionModeLL, prediction, false)
				return
			}
			panic(failure)
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	source := projection.Text
	inputView, input := sourceInputStream(ctx, projection)
	lexer := NewCypherLexer(input)
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
	if mode == antlr4.PredictionModeSLL && caseResults {
		parser.AddParseListener(&caseResultPrediction{parser: parser, mode: mode})
	}
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
		trailing := stream.LT(1)
		listener.errors = append(listener.errors, localization.CypherANTLRUnexpectedInput(trailing.GetLine(), trailing.GetColumn()))
	}
	tokens.Fill()
	if len(listener.errors) != 0 && mode == antlr4.PredictionModeSLL && ctx.Err() == nil {
		return parseStatementSyntaxAttempt(ctx, projection, entry, observe, antlr4.PredictionModeLL, prediction, false)
	}
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
	data = &statementSyntaxData{source: source, view: projection, sourceEnd: len(projection.Original), diagnostics: append([]localization.Message(nil), listener.errors...)}
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

// A fast attempt has no diagnostic or recovery contract. Stop at its first
// error; the full-context attempt alone recovers and publishes diagnostics.
type syntaxPredictionRetry struct{}

type syntaxPredictionListener struct{ *parseErrorListener }

func (*syntaxPredictionListener) SyntaxError(antlr4.Recognizer, interface{}, int, int, string, antlr4.RecognitionException) {
	panic(syntaxPredictionRetry{})
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
func (syntax StatementSyntax) Source() string {
	return syntax.data.view.Original[syntax.data.offset:syntax.data.sourceEnd]
}

// Root is the root of this grammar entry. Node IDs are stable within the source.
func (syntax StatementSyntax) Root() SyntaxNode {
	return SyntaxNode{syntaxView[syntaxNode]{syntax.data, syntax.data.root}}
}

// TokenCount includes hidden whitespace, comment channels and EOF.
func (syntax StatementSyntax) TokenCount() int {
	return syntax.data.afterToken - syntax.data.firstToken
}

// Token returns an immutable token view, not a lexer token pointer.
func (syntax StatementSyntax) Token(index int) SyntaxToken {
	return SyntaxToken{syntaxView[syntaxToken]{syntax.data, syntax.data.firstToken + index}}
}

// Window selects a retained rule by its live-token interval. Whitespace and
// comments around a braced body do not change its identity or original spans.
// A query view may omit only its grammar-owned preamble/prefix children.
func (syntax StatementSyntax) Window(start, end int, rules ...int) (SyntaxNode, bool) {
	first, after := -1, -1
	for index := syntax.data.firstToken; index < syntax.data.afterToken; index++ {
		token := syntax.data.tokens[index]
		tokenStart, tokenEnd := (SyntaxToken{syntaxView[syntaxToken]{syntax.data, index}}).Span()
		if token.channel != 0 || token.kind == -1 || tokenStart < start || tokenEnd > end {
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
				nodeStart, nodeEnd := (SyntaxNode{syntaxView[syntaxNode]{syntax.data, id}}).Span()
				if nodeStart == nodeEnd && nodeStart >= start && nodeEnd <= end {
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
func (node SyntaxNode) Source() string {
	return node.data.view.Original[node.data.offset:node.data.sourceEnd]
}

// Statement returns the retained grammar owner of this node, preserving its
// node namespace, complete input and existing source-window origin.
func (node SyntaxNode) Statement() StatementSyntax { return StatementSyntax{node.data} }

// Window selects this exact occurrence as a source-local grammar product.
// The node/token arenas and canonical Source mapping remain shared. Consumers
// that need the enclosing statement's node namespace keep the original node;
// source-local compatibility descriptions use this bounded view instead.
func (node SyntaxNode) Window() StatementSyntax {
	item := node.data.nodes[node.id]
	span, _ := node.data.view.OriginalSpan(cyphertext.Span{Start: item.start, End: item.end}, cyphertext.SourceExtent)
	view := syntaxInspection{data: node.data, node: node.id, start: span.Start, end: span.End, codeStart: item.start, codeEnd: item.end}
	return StatementSyntax{view.rebase()}
}

// Valid reports whether a structural lookup found a node.
func (node SyntaxNode) Valid() bool { return node.data != nil && node.id >= 0 }

// Rule is a generated CypherParserRULE value, or -1 for a terminal.
func (node SyntaxNode) Rule() int { return node.data.nodes[node.id].rule }

// TokenRange returns the half-open interval in the one retained token stream.
func (node SyntaxNode) TokenRange() (int, int) {
	item := node.data.nodes[node.id]
	return item.firstToken - node.data.firstToken, item.afterToken - node.data.firstToken
}

// Code removes comments using the retained lexer channels, preserving gaps
// between tokens. It never scans quotes, splits an item list or lexes again.
func (node SyntaxNode) Code() string {
	return node.code(nil)
}

// CodeSource retains the grammar input's canonical Source association while
// selecting this occurrence and removing comments through the retained lexer
// channels. Nil selects the source already owned by this CST. A supplied source
// must carry the same parser-input bytes, including any prior source mapping.
func (node SyntaxNode) CodeSource(source *cyphertext.Source) *cyphertext.Source {
	start, end := node.inputSpan()
	return node.codeSourceWindow(source, start, end)
}

// CodeSourceWindow carries a clause stretch between two retained occurrences.
// It does not create a grammar product admitting an omitted continuation.
func (node SyntaxNode) CodeSourceWindow(first, last SyntaxNode) *cyphertext.Source {
	if !node.Valid() || !first.Valid() || !last.Valid() || &node.data.nodes[0] != &first.data.nodes[0] || &node.data.nodes[0] != &last.data.nodes[0] {
		panic("antlr: source window requires one retained syntax owner")
	}
	start, _ := first.inputSpan()
	_, end := last.inputSpan()
	a, b := node.inputSpan()
	if start < a || end > b || end < start {
		panic("antlr: source window requires a contained extent")
	}
	return node.codeSourceWindow(nil, start, end)
}

func (node SyntaxNode) codeSourceWindow(source *cyphertext.Source, start, end int) *cyphertext.Source {
	if source == nil {
		source = node.data.view
	}
	if source.Text != node.data.source {
		panic("antlr: CodeSource requires the carried grammar input")
	}
	view := source.Window(start, end)
	var edits []cyphertext.SourceEdit
	code := node.codeRange(start, end, func(original, accepted cyphertext.Span) {
		edits = append(edits, cyphertext.SourceEdit{Original: original, Accepted: accepted})
	})
	if len(edits) == 0 {
		return view
	}
	return view.Rewrite(code, edits)
}

// InputBefore carries this container's parser-input prefix before a known
// descendant occurrence. It is a Source window for lexical consumers, not a
// grammar product admitting a truncated statement. Comments retain their input
// bytes and source association; existing lexical readers own their policy.
func (node SyntaxNode) InputBefore(child SyntaxNode) *cyphertext.Source {
	if &node.data.nodes[0] != &child.data.nodes[0] {
		panic("antlr: source window requires one retained syntax owner")
	}
	container, item := node.data.nodes[node.id], child.data.nodes[child.id]
	if item.start < container.start || item.end > container.end {
		panic("antlr: source window requires a descendant extent")
	}
	return node.data.view.Window(container.start, item.start)
}

func (node SyntaxNode) code(record func(cyphertext.Span, cyphertext.Span)) string {
	start, end := node.inputSpan()
	return node.codeRange(start, end, record)
}

func (node SyntaxNode) codeRange(start, end int, record func(cyphertext.Span, cyphertext.Span)) string {
	first, after := node.TokenRange()
	var builder strings.Builder
	last := start
	for index := first; index >= 0 && index < after; index++ {
		token := node.data.tokens[node.data.firstToken+index]
		if token.start < start || token.end > end {
			continue
		}
		if token.kind != CypherLexerCOMMENT && token.kind != CypherLexerLINE_COMMENT {
			continue
		}
		builder.WriteString(node.data.source[last:token.start])
		accepted := builder.Len()
		builder.WriteByte(' ')
		if record != nil {
			record(cyphertext.Span{Start: token.start - start, End: token.end - start}, cyphertext.Span{Start: accepted, End: accepted + 1})
		}
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
	if node.id == node.data.root {
		return SyntaxNode{}, false
	}
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
