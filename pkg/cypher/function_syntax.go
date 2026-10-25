package cypher

import (
	"strings"
	"sync/atomic"
)

// firstTopLevelByte searches retained punctuation without scanning source again.
func (syntax *functionSyntax) firstTopLevelByte(target byte) int {
	for _, at := range syntax.rootPunctuation {
		if syntax.text[at] == target {
			return at
		}
	}
	return -1
}

// Leading trivia is a lexical request of its own. Statement routing must not
// construct or look up a complete call inventory just to find its first token.
func lexicalLeadingTriviaEnd(text string) int {
	return queryGapEnd(text, 0)
}

// A missing opening delimiter conclusively excludes every function call,
// including quoted names. Presence still requires the lexical owner to read it.
func mayHaveFunctionCall(text string) bool {
	return strings.ContainsRune(text, '(')
}

// lexicalExpressionText returns expression code independently of optional query
// normalization. Comment-looking quoted values and names remain data, and a
// comment between tokens must not join them into a different identifier.
// Retained lexical facts own this operation for identity and grouping readers
// just as they do for function arguments.
func lexicalExpressionText(text string) string {
	return strings.TrimSpace(lexicalSourceText(text))
}

// lexicalSourceText is lexicalExpressionText without trimming: each comment
// becomes one space, as the retained facts' source renders a span.
func lexicalSourceText(text string) string {
	if !strings.Contains(text, "//") && !strings.Contains(text, "/*") {
		return text
	}
	facts := readExpressionLexicalFacts(text)
	return facts.source(syntaxSpan{0, len(text)})
}

// parseFunctionCallWS parses a standalone function call expression, tolerating
// whitespace between the function name and the opening parenthesis.
//
// Examples:
//   - "toLower(n.name)"   -> ("toLower", "n.name", true)
//   - "toLower ( n.name)" -> ("toLower", " n.name", true)
//
// Returns ok=false if the expression is not a standalone function call.
func parseFunctionCallWS(expr string) (name string, inner string, ok bool) {
	call, close, ok := lexicalStandaloneCall(expr)
	if !ok {
		return "", "", false
	}
	return call.name, call.syntax.source(syntaxSpan{call.open + 1, close}), true
}

// lexicalCallAt looks up a non-procedure call by its first token offset.
func lexicalCallAt(text string, at int) (functionCallSpan, bool) {
	facts := readExpressionLexicalFacts(text)
	if i, ok := facts.at[at]; ok {
		token := facts.tokens[i]
		if token.kind == lexicalName && token.call >= 0 {
			call := facts.calls[token.call]
			return call, !call.procedure
		}
	}
	return functionCallSpan{}, false
}

// lexicalStandaloneCall is the call that is the whole text, with its close,
// as the retained facts' standaloneFunctionCall reads it, without them.
func lexicalStandaloneCall(text string) (functionCallSpan, int, bool) {
	call, ok := lexicalCallAt(text, lexicalLeadingTriviaEnd(text))
	if !ok {
		return functionCallSpan{}, -1, false
	}
	close := call.close(text)
	if close < 0 || queryGapEnd(text, close+1) != len(text) {
		return functionCallSpan{}, -1, false
	}
	return call, close, true
}

// lexicalLeadingNameMayBe is false when the call that opens text cannot be
// named name: an unquoted name is the text's leading bytes as written, so an
// undotted ASCII name that differs from them settles the question before
// any token is read. Quoted, dotted or non-ASCII names are not settled.
func lexicalLeadingNameMayBe(text, name string) bool {
	at := lexicalLeadingTriviaEnd(text)
	if at >= len(text) || name == "" || isCypherBacktickByte(text[at]) {
		return at < len(text)
	}
	for index := 0; index < len(name); index++ {
		c := name[index]
		if c >= 0x80 || c == '.' {
			return true
		}
		if at+index >= len(text) {
			return false
		}
		b := text[at+index]
		if b >= 0x80 {
			return true
		}
		if asciiLowerByte(b) != asciiLowerByte(c) {
			return false
		}
	}
	return true
}

func standaloneFunctionCall(text string) (functionCallSpan, bool) {
	// Absence is conclusive even inside quoted data: no call can open here.
	if !strings.ContainsRune(text, '(') {
		return functionCallSpan{}, false
	}
	syntax := readFunctionSyntax(text)
	index := syntax.callIndex(syntax.leading)
	if index >= len(syntax.calls) {
		return functionCallSpan{}, false
	}
	call := syntax.calls[index]
	if call.procedure || call.start != syntax.leading || call.close(text) < 0 || queryGapEnd(text, call.close(text)+1) != len(text) {
		return functionCallSpan{}, false
	}
	return call, true
}

// functionCallSpan refers to the immutable call identity and argument group.
// Matching and argument boundaries are computed by the owner once.
type functionCallSpan struct {
	name             string
	normalized       string
	start, open      int
	nameEnd          int
	syntax           *functionSyntax
	group            int
	next             int
	procedure        bool
	inScalarSubquery bool
	annotation       bool
	syntaxKeyword    bool
}

// expressionCall separates clause-like parenthesized syntax from expression
// calls. The legacy filter binding form remains a supported native function.
func (call functionCallSpan) expressionCall() bool {
	return !call.procedure && !call.annotation && (!call.syntaxKeyword || call.normalized == "filter")
}

// The legacy matcher API permits ASCII spacing between a name and its open.
// Its compatibility policy reads the owned gap, with comments already rendered;
// general call parsing continues to accept the query lexer's Unicode trivia.
func (call functionCallSpan) asciiNameSpacing(text string) bool {
	gap := call.syntax.source(syntaxSpan{call.nameEnd, call.open})
	for index := range len(gap) {
		if !isSpaceByte(gap[index]) {
			return false
		}
	}
	return true
}

func (call functionCallSpan) close(text string) int {
	if call.syntax == nil || call.group < 0 {
		return -1
	}
	return call.syntax.groups[call.group].close
}

// nextCall visits the retained inventory for static and runtime consumers.
// It advances into arguments so nested calls are visited, and excludes syntax,
// procedure/graph names, type annotations, comments and string literals.
func (syntax *functionSyntax) nextCall(index int, descendSubqueries, includeProcedures, syntaxNames bool) (functionCallSpan, bool) {
	for _, call := range syntax.calls[syntax.callIndex(index):] {
		if call.start < index || call.procedure && !includeProcedures {
			continue
		}
		if !call.procedure && (call.annotation || !syntaxNames && call.syntaxKeyword) {
			continue
		}
		if !descendSubqueries && call.inScalarSubquery {
			continue
		}
		return call, true
	}
	return functionCallSpan{}, false
}

// functionCallBeforeParen looks up a retained call by its opening. The resume
// positions are ordered even for procedures without explicit parentheses.
func functionCallBeforeParen(text string, open int) (functionCallSpan, bool) {
	return readFunctionSyntax(text).callBeforeParen(open)
}

func (syntax *functionSyntax) callBeforeParen(open int) (functionCallSpan, bool) {
	calls := syntax.calls
	low, high := 0, len(calls)
	for low < high {
		middle := low + (high-low)/2
		if calls[middle].next < open+1 {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low < len(calls) && calls[low].open == open && !calls[low].procedure {
		return calls[low], true
	}
	return functionCallSpan{}, false
}

func functionCallAt(text string, at int) (functionCallSpan, bool) {
	return readFunctionSyntax(text).callAt(at)
}

// callCursor lets a text walk ask, at each candidate offset in ascending
// order, whether an expression call starts there. One forward call walk
// answers every query, with no fact list, so a walk over a long text costs
// one token pass. A query behind the previous one restarts the walk.
// A cursor holds the table across cache eviction. Both lookup directions are
// indexed; asking an earlier offset never restarts a lexical walk.
type callCursor struct {
	facts *expressionLexicalFacts
	view  expressionLexicalView
}

func newCallCursor(text string, views ...expressionLexicalView) callCursor {
	v := borrowedExpressionView(text, views)
	return callCursor{facts: v.facts, view: v}
}

func (cursor *callCursor) callAt(at int) (functionCallSpan, bool) {
	at += cursor.view.span.start
	if i, ok := cursor.facts.at[at]; ok {
		token := cursor.facts.tokens[i]
		if token.kind == lexicalName && token.call >= 0 {
			call := cursor.facts.calls[token.call]
			return call, call.expressionCall()
		}
	}
	return functionCallSpan{}, false
}

func (cursor *callCursor) callOpenedAt(open int) (functionCallSpan, bool) {
	open += cursor.view.span.start
	if i, ok := cursor.facts.at[open]; ok {
		token := cursor.facts.tokens[i]
		if token.kind == lexicalPunctuation && cursor.facts.text[open] == '(' && token.call >= 0 {
			call := cursor.facts.calls[token.call]
			return call, !call.procedure
		}
	}
	return functionCallSpan{}, false
}

// singleNameArgument is the variable a call reads through its one argument,
// as simpleSemanticIdentifier of the only element of call.arguments(nil), or
// "". It reads at most that argument: a group, quoted data or other
// punctuation in it is no simple name, so the reader stops there.
func singleNameArgument(text string, call functionCallSpan) string {
	if call.syntax == nil || call.group < 0 || call.close(text) < 0 {
		return ""
	}
	args := call.arguments(nil)
	if len(args) != 1 {
		return ""
	}
	return simpleSemanticIdentifier(args[0])
}

func (syntax *functionSyntax) callAt(at int) (functionCallSpan, bool) {
	index := syntax.callIndex(at)
	if index < len(syntax.calls) {
		call := syntax.calls[index]
		if call.start == at && !call.procedure {
			return call, true
		}
	}
	return functionCallSpan{}, false
}

// functionNameStartsAt prevents a builtin scanner from matching only the last
// component of a qualified function name (custom.range is not range), and is
// the one test for a word that follows a dot: a property key (x . and) or a
// name component is never a keyword or a function of its own. Whitespace and
// comments may stand around the dot.
func functionNameStartsAt(text string, at int) bool {
	if at == 0 {
		return true
	}
	if isIdentByte(text[at-1]) {
		return false
	}
	previous := queryGapStart(text, at) - 1
	return previous < 0 || text[previous] != '.'
}

// canonicalCallableName shares the qualified identifier reader used by semantic
// validation, preserving component case while removing trivia around separators.
func canonicalCallableName(text string) (string, bool) {
	facts := readExpressionLexicalFacts(text)
	i, ok := facts.at[facts.leading]
	if !ok || facts.tokens[i].kind != lexicalName {
		return "", false
	}
	name, last := facts.qualifiedName(i)
	next := facts.tokens[last].next
	return name, next >= 0 && facts.tokens[next].kind == lexicalEnd
}

// syntaxSpan uses byte offsets into immutable source text.
type syntaxSpan struct{ start, end int }
type syntaxGroup struct {
	open, close int
	// These hold a temporary comma-chain head/tail during the lexical pass,
	// then indexes into the one flat argument pool. No per-group comma slice
	// remains in the immutable facts.
	argumentStart, argumentEnd int
}
type syntaxSeparator struct{ at, next int }
type functionSyntax struct {
	facts *expressionLexicalFacts
	// Delimiter consumers read only the outer expression; nested arguments
	// have their own spans and do not need duplicate punctuation inventories.
	rootPunctuation []int
	text            string
	calls           []functionCallSpan
	groups          []syntaxGroup
	arguments       []syntaxSpan
	comments        []syntaxSpan
	quoted          []syntaxSpan
	leading         int
}

type functionSyntaxKey struct {
	text          string
	argumentsOnly bool
}

var functionSyntaxCache = newBoundedCache[functionSyntaxKey, *functionSyntax](512, func(key functionSyntaxKey) int {
	return boundedCacheStringShardIndex(key.text)
})

func readFunctionSyntax(text string) *functionSyntax {
	return readFunctionSyntaxFacts(text, false)
}

func cachedFunctionSyntax(text string) (*functionSyntax, bool) {
	if len(text) > 16384 {
		return nil, false
	}
	return functionSyntaxCache.get(functionSyntaxKey{text, false})
}

// Lexical tokens own Cypher comment, trivia, quoted-data and name rules.
// The expression fact builder is their sole production consumer.
type lexicalTokenKind uint8

const (
	lexicalEnd lexicalTokenKind = iota
	lexicalComment
	lexicalQuoted
	lexicalName
	lexicalPunctuation
)

// nextLexicalToken skips trivia and reads the token at or after index. Names
// are plain or backticked identifiers; an unclosed backtick is punctuation.
var lexicalTokenWorkObserver atomic.Pointer[func(int)]

func nextLexicalToken(text string, index int) (kind lexicalTokenKind, start, end int, name string) {
	origin := index
	defer func() {
		if observe := lexicalTokenWorkObserver.Load(); observe != nil {
			(*observe)(end - origin + 1)
		}
	}()
	for index < len(text) {
		c := text[index]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\f' || c == '\v':
			index++
			continue
		case c == '/':
			if end := queryCommentEnd(text, index); end >= 0 {
				return lexicalComment, index, end, ""
			}
		case isCypherStringQuoteByte(c):
			return lexicalQuoted, index, skipCypherQuotedText(text, index, c), ""
		case c >= 0x80:
			if size := queryWhitespaceAt(text, index); size > 0 {
				index += size
				continue
			}
		}
		// Only these bytes can start a name (scanIdentifierToken); any other
		// is punctuation without an identifier scan.
		if isCypherBacktickByte(c) || isIdentStartByte(c) {
			if name, next, ok := callableIdentifierToken(text, index); ok {
				return lexicalName, index, next, name
			}
		}
		return lexicalPunctuation, index, index + 1, ""
	}
	return lexicalEnd, len(text), len(text), ""
}

// A function call is a non-procedure identity that is neither a type name
// (x IS :: DATE) nor clause syntax followed by a group (MATCH (n)). A type
// name follows "::" or a word, after ASCII spacing, so any other byte right
// before the name settles that test.
func lexicalFunctionCall(text string, call *functionCallSpan) bool {
	return !call.procedure && !call.annotation && !call.syntaxKeyword
}

// callWalk iterates the already classified tokens in the expression table.
type callWalk struct {
	facts *expressionLexicalFacts
	token int
	text  string
	found bool
	call  functionCallSpan
}

func newCallWalk(text string) callWalk {
	return callWalk{text: text, facts: readExpressionLexicalFacts(text)}
}

// next reads the next token. A comment changes no state and an unsuppressed
// name is classified. The end token repeats.
func (walk *callWalk) next() (lexicalTokenKind, int, int) {
	t := walk.facts.tokens[min(walk.token, len(walk.facts.tokens)-1)]
	if t.kind != lexicalEnd {
		walk.token++
	}
	walk.found = false
	if t.kind == lexicalName && t.call >= 0 {
		walk.call = walk.facts.calls[t.call]
		walk.found = true
	}
	return t.kind, t.start, t.end
}

// Invocation visitors request identities, not argument or punctuation facts.
// Reuse retained facts when present; otherwise walk the tokens.
func lexicalFunctionInvocations(text string, visit func(name, normalized string, open int) error) error {
	if !mayHaveFunctionCall(text) {
		return nil
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, true, false, false)
			if !ok {
				break
			}
			if err := visit(call.name, call.normalized, call.open); err != nil {
				return err
			}
			index = call.open + 1
		}
		return nil
	}
	walk := newCallWalk(text)
	for {
		if kind, _, _ := walk.next(); kind == lexicalEnd {
			return nil
		}
		if walk.found && lexicalFunctionCall(text, &walk.call) {
			if err := visit(walk.call.name, walk.call.normalized, walk.call.open); err != nil {
				return err
			}
		}
	}
}

// lexicalCallSpans visits, in source order, every call identity the retained
// facts list: expression calls, including keywords and type names followed
// by a group, and procedures. visit returns false to stop.
func lexicalCallSpans(text string, visit func(call functionCallSpan) bool) {
	walk := newCallWalk(text)
	for {
		if kind, _, _ := walk.next(); kind == lexicalEnd {
			return
		}
		if walk.found && !visit(walk.call) {
			return
		}
	}
}

// lexicalFunctionCallSpans visits the calls lexicalFunctionInvocations does,
// with their spans. visit returns false to stop.
func lexicalFunctionCallSpans(text string, visit func(call functionCallSpan) bool) {
	if !mayHaveFunctionCall(text) {
		return
	}
	lexicalCallSpans(text, func(call functionCallSpan) bool {
		return !lexicalFunctionCall(text, &call) || visit(call)
	})
}

// hasLexicalAggregate reads closed calls outside scalar bodies in the table.
func hasLexicalAggregate(text string, views ...expressionLexicalView) bool {
	if !mayHaveFunctionCall(text) {
		return false
	}
	v := borrowedExpressionView(text, views)
	scalarEnd := -1
	for _, token := range v.tokens() {
		if token.scalar {
			scalarEnd = max(scalarEnd, v.facts.groups[v.facts.tokens[token.next].group].close)
		}
		if token.kind != lexicalName || token.call < 0 || token.start < scalarEnd {
			continue
		}
		call := v.facts.calls[token.call]
		if call.expressionCall() && lexicalAggregateName(call.name) && call.close(text) >= 0 {
			return true
		}
	}
	return false
}

// lexicalAggregateSpans is findAggregateSpans read from the tokens: each
// outer aggregate call that closes, in order, past the calls in its own
// arguments, and outside the scalar subqueries (EXISTS / COUNT / COLLECT
// { … }) that close. A span found inside an open subquery body is kept
// until that body closes, and dropped then: the retained facts label only
// closed bodies. An aggregate's close is read ahead once, over its own
// arguments, which the walk then passes without reporting.
func lexicalAggregateSpans(text string) []aggregateSpan {
	var spans []aggregateSpan
	if !mayHaveFunctionCall(text) {
		return spans
	}
	facts := readExpressionLexicalFacts(text)
	resume := 0
	for _, call := range facts.calls {
		if call.start < resume || !call.expressionCall() || call.inScalarSubquery || !lexicalAggregateName(call.name) {
			continue
		}
		if close := call.close(text); close >= 0 {
			spans = append(spans, aggregateSpan{start: call.start, end: close + 1})
			resume = close + 1
		}
	}
	return spans
}

// lexicalNamedArguments returns each outer call to name's comment-free
// arguments. Calls nested in a matched call's arguments are not repeated.
func lexicalNamedArguments(text, name string) []string {
	arguments := make([]string, 0)
	if !mayHaveFunctionCall(text) {
		return arguments
	}
	facts := readExpressionLexicalFacts(text)
	resume := 0
	for _, call := range facts.calls {
		if call.start < resume || !call.expressionCall() || !strings.EqualFold(call.name, name) {
			continue
		}
		close := call.close(text)
		if close < 0 {
			break
		}
		arguments = append(arguments, facts.source(syntaxSpan{call.open + 1, close}))
		resume = close + 1
	}
	return arguments
}

// lexicalGroupWalk visits each code token from `from`, an offset where a token
// starts: its kind and span, the nesting depth before it and, for a close
// that ends a group, that group's opening (else -1). A final event has kind
// lexicalEnd. visit returns false to stop. The nesting counts groups opened
// from `from` on, and an unmatched close changes nothing, as in the retained
// groups. Comments are not visited.
func lexicalGroupWalk(text string, from int, visit func(kind lexicalTokenKind, start, end, depth, opened int) bool) {
	facts := readExpressionLexicalFacts(text)
	base := 0
	if i, ok := facts.at[from]; ok {
		base = facts.tokens[i].depth
	}
	low, high := 0, len(facts.tokens)
	for low < high {
		mid := low + (high-low)/2
		if facts.tokens[mid].start < from {
			low = mid + 1
		} else {
			high = mid
		}
	}
	for _, t := range facts.tokens[low:] {
		if t.kind == lexicalComment {
			continue
		}
		opened := t.opened
		if opened < from {
			opened = -1
		}
		if !visit(t.kind, t.start, t.end, max(0, t.depth-base), opened) || t.kind == lexicalEnd {
			return
		}
	}
}

// lexicalGroupClose is the close of the group opening at open, or -1 when
// it is unclosed, as the retained groups record it.
func lexicalGroupClose(text string, open int) int {
	facts := readExpressionLexicalFacts(text)
	if i, ok := facts.at[open]; ok {
		t := facts.tokens[i]
		if t.kind == lexicalPunctuation && (text[open] == '(' || text[open] == '[' || text[open] == '{') {
			return facts.groups[t.group].close
		}
	}
	return -1
}

// lexicalGroupOpening is the opening of the group closed at close, or -1.
// Postfix readers consume the same group tokens as forward argument readers.
func lexicalGroupOpening(text string, close int) int {
	facts := readExpressionLexicalFacts(text)
	if i, ok := facts.at[close]; ok {
		return facts.tokens[i].opened
	}
	return -1
}

// lexicalPreviousToken is an indexed lookup in the shared expression table.
func lexicalPreviousToken(text string, limit int) (kind lexicalTokenKind, start, end int) {
	t, ok := readExpressionLexicalFacts(text).tokenBefore(limit)
	if !ok {
		return lexicalEnd, 0, 0
	}
	return t.kind, t.start, t.end
}

// lexicalEnclosedGroup is the single whole-expression wrapper decision.
// Its slice preserves source text; rendering is the consumer's choice.
func lexicalEnclosedGroup(text string, open, close byte) (string, bool) {
	if len(text) < 2 || text[0] != open || text[len(text)-1] != close {
		return "", false
	}
	v := expressionView(text)
	if _, enclosed := v.enclosed(open, close); !enclosed {
		return "", false
	}
	return strings.TrimSpace(text[1 : len(text)-1]), true
}

// lexicalProjectionGroup reads a trailing root brace and renders its prefix.
// Variable eligibility belongs to the static scope or runtime binding owner.
func lexicalProjectionGroup(text string) (receiver, items string, ok bool) {
	facts := readExpressionLexicalFacts(text)
	last, found := facts.tokenBefore(len(text))
	if !found || last.opened <= 0 || last.depth != 1 || text[last.opened] != '{' || isSubqueryBrace(text, last.opened) {
		return "", "", false
	}
	open, close := last.opened, last.start
	return strings.TrimSpace(facts.source(syntaxSpan{0, open})), strings.TrimSpace(text[open+1 : close]), true
}

type lexicalPostfix struct {
	receiver           syntaxSpan
	key                syntaxSpan
	open, close, depth int
	subscript          bool
}

// lexicalPostfixWalk visits the receiver spans retained by the fact builder.
func lexicalPostfixWalk(text string, visit func(lexicalPostfix) bool) {
	for _, access := range readExpressionLexicalFacts(text).postfix {
		if !visit(access) {
			return
		}
	}
}

func lexicalPostfixAt(text string, open int) (lexicalPostfix, bool) {
	facts := readExpressionLexicalFacts(text)
	if i, ok := facts.postfixAt[open]; ok {
		return facts.postfix[i], true
	}
	return lexicalPostfix{}, false
}

func lexicalTrailingPostfix(text string) (lexicalPostfix, bool) {
	facts := readExpressionLexicalFacts(text)
	if facts.trailing >= 0 {
		return facts.postfix[facts.trailing], true
	}
	return lexicalPostfix{}, false
}

// lexicalFirstOpening is the first code opening byte at or after from, an
// offset where a token starts, or -1, as the retained groups' firstGroup.
func lexicalFirstOpening(text string, opening byte, from int) int {
	open := -1
	lexicalGroupWalk(text, from, func(kind lexicalTokenKind, start, _, _, _ int) bool {
		if kind == lexicalPunctuation && text[start] == opening {
			open = start
			return false
		}
		return true
	})
	return open
}

// lexicalGroupArguments appends the rendered arguments of the group opening
// at open, or of the whole text when open is -1, as appendArguments reads
// that group's retained argument spans. ok is false for an unclosed group.
// Each part's boundaries are code offsets, so rendering the part alone
// gives the text the retained spans render.
func lexicalGroupArguments(parts []string, text string, open int, keepEmpty bool) ([]string, bool) {
	parts, close := lexicalGroupArgumentsClose(parts, text, open, keepEmpty)
	return parts, close >= 0
}

// lexicalGroupArgumentsClose is lexicalGroupArguments with the group's close,
// or -1 when it is unclosed.
func lexicalGroupArgumentsClose(parts []string, text string, open int, keepEmpty bool) ([]string, int) {
	facts := readExpressionLexicalFacts(text)
	group := 0
	if open >= 0 {
		i, ok := facts.at[open]
		if !ok {
			return parts, -1
		}
		group = facts.tokens[i].group
	}
	close := facts.groups[group].close
	if close < 0 {
		return parts, -1
	}
	return facts.appendArguments(parts, group, keepEmpty), close
}

// functionSyntaxFactObserver, when set, is told the text of every request
// for retained facts, cached or built. Tests pin with it which paths may ask
// for them: static checks and statement validation read only the token
// readers, and only execution and the label rewrite keep facts.
var functionSyntaxFactObserver atomic.Pointer[func(text string)]

// Full and argument-only inventories are the retained facts. Argument-only
// consumers retain outer separators and comments.
func readFunctionSyntaxFacts(text string, argumentsOnly bool) *functionSyntax {
	if observe := functionSyntaxFactObserver.Load(); observe != nil {
		(*observe)(text)
	}
	key := functionSyntaxKey{text, argumentsOnly}
	if len(text) <= 16384 {
		if syntax, ok := functionSyntaxCache.get(key); ok {
			return syntax
		}
	}
	syntax := &functionSyntax{text: text, leading: lexicalLeadingTriviaEnd(text)}
	scanFunctionSyntax(syntax, argumentsOnly)
	// Execution views bind their call copies to the requested argument inventory.
	for index := range syntax.calls {
		syntax.calls[index].syntax = syntax
	}
	if len(text) <= 16384 {
		functionSyntaxCache.put(key, syntax)
	}
	return syntax
}

// scanFunctionSyntax builds the retained facts from the shared tokens: calls,
// groups with their argument spans, comments, quoted data, root punctuation
// and scalar-subquery labels.
func scanFunctionSyntax(syntax *functionSyntax, argumentsOnly bool) {
	facts := readExpressionLexicalFacts(syntax.text)
	*syntax = facts.functionSyntax
	// Call values may be rebound by execution consumers; the immutable table
	// must not share their writable slice. Group/argument spans remain immutable.
	syntax.calls = append([]functionCallSpan(nil), facts.calls...)
	if argumentsOnly {
		syntax.calls = nil
		syntax.quoted = nil
		syntax.rootPunctuation = nil
	}
}

// render removes comments recorded by the lexer, never quoted data. A space
// prevents tokens on either side of a removed comment from being concatenated.
func (syntax *functionSyntax) source(span syntaxSpan) string {
	if len(syntax.comments) == 0 {
		return syntax.text[span.start:span.end]
	}
	var out strings.Builder
	start := span.start
	for _, comment := range syntax.comments[syntax.commentIndex(span.start):] {
		if comment.start >= span.end {
			break
		}
		if out.Cap() == 0 {
			out.Grow(span.end - span.start)
		}
		out.WriteString(syntax.text[start:comment.start])
		out.WriteByte(' ')
		start = comment.end
	}
	if out.Cap() == 0 {
		return syntax.text[span.start:span.end]
	}
	out.WriteString(syntax.text[start:span.end])
	return out.String()
}

func (syntax *functionSyntax) argumentSpans(group int) []syntaxSpan {
	shape := syntax.groups[group]
	return syntax.arguments[shape.argumentStart:shape.argumentEnd]
}

// mapEntryValue excludes a symbolic key and its separator from an expression
// consumer. Entry boundaries already belong to the containing brace group;
// comments and quoted keys use the same token/trivia rules as call identities.
// Projection entries without an explicit key retain their complete expression.
func (syntax *functionSyntax) mapEntryValue(entry syntaxSpan) syntaxSpan {
	facts := syntax.facts
	start := queryGapEnd(syntax.text, entry.start)
	if index, ok := facts.at[start]; ok {
		token := facts.tokens[index]
		if token.kind == lexicalName && token.role == expressionMapKey && token.next >= 0 {
			colon := facts.tokens[token.next]
			if colon.start < entry.end {
				return syntaxSpan{colon.end, entry.end}
			}
		}
	}
	return entry
}

// appendArguments preserves strict empty entries when requested. Legacy
// callers historically drop only the last empty entry, not interior ones.
func (syntax *functionSyntax) appendArguments(parts []string, group int, keepEmpty bool) []string {
	spans := syntax.argumentSpans(group)
	if len(spans) == 1 && syntax.render(spans[0]) == "" {
		return parts
	}
	if cap(parts)-len(parts) < len(spans) {
		grown := make([]string, len(parts), len(parts)+len(spans))
		copy(grown, parts)
		parts = grown
	}
	for index, span := range spans {
		value := syntax.render(span)
		if index == len(spans)-1 && value == "" && !keepEmpty {
			continue
		}
		parts = append(parts, value)
	}
	return parts
}

// appendFunctionArguments is the argument-only entry into the same owner.
// Without any comma or comment marker, there can be neither a split nor a
// rendered comment; the one span is the trimmed input, including quoted data.
func appendFunctionArguments(parts []string, text string, keepEmpty bool) []string {
	if !strings.ContainsRune(text, ',') && !strings.Contains(text, "/*") && !strings.Contains(text, "//") {
		if value := strings.TrimSpace(text); value != "" {
			return append(parts, value)
		}
		return parts
	}
	parts, _ = lexicalGroupArguments(parts, text, -1, keepEmpty)
	return parts
}

func (call functionCallSpan) arguments(parts []string) []string {
	return call.syntax.appendArguments(parts, call.group, false)
}

// splitDelimiter is the compatibility policy for generic AST separators. The
// lexer has already excluded quoted/comment text and nested groups.
func (syntax *functionSyntax) splitDelimiter(delimiter rune) []string {
	if delimiter == ',' {
		return syntax.appendArguments(nil, 0, false)
	}
	var parts []string
	start := 0
	for _, at := range syntax.rootPunctuation {
		if rune(syntax.text[at]) == delimiter {
			parts = append(parts, syntax.render(syntaxSpan{start, at}))
			start = at + 1
		}
	}
	if value := syntax.render(syntaxSpan{start, len(syntax.text)}); value != "" {
		parts = append(parts, value)
	}
	return parts
}

func parseFunctionCallArguments(text string) (string, []string, bool) {
	// Execution may already retain the call's complete argument structure.
	// Reuse it, but never build facts for this narrow dispatch request.
	if syntax, ok := cachedFunctionSyntax(text); ok {
		call, ok := syntax.callAt(syntax.leading)
		if !ok {
			return "", nil, false
		}
		close := call.close(text)
		if close < 0 || queryGapEnd(text, close+1) != len(text) {
			return "", nil, false
		}
		return call.name, call.arguments(nil), true
	}
	call, ok := lexicalCallAt(text, lexicalLeadingTriviaEnd(text))
	if !ok {
		return "", nil, false
	}
	close := call.close(text)
	if close < 0 || queryGapEnd(text, close+1) != len(text) {
		return "", nil, false
	}
	return call.name, call.arguments(nil), true
}

// singleFunctionArgument is the one argument of a standalone call, or ""
// when it has another number of argument spans.
func singleFunctionArgument(text string) string {
	call, close, ok := lexicalStandaloneCall(text)
	if !ok {
		return ""
	}
	if len(call.syntax.argumentSpans(call.group)) != 1 {
		return ""
	}
	return call.syntax.render(syntaxSpan{call.open + 1, close})
}

// firstProcedureCall is the first procedure the text calls, read from the
// call spans. Only a CALL names one.
func firstProcedureCall(text string) (functionCallSpan, bool) {
	var procedure functionCallSpan
	found := false
	if indexASCIIFold(text, "call") < 0 {
		return procedure, false
	}
	lexicalCallSpans(text, func(call functionCallSpan) bool {
		procedure, found = call, call.procedure
		return !found
	})
	return procedure, found
}

func (syntax *functionSyntax) render(span syntaxSpan) string {
	return strings.TrimSpace(syntax.source(span))
}

// Parentheses around a call are part of the same lexical structure as its
// arguments. Expression and predicate adapters must not reinterpret quotes or
// delimiters inside comments while deciding whether to unwrap an operand.
func parenthesizedFunctionExpression(text string) (string, bool) {
	inner, ok := lexicalEnclosedGroup(text, '(', ')')
	if !ok {
		return "", false
	}
	return lexicalExpressionText(inner), true
}

// commentEndAt and groupAt expose the same retained facts to graph-expression
// consumers. A generic expression walk must never reinterpret comment text as
// a call, or rediscover a call's identity by scanning backwards from "(".
func (syntax *functionSyntax) commentEndAt(index int) int {
	first := syntax.commentIndex(index)
	if first < len(syntax.comments) && syntax.comments[first].start <= index {
		return syntax.comments[first].end
	}
	return index
}

// commentIndex is the shared lower bound for comment lookup and rendering.
// Readers start at the first potentially overlapping comment, never at the
// beginning of the inventory for each later argument or graph token.
func (syntax *functionSyntax) commentIndex(index int) int {
	low, high := 0, len(syntax.comments)
	for low < high {
		middle := low + (high-low)/2
		if syntax.comments[middle].end <= index {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

func (syntax *functionSyntax) groupAt(open int) (syntaxGroup, bool) {
	low, high := 1, len(syntax.groups)
	for low < high {
		middle := low + (high-low)/2
		if syntax.groups[middle].open < open {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low < len(syntax.groups) && syntax.groups[low].open == open {
		return syntax.groups[low], true
	}
	return syntaxGroup{}, false
}

func firstSyntaxGroup(text string, opening byte, from int) int {
	return readFunctionSyntax(text).firstGroup(opening, from)
}

func (syntax *functionSyntax) firstGroup(opening byte, from int) int {
	for _, group := range syntax.groups {
		if group.open >= from && group.open >= 0 && syntax.text[group.open] == opening {
			return group.open
		}
	}
	return -1
}

func functionReadsVariable(text, name, variable string) bool {
	for _, call := range readFunctionSyntax(text).calls {
		if call.procedure || !strings.EqualFold(call.name, name) {
			continue
		}
		arguments := call.arguments(nil)
		if len(arguments) == 1 && simpleSemanticIdentifier(arguments[0]) == variable {
			return true
		}
	}
	return false
}

func lexicalProcedureNameAt(text string, offset int) (string, bool) {
	start := queryGapEnd(text, offset)
	if name, ok := lexicalCallNameStartingAt(text, start, false); ok {
		return name, true
	}
	// The registry's standalone-name protocol has no CALL keyword. Parse that
	// protocol through the same owner; whole-statement offsets never use it.
	if offset == 0 {
		return lexicalCallNameStartingAt("CALL "+text, len("CALL ")+start, true)
	}
	return "", false
}

// lexicalCallNameStartingAt is the name of the call, or with procedure set
// the procedure, whose name starts at at. Call spans arrive in start order.
func lexicalCallNameStartingAt(text string, at int, procedure bool) (string, bool) {
	name, found := "", false
	lexicalCallSpans(text, func(call functionCallSpan) bool {
		if call.start == at && (call.procedure || !procedure) {
			name, found = call.name, true
		}
		return !found && call.start <= at
	})
	return name, found
}

func containsProcedureCallNamed(text, expected string) bool {
	found := false
	lexicalCallSpans(text, func(call functionCallSpan) bool {
		found = call.procedure && strings.EqualFold(call.name, expected)
		return !found
	})
	return found
}

// callIndex finds the first recorded call at or after a byte offset. Walkers
// retain this structure, including texts too large for the bounded cache.
func (syntax *functionSyntax) callIndex(offset int) int {
	low, high := 0, len(syntax.calls)
	for low < high {
		middle := low + (high-low)/2
		if syntax.calls[middle].start < offset {
			low = middle + 1
		} else {
			high = middle
		}
	}
	return low
}

// callableIdentifierToken preserves quoted identifiers and treats Unicode
// whitespace as trivia rather than part of an unquoted callable component.
func callableIdentifierToken(text string, start int) (string, int, bool) {
	if start < 0 || start >= len(text) || isCypherBacktickByte(text[start]) || !isIdentStartByte(text[start]) {
		return scanIdentifierToken(text, start)
	}
	// One pass reads the unquoted name: identifier bytes up to the first
	// non-identifier byte or Unicode whitespace.
	for index := start; index < len(text); index++ {
		if c := text[index]; c >= 0x80 {
			if queryWhitespaceAt(text, index) > 0 {
				return text[start:index], index, index > start
			}
		} else if !isIdentByte(c) {
			return text[start:index], index, true
		}
	}
	return text[start:], len(text), true
}

// syntaxGroupClose adapts known group offsets to the owner's matching facts.
// Group offsets are ordered by the one forward lexical pass.
func syntaxGroupClose(text string, open int) int {
	syntax := readFunctionSyntax(text)
	low, high := 1, len(syntax.groups)
	for low < high {
		middle := low + (high-low)/2
		if syntax.groups[middle].open < open {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low < len(syntax.groups) && syntax.groups[low].open == open {
		return syntax.groups[low].close
	}
	return -1
}
