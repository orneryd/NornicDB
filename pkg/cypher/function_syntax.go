package cypher

import "strings"

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
	if strings.IndexByte(text, '/') < 0 {
		return strings.TrimSpace(text)
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		return syntax.render(syntaxSpan{0, len(text)})
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{renderOnly: true, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	return syntax.render(syntaxSpan{0, len(text)})
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
	call, ok := standaloneFunctionCall(expr)
	if !ok {
		return "", "", false
	}
	return call.name, call.syntax.source(syntaxSpan{call.open + 1, call.close(expr)}), true
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
	if call.procedure || call.start != syntax.leading || call.close(text) < 0 || call.close(text)+1 != syntax.trailing {
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
	typed            bool
	asciiSpacing     bool
}

// expressionCall separates clause-like parenthesized syntax from expression
// calls. The legacy filter binding form remains a supported native function.
func (call functionCallSpan) expressionCall() bool {
	return !call.procedure && (!isNonFunctionSemanticKeyword(call.name) || strings.EqualFold(call.name, "filter"))
}

// The legacy matcher API permits ASCII spacing between a name and its open.
// The forward cursor records this policy while consuming that gap;
// general call parsing continues to accept the query lexer's Unicode trivia.
func (call functionCallSpan) asciiNameSpacing() bool {
	return call.asciiSpacing
}

func (call functionCallSpan) close(text string) int {
	if call.syntax == nil || call.group < 0 {
		return -1
	}
	return call.syntax.groups[call.group].close
}

// readFunctionIdentity classifies the identifier already read by the forward
// owner. Its supplied token boundary avoids scanning every identifier twice.
type functionCallIdentity struct {
	name                 string
	start, nameEnd, open int
}

func (identity functionCallIdentity) span() functionCallSpan {
	return functionCallSpan{name: identity.name, start: identity.start, nameEnd: identity.nameEnd, open: identity.open, next: identity.open + 1}
}

func readFunctionIdentity(text, name string, at, next, gap, previous int, groupOpening byte, call *functionCallIdentity) bool {
	sliceBoundary := groupOpening == '[' && previous > 0 && text[previous] == '.' && text[previous-1] == '.'
	expressionColon := groupOpening == '{' || groupOpening == '['
	if at < 0 || at >= len(text) || at > 0 && previous == at-1 && isIdentByte(text[at-1]) || previous >= 0 && text[previous] == '.' && !sliceBoundary ||
		at > 0 && text[at-1] == '$' || previous >= 0 && text[previous] == ':' && !expressionColon {
		return false
	}
	end, open := next, gap
	if open >= len(text) || text[open] != '(' {
		return false
	}
	*call = functionCallIdentity{name: name, start: at, nameEnd: end, open: open}
	return true
}

// nextCall visits the retained inventory for static and runtime consumers.
// It advances into arguments so nested calls are visited, and excludes syntax,
// procedure/graph names, type annotations, comments and string literals.
func (syntax *functionSyntax) nextCall(index int, descendSubqueries, includeProcedures, syntaxNames bool) (functionCallSpan, bool) {
	for _, call := range syntax.calls[syntax.callIndex(index):] {
		if call.start < index || call.procedure && !includeProcedures {
			continue
		}
		if !call.procedure && (call.typed || !syntaxNames && isNonFunctionSemanticKeyword(call.normalized)) {
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

// namedFunctionCallAt rejects a different leading identity before requesting
// argument facts. Identity and trivia still use the shared forward readers;
// a matching name goes through the complete delimiter and spacing policies.
func namedFunctionCallAt(text, expected string) (functionCallSpan, bool) {
	if syntax, ok := cachedFunctionSyntax(text); ok {
		call, found := syntax.callAt(syntax.leading)
		return call, found && strings.EqualFold(call.name, strings.TrimSpace(expected))
	}
	syntax := functionSyntax{text: text, leading: -1, callsOnly: true}
	request := functionSyntaxRequest{retainCalls: true, headName: strings.TrimSpace(expected), activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	if !request.found || len(syntax.calls) == 0 {
		return functionCallSpan{}, false
	}
	owned := new(functionSyntax)
	*owned = syntax
	for index := range owned.calls {
		owned.calls[index].syntax = owned
	}
	return owned.calls[0], true
}

// A text walk keeps one lazy view, including for texts above the cache bound.
// Call-free expressions need no inventory, and large inventories stay local
// to the walk rather than being rebuilt at each candidate offset.
type functionSyntaxView struct {
	text   string
	syntax *functionSyntax
	loaded bool
}

func (view *functionSyntaxView) facts() *functionSyntax {
	if !view.loaded {
		view.loaded = true
		if strings.ContainsRune(view.text, '(') {
			view.syntax = readFunctionSyntax(view.text)
		}
	}
	return view.syntax
}

func (view *functionSyntaxView) callAt(at int) (functionCallSpan, bool) {
	if syntax := view.facts(); syntax != nil {
		call, ok := syntax.callAt(at)
		// An operator such as NOT (n) must leave its grouping to the
		// expression/pattern owner, rather than skip it as function arguments.
		return call, ok && call.expressionCall()
	}
	return functionCallSpan{}, false
}

// A source walk retains its own facts even when the input is too large for
// the shared cache. Looking up each opening must not rebuild that inventory.
func (view *functionSyntaxView) callBeforeParen(open int) (functionCallSpan, bool) {
	if syntax := view.facts(); syntax != nil {
		return syntax.callBeforeParen(open)
	}
	return functionCallSpan{}, false
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
	text = strings.TrimSpace(text)
	// Ordinary unquoted names need no normalization. Validate their lexical
	// components in one pass before using the trivia/quoted-token reader.
	if len(text) > 0 && isIdentStartByte(text[0]) {
		compact := true
		for index := 1; index < len(text); index++ {
			if text[index] >= 0x80 && queryWhitespaceAt(text, index) > 0 {
				compact = false
				break
			}
			if isIdentByte(text[index]) {
				continue
			}
			if text[index] == '.' && index+1 < len(text) && isIdentStartByte(text[index+1]) {
				index++
				continue
			}
			compact = false
			break
		}
		if compact {
			return text, true
		}
	}
	name, end, ok := callableIdentifierToken(text, 0)
	if !ok {
		return "", false
	}
	if end == len(text) {
		return name, true
	}
	parts, end, gapEnd := dottedNameParts(text, end, true)
	if end != len(text) && gapEnd != len(text) {
		return "", false
	}
	if len(name)+len(parts) == len(text) {
		return text, true
	}
	return name + parts, true
}

// syntaxSpan uses byte offsets into immutable source text.
type syntaxSpan struct{ start, end int }
type syntaxGroup struct {
	open, close int
	parent      int
	// These hold a temporary comma-chain head/tail during the lexical pass,
	// then indexes into the one flat argument pool. No per-group comma slice
	// remains in the immutable facts.
	argumentStart, argumentEnd int
}
type syntaxSeparator struct{ at, next int }
type syntaxFrame struct {
	open                                     int
	expressionCall                           bool
	group                                    int
	hasIn                                    bool
	scalar                                   bool
	scalarStart                              int
	aggregateCall                            bool
	hasClosedAggregate                       bool
	referenceFunction                        string
	referenceStart                           int
	commas                                   int
	present, isIdentifier, firstIsIdentifier bool
	identifier, firstIdentifier              string
	entryStart                               int
}

type functionRewriteKind uint8

const (
	functionRewriteVector functionRewriteKind = 1 << iota
	functionRewritePattern
	functionRewriteLabel
)

const functionRewriteAll = functionRewriteVector | functionRewritePattern | functionRewriteLabel

type functionSyntax struct {
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
	trailing        int
	argumentsOnly   bool
	callsOnly       bool
	mapValues       []syntaxMapValue
	rewrite         functionRewriteKind
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

// A request changes retained detail, never lexical rules. Sparse consumers use
// the same scan and keep only transient nesting plus the result they requested.
type functionSyntaxRequest struct {
	functionName   string
	nameOnly       bool
	countArguments bool
	argumentCount  int
	proceduresOnly bool
	rewrite        functionRewriteKind
	aggregate      bool
	renderOnly     bool
	err            error
	activeOpen     int
	arguments      []string
	found          bool
	unclosedScalar bool
	retainCalls    bool
	headName       string
	references     bool
}

// Boolean consumers request a result from the same forward scan. They need no
// retained calls, argument spans or punctuation just to rule out a rewrite.
func lexicalHasFunction(text, name string) bool {
	return lexicalHasAnyFunction(text, []string{name})
}

func callableNameIn(name string, names []string) bool {
	for _, expected := range names {
		if strings.EqualFold(name, expected) {
			return true
		}
	}
	return false
}

// A set of boolean name checks needs one lexical walk, with the same rules
// for nested calls, quoted names and incomplete groups as a single-name check.
func lexicalHasAnyFunction(text string, names []string) bool {
	if !mayHaveFunctionCall(text) {
		return false
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, true, false, false)
			if !ok {
				break
			}
			if callableNameIn(call.name, names) {
				return true
			}
			index = call.open + 1
		}
		return false
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{nameOnly: true, activeOpen: -1}
	scanFunctionSyntaxNames(&syntax, false, &request, nil, names, nil)
	return request.found
}

// functionArgumentCount answers the splitter's count question without creating
// strings or a result slice. In particular a comma inside quoted data is not
// an argument separator, and an empty trailing argument is omitted.
func functionArgumentCount(text string) int {
	if !strings.ContainsRune(text, ',') && !strings.Contains(text, "/*") && !strings.Contains(text, "//") {
		if strings.TrimSpace(text) == "" {
			return 0
		}
		return 1
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{countArguments: true, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	return request.argumentCount
}

// functionArgumentsAt adapts one-off callers to retained call arguments.
// The same lexer owns its closing delimiter, comments and nested groups.
func functionArgumentsAt(text string, open int) ([]string, bool) {
	if open < 0 || open >= len(text) || text[open] != '(' {
		return nil, false
	}
	syntax := readFunctionCallFacts(text)
	for index, group := range syntax.groups {
		if group.open == open && group.close >= 0 {
			return syntax.appendArguments(nil, index, false), true
		}
	}
	return nil, false
}

// A query compiler keeps this one result for both function and procedure
// identities and their argument spans. Only requested call data is retained.
func readFunctionCallFacts(text string) *functionSyntax {
	if syntax, ok := cachedFunctionSyntax(text); ok {
		return syntax
	}
	syntax := &functionSyntax{text: text, leading: -1, callsOnly: true}
	request := functionSyntaxRequest{retainCalls: true, activeOpen: -1}
	scanFunctionSyntax(syntax, false, &request, nil)
	for index := range syntax.calls {
		syntax.calls[index].syntax = syntax
	}
	return syntax
}

func lexicalMayRewrite(text string, kind functionRewriteKind) bool {
	// Absence of required bytes is conclusive; presence still goes through
	// the lexer, including when those bytes occur only in quotes or comments.
	if !containsFold(text, "vector") {
		kind &^= functionRewriteVector
	}
	if !containsFold(text, "WHERE") {
		kind &^= functionRewritePattern
	}
	if !strings.ContainsRune(text, ':') {
		kind &^= functionRewriteLabel
	}
	if kind == 0 {
		return false
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		return syntax.rewrite&kind != 0
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{rewrite: kind, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	return request.found
}

func cachedFunctionSyntax(text string) (*functionSyntax, bool) {
	if len(text) > 16384 {
		return nil, false
	}
	return functionSyntaxCache.get(functionSyntaxKey{text, false})
}

// Invocation visitors request identities, not argument or punctuation facts.
// Reuse retained facts when present; otherwise emit from the same lexical pass
// without retaining a complete inventory just for a name-only walk.
func lexicalFunctionInvocations(text string, visit func(name, normalized string, open int) error) error {
	return lexicalInvocations(text, false, visit)
}

func lexicalProcedureInvocations(text string, visit func(name, normalized string, open int) error) error {
	return lexicalInvocations(text, true, visit)
}

func lexicalInvocations(text string, proceduresOnly bool, visit func(name, normalized string, open int) error) error {
	if proceduresOnly && !containsFold(text, "CALL") || !proceduresOnly && !mayHaveFunctionCall(text) {
		return nil
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		if proceduresOnly {
			for _, call := range syntax.calls {
				if call.procedure {
					if err := visit(call.name, call.normalized, call.open); err != nil {
						return err
					}
				}
			}
			return nil
		}
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
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{proceduresOnly: proceduresOnly, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, visit)
	return request.err
}

// hasLexicalAggregate answers the boolean question without materializing an
// inventory. Existing complete facts are reused; otherwise the shared scan
// excludes scalar bodies and stops when any qualifying call closes.
func hasLexicalAggregate(text string) bool {
	if !strings.ContainsRune(text, '(') {
		return false
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, false, false, false)
			if !ok {
				break
			}
			if lexicalAggregateName(call.name) && call.close(text) >= 0 {
				return true
			}
			index = call.open + 1
		}
		return false
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{aggregate: true, activeOpen: -1}
	scanFunctionSyntax(&syntax, false, &request, nil)
	return request.found
}

func lexicalNamedArguments(text, name string) []string {
	arguments := make([]string, 0)
	if !strings.ContainsRune(text, '(') {
		return arguments
	}
	if syntax, ok := cachedFunctionSyntax(text); ok {
		for index := 0; index < len(text); {
			call, ok := syntax.nextCall(index, true, false, false)
			if !ok {
				break
			}
			index = call.open + 1
			if !strings.EqualFold(call.name, name) {
				continue
			}
			close := call.close(text)
			if close < 0 {
				break
			}
			arguments = append(arguments, syntax.source(syntaxSpan{call.open + 1, close}))
			index = close + 1
		}
		return arguments
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{functionName: name, activeOpen: -1, arguments: arguments}
	scanFunctionSyntax(&syntax, false, &request, nil)
	return request.arguments
}

// Full and argument-only inventories use the same lexical pass as sparse
// requests. Argument-only consumers retain outer separators and comments.
func readFunctionSyntaxFacts(text string, argumentsOnly bool) *functionSyntax {
	key := functionSyntaxKey{text, argumentsOnly}
	if len(text) <= 16384 {
		if syntax, ok := functionSyntaxCache.get(key); ok {
			return syntax
		}
	}
	syntax := &functionSyntax{text: text, leading: -1}
	scanFunctionSyntax(syntax, argumentsOnly, nil, nil)
	// Only retained inventories bind calls back to their owner. Keeping this
	// self-reference out of the scanner lets sparse callers stay on the stack.
	for index := range syntax.calls {
		syntax.calls[index].syntax = syntax
	}
	if len(text) <= 16384 {
		functionSyntaxCache.put(key, syntax)
	}
	return syntax
}

// scanFunctionSyntax is the only lexical pass for full, argument-only,
// comment-rendering, named-argument and boolean requests.
// The visitor is synchronous and separate from retained request results, so
// storing argument slices cannot force an invocation adapter onto the heap.
func scanFunctionSyntax(syntax *functionSyntax, argumentsOnly bool, request *functionSyntaxRequest, visit func(name, normalized string, open int) error) {
	scanFunctionSyntaxNames(syntax, argumentsOnly, request, visit, nil, nil)
}

// A cursor reads each source token once. Lookahead borrows the recorded token;
// it never asks a consumer to scan a gap, a qualified name or an argument again.
const (
	syntaxIdentifier = 256 + iota
	syntaxString
	syntaxValue
	syntaxEOF
)

type syntaxToken struct {
	kind, start, end, previous int
	name                       string
	quoted, typed              bool
	spaceGap, simpleGap        bool
	asciiGap                   bool
}

type syntaxMapValue struct{ entryStart, valueStart int }

type syntaxCursor struct {
	text              string
	offset            int
	ready             bool
	look              syntaxToken
	lastEnd, lastKind int
	typeContinuation  bool
}

// Projections borrow the destination and current group separately from the
// token cursor. Tokens only borrow immutable source text; returning one cannot
// make the request, reference state or transient nesting escape to the heap.
type syntaxProjection struct {
	syntax    *functionSyntax
	request   *functionSyntaxRequest
	depth     int
	refs      *syntaxReferenceState
	arg       *syntaxFrame
	mapValues bool
}

func (cursor *syntaxCursor) peek(projection *syntaxProjection, referenceVisit func(syntaxReference)) *syntaxToken {
	if cursor.ready {
		return &cursor.look
	}
	text := cursor.text
	for {
		spaceGap, simpleGap, asciiGap := true, true, true
		for cursor.offset < len(text) {
			at := cursor.offset
			c := text[at]
			if c == '/' {
				if end := queryCommentEnd(text, at); end >= 0 {
					if projection.request == nil || projection.request.renderOnly || projection.request.activeOpen >= 0 && at > projection.request.activeOpen || projection.request.retainCalls && (projection.depth > 0 || cursor.lastKind == syntaxIdentifier) {
						projection.syntax.comments = append(projection.syntax.comments, syntaxSpan{at, end})
					}
					cursor.offset = end
					spaceGap, simpleGap = false, false
					continue
				}
			}
			if c <= ' ' || c >= 0x80 {
				if size := queryWhitespaceAt(text, at); size > 0 {
					cursor.offset += size
					ascii := size == 1 && isASCIISpace(c)
					spaceGap, simpleGap, asciiGap = spaceGap && ascii, simpleGap && ascii, asciiGap && ascii
					continue
				}
			}
			break
		}
		at := cursor.offset
		kind, end := syntaxEOF, at
		if at < len(text) {
			if projection.syntax.leading < 0 {
				projection.syntax.leading = at
			}
			c := text[at]
			kind, end = int(c), at+1
			if c >= '0' && c <= '9' {
				kind = syntaxValue
				for end < len(text) && isIdentByte(text[end]) {
					end++
				}
			}
			// Sparse identity/reference requests do not observe scalar operators and
			// values outside a selected argument. Keep their boundary context without
			// constructing a token or dispatching it through every consumer. A name or
			// dot still needs its immediate next token, and rewrite/count requests need
			// all their punctuation. Every byte advances through this same cursor.
			request := projection.request
			if request != nil && !request.retainCalls && !request.countArguments && request.rewrite == 0 && request.activeOpen < 0 && projection.arg == nil && cursor.lastKind != syntaxIdentifier && cursor.lastKind != '.' && c < 0x80 && !isIdentStartByte(c) && !isCypherStringQuoteByte(c) && !isCypherBacktickByte(c) {
				switch c {
				case '.', ':', '(', ')', '[', ']', '{', '}', ',':
				default:
					if state := projection.refs; state != nil {
						if state.stage > 0 {
							token := syntaxToken{kind: kind, start: at, end: end, spaceGap: spaceGap, simpleGap: simpleGap}
							state.record(&token, text, referenceVisit)
						} else {
							state.lastStart, state.lastEnd, state.lastKind, state.lastQuoted = at, end, kind, false
						}
					}
					cursor.offset, cursor.lastEnd, cursor.lastKind = end, end, kind
					cursor.typeContinuation = false
					projection.syntax.trailing = end
					continue
				}
			}
		}
		token := &cursor.look
		token.kind, token.start, token.end = kind, at, end
		token.previous, token.name = cursor.lastEnd-1, ""
		token.quoted, token.typed = false, false
		token.spaceGap, token.simpleGap, token.asciiGap = spaceGap, simpleGap, asciiGap
		if at < len(text) {
			c := text[at]
			switch {
			case isCypherStringQuoteByte(c):
				token.kind, token.end = syntaxString, skipCypherQuotedText(text, at, c)
				if projection.request == nil && !projection.syntax.argumentsOnly {
					projection.syntax.quoted = append(projection.syntax.quoted, syntaxSpan{at, token.end})
				}
			case isCypherBacktickByte(c) && projection.request != nil && (projection.request.countArguments || projection.request.renderOnly || projection.request.activeOpen >= 0):
				token.kind, token.end, token.quoted = syntaxValue, skipCypherQuotedText(text, at, c), true
			case isCypherBacktickByte(c) || isIdentStartByte(c):
				name, end, ok := callableIdentifierToken(text, at)
				if ok {
					token.kind, token.name, token.end, token.quoted = syntaxIdentifier, name, end, isCypherBacktickByte(c)
				}
			}
			token.typed = token.spaceGap && cursor.typeContinuation
			cursor.offset = token.end
			projection.syntax.trailing = token.end
		}
		cursor.ready = true
		return token
	}
}
func (cursor *syntaxCursor) take(projection *syntaxProjection, referenceVisit func(syntaxReference)) *syntaxToken {
	token := cursor.peek(projection, referenceVisit)
	cursor.ready, cursor.lastEnd, cursor.lastKind = false, token.end, token.kind
	cursor.typeContinuation = token.kind == ':' && token.start > 0 && cursor.text[token.start-1] == ':' ||
		token.kind == syntaxIdentifier && !token.quoted && (strings.EqualFold(token.name, "TYPED") ||
			token.typed && (strings.EqualFold(token.name, "LOCAL") || strings.EqualFold(token.name, "ZONED")))
	if projection.refs != nil {
		projection.refs.record(token, cursor.text, referenceVisit)
	}
	if projection.arg != nil && token.kind != syntaxEOF {
		if projection.mapValues && token.kind == ':' && projection.arg.isIdentifier && projection.arg.open >= 0 && cursor.text[projection.arg.open] == '{' && (token.end >= len(cursor.text) || cursor.text[token.end] != ':') {
			projection.syntax.mapValues = append(projection.syntax.mapValues, syntaxMapValue{projection.arg.entryStart, token.end})
		}
		projection.arg.recordArgument(token)
	}
	if projection.request == nil && !projection.syntax.argumentsOnly && !projection.syntax.callsOnly && projection.depth == 0 && token.kind != syntaxIdentifier && token.kind != syntaxString && token.kind != syntaxEOF {
		switch token.kind {
		case '(', '[', '{', ')', ']', '}':
		default:
			for at := token.start; at < token.end; at++ {
				projection.syntax.rootPunctuation = append(projection.syntax.rootPunctuation, at)
			}
		}
	}
	return token
}

// Qualified names are assembled from the same cursor's saved positions. Its
// lookahead token remains available to the group/argument owner.
func (cursor *syntaxCursor) name(projection *syntaxProjection, referenceVisit func(syntaxReference), head syntaxToken) (string, int, bool) {
	name, end := head.name, head.end
	for cursor.peek(projection, referenceVisit).kind == '.' {
		dot := *cursor.take(projection, referenceVisit)
		part := *cursor.peek(projection, referenceVisit)
		if part.kind != syntaxIdentifier {
			return name, end, false
		}
		cursor.take(projection, referenceVisit)
		if !head.quoted && !part.quoted && end == dot.start && dot.end == part.start && len(name) == end-head.start {
			name = cursor.text[head.start:part.end]
		} else {
			name += "." + part.name
		}
		end = part.end
	}
	return name, end, true
}

// A frame records argument positions/classification while the lexical owner
// consumes its tokens. Empty interior entries count; only a final empty one
// is omitted, matching appendArguments without tokenizing its text again.
func (frame *syntaxFrame) recordArgument(token *syntaxToken) {
	switch token.kind {
	case ')', ']', '}':
		return
	case ',':
		if frame.commas == 0 {
			frame.firstIdentifier, frame.firstIsIdentifier = frame.identifier, frame.isIdentifier
		}
		frame.commas++
		frame.entryStart = token.end
		frame.present, frame.isIdentifier = false, false
		frame.identifier = ""
	default:
		if frame.present {
			frame.isIdentifier = false
		} else {
			frame.identifier, frame.isIdentifier = token.name, token.kind == syntaxIdentifier
		}
		frame.present = true
	}
}

func (frame *syntaxFrame) argumentCount() int {
	count := frame.commas
	if frame.present {
		count++
	}
	return count
}

func (frame *syntaxFrame) singleIdentifier() string {
	if frame.argumentCount() != 1 {
		return ""
	}
	if frame.commas == 0 && frame.isIdentifier {
		return frame.identifier
	}
	if frame.commas == 1 && frame.firstIsIdentifier {
		return frame.firstIdentifier
	}
	return ""
}

// Reference facts are lexical positions and identities, not a deleted-entity
// policy. The caller chooses which function inputs matter to that policy.
type syntaxReference struct {
	variable, function string
	start              int
	nullTest           bool
}

type syntaxReferenceState struct {
	lastStart, lastEnd, lastKind int
	lastQuoted                   bool
	pending                      syntaxReference
	stage                        int
}

// A property reference stays pending only until its following null-test tokens
// are known. Emitted facts borrow source identities, never a mutable result slice.
func (state *syntaxReferenceState) record(token *syntaxToken, text string, visit func(syntaxReference)) {
	emit := false
	switch state.stage {
	case 1:
		if token.kind == '.' && state.lastEnd == token.start {
			state.stage = 0 // '..' belongs to a list slice, not a property read.
		} else if state.lastEnd == token.start && (token.kind == syntaxIdentifier && !token.quoted || token.kind == syntaxValue) {
			state.stage = 2
		} else {
			emit = true
		}
	case 2:
		if token.spaceGap && !token.quoted && strings.EqualFold(token.name, "IS") {
			state.stage = 3
		} else {
			emit = true
		}
	case 3:
		if token.simpleGap && !token.quoted && strings.EqualFold(token.name, "NOT") {
			state.stage = 4
		} else {
			state.pending.nullTest = token.simpleGap && !token.quoted && strings.EqualFold(token.name, "NULL")
			emit = true
		}
	case 4:
		state.pending.nullTest = token.simpleGap && !token.quoted && strings.EqualFold(token.name, "NULL")
		emit = true
	}
	if emit {
		visit(state.pending)
		state.stage = 0
	}
	if token.kind == '.' && (state.lastKind == syntaxIdentifier && !state.lastQuoted || state.lastKind == syntaxValue) {
		start, end := state.lastStart, state.lastEnd
		if start == 0 || !isIdentByte(text[start-1]) && text[start-1] != '.' {
			state.pending = syntaxReference{variable: text[start:end], start: start}
			state.stage = 1
		}
	}
	state.lastStart, state.lastEnd, state.lastKind, state.lastQuoted = token.start, token.end, token.kind, token.quoted
}

func readFunctionReferenceFacts(text string, into []syntaxReference, names []string) []syntaxReference {
	syntax := functionSyntax{text: text, leading: -1}
	request := functionSyntaxRequest{references: true, activeOpen: -1}
	scanFunctionSyntaxNames(&syntax, false, &request, nil, names, func(fact syntaxReference) {
		into = append(into, fact)
	})
	return into
}

// Requested names and visitors are borrowed. All request modes consume this
// one token cursor; full facts and sparse facts differ only in what is kept.
func scanFunctionSyntaxNames(syntax *functionSyntax, argumentsOnly bool, request *functionSyntaxRequest, visit func(name, normalized string, open int) error, names []string, referenceVisit func(syntaxReference)) {
	text := syntax.text
	syntax.argumentsOnly, syntax.leading = argumentsOnly, -1
	retained := request == nil || request.retainCalls
	rewrite := functionRewriteKind(0)
	if request == nil && !argumentsOnly {
		rewrite = functionRewriteAll
	} else if request != nil {
		rewrite = request.rewrite
	}
	var frameBuffer [16]syntaxFrame
	frames := frameBuffer[:1]
	frames[0].open = -1
	var separatorBuffer [32]syntaxSeparator
	separators := separatorBuffer[:0]
	var subqueryBuffer [16]syntaxSpan
	subqueries := subqueryBuffer[:0]
	pendingScalarOpen, pendingScalarStart := -1, -1
	var pending functionCallSpan
	hasPending := false
	var referenceState syntaxReferenceState
	cursor := syntaxCursor{text: text}
	projection := syntaxProjection{syntax: syntax, request: request, mapValues: request == nil && !argumentsOnly}
	if request != nil && request.references {
		projection.refs = &referenceState
	}
	if retained && (request == nil || request.headName == "") {
		syntax.groups = append(syntax.groups, syntaxGroup{open: -1, close: len(text), parent: -1})
	}
	for {
		projection.depth = len(frames) - 1
		projection.arg = nil
		if request != nil && (request.references && frames[len(frames)-1].referenceFunction != "" || request.countArguments && len(frames) == 1) || projection.mapValues && frames[len(frames)-1].open >= 0 && text[frames[len(frames)-1].open] == '{' {
			projection.arg = &frames[len(frames)-1]
		}
		token := cursor.take(&projection, referenceVisit)
		if token.kind == syntaxEOF {
			break
		}
		if request != nil && request.headName != "" && len(syntax.groups) == 0 && token.kind != syntaxIdentifier {
			return
		}
		if request != nil && request.renderOnly {
			continue
		}
		if token.kind == syntaxIdentifier {
			token := *token
			if argumentsOnly || request != nil && (request.countArguments || request.activeOpen >= 0) {
				continue
			}
			frame := &frames[len(frames)-1]
			if !token.quoted && (token.previous < 0 || text[token.previous] != '.') {
				if strings.EqualFold(token.name, "IN") {
					frame.hasIn = true
				}
				if rewrite&functionRewritePattern != 0 && strings.EqualFold(token.name, "WHERE") && frame.open >= 0 && text[frame.open] != '{' && !frame.expressionCall && !frame.hasIn {
					if request == nil {
						syntax.rewrite |= functionRewritePattern
					} else {
						request.found = true
						return
					}
				}
				if (retained || request.aggregate) && (strings.EqualFold(token.name, "COUNT") || strings.EqualFold(token.name, "COLLECT") || strings.EqualFold(token.name, "EXISTS")) && cursor.peek(&projection, referenceVisit).kind == '{' {
					pendingScalarOpen, pendingScalarStart = cursor.peek(&projection, referenceVisit).start, token.start
				}
				if strings.EqualFold(token.name, "CALL") || strings.EqualFold(token.name, "USE") {
					called := *cursor.peek(&projection, referenceVisit)
					if called.kind != syntaxIdentifier {
						continue
					}
					cursor.take(&projection, referenceVisit)
					name, end, valid := cursor.name(&projection, referenceVisit, called)
					next := *cursor.peek(&projection, referenceVisit)
					if !valid {
						continue
					}
					if strings.EqualFold(token.name, "CALL") {
						open := -1
						if next.kind == '(' {
							open = next.start
						}
						if retained {
							call := functionCallSpan{name: name, normalized: lowerASCII(name), start: called.start, nameEnd: end, open: open, next: next.start, procedure: true, group: -1}
							if open >= 0 {
								call.next = open + 1
								pending, hasPending = call, true
							} else {
								syntax.calls = append(syntax.calls, call)
							}
						} else if request.proceduresOnly {
							if request.err = visit(name, lowerASCII(name), open); request.err != nil {
								return
							}
						}
					}
					continue
				}
			}
			if request != nil && request.proceduresOnly {
				continue
			}
			name, end, valid := cursor.name(&projection, referenceVisit, token)
			next := *cursor.peek(&projection, referenceVisit)
			if !valid || next.kind != '(' {
				if request != nil && request.headName != "" && len(syntax.groups) == 0 {
					return
				}
				continue
			}
			var opening byte
			if frame.open >= 0 {
				opening = text[frame.open]
			}
			var identity functionCallIdentity
			if !readFunctionIdentity(text, name, token.start, end, next.start, token.previous, opening, &identity) {
				continue
			}
			if request != nil && request.headName != "" && len(syntax.groups) == 0 {
				if token.start != syntax.leading || !strings.EqualFold(name, request.headName) {
					return
				}
				syntax.groups = append(syntax.groups, syntaxGroup{open: -1, close: len(text), parent: -1})
				request.found = true
			}
			if rewrite&functionRewriteVector != 0 && (strings.EqualFold(name, "vector") || strings.EqualFold(name, "vector_distance") || strings.EqualFold(name, "vector_norm")) {
				if request == nil {
					syntax.rewrite |= functionRewriteVector
				} else {
					request.found = true
					return
				}
			}
			if retained {
				pending, hasPending = identity.span(), true
				pending.normalized = lowerASCII(name)
			} else if request.rewrite != 0 || request.aggregate || request.references {
				pending, hasPending = identity.span(), true
			}
			if !token.typed && !isNonFunctionSemanticKeyword(name) {
				if visit != nil {
					if request.err = visit(name, lowerASCII(name), next.start); request.err != nil {
						return
					}
				} else if request != nil && request.nameOnly && callableNameIn(name, names) {
					request.found = true
					return
				} else if request != nil && request.functionName != "" && strings.EqualFold(name, request.functionName) {
					request.activeOpen = next.start
				}
			}
			if hasPending {
				pending.typed, pending.asciiSpacing = token.typed, next.asciiGap
			}
			continue
		}
		switch token.kind {
		case '(', '[', '{':
			frame := syntaxFrame{open: token.start, group: -1, entryStart: token.end}
			if retained {
				frame.group = len(syntax.groups)
				syntax.groups = append(syntax.groups, syntaxGroup{open: token.start, close: -1, parent: frames[len(frames)-1].group})
			}
			if hasPending && pending.open == token.start {
				frame.expressionCall = rewrite&functionRewritePattern != 0 && pending.expressionCall()
				frame.aggregateCall = request != nil && request.aggregate && !pending.typed && !isNonFunctionSemanticKeyword(pending.name) && lexicalAggregateName(pending.name)
				if request != nil && request.references && !pending.typed && callableNameIn(pending.name, names) {
					frame.referenceFunction, frame.referenceStart = lowerASCII(pending.name), pending.start
				}
				if retained {
					pending.group = frame.group
					syntax.calls = append(syntax.calls, pending)
				}
				hasPending = false
			}
			if token.start == pendingScalarOpen {
				frame.scalar, frame.scalarStart = true, pendingScalarStart
				pendingScalarOpen, pendingScalarStart = -1, -1
			}
			frames = append(frames, frame)
		case ')', ']', '}':
			token := *token
			if len(frames) <= 1 {
				continue
			}
			frame := frames[len(frames)-1]
			opening := text[frame.open]
			if !(opening == '(' && token.kind == ')' || opening == '[' && token.kind == ']' || opening == '{' && token.kind == '}') {
				continue
			}
			if rewrite&functionRewriteLabel != 0 && opening == '(' && cursor.peek(&projection, referenceVisit).kind == ':' {
				colon := *cursor.peek(&projection, referenceVisit)
				if colon.end >= len(text) || text[colon.end] != ':' {
					if request == nil {
						syntax.rewrite |= functionRewriteLabel
					} else {
						request.found = true
						return
					}
				}
			}
			if request != nil && request.references && frame.referenceFunction != "" {
				if variable := frame.singleIdentifier(); variable != "" {
					referenceVisit(syntaxReference{variable: variable, function: frame.referenceFunction, start: frame.referenceStart})
				}
			}
			if retained {
				syntax.groups[frame.group].close = token.start
				if argumentsOnly {
					syntax.groups = syntax.groups[:frame.group]
				}
				if frame.scalar {
					subqueries = append(subqueries, syntaxSpan{frame.scalarStart, token.end})
				}
			}
			frames = frames[:len(frames)-1]
			if request != nil && request.activeOpen == frame.open {
				request.arguments = append(request.arguments, syntax.source(syntaxSpan{frame.open + 1, token.start}))
				request.activeOpen = -1
				syntax.comments = syntax.comments[:0]
			}
			if !frame.scalar && (frame.aggregateCall || frame.hasClosedAggregate) {
				frames[len(frames)-1].hasClosedAggregate = true
				if request != nil && request.aggregate {
					hidden := false
					for _, outer := range frames {
						hidden = hidden || outer.scalar
					}
					if !hidden {
						request.found = true
						return
					}
				}
			}
		case ',':
			group := frames[len(frames)-1].group
			if retained && (!argumentsOnly || group == 0) {
				shape := &syntax.groups[group]
				separator := len(separators)
				separators = append(separators, syntaxSeparator{token.start, -1})
				if shape.argumentStart == 0 {
					shape.argumentStart = separator + 1
				} else {
					separators[shape.argumentEnd].next = separator
				}
				shape.argumentEnd = separator
			}
		}
	}
	if syntax.leading < 0 {
		syntax.leading = len(text)
	}
	if request != nil && !retained {
		if request.countArguments {
			request.argumentCount = frames[0].argumentCount()
		}
		for _, frame := range frames {
			request.unclosedScalar = request.unclosedScalar || frame.scalar
			if request.aggregate && frame.hasClosedAggregate {
				request.found = true
			}
		}
		return
	}
	if len(subqueries) > 0 {
		outer := subqueries[:0]
		for _, span := range subqueries {
			for len(outer) > 0 && outer[len(outer)-1].start >= span.start {
				outer = outer[:len(outer)-1]
			}
			outer = append(outer, span)
		}
		scope := 0
		for index := range syntax.calls {
			call := &syntax.calls[index]
			for scope < len(outer) && outer[scope].end <= call.start {
				scope++
			}
			call.inScalarSubquery = scope < len(outer) && outer[scope].start <= call.start
		}
	}
	totalArguments := 0
	for index := range syntax.groups {
		if shape := &syntax.groups[index]; shape.close >= 0 {
			totalArguments++
			for separator := shape.argumentStart - 1; separator >= 0; separator = separators[separator].next {
				totalArguments++
			}
		}
	}
	syntax.arguments = make([]syntaxSpan, 0, totalArguments)
	for index := range syntax.groups {
		shape := &syntax.groups[index]
		if shape.close < 0 {
			shape.argumentStart, shape.argumentEnd = 0, 0
			continue
		}
		start := shape.open + 1
		firstSeparator := shape.argumentStart - 1
		shape.argumentStart = len(syntax.arguments)
		for separator := firstSeparator; separator >= 0; separator = separators[separator].next {
			comma := separators[separator].at
			syntax.arguments = append(syntax.arguments, syntaxSpan{start, comma})
			start = comma + 1
		}
		syntax.arguments = append(syntax.arguments, syntaxSpan{start, shape.close})
		shape.argumentEnd = len(syntax.arguments)
	}
	return
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
	low, high := 0, len(syntax.mapValues)
	for low < high {
		middle := low + (high-low)/2
		if syntax.mapValues[middle].entryStart < entry.start {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low < len(syntax.mapValues) && syntax.mapValues[low].entryStart == entry.start {
		return syntaxSpan{syntax.mapValues[low].valueStart, entry.end}
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
	return readFunctionSyntaxFacts(text, true).appendArguments(parts, 0, keepEmpty)
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
	call, ok := standaloneFunctionCall(text)
	if !ok {
		return "", nil, false
	}
	return call.name, call.arguments(nil), true
}

func singleFunctionArgument(text string) string {
	call, ok := standaloneFunctionCall(text)
	if !ok {
		return ""
	}
	spans := call.syntax.argumentSpans(call.group)
	if len(spans) != 1 {
		return ""
	}
	return call.syntax.render(spans[0])
}

// firstProcedureCall adapts statement consumers to the same call inventory.
func firstProcedureCall(text string) (functionCallSpan, bool) {
	for _, call := range readFunctionSyntax(text).calls {
		if call.procedure {
			return call, true
		}
	}
	return functionCallSpan{}, false
}

func (syntax *functionSyntax) render(span syntaxSpan) string {
	return strings.TrimSpace(syntax.source(span))
}

// Parentheses around a call are part of the same lexical structure as its
// arguments. Expression and predicate adapters must not reinterpret quotes or
// delimiters inside comments while deciding whether to unwrap an operand.
func parenthesizedFunctionExpression(text string) (string, bool) {
	if len(text) < 2 || text[0] != '(' || text[len(text)-1] != ')' {
		return "", false
	}
	syntax := readFunctionSyntax(text)
	group, ok := syntax.groupAt(0)
	if !ok || group.close != len(text)-1 {
		return "", false
	}
	return syntax.render(syntaxSpan{1, group.close}), true
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
	for _, call := range readFunctionSyntax(text).calls {
		if call.start == start {
			return call.name, true
		}
	}
	// The registry's standalone-name protocol has no CALL keyword. Parse that
	// protocol through the same owner; whole-statement offsets never use it.
	if offset == 0 {
		for _, call := range readFunctionSyntax("CALL " + text).calls {
			if call.procedure && call.start == len("CALL ")+start {
				return call.name, true
			}
		}
	}
	return "", false
}

func containsProcedureCallNamed(text, expected string) bool {
	for _, call := range readFunctionSyntax(text).calls {
		if call.procedure && strings.EqualFold(call.name, expected) {
			return true
		}
	}
	return false
}

// dottedNameParts returns the dotted parts (.b.c) following a name that ends
// at end, where they end, and the following trivia's end. Each name is read as the code that runs it
// reads it: whitespace may stand around a dot only when spaced is set, for a
// procedure, function or graph name. Contiguous unquoted components reuse
// the source slice; only names containing trivia or quotes are normalized.
func dottedNameParts(expression string, end int, spaced bool) (string, int, int) {
	skip := func(index int) int {
		if spaced && index < len(expression) && (isASCIIWhitespace(expression[index]) || expression[index] == '/' || expression[index] >= 0x80) {
			return queryGapEnd(expression, index)
		}
		return index
	}
	origin := end
	var normalized strings.Builder
	normalizing := false
	for {
		dot := skip(end)
		if dot >= len(expression) || expression[dot] != '.' {
			if normalizing {
				return normalized.String(), end, dot
			}
			return expression[origin:end], end, dot
		}
		partStart := skip(dot + 1)
		part, partEnd, ok := callableIdentifierToken(expression, partStart)
		if !ok {
			if normalizing {
				return normalized.String(), end, dot
			}
			return expression[origin:end], end, dot
		}
		if !normalizing && (dot != end || partStart != dot+1 || isCypherBacktickByte(expression[partStart])) {
			normalized.WriteString(expression[origin:end])
			normalizing = true
		}
		if normalizing {
			normalized.WriteByte('.')
			normalized.WriteString(part)
		}
		end = partEnd
	}
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
	return readIdentifierToken(text, start, true)
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
