package cypher

import "strings"

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
}

// expressionCall separates clause-like parenthesized syntax from expression
// calls. The legacy filter binding form remains a supported native function.
func (call functionCallSpan) expressionCall() bool {
	return !call.procedure && (!isNonFunctionSemanticKeyword(call.name) || strings.EqualFold(call.name, "filter"))
}

// The legacy matcher API permits ASCII spacing between a name and its open.
// Its compatibility policy reads the owned gap, with comments already rendered;
// general call parsing continues to accept the query lexer's Unicode trivia.
func (call functionCallSpan) asciiNameSpacing() bool {
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

// readFunctionIdentity classifies the identifier already read by the forward
// owner. Its supplied token boundary avoids scanning every identifier twice.
func readFunctionIdentity(text, name string, at, next, gap, previous int, groupOpening byte, call *functionCallSpan) bool {
	sliceBoundary := groupOpening == '[' && previous > 0 && text[previous] == '.' && text[previous-1] == '.'
	expressionColon := groupOpening == '{' || groupOpening == '['
	if at < 0 || at >= len(text) || at > 0 && previous == at-1 && isIdentByte(text[at-1]) || previous >= 0 && text[previous] == '.' && !sliceBoundary ||
		at > 0 && text[at-1] == '$' || previous >= 0 && text[previous] == ':' && !expressionColon {
		return false
	}
	parts, end, open := "", next, gap
	if gap < len(text) && text[gap] == '.' {
		parts, end, open = dottedNameParts(text, next, true)
	}
	if open >= len(text) || text[open] != '(' {
		return false
	}
	if len(name)+len(parts) == end-at {
		name = text[at:end]
	} else {
		name += parts
	}
	*call = functionCallSpan{name: name, start: at, nameEnd: end, open: open, next: open + 1}
	return true
}

// nextCall visits the retained inventory for static and runtime consumers.
// It advances into arguments so nested calls are visited, and excludes syntax,
// procedure/graph names, type annotations, comments and string literals.
func (syntax *functionSyntax) nextCall(index int, descendSubqueries, includeProcedures, syntaxNames bool) (functionCallSpan, bool) {
	text := syntax.text
	for _, call := range syntax.calls[syntax.callIndex(index):] {
		if call.start < index || call.procedure && !includeProcedures {
			continue
		}
		if !call.procedure && (precededByTypeAnnotation(text, call.start) || !syntaxNames && isNonFunctionSemanticKeyword(call.normalized)) {
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
	at := lexicalLeadingTriviaEnd(text)
	name, next, ok := callableIdentifierToken(text, at)
	if !ok {
		return functionCallSpan{}, false
	}
	var identity functionCallSpan
	if !readFunctionIdentity(text, name, at, next, queryGapEnd(text, next), -1, 0, &identity) ||
		!strings.EqualFold(identity.name, strings.TrimSpace(expected)) {
		return functionCallSpan{}, false
	}
	return functionCallAt(text, at)
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
	open           int
	expressionCall bool
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
	startAt        int
	groupArguments bool
	inner          string
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
	scanFunctionSyntaxNames(&syntax, false, &request, nil, names)
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

// functionArgumentsAt reads only a known call's argument group. Policy
// compilation needs arguments of temporal calls, not all calls in a statement.
// The same lexer owns its closing delimiter, comments and nested groups.
func functionArgumentsAt(text string, open int) ([]string, bool) {
	if open < 0 || open >= len(text) || text[open] != '(' {
		return nil, false
	}
	syntax := functionSyntax{text: text}
	request := functionSyntaxRequest{startAt: open, activeOpen: open, groupArguments: true}
	scanFunctionSyntax(&syntax, false, &request, nil)
	if !request.found {
		return nil, false
	}
	return appendFunctionArguments(nil, request.inner, false), true
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
	if request.unclosedScalar {
		// Preserve the full reader's behavior on malformed, unclosed bodies:
		// only closed scalar scopes exclude calls. Public syntax validation
		// remains responsible for rejecting the malformed statement.
		return len(findAggregateSpans(text)) > 0
	}
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
	syntax := &functionSyntax{text: text, leading: lexicalLeadingTriviaEnd(text)}
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
	scanFunctionSyntaxNames(syntax, argumentsOnly, request, visit, nil)
}

// Requested names, like the visitor, are borrowed only during this scan.
// Keeping them outside retained request results avoids escaping a caller's
// temporary name list when another request renders text or returns arguments.
func scanFunctionSyntaxNames(syntax *functionSyntax, argumentsOnly bool, request *functionSyntaxRequest, visit func(name, normalized string, open int) error, names []string) {
	text := syntax.text
	rewrite := functionRewriteKind(0)
	if request != nil {
		rewrite = request.rewrite
	} else if !argumentsOnly {
		rewrite = functionRewriteAll
	}
	// Counts are allocation hints only, never token classification. Bound the
	// reservation so parentheses inside a large literal cannot inflate it.
	parentheses, groups := 0, 0
	if request == nil {
		parentheses = strings.Count(text, "(")
		groups = parentheses + strings.Count(text, "[") + strings.Count(text, "{")
	}
	if parentheses > 64 {
		parentheses = 64
	}
	if groups > 64 {
		groups = 64
	}
	if !argumentsOnly && parentheses > 0 {
		syntax.calls = make([]functionCallSpan, 0, parentheses)
	}
	if argumentsOnly && groups > 3 {
		groups = 3
	}
	// Group zero represents the whole input. Retained groups and transient
	// nesting have separate storage: a sparse stack never escapes into facts.
	root := syntaxGroup{open: -1, close: len(text), parent: -1}
	if request == nil {
		syntax.groups = make([]syntaxGroup, 1, groups+1)
		syntax.groups[0] = root
	}
	var sparseBuffer [16]syntaxFrame
	sparseGroups := sparseBuffer[:1]
	sparseGroups[0] = syntaxFrame{open: -1}
	// Temporary nesting and comma chains never escape the lexical pass. Most
	// expressions fit these buffers; larger inputs grow through append.
	var stackBuffer [16]int
	stack := stackBuffer[:1]
	var pending functionCallSpan
	var call functionCallSpan
	hasPending := false
	var separatorBuffer [32]syntaxSeparator
	separators := separatorBuffer[:0]
	subqueryOpen := make(map[int]int)
	var subqueryBuffer [16]syntaxSpan
	subqueries := subqueryBuffer[:0]
	var candidateBuffer [16]int
	candidates := candidateBuffer[:0]
	hiddenOpen, hiddenDepth := -1, 0
	previous := -1
	suppressedNameEnd := 0
	argumentPresent := false
	start := 0
	if request != nil {
		start = request.startAt
	}
	for index := start; index < len(text); {
		if text[index] == '/' {
			if end := queryCommentEnd(text, index); end >= 0 {
				if request == nil || request.renderOnly || request.activeOpen >= 0 && index > request.activeOpen {
					syntax.comments = append(syntax.comments, syntaxSpan{index, end})
				}
				index = end
				continue
			}
		}
		if text[index] <= ' ' || text[index] >= 0x80 {
			if size := queryWhitespaceAt(text, index); size > 0 {
				index += size
				continue
			}
		}
		if request != nil && request.countArguments && !(text[index] == ',' && len(sparseGroups) == 1) {
			argumentPresent = true
		}
		if text[index] == '\'' || text[index] == '"' || request != nil && request.countArguments && text[index] == '`' {
			end := skipCypherQuotedText(text, index, text[index])
			if !argumentsOnly && request == nil {
				syntax.quoted = append(syntax.quoted, syntaxSpan{index, end})
			}
			index = end
			previous = index - 1
			continue
		}
		if name, next, ok := callableIdentifierToken(text, index); ok {
			if argumentsOnly || request != nil && (request.renderOnly || request.countArguments || hiddenDepth > 0 || request.activeOpen >= 0) || index < suppressedNameEnd {
				index, previous = next, next-1
				continue
			}
			if text[index] != '`' && (previous < 0 || text[previous] != '.') {
				if rewrite&functionRewritePattern != 0 && strings.EqualFold(name, "WHERE") {
					frame := sparseGroups[len(sparseGroups)-1]
					if request == nil {
						frame.open = syntax.groups[stack[len(stack)-1]].open
						call, called := syntax.callBeforeParen(frame.open)
						frame.expressionCall = called && call.expressionCall()
					}
					if frame.open >= 0 && text[frame.open] != '{' && !frame.expressionCall && topLevelKeywordIndex(text[frame.open+1:index], "IN") < 0 {
						if request == nil {
							syntax.rewrite |= functionRewritePattern
						} else {
							request.found = true
							return
						}
					}
				}
				if (request == nil || request.aggregate) && (strings.EqualFold(name, "EXISTS") || strings.EqualFold(name, "COUNT") || strings.EqualFold(name, "COLLECT")) {
					_, open, ok := subqueryExpressionKeywordAt(text, index)
					if ok {
						if request == nil {
							subqueryOpen[open] = index
						} else if request.aggregate {
							hiddenOpen = open
						}
					}
				}
				if len(name) == len("CALL") && strings.EqualFold(name, "CALL") || len(name) == len("USE") && strings.EqualFold(name, "USE") {
					start := queryGapEnd(text, next)
					called, end, named := callableIdentifierToken(text, start)
					if !named {
						index = next
						previous = next - 1
						continue
					}
					parts, end, cursor := dottedNameParts(text, end, true)
					if cursor < len(text) && text[cursor] == '.' {
						suppressedNameEnd = end
						index, previous = next, next-1
						continue
					}
					if strings.EqualFold(name, "CALL") && request == nil {
						name := called + parts
						call := functionCallSpan{name: name, normalized: lowerASCII(name), start: start, nameEnd: end, open: -1, next: cursor, procedure: true, group: -1}
						if cursor < len(text) && text[cursor] == '(' {
							call.open = cursor
							call.next = cursor + 1
							pending, hasPending = call, true
						} else {
							syntax.calls = append(syntax.calls, call)
						}
					} else if request != nil && request.proceduresOnly && strings.EqualFold(name, "CALL") {
						name := called + parts
						open := -1
						if cursor < len(text) && text[cursor] == '(' {
							open = cursor
						}
						if request.err = visit(name, lowerASCII(name), open); request.err != nil {
							return
						}
					}
					suppressedNameEnd = end
					index, previous = next, next-1
					continue
				}
			}
			if request != nil && request.proceduresOnly {
				index, previous = next, next-1
				continue
			}
			// A name whose next live byte is neither a dot nor '(' cannot
			// start a call. Sparse requests can also consume this owned gap;
			// retained facts still visit it to keep comment spans.
			gap := queryGapEnd(text, next)
			if gap >= len(text) || text[gap] != '.' && text[gap] != '(' {
				index, previous = next, next-1
				if request != nil {
					index = gap
				}
				continue
			}
			// A colon in a map value or list subscript introduces an expression.
			// Pattern label/type colons do not. The forward group's context and
			// previous live position survive comments and quoted map keys.
			groupOpen := sparseGroups[len(sparseGroups)-1].open
			if request == nil {
				groupOpen = syntax.groups[stack[len(stack)-1]].open
			}
			var groupOpening byte
			if groupOpen >= 0 {
				groupOpening = text[groupOpen]
			}
			if readFunctionIdentity(text, name, index, next, gap, previous, groupOpening, &call) {
				if request == nil {
					call.normalized = lowerASCII(call.name)
				}
				if rewrite&functionRewriteVector != 0 && (strings.EqualFold(call.name, "vector") || strings.EqualFold(call.name, "vector_distance") || strings.EqualFold(call.name, "vector_norm")) {
					if request == nil {
						syntax.rewrite |= functionRewriteVector
					} else {
						request.found = true
						return
					}
				}
				if request == nil {
					pending, hasPending = call, true
				} else {
					if request.rewrite != 0 {
						pending, hasPending = call, true
					}
					if !precededByTypeAnnotation(text, call.start) && !isNonFunctionSemanticKeyword(call.name) {
						if visit != nil {
							if request.err = visit(call.name, lowerASCII(call.name), call.open); request.err != nil {
								return
							}
						} else if request.aggregate && lexicalAggregateName(call.name) {
							candidates = append(candidates, call.open)
						} else if request.nameOnly && callableNameIn(call.name, names) {
							request.found = true
							return
						} else if request.functionName != "" && strings.EqualFold(call.name, request.functionName) {
							if request.nameOnly {
								request.found = true
								return
							}
							request.activeOpen = call.open
						}
					}
				}
				// A qualified name is one token; its components are never separate calls.
				if visit != nil || request != nil && (request.nameOnly || request.rewrite != 0) {
					// Identities already own this complete qualified name and gap.
					// An invocation visitor retains neither comments nor quoted spans.
					index, previous = call.open, call.nameEnd-1
				} else {
					index, previous = next, next-1
				}
				continue
			}
			index = next
			previous = next - 1
			continue
		}
		// Rendering needs comments and quoted-data boundaries, not nesting.
		if request != nil && request.renderOnly {
			previous = index
			index++
			continue
		}
		switch text[index] {
		case '(', '[', '{':
			if request != nil {
				frame := syntaxFrame{open: index}
				if hasPending && pending.open == index {
					frame.expressionCall = pending.expressionCall()
					hasPending = false
				}
				sparseGroups = append(sparseGroups, frame)
				if request.aggregate && index == hiddenOpen {
					hiddenDepth, hiddenOpen = len(sparseGroups), -1
				}
				break
			}
			group := len(syntax.groups)
			syntax.groups = append(syntax.groups, syntaxGroup{open: index, close: -1, parent: stack[len(stack)-1]})
			stack = append(stack, group)
			if hasPending && pending.open == index {
				call := pending
				call.group = group
				syntax.calls = append(syntax.calls, call)
				hasPending = false
			}
		case ')', ']', '}':
			groupOpen := sparseGroups[len(sparseGroups)-1].open
			if request == nil {
				if len(stack) <= 1 {
					break
				}
				groupOpen = syntax.groups[stack[len(stack)-1]].open
			} else if len(sparseGroups) <= 1 {
				break
			}
			opening := text[groupOpen]
			if !(opening == '(' && text[index] == ')' || opening == '[' && text[index] == ']' || opening == '{' && text[index] == '}') {
				break
			}
			if rewrite&functionRewriteLabel != 0 && opening == '(' {
				at := queryGapEnd(text, index+1)
				if at < len(text) && text[at] == ':' && (at+1 >= len(text) || text[at+1] != ':') {
					if request == nil {
						syntax.rewrite |= functionRewriteLabel
					} else {
						request.found = true
						return
					}
				}
			}
			if request != nil {
				sparseGroups = sparseGroups[:len(sparseGroups)-1]
				if len(candidates) > 0 && candidates[len(candidates)-1] == groupOpen {
					request.found = true
					return
				}
				if request.activeOpen == groupOpen {
					if request.groupArguments {
						request.inner = syntax.source(syntaxSpan{groupOpen + 1, index})
						request.found = true
						return
					}
					request.arguments = append(request.arguments, syntax.source(syntaxSpan{groupOpen + 1, index}))
					request.activeOpen = -1
					syntax.comments = syntax.comments[:0]
				}
				if hiddenDepth == len(sparseGroups)+1 {
					hiddenDepth = 0
				}
				break
			}
			groupIndex := stack[len(stack)-1]
			syntax.groups[groupIndex].close = index
			stack = stack[:len(stack)-1]
			if argumentsOnly {
				syntax.groups = syntax.groups[:groupIndex]
				break
			}
			if start, ok := subqueryOpen[groupOpen]; ok {
				subqueries = append(subqueries, syntaxSpan{start, index + 1})
			}
		case ',':
			if request != nil && request.countArguments && len(sparseGroups) == 1 {
				request.argumentCount++
				argumentPresent = false
			}
			group := stack[len(stack)-1]
			if request == nil && (!argumentsOnly || group == 0) {
				shape := &syntax.groups[group]
				separator := len(separators)
				separators = append(separators, syntaxSeparator{index, -1})
				if shape.argumentStart == 0 {
					shape.argumentStart = separator + 1
				} else {
					separators[shape.argumentEnd].next = separator
				}
				shape.argumentEnd = separator
			}
			if !argumentsOnly && request == nil && group == 0 {
				syntax.rootPunctuation = append(syntax.rootPunctuation, index)
			}
		default:
			group := stack[len(stack)-1]
			if !argumentsOnly && request == nil && group == 0 {
				syntax.rootPunctuation = append(syntax.rootPunctuation, index)
			}
		}
		previous = index
		index++
	}
	if request != nil {
		if request.countArguments && argumentPresent {
			request.argumentCount++
		}
		request.unclosedScalar = hiddenDepth > 0
		return
	}
	// Closed scopes arrive in closing order. Collapse nested scopes to
	// disjoint outer ranges, then label the ordered calls in one sweep.
	// This is the existing lexical boundary for aggregate visitors, not an
	// import or validation model; those remain with semanticScopeWalk.
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
	start := queryGapEnd(syntax.text, entry.start)
	_, next, key := callableIdentifierToken(syntax.text, start)
	if key {
		colon := queryGapEnd(syntax.text, next)
		if colon < entry.end && syntax.text[colon] == ':' && (colon+1 >= entry.end || syntax.text[colon+1] != ':') {
			return syntaxSpan{colon + 1, entry.end}
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
		if !normalizing && (dot != end || partStart != dot+1 || expression[partStart] == '`') {
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
