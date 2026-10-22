package cypher

import (
	"context"
	"maps"
	"reflect"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// validateListOperands rejects, before any route executes the statement, a
// list position whose operand has a static type that is not a list, as
// Neo4j's compile-time type check does ("Type mismatch: expected List<T> but
// was Integer"). A list position is the operand after IN: the IN operator,
// all / any / none / single, list comprehensions, reduce and FOREACH. The
// operands checked are a whole literal (number, string, boolean, map) and a
// whole parameter bound to a non-null value that is not a list ("Type
// mismatch for parameter 'p': …"); `$p.list`, `$p[0]`, `'a' + x` and
// variables are expressions whose type is only known per row, where a value
// that isn't a list is a list of that one value (coerceToUnwindItems). Variables
// bound to a node, relationship or path are checked with their scope
// (graphListOperandTypeError).
func validateListOperands(cypher string, params map[string]interface{}) error {
	return forEachListOperand(cypher, func(start, in int) error {
		// FOREACH (x IN 5 | …) runs once with x = 5 in Neo4j, not a type error.
		if foreachDeclaration(cypher, in) || forIterationDeclaration(cypher, in) {
			return nil
		}
		end, typeName, parameter := staticListOperand(cypher, start, params)
		if typeName == "" || !wholeListOperand(cypher, end) {
			return nil
		}
		message := localization.CypherCoreListOperandTypeMismatch(typeName)
		if parameter != "" {
			message = localization.CypherCoreListParameterTypeMismatch(parameter, typeName)
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", message)
	})
}

// forEachListOperand calls visit with the offset of each list-position
// operand in text (the operand after an IN keyword outside quotes) and the
// offset of that IN, stopping at the first error.
func forEachListOperand(text string, visit func(start, in int) error) error {
	for i := 0; i < len(text); i++ {
		if end, quoted := skipCypherLexicalToken(text, i); quoted {
			i = end - 1
			continue
		}
		if (text[i] != 'I' && text[i] != 'i') || i+1 >= len(text) || (text[i+1] != 'N' && text[i+1] != 'n') || (i > 0 && (isIdentByte(text[i-1]) || text[i-1] == ':' || text[i-1] == '.')) ||
			i+2 >= len(text) || isIdentByte(text[i+2]) {
			continue
		}
		if err := visit(queryGapEnd(text, i+2), i); err != nil {
			return err
		}
	}
	return nil
}

// wholeListOperand reports whether the operand ending at end is the whole
// list-position operand, not the start of a longer expression (n.list,
// n[0], x + y).
func wholeListOperand(text string, end int) bool {
	next := queryGapEnd(text, end)
	return next >= len(text) || strings.IndexByte(")],}|", text[next]) >= 0 || isIdentByte(text[next])
}

// graphListOperandTypeError is Neo4j's compile-time SyntaxError for a list
// position given a variable bound to a node, relationship or path ("Type
// mismatch: expected List<T> but was Node"), whatever the data: `1 IN n`,
// `[x IN p | …]`, `any(x IN r WHERE …)`, in projections, WHERE, SET values
// and subquery bodies. FOREACH (x IN n | …) is not a type error in Neo4j (it
// runs once). A list element declaration (`[n IN list | …]`) is not itself
// an operand and cannot change the type of a separate `IN n` operand.
func graphListOperandTypeError(text string, scope matchSemanticScope) error {
	return staticListOperandTypeError(text, staticTypeScope{kinds: scope})
}

func staticListOperandTypeError(text string, scope staticTypeScope) error {
	if len(scope.kinds) == 0 && len(scope.values) == 0 {
		return nil
	}
	return forEachListOperand(text, func(start, in int) error {
		end := start
		for end < len(text) && isIdentByte(text[end]) {
			end++
		}
		if end == start || !wholeListOperand(text, end) {
			return nil
		}
		name := text[start:end]
		typeName := scope.typeOf(name)
		if typeName == "" || mayBeListType(typeName) {
			return nil
		}
		if foreachDeclaration(text, in) || forIterationDeclaration(text, in) || localVariableShadowsAt(text, start, name) {
			return nil
		}
		return localizedStatusError("Neo.ClientError.Statement.SyntaxError", "InvalidArgumentType", localization.CypherCoreListOperandTypeMismatch(typeName))
	})
}

// mayBeListType reports whether a static type is a list or names one among
// its choices (a property value's type may be a List<T>).
func mayBeListType(typeName string) bool {
	// The owner's admission: a null alternative fits too.
	return typeName != "" && !typedOperand(typeName).proved().excludes(isListTypeName)
}

// foreachDeclaration reports whether the IN at offset in is FOREACH's own
// (`FOREACH (x IN …`).
func foreachDeclaration(text string, in int) bool {
	if !containsFold(text[:in], "FOREACH") {
		return false
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOREACH", from, opts)
		if start < 0 || start >= in {
			return false
		}
		open := queryGapEnd(text, start+len("FOREACH"))
		if open < len(text) && text[open] == '(' {
			_, end, ok := scanIdentifierToken(text, queryGapEnd(text, open+1))
			if ok && queryGapEnd(text, end) == in {
				return true
			}
		}
		from = start + len("FOREACH")
	}
	return false
}

// forIterationDeclaration reports whether the IN at offset in is a FOR
// iteration clause's own (`FOR x IN …`). FOR iterates like UNWIND, coercing a
// non-list operand to a single item, so its operand is not a list type error.
func forIterationDeclaration(text string, in int) bool {
	if !containsFold(text[:in], "FOR") {
		return false
	}
	opts := defaultKeywordScanOpts()
	opts.SkipParens = false
	opts.SkipBrackets = false
	for from := 0; from < in; {
		start := keywordIndexFrom(text, "FOR", from, opts)
		if start < 0 || start >= in {
			return false
		}
		nameStart := queryGapEnd(text, start+len("FOR"))
		if nameStart < in {
			if _, nameEnd, ok := scanIdentifierToken(text, nameStart); ok && queryGapEnd(text, nameEnd) == in {
				return true
			}
		}
		from = start + len("FOR")
	}
	return false
}

// localBinding is an expression that binds variables of its own, closing at
// text[close]: a list comprehension [v IN list WHERE … | …], a named pattern
// comprehension [p = (a)-->(b) WHERE … | …], all / any / none / single /
// filter (v IN list WHERE …) and reduce / allReduce (acc = initial,
// v IN list | step[, predicate]). It declares count names, each at its
// declared offset, visible from scope (the WHERE or | after the list, the
// pattern after a path's =) to close.
type localBinding struct {
	close, scope, count int
	names               [2]string
	declared            [2]int
	// source is the text span that types each name: the list an element
	// iterates (element), reduce's initial value for its accumulator; a
	// named path (path) is a Path.
	source  [2][2]int
	element [2]bool
	path    [2]bool
}

func (binding *localBinding) declare(name string, at, sourceStart, sourceEnd int, element, path bool) {
	index := binding.count
	binding.names[index], binding.declared[index] = name, at
	binding.source[index], binding.element[index], binding.path[index] = [2]int{sourceStart, sourceEnd}, element, path
	binding.count++
}

// localBindingAt reads the local binding that opens at text[open] ('[' or
// the '(' of a binding function), if one does. It is the one reader of
// these declarations: the names an expression binds (expressionLocalBindings)
// and where each is in scope (localVariableShadowsAt) both come from it, so
// x IN[…], x /* … */ IN … and p = (…) read alike everywhere.
func localBindingAt(text string, open int) (localBinding, bool) {
	var binding localBinding
	list := text[open] == '[' // else the '(' of a call: callers pass only these
	function := ""
	if !list {
		if function = bindingFunctionBeforeParen(text, open); function == "" {
			return binding, false
		}
	}
	closing := func() int {
		closeBy := ')'
		if list {
			closeBy = ']'
		}
		if close := findMatchingDelimiter(text, open, rune(text[open]), closeBy); close >= 0 {
			return close
		}
		return len(text)
	}
	if isReduceFormFunction(function) {
		binding.close = closing()
		if binding.close == len(text) {
			return binding, false
		}
		form, ok := parseReduceForm(function, text[open+1:binding.close])
		if !ok {
			return binding, false
		}
		comma := open + 1 + findTopLevelOperator(text[open+1:binding.close], ",", false, true)
		binding.scope = localScopeStart(text, comma+1, binding.close, true, false)
		accumulator := queryGapEnd(text, open+1)
		equals := open + 1 + strings.IndexByte(text[open+1:comma], '=')
		binding.declare(form.accumulator, accumulator, equals+1, comma, false, false)
		_, element, list, _ := bindingDeclaration(text, comma+1)
		binding.declare(form.variable, element, list, binding.scope, true, false)
		return binding, true
	}
	if name, declaration, listStart, declared := bindingDeclaration(text, open+1); declared {
		binding.close = closing()
		binding.scope = localScopeStart(text, listStart, binding.close, list, true)
		binding.declare(name, declaration, listStart, binding.scope, true, false)
		return binding, true
	}
	if !list {
		return binding, false
	}
	// [p = (a)-->(b) …]: a named pattern comprehension.
	if name, declaration, pattern, named := namedPathDeclaration(text, open+1); named {
		binding.close = closing()
		binding.declare(name, declaration, 0, 0, false, true)
		binding.scope = pattern
		return binding, true
	}
	return binding, false
}

// namedPathDeclaration reads a path variable declared at text[from] before
// its pattern: p = (a)-->(b), `p p` = (a)-->(b). It returns the name,
// where it is written and where the pattern starts. It is the one reader of
// a named pattern comprehension's head, for localBindingAt,
// patternComprehensionHead and the quoted-variable classifier.
func namedPathDeclaration(text string, from int) (name string, at, pattern int, ok bool) {
	at = queryGapEnd(text, from)
	name, end, ok := scanIdentifierToken(text, at)
	if !ok {
		return "", 0, 0, false
	}
	next := queryGapEnd(text, end)
	if next+1 >= len(text) || text[next] != '=' || text[next+1] == '~' {
		return "", 0, 0, false
	}
	pattern = queryGapEnd(text, next+1)
	if pattern >= len(text) || text[pattern] != '(' {
		return "", 0, 0, false
	}
	return name, at, pattern, true
}

// bindingDeclaration reads the declaration that opens a binding's arguments
// at text[from]: a name, then IN (x IN l, x IN[1], x /* … */ IN l). It
// returns the name, where it is written and where the list after IN starts.
// It is the one reader of that declaration, for the scope reader
// (localBindingAt) and the evaluators (parseListComprehension,
// parseQuantifierArguments).
func bindingDeclaration(text string, from int) (name string, at, list int, ok bool) {
	at = queryGapEnd(text, from)
	name, end, ok := scanIdentifierToken(text, at)
	if !ok {
		return "", 0, 0, false
	}
	next := queryGapEnd(text, end)
	if next+2 > len(text) || !strings.EqualFold(text[next:next+2], "IN") || next+2 < len(text) && isIdentByte(text[next+2]) {
		return "", 0, 0, false
	}
	return name, at, next + 2, true
}

// localScopeStart is where a binding's names come into scope in
// text[from:close]: its first top-level | (pipe) or WHERE (where), or close
// when it has neither ([v IN l] binds v nowhere).
func localScopeStart(text string, from, close int, pipe, where bool) int {
	depth := 0
	for index := from; index < close; index++ {
		switch c := text[index]; cypherLexicalByte(c) {
		case cypherQuoteClass:
			index = skipCypherQuotedText(text, index, c) - 1
			continue
		case '/':
			if end := queryCommentEnd(text, index); end >= 0 {
				index = end - 1
				continue
			}
		case '(', '[', '{':
			depth++
		case ')', ']', '}':
			depth--
		case '|':
			if pipe && depth == 0 {
				return index
			}
		}
		if where && depth == 0 && index+5 <= close && strings.EqualFold(text[index:index+5], "WHERE") &&
			!isIdentByte(text[index-1]) && (index+5 == len(text) || !isIdentByte(text[index+5])) {
			return index
		}
	}
	return close
}

// localVariableShadowsAt reports whether name, read at offset operand of
// text, is a variable an enclosing expression binds itself rather than the
// clause's (localBindingAt): a list comprehension's element in its WHERE and
// projection ([n IN l WHERE 1 IN n | n[0]]), a named pattern comprehension's
// path ([p = (a)-->(b) | length(p)]), the element of all / any / none /
// single in its predicate (all(n IN l WHERE n[0])), and reduce's
// accumulator and element in its step (reduce(acc = [], n IN l | acc + n)).
// The list a binding iterates and reduce's initial value are read outside
// it: in 1 IN n, [n IN [1] | n] the first n is the clause's. A binding's own
// declaration (the n of n IN, the acc of acc =) is the local too. It is the
// one reader of these scopes for the code that reads raw text: the static
// checks (list operands, subscripts, dynamic labels, function arguments,
// property accesses, subquery patterns; withoutLocalVariables), the
// reference scanners and the writing of a row's values into an expression
// (localOccurrences).
func localVariableShadowsAt(text string, operand int, name string) bool {
	_, _, ok := localBindingOf(text, operand, name)
	return ok
}

// localBindingOf is the innermost local binding that binds name where it is
// read at offset operand of text (localVariableShadowsAt), and the index of
// name among its declarations. A '[' or '(' whose first declared name isn't
// name (and isn't a reduce, which declares two) is passed over without
// being read whole.
func localBindingOf(text string, operand int, name string) (localBinding, int, bool) {
	var found localBinding
	foundIndex, ok := 0, false
	for open := 0; open < operand; {
		if !localBindingStepReads(text[open]) {
			open++
			continue
		}
		next, binding, bound := localBindingStep(text, open, name)
		open = next
		for index := 0; bound && index < binding.count; index++ {
			if binding.names[index] == name && (binding.declared[index] == operand || binding.scope <= operand && operand < binding.close) {
				// A binding opening later inside this one is inner.
				found, foundIndex, ok = binding, index, true
			}
		}
	}
	return found, foundIndex, ok
}

// localBindingStepReads reports whether localBindingStep reads more of text
// than the byte c at its offset; the walks step over any other byte
// themselves.
func localBindingStepReads(c byte) bool {
	return isCypherQuoteByte(c) || c == '/' || c == '[' || c == '('
}

// localBindingStep is one step of the walk that finds the bindings of name
// (localBindingOf, localOccurrences.keeps): the binding that opens at
// text[open] when it may declare name, and the offset the walk goes on
// from (past a quoted name, string or comment). A '[' or '(' whose first
// declared name isn't name (and isn't a reduce, which declares two) is
// passed over without being read whole. It is called only at a byte
// localBindingStepReads reads.
func localBindingStep(text string, open int, name string) (int, localBinding, bool) {
	switch cypherLexicalByte(text[open]) {
	case cypherQuoteClass:
		return skipCypherQuotedText(text, open, text[open]), localBinding{}, false
	case '/':
		if end := queryCommentEnd(text, open); end >= 0 {
			return end, localBinding{}, false
		}
		return open + 1, localBinding{}, false
	}
	// text[open] is '[' or '(': the walks step over the bytes
	// localBindingStepReads doesn't read themselves.
	if declared, _, named := scanIdentifierToken(text, queryGapEnd(text, open+1)); !named || declared != name {
		if text[open] != '(' || !isReduceFormFunction(bindingFunctionBeforeParen(text, open)) {
			return open + 1, localBinding{}, false
		}
	}
	binding, bound := localBindingAt(text, open)
	return open + 1, binding, bound
}

// localBindingOperand is the static type of the local name read at offset
// at of text (localBindingOf): a named path is a Path; an element is its
// list's element (listElementOperand: an Integer of [1], a node of [y]
// for a node y); reduce's accumulator is typed from its initial value as the
// deriver types it (reduceAccumulatorOperand). The
// source is typed in the scope at its own position, with the locals
// around it typed first (withLocalTypes): in
// all(x IN [y] WHERE all(r IN [x] WHERE …)) [x] holds the outer local's
// node. It is unknown when that type isn't known.
func localBindingOperand(text string, at int, name string, scope staticTypeScope) staticOperand {
	binding, index, ok := localBindingOf(text, at, name)
	if !ok {
		return staticOperand{}
	}
	if binding.path[index] {
		return provenOperand("Path")
	}
	span := binding.source[index]
	if span[0] <= 0 || span[1] <= span[0] || span[1] > len(text) {
		return staticOperand{}
	}
	source := strings.TrimSpace(text[span[0]:span[1]])
	inner := withLocalTypes(scope, text, span[0])
	types := typeScopeOf(inner)
	if binding.element[index] {
		return listElementOperand(types, inner, source)
	}
	// The one other local is reduce's accumulator.
	return reduceAccumulatorOperand(projectedOperand(types, inner, source))
}

// localBindingKind is localBindingOperand's binding kind.
func localBindingKind(text string, at int, name string, scope staticTypeScope) matchBindingKind {
	return localBindingOperand(text, at, name, scope).bindingKind()
}

// withLocalTypes is scope with each name a local binds around offset at of
// text in place of the outer variable of that name, typed as that local
// (localBindingOperand): in all(r IN [1] WHERE EXISTS { (r)-->() }) the r
// a subquery's pattern reads is an Integer, not the clause's relationship.
// The scope goes through typeScope, the one type of every variable.
func withLocalTypes(scope staticTypeScope, text string, at int) staticTypeScope {
	var types typeScope
	for name := range expressionLocalBindings(text) {
		if !localVariableShadowsAt(text, at, name) {
			continue
		}
		if types == nil {
			types = typeScopeOf(scope)
		}
		types[name] = localBindingOperand(text, at, name, scope)
	}
	if types == nil {
		return scope
	}
	local := types.static(scope.cypher25)
	local.complete, local.params = scope.complete, scope.params
	return local
}

// bindingFunctionBeforeParen is the name of the function whose arguments
// open at text[open] when it binds variables of its own (all / any / none /
// single / filter, reduce / allReduce), as written; "" for any other.
func bindingFunctionBeforeParen(text string, open int) string {
	end := open
	for end > 0 && isWhitespace(text[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	if start == end || start > 0 && isIdentByte(text[start-1]) {
		return ""
	}
	switch strings.ToLower(text[start:end]) {
	case "any", "all", "none", "single", "filter", "reduce", "allreduce":
		return text[start:end]
	default:
		return ""
	}
}

// localBindingNames caches the names a text binds itself
// (expressionLocalBindings), by text, for localOccurrences.
var localBindingNames = newBoundedCache[string, map[string]struct{}](1024, boundedCacheStringShardIndex)

// localOccurrences reads, for writing a row's values into text, which
// occurrences of a name read a variable an enclosing expression binds itself
// (localVariableShadowsAt) and keep their text. It is a value the caller
// keeps on its stack: the names text binds are read once per text
// (localBindingNames), when first asked, and the occurrences of a name in
// one input are read in one walk (keeps), so a row predicate costs one cache
// lookup and no allocation. (Not every binding form has an IN: a named
// pattern comprehension [p = (a)-->(b) | p] declares with =.)
type localOccurrences struct {
	// text is read, never changed: the names it binds are cached by text
	// (localBindingNames), shared and never written to.
	text   string
	read   bool
	locals map[string]struct{}

	// The walk of input for name (keeps): it has read the bindings that
	// open before walked, and holds those of name a later occurrence can
	// still read (scopes, then more once scopes is full).
	input, name string
	bound       bool
	walked      int
	scopes      [8]localOccurrenceScope
	open        int
	more        []localOccurrenceScope
}

// localOccurrenceScope is where one binding's local is read: at its
// declaration, and from scope up to close (localBindingOf).
type localOccurrenceScope struct{ declared, scope, close int }

// keeps reports whether the occurrence of name at offset at of input reads a
// local variable of that name (localVariableShadowsAt), so its text is
// kept. The replacers ask it for each occurrence of input in order: the
// bindings opening before at are read once for them all (localBindingStep),
// and an occurrence before the last one asked, another input or another
// name starts the walk again. A nil localOccurrences keeps nothing.
func (occurrences *localOccurrences) keeps(input, name string, at int) bool {
	if occurrences == nil {
		return false
	}
	if input != occurrences.input || name != occurrences.name || at < occurrences.walked {
		occurrences.input, occurrences.name, occurrences.walked = input, name, 0
		occurrences.open, occurrences.more = 0, occurrences.more[:0]
		occurrences.bound = occurrences.binds(name)
	}
	if !occurrences.bound {
		return false
	}
	for occurrences.walked < at {
		if !localBindingStepReads(input[occurrences.walked]) {
			occurrences.walked++
			continue
		}
		next, binding, bound := localBindingStep(input, occurrences.walked, name)
		occurrences.walked = next
		for index := 0; bound && index < binding.count; index++ {
			if binding.names[index] == name {
				occurrences.hold(localOccurrenceScope{declared: binding.declared[index], scope: binding.scope, close: binding.close})
			}
		}
	}
	// A binding that closes by at closes before every later occurrence
	// too: it is let go.
	kept, held := false, 0
	for _, scope := range occurrences.scopes[:occurrences.open] {
		if scope.close > at {
			kept = kept || scope.declared == at || scope.scope <= at
			occurrences.scopes[held] = scope
			held++
		}
	}
	occurrences.open, held = held, 0
	for _, scope := range occurrences.more {
		if scope.close > at {
			kept = kept || scope.declared == at || scope.scope <= at
			occurrences.more[held] = scope
			held++
		}
	}
	occurrences.more = occurrences.more[:held]
	return kept
}

func (occurrences *localOccurrences) hold(scope localOccurrenceScope) {
	if occurrences.open < len(occurrences.scopes) {
		occurrences.scopes[occurrences.open] = scope
		occurrences.open++
		return
	}
	occurrences.more = append(occurrences.more, scope)
}

// binds reports whether text binds a local named name.
func (occurrences *localOccurrences) binds(name string) bool {
	if !occurrences.read {
		occurrences.read = true
		// A clause's text is the same for every row: its names are read
		// once.
		locals, cached := localBindingNames.get(occurrences.text)
		if !cached {
			locals = expressionLocalBindings(occurrences.text)
			localBindingNames.put(occurrences.text, locals)
		}
		occurrences.locals = locals
	}
	_, bound := occurrences.locals[name]
	return bound
}

// localReader answers localVariableShadowsAt for one text, reading the
// names the text binds once (expressionLocalBindings), so a name no binding
// declares costs a map lookup. The reference scanners
// (semanticFreeReferences, expressionFreeVariables) ask it for each
// occurrence they read.
type localReader struct {
	text  string
	names map[string]struct{}
}

func newLocalReader(text string) localReader {
	return localReader{text: text, names: expressionLocalBindings(text)}
}

// shadows reports whether the occurrence of name at offset at of the text
// reads a local variable of that name.
func (reader localReader) shadows(name string, at int) bool {
	if _, declared := reader.names[name]; !declared {
		return false
	}
	return localVariableShadowsAt(reader.text, at, name)
}

// withoutTypes is types (static type names by variable) without the names
// a local binds around offset at, for a part checked as text of its own (a
// pattern comprehension, a subquery body); types itself when none is.
func (reader localReader) withoutTypes(types map[string]string, at int) map[string]string {
	var inner map[string]string
	for name := range reader.names {
		if _, typed := types[name]; !typed || !localVariableShadowsAt(reader.text, at, name) {
			continue
		}
		if inner == nil {
			inner = maps.Clone(types)
		}
		delete(inner, name)
	}
	if inner == nil {
		return types
	}
	return inner
}

// withoutLocalVariables is scope without the variables an enclosing
// expression binds around offset at of text (localVariableShadowsAt), and
// whether it bound any of scope's: in all(r IN [[true]] WHERE r[0]) the r
// read is the list's element, whatever the clause binds r to. The names
// come from expressionLocalBindings, read only when a check would fail.
func withoutLocalVariables(scope staticTypeScope, text string, at int) (staticTypeScope, bool) {
	var shadowed []string
	for name := range expressionLocalBindings(text) {
		if scope.bound(name) && localVariableShadowsAt(text, at, name) {
			shadowed = append(shadowed, name)
		}
	}
	if len(shadowed) == 0 {
		return scope, false
	}
	local := scope
	local.kinds, local.values, local.members = maps.Clone(scope.kinds), maps.Clone(scope.values), maps.Clone(scope.members)
	local.derived = maps.Clone(scope.derived)
	for _, name := range shadowed {
		delete(local.kinds, name)
		delete(local.values, name)
		delete(local.members, name)
		delete(local.derived, name)
	}
	return local, true
}

// staticListOperand reads the operand of a list position starting at start
// and returns where it ends and, when its type is static and not a list, that
// type's Neo4j name (with the parameter's name for a parameter).
func staticListOperand(cypher string, start int, params map[string]interface{}) (end int, typeName, parameter string) {
	if start >= len(cypher) {
		return start, "", ""
	}
	switch c := cypher[start]; {
	case c == '$':
		end = start + 1
		for end < len(cypher) && isIdentByte(cypher[end]) {
			end++
		}
		name := cypher[start+1 : end]
		value, bound := params[name]
		if name == "" || !bound || value == nil || isCypherListParameter(value) {
			return end, "", ""
		}
		typeName = cypherTypeName(value)
		if typeName == "Map" {
			// A map parameter can stand for a node or relationship.
			typeName = "Map, Node or Relationship"
		}
		return end, typeName, name
	case isCypherStringQuoteByte(c):
		end = start + 1
		for end < len(cypher) && (cypher[end] != c || isBackslashEscaped(cypher, end)) {
			end++
		}
		end++
	case c == '{':
		close := findMatchingDelimiter(cypher, start, '{', '}')
		if close < 0 {
			return start, "", ""
		}
		end = close + 1
	default:
		end = start
		for end < len(cypher) && (isIdentByte(cypher[end]) || cypher[end] == '.' || cypher[end] == '-' && end == start) {
			end++
		}
		// A whole call ([x IN vector(…) | x], 1 IN size(l)) is the operand.
		if c != '-' && c != '.' && (c < '0' || c > '9') {
			if open := queryGapEnd(cypher, end); open < len(cypher) && cypher[open] == '(' {
				if close := findMatchingParen(cypher, open); close > open {
					end = close + 1
				}
			}
		}
	}
	// The operator check types the operand (a variable or a property is
	// known per row); a list position rejects a type with no list or null
	// alternative.
	operand := (staticTypeScope{}).operandOf(cypher[start:end])
	if operand.typeName() == "" || !operand.excludes(isListTypeName) {
		return end, "", ""
	}
	return end, operand.typeName(), ""
}

func isCypherListParameter(value interface{}) bool {
	if _, isBytes := value.([]byte); isBytes {
		return false
	}
	kind := reflect.TypeOf(value).Kind()
	return kind == reflect.Slice || kind == reflect.Array
}

// validateStaticBooleanOperands rejects statically-known non-boolean operands
// before execution. Expressions whose type depends on a row remain subject to
// runtime validation by the row evaluator.
func (e *StorageExecutor) validateStaticBooleanOperands(admission admittedRelationshipStatement, ctx context.Context, expression string) error {
	expression = strings.TrimSpace(expression)
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		return e.validateStaticBooleanOperands(admission, ctx, inner)
	}
	if hasPrefixFoldASCII(expression, "NOT ") {
		operand := strings.TrimSpace(expression[len("NOT "):])
		if err := e.validateStaticBooleanOperands(admission, ctx, operand); err != nil {
			return err
		}
		value, known := e.staticBooleanOperandValue(admission, ctx, operand)
		if !known || value == nil {
			return nil
		}
		if _, boolean := value.(bool); !boolean {
			return invalidBooleanOperandError(value)
		}
		return nil
	}
	for _, operator := range []string{" OR ", " XOR ", " AND "} {
		// Only the expression's own operators: not those inside a list, a
		// comprehension, a map or a CASE.
		left, right, found := splitByOperatorOutsideCase(expression, operator, true, true)
		if !found {
			continue
		}
		for _, operand := range []string{left, right} {
			if err := e.validateStaticBooleanOperands(admission, ctx, operand); err != nil {
				return err
			}
			value, known := e.staticBooleanOperandValue(admission, ctx, operand)
			if !known || value == nil {
				continue
			}
			if _, boolean := value.(bool); !boolean {
				return invalidBooleanOperandError(value)
			}
		}
		return nil
	}
	return nil
}

func (e *StorageExecutor) staticBooleanOperandValue(admission admittedRelationshipStatement, ctx context.Context, expression string) (interface{}, bool) {
	expression = strings.TrimSpace(expression)
	if inner, ok := stripEnclosingExpressionParentheses(expression); ok {
		expression = inner
	}
	if value, literal := parseLiteralValueFromComputedRow(expression); literal {
		return value, true
	}
	if len(expressionVariables(expression)) > 0 {
		return nil, false
	}
	if strings.HasPrefix(expression, "{") && strings.HasSuffix(expression, "}") {
		value, err := e.evaluateMapLiteralFromValues(admission, ctx, expression, nil)
		recordExpressionFailure(ctx, err)
		return value, err == nil
	}
	for _, operator := range []string{" OR ", " XOR ", " AND ", "<=", ">=", "<>", "!=", "=", "<", ">"} {
		if _, _, found := splitByOperatorWithOptions(expression, operator, true, true); found {
			return true, true
		}
	}
	upper := upperASCII(expression)
	if strings.HasSuffix(upper, " IS NULL") || strings.HasSuffix(upper, " IS NOT NULL") || strings.HasPrefix(upper, "NOT ") {
		return true, true
	}
	value, defined := e.evaluateExpressionWithContextDefined(admission, ctx, expression, nil, nil)
	if !defined {
		return nil, false
	}
	return value, true
}

// validateListComprehensionVariables rejects a property expression where
// Cypher requires a local iterator name, including nested projections.
func validateListComprehensionVariables(text string) error {
	for index := 0; index < len(text); index++ {
		if isCypherQuoteByte(text[index]) {
			index = skipCypherQuotedText(text, index, text[index]) - 1
			continue
		}
		if text[index] != '[' {
			continue
		}
		close := findMatchingBracket(text, index)
		if close < 0 {
			break
		}
		inner := text[index+1 : close]
		in := findListComprehensionToken(inner, " IN ")
		pipe := findListComprehensionToken(inner, "|")
		_, _, _, declared := bindingDeclaration(inner, 0)
		emptyIterator := matchKeywordAt(inner, queryGapEnd(inner, 0), "IN")
		if pipe >= 0 && (in >= 0 && pipe > in || emptyIterator) && !patternComprehensionHead(inner) && !declared {
			return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "a list comprehension iterator must be a variable")
		}
	}
	return nil
}
