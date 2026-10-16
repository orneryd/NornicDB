package cypher

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

// Backtick-quoted variables (#734).
//
// A Cypher variable may be backtick-quoted (`n n`, `a``b` for a name holding
// a backtick). The executor reads statement text in many places - pattern
// parsers, scope checks, the row and shared evaluators, compiled WHERE fast
// paths, text substitution - and each read identifiers its own way, so a
// quoted variable was bound under one spelling and looked up under another.
// Execute therefore canonicalizes the statement once: every variable-position
// occurrence of a quoted variable the statement binds is rewritten to one
// plain identifier (the real name itself when it is a plain identifier that
// isn't a keyword, else an internal identifier absent from the statement),
// and the result's columns, EXPLAIN / PROFILE text and error messages are
// mapped back to what the statement said. Quoted labels, relationship types,
// property keys, map keys, parameter names, function names and names the
// statement never binds are left as written; they unquote through
// symbolicNameValue.

// symbolicNameValue is the name a symbolic name as written stands for: a
// backtick-quoted name without its quotes and with each doubled backtick as
// one backtick; any other text as is.
func symbolicNameValue(written string) string { return symbolicNameValueWithCancellation(written, nil) }
func symbolicNameValueWithCancellation(written string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}

	if len(written) >= 2 && written[0] == '`' && written[len(written)-1] == '`' {
		return replaceTextWithCancellation(written[1:len(written)-1], "``", "`", probe)
	}
	return written
}

// isOneSymbolicName reports whether text (surrounding whitespace ignored) is
// exactly one symbolic name, plain or backtick-quoted, and returns its value.
func isOneSymbolicName(text string) (string, bool) {
	return isOneSymbolicNameWithCancellation(text, nil)
}
func isOneSymbolicNameWithCancellation(text string, probe *pipelineCancellation) (string, bool) {
	if probe != nil && !probe.check() {
		return "", false
	}
	text = trimSpaceWithCancellation(text, probe)
	written, end, ok := scanSymbolicNameWithCancellation(text, 0, probe)
	if !ok || end != len(text) {
		return "", false
	}
	return symbolicNameValueWithCancellation(written, probe), true
}

// isBacktickQuotedName reports whether name is one backtick-quoted name
// (`a b`, with “ for a backtick inside).
func isBacktickQuotedName(name string) bool { return isBacktickQuotedNameWithCancellation(name, nil) }
func isBacktickQuotedNameWithCancellation(name string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	if len(name) < 3 || name[0] != '`' {
		return false
	}
	_, end, ok := scanSymbolicNameWithCancellation(name, 0, probe)
	return ok && end == len(name)
}

// quotedVariableEdit is one rewritten occurrence: original[origStart:origEnd]
// became canonical[canonStart:canonEnd].
type quotedVariableEdit struct {
	origStart, origEnd   int
	canonStart, canonEnd int
}

// quotedVariableNames maps a canonicalized statement back to the one the
// client sent.
type quotedVariableNames struct {
	original  string
	canonical string
	edits     []quotedVariableEdit
	// internal maps each internal identifier to its variable's name and to
	// the quoted token as first written.
	internal map[string]quotedVariableName
	// internalOrder lists the internal identifiers, longest first, for text
	// replacement.
	internalOrder []string
	// parameters maps each internal parameter name to the parameter's name.
	parameters map[string]string
	// Checked indexes contain no borrowed probe. Native maps are the nil-work
	// representation; checked operations borrow pipelineStringMap's owner.
	internalIndex     *quotedVariableIndex[quotedVariableName]
	parameterIndex    *quotedVariableIndex[string]
	parameterBindings []quotedParameterBinding
}

type quotedParameterBinding struct{ internal, name string }

// This adapter only selects storage. All bounded hashing, comparisons and
// collision storage remain owned by pipelineStringMap.
type quotedVariableIndex[T any] struct {
	native  map[string]T
	bounded pipelineStringMap[T]
}

func newQuotedVariableIndex[T any](probe *pipelineCancellation) *quotedVariableIndex[T] {
	index := &quotedVariableIndex[T]{}
	if probe == nil {
		index.native = make(map[string]T)
	}
	return index
}

func (index *quotedVariableIndex[T]) get(key string, probe *pipelineCancellation) (value T, exists bool) {
	if index.native != nil {
		if probe == nil {
			value, exists = index.native[key]
			return
		}
		for candidate, item := range index.native {
			if !probe.check() {
				return value, false
			}
			if comparePipelineStrings(candidate, key, probe) == 0 && probe.err == nil {
				return item, true
			}
		}
		return value, false
	}
	work := index.bounded
	work.probe = probe
	return work.Get(key)
}

func (index *quotedVariableIndex[T]) set(key string, value T, probe *pipelineCancellation) bool {
	if index.native != nil {
		index.native[key] = value
		return true
	}
	work := index.bounded
	work.probe = probe
	ok := work.Set(key, value)
	work.probe = nil
	index.bounded = work
	return ok
}

func (names *quotedVariableNames) internalName(identifier string, probe *pipelineCancellation) (quotedVariableName, bool) {
	index := names.internalIndex
	if index == nil {
		index = &quotedVariableIndex[quotedVariableName]{native: names.internal}
	}
	return index.get(identifier, probe)
}

func (names *quotedVariableNames) internalWritten(identifier string, probe *pipelineCancellation) string {
	value, _ := names.internalName(identifier, probe)
	return value.written
}

type quotedVariableName struct {
	name    string
	written string
}

type quotedToken struct {
	start, end int
	written    string
	name       string
	variable   bool // in a variable position
	binding    bool // in a position that binds the variable
	procColumn bool // a YIELD procedure column (no AS): unquoted only
	selector   bool // a map projection variable selector
	parameter  bool // a parameter name ($`a b`)
}

// canonicalizeQuotedVariables rewrites the backtick-quoted variables of
// query (see the file comment). It returns nil when there is nothing to
// rewrite; a query without a backtick isn't scanned.
func canonicalizeQuotedVariables(query string) (string, *quotedVariableNames) {
	return canonicalizeQuotedVariablesWithCancellation(query, nil)
}
func canonicalizeQuotedVariablesWithCancellation(query string, probe *pipelineCancellation) (string, *quotedVariableNames) {
	if probe != nil && !probe.check() {
		return "", nil
	}
	if indexByteWithCancellation(query, '`', probe) < 0 {
		if probe != nil && probe.err != nil {
			return "", nil
		}
		return query, nil
	}
	tokens := scanQuotedTokensWithCancellation(query, probe)
	if probe != nil && probe.err != nil {
		return "", nil
	}
	if len(tokens) == 0 {
		return query, nil
	}
	bound := newQuotedVariableIndex[struct{}](probe)
	for _, token := range tokens {
		if probe != nil && !probe.check() {
			return "", nil
		}
		if token.binding && !bound.set(token.name, struct{}{}, probe) {
			return "", nil
		}
	}
	names := &quotedVariableNames{original: query, internalIndex: newQuotedVariableIndex[quotedVariableName](probe), parameterIndex: newQuotedVariableIndex[string](probe)}
	names.internal, names.parameters = names.internalIndex.native, names.parameterIndex.native
	identifiers := newQuotedVariableIndex[string](probe)
	used := newQuotedVariableIndex[struct{}](probe)
	newIdentifier := func(name string) string {
		return quotedVariableIdentifier(query, name, func(candidate string) bool { _, exists := used.get(candidate, probe); return exists }, probe)
	}
	remember := func(identifier string, token quotedToken) bool {
		if !names.internalIndex.set(identifier, quotedVariableName{name: token.name, written: token.written}, probe) {
			return false
		}
		names.internalOrder = appendPipelineItem(names.internalOrder, identifier, probe)
		return probe == nil || probe.err == nil
	}
	replacementFor := func(token quotedToken) string {
		if identifier, ok := identifiers.get(token.name, probe); ok {
			return identifier
		}
		identifier := token.name
		if !isPlainVariableNameWithCancellation(identifier, probe) {
			identifier = newIdentifier(token.name)
			if !remember(identifier, token) {
				return ""
			}
		}
		if !identifiers.set(token.name, identifier, probe) || !used.set(identifier, struct{}{}, probe) {
			return ""
		}
		return identifier
	}
	var out strings.Builder
	if probe == nil {
		out.Grow(len(query) + 16)
	}
	last := 0
	for _, token := range tokens {
		if probe != nil && !probe.check() {
			return "", nil
		}
		_, isBound := bound.get(token.name, probe)
		var replacement string
		switch {
		case token.parameter:
			replacement = token.name
			if !isPlainVariableNameWithCancellation(token.name, probe) {
				key := concatPipelineStrings("$", token.name, probe)
				if identifier, ok := identifiers.get(key, probe); ok {
					replacement = identifier
				} else {
					replacement = newIdentifier(key)
					if !identifiers.set(key, replacement, probe) || !used.set(replacement, struct{}{}, probe) || !remember(replacement, token) || !names.parameterIndex.set(replacement, token.name, probe) {
						return "", nil
					}
					names.parameterBindings = appendPipelineItem(names.parameterBindings, quotedParameterBinding{replacement, token.name}, probe)
				}
			}
		case token.procColumn:
			if !isPlainVariableNameWithCancellation(token.name, probe) {
				continue
			}
			replacement = token.name
		case token.variable && (isBound || isPlainVariableNameWithCancellation(token.name, probe)):
			replacement = replacementFor(token)
			if token.selector && comparePipelineStrings(replacement, token.name, probe) != 0 {
				replacement = concatPipelineStrings(token.written, concatPipelineStrings(": ", replacement, probe), probe)
			}
		default:
			continue
		}
		if probe != nil && probe.err != nil {
			return "", nil
		}
		if !appendPipelineString(&out, query[last:token.start], probe) {
			return "", nil
		}
		canonStart := out.Len()
		if !appendPipelineString(&out, replacement, probe) {
			return "", nil
		}
		names.edits = appendPipelineItem(names.edits, quotedVariableEdit{origStart: token.start, origEnd: token.end, canonStart: canonStart, canonEnd: out.Len()}, probe)
		last = token.end
	}
	if probe != nil && probe.err != nil {
		return "", nil
	}
	if len(names.edits) == 0 {
		return query, nil
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return "", nil
	}
	names.canonical = out.String()
	less := func(i, j int) bool {
		if len(names.internalOrder[i]) != len(names.internalOrder[j]) {
			return len(names.internalOrder[i]) > len(names.internalOrder[j])
		}
		return comparePipelineStrings(names.internalOrder[i], names.internalOrder[j], probe) < 0
	}
	if probe == nil {
		sort.Slice(names.internalOrder, less)
	} else if !probe.sort(names.internalOrder, less, false) {
		return "", nil
	}
	if probe != nil && probe.err != nil {
		return "", nil
	}
	return names.canonical, names
}

// parameterValues returns params with each internal parameter name bound
// to its parameter's value.
func (names *quotedVariableNames) parameterValues(params map[string]interface{}) map[string]interface{} {
	if names == nil || len(names.parameters) == 0 && len(names.parameterBindings) == 0 {
		return params
	}
	out := make(map[string]interface{}, len(params)+len(names.parameters))
	for key, value := range params {
		out[key] = value
	}
	if len(names.parameterBindings) > 0 {
		for _, binding := range names.parameterBindings {
			if value, ok := params[binding.name]; ok {
				out[binding.internal] = value
			}
		}
		return out
	}
	for internal, name := range names.parameters {
		if value, ok := params[name]; ok {
			out[internal] = value
		}
	}
	return out
}

// parameterName is the name a parameter reference in the canonical
// statement stands for.
func (names *quotedVariableNames) parameterName(name string) string {
	return names.parameterNameWithCancellation(name, nil)
}
func (names *quotedVariableNames) parameterNameWithCancellation(name string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	if names != nil {
		index := names.parameterIndex
		if index == nil {
			index = &quotedVariableIndex[string]{native: names.parameters}
		}
		if original, ok := index.get(name, probe); ok {
			return original
		}
	}
	return name
}

// isPlainVariableName reports whether name can be written without quotes
// as a variable: an identifier that isn't a Cypher keyword, and without a
// '$', which only Cypher 5 reads inside a name (Cypher 25 starts a
// parameter there).
func isPlainVariableName(name string) bool {
	return isPlainVariableNameWithCancellation(name, nil)
}
func isPlainVariableNameWithCancellation(name string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	_, end, ok := scanIdentifierTokenWithCancellation(name, 0, probe)
	return ok && end == len(name) && isUnquotedNameWithCancellation(name, probe) && !isCypherKeywordWithCancellation(name, probe) && indexByteWithCancellation(name, '$', probe) < 0
}

// internalVariableIdentifier is the plain identifier a quoted variable is
// renamed to: derived from its name, so the same statement always gets the
// same text and different names get different text, and bumped until it
// occurs nowhere in query and isn't taken.
func internalVariableIdentifier(query, name string, used map[string]struct{}) string {
	return internalVariableIdentifierWithCancellation(query, name, used, nil)
}
func internalVariableIdentifierWithCancellation(query, name string, used map[string]struct{}, probe *pipelineCancellation) string {
	return quotedVariableIdentifier(query, name, func(candidate string) bool { _, taken := used[candidate]; return taken }, probe)
}
func quotedVariableIdentifier(query, name string, taken func(string) bool, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	hasher := fnv.New32a()
	if probe == nil {
		_, _ = hasher.Write([]byte(name))
	} else {
		for from := 0; from < len(name); from += 4096 {
			if !probe.check() {
				return ""
			}
			_, _ = hasher.Write([]byte(name[from:min(from+4096, len(name))]))
		}
	}
	base := fmt.Sprintf("nornicq%08xv", hasher.Sum32())
	candidate := base
	for n := 2; ; n++ {
		if probe != nil && !probe.check() {
			return ""
		}
		if !taken(candidate) && !containsTextWithCancellation(query, candidate, probe) {
			if probe != nil && probe.err != nil {
				return ""
			}
			return candidate
		}
		candidate = fmt.Sprintf("%s%dv", base, n)
	}
}

// bindParameterValuesWithCancellation borrows the caller's parameter lookup and
// alias overlay. Neither callback nor the query probe is retained in metadata.
// The caller owns the original parameter representation and its key operations.
func (names *quotedVariableNames) bindParameterValuesWithCancellation(lookup func(string) (interface{}, bool), bind func(string, interface{}) bool, probe *pipelineCancellation) bool {
	if probe != nil && !probe.poll() {
		return false
	}
	if names == nil {
		return true
	}
	for _, binding := range names.parameterBindings {
		if probe != nil && !probe.check() {
			return false
		}
		value, exists := lookup(binding.name)
		if probe != nil && !probe.poll() {
			return false
		}
		if exists && !bind(binding.internal, value) {
			return false
		}
		if probe != nil && !probe.poll() {
			return false
		}
	}
	return true
}

// scanQuotedTokens lists the backtick-quoted symbolic names of query outside
// string literals, with their positions classified.
func scanQuotedTokens(query string) []quotedToken {
	return scanQuotedTokensWithCancellation(query, nil)
}
func scanQuotedTokensWithCancellation(query string, probe *pipelineCancellation) []quotedToken {
	if probe != nil && !probe.check() {
		return nil
	}
	var tokens []quotedToken
	var brackets []byte // open ( [ { at the current position
	for i := 0; i < len(query); {
		if probe != nil && !probe.check() {
			return nil
		}
		switch c := query[i]; c {
		case '\'', '"':
			i = skipCypherQuotedTextWithCancellation(query, i, query[i], probe)
			continue
		case '(', '[', '{':
			brackets = appendPipelineItem(brackets, c, probe)
		case ')', ']', '}':
			if len(brackets) > 0 {
				brackets = brackets[:len(brackets)-1]
			}
		case '$':
			// A quoted parameter name ($`a b`) becomes a plain one, like a
			// quoted variable.
			if i+1 < len(query) && query[i+1] == '`' {
				if written, end, ok := scanSymbolicNameWithCancellation(query, i+1, probe); ok {
					tokens = appendPipelineItem(tokens, quotedToken{start: i + 1, end: end, written: written, name: symbolicNameValueWithCancellation(written, probe), parameter: true}, probe)
					i = end
					continue
				}
			}
		case '`':
			written, end, ok := scanSymbolicNameWithCancellation(query, i, probe)
			if !ok {
				i++
				continue
			}
			inner := byte(0)
			if len(brackets) > 0 {
				inner = brackets[len(brackets)-1]
			}
			token := quotedToken{start: i, end: end, written: written, name: symbolicNameValueWithCancellation(written, probe)}
			classifyQuotedTokenWithCancellation(query, &token, inner, probe)
			tokens = appendPipelineItem(tokens, token, probe)
			i = end
			continue
		}
		i++
	}
	return tokens
}

// classifyQuotedToken decides from its surroundings whether a quoted name is
// a variable, and whether it binds one.
func classifyQuotedToken(query string, token *quotedToken, inner byte) {
	classifyQuotedTokenWithCancellation(query, token, inner, nil)
}
func classifyQuotedTokenWithCancellation(query string, token *quotedToken, inner byte, probe *pipelineCancellation) {
	if probe != nil && !probe.check() {
		return
	}
	prev, prevIndex := previousSignificantWithCancellation(query, token.start, probe)
	next, nextIndex := nextSignificantWithCancellation(query, token.end, probe)
	prevWord := upperASCIIWithCancellation(previousWordWithCancellation(query, token.start, probe), probe)
	nextWord := upperASCIIWithCancellation(nextWordAtWithCancellation(query, nextIndex, probe), probe)
	switch {
	case prev == '.' && !(prevIndex > 0 && query[prevIndex-1] == '.'):
		return // property key (n.`p q`) or map projection property selector
	case next == '(':
		return // function or procedure name
	case next == '.' && dottedNameCallWithCancellation(query, nextIndex, probe):
		return // procedure or function namespace
	case prev == ':' && isLabelColonWithCancellation(query, prevIndex, inner, probe):
		return // label or relationship type
	case (prev == '|' || prev == '&' || prev == '!') && inLabelExpressionWithCancellation(query, prevIndex, inner, probe):
		return // label expression
	case next == ':' && inner == '{' && (prev == '{' || prev == ','):
		return // map key
	case isNonVariableKeyword(prevWord):
		return // database, index, constraint, alias, user or role name
	}
	token.variable = true
	switch {
	case inner == '{' && (prev == '{' || prev == ',') && (next == ',' || next == '}') && mapProjectionBraceWithCancellation(query, token.start, probe):
		token.selector = true
	case prevWord == "AS":
		token.binding = true
	case (prev == '[' || prev == '(' || prev == ',') && declaredAtWithCancellation(query, prevIndex+1, token.start, probe):
		token.binding = true // comprehension, quantifier, reduce, FOREACH or named path variable
	case prev == '(' && (next == ':' || next == ')' || next == '{') && !callParenthesisWithCancellation(query, prevIndex, probe):
		token.binding = true // node pattern variable
	case prev == '[' && (next == ':' || next == ']' || next == '*' || next == '{'):
		token.binding = true // relationship pattern variable
	case next == '=' && !followsOperator(query, nextIndex) && (prev == ',' || prev == '(' || prevWord == "MATCH" || prevWord == "MERGE" || prevWord == "CREATE"):
		token.binding = true // path variable or reduce accumulator
	case inYieldListWithCancellation(query, token.start, probe):
		if nextWord != "AS" {
			token.procColumn = true
			token.variable = false
		}
	}
}

// callParenthesis reports whether the '(' at index opens a call's
// arguments (f(x), CALL (x)) rather than a pattern or a grouping.
func callParenthesis(query string, index int) bool {
	return callParenthesisWithCancellation(query, index, nil)
}
func callParenthesisWithCancellation(query string, index int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	before, _ := previousSignificantWithCancellation(query, index, probe)
	if before == '`' {
		return true
	}
	word := previousWordWithCancellation(query, index, probe)
	return word != "" && !isCypherKeywordWithCancellation(word, probe)
}

func previousSignificant(query string, index int) (byte, int) {
	return previousSignificantWithCancellation(query, index, nil)
}
func previousSignificantWithCancellation(query string, index int, probe *pipelineCancellation) (byte, int) {
	if probe != nil && !probe.check() {
		return 0, -1
	}
	for i := index - 1; i >= 0; i-- {
		if probe != nil && !probe.check() {
			return 0, -1
		}
		if !isWhitespace(query[i]) {
			return query[i], i
		}
	}
	return 0, -1
}

func nextSignificant(query string, index int) (byte, int) {
	return nextSignificantWithCancellation(query, index, nil)
}
func nextSignificantWithCancellation(query string, index int, probe *pipelineCancellation) (byte, int) {
	if probe != nil && !probe.check() {
		return 0, len(query)
	}
	for i := index; i < len(query); i++ {
		if probe != nil && !probe.check() {
			return 0, len(query)
		}
		if !isWhitespace(query[i]) {
			return query[i], i
		}
	}
	return 0, len(query)
}

// previousWord is the identifier that ends right before index (whitespace
// skipped), or "".
func previousWord(query string, index int) string {
	return previousWordWithCancellation(query, index, nil)
}
func previousWordWithCancellation(query string, index int, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	end := index
	for end > 0 && isWhitespace(query[end-1]) {
		if probe != nil && !probe.check() {
			return ""
		}
		end--
	}
	start := end
	for start > 0 && isIdentByte(query[start-1]) {
		if probe != nil && !probe.check() {
			return ""
		}
		start--
	}
	return query[start:end]
}

func nextWordAt(query string, index int) string {
	return nextWordAtWithCancellation(query, index, nil)
}
func nextWordAtWithCancellation(query string, index int, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	word, _, ok := scanIdentifierTokenWithCancellation(query, index, probe)
	if !ok {
		return ""
	}
	return word
}

// dottedNameCall reports whether the dotted name continuing at dot (the
// index of a '.') ends in a call: `db`.labels().
func dottedNameCall(query string, dot int) bool {
	return dottedNameCallWithCancellation(query, dot, nil)
}
func dottedNameCallWithCancellation(query string, dot int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	for i := dot; i < len(query) && query[i] == '.'; {
		if probe != nil && !probe.check() {
			return false
		}
		_, end, ok := scanSymbolicNameWithCancellation(query, i+1, probe)
		if !ok {
			return false
		}
		next, nextIndex := nextSignificantWithCancellation(query, end, probe)
		if next == '(' {
			return true
		}
		i = nextIndex
	}
	return false
}

// isLabelColon reports whether the ':' at colon starts a label or a
// relationship type rather than a map value ({key: value}).
func isLabelColon(query string, colon int, inner byte) bool {
	return isLabelColonWithCancellation(query, colon, inner, nil)
}
func isLabelColonWithCancellation(query string, colon int, inner byte, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	if inner != '{' {
		return true
	}
	before, beforeIndex := previousSignificantWithCancellation(query, colon, probe)
	if before == 0 {
		return true
	}
	start := beforeIndex
	if before == '`' {
		for start = beforeIndex - 1; start >= 0 && query[start] != '`'; start-- {
			if probe != nil && !probe.check() {
				return false
			}
		}
	} else {
		for start > 0 && isIdentByte(query[start-1]) {
			if probe != nil && !probe.check() {
				return false
			}
			start--
		}
	}
	keyPrev, _ := previousSignificantWithCancellation(query, start, probe)
	return !(keyPrev == '{' || keyPrev == ',')
}

// inLabelExpression reports whether the operator at index continues a label
// expression (:A|B, :A&B, :!A).
func inLabelExpression(query string, index int, inner byte) bool {
	return inLabelExpressionWithCancellation(query, index, inner, nil)
}
func inLabelExpressionWithCancellation(query string, index int, inner byte, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	before, beforeIndex := previousSignificantWithCancellation(query, index, probe)
	if before == ':' {
		return isLabelColonWithCancellation(query, beforeIndex, inner, probe)
	}
	start := beforeIndex
	if before == '`' {
		for start = beforeIndex - 1; start >= 0 && query[start] != '`'; start-- {
			if probe != nil && !probe.check() {
				return false
			}
		}
	} else if isIdentByte(before) {
		for start > 0 && isIdentByte(query[start-1]) {
			if probe != nil && !probe.check() {
				return false
			}
			start--
		}
	} else {
		return false
	}
	operator, operatorIndex := previousSignificantWithCancellation(query, start, probe)
	switch operator {
	case ':':
		return isLabelColonWithCancellation(query, operatorIndex, inner, probe)
	case '|', '&', '!':
		return inLabelExpressionWithCancellation(query, operatorIndex, inner, probe)
	}
	return false
}

// followsOperator reports whether the '=' at index is part of an operator
// (=~, <=, >=, <>, ==).
func followsOperator(query string, index int) bool {
	if index+1 < len(query) && (query[index+1] == '~' || query[index+1] == '=') {
		return true
	}
	return index > 0 && (query[index-1] == '<' || query[index-1] == '>' || query[index-1] == '!' || query[index-1] == '=')
}

// mapProjectionBrace reports whether the '{' enclosing index follows a
// variable (n {…}), making it a map projection rather than a map literal.
func mapProjectionBrace(query string, index int) bool {
	return mapProjectionBraceWithCancellation(query, index, nil)
}
func mapProjectionBraceWithCancellation(query string, index int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	depth := 0
	for i := index - 1; i >= 0; i-- {
		if probe != nil && !probe.check() {
			return false
		}
		switch query[i] {
		case '}', ')', ']':
			depth++
		case '(', '[':
			if depth == 0 {
				return false
			}
			depth--
		case '{':
			if depth > 0 {
				depth--
				continue
			}
			before, _ := previousSignificantWithCancellation(query, i, probe)
			return before == '`' || isIdentByte(before)
		}
	}
	return false
}

// inYieldList reports whether index is inside the item list of a YIELD.
func inYieldList(query string, index int) bool {
	return inYieldListWithCancellation(query, index, nil)
}
func inYieldListWithCancellation(query string, index int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	yield := lastKeywordBeforeWithCancellation(query, index, "YIELD", probe)
	if yield < 0 {
		return false
	}
	between := query[yield:index]
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "MATCH", "UNWIND", "CALL", "ORDER", "SKIP", "LIMIT"} {
		if probe != nil && !probe.check() {
			return false
		}
		if keywordIndexFromDefaultWithCancellation(between, keyword, 0, probe) >= 0 {
			return false
		}
	}
	return true
}

func lastKeywordBefore(query string, index int, keyword string) int {
	return lastKeywordBeforeWithCancellation(query, index, keyword, nil)
}
func lastKeywordBeforeWithCancellation(query string, index int, keyword string, probe *pipelineCancellation) int {
	if probe != nil && !probe.check() {
		return -1
	}
	found := -1
	for from := 0; from < index; {
		if probe != nil && !probe.check() {
			return -1
		}
		rel := keywordIndexFromDefaultWithCancellation(query[from:index], keyword, 0, probe)
		if rel < 0 {
			break
		}
		found = from + rel
		from = found + len(keyword)
	}
	return found
}

// isNonVariableKeyword reports whether a quoted name after word names a
// database, graph, index, constraint, alias, user or role.
func isNonVariableKeyword(word string) bool {
	switch word {
	case "USE", "DATABASE", "DATABASES", "GRAPH", "INDEX", "CONSTRAINT", "ALIAS", "COMPOSITE", "USER", "ROLE", "ROLES", "USERS":
		return true
	}
	return false
}

// isCypherKeyword reports whether name is a Cypher keyword, which a plain
// variable can't be.
func isCypherKeyword(name string) bool {
	return isCypherKeywordWithCancellation(name, nil)
}
func isCypherKeywordWithCancellation(name string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	switch upperASCIIWithCancellation(name, probe) {
	case "ALL", "AND", "ANY", "AS", "ASC", "ASCENDING", "BY", "CALL", "CASE", "CONTAINS", "CREATE", "DELETE", "DESC", "DESCENDING",
		"DETACH", "DISTINCT", "ELSE", "END", "ENDS", "EXISTS", "FALSE", "FOREACH", "IN", "IS", "LIMIT", "MATCH", "MERGE", "NONE",
		"NOT", "NULL", "ON", "OPTIONAL", "OR", "ORDER", "REMOVE", "RETURN", "SET", "SINGLE", "SKIP", "STARTS", "THEN", "TRUE",
		"UNION", "UNWIND", "USE", "WHEN", "WHERE", "WITH", "XOR", "YIELD", "COUNT", "COLLECT", "REDUCE":
		return true
	}
	return false
}

// restore maps an Execute result and error of the canonical statement back
// to the statement the client sent.
func (names *quotedVariableNames) restore(result *ExecuteResult, err error, parseItems func(string) []returnItem) (*ExecuteResult, error) {
	return names.restoreWithCancellation(result, err, parseItems, nil)
}
func (names *quotedVariableNames) restoreWithCancellation(result *ExecuteResult, err error, parseItems func(string) []returnItem, probe *pipelineCancellation) (restoredResult *ExecuteResult, restoredErr error) {
	restoredResult, restoredErr = result, err
	if probe != nil {
		// This is deferred caller work: an already canceled query must not
		// reparse or render its source just to restore a result or error.
		defer func() {
			if !probe.poll() {
				restoredResult = result
				restoredErr = err
				if restoredErr == nil {
					restoredErr = probe.err
				}
			}
		}()
		if !probe.poll() {
			return
		}
	}
	if names == nil {
		return
	}
	if result != nil {
		restored := *result
		if len(result.Columns) > 0 {
			restored.Columns = make([]string, len(result.Columns))
			for i, column := range result.Columns {
				if probe != nil && !probe.check() {
					return
				}
				restored.Columns[i] = names.restoreColumnWithCancellation(column, probe)
			}
		}
		if len(result.Metadata) > 0 {
			restored.Metadata, _ = names.restoreValueWithCancellation(result.Metadata, probe).(map[string]interface{})
			if probe != nil && probe.err != nil {
				return
			}
			// The plan text is rendered again from the restored plan: names
			// replaced in the rendered text would break its column padding
			// and its truncation of long descriptions.
			if plan, ok := restored.Metadata["plan"].(*ExecutionPlan); ok && plan != nil {
				if _, rendered := restored.Metadata["planString"]; rendered {
					restored.Metadata["planString"] = formatPlanWithCancellation(plan, probe)
				}
			}
		}
		if isExplainOrProfileWithCancellation(names.original, probe) {
			restored.Rows, _ = names.restoreValueWithCancellation(result.Rows, probe).([][]interface{})
		} else if len(restored.Columns) > 0 {
			names.restoreProjectionWithCancellation(&restored, parseItems, probe)
		}
		restoredResult = &restored
	}
	if probe != nil && probe.err != nil {
		return
	}
	if err != nil && len(names.internalOrder) > 0 {
		message := err.Error()
		text := names.restoreTextWithCancellation(message, probe)
		if comparePipelineStrings(text, message, probe) != 0 {
			restoredErr = &quotedVariableError{cause: err, message: text}
		}
	}
	return
}

// restoreColumn names a result column as the client's statement does: a
// variable's column is its name; an expression's column is its text as
// written.
func (names *quotedVariableNames) restoreColumn(column string) string {
	return names.restoreColumnWithCancellation(column, nil)
}
func (names *quotedVariableNames) restoreColumnWithCancellation(column string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	if internal, ok := names.internalName(column, probe); ok {
		return internal.name
	}
	if isPlainVariableNameWithCancellation(column, probe) || !names.touchesWithCancellation(column, probe) {
		return column
	}
	// The final RETURN names the columns, so its text is the last
	// occurrence.
	if start := lastIndexTextWithCancellation(names.canonical, column, probe); start >= 0 {
		if original, ok := names.originalSpanWithCancellation(start, start+len(column), probe); ok {
			return original
		}
	}
	return names.restoreTextWithCancellation(column, probe)
}

// touches reports whether text holds a rewritten occurrence.
func (names *quotedVariableNames) touches(text string) bool {
	return names.touchesWithCancellation(text, nil)
}
func (names *quotedVariableNames) touchesWithCancellation(text string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	for _, identifier := range names.internalOrder {
		if probe != nil && !probe.check() {
			return false
		}
		if containsTextWithCancellation(text, identifier, probe) {
			return true
		}
	}
	for _, edit := range names.edits {
		if probe != nil && !probe.check() {
			return false
		}
		if containsTextWithCancellation(text, names.canonical[edit.canonStart:edit.canonEnd], probe) {
			return true
		}
	}
	return false
}

// originalSpan maps canonical[start:end] to the original text it came from,
// when both ends fall outside a rewritten occurrence or on its edges.
func (names *quotedVariableNames) originalSpan(start, end int) (string, bool) {
	return names.originalSpanWithCancellation(start, end, nil)
}
func (names *quotedVariableNames) originalSpanWithCancellation(start, end int, probe *pipelineCancellation) (string, bool) {
	if probe != nil && !probe.check() {
		return "", false
	}
	mapOffset := func(offset int, isEnd bool) (int, bool) {
		shift := 0
		for _, edit := range names.edits {
			if probe != nil && !probe.check() {
				return 0, false
			}
			switch {
			case offset < edit.canonStart || (!isEnd && offset == edit.canonStart):
				return offset + shift, true
			case offset == edit.canonEnd:
				return edit.origEnd, true
			case offset < edit.canonEnd:
				if offset == edit.canonStart {
					return edit.origStart, true
				}
				return 0, false
			}
			shift = edit.origEnd - edit.canonEnd
		}
		return offset + shift, true
	}
	from, ok := mapOffset(start, false)
	if !ok {
		return "", false
	}
	to, ok := mapOffset(end, true)
	if !ok || to < from || to > len(names.original) {
		return "", false
	}
	return names.original[from:to], true
}

// restoreText replaces each internal identifier in text with the quoted
// name as the statement wrote it.
func (names *quotedVariableNames) restoreText(text string) string {
	return names.restoreTextWithCancellation(text, nil)
}
func (names *quotedVariableNames) restoreTextWithCancellation(text string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	for _, identifier := range names.internalOrder {
		if probe != nil && !probe.check() {
			return ""
		}
		if containsTextWithCancellation(text, identifier, probe) {
			text = replaceTextWithCancellation(text, identifier, names.internalWritten(identifier, probe), probe)
		}
	}
	return text
}

func (names *quotedVariableNames) restoreValue(value interface{}) interface{} {
	return names.restoreValueWithCancellation(value, nil)
}
func (names *quotedVariableNames) restoreValueWithCancellation(value interface{}, probe *pipelineCancellation) interface{} {
	if probe != nil && !probe.check() {
		return nil
	}
	switch typed := value.(type) {
	case string:
		return names.restoreTextWithCancellation(typed, probe)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			if probe != nil && !probe.check() {
				return nil
			}
			key = names.restoreTextWithCancellation(key, probe)
			item = names.restoreValueWithCancellation(item, probe)
			if probe != nil && probe.err != nil {
				return nil
			}
			out[key] = item
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			if probe != nil && !probe.check() {
				return nil
			}
			out[i] = names.restoreValueWithCancellation(item, probe)
		}
		return out
	case *ExecutionPlan:
		if typed == nil {
			return value
		}
		plan := *typed
		plan.Query = names.restoreStatementTextWithCancellation(typed.Query, probe)
		plan.Root, _ = names.restoreValueWithCancellation(typed.Root, probe).(*PlanOperator)
		return &plan
	case *PlanOperator:
		if typed == nil {
			return value
		}
		operator := *typed
		operator.Description = names.restoreTextWithCancellation(typed.Description, probe)
		if typed.Arguments != nil {
			operator.Arguments, _ = names.restoreValueWithCancellation(typed.Arguments, probe).(map[string]interface{})
			if probe != nil && probe.err != nil {
				return nil
			}
		}
		operator.Identifiers = make([]string, len(typed.Identifiers))
		for i, identifier := range typed.Identifiers {
			if probe != nil && !probe.check() {
				return nil
			}
			if internal, ok := names.internalName(identifier, probe); ok {
				identifier = internal.name
			}
			operator.Identifiers[i] = identifier
		}
		operator.Children = make([]*PlanOperator, len(typed.Children))
		for i, child := range typed.Children {
			if probe != nil && !probe.check() {
				return nil
			}
			operator.Children[i], _ = names.restoreValueWithCancellation(child, probe).(*PlanOperator)
		}
		return &operator
	case [][]interface{}:
		out := make([][]interface{}, len(typed))
		for i, row := range typed {
			if probe != nil && !probe.check() {
				return nil
			}
			out[i], _ = names.restoreValueWithCancellation(row, probe).([]interface{})
		}
		return out
	}
	return value
}

func isExplainOrProfile(query string) bool {
	return isExplainOrProfileWithCancellation(query, nil)
}
func isExplainOrProfileWithCancellation(query string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	trimmed := trimSpaceWithCancellation(query, probe)
	return startsWithKeywordFold(trimmed, "EXPLAIN") || startsWithKeywordFold(trimmed, "PROFILE")
}

// quotedVariableError is an error of a canonicalized statement with its
// message restored; codes, details and unwrapping come from the cause.
type quotedVariableError struct {
	cause   error
	message string
}

func (e *quotedVariableError) Error() string { return e.message }
func (e *quotedVariableError) Unwrap() error { return e.cause }

// BoltErrorCode keeps the cause's Neo4j classification.
func (e *quotedVariableError) BoltErrorCode() string {
	var classified interface{ BoltErrorCode() string }
	if errors.As(e.cause, &classified) {
		return classified.BoltErrorCode()
	}
	return ""
}

// BoltErrorDetail keeps the cause's conformance detail.
func (e *quotedVariableError) BoltErrorDetail() string {
	var detailed interface{ BoltErrorDetail() string }
	if errors.As(e.cause, &detailed) {
		return detailed.BoltErrorDetail()
	}
	return ""
}

type quotedVariablesKey struct{}

// withQuotedVariableNames records the canonicalization of the statement ctx
// executes, so checks that must see the statement as written (column names,
// the validation cache) can map back to it.
func withQuotedVariableNames(ctx context.Context, names *quotedVariableNames) context.Context {
	return context.WithValue(ctx, quotedVariablesKey{}, names)
}

// quotedVariableNamesFor returns the canonicalization whose canonical text
// is cypher, or nil.
func quotedVariableNamesFor(ctx context.Context, cypher string) *quotedVariableNames {
	return quotedVariableNamesForWithCancellation(ctx, cypher, nil)
}
func quotedVariableNamesForWithCancellation(ctx context.Context, cypher string, probe *pipelineCancellation) *quotedVariableNames {
	if probe != nil && !probe.check() {
		return nil
	}
	if ctx == nil {
		return nil
	}
	names, _ := ctx.Value(quotedVariablesKey{}).(*quotedVariableNames)
	if names == nil || comparePipelineStrings(names.canonical, cypher, probe) != 0 || probe != nil && probe.err != nil {
		return nil
	}
	return names
}

// originalText maps canonical[start:end] back to the text the client wrote,
// or restores the internal names in it when the span can't be mapped.
func (names *quotedVariableNames) originalText(start, end int) string {
	return names.originalTextWithCancellation(start, end, nil)
}
func (names *quotedVariableNames) originalTextWithCancellation(start, end int, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	if original, ok := names.originalSpanWithCancellation(start, end, probe); ok {
		return original
	}
	return names.restoreTextWithCancellation(names.canonical[start:end], probe)
}

// finalReturnBody locates the body of text's last top-level RETURN (after
// the keyword).
func finalReturnBody(text string) (int, int, bool) {
	return finalReturnBodyWithCancellation(text, nil)
}
func finalReturnBodyWithCancellation(text string, probe *pipelineCancellation) (int, int, bool) {
	if probe != nil && !probe.check() {
		return 0, 0, false
	}
	positions := findAllTopLevelPipelineKeywordPositionsWithCancellation(text, "RETURN", probe)
	if len(positions) == 0 {
		return 0, 0, false
	}
	return positions[len(positions)-1] + len("RETURN"), len(text), true
}

// restoreProjection names the result columns from the client's final RETURN
// items, one by one: an alias or a bare variable is its name, any other
// item its text as written (x + 1 and `x` + 1 are different columns). For
// RETURN * the columns are sorted by name, as Neo4j lists them, with the
// rows reordered to match.
func (names *quotedVariableNames) restoreProjection(result *ExecuteResult, parseItems func(string) []returnItem) {
	names.restoreProjectionWithCancellation(result, parseItems, nil)
}
func (names *quotedVariableNames) restoreProjectionWithCancellation(result *ExecuteResult, parseItems func(string) []returnItem, probe *pipelineCancellation) {
	if probe != nil && !probe.check() {
		return
	}
	start, end, ok := finalReturnBodyWithCancellation(names.canonical, probe)
	if !ok {
		return
	}
	body := trimSpaceWithCancellation(names.canonical[start:end], probe)
	body, _ = cutDistinctWithCancellation(body, probe)
	if strings.HasPrefix(body, "*") && (len(body) == 1 || !isIdentByte(body[1])) && keywordIndexFromWithCancellation(body, "UNION", 0, keywordScanOpts{SkipParens: true, SkipBrackets: true, SkipBraces: true, SkipStrings: true, SkipBackticks: true, SkipComments: true, Boundary: keywordBoundaryWord}, probe) < 0 {
		sortColumnsByNameWithCancellation(result, probe)
		return
	}
	original := names.originalTextWithCancellation(start, end, probe)
	if probe != nil && !probe.poll() {
		return
	}
	items := parseItems(original)
	if probe != nil && !probe.poll() {
		return
	}
	canonicalItems := parseItems(names.canonical[start:end])
	if probe != nil && !probe.poll() {
		return
	}
	if len(items) != len(result.Columns) || len(canonicalItems) != len(items) {
		return
	}
	for i, item := range items {
		if probe != nil && !probe.check() {
			return
		}
		if comparePipelineStrings(item.alias, trimSpaceWithCancellation(item.expr, probe), probe) != 0 {
			// parseItems gives the alias's value already: unquoting it again
			// would trim ` m` to m and read ```a``` as a.
			result.Columns[i] = item.alias
			continue
		}
		if name, isName := isOneSymbolicNameWithCancellation(item.expr, probe); isName {
			result.Columns[i] = name
			continue
		}
		result.Columns[i] = trimSpaceWithCancellation(item.expr, probe)
	}
}

func sortColumnsByName(result *ExecuteResult) {
	sortColumnsByNameWithCancellation(result, nil)
}
func sortColumnsByNameWithCancellation(result *ExecuteResult, probe *pipelineCancellation) {
	if probe != nil && !probe.check() {
		return
	}
	order := make([]int, len(result.Columns))
	for i := range order {
		if probe != nil && !probe.check() {
			return
		}
		order[i] = i
	}
	less := func(a, b int) bool {
		return comparePipelineStrings(result.Columns[order[a]], result.Columns[order[b]], probe) < 0
	}
	if probe == nil {
		sort.SliceStable(order, less)
	} else if !probe.sort(order, less, true) {
		return
	}
	columns := make([]string, len(order))
	for i, from := range order {
		if probe != nil && !probe.check() {
			return
		}
		columns[i] = result.Columns[from]
	}
	rows := make([][]interface{}, len(result.Rows))
	for r, row := range result.Rows {
		if probe != nil && !probe.check() {
			return
		}
		if len(row) != len(order) {
			return
		}
		rows[r] = make([]interface{}, len(order))
		for i, from := range order {
			if probe != nil && !probe.check() {
				return
			}
			rows[r][i] = row[from]
		}
	}
	result.Columns, result.Rows = columns, rows
}

// restoreStatementText maps statement text taken from the canonical
// statement (the whole statement or a part of it) back to the client's.
func (names *quotedVariableNames) restoreStatementText(text string) string {
	return names.restoreStatementTextWithCancellation(text, nil)
}
func (names *quotedVariableNames) restoreStatementTextWithCancellation(text string, probe *pipelineCancellation) string {
	if probe != nil && !probe.check() {
		return ""
	}
	if text != "" {
		if start := lastIndexTextWithCancellation(names.canonical, text, probe); start >= 0 {
			return names.originalTextWithCancellation(start, start+len(text), probe)
		}
	}
	return names.restoreTextWithCancellation(text, probe)
}

// declaredAt reports whether the name written at offset at is the variable
// a declaration after text[from] declares, as the declaration readers read
// it: v IN … (bindingDeclaration, also FOREACH's) or p = (…)
// (namedPathDeclaration).
func declaredAt(text string, from, at int) bool {
	return declaredAtWithCancellation(text, from, at, nil)
}
func declaredAtWithCancellation(text string, from, at int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}
	if _, declared, _, ok := bindingDeclarationWithCancellation(text, from, probe); ok && declared == at {
		return true
	}
	_, declared, _, ok := namedPathDeclarationWithCancellation(text, from, probe)
	return ok && declared == at
}
