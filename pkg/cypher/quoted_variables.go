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
func symbolicNameValue(written string) string {
	if len(written) >= 2 && written[0] == '`' && written[len(written)-1] == '`' {
		return strings.ReplaceAll(written[1:len(written)-1], "``", "`")
	}
	return written
}

// isOneSymbolicName reports whether text (surrounding whitespace ignored) is
// exactly one symbolic name, plain or backtick-quoted, and returns its value.
func isOneSymbolicName(text string) (string, bool) {
	text = strings.TrimSpace(text)
	written, end, ok := scanSymbolicName(text, 0)
	if !ok || end != len(text) {
		return "", false
	}
	return symbolicNameValue(written), true
}

// isBacktickQuotedName reports whether name is one backtick-quoted name
// (`a b`, with “ for a backtick inside).
func isBacktickQuotedName(name string) bool {
	if len(name) < 3 || name[0] != '`' {
		return false
	}
	_, end, ok := scanSymbolicName(name, 0)
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
	if strings.IndexByte(query, '`') < 0 {
		return query, nil
	}
	tokens := scanQuotedTokens(query)
	if len(tokens) == 0 {
		return query, nil
	}
	bound := make(map[string]struct{})
	for _, token := range tokens {
		if token.binding {
			bound[token.name] = struct{}{}
		}
	}

	names := &quotedVariableNames{original: query, internal: make(map[string]quotedVariableName)}
	identifiers := make(map[string]string) // name -> replacement identifier
	used := make(map[string]struct{})
	replacementFor := func(token quotedToken) string {
		if identifier, ok := identifiers[token.name]; ok {
			return identifier
		}
		identifier := token.name
		if !isPlainVariableName(identifier) {
			identifier = internalVariableIdentifier(query, token.name, used)
			names.internal[identifier] = quotedVariableName{name: token.name, written: token.written}
		}
		identifiers[token.name] = identifier
		used[identifier] = struct{}{}
		return identifier
	}
	var out strings.Builder
	out.Grow(len(query) + 16)
	last := 0
	for _, token := range tokens {
		_, isBound := bound[token.name]
		var replacement string
		switch {
		case token.parameter:
			replacement = token.name
			if !isPlainVariableName(token.name) {
				key := "$" + token.name
				if identifier, ok := identifiers[key]; ok {
					replacement = identifier
				} else {
					replacement = internalVariableIdentifier(query, key, used)
					identifiers[key] = replacement
					used[replacement] = struct{}{}
					names.internal[replacement] = quotedVariableName{name: token.name, written: token.written}
					if names.parameters == nil {
						names.parameters = make(map[string]string)
					}
					names.parameters[replacement] = token.name
				}
			}
		case token.procColumn:
			if !isPlainVariableName(token.name) {
				continue
			}
			replacement = token.name
		case token.variable && (isBound || isPlainVariableName(token.name)):
			// A quoted name that can be written plain is the plain name
			// (`x` is x), bound here or not.
			replacement = replacementFor(token)
			if token.selector && replacement != token.name {
				replacement = token.written + ": " + replacement
			}
		default:
			continue
		}
		out.WriteString(query[last:token.start])
		canonStart := out.Len()
		out.WriteString(replacement)
		names.edits = append(names.edits, quotedVariableEdit{origStart: token.start, origEnd: token.end, canonStart: canonStart, canonEnd: out.Len()})
		last = token.end
	}
	if len(names.edits) == 0 {
		return query, nil
	}
	out.WriteString(query[last:])
	names.canonical = out.String()
	for identifier := range names.internal {
		names.internalOrder = append(names.internalOrder, identifier)
	}
	sort.Slice(names.internalOrder, func(i, j int) bool {
		if len(names.internalOrder[i]) != len(names.internalOrder[j]) {
			return len(names.internalOrder[i]) > len(names.internalOrder[j])
		}
		return names.internalOrder[i] < names.internalOrder[j]
	})
	return names.canonical, names
}

// parameterValues returns params with each internal parameter name bound
// to its parameter's value.
func (names *quotedVariableNames) parameterValues(params map[string]interface{}) map[string]interface{} {
	if names == nil || len(names.parameters) == 0 {
		return params
	}
	out := make(map[string]interface{}, len(params)+len(names.parameters))
	for key, value := range params {
		out[key] = value
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
	if names != nil {
		if original, ok := names.parameters[name]; ok {
			return original
		}
	}
	return name
}

// isPlainVariableName reports whether name can be written without quotes
// as a variable: an identifier that isn't a Cypher keyword.
func isPlainVariableName(name string) bool {
	_, end, ok := scanIdentifierToken(name, 0)
	return ok && end == len(name) && !isCypherKeyword(name)
}

// internalVariableIdentifier is the plain identifier a quoted variable is
// renamed to: derived from its name, so the same statement always gets the
// same text and different names get different text, and bumped until it
// occurs nowhere in query and isn't taken.
func internalVariableIdentifier(query, name string, used map[string]struct{}) string {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(name))
	base := fmt.Sprintf("nornicq%08xv", hasher.Sum32())
	candidate := base
	for n := 2; ; n++ {
		if _, taken := used[candidate]; !taken && !strings.Contains(query, candidate) {
			return candidate
		}
		candidate = fmt.Sprintf("%s%dv", base, n)
	}
}

// scanQuotedTokens lists the backtick-quoted symbolic names of query outside
// string literals, with their positions classified.
func scanQuotedTokens(query string) []quotedToken {
	var tokens []quotedToken
	var brackets []byte // open ( [ { at the current position
	for i := 0; i < len(query); {
		switch c := query[i]; c {
		case '\'', '"':
			i = skipQuotedText(query, i)
			continue
		case '(', '[', '{':
			brackets = append(brackets, c)
		case ')', ']', '}':
			if len(brackets) > 0 {
				brackets = brackets[:len(brackets)-1]
			}
		case '$':
			// A quoted parameter name ($`a b`) becomes a plain one, like a
			// quoted variable.
			if i+1 < len(query) && query[i+1] == '`' {
				if written, end, ok := scanSymbolicName(query, i+1); ok {
					tokens = append(tokens, quotedToken{start: i + 1, end: end, written: written, name: symbolicNameValue(written), parameter: true})
					i = end
					continue
				}
			}
		case '`':
			written, end, ok := scanSymbolicName(query, i)
			if !ok {
				i++
				continue
			}
			inner := byte(0)
			if len(brackets) > 0 {
				inner = brackets[len(brackets)-1]
			}
			token := quotedToken{start: i, end: end, written: written, name: symbolicNameValue(written)}
			classifyQuotedToken(query, &token, inner)
			tokens = append(tokens, token)
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
	prev, prevIndex := previousSignificant(query, token.start)
	next, nextIndex := nextSignificant(query, token.end)
	prevWord := upperASCII(previousWord(query, token.start))
	nextWord := upperASCII(nextWordAt(query, nextIndex))
	switch {
	case prev == '.' && !(prevIndex > 0 && query[prevIndex-1] == '.'):
		return // property key (n.`p q`) or map projection property selector
	case next == '(':
		return // function or procedure name
	case next == '.' && dottedNameCall(query, nextIndex):
		return // procedure or function namespace
	case prev == ':' && isLabelColon(query, prevIndex, inner):
		return // label or relationship type
	case (prev == '|' || prev == '&' || prev == '!') && inLabelExpression(query, prevIndex, inner):
		return // label expression
	case next == ':' && inner == '{' && (prev == '{' || prev == ','):
		return // map key
	case isNonVariableKeyword(prevWord):
		return // database, index, constraint, alias, user or role name
	}
	token.variable = true
	switch {
	case inner == '{' && (prev == '{' || prev == ',') && (next == ',' || next == '}') && mapProjectionBrace(query, token.start):
		token.selector = true
	case prevWord == "AS":
		token.binding = true
	case nextWord == "IN" && (prev == '[' || prev == '(' || prev == ','):
		token.binding = true // comprehension, quantifier, reduce or FOREACH variable
	case prev == '(' && (next == ':' || next == ')' || next == '{') && !callParenthesis(query, prevIndex):
		token.binding = true // node pattern variable
	case prev == '[' && (next == ':' || next == ']' || next == '*' || next == '{'):
		token.binding = true // relationship pattern variable
	case next == '=' && !followsOperator(query, nextIndex) && (prev == ',' || prev == '(' || prevWord == "MATCH" || prevWord == "MERGE" || prevWord == "CREATE"):
		token.binding = true // path variable or reduce accumulator
	case inYieldList(query, token.start):
		if nextWord != "AS" {
			token.procColumn = true
			token.variable = false
		}
	}
}

// callParenthesis reports whether the '(' at index opens a call's
// arguments (f(x), CALL (x)) rather than a pattern or a grouping.
func callParenthesis(query string, index int) bool {
	before, _ := previousSignificant(query, index)
	if before == '`' {
		return true
	}
	word := previousWord(query, index)
	return word != "" && !isCypherKeyword(word)
}

func previousSignificant(query string, index int) (byte, int) {
	for i := index - 1; i >= 0; i-- {
		if !isWhitespace(query[i]) {
			return query[i], i
		}
	}
	return 0, -1
}

func nextSignificant(query string, index int) (byte, int) {
	for i := index; i < len(query); i++ {
		if !isWhitespace(query[i]) {
			return query[i], i
		}
	}
	return 0, len(query)
}

// previousWord is the identifier that ends right before index (whitespace
// skipped), or "".
func previousWord(query string, index int) string {
	end := index
	for end > 0 && isWhitespace(query[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentifierPart(query[start-1]) {
		start--
	}
	return query[start:end]
}

func nextWordAt(query string, index int) string {
	word, _, ok := scanIdentifierToken(query, index)
	if !ok {
		return ""
	}
	return word
}

// dottedNameCall reports whether the dotted name continuing at dot (the
// index of a '.') ends in a call: `db`.labels().
func dottedNameCall(query string, dot int) bool {
	for i := dot; i < len(query) && query[i] == '.'; {
		_, end, ok := scanSymbolicName(query, i+1)
		if !ok {
			return false
		}
		next, nextIndex := nextSignificant(query, end)
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
	if inner != '{' {
		return true
	}
	before, beforeIndex := previousSignificant(query, colon)
	if before == 0 {
		return true
	}
	start := beforeIndex
	if before == '`' {
		for start = beforeIndex - 1; start >= 0 && query[start] != '`'; start-- {
		}
	} else {
		for start > 0 && isIdentifierPart(query[start-1]) {
			start--
		}
	}
	keyPrev, _ := previousSignificant(query, start)
	return !(keyPrev == '{' || keyPrev == ',')
}

// inLabelExpression reports whether the operator at index continues a label
// expression (:A|B, :A&B, :!A).
func inLabelExpression(query string, index int, inner byte) bool {
	before, beforeIndex := previousSignificant(query, index)
	if before == ':' {
		return isLabelColon(query, beforeIndex, inner)
	}
	start := beforeIndex
	if before == '`' {
		for start = beforeIndex - 1; start >= 0 && query[start] != '`'; start-- {
		}
	} else if isIdentifierPart(before) {
		for start > 0 && isIdentifierPart(query[start-1]) {
			start--
		}
	} else {
		return false
	}
	operator, operatorIndex := previousSignificant(query, start)
	switch operator {
	case ':':
		return isLabelColon(query, operatorIndex, inner)
	case '|', '&', '!':
		return inLabelExpression(query, operatorIndex, inner)
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
	depth := 0
	for i := index - 1; i >= 0; i-- {
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
			before, _ := previousSignificant(query, i)
			return before == '`' || isIdentifierPart(before)
		}
	}
	return false
}

// inYieldList reports whether index is inside the item list of a YIELD.
func inYieldList(query string, index int) bool {
	yield := lastKeywordBefore(query, index, "YIELD")
	if yield < 0 {
		return false
	}
	between := query[yield:index]
	for _, keyword := range []string{"WHERE", "RETURN", "WITH", "MATCH", "UNWIND", "CALL", "ORDER", "SKIP", "LIMIT"} {
		if FindKeywordIndex(between, keyword) >= 0 {
			return false
		}
	}
	return true
}

func lastKeywordBefore(query string, index int, keyword string) int {
	found := -1
	for from := 0; from < index; {
		rel := FindKeywordIndex(query[from:index], keyword)
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
	switch upperASCII(name) {
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
	if names == nil {
		return result, err
	}
	if result != nil {
		restored := *result
		if len(result.Columns) > 0 {
			restored.Columns = make([]string, len(result.Columns))
			for i, column := range result.Columns {
				restored.Columns[i] = names.restoreColumn(column)
			}
		}
		if len(result.Metadata) > 0 {
			restored.Metadata = names.restoreValue(result.Metadata).(map[string]interface{})
			// The plan text is rendered again from the restored plan: names
			// replaced in the rendered text would break its column padding
			// and its truncation of long descriptions.
			if plan, ok := restored.Metadata["plan"].(*ExecutionPlan); ok && plan != nil {
				if _, rendered := restored.Metadata["planString"]; rendered {
					restored.Metadata["planString"] = formatPlan(plan)
				}
			}
		}
		if isExplainOrProfile(names.original) {
			restored.Rows = names.restoreValue(result.Rows).([][]interface{})
		} else if len(restored.Columns) > 0 {
			names.restoreProjection(&restored, parseItems)
		}
		result = &restored
	}
	if err != nil && len(names.internal) > 0 {
		if message := err.Error(); names.restoreText(message) != message {
			err = &quotedVariableError{cause: err, message: names.restoreText(message)}
		}
	}
	return result, err
}

// restoreColumn names a result column as the client's statement does: a
// variable's column is its name; an expression's column is its text as
// written.
func (names *quotedVariableNames) restoreColumn(column string) string {
	if internal, ok := names.internal[column]; ok {
		return internal.name
	}
	if isPlainVariableName(column) || !names.touches(column) {
		return column
	}
	// The final RETURN names the columns, so its text is the last
	// occurrence.
	if start := strings.LastIndex(names.canonical, column); start >= 0 {
		if original, ok := names.originalSpan(start, start+len(column)); ok {
			return original
		}
	}
	return names.restoreText(column)
}

// touches reports whether text holds a rewritten occurrence.
func (names *quotedVariableNames) touches(text string) bool {
	for identifier := range names.internal {
		if strings.Contains(text, identifier) {
			return true
		}
	}
	for _, edit := range names.edits {
		if strings.Contains(text, names.canonical[edit.canonStart:edit.canonEnd]) {
			return true
		}
	}
	return false
}

// originalSpan maps canonical[start:end] to the original text it came from,
// when both ends fall outside a rewritten occurrence or on its edges.
func (names *quotedVariableNames) originalSpan(start, end int) (string, bool) {
	mapOffset := func(offset int, isEnd bool) (int, bool) {
		shift := 0
		for _, edit := range names.edits {
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
	for _, identifier := range names.internalOrder {
		if strings.Contains(text, identifier) {
			text = strings.ReplaceAll(text, identifier, names.internal[identifier].written)
		}
	}
	return text
}

func (names *quotedVariableNames) restoreValue(value interface{}) interface{} {
	switch typed := value.(type) {
	case string:
		return names.restoreText(typed)
	case map[string]interface{}:
		out := make(map[string]interface{}, len(typed))
		for key, item := range typed {
			out[names.restoreText(key)] = names.restoreValue(item)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(typed))
		for i, item := range typed {
			out[i] = names.restoreValue(item)
		}
		return out
	case *ExecutionPlan:
		if typed == nil {
			return value
		}
		plan := *typed
		plan.Query = names.restoreStatementText(typed.Query)
		plan.Root, _ = names.restoreValue(typed.Root).(*PlanOperator)
		return &plan
	case *PlanOperator:
		if typed == nil {
			return value
		}
		operator := *typed
		operator.Description = names.restoreText(typed.Description)
		if typed.Arguments != nil {
			operator.Arguments = names.restoreValue(typed.Arguments).(map[string]interface{})
		}
		operator.Identifiers = make([]string, len(typed.Identifiers))
		for i, identifier := range typed.Identifiers {
			if internal, ok := names.internal[identifier]; ok {
				identifier = internal.name
			}
			operator.Identifiers[i] = identifier
		}
		operator.Children = make([]*PlanOperator, len(typed.Children))
		for i, child := range typed.Children {
			operator.Children[i], _ = names.restoreValue(child).(*PlanOperator)
		}
		return &operator
	case [][]interface{}:
		out := make([][]interface{}, len(typed))
		for i, row := range typed {
			out[i] = names.restoreValue(row).([]interface{})
		}
		return out
	}
	return value
}

func isExplainOrProfile(query string) bool {
	trimmed := strings.TrimSpace(query)
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
	if ctx == nil {
		return nil
	}
	names, _ := ctx.Value(quotedVariablesKey{}).(*quotedVariableNames)
	if names == nil || names.canonical != cypher {
		return nil
	}
	return names
}

// originalText maps canonical[start:end] back to the text the client wrote,
// or restores the internal names in it when the span can't be mapped.
func (names *quotedVariableNames) originalText(start, end int) string {
	if original, ok := names.originalSpan(start, end); ok {
		return original
	}
	return names.restoreText(names.canonical[start:end])
}

// finalReturnBody locates the body of text's last top-level RETURN (after
// the keyword).
func finalReturnBody(text string) (int, int, bool) {
	positions := findAllTopLevelPipelineKeywordPositions(text, "RETURN")
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
	start, end, ok := finalReturnBody(names.canonical)
	if !ok {
		return
	}
	body := strings.TrimSpace(names.canonical[start:end])
	if distinct, cut := cutDistinctKeyword(body); cut {
		body = distinct
	}
	if strings.HasPrefix(body, "*") && (len(body) == 1 || !isIdentifierPart(body[1])) && topLevelKeywordIndex(body, "UNION") < 0 {
		sortColumnsByName(result)
		return
	}
	items := parseItems(names.originalText(start, end))
	canonicalItems := parseItems(names.canonical[start:end])
	if len(items) != len(result.Columns) || len(canonicalItems) != len(items) {
		return
	}
	for i, item := range items {
		if item.alias != "" && item.alias != strings.TrimSpace(item.expr) {
			result.Columns[i] = normalizeProjectionColumnName(item.alias)
			continue
		}
		if name, isName := isOneSymbolicName(item.expr); isName {
			result.Columns[i] = name
			continue
		}
		result.Columns[i] = strings.TrimSpace(item.expr)
	}
}

// cutDistinctKeyword removes a leading DISTINCT keyword.
func cutDistinctKeyword(body string) (string, bool) {
	if startsWithKeywordFold(body, "DISTINCT") {
		return strings.TrimSpace(body[len("DISTINCT"):]), true
	}
	return body, false
}

func sortColumnsByName(result *ExecuteResult) {
	order := make([]int, len(result.Columns))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool { return result.Columns[order[a]] < result.Columns[order[b]] })
	columns := make([]string, len(order))
	for i, from := range order {
		columns[i] = result.Columns[from]
	}
	rows := make([][]interface{}, len(result.Rows))
	for r, row := range result.Rows {
		if len(row) != len(order) {
			return
		}
		rows[r] = make([]interface{}, len(order))
		for i, from := range order {
			rows[r][i] = row[from]
		}
	}
	result.Columns, result.Rows = columns, rows
}

// restoreStatementText maps statement text taken from the canonical
// statement (the whole statement or a part of it) back to the client's.
func (names *quotedVariableNames) restoreStatementText(text string) string {
	if text != "" {
		if start := strings.LastIndex(names.canonical, text); start >= 0 {
			return names.originalText(start, start+len(text))
		}
	}
	return names.restoreText(text)
}
