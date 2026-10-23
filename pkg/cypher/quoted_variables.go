package cypher

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
 cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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
	return cyphertext.SymbolicNameValue(written)
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
	if len(name) < 2 || name[0] != '`' {
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
            if !isPlainVariableName(token.name){
                if !token.binding{continue}
                replacement=token.written+" AS "+replacementFor(token)
            }else{replacement=token.name}
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
// as a variable: an identifier that isn't a Cypher keyword, and without a
// '$', which only Cypher 5 reads inside a name (Cypher 25 starts a
// parameter there).
func isPlainVariableName(name string) bool {
	_, end, ok := scanIdentifierToken(name, 0)
	return ok && end == len(name) && isUnquotedName(name) && !isCypherKeyword(name) && strings.IndexByte(name, '$') < 0
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
func scanQuotedTokens(source string)[]quotedToken{
 syntax,err:=cypherantlr.ParseSyntax(context.Background(),source,cypherantlr.StatementEntry,false)
 if err!=nil{return nil} // Syntax admission owns this statement's error.
 var tokens []quotedToken
 syntax.Walk(func(node cypherantlr.SyntaxNode)func(){
  name,symbol:=node.Name()
  if !symbol || !strings.HasPrefix(node.Text(),"`"){return nil}
  start,end:=node.Span()
  token:=quotedToken{start:start,end:end,written:node.Text(),name:name}
  parent,ok:=node.Parent();if !ok{return nil}
  switch parent.Rule(){
  case cypherantlr.CypherParserRULE_atom,cypherantlr.CypherParserRULE_setItem,cypherantlr.CypherParserRULE_removeItem,cypherantlr.CypherParserRULE_callSubquery,cypherantlr.CypherParserRULE_mapProjection:
   token.variable=true
  case cypherantlr.CypherParserRULE_mapProjectionItem:
   token.variable,token.selector=true,true
  case cypherantlr.CypherParserRULE_nodePattern,cypherantlr.CypherParserRULE_relationDetail,cypherantlr.CypherParserRULE_patternPart,cypherantlr.CypherParserRULE_projectionItem,cypherantlr.CypherParserRULE_unwindSt,cypherantlr.CypherParserRULE_forSt,cypherantlr.CypherParserRULE_filterExpression,cypherantlr.CypherParserRULE_reduceExpression,cypherantlr.CypherParserRULE_allReduceExpression,cypherantlr.CypherParserRULE_foreachSt,cypherantlr.CypherParserRULE_letItem:
   token.variable,token.binding=true,true
  case cypherantlr.CypherParserRULE_yieldItem:
   first:=parent.ChildRule(cypherantlr.CypherParserRULE_symbol)
   _,aliased:=parent.DirectToken(cypherantlr.CypherLexerAS)
   if first.ID()==node.ID(){token.procColumn=true;token.binding=!aliased}else{token.variable,token.binding=true,true}
  case cypherantlr.CypherParserRULE_name:
   if owner,exists:=parent.Parent();exists && owner.Rule()==cypherantlr.CypherParserRULE_parameter{token.parameter=true}
  }
  tokens=append(tokens,token)
  return nil
 })
 return tokens
}
func previousWord(query string, index int) string {
	end := index
	for end > 0 && isWhitespace(query[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(query[start-1]) {
		start--
	}
	return query[start:end]
}

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
	body, _ = cutDistinct(body)
	if strings.HasPrefix(body, "*") && (len(body) == 1 || !isIdentByte(body[1])) && topLevelKeywordIndex(body, "UNION") < 0 {
		sortColumnsByName(result)
		return
	}
	items := parseItems(names.originalText(start, end))
	canonicalItems := parseItems(names.canonical[start:end])
	if len(items) != len(result.Columns) || len(canonicalItems) != len(items) {
		return
	}
	for i, item := range items {
		if item.alias != strings.TrimSpace(item.expr) {
			// parseItems gives the alias's value already: unquoting it again
			// would trim ` m` to m and read ```a``` as a.
			result.Columns[i] = item.alias
			continue
		}
		if name, isName := isOneSymbolicName(item.expr); isName {
			result.Columns[i] = name
			continue
		}
		result.Columns[i] = strings.TrimSpace(item.expr)
	}
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

// declaredAt reports whether the name written at offset at is the variable
// a declaration after text[from] declares, as the declaration readers read
// it: v IN … (bindingDeclaration, also FOREACH's) or p = (…)
// (namedPathDeclaration).
func declaredAt(text string, from, at int) bool {
	if _, declared, _, ok := bindingDeclaration(text, from); ok && declared == at {
		return true
	}
	_, declared, _, ok := namedPathDeclaration(text, from)
	return ok && declared == at
}
