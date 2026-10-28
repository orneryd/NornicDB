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

// quotedVariableNames maps a canonicalized statement back to the one the
// client sent.
type quotedVariableNames struct {
	cyphertext.Source
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

// Fabric's planner and remote participants consume native Cypher names.
// Local fragments translate identifiers at the local execution boundary.
func canonicalizeLocalQuotedVariables(ctx context.Context, query string, fabricQuery bool) (string, *quotedVariableNames) {
	if fabricQuery {
		return query, nil
	}
	return canonicalizeQuotedVariablesWithBindings(ctx, query, nil)
}

// External row bindings have the same identifier roles as bindings declared
// in the query. The local query and its row keys are translated together.
func canonicalizeQuotedVariablesWithBindings(ctx context.Context, query string, bindings map[string]interface{}) (string, *quotedVariableNames) {
	if strings.IndexByte(query, '`') < 0 {
		return query, nil
	}
	tokens := scanQuotedTokens(ctx, query)
	if len(tokens) == 0 {
		return query, nil
	}
	bound := make(map[string]struct{})
	for name := range bindings {
		bound[name] = struct{}{}
	}
	for _, token := range tokens {
		if token.binding {
			bound[token.name] = struct{}{}
		}
	}

	names := &quotedVariableNames{Source: cyphertext.Source{Original: query}, internal: make(map[string]quotedVariableName)}
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
				if !token.binding {
					continue
				}
				replacement = token.written + " AS " + replacementFor(token)
			} else {
				replacement = token.name
			}
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
		if replacement == token.written {
			continue
		}
		out.WriteString(query[last:token.start])
		canonStart := out.Len()
		out.WriteString(replacement)
		names.Edits = append(names.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: token.start, End: token.end}, Accepted: cyphertext.Span{Start: canonStart, End: out.Len()}})
		last = token.end
	}
	if len(names.Edits) == 0 {
		return query, nil
	}
	out.WriteString(query[last:])
	names.Text = out.String()
	for identifier := range names.internal {
		names.internalOrder = append(names.internalOrder, identifier)
	}
	sort.Slice(names.internalOrder, func(i, j int) bool {
		if len(names.internalOrder[i]) != len(names.internalOrder[j]) {
			return len(names.internalOrder[i]) > len(names.internalOrder[j])
		}
		return names.internalOrder[i] < names.internalOrder[j]
	})
	return names.Text, names
}

func (names *quotedVariableNames) bindingValues(bindings map[string]interface{}) map[string]interface{} {
	if names == nil || len(bindings) == 0 {
		return bindings
	}
	var out map[string]interface{}
	for internal, original := range names.internal {
		if _, parameter := names.parameters[internal]; parameter {
			continue
		}
		value, exists := bindings[original.name]
		if !exists {
			continue
		}
		if out == nil {
			out = make(map[string]interface{}, len(bindings))
			for name, value := range bindings {
				out[name] = value
			}
		}
		delete(out, original.name)
		out[internal] = value
	}
	if out != nil {
		return out
	}
	return bindings
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
// as a variable: an identifier that isn't a Cypher keyword or a literal word
// (NaN and Infinity read as their float), and without a '$', which only
// Cypher 5 reads inside a name (Cypher 25 starts a parameter there).
func isPlainVariableName(name string) bool {
	end, ok := cyphertext.ScanIdentifier(name, 0)
	return ok && end.End == len(name) && isUnquotedName(name) && !isCypherKeyword(name) && !isLiteralKeyword(name) && strings.IndexByte(name, '$') < 0
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

// scanQuotedTokens classifies quoted names and non-plain variable identities
// in a quoted query using the same retained syntax walk.
func scanQuotedTokens(ctx context.Context, source string) []quotedToken {
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return nil
	} // Syntax admission owns this statement's error.
	var tokens []quotedToken
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		name, symbol := node.Name()
		parent, hasParent := node.Parent()
		if !symbol {
			return nil
		}
		scopeName := hasParent && parent.Rule() == cypherantlr.CypherParserRULE_callSubquery
		quoted := strings.HasPrefix(node.Text(), "`")
		start, end := node.Span()
		token := quotedToken{start: start, end: end, written: node.Text(), name: name}
		parent, ok := node.Parent()
		if !ok {
			return nil
		}
		switch parent.Rule() {
		case cypherantlr.CypherParserRULE_atom, cypherantlr.CypherParserRULE_setItem, cypherantlr.CypherParserRULE_removeItem, cypherantlr.CypherParserRULE_callSubquery, cypherantlr.CypherParserRULE_mapProjection:
			token.variable = true
		case cypherantlr.CypherParserRULE_mapProjectionItem:
			token.variable, token.selector = true, true
		case cypherantlr.CypherParserRULE_nodePattern, cypherantlr.CypherParserRULE_relationDetail, cypherantlr.CypherParserRULE_patternPart, cypherantlr.CypherParserRULE_lhs, cypherantlr.CypherParserRULE_projectionItem, cypherantlr.CypherParserRULE_unwindSt, cypherantlr.CypherParserRULE_forSt, cypherantlr.CypherParserRULE_filterExpression, cypherantlr.CypherParserRULE_reduceExpression, cypherantlr.CypherParserRULE_allReduceExpression, cypherantlr.CypherParserRULE_foreachSt, cypherantlr.CypherParserRULE_letItem:
			token.variable, token.binding = true, true
		case cypherantlr.CypherParserRULE_yieldItem:
			first := parent.ChildRule(cypherantlr.CypherParserRULE_symbol)
			_, aliased := parent.DirectToken(cypherantlr.CypherLexerAS)
			if first.ID() == node.ID() {
				token.procColumn = true
				token.binding = !aliased
			} else {
				token.variable, token.binding = true, true
			}
		case cypherantlr.CypherParserRULE_name:
			if owner, exists := parent.Parent(); exists && owner.Rule() == cypherantlr.CypherParserRULE_parameter {
				token.parameter = true
			}
		}
		if !quoted && !scopeName && (!token.variable || isPlainVariableName(name)) {
			return nil
		}
		tokens = append(tokens, token)
		return nil
	})
	return tokens
}
func isCypherKeyword(name string) bool {
	switch len(name) {
	case 2:
		return equalFoldASCII(name, "AS") ||
			equalFoldASCII(name, "BY") ||
			equalFoldASCII(name, "IN") ||
			equalFoldASCII(name, "IS") ||
			equalFoldASCII(name, "ON") ||
			equalFoldASCII(name, "OR")
	case 3:
		return equalFoldASCII(name, "ALL") ||
			equalFoldASCII(name, "AND") ||
			equalFoldASCII(name, "ANY") ||
			equalFoldASCII(name, "ASC") ||
			equalFoldASCII(name, "END") ||
			equalFoldASCII(name, "NOT") ||
			equalFoldASCII(name, "SET") ||
			equalFoldASCII(name, "USE") ||
			equalFoldASCII(name, "XOR")
	case 4:
		return equalFoldASCII(name, "CALL") ||
			equalFoldASCII(name, "CASE") ||
			equalFoldASCII(name, "DESC") ||
			equalFoldASCII(name, "ELSE") ||
			equalFoldASCII(name, "ENDS") ||
			equalFoldASCII(name, "NONE") ||
			equalFoldASCII(name, "NULL") ||
			equalFoldASCII(name, "SKIP") ||
			equalFoldASCII(name, "THEN") ||
			equalFoldASCII(name, "TRUE") ||
			equalFoldASCII(name, "WHEN") ||
			equalFoldASCII(name, "WITH")
	case 5:
		return equalFoldASCII(name, "FALSE") ||
			equalFoldASCII(name, "LIMIT") ||
			equalFoldASCII(name, "MATCH") ||
			equalFoldASCII(name, "MERGE") ||
			equalFoldASCII(name, "ORDER") ||
			equalFoldASCII(name, "UNION") ||
			equalFoldASCII(name, "WHERE") ||
			equalFoldASCII(name, "YIELD") ||
			equalFoldASCII(name, "COUNT")
	case 6:
		return equalFoldASCII(name, "CREATE") ||
			equalFoldASCII(name, "DELETE") ||
			equalFoldASCII(name, "DETACH") ||
			equalFoldASCII(name, "EXISTS") ||
			equalFoldASCII(name, "REMOVE") ||
			equalFoldASCII(name, "RETURN") ||
			equalFoldASCII(name, "SINGLE") ||
			equalFoldASCII(name, "STARTS") ||
			equalFoldASCII(name, "UNWIND") ||
			equalFoldASCII(name, "REDUCE")
	case 7:
		return equalFoldASCII(name, "FOREACH") ||
			equalFoldASCII(name, "COLLECT")
	case 8:
		return equalFoldASCII(name, "CONTAINS") ||
			equalFoldASCII(name, "DISTINCT") ||
			equalFoldASCII(name, "OPTIONAL")
	case 9:
		return equalFoldASCII(name, "ASCENDING")
	case 10:
		return equalFoldASCII(name, "DESCENDING")
	}
	return false
}

// restoreColumn names a result column as the client's statement does: a
// variable's column is its name; an expression's column is its text as
// written.
func (names *quotedVariableNames) restoreColumn(column string) string {
	if internal, ok := names.internal[column]; ok {
		return internal.name
	}
	if isPlainVariableName(column) || !names.HasRewrite(cyphertext.RewriteMatch{Policy: cyphertext.RewriteFragmentMention, Fragment: column, Markers: names.internalOrder}) {
		return column
	}
	// The final RETURN names the columns, so its text is the last
	// occurrence.
	if start := strings.LastIndex(names.Text, column); start >= 0 {
		if span, ok := names.OriginalSpan(cyphertext.Span{Start: start, End: start + len(column)}, cyphertext.SourceExactBoundaries); ok {
			return names.Original[span.Start:span.End]
		}
	}
	return names.restoreSourceText(column, quotedNamesOnly)
}

type quotedSourceTextPolicy uint8

const (
	quotedNamesOnly quotedSourceTextPolicy = iota
	quotedStatementFragment
)

// restoreSourceText keeps statement-fragment recovery and generated-name
// substitution on one owner. Only statement fragments acquire strict source
// span recovery; arbitrary metadata and error prose substitute names only.
func (names *quotedVariableNames) restoreSourceText(text string, policy quotedSourceTextPolicy) string {
	if policy == quotedStatementFragment && text != "" {
		if start := strings.LastIndex(names.Text, text); start >= 0 {
			originalText, _ := names.OriginalText(cyphertext.Span{Start: start, End: start + len(text)}, cyphertext.SourceExactBoundaries, func(text string) string { return names.restoreSourceText(text, quotedNamesOnly) })
			return originalText
		}
	}
	for _, identifier := range names.internalOrder {
		if strings.Contains(text, identifier) {
			text = strings.ReplaceAll(text, identifier, names.internal[identifier].written)
		}
	}
	return text
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
	if names == nil || names.Text != cypher {
		return nil
	}
	return names
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
