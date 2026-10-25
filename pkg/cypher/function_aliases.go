package cypher

import "strings"

// GQL function aliases (Neo4j 2026.02): another name for a Cypher function,
// with its arguments, results and errors. canonicalizeFunctionAliases
// rewrites a call of one to the function it names, once, where a statement
// enters the executor, so every route, evaluator, aggregate check and
// catalog lookup reads one name. The column name stays the text the client
// wrote (RETURN collect_list(x) names its column collect_list(x)). Under
// Cypher 5 they are NornicDB extensions.
var cypherFunctionAliases = map[string]string{
	"ceiling":          "ceil",
	"ln":               "log",
	"local_time":       "localtime",
	"local_datetime":   "localdatetime",
	"zoned_time":       "time",
	"zoned_datetime":   "datetime",
	"duration_between": "duration.between",
	"path_length":      "length",
	"collect_list":     "collect",
	"percentile_cont":  "percentileCont",
	"percentile_disc":  "percentileDisc",
	"stdev_samp":       "stDev",
	"stdev_pop":        "stDevP",
}

// canonicalizeFunctionAliases returns query with each call of a GQL function
// alias written as the function it names, and the property key name of each
// PROPERTY_EXISTS(n, key) written as a string (PROPERTY_EXISTS(n, 'key')),
// so every evaluator and check reads a plain function call; and the rewrite
// that maps the result back, or query and nil when it has none. A name is a
// call when '(' follows it, and an alias only on its own: not a property
// (n.ln(), which isn't a call), a namespace part (x.ln) or a parameter ($ln).
func canonicalizeFunctionAliases(query string) (string, *queryRewrite) {
	if !mayCallFunctionAlias(query) {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	lexicalCallSpans(query, func(call functionCallSpan) bool {
		if call.procedure {
			return true
		}
		name := call.normalized
		start, stop, replacement := call.start, call.nameEnd, cypherFunctionAliases[name]
		if name == "property_exists" {
			start, stop, replacement = propertyExistsKeyName(query, call.open)
		}
		if replacement == "" {
			return true
		}
		if rewrite == nil {
			rewrite = &queryRewrite{original: query}
			out.Grow(len(query) + 16)
		}
		out.WriteString(query[last:start])
		canonStart := out.Len()
		out.WriteString(replacement)
		rewrite.edits = append(rewrite.edits, queryTextEdit{origStart: start, origEnd: stop, canonStart: canonStart, canonEnd: out.Len()})
		last = stop
		return true
	})
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// propertyExistsKeyName finds the key of PROPERTY_EXISTS(variable, key)
// whose parentheses open at open: its span and the key as a string literal,
// or an empty replacement when the call doesn't have that form.
func propertyExistsKeyName(query string, open int) (start, end int, literal string) {
	call, ok := functionCallBeforeParen(query, open)
	if !ok || call.close(query) < 0 {
		return 0, 0, ""
	}
	spans := call.syntax.argumentSpans(call.group)
	if len(spans) != 2 || simpleSemanticIdentifier(call.syntax.render(spans[0])) == "" {
		return 0, 0, ""
	}
	start = queryGapEnd(query, spans[1].start)
	key, end, ok := callableIdentifierToken(query, start)
	if !ok || queryGapEnd(query, end) != spans[1].end {
		return 0, 0, ""
	}
	return start, end, "'" + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `'`, `\'`) + "'"
}

// mayCallFunctionAlias is canonicalizeFunctionAliases's quick check, in one
// pass over the statement: every alias but ln and ceiling has an underscore,
// as has property_exists.
func mayCallFunctionAlias(query string) bool {
	for i := 0; i < len(query); i++ {
		if query[i] == '_' {
			return true
		}
		switch query[i] | 0x20 {
		case 'l':
			if i+1 < len(query) && query[i+1]|0x20 == 'n' {
				return true
			}
		case 'c':
			if len(query)-i >= len("ceiling") && strings.EqualFold(query[i:i+len("ceiling")], "ceiling") {
				return true
			}
		}
	}
	return false
}
