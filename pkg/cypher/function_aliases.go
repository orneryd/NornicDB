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
	return canonicalizeFunctionAliasesWithCancellation(query, nil)
}

func canonicalizeFunctionAliasesWithCancellation(query string, probe *pipelineCancellation) (canonical string, result *queryRewrite) {
	defer func() {
		if probe != nil && probe.err != nil {
			canonical, result = "", nil
		}
	}()
	if !mayCallFunctionAliasWithCancellation(query, probe) {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	for index := 0; index < len(query); {
		if probe != nil && !probe.check() {
			return "", nil
		}
		c := query[index]
		switch {
		case c == '\'' || c == '"' || c == '`':
			index = skipCypherQuotedTextWithCancellation(query, index, c, probe)
			continue
		case c == '/':
			if end := queryCommentEndWithCancellation(query, index, probe); end >= 0 {
				index = end
				continue
			}
		}
		if !isIdentByte(c) || isDigitByte(c) || index > 0 && (isIdentByte(query[index-1]) || query[index-1] == '.' || query[index-1] == '$') {
			index++
			continue
		}
		end := index + 1
		for end < len(query) && isIdentByte(query[end]) {
			if probe != nil && !probe.check() {
				return "", nil
			}
			end++
		}
		next := skipSpacesWithCancellation(query, end, probe)
		if next >= len(query) || query[next] != '(' {
			index = end
			continue
		}
		// The alias inventory has bounded ASCII names. A long identifier
		// cannot match it, so never hash that whole token in a native map.
		if end-index > len("duration_between") {
			index = end
			continue
		}
		name := lowerASCIIWithCancellation(query[index:end], probe)
		start, stop, replacement := index, end, cypherFunctionAliases[name]
		if name == "property_exists" {
			start, stop, replacement = propertyExistsKeyNameWithCancellation(query, next, probe)
		}
		if replacement == "" {
			index = end
			continue
		}
		if rewrite == nil {
			rewrite = &queryRewrite{original: query}
			out.Grow(pipelineInitialCapacity(len(query)+16, probe))
		}
		if !appendPipelineString(&out, query[last:start], probe) {
			return "", nil
		}
		canonStart := out.Len()
		if !appendPipelineString(&out, replacement, probe) {
			return "", nil
		}
		rewrite.edits = appendPipelineItem(rewrite.edits, queryTextEdit{origStart: start, origEnd: stop, canonStart: canonStart, canonEnd: out.Len()}, probe)
		if probe != nil && probe.err != nil {
			return "", nil
		}
		last = stop
		index = stop
	}
	if rewrite == nil {
		return query, nil
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return "", nil
	}
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// propertyExistsKeyName finds the key of PROPERTY_EXISTS(variable, key)
// whose parentheses open at open: its span and the key as a string literal,
// or an empty replacement when the call doesn't have that form.
func propertyExistsKeyName(query string, open int) (start, end int, literal string) {
	return propertyExistsKeyNameWithCancellation(query, open, nil)
}

func propertyExistsKeyNameWithCancellation(query string, open int, probe *pipelineCancellation) (start, end int, literal string) {
	_, after, ok := scanIdentifierTokenWithCancellation(query, skipSpacesWithCancellation(query, open+1, probe), probe)
	comma := skipSpacesWithCancellation(query, after, probe)
	if !ok || comma >= len(query) || query[comma] != ',' {
		return 0, 0, ""
	}
	start = skipSpacesWithCancellation(query, comma+1, probe)
	key, end, ok := scanIdentifierTokenWithCancellation(query, start, probe)
	if closing := skipSpacesWithCancellation(query, end, probe); !ok || closing >= len(query) || query[closing] != ')' {
		return 0, 0, ""
	}
	if probe == nil {
		return start, end, "'" + strings.ReplaceAll(strings.ReplaceAll(key, `\`, `\\`), `'`, `\'`) + "'"
	}
	key = replaceTextWithCancellation(key, `\`, `\\`, probe)
	if probe.err != nil {
		return 0, 0, ""
	}
	key = replaceTextWithCancellation(key, `'`, `\'`, probe)
	return start, end, concatPipelineStrings(concatPipelineStrings("'", key, probe), "'", probe)
}

// mayCallFunctionAlias is canonicalizeFunctionAliases's quick check, in one
// pass over the statement: every alias but ln and ceiling has an underscore,
// as has property_exists.
func mayCallFunctionAlias(query string) bool {
	return mayCallFunctionAliasWithCancellation(query, nil)
}

func mayCallFunctionAliasWithCancellation(query string, probe *pipelineCancellation) bool {
	for i := 0; i < len(query); i++ {
		if probe != nil && !probe.check() {
			return false
		}
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
