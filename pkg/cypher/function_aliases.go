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
// alias written as the function it names, and the rewrite that maps the
// result back, or query and nil when it calls none. A name is a call when '('
// follows it, and an alias only on its own: not a property (n.ln(), which
// isn't a call), a namespace part (x.ln) or a parameter ($ln).
func canonicalizeFunctionAliases(query string) (string, *queryRewrite) {
	if !mayCallFunctionAlias(query) {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	for index := 0; index < len(query); {
		c := query[index]
		switch {
		case c == '\'' || c == '"' || c == '`':
			index = skipCypherQuotedText(query, index, c)
			continue
		case c == '/':
			if end := queryCommentEnd(query, index); end >= 0 {
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
			end++
		}
		canonical, alias := cypherFunctionAliases[strings.ToLower(query[index:end])]
		if next := skipSpaces(query, end); !alias || next >= len(query) || query[next] != '(' {
			index = end
			continue
		}
		if rewrite == nil {
			rewrite = &queryRewrite{original: query}
			out.Grow(len(query) + 16)
		}
		out.WriteString(query[last:index])
		canonStart := out.Len()
		out.WriteString(canonical)
		rewrite.edits = append(rewrite.edits, queryTextEdit{origStart: index, origEnd: end, canonStart: canonStart, canonEnd: out.Len()})
		last = end
		index = end
	}
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// mayCallFunctionAlias is canonicalizeFunctionAliases's quick check: every
// alias but ln and ceiling has an underscore.
func mayCallFunctionAlias(query string) bool {
	return strings.IndexByte(query, '_') >= 0 || containsFold(query, "ln") || containsFold(query, "ceiling")
}
