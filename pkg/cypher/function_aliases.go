package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

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
// call when '(' follows it, spaces or comments apart, and an alias only on
// its own: not a property (n.ln(), which isn't a call), a namespace part
// (x.ln) or a parameter ($ln). Backticks quote a name rather than spell data,
// so `ceiling`(1.2) calls ceiling as ceiling(1.2) does; strings and comments
// are data.
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
		case isCypherStringQuoteByte(c):
			index = skipCypherQuotedText(query, index, c)
			continue
		case c == '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end
				continue
			}
		}
		if !isCypherBacktickByte(c) && !cyphertext.NameStartAt(query, index) {
			index++
			continue
		}
		token, ok := cyphertext.ScanIdentifier(query, index)
		if !ok {
			index++
			continue
		}
		end, name := token.End, token.Value
		if !functionNameStartsAt(query, index) {
			index = end
			continue
		}
		next := queryGapEnd(query, end)
		if next >= len(query) || query[next] != '(' {
			index = end
			continue
		}
		lower := strings.ToLower(name)
		start, stop, replacement := index, end, cypherFunctionAliases[lower]
		if lower == "property_exists" {
			start, stop, replacement = propertyExistsKeyName(query, next)
		}
		if replacement == "" {
			index = end
			continue
		}
		if rewrite == nil {
			rewrite = &queryRewrite{Source: cyphertext.Source{Original: query}}
			out.Grow(len(query) + 16)
		}
		out.WriteString(query[last:start])
		canonStart := out.Len()
		out.WriteString(replacement)
		rewrite.Edits = append(rewrite.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: start, End: stop}, Accepted: cyphertext.Span{Start: canonStart, End: out.Len()}})
		last = stop
		index = stop
	}
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.Text = out.String()
	return rewrite.Text, rewrite
}

// propertyExistsKeyName finds the key of PROPERTY_EXISTS(variable, key)
// whose parentheses open at open: its span and the key as a string literal,
// or an empty replacement when the call doesn't have that form. Arguments
// may be separated by spaces or comments.
func propertyExistsKeyName(query string, open int) (start, end int, literal string) {
	after, ok := cyphertext.ScanIdentifier(query, queryGapEnd(query, open+1))
	comma := queryGapEnd(query, after.End)
	if !ok || comma >= len(query) || query[comma] != ',' {
		return 0, 0, ""
	}
	start = queryGapEnd(query, comma+1)
	key, ok := cyphertext.ScanIdentifier(query, start)
	if closing := queryGapEnd(query, key.End); !ok || closing >= len(query) || query[closing] != ')' {
		return 0, 0, ""
	}
	return start, key.End, "'" + strings.ReplaceAll(strings.ReplaceAll(key.Value, `\`, `\\`), `'`, `\'`) + "'"
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
