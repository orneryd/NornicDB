package antlr

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// Author: #1663 implementation lane. One selector grammar serves rewriting and
// original brace context; its keyword facts come from the lexical owner.
type PathPatternPrefix struct {
	Start, End  int
	Kind, Count string
	Groups      bool
	Mode        string
}

func ScanPathPatternPrefix(q string, i, end int) (PathPatternPrefix, bool, error) {
	prefix := PathPatternPrefix{Start: braceSkipSpaces(q, i, end)}
	at := prefix.Start
	next := func() (string, int) {
		k := braceSkipSpaces(q, at, end)
		j := k
		if j < end && q[j] == '$' {
			j++
		}
		for j < end && cyphertext.NamePartAt(q, j) {
			j++
		}
		return q[k:j], j
	}
	accept := func(words ...string) string {
		token, tokenEnd := next()
		for _, word := range words {
			if cyphertext.KeywordEnd(q, tokenEnd-len(token), word, cyphertext.CypherWord) == tokenEnd {
				at = tokenEnd
				return word
			}
		}
		return ""
	}
	acceptCount := func() bool {
		token, tokenEnd := next()
		if len(token) > 1 && token[0] == '$' || token != "" && strings.Trim(token, "0123456789") == "" {
			prefix.Count, at = token, tokenEnd
			return true
		}
		return false
	}
	// PATH or PATHS ends the prefix, but for SHORTEST k PATHS GROUPS.
	switch accept("ANY", "ALL", "SHORTEST") {
	case "ANY":
		if accept("SHORTEST") != "" {
			prefix.Kind, prefix.Count = "SHORTEST", "1"
		} else {
			prefix.Kind = "ANY"
			acceptCount()
		}
	case "ALL":
		if accept("SHORTEST") != "" {
			prefix.Kind, prefix.Count, prefix.Groups = "SHORTEST", "1", true
		} else {
			prefix.Kind = "ALL"
		}
	case "SHORTEST":
		prefix.Kind = "SHORTEST"
		counted := acceptCount()
		path := accept("PATH", "PATHS")
		prefix.Groups = accept("GROUP", "GROUPS") != ""
		if !counted && !prefix.Groups {
			return PathPatternPrefix{}, false, nil
		}
		if path != "" && !prefix.Groups {
			return prefix.finish(q, at, end)
		}
	}
	prefix.Mode = accept("WALK", "TRAIL", "ACYCLIC")
	accept("PATH", "PATHS")
	return prefix.finish(q, at, end)
}

// finish checks the prefix read up to query[at:end], where its pattern
// starts.
func (prefix PathPatternPrefix) finish(q string, at, end int) (PathPatternPrefix, bool, error) {
	if prefix.Kind == "" && prefix.Mode == "" {
		return PathPatternPrefix{}, false, nil
	}
	if name, ok := cyphertext.ScanIdentifier(q[:end], braceSkipSpaces(q, at, end)); ok && ((cyphertext.KeywordEnd(q, braceSkipSpaces(q, at, end), "shortestPath", cyphertext.CypherWord) == name.End) || (cyphertext.KeywordEnd(q, braceSkipSpaces(q, at, end), "allShortestPaths", cyphertext.CypherWord) == name.End)) {
		return PathPatternPrefix{}, false, bracePathSyntaxError(localization.CypherMatchingPathSelectorWithShortestPathFunction())
	}
	prefix.End = braceSkipSpaces(q, at, end)
	if prefix.End >= end || q[prefix.End] != '(' {
		return PathPatternPrefix{}, false, nil
	}
	if prefix.Count != "" && strings.Trim(prefix.Count, "0") == "" {
		if prefix.Groups {
			return PathPatternPrefix{}, false, bracePathSyntaxError(localization.CypherMatchingPathSelectorGroupCountNotPositive())
		}
		return PathPatternPrefix{}, false, bracePathSyntaxError(localization.CypherMatchingPathSelectorPathCountNotPositive())
	}
	return prefix, true, nil
}
