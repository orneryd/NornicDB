// Package querytextmatrix supplies the same generated query population to
// Execute, Bolt and HTTP regression tests. It contains test data, not a parser.
package querytextmatrix

import (
	"fmt"
	"strings"
)

// MapBraceCase is one spelling of a map or CALL query. SyntaxError and Want
// describe the contract independently of any parser. Empty marks zero-row forms.
// Explain checks admission without comparing the servers' plan representations.
type MapBraceCase struct {
	Name, Query, Alias                 string
	Want                               interface{}
	Params                             map[string]interface{}
	SyntaxError, Empty, Explain, Write bool
}

// MapBraceCases visits every seed in ordinary, zero-row and EXPLAIN form. For
// each form it inserts every trivia spelling at each token boundary, and then
// at all boundaries together. Quoted tokens remain opaque. All result aliases
// are unique, including both sides of UNION, to exercise fresh syntax admission.
func MapBraceCases(visit func(MapBraceCase)) {
	seeds := []struct {
		name       string
		tokens     []string
		want       interface{}
		bad, write bool
	}{
		{"review_pattern_comprehension", []string{"RETURN", "[(a", "{", "bad", "}", ")-->(b) | a]", "AS", "out"}, nil, true, false},
		{"review_invalid_key", []string{"RETURN", "{", "1bad", ":", "1", "}", "AS", "out"}, nil, true, false},
		{"review_string_key_single", []string{"RETURN", "{", "'k'", ":", "1", "}", "AS", "out"}, nil, true, false},
		{"review_string_key_double", []string{"RETURN", "{", "\"k\"", ":", "1", "}", "AS", "out"}, nil, true, false},
		{"dollar_literal", []string{"RETURN", "{", "a$b", ":", "1", "}", "AS", "out"}, map[string]interface{}{"a$b": int64(1)}, false, false},
		{"dollar_property", []string{"WITH", "{", "a$b", ":", "1", "}", "AS", "n", "RETURN", "n", "{", ".", "a$b", "}", "AS", "out"}, map[string]interface{}{"a$b": int64(1)}, false, false},
		{"dollar_variable", []string{"WITH", "{", "}", "AS", "n", ",", "2", "AS", "a$b", "RETURN", "n", "{", "a$b", "}", "AS", "out"}, map[string]interface{}{"a$b": int64(2)}, false, false},
		{"quoted_dollar", []string{"WITH", "{", "`a$b`", ":", "1", "}", "AS", "`n$b`", "RETURN", "`n$b`", "{", ".", "`a$b`", "}", "AS", "out"}, map[string]interface{}{"a$b": int64(1)}, false, false},
		{"unicode_dollar", []string{"RETURN", "{", "é$中", ":", "1", "}", "AS", "out"}, map[string]interface{}{"é$中": int64(1)}, false, false},
		{"mixed_projection", []string{"WITH", "{", "a", ":", "1", "}", "AS", "n", ",", "2", "AS", "b", "RETURN", "n", "{", ".", "a", ",", "b", ",", "c", ":", "3", "}", "AS", "out"}, map[string]interface{}{"a": int64(1), "b": int64(2), "c": int64(3)}, false, false},
		{"keyword_projection", []string{"WITH", "{", "match", ":", "1", "}", "AS", "count", "RETURN", "count", "{", ".", "match", ",", "call", ":", "2", "}", "AS", "out"}, map[string]interface{}{"match": int64(1), "call": int64(2)}, false, false},
		{"nested_dollar", []string{"RETURN", "{", "outer", ":", "{", "a$b", ":", "1", "}", "}", "AS", "out"}, map[string]interface{}{"outer": map[string]interface{}{"a$b": int64(1)}}, false, false},
		{"call_write", []string{"CALL", "(", ")", "{", "CREATE", "(", "n", ":", "MapBraceMatrix", "{", "a$b", ":", "1", "}", ")", "RETURN", "n", ".", "a$b", "AS", "k", "}", "RETURN", "k", "AS", "out"}, int64(1), false, true},
		{"call_unit", []string{"CALL", "(", ")", "{", "CREATE", "(", "n", ":", "MapBraceMatrix", "{", "a$b", ":", "1", "}", ")", "}", "RETURN", "1", "AS", "out"}, int64(1), false, true},
		{"nested_call", []string{"CALL", "{", "CALL", "(", ")", "{", "RETURN", "{", "a$b", ":", "1", "}", "AS", "k", "}", "RETURN", "k", "}", "RETURN", "k", "AS", "out"}, map[string]interface{}{"a$b": int64(1)}, false, false},
		{"keyless_name", []string{"WITH", "1", "AS", "a", "RETURN", "{", "a", "}", "AS", "out"}, nil, true, false},
		{"keyless_property", []string{"RETURN", "{", ".", "a", "}", "AS", "out"}, nil, true, false},
		{"keyless_wildcard", []string{"RETURN", "{", ".", "*", "}", "AS", "out"}, nil, true, false},
		{"keyless_mixed", []string{"WITH", "1", "AS", "a", "RETURN", "{", "a", ",", "b", ":", "2", "}", "AS", "out"}, nil, true, false},
		{"nested_keyless", []string{"WITH", "1", "AS", "a", "RETURN", "{", "outer", ":", "{", "a", "}", "}", "AS", "out"}, nil, true, false},
		{"call_keyless", []string{"CALL", "{", "WITH", "1", "AS", "a", "RETURN", "{", "a", "}", "AS", "out", "}", "RETURN", "out"}, nil, true, false},
		{"union_keyless_first", []string{"WITH", "1", "AS", "a", "RETURN", "{", "a", "}", "AS", "out", "UNION", "RETURN", "1", "AS", "out"}, nil, true, false},
		{"union_keyless_last", []string{"RETURN", "1", "AS", "out", "UNION", "WITH", "1", "AS", "a", "RETURN", "{", "a", "}", "AS", "out"}, nil, true, false},
		{"comma_only", []string{"RETURN", "{", ",", "}", "AS", "out"}, nil, true, false},
		{"keyless_number", []string{"RETURN", "{", "1", "}", "AS", "out"}, nil, true, false},
		{"keyless_list", []string{"RETURN", "{", "[", "]", "}", "AS", "out"}, nil, true, false},
		{"keyless_map", []string{"RETURN", "{", "{", "}", "}", "AS", "out"}, nil, true, false},
		{"keyless_case", []string{"WITH", "1", "AS", "a", "RETURN", "CASE", "WHEN", "true", "THEN", "{", "a", "}", "ELSE", "{", "}", "END", "AS", "out"}, nil, true, false},
		{"empty_value", []string{"RETURN", "{", "a", ":", "}", "AS", "out"}, nil, true, false},
		{"trailing_comma", []string{"RETURN", "{", "a", ":", "1", ",", "}", "AS", "out"}, nil, true, false},
		{"exists_map", []string{"RETURN", "EXISTS", "{", "WITH", "{", "a", ":", "1", "}", "AS", "m", "RETURN", "m", "}", "AS", "out"}, true, false, false},
		{"count_keyless", []string{"WITH", "1", "AS", "a", "RETURN", "COUNT", "{", "RETURN", "{", "a", "}", "AS", "m", "}", "AS", "out"}, nil, true, false},
		{"collect_keyless", []string{"WITH", "1", "AS", "a", "RETURN", "COLLECT", "{", "RETURN", "{", "a", "}", "AS", "m", "}", "AS", "out"}, nil, true, false},
		{"pattern_keyless", []string{"MATCH", "(", "n", "{", "a", "}", ")", "RETURN", "n", "AS", "out"}, nil, true, false},
		{"pattern_selector", []string{"MATCH", "(", "n", "{", ".", "a", "}", ")", "RETURN", "n", "AS", "out"}, nil, true, false},
		{"relationship_keyless", []string{"MATCH", "(", "a", ")", "-", "[", "r", "{", "a", "}", "]", "->", "(", "b", ")", "RETURN", "r", "AS", "out"}, nil, true, false},
		{"relationship_quantifier", []string{"OPTIONAL", "MATCH", "(a)-[:MapBraceMatrixR]->", "{", "1", ",", "2", "}", "(b)", "RETURN", "a", "AS", "out"}, nil, false, false},
		{"group_quantifier", []string{"OPTIONAL", "MATCH", "((a)-[:MapBraceMatrixR]->(b))", "{", "1", ",", "2", "}", "RETURN", "a", "AS", "out"}, nil, false, false},
	}
	trivia := []struct{ name, text string }{
		{"space", " "}, {"tab", "\t"}, {"newline", "\n"}, {"crlf", "\r\n"},
		{"block", " /* gap ' ` // { CALL () : } */ "},
		{"line", " // gap ' ` /* { CALL () : }\n"},
		{"mixed", " \t/* gap */\r\n// gap\n "},
	}
	sequence := 0
	for _, seed := range seeds {
		for _, form := range []string{"rows", "zero_rows", "explain"} {
			tokens := make([]string, 0, len(seed.tokens)+6)
			zeroPrefix := []string{"UNWIND", "[", "]", "AS", "matrixEmpty"}
			if form == "explain" {
				tokens = append(tokens, "EXPLAIN")
			}
			if form == "zero_rows" {
				tokens = append(tokens, zeroPrefix...)
			}
			for _, token := range seed.tokens {
				tokens = append(tokens, token)
				if form == "zero_rows" && token == "UNION" {
					tokens = append(tokens, zeroPrefix...)
				}
			}
			for _, gap := range trivia {
				for boundary := -1; boundary+1 < len(tokens); boundary++ {
					alias := fmt.Sprintf("map_matrix_%d", sequence)
					sequence++
					var query strings.Builder
					for index, token := range tokens {
						if index > 0 {
							if boundary == -1 || boundary == index-1 {
								query.WriteString(gap.text)
							} else {
								query.WriteByte(' ')
							}
						}
						if token == "out" {
							query.WriteString(alias)
						} else {
							query.WriteString(token)
						}
					}
					spelling := "all_" + gap.name
					if boundary >= 0 {
						spelling = fmt.Sprintf("gap_%02d_%s", boundary, gap.name)
					}
					visit(MapBraceCase{Name: seed.name + "/" + form + "/" + spelling, Query: query.String(), Alias: alias, Want: seed.want, SyntaxError: seed.bad, Empty: form == "zero_rows", Explain: form == "explain", Write: seed.write && form == "rows"})
				}
			}
		}
	}
}
