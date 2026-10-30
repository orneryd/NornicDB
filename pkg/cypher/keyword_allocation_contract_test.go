package cypher

import (
	"strings"
	"testing"
)

var keywordAllocationSink bool

func TestCypherKeywordAllocationFree(t *testing.T) {
	for _, name := range []string{"match", "return", "ordinary", "İN", "mAtCh"} {
		if n := testing.AllocsPerRun(100, func() { keywordAllocationSink = isCypherKeyword(name) }); n != 0 {
			t.Errorf("%q: allocations=%g", name, n)
		}
	}
}
func TestCypherKeywordASCIIParity(t *testing.T) {
	keywords := []string{"ALL", "AND", "ANY", "AS", "ASC", "ASCENDING", "BY", "CALL", "CASE", "CONTAINS", "CREATE", "DELETE", "DESC", "DESCENDING", "DETACH", "DISTINCT", "ELSE", "END", "ENDS", "EXISTS", "FALSE", "FOREACH", "IN", "IS", "LIMIT", "MATCH", "MERGE", "NONE", "NOT", "NULL", "ON", "OPTIONAL", "OR", "ORDER", "REMOVE", "RETURN", "SET", "SINGLE", "SKIP", "STARTS", "THEN", "TRUE", "UNION", "UNWIND", "USE", "WHEN", "WHERE", "WITH", "XOR", "YIELD", "COUNT", "COLLECT", "REDUCE"}
	cases := []string{"", "İN", "ın", "ſET", "K", "\xffmatch", "\xed\xa0\x80", "matché", "éMATCH"}
	for _, word := range keywords {
		for bits := 0; bits < (1 << len(word)); bits++ {
			v := []byte(word)
			for i := range v {
				if bits&(1<<i) != 0 {
					v[i] += 'a' - 'A'
				}
			}
			cases = append(cases, string(v))
		}
		cases = append(cases, "_"+word, word+"_", word+" ", " "+word, strings.ToLower(word)+"x")
	}
	for _, name := range cases {
		want := false
		upper := upperASCII(name)
		for _, word := range keywords {
			if upper == word {
				want = true
				break
			}
		}
		if got := isCypherKeyword(name); got != want {
			t.Errorf("%q got %v want %v", name, got, want)
		}
	}
	t.Logf("checked %d exact ASCII parity inputs", len(cases))
}
