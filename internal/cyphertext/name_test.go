package cyphertext

import "testing"

func TestSymbolicNameValue(t *testing.T) {
	for _, sample := range []struct{ written, identity string }{
		{"plain", "plain"},
		{"`plain`", "plain"},
		{"`a,b`", "a,b"},
		{"`a``b`", "a`b"},
		{"`écho`", "écho"},
		{"``", ""},
		{"\"literal\"", "\"literal\""},
	} {
		if actual := SymbolicNameValue(sample.written); actual != sample.identity {
			t.Errorf("%q: decoded identity %q, want %q", sample.written, actual, sample.identity)
		}
	}
}
