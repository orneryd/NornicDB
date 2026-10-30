package cyphertext

import "testing"

func TestIdentifierExtentAndIdentity(t *testing.T) {
	for _, tc := range []struct {
		text, value string
		end         int
		quoted, ok  bool
	}{
		{"plain tail", "plain", 5, false, true}, {"日本 tail", "日本", 6, false, true},
		{"`日本` tail", "日本", 8, true, true}, {"`a``b` tail", "a`b", 6, true, true},
		{"``", "", 2, true, true}, {"`unclosed", "", 0, false, false}, {"1x", "", 0, false, false},
	} {
		span, quoted, ok := ScanIdentifierExtent(tc.text, 0)
		if span.Start != 0 || span.End != tc.end || quoted != tc.quoted || ok != tc.ok {
			t.Fatalf("extent %q: %+v %v %v", tc.text, span, quoted, ok)
		}
		decoded, decodedOK := ScanIdentifier(tc.text, 0)
		if decoded.Value != tc.value || decodedOK != tc.ok || decoded.End != tc.end || decoded.Quoted != tc.quoted {
			t.Fatalf("identity %q: %+v %v", tc.text, decoded, decodedOK)
		}
		if n := testing.AllocsPerRun(100, func() { ScanIdentifierExtent(tc.text, 0) }); n != 0 {
			t.Fatalf("extent %q allocated %g", tc.text, n)
		}
	}
}
