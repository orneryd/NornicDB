package cyphertext

import "testing"

// Author: #1663 implementation lane. Original lexical delimiter pairs share
// the Source endpoint projection used by brace records; generated syntax
// cannot supply an undeclared endpoint.
func TestDelimiterSpanUsesOriginalEndpointPair(t *testing.T) {
	root := NormalizeSource("RETURN id({x: 1})")
	view := root.RewriteWithCopies("RETURN coalesce(id({x: 1}), 2)", []SourceEdit{{Original: Span{7, 17}, Accepted: Span{7, 30}}}, []SourceCopy{{Original: Span{7, 17}, Accepted: Span{16, 26}}})
	for _, item := range []struct {
		open int
		want Span
	}{{18, Span{18, 26}}, {19, Span{19, 25}}} {
		span, found, err := view.DelimiterSpan(item.open)
		if err != nil || !found || span != item.want {
			t.Fatalf("open=%d: %v %v %v", item.open, span, found, err)
		}
	}
	if _, _, err := view.DelimiterSpan(15); err == nil {
		t.Fatal("generated parenthesis became an original endpoint")
	}
	removed := view.RewriteWithCopies("RETURN coalesce(id({x: 1}), 2)", []SourceEdit{{Original: Span{25, 26}, Accepted: Span{25, 26}}}, nil)
	if _, _, err := removed.DelimiterSpan(18); err == nil {
		t.Fatal("generated closing parenthesis substituted for original endpoint")
	}
}
