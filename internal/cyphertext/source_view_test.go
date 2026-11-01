// Author: Codex R17 implementation lane, 1 November 2026.
package cyphertext

import (
	"context"
	"testing"
)

func TestSourceViewFromComposedRewriteAndWindow(t *testing.T) {
	input := "  RETURN item.old AS value  "
	root := NewSource(context.Background(), input, AcceptedSource)
	window := root.Window(2, len(input)-2)
	current := window.Rewrite("RETURN item.newLong AS value", []SourceEdit{{Original: Span{7, 15}, Accepted: Span{7, 19}}})
	view, extent, ok := current.ViewFrom(input)
	if !ok || extent != (Span{2, len(input) - 2}) {
		t.Fatalf("input extent: %v, %v", extent, ok)
	}
	for _, tc := range []struct {
		span Span
		text string
	}{
		{Span{0, 2}, ""},
		{Span{2, 8}, "RETURN"},
		{Span{9, 17}, "item.newLong"},
		{Span{21, 26}, "value"},
		{Span{26, 28}, ""},
	} {
		text, valid := view.OriginalText(tc.span, SourceExtent, nil)
		if !valid || text != tc.text {
			t.Errorf("%v: got %q, %v; want %q", tc.span, text, valid, tc.text)
		}
	}
	if _, _, found := current.ViewFrom("RETURN unrelated"); found {
		t.Fatal("an unrelated spelling has no retained association")
	}
}
