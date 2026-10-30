package cyphertext

import "testing"

func TestSourceCopyProjectionWholeStatementReplacement(t *testing.T) {
	root := NormalizeSource("RETURN {x: 1}")
	text := "RETURN id({x: 1})"
	edits := []SourceEdit{{Original: Span{0, 13}, Accepted: Span{0, 17}}}
	legacy := root.Rewrite(text, edits)
	if got, valid := legacy.OriginalSpan(Span{10, 16}, SourceExtent); !valid || got != (Span{0, 13}) {
		t.Fatalf("legacy mapping changed: %+v", got)
	}
	view := root.RewriteWithCopies(text, edits, []SourceCopy{{Original: Span{0, 7}, Accepted: Span{0, 7}}, {Original: Span{7, 13}, Accepted: Span{10, 16}}})
	if got, valid := view.OriginalSpan(Span{10, 16}, SourceExtent); !valid || got != (Span{7, 13}) {
		t.Fatalf("inner brace mapped to replacement extent: %+v", got)
	}
	if got, valid := view.OriginalSpan(Span{11, 12}, SourceExtent); !valid || got != (Span{8, 9}) {
		t.Fatalf("inner token mapping: %+v", got)
	}
	if !view.Origin(Span{7, 10}).Generated {
		t.Fatal("wrapper has no explicit generated origin")
	}
}

func TestSourceCopyProjectionRebasesCachedRecipe(t *testing.T) {
	first := NormalizeSource("RETURN {x: 1}")
	second := NormalizeSource("RETURN ｛x: 1｝")
	recipe := first.RewriteWithCopies("RETURN id({x: 1})", []SourceEdit{{Original: Span{7, 13}, Accepted: Span{7, 17}}}, []SourceCopy{{Original: Span{7, 13}, Accepted: Span{10, 16}}}).Window(10, 16)
	view := recipe.Rebase(first, second)
	if got, valid := view.OriginalSpan(Span{1, 2}, SourceExtent); !valid || got != (Span{10, 11}) {
		t.Fatalf("cached recipe retained old origin: %v", got)
	}
	if view.BraceFacts() != second.BraceFacts() || view.BraceFacts() == first.BraceFacts() {
		t.Fatal("cached recipe retained old statement's record")
	}
}

// Author: #1663 implementation lane. Contract allocation: Nornic2.
func TestSourceCopyProjection(t *testing.T) {
	source := NormalizeSource("RETURN {x: 1}")
	text := "RETURN id({x: 1})"
	edits := []SourceEdit{{Original: Span{7, 13}, Accepted: Span{7, 17}}}
	copies := []SourceCopy{{Original: Span{7, 13}, Accepted: Span{10, 16}}}
	legacy := source.Rewrite(text, edits)
	if got, valid := legacy.OriginalSpan(Span{11, 12}, SourceExtent); !valid || got != (Span{7, 13}) {
		t.Fatalf("legacy replacement behavior changed: %v", got)
	}
	view := source.RewriteWithCopies(text, edits, copies)
	for _, item := range []struct{ accepted, original Span }{
		{Span{10, 16}, Span{7, 13}}, {Span{11, 12}, Span{8, 9}}, {Span{15, 16}, Span{12, 13}},
	} {
		if got, valid := view.OriginalSpan(item.accepted, SourceExtent); !valid || got != item.original {
			t.Errorf("%v mapped to %v, want %v", item.accepted, got, item.original)
		}
		if origin := view.Origin(item.accepted); !origin.Exact || origin.Generated || origin.Original != item.original {
			t.Errorf("copy origin: %+v", origin)
		}
	}
	if origin := view.Origin(Span{7, 10}); !origin.Generated || origin.Exact {
		t.Errorf("generated wrapper impersonated source: %+v", origin)
	}
	if origin := view.Origin(Span{7, 17}); !origin.Generated || origin.Exact {
		t.Errorf("mixed replacement impersonated one copy: %+v", origin)
	}
	window := view.Window(10, 16)
	if got, valid := window.OriginalSpan(Span{1, 2}, SourceExtent); !valid || got != (Span{8, 9}) {
		t.Errorf("window origin: %v", got)
	}
	// The published view owns its relations, independent of caller buffers.
	copies[0].Original = Span{0, 6}
	edits[0].Original = Span{0, 6}
	if got, valid := view.OriginalSpan(Span{11, 12}, SourceExtent); !valid || got != (Span{8, 9}) {
		t.Errorf("mutable relation escaped: %v", got)
	}
}

func TestSourceCopyProjectionAfterNormalization(t *testing.T) {
	source := NormalizeSource("RETURN ｛x: 1｝")
	view := source.RewriteWithCopies("RETURN id({x: 1})", []SourceEdit{{Original: Span{7, 13}, Accepted: Span{7, 17}}}, []SourceCopy{{Original: Span{7, 13}, Accepted: Span{10, 16}}})
	if got, valid := view.Window(10, 16).OriginalSpan(Span{1, 2}, SourceExtent); !valid || got != (Span{10, 11}) {
		t.Fatalf("copy bypassed the normalization owner: %v", got)
	}
}

func TestSourceCopyProjectionReorderedAndRepeated(t *testing.T) {
	source := NormalizeSource("left right")
	view := source.RewriteWithCopies("right + left + right", []SourceEdit{{Original: Span{0, 10}, Accepted: Span{0, 20}}}, []SourceCopy{
		{Original: Span{5, 10}, Accepted: Span{0, 5}},
		{Original: Span{0, 4}, Accepted: Span{8, 12}},
		{Original: Span{5, 10}, Accepted: Span{15, 20}},
	})
	for _, span := range []Span{{0, 5}, {15, 20}} {
		if got, valid := view.OriginalSpan(span, SourceExtent); !valid || got != (Span{5, 10}) {
			t.Errorf("%v -> %v", span, got)
		}
	}
	if got, valid := view.OriginalSpan(Span{8, 12}, SourceExtent); !valid || got != (Span{0, 4}) {
		t.Errorf("reordered: %v", got)
	}
}
