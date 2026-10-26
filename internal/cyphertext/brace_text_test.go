package cyphertext

import "testing"

func TestBraceTextLiveSpans(t *testing.T) {
	for _, test := range []struct {
		text        string
		open, close int
	}{
		{"'{' /* } */ {k: '}'}", 12, 19},
		{"`{}`", -1, -1},
		{"// { }\n{}", 7, 8},
		{"{a: {b: 1}}", 0, 10},
	} {
		if got := FirstBrace(test.text, 0); got != test.open {
			t.Errorf("open %q: got %d want %d", test.text, got, test.open)
		}
		if got := LastSymbol(test.text, "}"); got != test.close {
			t.Errorf("close %q: got %d want %d", test.text, got, test.close)
		}
	}
}

func TestBraceTextDepthFacts(t *testing.T) {
	for _, text := range []string{"({k: [1, {x: 2}]})", "{k: '}'} // {", "'}'", "/* { */"} {
		reader := NewReader(text, 0)
		braces, all := BraceDepth(0), 0
		for token, ok := reader.Next(); ok; token, ok = reader.Next() {
			if token.Kind != Symbol {
				continue
			}
			braces.Advance(text[token.Start], false)
			all = AdvanceDelimiterDepth(all, text[token.Start], false)
			if braces < 0 || all < 0 {
				t.Fatalf("balanced fixture went negative: %s", text)
			}
		}
		if braces != 0 || all != 0 {
			t.Errorf("unbalanced result for %s: %d %d", text, braces, all)
		}
	}
	depth := BraceDepth(0)
	depth.Close(true)
	if depth != 0 {
		t.Fatal("clamped closer changed zero")
	}
	depth.Close(false)
	if depth != -1 || AdvanceDelimiterDepth(0, '}', false) != -1 || AdvanceDelimiterDepth(0, '}', true) != 0 {
		t.Fatal("unmatched closer lost its sign")
	}
}
