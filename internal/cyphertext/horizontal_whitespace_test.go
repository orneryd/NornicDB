package cyphertext

import "testing"

func TestHorizontalASCIIWhitespaceKeepsLineBoundaries(t *testing.T) {
	for value := 0; value < 256; value++ {
		character := byte(value)
		want := character == ' ' || character == '\t'
		if got := IsHorizontalASCIIWhitespace(character); got != want {
			t.Fatalf("byte %02x: horizontal whitespace=%v, want %v", value, got, want)
		}
	}
	for _, text := range []string{"\n", "\r", "\u0085", "\u2028", "\u2029", "\u3000"} {
		if IsHorizontalASCIIWhitespace(text[0]) || WhitespaceAt(text, 0) != len(text) {
			t.Fatalf("full whitespace %q must retain its boundary/width outside the ASCII fast path", text)
		}
	}
}
