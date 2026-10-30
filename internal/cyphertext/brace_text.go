package cyphertext

import "strings"

// FirstBrace finds the next live opening brace. Quotes and comments stay
// opaque; callers needing its grammar use BraceScanner or VisitMapEntries.
func FirstBrace(text string, start int) int {
	reader := NewReader(text, start)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		if token.Is(text, "{") {
			return token.Start
		}
	}
	return -1
}

// FirstSymbol and LastSymbol locate live punctuation without interpreting
// contents of strings, comments, names or parameters as delimiters.
func FirstSymbol(text, symbol string) int {
	reader := NewReader(text, 0)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		if token.Kind == Symbol && token.Is(text, symbol) {
			return token.Start
		}
	}
	return -1
}
func LastSymbol(text, symbol string) int {
	last := -1
	reader := NewReader(text, 0)
	for token, ok := reader.Next(); ok; token, ok = reader.Next() {
		if token.Kind == Symbol && token.Is(text, symbol) {
			last = token.Start
		}
	}
	return last
}

// SyntaxByteInSet is the common punctuation-set lookup used by lexical
// boundary readers. Its input byte has already been established as live.
func SyntaxByteInSet(set string, c byte) bool { return strings.IndexByte(set, c) >= 0 }

// ContainsSyntaxAny is a conservative lexical fast-path screen. It does not
// assign a brace kind or parse entries; those decisions remain with the owner.
func ContainsSyntaxAny(text, set string) bool { return strings.ContainsAny(text, set) }

// IsOpenBrace and IsCloseBrace classify delimiter bytes already established
// as live by a lexical reader. They make no map/body/quantifier decision.
func IsOpenBrace[T ~byte | ~rune | ~int](c T) bool  { return c == '{' }
func IsCloseBrace[T ~byte | ~rune | ~int](c T) bool { return c == '}' }

// BraceDepth owns brace nesting for readers that already skip opaque tokens.
// Advance returns whether c is a brace, and preserves the reader's choice of
// whether an unmatched closer is clamped at zero.
type BraceDepth int

func (depth *BraceDepth) Open() { *depth++ }
func (depth *BraceDepth) Close(clamp bool) {
	if !clamp || *depth > 0 {
		*depth--
	}
}
func (depth *BraceDepth) Advance(c byte, clamp bool) bool {
	switch c {
	case '{':
		depth.Open()
		return true
	case '}':
		depth.Close(clamp)
		return true
	}
	return false
}

// AdvanceDelimiterDepth applies the shared delimiter event to combined
// nesting. It is used after a lexical reader has skipped opaque spans.
func AdvanceDelimiterDepth[T ~byte | ~rune | ~int](depth int, c T, clamp bool) int {
	delta := DelimiterDelta(c)
	if clamp && delta < 0 && depth == 0 {
		return depth
	}
	return depth + delta
}

// DelimiterDelta classifies nesting punctuation outside opaque tokens.
// Readers with a combined delimiter stack use the same brace vocabulary.
func DelimiterDelta[T ~byte | ~rune | ~int](c T) int {
	switch c {
	case '(', '[', '{':
		return 1
	case ')', ']', '}':
		return -1
	}
	return 0
}

// ClosingDelimiter returns an opener's matching delimiter, or zero.
func DelimiterCloser(c byte) byte {
	switch c {
	case '(':
		return ')'
	case '[':
		return ']'
	case '{':
		return '}'
	}
	return 0
}
