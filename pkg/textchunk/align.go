package textchunk

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

// Span is a chunk's [Start, End) byte offsets in the text it was cut from;
// -1, -1 for a chunk that could not be found there.
type Span struct {
	Start, End int
}

// Found reports whether the chunk was found in the text.
func (s Span) Found() bool { return s.Start >= 0 }

// Align finds where each chunk lies in text. Chunks come in text order and
// may overlap (a chunker's overlap), so each is looked for from just after
// the previous chunk's start, within a window a little longer than the
// chunk past where the previous one ended. A chunker moves forward, so a
// chunk ends past the previous one: an occurrence lying wholly inside the
// previous chunk (repetitive text repeated a few characters in) is skipped
// for the next one, and each chunk takes its own occurrence, in order. A chunker that returns the text
// verbatim (NornicDB's own; Voyage's contextualized auto-chunking, checked
// against its API) is matched exactly; one that trims or collapses
// whitespace is matched with whitespace ignored. A chunk found neither way
// is -1, -1, and the next is looked for from the same place.
func Align(text string, chunks []string) []Span {
	spans := make([]Span, len(chunks))
	from, previousEnd, skipped := 0, 0, 0
	for index, chunk := range chunks {
		spans[index] = Span{Start: -1, End: -1}
		if strings.TrimSpace(chunk) == "" || from >= len(text) {
			skipped += len(chunk)
			continue
		}
		windowEnd := min(len(text), max(previousEnd, from)+skipped+2*len(chunk)+64)
		span, ok := findChunk(text, chunk, from, windowEnd)
		for ok && index > 0 && span.End <= previousEnd {
			_, size := utf8.DecodeRuneInString(text[span.Start:])
			span, ok = findChunk(text, chunk, span.Start+size, windowEnd)
		}
		if !ok {
			skipped += len(chunk)
			continue
		}
		spans[index] = span
		_, size := utf8.DecodeRuneInString(text[span.Start:])
		from, previousEnd, skipped = span.Start+size, span.End, 0
	}
	return spans
}

// findChunk finds chunk in text[from:windowEnd], verbatim or else with
// whitespace ignored.
func findChunk(text, chunk string, from, windowEnd int) (Span, bool) {
	if span, ok := findExact(text, chunk, from, windowEnd); ok {
		return span, true
	}
	return findIgnoringWhitespace(text, chunk, from, windowEnd)
}

func findExact(text, chunk string, from, windowEnd int) (Span, bool) {
	at := strings.Index(text[from:windowEnd], chunk)
	if at < 0 {
		return Span{}, false
	}
	return Span{Start: from + at, End: from + at + len(chunk)}, true
}

// findIgnoringWhitespace matches chunk against text[from:windowEnd] with
// every whitespace rune removed from both, and maps the match back to the
// text: from its first non-space rune to the end of its last.
func findIgnoringWhitespace(text, chunk string, from, windowEnd int) (Span, bool) {
	needle := withoutWhitespace(chunk) // not empty: Align skips blank chunks
	var squashed strings.Builder
	positions := make([]int, 0, windowEnd-from)
	for offset, r := range text[from:windowEnd] {
		if unicode.IsSpace(r) {
			continue
		}
		for range utf8.RuneLen(r) { // as WriteRune writes it (U+FFFD for an invalid byte)
			positions = append(positions, from+offset)
		}
		squashed.WriteRune(r)
	}
	at := strings.Index(squashed.String(), needle)
	if at < 0 {
		return Span{}, false
	}
	last := positions[at+len(needle)-1]
	_, size := utf8.DecodeRuneInString(text[last:])
	return Span{Start: positions[at], End: last + size}, true
}

func withoutWhitespace(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsSpace(r) {
			return -1
		}
		return r
	}, text)
}
