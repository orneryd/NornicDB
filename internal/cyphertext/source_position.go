package cyphertext

import (
	"strings"
)

// OriginalPosition maps an accepted parser position to its original position.
// Lines start at one and columns count Unicode codepoints from zero, not bytes
// or graphemes. The accepted parser counts LF; original CR and CRLF preserve
// the existing presentation contract. Replacement interiors use extent policy.
// For example, a collapsed CRLF/indentation run restores the following token's
// original line and indentation column instead of exposing its canonical offset.
func (source *Source) OriginalPosition(line, column int) (int, int, bool) {
	offset, valid := source.InputOffset(line, column)
	if !valid {
		return 0, 0, false
	}
	offset, _ = source.OriginalOffset(offset, true, SourceExtent)
	if offset > len(source.Original) {
		offset = len(source.Original)
	}
	line, column = sourcePosition(source.Original, offset, true)
	return line, column, true
}

// InputOffset resolves a parser's LF/codepoint position in this accepted view.
// Diagnostic adapters reuse it rather than implementing another line scanner.
func (source *Source) InputOffset(line, column int) (int, bool) {
	if line < 1 || column < 0 {
		return 0, false
	}
	offset := 0
	for current := 1; current < line; current++ {
		next := strings.IndexByte(source.Text[offset:], '\n')
		if next < 0 {
			return 0, false
		}
		offset += next + 1
	}
	for ; column > 0 && offset < len(source.Text) && source.Text[offset] != '\n'; column-- {
		_, width := ReadCodepoint(source.Text, offset)
		offset += width
	}
	if column != 0 {
		return 0, false
	}
	return offset, true
}

// InputPosition is the inverse parser-position observation at an accepted byte
// offset. Source projections and diagnostics share this coordinate owner.
func (source *Source) InputPosition(offset int) (int, int, bool) {
	if offset < 0 || offset > len(source.Text) {
		return 0, 0, false
	}
	line, column := sourcePosition(source.Text, offset, false)
	return line, column, true
}

func sourcePosition(text string, offset int, original bool) (int, int) {
	line, lineStart := 1, 0
	for index := 0; index < offset; index++ {
		if original && text[index] == '\r' || text[index] == '\n' && (!original || index == 0 || text[index-1] != '\r') {
			line++
			lineStart = index + 1
		} else if text[index] == '\n' && index > 0 && text[index-1] == '\r' {
			lineStart = index + 1
		}
	}
	column := 0
	for i := lineStart; i < offset; {
		_, width := ReadCodepoint(text, i)
		i += width
		column++
	}
	return line, column
}
