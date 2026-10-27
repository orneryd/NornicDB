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
	offset := 0
	for current := 1; current < line; current++ {
		next := strings.IndexByte(source.Text[offset:], '\n')
		if next < 0 {
			return 0, 0, false
		}
		offset += next + 1
	}
	for ; column > 0 && offset < len(source.Text) && source.Text[offset] != '\n'; column-- {
		_, width := ReadCodepoint(source.Text, offset)
		offset += width
	}
	if column != 0 {
		return 0, 0, false
	}
	offset, _ = source.OriginalOffset(offset, true, SourceExtent)
	if offset > len(source.Original) {
		offset = len(source.Original)
	}
	originalLine, lineStart := 1, 0
	for index := 0; index < offset; index++ {
		if source.Original[index] == '\r' || source.Original[index] == '\n' && (index == 0 || source.Original[index-1] != '\r') {
			originalLine++
			lineStart = index + 1
		} else if source.Original[index] == '\n' && index > 0 && source.Original[index-1] == '\r' {
			lineStart = index + 1
		}
	}
	originalColumn := 0
	for i := lineStart; i < offset; {
		_, width := ReadCodepoint(source.Original, i)
		i += width
		originalColumn++
	}
	return originalLine, originalColumn, true
}
