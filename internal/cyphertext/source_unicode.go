package cyphertext

import (
	"context"
	"fmt"
	"strings"
	"unicode/utf8"
)

// SourceStage distinguishes client source from an already projected token.
// A generated backslash must never start a second Unicode expansion.
type SourceStage uint8

const (
	RawSource SourceStage = iota
	AcceptedSource
)

// UnicodeSourceError identifies a malformed lowercase-u source escape without
// retaining or exposing any query text. Offset is an original byte offset.
type UnicodeSourceError struct{ Offset int }

func (e *UnicodeSourceError) Error() string {
	return fmt.Sprintf("invalid Unicode escape at byte offset %d", e.Offset)
}

// ReadCodepoint reads one scalar or an unpaired UTF16 unit in canonical WTF8.
// Ordinary UTF8 retains its bytes; surrogate units occupy three bytes and one
// lexer position. No persisted identity is normalized by this reader.
func ReadCodepoint(text string, at int) (rune, int) {
	if at+3 <= len(text) && text[at] == 0xed && text[at+1] >= 0xa0 && text[at+1] <= 0xbf && text[at+2] >= 0x80 && text[at+2] <= 0xbf {
		return rune(text[at]&15)<<12 | rune(text[at+1]&63)<<6 | rune(text[at+2]&63), 3
	}
	return utf8.DecodeRuneInString(text[at:])
}

// AppendCodepoint appends a Unicode scalar or a literal unpaired UTF16 unit.
// Its surrogate form is internal text, never an implicit JSON/wire encoding.
func AppendCodepoint(dst []byte, value rune) []byte {
	if value >= 0xd800 && value <= 0xdfff {
		return append(dst, byte(0xe0|value>>12), byte(0x80|(value>>6)&63), byte(0x80|value&63))
	}
	return utf8.AppendRune(dst, value)
}

// ProjectUnicode expands raw lowercase-u escapes once, without applying any
// executor-specific whitespace or syntax-confusable transformations.
func ProjectUnicode(ctx context.Context, original string) *Source {
	source := &Source{Original: original, Text: original}
	if !strings.Contains(original, "\\u") {
		return source
	}
	var out strings.Builder
	last := 0
	nextPoll := 0
	for i := 0; i < len(original); {
		if i >= nextPoll {
			nextPoll = i + 1024
			if err := ctx.Err(); err != nil {
				source.Err = err
				return source
			}
		}
		if original[i] != '\\' || i+1 == len(original) {
			i++
			continue
		}
		if original[i+1] == '\\' {
			i += 2
			continue
		}
		if original[i+1] != 'u' {
			i += 2
			continue
		}
		value, ok := ParseUnicodeEscape(original, i+2, 4)
		if !ok {
			source.Err = &UnicodeSourceError{Offset: i}
			return source
		}
		end := i + 6
		if value >= 0xd800 && value <= 0xdbff && end+6 <= len(original) && original[end:end+2] == "\\u" {
			low, ok := ParseUnicodeEscape(original, end+2, 4)
			if ok && low >= 0xdc00 && low <= 0xdfff {
				value = 0x10000 + (value-0xd800)*0x400 + low - 0xdc00
				end += 6
			}
		}
		out.WriteString(original[last:i])
		start := out.Len()
		out.Write(AppendCodepoint(nil, rune(value)))
		source.Edits = append(source.Edits, SourceEdit{Original: Span{i, end}, Accepted: Span{start, out.Len()}})
		last = end
		i = end
	}
	if len(source.Edits) > 0 {
		out.WriteString(original[last:])
		source.Text = out.String()
	}
	return source
}

// CodepointBoundaries relates accepted lexer positions to accepted byte offsets.
// OriginalSpan then supplies the exact submitted source extent.
func (source *Source) CodepointBoundaries(ctx context.Context) ([]int, error) {
	var boundaries []int
	nextPoll := 0
	for i := 0; i < len(source.Text); {
		if i >= nextPoll {
			nextPoll = i + 1024
			if err := ctx.Err(); err != nil {
				return nil, err
			}
		}
		boundaries = append(boundaries, i)
		_, width := ReadCodepoint(source.Text, i)
		i += width
	}
	return append(boundaries, len(source.Text)), ctx.Err()
}

// EmitRawSource reverses the one-time Unicode source projection. Every
// accepted literal backslash is escaped globally, including inside comments;
// unpaired units receive their explicit source spelling. This is not a token
// heuristic and does not rename any stored identity.
func EmitRawSource(accepted string) string {
	if !strings.Contains(accepted, "\\") && utf8.ValidString(accepted) {
		return accepted
	}
	var out strings.Builder
	for i := 0; i < len(accepted); {
		value, width := ReadCodepoint(accepted, i)
		switch {
		case value == '\\':
			out.WriteString("\\u005c")
		case value >= 0xd800 && value <= 0xdfff:
			fmt.Fprintf(&out, "\\u%04x", value)
		default:
			out.WriteString(accepted[i : i+width])
		}
		i += width
	}
	return out.String()
}

// PrintIdentifier quotes one semantic identity. The caller retains its plain-name
// admission policy; raw output uses the one global source emitter, while
// accepted output never inserts another Unicode-preprocessing stage.
func PrintIdentifier(name string, allowPlain func(string) bool, stage SourceStage) string {
	if allowPlain != nil && allowPlain(name) {
		return name
	}
	var out strings.Builder
	out.WriteByte('`')
	for i := 0; i < len(name); {
		value, width := ReadCodepoint(name, i)
		if value == '`' {
			out.WriteString("``")
		} else {
			out.WriteString(name[i : i+width])
		}
		i += width
	}
	out.WriteByte('`')
	text := out.String()
	if stage == RawSource {
		return EmitRawSource(text)
	}
	return text
}
