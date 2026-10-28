package jsontext

import (
	"bytes"
	"strconv"
	"unicode/utf8"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

type stringPolicy uint8

const (
	rawJSON stringPolicy = iota
	projectedJSON
	wireJSON
	opaqueJSON
)

type stringProjection struct {
	bytes []byte
	edits []cyphertext.SourceEdit
}

// projectJSONStrings owns lexical string projection in both directions. JSON
// grammar, escape validity, object structure and numbers remain parser-owned.
func projectJSONStrings(raw []byte, policy stringPolicy) stringProjection {
	// Most JSON contains neither escaped UTF16 units nor invalid UTF8. The
	// library validators already own its structure; avoid rescanning quotes.
	if utf8.Valid(raw) {
		if policy < wireJSON && !bytes.Contains(raw, []byte(`\u`)) {
			return stringProjection{bytes: raw}
		}
		if policy == opaqueJSON {
			return stringProjection{bytes: raw}
		}
		if policy == wireJSON && !bytes.Contains(raw, []byte("\u2028")) && !bytes.Contains(raw, []byte("\u2029")) {
			return stringProjection{bytes: raw}
		}
	}
	var out []byte
	var edits []cyphertext.SourceEdit
	var scratch [6]byte
	inside := false
	replace := func(start, end int, replacement []byte) {
		if out == nil {
			out = make([]byte, 0, len(raw))
			out = append(out, raw[:start]...)
		}
		at := len(out)
		out = append(out, replacement...)
		edits = append(edits, cyphertext.SourceEdit{Accepted: cyphertext.Span{Start: at, End: len(out)}, Original: cyphertext.Span{Start: start, End: end}})
	}
	for i := 0; i < len(raw); {
		if raw[i] == '"' {
			inside = !inside
			if out != nil {
				out = append(out, raw[i])
			}
			i++
			continue
		}
		if inside && raw[i] == '\\' && i+1 < len(raw) {
			if policy < wireJSON && raw[i+1] == 'u' && i+6 <= len(raw) {
				unit, err := strconv.ParseUint(string(raw[i+2:i+6]), 16, 16)
				if err == nil && unit >= 0xd800 && unit <= 0xdfff {
					if unit <= 0xdbff && i+12 <= len(raw) && bytes.Equal(raw[i+6:i+8], []byte(`\u`)) {
						low, e := strconv.ParseUint(string(raw[i+8:i+12]), 16, 16)
						if e == nil && low >= 0xdc00 && low <= 0xdfff {
							if out != nil {
								out = append(out, raw[i:i+12]...)
							}
							i += 12
							continue
						}
					}
					replace(i, i+6, cyphertext.AppendCodepoint(scratch[:0], rune(unit)))
					i += 6
					continue
				}
			}
			if out != nil {
				out = append(out, raw[i:i+2]...)
			}
			i += 2
			continue
		}
		if inside && raw[i] >= utf8.RuneSelf {
			// A codepoint needs at most four bytes, not a copy of the batch.
			value, width := cyphertext.ReadCodepoint(string(raw[i:min(i+utf8.UTFMax, len(raw))]), 0)
			surrogate := value >= 0xd800 && value <= 0xdfff
			if policy >= wireJSON && (surrogate || policy == wireJSON && (value == 0x2028 || value == 0x2029)) {
				replace(i, i+width, appendJSONUnit(scratch[:0], value))
				i += width
				continue
			}
			if surrogate && policy == rawJSON || value == utf8.RuneError && width == 1 {
				replace(i, i+1, []byte(`\ufffd`))
				i++
				continue
			}
			if out != nil {
				out = append(out, raw[i:i+width]...)
			}
			i += width
			continue
		}
		if out != nil {
			out = append(out, raw[i])
		}
		i++
	}
	if out == nil {
		out = raw
	}
	return stringProjection{bytes: out, edits: edits}
}

func appendJSONUnit(dst []byte, unit rune) []byte {
	const hex = "0123456789abcdef"
	return append(dst, '\\', 'u', hex[(unit>>12)&15], hex[(unit>>8)&15], hex[(unit>>4)&15], hex[unit&15])
}
