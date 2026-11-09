package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"
)

// normalizeLiteralText returns the canonical form of a statement used as a
// cache key for the compile caches whose verdict cannot depend on literal
// values (semantic validation, merge-semantic validation, parser syntax
// validity). Two statements that differ only in literal values share one
// canonical form and therefore one compile verdict, which is what makes
// corpus-style statements with embedded ids hit instead of recompiling
// (PR #1043 write-path item #6, auto-parameterized literals).
//
// The form is never parsed or validated as text, so markers are bytes no
// Cypher statement can contain. Quoted strings are replaced as a whole,
// backticked names and comments are copied verbatim, existing $parameters
// collapse to a marker, dynamic label expressions ($( ... )) are copied
// verbatim, and everything else is preserved, so structurally different
// statements normalize differently:
//
//	CREATE (n:Node {id: 5})  -> CREATE (n:Node {id: \x00I})
//	CREATE (n:Node {id: 6})  -> CREATE (n:Node {id: \x00I})
//	CREATE (n:Node {id: 1.5})-> CREATE (n:Node {id: \x00F})
//	CREATE (n:Node {id: $p}) -> CREATE (n:Node {id: \x00P})
//	RETURN 'a' + 'b'         -> RETURN \x00S + \x00S
//	RETURN 'ab'              -> RETURN \x00S
//
// Returns changed=false when the statement holds nothing to normalize; the
// caller can then keep using the exact text as its cache key.
func normalizeLiteralText(s string) (canonical string, changed bool) {
	var out strings.Builder
	out.Grow(len(s))
	last := 0
	flushAt := func(end int) {
		out.WriteString(s[last:end])
		last = end
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if end := queryCommentEnd(s, i); end >= 0 {
			flushAt(end)
			i = end - 1
			continue
		}
		if isCypherBacktickByte(c) {
			end, _ := scanCypherQuotedText(s, i, c)
			flushAt(end)
			i = end - 1
			continue
		}
		if isCypherStringQuoteByte(c) {
			end, hasEscape, closed := cyphertext.ScanStringToken(s, i)
			flushAt(i)
			if !closed {
				flushAt(len(s))
				return out.String(), changed
			}
			if hasEscape {
				flushAt(end)
			} else {
				out.WriteString(normStringMarker)
				changed = true
				last = end
			}
			i = end - 1
			continue
		}
		switch {
		case c == '$':
			// $param: names are interchangeable for structural validity.
			if i+1 < len(s) && (isASCIIDigit(s[i+1]) || cyphertext.NamePartAt(s, i+1)) {
				j := i + 1
				for j < len(s) && (isASCIIDigit(s[j]) || cyphertext.NamePartAt(s, j)) {
					j++
				}
				flushAt(i)
				out.WriteString(normParamMarker)
				changed = true
				last = j
				i = j - 1
				continue
			}
			// $( ... ) dynamic label expressions are copied verbatim.
			if i+1 < len(s) && s[i+1] == '(' {
				closing := cyphertext.FindMatchingDelimiter(s, i+1, '(', ')', cyphertext.OpeningDelimiter)
				if closing > 0 {
					i = closing
					flushAt(i + 1)
					continue
				}
			}
		case cyphertext.NumberStartsAt(s, i):
			number, _ := cyphertext.ScanNumber(s, i)
			j := number.TailEnd
			flushAt(i)
			// Only normalize numbers whose value cannot change validity:
			// out-of-range integers and leading-zero forms keep their text
			// so their canonical form stays distinct from valid ones.
			if !number.Invalid && !number.Grouped && number.Suffix == 0 && normNumberSafe(s[i:j]) {
				if number.Fraction || number.Exponent {
					out.WriteString(normFloatMarker)
				} else {
					out.WriteString(normIntMarker)
				}
				changed = true
				last = j
			} else {
				last = i
			}
			i = j - 1
		}
	}
	flushAt(len(s))
	return out.String(), changed
}

// normNumberSafe reports whether a scanned numeric literal text is
// value-independent for validity: no leading-zero integer form and a value
// that parses in range.
func normNumberSafe(num string) bool {
	if len(num) == 0 {
		return false
	}
	hex := len(num) > 1 && num[0] == '0' && (num[1] == 'x' || num[1] == 'X')
	if !hex && len(num) > 1 && num[0] == '0' {
		// 01 / 00 / 0x handled above: leading-zero decimal forms are not
		// interchangeable with their value.
		return false
	}
	digits := num
	if hex {
		digits = num[2:]
	}
	if !strings.ContainsAny(num, ".eE") && !hex {
		// Integer: must fit int64.
		if len(digits) > 19 {
			return false
		}
		if len(digits) == 19 && digits > "9223372036854775807" {
			return false
		}
		return true
	}
	if hex {
		_, err := strconv.ParseInt(digits, 16, 64)
		return err == nil
	}
	_, err := strconv.ParseFloat(num, 64)
	return err == nil
}

const (
	normStringMarker = "\x00S"
	normIntMarker    = "\x00I"
	normFloatMarker  = "\x00F"
	normParamMarker  = "\x00P"
)
