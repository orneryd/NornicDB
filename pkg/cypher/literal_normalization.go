package cypher

import (
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
	return normalizeLiteralTextWithCancellation(s, nil)
}

func normalizeLiteralTextWithCancellation(s string, probe *pipelineCancellation) (canonical string, changed bool) {
	var out strings.Builder
	out.Grow(pipelineInitialCapacity(len(s), probe))
	last := 0
	flushAt := func(end int) bool {
		if !appendPipelineString(&out, s[last:end], probe) {
			return false
		}
		last = end
		return true
	}
	inSingle, inDouble, inBacktick := false, false, false
	inLine, inBlock := false, false
	// A string with an escape sequence is copied verbatim: its validity can
	// depend on the escape itself (\u escapes), so it must not share a
	// canonical form with a plain string.
	stringHasEscape := false
	for i := 0; i < len(s); i++ {
		if probe != nil && !probe.check() {
			return "", false
		}
		c := s[i]
		switch {
		case inLine:
			if c == '\n' || c == '\r' {
				inLine = false
				if !flushAt(i) {
					return "", false
				}
			}
		case inBlock:
			if c == '*' && i+1 < len(s) && s[i+1] == '/' {
				inBlock = false
				i++
				if !flushAt(i + 1) {
					return "", false
				}
			}
		case inSingle:
			if c == '\\' && i+1 < len(s) {
				stringHasEscape = true
				i++
				continue
			}
			if c == '\'' {
				// A doubled quote is no escape in Cypher: it ends the string
				// and opens another ('it''s' is two adjacent literals, which
				// the validator rejects), so it must not share a canonical
				// form with one string.
				inSingle = false
				if stringHasEscape {
					if !flushAt(i + 1) {
						return "", false
					}
				} else {
					if !appendPipelineString(&out, normStringMarker, probe) {
						return "", false
					}

					changed = true
					last = i + 1
				}
				stringHasEscape = false
			}
		case inDouble:
			if c == '\\' && i+1 < len(s) {
				stringHasEscape = true
				i++
				continue
			}
			if c == '"' {
				inDouble = false
				if stringHasEscape {
					if !flushAt(i + 1) {
						return "", false
					}
				} else {
					if !appendPipelineString(&out, normStringMarker, probe) {
						return "", false
					}

					changed = true
					last = i + 1
				}
				stringHasEscape = false
			}
		case inBacktick:
			if c == '`' {
				if i+1 < len(s) && s[i+1] == '`' {
					i++
					continue
				}
				inBacktick = false
				if !flushAt(i + 1) {
					return "", false
				}
			}
		default:
			switch {
			case c == '/' && i+1 < len(s):
				if s[i+1] == '/' {
					inLine = true
					i++
					if !flushAt(i - 1) {
						return "", false
					}
					continue
				}
				if s[i+1] == '*' {
					inBlock = true
					i++
					if !flushAt(i - 1) {
						return "", false
					}
					continue
				}
			case c == '\'':
				inSingle = true
				if !flushAt(i) {
					return "", false
				}
				continue
			case c == '"':
				inDouble = true
				if !flushAt(i) {
					return "", false
				}
				continue
			case c == '`':
				inBacktick = true
				if !flushAt(i) {
					return "", false
				}
				continue
			case c == '$':
				// $param: names are interchangeable for structural validity.
				if i+1 < len(s) && (isASCIIDigit(s[i+1]) || isIdentByte(s[i+1])) {
					j := i + 1
					for j < len(s) && (isASCIIDigit(s[j]) || isIdentByte(s[j])) {
						if probe != nil && !probe.check() {
							return "", false
						}
						j++
					}
					if !flushAt(i) {
						return "", false
					}
					if !appendPipelineString(&out, normParamMarker, probe) {
						return "", false
					}

					changed = true
					last = j
					i = j - 1
					continue
				}
				// $( ... ) dynamic label expressions are copied verbatim.
				if i+1 < len(s) && s[i+1] == '(' {
					closing := findMatchingDelimiter(s, i+1, '(', ')')
					if closing > 0 {
						i = closing
						if !flushAt(i + 1) {
							return "", false
						}
						continue
					}
				}
			case c >= '0' && c <= '9':
				if i > 0 && (isIdentByte(s[i-1]) || s[i-1] == '$' || s[i-1] == '`') {
					continue
				}
				j := i + 1
				if s[i] == '0' && j < len(s) && (s[j] == 'x' || s[j] == 'X') {
					// A bare 0x is not a number; only consume it as hex
					// when at least one hex digit follows.
					k := j + 1
					digits := 0
					for k < len(s) && isHexDigit(s[k]) {
						if probe != nil && !probe.check() {
							return "", false
						}
						k++
						digits++
					}
					if digits > 0 {
						j = k
					}
				}
				if j == i+1 {
					for j < len(s) && isASCIIDigit(s[j]) {
						if probe != nil && !probe.check() {
							return "", false
						}
						j++
					}
					if j < len(s) && s[j] == '.' && j+1 < len(s) && isASCIIDigit(s[j+1]) {
						j++
						for j < len(s) && isASCIIDigit(s[j]) {
							if probe != nil && !probe.check() {
								return "", false
							}
							j++
						}
					}
					if j < len(s) && (s[j] == 'e' || s[j] == 'E') {
						k := j + 1
						if k < len(s) && (s[k] == '+' || s[k] == '-') {
							k++
						}
						if k < len(s) && isASCIIDigit(s[k]) {
							j = k
							for j < len(s) && isASCIIDigit(s[j]) {
								if probe != nil && !probe.check() {
									return "", false
								}
								j++
							}
						}
					}
				}
				if !flushAt(i) {
					return "", false
				}
				// Only normalize numbers whose value cannot change validity:
				// out-of-range integers and leading-zero forms keep their text
				// so their canonical form stays distinct from valid ones.
				if normNumberSafe(s[i:j]) {
					if containsAnyWithCancellation(s[i:j], ".eE", probe) && !(len(s[i:j]) > 1 && (s[i:j][1] == 'x' || s[i:j][1] == 'X')) {
						if !appendPipelineString(&out, normFloatMarker, probe) {
							return "", false
						}

					} else {
						if !appendPipelineString(&out, normIntMarker, probe) {
							return "", false
						}

					}
					changed = true
					last = j
				} else {
					last = i
				}
				i = j - 1
			}
		}
	}
	if !flushAt(len(s)) {
		return "", false
	}
	if probe != nil && probe.err != nil {
		return "", false
	}
	return out.String(), changed
}

// normNumberSafe reports whether a scanned numeric literal text is
// value-independent for validity: no leading-zero integer form and a value
// that parses in range.
func normNumberSafe(num string) bool {
	return normNumberSafeWithCancellation(num, nil)
}

func normNumberSafeWithCancellation(num string, probe *pipelineCancellation) bool {
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
	if !containsAnyWithCancellation(num, ".eE", probe) && !hex {
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
		if digits == "" {
			return false
		}
		for len(digits) > 0 && digits[0] == '0' {
			if probe != nil && !probe.check() {
				return false
			}
			digits = digits[1:]
		}
		if digits == "" {
			return true
		}
		if len(digits) > 16 {
			return false
		}
		_, err := strconv.ParseInt(digits, 16, 64)
		return err == nil
	}
	return floatLiteralInRangeWithCancellation(num, probe)
}

func containsAnyWithCancellation(text, characters string, probe *pipelineCancellation) bool {
	return indexAnyWithCancellation(text, characters, probe) >= 0
}

// Range, rather than the rounded value, is the literal cache's concern. A
// decimal fallback retains 800 digits. Keep a larger bounded window and a
// sticky digit, preserving the original decimal-point form: converting an
// integer mantissa to a fractional one changes the fallback's range verdict
// for very long tokens near binary64 overflow. Only range is consumed here.
// The complete token still consumes the statement's probe.
func floatLiteralInRangeWithCancellation(num string, probe *pipelineCancellation) bool {
	if len(num) <= 4096 {
		if probe != nil && !probe.check() {
			return false
		}
		_, err := strconv.ParseFloat(num, 64)
		return err == nil
	}
	var digits strings.Builder
	sticky := false
	sawDot := false
	ordinal, first, before := 0, -1, -1
	exponent, exponentSign := 0, 1
	limit := len(num) + 1000
	for index := 0; index < len(num); index++ {
		if probe != nil && !probe.check() {
			return false
		}
		character := num[index]
		switch character {
		case '.':
			sawDot = true
			before = ordinal
		case 'e', 'E':
			if before < 0 {
				before = ordinal
			}
			index++
			if index < len(num) && (num[index] == '+' || num[index] == '-') {
				if num[index] == '-' {
					exponentSign = -1
				}
				index++
			}
			for ; index < len(num); index++ {
				if probe != nil && !probe.check() {
					return false
				}
				if exponent < limit {
					exponent = min(limit, exponent*10+int(num[index]-'0'))
				}
			}
		default:
			if first < 0 && character != '0' {
				first = ordinal
			}
			if first >= 0 {
				if digits.Len() < 4096 {
					if !appendPipelineByte(&digits, character, probe) {
						return false
					}

				} else if character != '0' {
					sticky = true
				}
			}
			ordinal++
		}
	}
	if first < 0 {
		return true
	}
	if before < 0 {
		before = ordinal
	}
	if sticky {
		if !appendPipelineByte(&digits, '1', probe) {
			return false
		}

	}
	coefficient := digits.String()
	point := digits.Len()
	if sawDot {
		point = max(1, min(before-first, digits.Len()))
		coefficient = coefficient[:point] + "." + coefficient[point:]
	}
	exponent = exponentSign*exponent + before - first - point
	_, err := strconv.ParseFloat(coefficient+"e"+strconv.Itoa(exponent), 64)
	return err == nil
}

const (
	normStringMarker = "\x00S"
	normIntMarker    = "\x00I"
	normFloatMarker  = "\x00F"
	normParamMarker  = "\x00P"
)

func isHexDigit(c byte) bool {
	return isASCIIDigit(c) || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')
}
