package cypher

import (
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"golang.org/x/text/unicode/norm"
)

// nornicdb.text.key, a NornicDB extension: a text's comparison key, so names
// written with other compatibility forms, case, digits or whitespace compare
// equal, computed per row without a digit map passed to the statement.
func init() {
	cypherfn.Register("nornicdb.text.key", fnTextKey)
}

// fnTextKey is nornicdb.text.key(input): textKey of a string, null for null;
// any other value is a type mismatch, as toLower's is.
func fnTextKey(ctx cypherfn.Context, args []string) (interface{}, error) {
	if len(args) != 1 {
		return nil, argumentCountError("nornicdb.text.key", "1", len(args))
	}
	values, err := evalArgs(ctx, args)
	if err != nil {
		return nil, err
	}
	text, ok, err := stringArgument("nornicdb.text.key", values, 0)
	if err != nil || !ok {
		return nil, err
	}
	return textKey(text), nil
}

// textKey is text's key, these steps in order, each as the Cypher function
// named computes it, so the key is the one the same steps written in Cypher
// give:
//
//  1. normalize(text, NFKC);
//  2. toLower (cypherfn.LowerCaseText: x/text's full lower case without a
//     language, so İ is i followed by U+0307, as Python's str.lower() has
//     it; Go's strings.ToLower maps one rune to one and gives a plain i);
//  3. replace ß with ss and ς (final sigma) with σ;
//  4. normalize(…, NFKC) again (lowering can leave a decomposed letter: Ϊ
//     with a combining acute, U+03AA U+0301, lowers to U+03CA U+0301, which
//     composes to ΐ, U+0390);
//  5. every decimal digit (category Nd) as its ASCII digit, its value as
//     Python's unicodedata.decimal gives it (٢٠٢٦ is 2026);
//  6. every run of whitespace as one space, as string.regexReplace reads
//     the class [\s\x{0b}\x{1c}-\x{1f}\x{85}\p{Z}] (keySpace), which is the
//     set Python's \s matches in a str pattern;
//  7. trim: unicode.IsSpace holds nothing outside that class, so only a
//     leading or trailing space is left to trim.
//
// The Unicode tables are Go's and x/text's for the toolchain the binary is
// built with (Unicode 15.0.0 for Go 1.26). A text that is ASCII is its key
// after one pass that only lowers letters and folds whitespace, without
// allocating when it is already a key.
func textKey(text string) string {
	for index := 0; index < len(text); index++ {
		if text[index] >= utf8.RuneSelf {
			lowered := cypherfn.LowerCaseText(norm.NFKC.String(text))
			lowered = strings.ReplaceAll(strings.ReplaceAll(lowered, "ß", "ss"), "ς", "σ")
			return spaceDigitKey(norm.NFKC.String(lowered))
		}
	}
	return asciiTextKey(text)
}

// asciiKeySpace is keySpace below utf8.RuneSelf: tab, line feed, vertical
// tab, form feed, carriage return, the information separators U+001C to
// U+001F and space.
var asciiKeySpace = [utf8.RuneSelf]bool{'\t': true, '\n': true, '\v': true, '\f': true, '\r': true,
	0x1c: true, 0x1d: true, 0x1e: true, 0x1f: true, ' ': true}

// keySpace reports whether r is in the regular expression class
// [\s\x{0b}\x{1c}-\x{1f}\x{85}\p{Z}] (Go's \s is ASCII's tab, line feed, form
// feed, carriage return and space): the ASCII ones (asciiKeySpace), U+0085
// and the separators (category Z: Zs, Zl and Zp).
func keySpace(r rune) bool {
	if r < utf8.RuneSelf {
		return asciiKeySpace[r]
	}
	return r == 0x85 || unicode.Is(unicode.Z, r)
}

// asciiTextKey is textKey of an ASCII text: NFKC leaves it as it is, toLower
// lowers only its capital letters, and it has neither ß, ς nor another
// script's digit, so its key is it lowered with its whitespace folded.
func asciiTextKey(text string) string {
	if asciiIsKey(text) {
		return text
	}
	var out strings.Builder
	out.Grow(len(text))
	space := false
	for index := 0; index < len(text); index++ {
		c := text[index]
		if asciiKeySpace[c] {
			space = out.Len() > 0
			continue
		}
		if space {
			out.WriteByte(' ')
			space = false
		}
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		out.WriteByte(c)
	}
	return out.String()
}

// asciiIsKey reports whether an ASCII text is its own key: no capital letter
// and no whitespace but single spaces between other characters.
func asciiIsKey(text string) bool {
	for index := 0; index < len(text); index++ {
		c := text[index]
		if 'A' <= c && c <= 'Z' {
			return false
		}
		if asciiKeySpace[c] && (c != ' ' || index == 0 || index == len(text)-1 || text[index-1] == ' ') {
			return false
		}
	}
	return true
}

// keyDigitZeros are the zeros of the decimal digits outside ASCII, in order.
// Unicode encodes the decimal digits (Numeric_Type=Decimal, category Nd) in
// contiguous runs of ten from zero to nine, so a digit's value is its
// distance from the zero before it.
var keyDigitZeros = decimalDigitZeros(unicode.Nd)

// decimalDigitZeros are the code points beyond ASCII of table's runs of ten:
// the first of each contiguous run and every tenth one after it.
func decimalDigitZeros(table *unicode.RangeTable) []rune {
	var zeros []rune
	next, run := rune(-1), 0
	visit := func(r rune) {
		if r != next {
			run = 0
		}
		if run%10 == 0 && r >= utf8.RuneSelf {
			zeros = append(zeros, r)
		}
		run, next = run+1, r+1
	}
	for _, span := range table.R16 {
		for r := rune(span.Lo); r <= rune(span.Hi); r += rune(span.Stride) {
			visit(r)
		}
	}
	for _, span := range table.R32 {
		for r := rune(span.Lo); r <= rune(span.Hi); r += rune(span.Stride) {
			visit(r)
		}
	}
	return zeros
}

// keyDigit is the ASCII digit of a decimal digit beyond ASCII.
func keyDigit(r rune) (byte, bool) {
	if r < utf8.RuneSelf {
		return 0, false
	}
	at, zero := slices.BinarySearch(keyDigitZeros, r)
	if zero {
		return '0', true
	}
	if at == 0 || r-keyDigitZeros[at-1] >= 10 {
		return 0, false
	}
	return byte('0' + r - keyDigitZeros[at-1]), true
}

// spaceDigitKey is steps 5 to 7 of textKey: digits as ASCII, whitespace
// folded, the text itself when that changes nothing. A byte that isn't
// valid UTF-8 stays as it is.
func spaceDigitKey(text string) string {
	if isSpaceDigitKey(text) {
		return text
	}
	var out strings.Builder
	out.Grow(len(text))
	space := false
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		if keySpace(r) {
			space = out.Len() > 0
			index += size
			continue
		}
		if space {
			out.WriteByte(' ')
			space = false
		}
		if digit, isDigit := keyDigit(r); isDigit {
			out.WriteByte(digit)
		} else {
			out.WriteString(text[index : index+size])
		}
		index += size
	}
	return out.String()
}

// isSpaceDigitKey reports whether spaceDigitKey leaves text as it is: no
// digit beyond ASCII and no whitespace but single spaces between other
// characters.
func isSpaceDigitKey(text string) bool {
	space := true
	for index := 0; index < len(text); {
		r, size := utf8.DecodeRuneInString(text[index:])
		if keySpace(r) {
			if r != ' ' || space {
				return false
			}
			space = true
		} else if _, isDigit := keyDigit(r); isDigit {
			return false
		} else {
			space = false
		}
		index += size
	}
	return !space || text == ""
}
