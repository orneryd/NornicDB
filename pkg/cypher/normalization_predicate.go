package cypher

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Normalization predicates (#907): `x IS [NOT] [NFC | NFD | NFKC | NFKD]
// NORMALIZED`, NFC when no form is written. As in Neo4j 5.26, a string is
// tested against the form, null gives null, and so does a value of any other
// type: `3 IS NORMALIZED` is null, not an error.

// splitNormalizationPredicate splits `operand IS [NOT] [form] NORMALIZED`,
// with any whitespace between the words, as Cypher allows.
func splitNormalizationPredicate(expr string) (operand string, negated bool, form norm.Form, ok bool) {
	expr = strings.TrimSpace(expr)
	if !hasSuffixFoldASCII(expr, "normalized") {
		return "", false, norm.NFC, false
	}
	rest, word := lastPredicateWord(expr[:len(expr)-len("normalized")])
	form = norm.NFC
	if named, isForm := unicodeNormalForm(upperASCII(word)); isForm && word != "" {
		form = named
		rest, word = lastPredicateWord(rest)
	}
	if strings.EqualFold(word, "NOT") {
		negated = true
		rest, word = lastPredicateWord(rest)
	}
	if !strings.EqualFold(word, "IS") || rest == "" {
		return "", false, norm.NFC, false
	}
	return strings.TrimSpace(rest), negated, form, true
}

// lastPredicateWord splits off the last word of text, which must end with
// the whitespace separating that word from the next ('x'IS NORMALIZED needs
// none before IS). word is "" when text doesn't end with whitespace after a
// word.
func lastPredicateWord(text string) (rest, word string) {
	end := len(text)
	if end == 0 || !isASCIISpace(text[end-1]) {
		return text, ""
	}
	for end > 0 && isASCIISpace(text[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(text[start-1]) {
		start--
	}
	return text[:start], text[start:end]
}

// evaluateNormalizationPredicate is the predicate's value: a Boolean for a
// string, null for anything else.
func evaluateNormalizationPredicate(value interface{}, negated bool, form norm.Form) interface{} {
	text, isString := value.(string)
	if !isString {
		return nil
	}
	return form.IsNormalString(text) != negated
}
