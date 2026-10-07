package cypher

import (
	"strings"

	"golang.org/x/text/unicode/norm"
)

// Normalization predicates (#907): `x IS [NOT] [NFC | NFD | NFKC | NFKD]
// NORMALIZED`, NFC when no form is written. As in Neo4j 5.26, a string is
// tested against the form, null gives null, and so does a value of any other
// type: `3 IS NORMALIZED` is null, not an error.

// normalizationPredicateSuffixes are the predicate's spellings, longest
// first so " IS NOT NFC NORMALIZED" is not read as " NFC NORMALIZED".
var normalizationPredicateSuffixes = func() []struct {
	text    string
	negated bool
	form    norm.Form
} {
	var suffixes []struct {
		text    string
		negated bool
		form    norm.Form
	}
	for _, negated := range []bool{true, false} {
		for _, form := range []string{"NFKC", "NFKD", "NFC", "NFD", ""} {
			text := " IS "
			if negated {
				text += "NOT "
			}
			if form != "" {
				text += form + " "
			}
			named, _ := unicodeNormalForm(form)
			suffixes = append(suffixes, struct {
				text    string
				negated bool
				form    norm.Form
			}{text + "NORMALIZED", negated, named})
		}
	}
	return suffixes
}()

// splitNormalizationPredicate splits `operand IS [NOT] [form] NORMALIZED`.
func splitNormalizationPredicate(expr string) (operand string, negated bool, form norm.Form, ok bool) {
	if !hasSuffixFoldASCII(expr, "normalized") {
		return "", false, norm.NFC, false
	}
	for _, suffix := range normalizationPredicateSuffixes {
		if hasSuffixFoldASCII(expr, lowerASCII(suffix.text)) {
			return strings.TrimSpace(expr[:len(expr)-len(suffix.text)]), suffix.negated, suffix.form, true
		}
	}
	return "", false, norm.NFC, false
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
