package cypher

import (
	"strings"
	"unicode"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"golang.org/x/text/cases"
)

// NornicDB extensions in Neo4j's string.* namespace, for comparing names and
// identifiers as keys (Personal Documents): with normalize(s, NFKC) and
// string.regexReplace they build a match key in Cypher. Neo4j 2026.09 has
// neither. Null in, null out; a non-string argument is the "Type mismatch"
// SyntaxError, as for the other string.* functions.
func init() {
	cypherfn.Register("string.casefold", fnStringCaseFold)
	cypherfn.Register("string.asciidigits", fnStringASCIIDigits)
}

// fnStringCaseFold is string.caseFold(input): Unicode full case folding,
// which toLower() isn't ('Straße' and 'STRASSE' both fold to 'strasse';
// final and medial sigma fold alike).
func fnStringCaseFold(ctx cypherfn.Context, args []string) (interface{}, error) {
	texts, null, err := stringArguments(ctx, "string.caseFold", args, 1)
	if err != nil || null {
		return nil, err
	}
	return cases.Fold().String(texts[0]), nil
}

// fnStringASCIIDigits is string.asciiDigits(input): every Unicode decimal
// digit (general category Nd: Arabic-Indic '٣', Devanagari '३', fullwidth
// '３', …) replaced by its ASCII digit; other characters are kept.
func fnStringASCIIDigits(ctx cypherfn.Context, args []string) (interface{}, error) {
	texts, null, err := stringArguments(ctx, "string.asciiDigits", args, 1)
	if err != nil || null {
		return nil, err
	}
	return strings.Map(asciiDigit, texts[0]), nil
}

// asciiDigit is r's ASCII digit when r is a decimal digit, else r. Unicode
// encodes decimal digits in contiguous runs of ten from zero (a stability
// policy), so a digit's value is its offset into its Nd range, modulo ten.
func asciiDigit(r rune) rune {
	if r >= '0' && r <= '9' || !unicode.Is(unicode.Nd, r) {
		return r
	}
	for _, span := range unicode.Nd.R16 {
		if lo, hi := rune(span.Lo), rune(span.Hi); r >= lo && r <= hi {
			return '0' + (r-lo)%10
		}
	}
	for _, span := range unicode.Nd.R32 {
		if lo, hi := rune(span.Lo), rune(span.Hi); r >= lo && r <= hi {
			return '0' + (r-lo)%10
		}
	}
	return r
}
