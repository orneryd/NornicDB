package cypher

import (
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strings"
)

// Numeric literals may group their digits with underscores, as in Neo4j 5
// (#907): 1_000, 1_000.5, 1.0_5, 1e1_0, 0x_1F, 0x1_F, 0o1_7. A single
// underscore may stand between two digits of the integer part, the fraction
// and the exponent, and right after the 0x / 0o prefix; never two in a row,
// last, or next to the dot or the e. The digits mean what they mean without
// the underscores, while the column name stays the text the client wrote
// (RETURN 1_000 names its column 1_000).
//
// canonicalizeNumericLiterals takes the underscores out of every literal
// that groups its digits that way, once, where a statement enters the
// executor, so every route and evaluator reads plain digits. A literal that
// breaks the rule (1__000, 1_, 1_e5, 0_1) keeps its text, and
// validateNumericLiterals rejects it as Neo4j does.

// canonicalizeNumericLiterals returns query with the digit-grouping
// underscores of its numeric literals removed and the rewrite that maps the
// result back, or query and nil when it has none.
func canonicalizeNumericLiterals(query string) (string, *queryRewrite) {
	if !mayHaveGroupedNumericLiteral(query) {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	for index := 0; index < len(query); {
		c := query[index]
		switch {
		case isCypherQuoteByte(c):
			index = skipCypherQuotedText(query, index, c)
			continue
		case c == '/':
			if end := queryCommentEnd(query, index); end >= 0 {
				index = end
				continue
			}
		}
		if !numericLiteralStartsAt(query, index) {
			index++
			continue
		}
		end, grouped := groupedNumericLiteralEnd(query, index)
		if grouped {
			if rewrite == nil {
				rewrite = &queryRewrite{Source: cyphertext.Source{Original: query}}
				out.Grow(len(query))
			}
			out.WriteString(query[last:index])
			canonStart := out.Len()
			out.WriteString(strings.ReplaceAll(query[index:end], "_", ""))
			rewrite.Edits = append(rewrite.Edits, cyphertext.SourceEdit{Original: cyphertext.Span{Start: index, End: end}, Accepted: cyphertext.Span{Start: canonStart, End: out.Len()}})
			last = end
		}
		index = end
	}
	if rewrite == nil {
		return query, nil
	}
	out.WriteString(query[last:])
	rewrite.Text = out.String()
	return rewrite.Text, rewrite
}

// mayHaveGroupedNumericLiteral reports whether query has an underscore at
// the end of a word that starts with a digit (1_000, 0x1F_E, .5_0): every
// grouped literal has one, and names (user_id, node_1_a) don't.
func mayHaveGroupedNumericLiteral(query string) bool {
	for index := strings.IndexByte(query, '_'); index >= 0; {
		start := index
		for start > 0 && cyphertext.NamePartAt(query, start-1) {
			start--
		}
		if start < index && isASCIIDigit(query[start]) {
			return true
		}
		next := strings.IndexByte(query[index+1:], '_')
		if next < 0 {
			return false
		}
		index += 1 + next
	}
	return false
}

// numericLiteralStartsAt reports whether a numeric literal starts at
// query[index]: a digit, or a dot before a digit, that doesn't continue a
// name or a parameter ($1_0 is the parameter named 1_0). The rewrite and
// validateNumericLiterals read literals from the same starts.
func numericLiteralStartsAt(query string, index int) bool {
	return cyphertext.NumberStartsAt(query, index)
}

// groupedNumericLiteralEnd returns the end of the numeric literal at
// query[start] (see numericLiteralStartsAt) and whether it groups its digits
// with underscores as Neo4j allows. A decimal integer with a leading zero
// (0_1, the legacy octal form) is not one.
func groupedNumericLiteralEnd(query string, start int) (int, bool) {
	token, ok := cyphertext.ScanNumber(query, start)
	if !ok {
		return start, false
	}
	valid := !token.Invalid && token.Grouped && token.Suffix == 0
	if token.Base == 10 && !token.Fraction && !token.Exponent && query[start] == '0' && token.IntegerEnd-start > 1 {
		valid = false
	}
	// Preserve the existing extension: uppercase radix prefixes accept digits,
	// while digit grouping is admitted only for the lowercase radix spellings.
	if token.Base != 10 && (query[start+1] == 'X' || query[start+1] == 'O') {
		valid = false
	}
	return token.TailEnd, valid
}
