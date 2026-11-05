package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

type numericAdmissionPurpose uint8

const (
	numericValuePrefix numericAdmissionPurpose = iota
	numericConstantPopulation
)

// numericExpressionAccepted distinguishes the fast raw numeric-value prefix
// from the complete literal/operator population used by constant folding.
// Neither purpose is a grammar validator: malformed admitted expressions still
// receive the normal parser/evaluator diagnostic later.
func numericExpressionAccepted(expr string, purpose numericAdmissionPurpose) bool {
	if purpose == numericValuePrefix {
		if expr == "" {
			return false
		}
		first := expr[0]
		return cyphertext.IsDigit(first) || first == '-' && len(expr) > 1 && cyphertext.IsDigit(expr[1])
	}
	if purpose != numericConstantPopulation {
		return false
	}
	numbers := 0
	for i := 0; i < len(expr); {
		ch := expr[i]
		switch {
		case cyphertext.IsWhitespaceAt(expr, i) || ch == '(' || ch == ')' ||
			ch == '+' || ch == '-' || ch == '*' || ch == '/' || ch == '%' || ch == '^':
			i++
		// Signs are consumed above; numeric tokens share the canonical
		// lexical reader, with its broader leading-dot admission here.
		case cyphertext.IsDigit(ch) || ch == '.' && cyphertext.NumberStartsAt(expr, i):
			token, _ := cyphertext.ScanNumber(expr, i)
			if token.Invalid {
				return false
			}
			numbers++
			i = token.End
		default:
			return false
		}
	}
	return numbers > 0
}
