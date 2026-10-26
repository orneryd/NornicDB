package cypher

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"strconv"
	"strings"
)

// validateNumericLiterals performs the numeric part of lexical validation for
// both parser frontends. It scans the query text in place and deliberately
// ignores quoted text, quoted identifiers, and comments. Keeping this check at
// the shared compile boundary prevents individual executors from interpreting
// malformed or overflowing tokens differently.
func validateNumericLiterals(cypher string) error {
	upper := upperASCII(strings.TrimSpace(cypher))
	if strings.HasPrefix(upper, "ALTER DATABASE ") && strings.Contains(upper, " SET LIMIT ") {
		return nil
	}
	for index := 0; index < len(cypher); {
		if end, opaque := cyphertext.SkipToken(cypher, index); opaque {
			index = end
			continue
		}

		if !numericLiteralStartsAt(cypher, index) {
			index++
			continue
		}

		next, err := validateNumericLiteralAt(cypher, index)
		if err != nil {
			return err
		}
		index = next
	}
	return nil
}

func validateNumericLiteralAt(cypher string, start int) (int, error) {
	token, ok := cyphertext.ScanNumber(cypher, start)
	if !ok {
		return start + 1, numericLiteralError("InvalidNumberLiteral", cypher[start:start+1])
	}
	end := token.TailEnd
	if token.Invalid || token.Grouped || token.Suffix != 0 {
		if end < len(cypher) && cypher[end] == '.' && (end+1 == len(cypher) || cypher[end+1] != '.') {
			end++
		}
		return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
	}
	if end < len(cypher) && cypher[end] == '#' {
		return end, numericLiteralError("UnexpectedSyntax", cypher[start:end+1])
	}
	if token.Fraction || token.Exponent {
		value, err := strconv.ParseFloat(cypher[start:end], 64)
		if math.IsInf(value, 0) {
			return end, numericLiteralError("FloatingPointOverflow", cypher[start:end])
		}
		if err != nil {
			return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
		}
		return end, nil
	}
	if token.Base == 10 && cypher[start] == '0' && end-start > 1 {
		return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
	}
	magnitude, err := strconv.ParseUint(cypher[token.DigitsStart:end], token.Base, 64)
	if err != nil || numericMagnitudeOverflowsInt64(magnitude, numericLiteralHasUnaryMinus(cypher, start)) {
		return end, numericLiteralError("IntegerOverflow", cypher[start:end])
	}
	return end, nil
}

func numericMagnitudeOverflowsInt64(magnitude uint64, negative bool) bool {
	if negative {
		return magnitude > uint64(math.MaxInt64)+1
	}
	return magnitude > uint64(math.MaxInt64)
}

func numericLiteralHasUnaryMinus(cypher string, numberStart int) bool {
	minus := numberStart - 1
	for minus >= 0 && cyphertext.IsWhitespaceAt(cypher, minus) {
		minus--
	}
	if minus < 0 || cypher[minus] != '-' {
		return false
	}
	previous := minus - 1
	for previous >= 0 && cyphertext.IsWhitespaceAt(cypher, previous) {
		previous--
	}
	if previous < 0 {
		return true
	}
	if syntaxByteInSet("([{,:=<>+-*/%|&", cypher[previous]) {
		return true
	}
	if !cyphertext.NamePartAt(cypher, previous) {
		return false
	}
	wordEnd := previous + 1
	for previous >= 0 && cyphertext.NamePartAt(cypher, previous) {
		previous--
	}
	word := cypher[previous+1 : wordEnd]
	for _, keyword := range [...]string{"RETURN", "WITH", "AS", "WHERE", "WHEN", "THEN", "ELSE", "IN", "AND", "OR", "XOR", "NOT", "CASE"} {
		if strings.EqualFold(word, keyword) {
			return true
		}
	}
	return false
}

func numericLiteralError(detail, literal string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		detail,
		fmt.Sprintf("invalid numeric literal %q", literal),
	)
}

func isASCIIDigit(character byte) bool { return cyphertext.IsDigit(character) }
