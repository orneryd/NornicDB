package cypher

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// validateNumericLiterals performs the numeric part of lexical validation for
// both parser frontends. It scans the query text in place and deliberately
// ignores quoted text, quoted identifiers, and comments. Keeping this check at
// the shared compile boundary prevents individual executors from interpreting
// malformed or overflowing tokens differently.
func validateNumericLiterals(cypher string) error {
	upper := strings.ToUpper(strings.TrimSpace(cypher))
	if strings.HasPrefix(upper, "ALTER DATABASE ") && strings.Contains(upper, " SET LIMIT ") {
		return nil
	}
	for index := 0; index < len(cypher); {
		switch cypher[index] {
		case '\'', '"', '`':
			index = numericValidationSkipQuoted(cypher, index)
			continue
		case '/':
			if index+1 < len(cypher) && cypher[index+1] == '/' {
				index += 2
				for index < len(cypher) && cypher[index] != '\n' && cypher[index] != '\r' {
					index++
				}
				continue
			}
			if index+1 < len(cypher) && cypher[index+1] == '*' {
				index += 2
				for index+1 < len(cypher) && (cypher[index] != '*' || cypher[index+1] != '/') {
					index++
				}
				if index+1 < len(cypher) {
					index += 2
				}
				continue
			}
		}

		if !isASCIIDigit(cypher[index]) && !(cypher[index] == '.' && index+1 < len(cypher) && isASCIIDigit(cypher[index+1])) {
			index++
			continue
		}
		if index > 0 && isNumericIdentifierByte(cypher[index-1]) {
			index++
			continue
		}
		if index > 0 && cypher[index-1] == ':' {
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
	negative := numericLiteralHasUnaryMinus(cypher, start)
	end := start
	base := 10

	if cypher[start] == '.' {
		end++
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			end++
		}
		return validateFloatingLiteralTail(cypher, start, end)
	}

	if start+1 < len(cypher) && cypher[start] == '0' {
		switch cypher[start+1] {
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		}
	}
	if base != 10 {
		end = start + 2
		digitStart := end
		for end < len(cypher) && isASCIIAlphaNumeric(cypher[end]) {
			if !isDigitForBase(cypher[end], base) {
				return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end+1])
			}
			end++
		}
		if end == digitStart {
			return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
		}
		if end < len(cypher) && cypher[end] == '#' {
			return end, numericLiteralError("UnexpectedSyntax", cypher[start:end+1])
		}
		magnitude, parseErr := strconv.ParseUint(cypher[digitStart:end], base, 64)
		if parseErr != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
			return end, numericLiteralError("IntegerOverflow", cypher[start:end])
		}
		return end, nil
	}

	for end < len(cypher) && isASCIIDigit(cypher[end]) {
		end++
	}
	if end < len(cypher) && cypher[end] == '.' {
		end++
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			end++
		}
		return validateFloatingLiteralTail(cypher, start, end)
	}
	if end < len(cypher) && (cypher[end] == 'e' || cypher[end] == 'E') {
		return validateFloatingLiteralTail(cypher, start, end)
	}
	if end < len(cypher) && isASCIIIdentifierStart(cypher[end]) {
		invalidEnd := end + 1
		for invalidEnd < len(cypher) && isNumericIdentifierByte(cypher[invalidEnd]) {
			invalidEnd++
		}
		return invalidEnd, numericLiteralError("InvalidNumberLiteral", cypher[start:invalidEnd])
	}
	if end < len(cypher) && cypher[end] == '#' {
		return end, numericLiteralError("UnexpectedSyntax", cypher[start:end+1])
	}
	magnitude, parseErr := strconv.ParseUint(cypher[start:end], 10, 64)
	if parseErr != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
		return end, numericLiteralError("IntegerOverflow", cypher[start:end])
	}
	return end, nil
}

func validateFloatingLiteralTail(cypher string, start, end int) (int, error) {
	if end < len(cypher) && (cypher[end] == 'e' || cypher[end] == 'E') {
		end++
		if end < len(cypher) && (cypher[end] == '+' || cypher[end] == '-') {
			end++
		}
		exponentStart := end
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			end++
		}
		if end == exponentStart {
			return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
		}
	}
	if end < len(cypher) && isASCIIIdentifierStart(cypher[end]) {
		invalidEnd := end + 1
		for invalidEnd < len(cypher) && isNumericIdentifierByte(cypher[invalidEnd]) {
			invalidEnd++
		}
		return invalidEnd, numericLiteralError("InvalidNumberLiteral", cypher[start:invalidEnd])
	}
	if end < len(cypher) && cypher[end] == '#' {
		return end, numericLiteralError("UnexpectedSyntax", cypher[start:end+1])
	}
	value, parseErr := strconv.ParseFloat(cypher[start:end], 64)
	if math.IsInf(value, 0) {
		return end, numericLiteralError("FloatingPointOverflow", cypher[start:end])
	}
	if parseErr != nil {
		return end, numericLiteralError("InvalidNumberLiteral", cypher[start:end])
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
	for minus >= 0 && isASCIISpace(cypher[minus]) {
		minus--
	}
	if minus < 0 || cypher[minus] != '-' {
		return false
	}
	previous := minus - 1
	for previous >= 0 && isASCIISpace(cypher[previous]) {
		previous--
	}
	if previous < 0 {
		return true
	}
	if strings.ContainsRune("([{,:=<>+-*/%|&", rune(cypher[previous])) {
		return true
	}
	if !isASCIIIdentifierPart(cypher[previous]) {
		return false
	}
	wordEnd := previous + 1
	for previous >= 0 && isASCIIIdentifierPart(cypher[previous]) {
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

func numericValidationSkipQuoted(cypher string, start int) int {
	quote := cypher[start]
	for index := start + 1; index < len(cypher); index++ {
		if cypher[index] == '\\' {
			index++
			continue
		}
		if cypher[index] != quote {
			continue
		}
		if index+1 < len(cypher) && cypher[index+1] == quote {
			index++
			continue
		}
		return index + 1
	}
	return len(cypher)
}

func numericLiteralError(detail, literal string) error {
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		detail,
		fmt.Sprintf("invalid numeric literal %q", literal),
	)
}

func isASCIIDigit(value byte) bool { return value >= '0' && value <= '9' }

func isASCIIAlphaNumeric(value byte) bool {
	return isASCIIDigit(value) || isASCIIIdentifierStart(value)
}

func isASCIIIdentifierStart(value byte) bool {
	return value == '_' || value >= 'a' && value <= 'z' || value >= 'A' && value <= 'Z'
}

func isASCIIIdentifierPart(value byte) bool {
	return isASCIIIdentifierStart(value) || isASCIIDigit(value)
}

func isNumericIdentifierByte(value byte) bool {
	return isASCIIIdentifierPart(value) || value >= 0x80
}

func isDigitForBase(value byte, base int) bool {
	if isASCIIDigit(value) {
		return int(value-'0') < base
	}
	if base == 16 {
		return value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
	}
	return false
}
