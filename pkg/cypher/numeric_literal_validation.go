package cypher

import (
	math "github.com/orneryd/nornicdb/pkg/math/libm"
	"reflect"
	"strconv"
	"strings"
)

// validateNumericLiterals performs the numeric part of lexical validation for
// both parser frontends. It scans the query text in place and deliberately
// ignores quoted text, quoted identifiers, and comments. Keeping this check at
// the shared compile boundary prevents individual executors from interpreting
// malformed or overflowing tokens differently.
func validateNumericLiterals(cypher string) error {
	return validateNumericLiteralsWithCancellation(cypher, nil)
}
func validateNumericLiteralsWithCancellation(cypher string, probe *pipelineCancellation) (err error) {
	defer func() {
		if probe != nil && probe.err != nil {
			err = probe.err
		}
	}()
	if probe != nil && !probe.check() {
		return probe.err
	}

	upper := upperASCII(trimSpaceWithCancellation(cypher, probe))
	if strings.HasPrefix(upper, "ALTER DATABASE ") && containsTextWithCancellation(upper, " SET LIMIT ", probe) {
		return nil
	}
	for index := 0; index < len(cypher); {
		if probe != nil && !probe.check() {
			return probe.err
		}

		switch cypher[index] {
		case '\'', '"', '`':
			index = numericValidationSkipQuoted(cypher, index)
			continue
		case '/':
			if index+1 < len(cypher) && cypher[index+1] == '/' {
				index += 2
				for index < len(cypher) && cypher[index] != '\n' && cypher[index] != '\r' {
					if probe != nil && !probe.check() {
						return probe.err
					}

					index++
				}
				continue
			}
			if index+1 < len(cypher) && cypher[index+1] == '*' {
				index += 2
				for index+1 < len(cypher) && (cypher[index] != '*' || cypher[index+1] != '/') {
					if probe != nil && !probe.check() {
						return probe.err
					}

					index++
				}
				if index+1 < len(cypher) {
					index += 2
				}
				continue
			}
		}

		if !numericLiteralStartsAt(cypher, index) {
			index++
			continue
		}

		next, err := validateNumericLiteralAtWithCancellation(cypher, index, probe)
		if err != nil {
			return err
		}
		index = next
	}
	return nil
}

func validateNumericLiteralAt(cypher string, start int) (int, error) {
	return validateNumericLiteralAtWithCancellation(cypher, start, nil)
}
func validateNumericLiteralAtWithCancellation(cypher string, start int, probe *pipelineCancellation) (int, error) {
	if probe != nil && !probe.check() {
		return 0, probe.err
	}

	negative := numericLiteralHasUnaryMinus(cypher, start)
	end := start
	base := 10

	if cypher[start] == '.' {
		end++
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			end++
		}
		return validateFloatingLiteralTailWithCancellation(cypher, start, end, probe)
	}

	if start+1 < len(cypher) && cypher[start] == '0' {
		switch cypher[start+1] {
		// The uppercase prefixes 0X and 0O are a NornicDB extension: Neo4j 5
		// takes only 0x and 0o, but the value is unambiguous, so NornicDB
		// keeps accepting them, as before (#907, at the owner's direction).
		// Digit grouping is for the lowercase forms (groupedNumericLiteralEnd).
		case 'x', 'X':
			base = 16
		case 'o', 'O':
			base = 8
		}
	}
	if base != 10 {
		end = start + 2
		digitStart := end
		for end < len(cypher) && isIdentByte(cypher[end]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			if !isDigitForBase(cypher[end], base) {
				return end, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end+1], probe)
			}
			end++
		}
		if end == digitStart {
			return end, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end], probe)
		}
		if end < len(cypher) && cypher[end] == '#' {
			return end, numericLiteralErrorWithCancellation("UnexpectedSyntax", cypher[start:end+1], probe)
		}
		magnitude, parseErr := parseUintLiteralWithCancellation(cypher[digitStart:end], base, probe)
		if parseErr != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
			return end, numericLiteralErrorWithCancellation("IntegerOverflow", cypher[start:end], probe)
		}
		return end, nil
	}

	for end < len(cypher) && isASCIIDigit(cypher[end]) {
		if probe != nil && !probe.check() {
			return 0, probe.err
		}

		end++
	}
	// 5. is not a number: Neo4j reads the dot as a property access, so
	// RETURN 5. AS v and 5.e3 are SyntaxErrors. In 1..2 (a range) the
	// integer ends at the dots.
	if end < len(cypher) && cypher[end] == '.' && (end+1 >= len(cypher) || (!isASCIIDigit(cypher[end+1]) && cypher[end+1] != '.')) {
		return end + 1, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end+1], probe)
	}
	if end+1 < len(cypher) && cypher[end] == '.' && cypher[end+1] != '.' {
		end++
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			end++
		}
		return validateFloatingLiteralTailWithCancellation(cypher, start, end, probe)
	}
	if end < len(cypher) && (cypher[end] == 'e' || cypher[end] == 'E') {
		return validateFloatingLiteralTailWithCancellation(cypher, start, end, probe)
	}
	if end < len(cypher) && isIdentStartByte(cypher[end]) {
		invalidEnd := end + 1
		for invalidEnd < len(cypher) && isIdentByte(cypher[invalidEnd]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			invalidEnd++
		}
		return invalidEnd, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:invalidEnd], probe)
	}
	if end < len(cypher) && cypher[end] == '#' {
		return end, numericLiteralErrorWithCancellation("UnexpectedSyntax", cypher[start:end+1], probe)
	}
	// 01 is the legacy octal form, which Neo4j 5 rejects (#907). It stays
	// rejected, unlike the uppercase prefixes: Cypher before 5 read 010 as
	// octal 8, and NornicDB read it as decimal 10, so accepting it would give
	// a statement written for that Cypher a different value.
	if cypher[start] == '0' && end-start > 1 {
		return end, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end], probe)
	}
	magnitude, parseErr := parseUintLiteralWithCancellation(cypher[start:end], 10, probe)
	if parseErr != nil || numericMagnitudeOverflowsInt64(magnitude, negative) {
		return end, numericLiteralErrorWithCancellation("IntegerOverflow", cypher[start:end], probe)
	}
	return end, nil
}

func validateFloatingLiteralTail(cypher string, start, end int) (int, error) {
	return validateFloatingLiteralTailWithCancellation(cypher, start, end, nil)
}
func validateFloatingLiteralTailWithCancellation(cypher string, start, end int, probe *pipelineCancellation) (int, error) {
	if probe != nil && !probe.check() {
		return 0, probe.err
	}

	if end < len(cypher) && (cypher[end] == 'e' || cypher[end] == 'E') {
		end++
		if end < len(cypher) && (cypher[end] == '+' || cypher[end] == '-') {
			end++
		}
		exponentStart := end
		for end < len(cypher) && isASCIIDigit(cypher[end]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			end++
		}
		if end == exponentStart {
			return end, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end], probe)
		}
	}
	if end < len(cypher) && isIdentStartByte(cypher[end]) {
		invalidEnd := end + 1
		for invalidEnd < len(cypher) && isIdentByte(cypher[invalidEnd]) {
			if probe != nil && !probe.check() {
				return 0, probe.err
			}

			invalidEnd++
		}
		return invalidEnd, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:invalidEnd], probe)
	}
	if end < len(cypher) && cypher[end] == '#' {
		return end, numericLiteralErrorWithCancellation("UnexpectedSyntax", cypher[start:end+1], probe)
	}
	if end-start > 768 {
		if !floatLiteralInRangeWithCancellation(cypher[start:end], probe) {
			return end, numericLiteralErrorWithCancellation("FloatingPointOverflow", cypher[start:end], probe)
		}
		return end, nil
	}
	value, parseErr := strconv.ParseFloat(cypher[start:end], 64)
	if math.IsInf(value, 0) {
		return end, numericLiteralErrorWithCancellation("FloatingPointOverflow", cypher[start:end], probe)
	}
	if parseErr != nil {
		return end, numericLiteralErrorWithCancellation("InvalidNumberLiteral", cypher[start:end], probe)
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
	return numericLiteralHasUnaryMinusWithCancellation(cypher, numberStart, nil)
}
func numericLiteralHasUnaryMinusWithCancellation(cypher string, numberStart int, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	minus := numberStart - 1
	for minus >= 0 && isASCIISpace(cypher[minus]) {
		if probe != nil && !probe.check() {
			return false
		}

		minus--
	}
	if minus < 0 || cypher[minus] != '-' {
		return false
	}
	previous := minus - 1
	for previous >= 0 && isASCIISpace(cypher[previous]) {
		if probe != nil && !probe.check() {
			return false
		}

		previous--
	}
	if previous < 0 {
		return true
	}
	if strings.ContainsRune("([{,:=<>+-*/%|&", rune(cypher[previous])) {
		return true
	}
	if !isIdentByte(cypher[previous]) {
		return false
	}
	wordEnd := previous + 1
	for previous >= 0 && isIdentByte(cypher[previous]) {
		if probe != nil && !probe.check() {
			return false
		}

		previous--
	}
	word := cypher[previous+1 : wordEnd]
	for _, keyword := range [...]string{"RETURN", "WITH", "AS", "WHERE", "WHEN", "THEN", "ELSE", "IN", "AND", "OR", "XOR", "NOT", "CASE"} {
		if probe != nil && !probe.check() {
			return false
		}

		if strings.EqualFold(word, keyword) {
			return true
		}
	}
	return false
}

func numericValidationSkipQuoted(cypher string, start int) int {
	return numericValidationSkipQuotedWithCancellation(cypher, start, nil)
}
func numericValidationSkipQuotedWithCancellation(cypher string, start int, probe *pipelineCancellation) int {
	if probe != nil && !probe.check() {
		return len(cypher)
	}

	quote := cypher[start]
	for index := start + 1; index < len(cypher); index++ {
		if probe != nil && !probe.check() {
			return len(cypher)
		}

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
	return numericLiteralErrorWithCancellation(detail, literal, nil)
}
func numericLiteralErrorWithCancellation(detail, literal string, probe *pipelineCancellation) error {
	if probe != nil && !probe.check() {
		return probe.err
	}

	var message strings.Builder
	message.WriteString("invalid numeric literal ")
	appendEquivalenceGoValue(&message, reflect.ValueOf(literal), probe, false)
	if probe != nil && probe.err != nil {
		return probe.err
	}
	return newSemanticError(
		"Neo.ClientError.Statement.SyntaxError",
		detail,
		message.String(),
	)
}

func parseUintLiteralWithCancellation(digits string, base int, probe *pipelineCancellation) (uint64, error) {
	for len(digits) > 1 && digits[0] == '0' {
		if probe != nil && !probe.check() {
			return 0, probe.err
		}
		digits = digits[1:]
	}
	// Even base eight needs at most 22 digits for a uint64. This bounds
	// strconv's input and the NumError copy on its overflow path.
	if len(digits) > 22 {
		return 0, strconv.ErrRange
	}
	return strconv.ParseUint(digits, base, 64)
}

func isASCIIDigit(value byte) bool { return value >= '0' && value <= '9' }

func isDigitForBase(value byte, base int) bool {
	if isASCIIDigit(value) {
		return int(value-'0') < base
	}
	if base == 16 {
		return value >= 'a' && value <= 'f' || value >= 'A' && value <= 'F'
	}
	return false
}
