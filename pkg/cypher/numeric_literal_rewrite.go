package cypher

import "strings"

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
	return canonicalizeNumericLiteralsWithCancellation(query, nil)
}
func canonicalizeNumericLiteralsWithCancellation(query string, probe *pipelineCancellation) (canonical string, result *queryRewrite) {
	defer func() {
		if probe != nil && probe.err != nil {
			canonical, result = "", nil
		}
	}()
	if probe != nil && !probe.check() {
		return "", nil
	}

	if !mayHaveGroupedNumericLiteral(query) {
		return query, nil
	}
	var (
		rewrite *queryRewrite
		out     strings.Builder
		last    int
	)
	for index := 0; index < len(query); {
		if probe != nil && !probe.check() {
			return "", nil
		}

		c := query[index]
		switch {
		case c == '\'' || c == '"' || c == '`':
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
		end, grouped := groupedNumericLiteralEndWithCancellation(query, index, probe)
		if grouped {
			if rewrite == nil {
				rewrite = &queryRewrite{original: query}
				out.Grow(pipelineInitialCapacity(len(query), probe))
			}
			if !appendPipelineString(&out, query[last:index], probe) {
				return "", nil
			}
			canonStart := out.Len()
			if !appendPipelineString(&out, replaceTextWithCancellation(query[index:end], "_", "", probe), probe) {
				return "", nil
			}
			rewrite.edits = appendPipelineItem(rewrite.edits, queryTextEdit{origStart: index, origEnd: end, canonStart: canonStart, canonEnd: out.Len()}, probe)
			if probe != nil && probe.err != nil {
				return "", nil
			}

			last = end
		}
		index = end
	}
	if rewrite == nil {
		return query, nil
	}
	if !appendPipelineString(&out, query[last:], probe) {
		return "", nil
	}
	rewrite.canonical = out.String()
	return rewrite.canonical, rewrite
}

// mayHaveGroupedNumericLiteral reports whether query has an underscore at
// the end of a word that starts with a digit (1_000, 0x1F_E, .5_0): every
// grouped literal has one, and names (user_id, node_1_a) don't.
func mayHaveGroupedNumericLiteral(query string) bool {
	return mayHaveGroupedNumericLiteralWithCancellation(query, nil)
}
func mayHaveGroupedNumericLiteralWithCancellation(query string, probe *pipelineCancellation) bool {
	if probe != nil && !probe.check() {
		return false
	}

	for index := indexByteWithCancellation(query, '_', probe); index >= 0; {
		if probe != nil && !probe.check() {
			return false
		}

		start := index
		for start > 0 && isIdentByte(query[start-1]) {
			if probe != nil && !probe.check() {
				return false
			}

			start--
		}
		if start < index && isASCIIDigit(query[start]) {
			return true
		}
		next := indexByteWithCancellation(query[index+1:], '_', probe)
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
	c := query[index]
	if !isASCIIDigit(c) && !(c == '.' && index+1 < len(query) && isASCIIDigit(query[index+1])) {
		return false
	}
	if index == 0 {
		return true
	}
	previous := query[index-1]
	return !isIdentByte(previous) && previous != '$' && !(c == '.' && previous == '.')
}

// groupedNumericLiteralEnd returns the end of the numeric literal at
// query[start] (see numericLiteralStartsAt) and whether it groups its digits
// with underscores as Neo4j allows. A decimal integer with a leading zero
// (0_1, the legacy octal form) is not one.
func groupedNumericLiteralEnd(query string, start int) (int, bool) {
	return groupedNumericLiteralEndWithCancellation(query, start, nil)
}
func groupedNumericLiteralEndWithCancellation(query string, start int, probe *pipelineCancellation) (int, bool) {
	if probe != nil && !probe.check() {
		return start, false
	}

	if start+1 < len(query) && query[start] == '0' && (query[start+1] == 'x' || query[start+1] == 'o') {
		digit := isASCIIHexDigit
		if query[start+1] == 'o' {
			digit = isASCIIOctalDigit
		}
		end, valid, grouped := numericDigitRunWithCancellation(query, start+2, digit, true, probe)
		return end, valid && grouped
	}
	end, valid, grouped := start, true, false
	if query[start] != '.' {
		end, valid, grouped = numericDigitRunWithCancellation(query, start, isASCIIDigit, false, probe)
	}
	integerEnd := end
	floating := false
	if end+1 < len(query) && query[end] == '.' && isASCIIDigit(query[end+1]) {
		var fractionValid, fractionGrouped bool
		end, fractionValid, fractionGrouped = numericDigitRunWithCancellation(query, end+1, isASCIIDigit, false, probe)
		valid, grouped, floating = valid && fractionValid, grouped || fractionGrouped, true
	}
	if end+1 < len(query) && (query[end] == 'e' || query[end] == 'E') {
		exponent := end + 1
		if query[exponent] == '+' || query[exponent] == '-' {
			exponent++
		}
		if exponent < len(query) && (isASCIIDigit(query[exponent]) || query[exponent] == '_') {
			var exponentValid, exponentGrouped bool
			end, exponentValid, exponentGrouped = numericDigitRunWithCancellation(query, exponent, isASCIIDigit, false, probe)
			valid, grouped, floating = valid && exponentValid, grouped || exponentGrouped, true
		}
	}
	if !floating && query[start] == '0' && integerEnd-start > 1 {
		valid = false
	}
	return end, valid && grouped
}

// numericDigitRun reads the run of digits and underscores at query[start].
// It is valid when it has a digit, ends with one, never has two underscores
// in a row and, unless leadingUnderscore, starts with a digit; grouped when it
// has an underscore.
func numericDigitRun(query string, start int, digit func(byte) bool, leadingUnderscore bool) (end int, valid, grouped bool) {
	return numericDigitRunWithCancellation(query, start, digit, leadingUnderscore, nil)
}
func numericDigitRunWithCancellation(query string, start int, digit func(byte) bool, leadingUnderscore bool, probe *pipelineCancellation) (end int, valid, grouped bool) {
	if probe != nil && !probe.check() {
		return start, false, false
	}

	end = start
	for end < len(query) && (digit(query[end]) || query[end] == '_') {
		if probe != nil && !probe.check() {
			return start, false, false
		}

		end++
	}
	run := query[start:end]
	grouped = indexByteWithCancellation(run, '_', probe) >= 0
	valid = run != "" && run[len(run)-1] != '_' && !containsTextWithCancellation(run, "__", probe) && (leadingUnderscore || run[0] != '_')
	return end, valid, grouped
}

func isASCIIHexDigit(value byte) bool { return isDigitForBase(value, 16) }

func isASCIIOctalDigit(value byte) bool { return isDigitForBase(value, 8) }
