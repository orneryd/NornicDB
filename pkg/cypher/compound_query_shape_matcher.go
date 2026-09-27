package cypher

import (
	"strconv"
	"strings"
)

func matchCompoundQueryShape(query string) (ShapeMatch, bool) {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" {
		return ShapeMatch{Kind: shapeKindUnknown}, false
	}

	if match, ok := matchCompoundCreateDeleteRelShape(trimmed); ok {
		return match, true
	}
	if match, ok := matchCompoundPropCreateDeleteRelShape(trimmed); ok {
		return match, true
	}
	if match, ok := matchCompoundPropCreateDeleteReturnCountRelShape(trimmed); ok {
		return match, true
	}

	return ShapeMatch{Kind: shapeKindUnknown, Probe: ShapeProbe{Matcher: "compound_query_matcher", Matched: false, RejectReason: "no matching compound hot-path shape", NormalizedQuery: trimmed}}, false
}

func matchCompoundCreateDeleteRelShape(query string) (ShapeMatch, bool) {
	const matcherName = "compound_query_create_delete_rel"
	match := ShapeMatch{Kind: shapeKindUnknown, Captures: NewShapeCaptures(), Probe: ShapeProbe{Matcher: matcherName, NormalizedQuery: strings.TrimSpace(query), CapturedFields: map[string]string{}}}

	if findKeywordIndex(query, "MATCH") != 0 {
		match.Probe.RejectReason = "missing leading MATCH clause"
		return match, false
	}

	withIdx := findKeywordIndex(query, "WITH")
	limitIdx := findKeywordIndex(query, "LIMIT")
	createIdx := findKeywordIndex(query, "CREATE")
	deleteIdx := findKeywordIndex(query, "DELETE")
	if withIdx <= 0 || limitIdx <= withIdx || createIdx <= limitIdx || deleteIdx <= createIdx {
		match.Probe.RejectReason = "compound WITH/LIMIT/CREATE/DELETE shape not found"
		return match, false
	}

	matchSection := strings.TrimSpace(query[len("MATCH"):withIdx])
	leftNode, rightNode, ok := splitTopLevelCommaShape(matchSection)
	if !ok {
		match.Probe.RejectReason = "expected two MATCH node patterns"
		return match, false
	}

	leftNodeMatch, ok := parseLabeledNodePattern(leftNode)
	if !ok {
		match.Probe.RejectReason = "invalid left MATCH node pattern"
		return match, false
	}
	rightNodeMatch, ok := parseLabeledNodePattern(rightNode)
	if !ok {
		match.Probe.RejectReason = "invalid right MATCH node pattern"
		return match, false
	}
	createMatch, ok := parseCreateRelationshipClause(query[createIdx:deleteIdx])
	if !ok {
		match.Probe.RejectReason = "invalid CREATE relationship clause"
		return match, false
	}
	deleteVar := strings.TrimSpace(query[deleteIdx+len("DELETE"):])
	deleteVar, _, _ = strings.Cut(deleteVar, " ")
	deleteVar = strings.TrimSpace(deleteVar)
	if deleteVar == "" {
		match.Probe.RejectReason = "missing DELETE variable"
		return match, false
	}

	limitValue := strings.TrimSpace(query[limitIdx+len("LIMIT") : createIdx])
	fields := strings.Fields(limitValue)
	if len(fields) == 0 {
		match.Probe.RejectReason = "missing LIMIT literal"
		return match, false
	}
	limit, err := strconv.Atoi(fields[0])
	if err != nil || limit < 0 {
		match.Probe.RejectReason = "invalid LIMIT literal"
		return match, false
	}

	match.Kind = shapeKindCompoundCreateDeleteRel
	match.Captures.Add("label1", leftNodeMatch.label)
	match.Captures.Add("label2", rightNodeMatch.label)
	match.Captures.Add("rel_var", createMatch.relVar)
	match.Captures.Add("rel_type", createMatch.relType)
	match.Captures.Add("delete_var", deleteVar)
	match.Captures.Add("limit", limit)
	match.Probe.Matched = true
	match.Probe.CapturedFields["label1"] = leftNodeMatch.label
	match.Probe.CapturedFields["label2"] = rightNodeMatch.label
	match.Probe.CapturedFields["rel_var"] = createMatch.relVar
	match.Probe.CapturedFields["rel_type"] = createMatch.relType
	match.Probe.CapturedFields["delete_var"] = deleteVar
	match.Probe.CapturedFields["limit"] = fields[0]
	return match, true
}

func matchCompoundPropCreateDeleteRelShape(query string) (ShapeMatch, bool) {
	const matcherName = "compound_query_prop_create_delete_rel"
	match := ShapeMatch{Kind: shapeKindUnknown, Captures: NewShapeCaptures(), Probe: ShapeProbe{Matcher: matcherName, NormalizedQuery: strings.TrimSpace(query), CapturedFields: map[string]string{}}}

	if findKeywordIndex(query, "MATCH") != 0 {
		match.Probe.RejectReason = "missing leading MATCH clause"
		return match, false
	}

	createIdx := findKeywordIndex(query, "CREATE")
	deleteIdx := findKeywordIndex(query, "DELETE")
	if createIdx <= 0 || deleteIdx <= createIdx {
		match.Probe.RejectReason = "compound property CREATE/DELETE shape not found"
		return match, false
	}

	matchSection := strings.TrimSpace(query[len("MATCH"):createIdx])
	leftNode, rightNode, ok := splitTopLevelCommaShape(matchSection)
	if !ok {
		match.Probe.RejectReason = "expected two MATCH node patterns"
		return match, false
	}

	leftNodeMatch, ok := parseLabeledNodePattern(leftNode)
	if !ok {
		match.Probe.RejectReason = "invalid left MATCH node pattern"
		return match, false
	}
	rightNodeMatch, ok := parseLabeledNodePattern(rightNode)
	if !ok {
		match.Probe.RejectReason = "invalid right MATCH node pattern"
		return match, false
	}
	createMatch, ok := parseCreateRelationshipClause(query[createIdx:deleteIdx])
	if !ok {
		match.Probe.RejectReason = "invalid CREATE relationship clause"
		return match, false
	}
	deleteVar := strings.TrimSpace(query[deleteIdx+len("DELETE"):])
	deleteVar, _, _ = strings.Cut(deleteVar, " ")
	deleteVar = strings.TrimSpace(deleteVar)
	if deleteVar == "" {
		match.Probe.RejectReason = "missing DELETE variable"
		return match, false
	}

	match.Kind = shapeKindCompoundPropCreateDeleteRel
	match.Captures.Add("label1", leftNodeMatch.label)
	match.Captures.Add("label2", rightNodeMatch.label)
	match.Captures.Add("prop1", leftNodeMatch.propKey)
	match.Captures.Add("prop2", rightNodeMatch.propKey)
	match.Captures.Add("value1", leftNodeMatch.propValue)
	match.Captures.Add("value2", rightNodeMatch.propValue)
	match.Captures.Add("rel_var", createMatch.relVar)
	match.Captures.Add("rel_type", createMatch.relType)
	match.Captures.Add("delete_var", deleteVar)
	match.Probe.Matched = true
	match.Probe.CapturedFields["label1"] = leftNodeMatch.label
	match.Probe.CapturedFields["label2"] = rightNodeMatch.label
	match.Probe.CapturedFields["prop1"] = leftNodeMatch.propKey
	match.Probe.CapturedFields["prop2"] = rightNodeMatch.propKey
	match.Probe.CapturedFields["value1"] = leftNodeMatch.propValue
	match.Probe.CapturedFields["value2"] = rightNodeMatch.propValue
	match.Probe.CapturedFields["rel_var"] = createMatch.relVar
	match.Probe.CapturedFields["rel_type"] = createMatch.relType
	match.Probe.CapturedFields["delete_var"] = deleteVar
	return match, true
}

func matchCompoundPropCreateDeleteReturnCountRelShape(query string) (ShapeMatch, bool) {
	const matcherName = "compound_query_prop_create_delete_return_count_rel"
	match := ShapeMatch{Kind: shapeKindUnknown, Captures: NewShapeCaptures(), Probe: ShapeProbe{Matcher: matcherName, NormalizedQuery: strings.TrimSpace(query), CapturedFields: map[string]string{}}}

	if findKeywordIndex(query, "MATCH") != 0 {
		match.Probe.RejectReason = "missing leading MATCH clause"
		return match, false
	}

	createIdx := findKeywordIndex(query, "CREATE")
	withIdx := findKeywordIndex(query, "WITH")
	deleteIdx := findKeywordIndex(query, "DELETE")
	returnIdx := findKeywordIndex(query, "RETURN")
	if createIdx <= 0 || withIdx <= createIdx || deleteIdx <= withIdx || returnIdx <= deleteIdx {
		match.Probe.RejectReason = "compound property WITH/DELETE/RETURN shape not found"
		return match, false
	}

	matchSection := strings.TrimSpace(query[len("MATCH"):createIdx])
	leftNode, rightNode, ok := splitTopLevelCommaShape(matchSection)
	if !ok {
		match.Probe.RejectReason = "expected two MATCH node patterns"
		return match, false
	}

	leftNodeMatch, ok := parseLabeledNodePattern(leftNode)
	if !ok {
		match.Probe.RejectReason = "invalid left MATCH node pattern"
		return match, false
	}
	rightNodeMatch, ok := parseLabeledNodePattern(rightNode)
	if !ok {
		match.Probe.RejectReason = "invalid right MATCH node pattern"
		return match, false
	}
	createMatch, ok := parseCreateRelationshipClause(query[createIdx:withIdx])
	if !ok {
		match.Probe.RejectReason = "invalid CREATE relationship clause"
		return match, false
	}
	withVar, ok := firstClauseWord(query[withIdx+len("WITH") : deleteIdx])
	if !ok {
		match.Probe.RejectReason = "missing WITH variable"
		return match, false
	}
	delVar, ok := firstClauseWord(query[deleteIdx+len("DELETE") : returnIdx])
	if !ok {
		match.Probe.RejectReason = "missing DELETE variable"
		return match, false
	}
	countPart := strings.TrimSpace(query[returnIdx+len("RETURN"):])
	countPart = strings.ReplaceAll(strings.ReplaceAll(countPart, " ", ""), "\t", "")
	if !strings.HasPrefix(upperASCII(countPart), "COUNT(") || !strings.HasSuffix(countPart, ")") {
		match.Probe.RejectReason = "RETURN clause is not COUNT(var)"
		return match, false
	}
	countVar := strings.TrimSpace(countPart[len("COUNT(") : len(countPart)-1])
	if countVar == "" {
		match.Probe.RejectReason = "missing COUNT variable"
		return match, false
	}
	if createMatch.relVar == "" || !strings.EqualFold(withVar, createMatch.relVar) || !strings.EqualFold(delVar, createMatch.relVar) || !strings.EqualFold(countVar, createMatch.relVar) {
		match.Probe.RejectReason = "relationship variable mismatch across WITH/DELETE/RETURN"
		return match, false
	}

	match.Kind = shapeKindCompoundPropCreateDeleteReturnCountRel
	match.Captures.Add("label1", leftNodeMatch.label)
	match.Captures.Add("label2", rightNodeMatch.label)
	match.Captures.Add("prop1", leftNodeMatch.propKey)
	match.Captures.Add("prop2", rightNodeMatch.propKey)
	match.Captures.Add("value1", leftNodeMatch.propValue)
	match.Captures.Add("value2", rightNodeMatch.propValue)
	match.Captures.Add("rel_var", createMatch.relVar)
	match.Captures.Add("rel_type", createMatch.relType)
	match.Captures.Add("with_var", withVar)
	match.Captures.Add("delete_var", delVar)
	match.Captures.Add("count_var", countVar)
	match.Probe.Matched = true
	match.Probe.CapturedFields["label1"] = leftNodeMatch.label
	match.Probe.CapturedFields["label2"] = rightNodeMatch.label
	match.Probe.CapturedFields["prop1"] = leftNodeMatch.propKey
	match.Probe.CapturedFields["prop2"] = rightNodeMatch.propKey
	match.Probe.CapturedFields["value1"] = leftNodeMatch.propValue
	match.Probe.CapturedFields["value2"] = rightNodeMatch.propValue
	match.Probe.CapturedFields["rel_var"] = createMatch.relVar
	match.Probe.CapturedFields["rel_type"] = createMatch.relType
	match.Probe.CapturedFields["with_var"] = withVar
	match.Probe.CapturedFields["delete_var"] = delVar
	match.Probe.CapturedFields["count_var"] = countVar
	return match, true
}

type parsedNodePattern struct {
	label     string
	propKey   string
	propValue string
}

type parsedCreatePattern struct {
	relVar  string
	relType string
}

func splitTopLevelCommaShape(s string) (string, string, bool) {
	depthParen := 0
	depthBrace := 0
	depthBracket := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			if s[i] == '\'' && !inDouble {
				inSingle = !inSingle
			} else if s[i] == '"' && !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depthParen++
			}
		case ')':
			if !inSingle && !inDouble {
				depthParen--
			}
		case '{':
			if !inSingle && !inDouble {
				depthBrace++
			}
		case '}':
			if !inSingle && !inDouble {
				depthBrace--
			}
		case '[':
			if !inSingle && !inDouble {
				depthBracket++
			}
		case ']':
			if !inSingle && !inDouble {
				depthBracket--
			}
		case ',':
			if !inSingle && !inDouble && depthParen == 0 && depthBrace == 0 && depthBracket == 0 {
				left := strings.TrimSpace(s[:i])
				right := strings.TrimSpace(s[i+1:])
				if left == "" || right == "" {
					return "", "", false
				}
				return left, right, true
			}
		}
	}
	return "", "", false
}

func firstClauseWord(s string) (string, bool) {
	fields := strings.Fields(strings.TrimSpace(s))
	if len(fields) == 0 {
		return "", false
	}
	return fields[0], true
}

func parseLabeledNodePattern(s string) (parsedNodePattern, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "(") || !strings.HasSuffix(s, ")") {
		return parsedNodePattern{}, false
	}
	inner := strings.TrimSpace(s[1 : len(s)-1])
	if inner == "" {
		return parsedNodePattern{}, false
	}
	varName, rest, ok := parseIdentifierToken(inner)
	if !ok {
		return parsedNodePattern{}, false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, ":") {
		return parsedNodePattern{}, false
	}
	rest = strings.TrimSpace(rest[1:])
	label, rest, ok := parseIdentifierToken(rest)
	if !ok {
		return parsedNodePattern{}, false
	}
	result := parsedNodePattern{label: label}
	_ = varName
	rest = strings.TrimSpace(rest)
	if rest == "" {
		return result, true
	}
	if !strings.HasPrefix(rest, "{") || !strings.HasSuffix(rest, "}") {
		return parsedNodePattern{}, false
	}
	props := strings.TrimSpace(rest[1 : len(rest)-1])
	propKey, propValue, ok := parseSinglePropertyAssignment(props)
	if !ok {
		return parsedNodePattern{}, false
	}
	result.propKey = propKey
	result.propValue = propValue
	return result, true
}

func parseSinglePropertyAssignment(s string) (string, string, bool) {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		return "", "", false
	}
	key := strings.TrimSpace(parts[0])
	val := strings.TrimSpace(parts[1])
	if key == "" || val == "" {
		return "", "", false
	}
	if _, _, ok := parseIdentifierToken(key); !ok {
		return "", "", false
	}
	return key, val, true
}

func parseCreateRelationshipClause(s string) (parsedCreatePattern, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(upperASCII(s), "CREATE") {
		return parsedCreatePattern{}, false
	}
	body := strings.TrimSpace(s[len("CREATE"):])
	left, rest, ok := parseBareNodeReference(body)
	if !ok {
		return parsedCreatePattern{}, false
	}
	_ = left
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "-") {
		return parsedCreatePattern{}, false
	}
	rest = strings.TrimSpace(rest[1:])
	if !strings.HasPrefix(rest, "[") {
		return parsedCreatePattern{}, false
	}
	relSpec, rest, ok := extractBracketSection(rest)
	if !ok {
		return parsedCreatePattern{}, false
	}
	rest = strings.TrimSpace(rest)
	if !strings.HasPrefix(rest, "->") {
		return parsedCreatePattern{}, false
	}
	rest = strings.TrimSpace(rest[2:])
	right, tail, ok := parseBareNodeReference(rest)
	if !ok || strings.TrimSpace(tail) != "" {
		return parsedCreatePattern{}, false
	}
	parts := strings.SplitN(strings.TrimSpace(relSpec), ":", 2)
	if len(parts) != 2 {
		return parsedCreatePattern{}, false
	}
	relVar := strings.TrimSpace(parts[0])
	relType := strings.TrimSpace(parts[1])
	if relVar == "" || relType == "" {
		return parsedCreatePattern{}, false
	}
	if _, _, ok := parseIdentifierToken(relVar); !ok {
		return parsedCreatePattern{}, false
	}
	if _, _, ok := parseIdentifierToken(relType); !ok {
		return parsedCreatePattern{}, false
	}
	_ = right
	return parsedCreatePattern{relVar: relVar, relType: relType}, true
}

func parseBareNodeReference(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if !strings.HasPrefix(s, "(") {
		return "", "", false
	}
	closeIdx, rest, ok := extractParenSection(s)
	if !ok {
		return "", "", false
	}
	inner := strings.TrimSpace(closeIdx)
	if inner == "" {
		return "", "", false
	}
	name, trailing, ok := parseIdentifierToken(inner)
	if !ok || strings.TrimSpace(trailing) != "" {
		return "", "", false
	}
	return name, rest, true
}

func parseIdentifierToken(s string) (string, string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || !isWordChar(s[0]) || (s[0] >= '0' && s[0] <= '9') {
		return "", "", false
	}
	i := 1
	for i < len(s) && isWordChar(s[i]) {
		i++
	}
	return s[:i], s[i:], true
}

func extractParenSection(s string) (inside string, rest string, ok bool) {
	if !strings.HasPrefix(s, "(") {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			if s[i] == '\'' && !inDouble {
				inSingle = !inSingle
			} else if s[i] == '"' && !inSingle {
				inDouble = !inDouble
			}
		case '(':
			if !inSingle && !inDouble {
				depth++
			}
		case ')':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					return s[1:i], s[i+1:], true
				}
			}
		}
	}
	return "", "", false
}

func extractBracketSection(s string) (inside string, rest string, ok bool) {
	if !strings.HasPrefix(s, "[") {
		return "", "", false
	}
	depth := 0
	inSingle := false
	inDouble := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '\'', '"':
			if s[i] == '\'' && !inDouble {
				inSingle = !inSingle
			} else if s[i] == '"' && !inSingle {
				inDouble = !inDouble
			}
		case '[':
			if !inSingle && !inDouble {
				depth++
			}
		case ']':
			if !inSingle && !inDouble {
				depth--
				if depth == 0 {
					return s[1:i], s[i+1:], true
				}
			}
		}
	}
	return "", "", false
}
