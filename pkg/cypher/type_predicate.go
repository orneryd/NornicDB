package cypher

import (
	"strconv"
	"strings"
)

// Type predicate expressions (#838): `x IS :: TYPE`, `x IS NOT :: TYPE`,
// `x :: TYPE`, `x IS TYPED TYPE` and `x IS NOT TYPED TYPE`, as Neo4j 5.26
// evaluates them. A value matches a type by its type-system name
// (cypherTypeSystemName), so every value kind the classifier knows is
// covered. null matches every type except NOTHING and NOT NULL types; NULL
// matches only null.

// cypherTypeSpec is a parsed type: a union of members, NOT NULL as a whole
// when written after a single member or a closed ANY<…> union.
type cypherTypeSpec struct {
	members []cypherTypeMember
}

// cypherTypeMember is one type of a union: a canonical type-system name, the
// element type of a LIST, and whether it excludes null.
type cypherTypeMember struct {
	name    string
	element *cypherTypeSpec
	notNull bool
	// vectorType and vectorDimension narrow a VECTOR member
	// (VECTOR<INTEGER32>(3)); nil takes any. A dimension no vector has
	// (VECTOR(0), VECTOR(-1)) is a valid type that matches nothing, as in
	// Neo4j 2026.09.
	vectorType      *VectorCoordinateType
	vectorDimension *int64
}

// typePredicateOperators are the spellings of a type predicate, longest
// first so " IS NOT :: " is not read as " :: ".
var typePredicateOperators = []struct {
	text    string
	negated bool
}{
	{" IS NOT TYPED ", true},
	{" IS NOT :: ", true},
	{" IS TYPED ", false},
	{" IS :: ", false},
	{" :: ", false},
}

// splitTypePredicate splits `operand IS [NOT] :: TYPE` at its top-level type
// predicate operator. ok is false when expr is not a type predicate; err is
// a SyntaxError for a type that cannot be parsed.
func splitTypePredicate(expr string) (operand string, negated bool, spec cypherTypeSpec, ok bool, err error) {
	if !strings.Contains(expr, "::") && !containsFold(expr, " TYPED ") {
		return "", false, cypherTypeSpec{}, false, nil
	}
	for _, operator := range typePredicateOperators {
		left, right, found := splitByOperatorOutsideCase(expr, operator.text, true, true)
		if !found || strings.TrimSpace(left) == "" {
			continue
		}
		spec, err := parseCypherTypeSpec(strings.TrimSpace(right))
		if err != nil {
			return "", false, cypherTypeSpec{}, true, err
		}
		return strings.TrimSpace(left), operator.negated, spec, true, nil
	}
	return "", false, cypherTypeSpec{}, false, nil
}

func typePredicateSyntaxError(text string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidType", "Invalid type: "+text)
}

// parseCypherTypeSpec parses a type: members separated by top-level `|`.
func parseCypherTypeSpec(text string) (cypherTypeSpec, error) {
	parts := splitTopLevelTypeUnion(text)
	spec := cypherTypeSpec{members: make([]cypherTypeMember, 0, len(parts))}
	for _, part := range parts {
		member, err := parseCypherTypeMember(strings.TrimSpace(part))
		if err != nil {
			return cypherTypeSpec{}, err
		}
		spec.members = append(spec.members, member)
	}
	if len(spec.members) > 1 {
		for _, member := range spec.members {
			if member.notNull {
				// Neo4j rejects NOT NULL on a member of an open union
				// (INTEGER NOT NULL | FLOAT); ANY<…> NOT NULL is the form.
				return cypherTypeSpec{}, typePredicateSyntaxError(text)
			}
		}
	}
	return spec, nil
}

// splitTopLevelTypeUnion splits a type at `|` outside <…>.
func splitTopLevelTypeUnion(text string) []string {
	var parts []string
	depth, start := 0, 0
	for index := 0; index < len(text); index++ {
		switch text[index] {
		case '<':
			depth++
		case '>':
			depth--
		case '|':
			if depth == 0 {
				parts = append(parts, text[start:index])
				start = index + 1
			}
		}
	}
	return append(parts, text[start:])
}

// cypherTypeSynonyms maps every spelling of a simple type to its
// type-system name.
var cypherTypeSynonyms = map[string]string{
	"BOOL": "BOOLEAN", "BOOLEAN": "BOOLEAN",
	"STRING": "STRING", "VARCHAR": "STRING",
	"INT": "INTEGER", "INTEGER": "INTEGER", "SIGNED INTEGER": "INTEGER",
	"FLOAT":      "FLOAT",
	"DATE":       "DATE",
	"LOCAL TIME": "LOCAL TIME", "TIME WITHOUT TIME ZONE": "LOCAL TIME",
	"ZONED TIME": "ZONED TIME", "TIME WITH TIME ZONE": "ZONED TIME",
	"LOCAL DATETIME": "LOCAL DATETIME", "TIMESTAMP WITHOUT TIME ZONE": "LOCAL DATETIME",
	"ZONED DATETIME": "ZONED DATETIME", "TIMESTAMP WITH TIME ZONE": "ZONED DATETIME",
	"DURATION": "DURATION",
	"POINT":    "POINT",
	"NODE":     "NODE", "ANY NODE": "NODE", "VERTEX": "NODE", "ANY VERTEX": "NODE",
	"RELATIONSHIP": "RELATIONSHIP", "ANY RELATIONSHIP": "RELATIONSHIP", "EDGE": "RELATIONSHIP", "ANY EDGE": "RELATIONSHIP",
	"MAP": "MAP", "ANY MAP": "MAP",
	"PATH": "PATH", "ANY PATH": "PATH",
	"ANY": "ANY", "ANY VALUE": "ANY",
	"NOTHING":        "NOTHING",
	"NULL":           "NULL",
	"PROPERTY VALUE": "PROPERTY VALUE", "ANY PROPERTY VALUE": "PROPERTY VALUE",
	"LIST": "LIST", "ARRAY": "LIST",
	"UUID": "UUID",
}

// parseCypherTypeMember parses one member: a simple type, LIST<…> /
// ARRAY<…>, or a closed union ANY<…>, optionally followed by NOT NULL.
func parseCypherTypeMember(text string) (cypherTypeMember, error) {
	words := strings.Fields(text)
	notNull := false
	if len(words) >= 2 && strings.EqualFold(words[len(words)-2], "NOT") && strings.EqualFold(words[len(words)-1], "NULL") {
		notNull = true
		text = strings.TrimSpace(text[:strings.LastIndex(upperASCII(text), "NOT")])
	}
	if len(text) >= len("VECTOR") && strings.EqualFold(text[:len("VECTOR")], "VECTOR") {
		member, ok := parseVectorTypeMember(strings.TrimSpace(text[len("VECTOR"):]))
		if !ok {
			return cypherTypeMember{}, typePredicateSyntaxError(text)
		}
		member.notNull = notNull
		return member, nil
	}
	if open := strings.IndexByte(text, '<'); open >= 0 {
		if !strings.HasSuffix(text, ">") {
			return cypherTypeMember{}, typePredicateSyntaxError(text)
		}
		head := upperASCII(strings.Join(strings.Fields(text[:open]), " "))
		inner, err := parseCypherTypeSpec(strings.TrimSpace(text[open+1 : len(text)-1]))
		if err != nil {
			return cypherTypeMember{}, err
		}
		switch head {
		case "LIST", "ARRAY":
			return cypherTypeMember{name: "LIST", element: &inner, notNull: notNull}, nil
		case "ANY":
			// A closed union: a value of any of its types. NOT NULL after
			// it applies to every member.
			if notNull {
				for index := range inner.members {
					inner.members[index].notNull = true
				}
			}
			return cypherTypeMember{name: "UNION", element: &inner, notNull: notNull}, nil
		}
		return cypherTypeMember{}, typePredicateSyntaxError(text)
	}
	name, known := cypherTypeSynonyms[upperASCII(strings.Join(strings.Fields(text), " "))]
	if !known {
		return cypherTypeMember{}, typePredicateSyntaxError(text)
	}
	if name == "LIST" {
		return cypherTypeMember{name: "LIST", element: &cypherTypeSpec{members: []cypherTypeMember{{name: "ANY"}}}, notNull: notNull}, nil
	}
	return cypherTypeMember{name: name, notNull: notNull}, nil
}

// parseVectorTypeMember reads what follows VECTOR in a type: an optional
// <coordinate type> and an optional (dimension).
func parseVectorTypeMember(rest string) (cypherTypeMember, bool) {
	member := cypherTypeMember{name: "VECTOR"}
	if strings.HasPrefix(rest, "<") {
		close := strings.IndexByte(rest, '>')
		if close < 0 {
			return member, false
		}
		coordinateType, ok := parseVectorCoordinateType(rest[1:close])
		if !ok {
			return member, false
		}
		member.vectorType = &coordinateType
		rest = strings.TrimSpace(rest[close+1:])
	}
	if strings.HasPrefix(rest, "(") && strings.HasSuffix(rest, ")") {
		dimension, err := strconv.ParseInt(strings.TrimSpace(rest[1:len(rest)-1]), 10, 64)
		if err != nil {
			return member, false
		}
		member.vectorDimension = &dimension
		rest = ""
	}
	return member, rest == ""
}

// matches reports whether value is of the type.
func (spec cypherTypeSpec) matches(value interface{}) bool {
	for _, member := range spec.members {
		if member.matches(value) {
			return true
		}
	}
	return false
}

func (member cypherTypeMember) matches(value interface{}) bool {
	if value == nil {
		switch member.name {
		case "NULL":
			return true
		case "NOTHING":
			return false
		case "UNION":
			return !member.notNull && member.element.matches(nil)
		}
		return !member.notNull
	}
	switch member.name {
	case "ANY":
		return true
	case "NOTHING", "NULL":
		return false
	case "UNION":
		return member.element.matches(value)
	case "PROPERTY VALUE":
		return isCypherPropertyValue(value)
	case "VECTOR":
		vector, err := vectorArgument(value)
		return err == nil && (member.vectorType == nil || *member.vectorType == vector.Type) &&
			(member.vectorDimension == nil || *member.vectorDimension == int64(vector.Dimension()))
	case "LIST":
		items, isList := cypherListValue(value)
		if !isList || cypherValueKindOf(value) != valueKindList {
			return false
		}
		for _, item := range items {
			if !member.element.matches(item) {
				return false
			}
		}
		return true
	}
	return cypherTypeSystemName(value) == member.name
}

// propertyValueTypeNames are the type-system names of storable scalars.
var propertyValueTypeNames = map[string]bool{
	"BOOLEAN": true, "STRING": true, "INTEGER": true, "FLOAT": true, "DATE": true, "LOCAL TIME": true,
	"ZONED TIME": true, "LOCAL DATETIME": true, "ZONED DATETIME": true, "DURATION": true, "POINT": true,
}

// isCypherPropertyValue reports whether value is a PROPERTY VALUE: a
// storable scalar, or a list of storable scalars of one type.
func isCypherPropertyValue(value interface{}) bool {
	if cypherValueKindOf(value) != valueKindList {
		return propertyValueTypeNames[cypherTypeSystemName(value)]
	}
	items, _ := cypherListValue(value)
	name := ""
	for _, item := range items {
		itemName := cypherTypeSystemName(item)
		if item == nil || !propertyValueTypeNames[itemName] || (name != "" && itemName != name) {
			return false
		}
		name = itemName
	}
	return true
}

// evaluateTypePredicate applies a parsed type predicate to the operand's
// value.
func evaluateTypePredicate(value interface{}, negated bool, spec cypherTypeSpec) bool {
	matched := spec.matches(value)
	if negated {
		return !matched
	}
	return matched
}

// typeGrammarWords are the words a type after :: or TYPED can be made of.
var typeGrammarWords = map[string]bool{
	"BOOL": true, "BOOLEAN": true, "STRING": true, "VARCHAR": true, "INT": true, "INTEGER": true, "SIGNED": true,
	"FLOAT": true, "DATE": true, "LOCAL": true, "ZONED": true, "TIME": true, "DATETIME": true, "TIMESTAMP": true,
	"WITH": true, "WITHOUT": true, "ZONE": true, "DURATION": true, "POINT": true, "NODE": true, "VERTEX": true,
	"RELATIONSHIP": true, "EDGE": true, "MAP": true, "PATH": true, "ANY": true, "VALUE": true, "NOTHING": true,
	"NULL": true, "NOT": true, "PROPERTY": true, "LIST": true, "ARRAY": true, "UUID": true, "VECTOR": true,
	"INTEGER8": true, "INTEGER16": true, "INTEGER32": true, "INTEGER64": true, "INT8": true, "INT16": true,
	"INT32": true, "INT64": true, "FLOAT32": true, "FLOAT64": true,
}

// typedFollowsIs reports whether the TYPED at index is the type predicate's
// keyword (x IS TYPED T, x IS NOT TYPED T), not a name (count(typed)).
func typedFollowsIs(expression string, index int) bool {
	end := index
	for end > 0 && isASCIISpace(expression[end-1]) {
		end--
	}
	start := end
	for start > 0 && isIdentByte(expression[start-1]) {
		start--
	}
	word := expression[start:end]
	if equalFoldASCII(word, "NOT") {
		word = wordBefore(expression, start)
	}
	return equalFoldASCII(word, "IS")
}

// maskTypePredicateTypes blanks the type after each `::` and `TYPED` in an
// expression, so scanners that look for variables (expressionFreeVariables)
// do not read type names (INTEGER, LIST<STRING>) as variables (#838).
func maskTypePredicateTypes(expression string) string {
	// Zero-allocation fast path: comparison scans call this per row, and the
	// function-argument checks per statement (a label's single colon is no
	// type predicate).
	if !strings.Contains(expression, "::") && indexASCIIFold(expression, "typed") < 0 {
		return expression
	}
	masked := []byte(expression)
	for index := 0; index < len(masked); index++ {
		start, position := -1, 0
		switch {
		case masked[index] == ':' && index+1 < len(masked) && masked[index+1] == ':':
			start, position = index, index+2
		case index+5 <= len(masked) && strings.EqualFold(string(masked[index:index+5]), "TYPED") &&
			(index == 0 || !isIdentByte(masked[index-1])) && (index+5 == len(masked) || !isIdentByte(masked[index+5])) &&
			typedFollowsIs(expression, index):
			start, position = index, index+5
		}
		if start < 0 {
			continue
		}
		vector := false
		for position < len(masked) {
			next := skipSpaces(expression, position)
			if next >= len(masked) {
				position = next
				break
			}
			if character := masked[next]; character == '<' || character == '>' || character == '|' {
				position = next + 1
				continue
			}
			if masked[next] == '(' && vector {
				// VECTOR(3), VECTOR<INT8>(3): the dimension is part of the
				// type, not a call.
				if close := strings.IndexByte(expression[next:], ')'); close >= 0 {
					position = next + close + 1
					continue
				}
			}
			word, end, ok := scanIdentifierToken(expression, next)
			if !ok || !typeGrammarWords[upperASCII(word)] {
				break
			}
			vector = strings.EqualFold(word, "VECTOR") || vector && !strings.EqualFold(word, "NOT")
			position = end
		}
		for blank := start; blank < position; blank++ {
			masked[blank] = ' '
		}
		index = position - 1
	}
	return string(masked)
}
