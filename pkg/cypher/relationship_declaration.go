package cypher

import "strings"

// A relationship pattern's inside ([r:A|B *1..2 {k: v}], [r:A WHERE p]) is
// read by one scanner wherever a statement uses it: CREATE, MERGE, MATCH,
// the clause rewrites, the fast-path plans, the write-stretch analysis and
// schema statements (relationshipDeclarationOf, and
// relationshipDeclarationHead for readers that need no type list): a
// backtick-quoted type may hold any character, ':', '|', '*', '{' and
// spaces included, and a doubled backtick inside it is one backtick, as
// labels are read (eachChainItem, #907). A dynamic type is an expression:
// a '{', '*' or '..' inside $(...) ($({k: t}.k), $(['A', 'B'][i * 1]),
// $(ts[0..1][0])) is neither the property map nor a length.

// relationshipDeclaration is a relationship pattern's inside, read outside
// backticks.
type relationshipDeclaration struct {
	// variable is the relationship variable as written, trimmed ("" when
	// there is none).
	variable string
	// hasColon is set when the declaration names types (r:T, :T).
	hasColon bool
	// typeText is the text after the colon and before any length, as
	// written: a dynamic type $(e) is resolved from it per row.
	typeText string
	// types are the type names in order, backtick-quoted ones unquoted
	// (`a``b` is a`b); a dynamic $(e) item is kept as written.
	types []string
	// invalidType is the first type as written that names no type: an
	// unquoted one that isn't an identifier (a name Neo4j can't parse
	// unquoted) or an empty quoted one (``); "" when there is none.
	invalidType string
	// hasLength is set for a variable length (*, *2, *1..3, before or after
	// the types); length is its digits and dots after the *.
	hasLength bool
	length    string
	// lengthInvalid is set when the length is malformed: '..' without a
	// '*', or anything but digits and at most one '..' after the '*'
	// (MATCH rejects it). Quoted text and expressions don't count (#879).
	lengthInvalid bool
	// properties is the property map, from its { to the end, trimmed, or a
	// trailing parameter ($props); "" when there is none.
	properties string
	// predicate is the element's own WHERE predicate ([r:T WHERE r.k = 1]),
	// the text after WHERE, trimmed; "" when there is none.
	predicate string
}

// relationshipDeclarationOf reads a relationship pattern's inside (the text
// between [ and ]). Types are separated by | (or by a second colon, the
// deprecated [:A|:B] form); empty items are skipped.
func relationshipDeclarationOf(inner string) relationshipDeclaration {
	declaration := relationshipDeclarationHead(inner)
	if !declaration.hasColon {
		return declaration
	}
	text := declaration.typeText
	inBacktick, depth, start := false, 0, 0
	for index := 0; index <= len(text); index++ {
		if index < len(text) {
			switch c := text[index]; {
			case c == '`':
				inBacktick = !inBacktick // a doubled backtick toggles twice
				continue
			case inBacktick:
				continue
			case c == '\'' || c == '"':
				index = skipCypherQuotedText(text, index, c) - 1
				continue
			case c == '(':
				depth++
				continue
			case c == ')':
				depth--
				continue
			case depth > 0 || c != '|' && c != ':':
				continue
			}
		}
		declaration.addType(strings.TrimSpace(text[start:index]))
		start = index + 1
	}
	return declaration
}

// relationshipDeclarationHead reads a relationship pattern's inside as
// relationshipDeclarationOf does, without the type list (types and
// invalidType stay empty): the variable, the type text, the length and the
// property map. MATCH's range check and variable-length variables read it.
func relationshipDeclarationHead(inner string) relationshipDeclaration {
	var declaration relationshipDeclaration
	head := strings.TrimSpace(inner)
	// The WHERE predicate ends the inside; it is an expression, so nothing in
	// it is the type, the length or the property map.
	if strings.IndexAny(head, "Ww") >= 0 {
		if where := elementWhereIndex(head, -1, len(head)); where >= 0 {
			declaration.predicate = strings.TrimSpace(head[where+len("WHERE"):])
			head = strings.TrimSpace(head[:where])
		}
	}
	masked := blankRelationshipExpressions(head)
	if brace := strings.IndexByte(masked, '{'); brace >= 0 {
		declaration.properties = strings.TrimSpace(head[brace:])
		head, masked = head[:brace], masked[:brace]
	}
	declaration.lengthInvalid = relationshipLengthInvalid(masked)
	if declaration.properties == "" {
		if rest, parameter, ok := splitPatternParameterMap(head); ok {
			declaration.properties = parameter
			head, masked = rest, masked[:len(rest)]
		}
	}
	if star := strings.IndexByte(masked, '*'); star >= 0 {
		end := star + 1
		for end < len(head) && (isDigitByte(head[end]) || head[end] == '.') {
			end++
		}
		declaration.hasLength = true
		declaration.length = head[star+1 : end]
		if strings.TrimSpace(head[end:]) == "" {
			head = head[:star]
		} else {
			head = head[:star] + head[end:]
		}
	}
	colon := indexByteOutsideBackticks(head, ':')
	if colon < 0 {
		declaration.variable = strings.TrimSpace(head)
		return declaration
	}
	declaration.hasColon = true
	declaration.variable = strings.TrimSpace(head[:colon])
	declaration.typeText = strings.TrimSpace(head[colon+1:])
	return declaration
}

// relationshipLengthInvalid reports a malformed length in masked, a
// declaration's text before its property map with quoted text and
// expressions blanked (blankRelationshipExpressions): '..' without a '*',
// or anything but digits and at most one '..' after the '*'.
func relationshipLengthInvalid(masked string) bool {
	masked = strings.TrimSpace(masked)
	star := strings.IndexByte(masked, '*')
	if star < 0 {
		return strings.Contains(masked, "..")
	}
	spec := strings.TrimSpace(masked[star+1:])
	for index := 0; index < len(spec); index++ {
		if !isDigitByte(spec[index]) && spec[index] != '.' {
			return true
		}
	}
	return strings.Count(spec, "..") > 1 || strings.Contains(spec, ".") && !strings.Contains(spec, "..")
}

// blankRelationshipExpressions is text with quoted text and the inside of
// parenthesized expressions blanked, at the same offsets, so a '{', '*' or
// '..' left in it belongs to the declaration: one inside a dynamic type
// ($({k: t}.k), $(t * 2), $(ts[0..1][0])) is part of the type expression.
func blankRelationshipExpressions(text string) string {
	text = blankQuotedText(text)
	var out []byte
	for index := 0; index < len(text); index++ {
		if text[index] != '(' {
			continue
		}
		end := findMatchingDelimiter(text, index, '(', ')')
		if end < 0 {
			break
		}
		if out == nil {
			out = []byte(text)
		}
		for inside := index + 1; inside < end; inside++ {
			out[inside] = ' '
		}
		index = end
	}
	if out == nil {
		return text
	}
	return string(out)
}

// addType adds one type item as written: unquoted when backtick-quoted,
// noted in invalidType when it names no type (an empty quoted name, or an
// unquoted one that is neither dynamic nor an identifier).
func (declaration *relationshipDeclaration) addType(item string) {
	switch written := item; {
	case item == "":
		return
	case item[0] == '`':
		if item = symbolicNameValue(item); item == "" && declaration.invalidType == "" {
			declaration.invalidType = written
		}
	case hasDynamicToken(item):
	case !isValidIdentifier(item) && declaration.invalidType == "":
		declaration.invalidType = item
	}
	declaration.types = append(declaration.types, item)
}

// singleType is the declaration's one type for CREATE and MERGE: its name,
// or the type text as written when it is dynamic or names more or fewer
// than one type (the shape validators reject those first).
func (declaration relationshipDeclaration) singleType() string {
	if len(declaration.types) == 1 && !hasDynamicToken(declaration.typeText) {
		return declaration.types[0]
	}
	return declaration.typeText
}

// boundOutgoingRelationship reads (a)-[inside]->(b), the shape the UNWIND
// batch plans take, into its two node variables (simple identifiers) and its
// relationship's declaration; ok is false for any other shape.
func boundOutgoingRelationship(body string) (start string, declaration relationshipDeclaration, end string, ok bool) {
	body = strings.TrimSpace(body)
	if !strings.HasPrefix(body, "(") {
		return "", declaration, "", false
	}
	startEnd := findMatchingParen(body, 0)
	if startEnd < 0 {
		return "", declaration, "", false
	}
	rest := strings.TrimSpace(body[startEnd+1:])
	if !strings.HasPrefix(rest, "-[") {
		return "", declaration, "", false
	}
	close := findMatchingBracket(rest, 1)
	if close < 0 {
		return "", declaration, "", false
	}
	after := strings.TrimSpace(rest[close+1:])
	if !strings.HasPrefix(after, "->") {
		return "", declaration, "", false
	}
	target := strings.TrimSpace(after[2:])
	if !strings.HasPrefix(target, "(") || findMatchingParen(target, 0) != len(target)-1 {
		return "", declaration, "", false
	}
	start, end = strings.TrimSpace(body[1:startEnd]), strings.TrimSpace(target[1:len(target)-1])
	if !isSimpleIdentifier(start) || !isSimpleIdentifier(end) {
		return "", declaration, "", false
	}
	return start, relationshipDeclarationOf(rest[2:close]), end, true
}

// singleStaticType is the declaration's one type when it names exactly one
// type by a plain name, with no length and no predicate: what the fast-path
// plans accept. ok is false for anything else.
func (declaration relationshipDeclaration) singleStaticType() (string, bool) {
	if len(declaration.types) != 1 || declaration.invalidType != "" || declaration.hasLength || declaration.lengthInvalid ||
		declaration.predicate != "" || !isSimpleIdentifier(declaration.types[0]) {
		return "", false
	}
	return declaration.types[0], true
}

// propertyMapBody is the text inside the declaration's property map ({…}),
// trimmed; ok is false when it has none or a parameter map.
func (declaration relationshipDeclaration) propertyMapBody() (string, bool) {
	properties := declaration.properties
	if len(properties) < 2 || properties[0] != '{' || findMatchingDelimiter(properties, 0, '{', '}') != len(properties)-1 {
		return "", false
	}
	return strings.TrimSpace(properties[1 : len(properties)-1]), true
}
