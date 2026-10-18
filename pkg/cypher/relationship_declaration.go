package cypher

import "strings"

// A relationship pattern's inside ([r:A|B *1..2 {k: v}]) is read by one
// scanner for CREATE, MERGE and MATCH (relationshipDeclarationOf, and
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
