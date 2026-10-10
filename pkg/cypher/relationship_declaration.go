package cypher

import "strings"

// A relationship pattern's inside ([r:A|B *1..2 {k: v}]) is read by one
// scanner for CREATE, MERGE and MATCH (relationshipDeclarationOf): a
// backtick-quoted type may hold any character, ':', '|', '*', '{' and
// spaces included, and a doubled backtick inside it is one backtick, as
// labels are read (eachChainItem, #907).

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
	// properties is the property map, from its { to the end, trimmed, or a
	// trailing parameter ($props); "" when there is none.
	properties string
}

// relationshipDeclarationOf reads a relationship pattern's inside (the text
// between [ and ]). Types are separated by | (or by a second colon, the
// deprecated [:A|:B] form); empty items are skipped.
func relationshipDeclarationOf(inner string) relationshipDeclaration {
	var declaration relationshipDeclaration
	head := strings.TrimSpace(inner)
	if brace := indexByteOutsideBackticks(head, '{'); brace >= 0 {
		declaration.properties = strings.TrimSpace(head[brace:])
		head = head[:brace]
	} else if rest, parameter, ok := splitPatternParameterMap(head); ok {
		declaration.properties = parameter
		head = rest
	}
	if star := indexByteOutsideBackticks(head, '*'); star >= 0 {
		end := star + 1
		for end < len(head) && (isDigitByte(head[end]) || head[end] == '.') {
			end++
		}
		declaration.hasLength = true
		declaration.length = head[star+1 : end]
		head = head[:star] + head[end:]
	}
	colon := indexByteOutsideBackticks(head, ':')
	if colon < 0 {
		declaration.variable = strings.TrimSpace(head)
		return declaration
	}
	declaration.hasColon = true
	declaration.variable = strings.TrimSpace(head[:colon])
	declaration.typeText = strings.TrimSpace(head[colon+1:])
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
