package cyphertext

import (
	"strings"
	"unsafe"
)

// RelationshipType is an owner-classified token. Its decoded name never
// becomes syntax again: a quoted `$(R)` is a literal type.
type RelationshipType struct {
	name            string
	quoted, dynamic bool
}

func (value RelationshipType) StorageName() string { return value.name }
func (value RelationshipType) IsQuoted() bool      { return value.quoted }
func (value RelationshipType) IsDynamic() bool     { return value.dynamic }
func (value RelationshipType) MatchesStorageName(name string) bool {
	return !value.dynamic && value.name == name
}

// RelationshipTypes keeps the common single token inline. Only this package
// constructs it; parser adapters and semantic consumers retain its provenance.
type RelationshipTypes struct {
	first RelationshipType
	rest  []RelationshipType
	count int
}

func (types RelationshipTypes) Len() int { return types.count }
func (types RelationshipTypes) At(index int) RelationshipType {
	if index == 0 {
		return types.first
	}
	return types.rest[index-1]
}
func (types *RelationshipTypes) append(value RelationshipType) {
	if types.count == 0 {
		types.first = value
	} else {
		types.rest = append(types.rest, value)
	}
	types.count++
}
func (types RelationshipTypes) Bytes() int {
	size := cap(types.rest) * int(unsafe.Sizeof(RelationshipType{}))
	for index := 0; index < types.Len(); index++ {
		size += len(types.At(index).name)
	}
	return size
}

// ReadRelationshipTypes is the relationship owner's existing type-list phase,
// shared by the executor declaration and generated-parser metadata adapters.
// It consumes written declaration syntax; no consumer splits decoded names.
func ReadRelationshipTypes(text string) (types RelationshipTypes, invalid string, empty, alternatives bool) {
	// A complete unquoted identifier has no type-list separators or dynamic
	// expression. Let the canonical identifier phase prove that once; the
	// general list phase below owns every other written representation.
	if name := strings.TrimSpace(text); IsIdentifier(name) {
		types.append(RelationshipType{name: name})
		return
	}
	depth, start := 0, 0
	for index := 0; index <= len(text); index++ {
		if index < len(text) {
			if end, opaque := SkipToken(text, index); opaque {
				index = end - 1
				continue
			}
			switch c := text[index]; {
			case c == '(':
				depth++
				continue
			case c == ')':
				depth--
				continue
			case depth > 0 || c != '|' && c != ':':
				continue
			}
			alternatives = alternatives || text[index] == '|'
		}
		item := strings.TrimSpace(text[start:index])
		fact := RelationshipType{name: item}
		written := item
		itemStart, unfinished := GapEnd(item, 0)
		switch {
		case unfinished >= 0:
			if invalid == "" {
				invalid = written
			}
		case itemStart == len(item):
			empty, fact.name = true, ""
		case !IsBacktick(item[itemStart]) && HasDynamicToken(item):
			fact.dynamic = true
		default:
			name, valid := ScanIdentifier(item, itemStart)
			end, unfinished := GapEnd(item, name.End)
			fact.quoted = name.Quoted
			if (!valid || unfinished >= 0 || end != len(item) || name.Value == "") && invalid == "" {
				invalid = written
			}
			if valid && unfinished < 0 && end == len(item) {
				item = name.Value
			}
		}
		if fact.name != "" {
			fact.name = item
			types.append(fact)
		}
		start = index + 1
	}
	return
}

// readLiteralType retains the written token's provenance and cursor. The
// protocol consumer decides which following token terminates its type item.
func readLiteralType(text string, start int) (RelationshipType, int, bool) {
	name, ok := ScanIdentifier(text, start)
	return RelationshipType{name: name.Value, quoted: name.Quoted}, name.End, ok && name.Value != ""
}

// DynamicLabelStartsAt recognizes the written dynamic token marker.
func DynamicLabelStartsAt(text string, index int) bool {
	if index+1 >= len(text) || text[index] != '$' {
		return false
	}
	if text[index+1] == '(' {
		return true
	}
	for _, function := range [...]string{"all(", "any("} {
		if index+1+len(function) <= len(text) && strings.EqualFold(text[index+1:index+1+len(function)], function) {
			return true
		}
	}
	return false
}

// HasDynamicToken skips quoted names before classifying dynamic syntax.
func HasDynamicToken(text string) bool {
	if strings.IndexByte(text, '$') < 0 {
		return false
	}
	for index := 0; index < len(text); index++ {
		if end, opaque := SkipToken(text, index); opaque {
			index = end - 1
			continue
		}
		if text[index] == '$' && DynamicLabelStartsAt(text, index) {
			return true
		}
	}
	return false
}
