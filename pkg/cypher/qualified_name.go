package cypher

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// qualifiedNameAt frames a dotted name, using the symbolic-name owner for each
// decoded component and its extent. Authorization and native administration
// share this framing; their eligibility and database policies stay with them.
func qualifiedNameAt(query string, offset int) (string, int, bool) {
	return separatedNameAt(query, offset, false)
}

// administrationNameAt retains the native catalog's dotted/hyphenated spelling.
func administrationNameAt(query string, offset int) (string, int, bool) {
	return separatedNameAt(query, offset, true)
}

// separatedNameAt owns component framing only. The primitive owns scanning,
// quotation, decoded values and numeric-component extents.
func separatedNameAt(query string, offset int, native bool) (string, int, bool) {
	i := offset
	skipSpace := func() {
		i, _ = cyphertext.GapEnd(query, i)
	}
	// Keep the ordinary dotted name as a source slice. Only quoted parts
	// or spaces around separators need normalization. Quoted interiors retain
	// every decoded byte; only separator trivia is removed.
	var normalized strings.Builder
	normalizing := false
	startName, endName, components := offset, offset, 0
	expectComponent := false
	separator := byte(0)
	for {
		skipSpace()
		if i >= len(query) {
			break
		}
		partStart := i
		identifier, ok := cyphertext.ScanIdentifier(query, i)
		part, next := identifier.Value, identifier.End
		if !ok && native && components > 0 && cyphertext.IsDigit(query[i]) {
			next = cyphertext.NamePartEnd(query, i)
			part, ok = query[i:next], true
		}
		if !ok {
			if expectComponent {
				return "", endName, false
			}
			break
		}
		expectComponent = false
		quoted := isCypherBacktickByte(query[i])
		i = next
		if components == 0 {
			startName = partStart
		}
		if !normalizing && (quoted || components > 0 && partStart != endName+1) {
			if components > 0 {
				normalized.WriteString(query[startName:endName])
			}
			normalizing = true
		}
		if normalizing {
			if components > 0 {
				normalized.WriteByte(separator)
			}
			normalized.WriteString(part)
		}
		endName = i
		components++
		skipSpace()
		if i < len(query) && (query[i] == '.' || native && query[i] == '-') {
			separator = query[i]
			i++
			expectComponent = true
			continue
		}
		break
	}
	if components == 0 || expectComponent {
		return "", endName, false
	}
	if normalizing {
		return normalized.String(), endName, true
	}
	return query[startName:endName], endName, true
}

// administrationName reads a complete native command name field. The enclosing
// command has already located its clause boundaries.
func administrationName(source string) (string, bool) {
	name, end, ok := administrationNameAt(source, 0)
	tail, unfinished := cyphertext.GapEnd(source, end)
	return name, ok && unfinished < 0 && tail == len(source)
}
