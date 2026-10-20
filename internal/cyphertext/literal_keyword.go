package cyphertext

import "strings"

// LiteralKeyword is a Cypher literal word, before value construction.
type LiteralKeyword uint8

const (
	LiteralKeywordNone LiteralKeyword = iota
	LiteralKeywordNaN
	LiteralKeywordTrue
	LiteralKeywordNull
	LiteralKeywordFalse
	LiteralKeywordInfinity
)

// LiteralKeywordKind owns literal-word recognition for both binding and value
// readers. A binding lookup needs the syntax role without constructing a value.
func LiteralKeywordKind(name string) LiteralKeyword {
	switch len(name) {
	case 3:
		if strings.EqualFold(name, "NaN") {
			return LiteralKeywordNaN
		}
	case 4:
		if strings.EqualFold(name, "true") {
			return LiteralKeywordTrue
		}
		if strings.EqualFold(name, "null") {
			return LiteralKeywordNull
		}
	case 5:
		if strings.EqualFold(name, "false") {
			return LiteralKeywordFalse
		}
	case 8:
		if strings.EqualFold(name, "Infinity") {
			return LiteralKeywordInfinity
		}
	}
	return LiteralKeywordNone
}
