package cypher

import "github.com/orneryd/nornicdb/internal/cyphertext"

// These lexical adapters are used after a caller has skipped strings and
// comments. Brace *meaning* and entry parsing stay in brace_syntax.go.
func isBraceOpen(c byte) bool                 { return cyphertext.IsOpenBrace(c) }
func isBraceClose(c byte) bool                { return cyphertext.IsCloseBrace(c) }
func firstLiveBrace(text string) int          { return cyphertext.FirstBrace(text, 0) }
func firstLiveBraceClose(text string) int     { return cyphertext.FirstSymbol(text, "}") }
func lastLiveBraceClose(text string) int      { return cyphertext.LastSymbol(text, "}") }
func syntaxByteInSet(set string, c byte) bool { return cyphertext.SyntaxByteInSet(set, c) }
func containsSyntaxAny(text, set string) bool { return cyphertext.ContainsSyntaxAny(text, set) }
func startsBrace(text string) bool            { return len(text) > 0 && isBraceOpen(text[0]) }
func endsBrace(text string) bool              { return len(text) > 0 && isBraceClose(text[len(text)-1]) }

// Author: #1663 implementation lane. Enclosure consumers read the final map
// role and paired endpoints; a whole braced query body cannot become a map.
func enclosingMapText(view braceView) bool {
	fact, found := view.at(0)
	if !found || fact.kind != literalBrace && fact.kind != patternPropertyBrace {
		return false
	}
	close, complete := view.close(0)
	return complete && close == len(view.text())-1
}
