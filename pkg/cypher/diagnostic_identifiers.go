package cypher

import (
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	cantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Only proven binding names and their qualified property names bypass the
// conservative diagnostic fallback. Unknown dotted bare words remain redacted.
// Public grammar-validated redaction retains its existing invalid-input policy.
func diagnosticBoundIdentifiers(tokens []antlr4.Token) map[int]bool {
	type item struct {
		token antlr4.Token
		index int
	}
	var significant []item
	for i, token := range tokens {
		if token.GetChannel() == antlr4.TokenDefaultChannel {
			significant = append(significant, item{token, i})
		}
	}
	bound := map[string]bool{}
	pattern := false
	depth := 0
	identifier := func(token antlr4.Token) bool {
		return token.GetTokenType() == cantlr.CypherLexerID || token.GetTokenType() == cantlr.CypherLexerESC_LITERAL
	}
	for i, current := range significant {
		typeID := current.token.GetTokenType()
		switch strings.ToUpper(current.token.GetText()) {
		case "MATCH", "CREATE", "MERGE":
			pattern = true
		case "WHERE", "WITH", "RETURN", "SET", "DELETE", "UNWIND":
			pattern = false
		}
		if typeID == cantlr.CypherLexerLPAREN {
			if pattern && depth == 0 && i+1 < len(significant) && identifier(significant[i+1].token) {
				bound[significant[i+1].token.GetText()] = true
			}
			depth++
		} else if typeID == cantlr.CypherLexerRPAREN {
			depth--
		}
		if typeID == cantlr.CypherLexerLBRACK && pattern && i+1 < len(significant) && identifier(significant[i+1].token) {
			bound[significant[i+1].token.GetText()] = true
		}
		if typeID == cantlr.CypherLexerAS && i+1 < len(significant) && identifier(significant[i+1].token) {
			bound[significant[i+1].token.GetText()] = true
		}
	}
	known := map[int]bool{}
	for i, current := range significant {
		if !identifier(current.token) {
			continue
		}
		if bound[current.token.GetText()] {
			known[current.index] = true
		}
		if i >= 2 && significant[i-1].token.GetTokenType() == cantlr.CypherLexerDOT && known[significant[i-2].index] {
			known[current.index] = true
		}
	}
	return known
}
