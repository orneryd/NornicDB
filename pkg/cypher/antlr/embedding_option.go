package antlr

import antlr4 "github.com/antlr4-go/antlr/v4"

// EmbeddingOptionTerminal recognizes only the syntactically unambiguous option.
// WITH has been consumed before this check; keeping its first token visible
// prevents this option predicate from obscuring unrelated invocation lookahead.
// Nonterminal WITH EMBEDDING remains a projection candidate; the execution
// pipeline's binding frame owns its interpretation, just like other variables.
func (p *CypherParser) EmbeddingOptionTerminal() bool {
	stream := p.GetTokenStream()
	if stream.LT(-1) == nil || stream.LT(-1).GetTokenType() != CypherParserWITH || stream.LA(1) != CypherParserEMBEDDING {
		return false
	}
	switch stream.LA(2) {
	case antlr4.TokenEOF, CypherParserSEMI, CypherParserRBRACE, CypherParserUNION:
		return true
	}
	return false
}
