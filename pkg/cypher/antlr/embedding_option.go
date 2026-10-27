package antlr

import antlr4 "github.com/antlr4-go/antlr/v4"

// EmbeddingOptionTerminal recognizes only the syntactically unambiguous option.
// Nonterminal WITH EMBEDDING remains a projection candidate; the execution
// pipeline's binding frame owns its interpretation, just like other variables.
func (p *CypherParser) EmbeddingOptionTerminal() bool {
	stream := p.GetTokenStream()
	if stream.LA(1) != CypherParserWITH || stream.LA(2) != CypherParserEMBEDDING {
		return false
	}
	switch stream.LA(3) {
	case antlr4.TokenEOF, CypherParserSEMI, CypherParserRBRACE, CypherParserUNION:
		return true
	}
	return false
}
