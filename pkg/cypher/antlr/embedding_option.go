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

// singleQueryClauses returns the clauses read so far of the query being
// parsed: the children of the nearest enclosing singleQuery, the clause in
// progress included. A subquery body is not a singleQuery; a prediction made
// inside one sees the query that holds it.
func (p *CypherParser) singleQueryClauses() []antlr4.Tree {
	for context := p.GetParserRuleContext(); context != nil; {
		if query, ok := context.(*SingleQueryContext); ok {
			return query.GetChildren()
		}
		parent, _ := context.GetParent().(antlr4.ParserRuleContext)
		context = parent
	}
	return nil
}

// embeddingOptionEnds reports whether WITH EMBEDDING at the cursor is the
// option that ends the query being read rather than a projection named
// embedding: it stands where the query ends (EmbeddingOptionTerminal), and the
// query has written and has not projected. For example CREATE (n) WITH
// EMBEDDING ends with the option; MATCH (n) WITH EMBEDDING and CREATE (a) WITH
// a CREATE (b) WITH EMBEDDING end with a projection.
func (p *CypherParser) embeddingOptionEnds() bool {
	if !p.EmbeddingOptionTerminal() {
		return false
	}
	updates := false
	for _, clause := range p.singleQueryClauses() {
		switch clause.(type) {
		case *WithStContext:
			return false
		case *UpdatingStatementContext:
			updates = true
		}
	}
	return updates
}

// orderEndsCallSubquery reports whether ORDER BY may end the query being read:
// the part it ends, the clauses after the last WITH, is one CALL subquery
// (CALL { ... } ORDER BY x).
func (p *CypherParser) orderEndsCallSubquery() bool {
	part := p.singleQueryClauses()
	for index := len(part) - 1; index >= 0; index-- {
		if _, projection := part[index].(*WithStContext); projection {
			part = part[index+1:]
			break
		}
	}
	if len(part) != 1 {
		return false
	}
	reading, ok := part[0].(*ReadingStatementContext)
	return ok && reading.CallSubquery() != nil
}
