package fabric

import (
	grammar "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/localization"
	"strings"
)

// Graph references and the optional GRAPH token are owned by the enclosing
// grammar. A clause-keyword spelling is still a name in this production.
func (r *structureReader) useReference(node grammar.IFabricUseClauseContext) (UseClause, bool) {
	if call := node.FunctionInvocation(); call != nil {
		name := call.GetStart().GetText()
		if invocation := call.InvocationName(); invocation != nil {
			parts := make([]string, 0, len(invocation.AllSymbol()))
			for _, part := range invocation.AllSymbol() {
				parts = append(parts, unquoteSyntaxName(part.GetText()))
			}
			name = strings.Join(parts, ".")
		}
		// An invalid argument may stop parsing before the closing delimiter.
		// Retain the grammar-established function identity for routing; the
		// incomplete structure keeps its syntax error and cannot be executed.
		if call.LPAREN() == nil || call.RPAREN() == nil {
			return UseClause{Function: name}, true
		}
		args := splitArguments(r.text[r.end(call.LPAREN().GetSymbol()):r.start(call.RPAREN().GetSymbol())])
		return UseClause{Function: name, Args: args}, true
	}
	parts := make([]string, 0, len(node.AllSymbol()))
	for _, part := range node.AllSymbol() {
		if part.GetStop() == nil {
			return UseClause{}, false
		}
		parts = append(parts, unquoteSyntaxName(part.GetText()))
	}
	return UseClause{Name: strings.Join(parts, ".")}, len(parts) > 0
}

// This is admission policy over actual clause kinds, not a recognizer over
// words following a graph name. Error descriptors keep the established API.
func (q *QueryStructure) validateUseAt(index int, inSubquery bool) error {
	clause := q.Clauses[index]
	if clause.endsStatement && q.statements > clause.statement {
		return useSyntaxError(localization.CypherCommandRoutingMultipleStatements(q.statements - clause.statement + 1))
	}
	if clause.endsStatement || index+1 == len(q.Clauses) {
		if inSubquery {
			return useSyntaxError(localization.CypherCommandRoutingUseSubqueryMustConclude())
		}
		return useSyntaxError(localization.CypherCommandRoutingUseQueryCannotConclude())
	}
	next := q.Clauses[index+1]
	allowed := true
	switch next.Kind {
	case "EXPLAIN", "PROFILE", "CYPHER", "LET", "FILTER", "FOR":
		allowed = false
	case "SHOW", "TERMINATE":
		allowed = !inSubquery
	case "SCHEMA":
		allowed = !inSubquery || !next.schemaDrop
	}
	if !allowed {
		token := inputToken(next.Text, 0)
		if inSubquery {
			return useSyntaxError(localization.CypherCommandRoutingUseInvalidSubqueryClauseAfterGraph(token))
		}
		return useSyntaxError(localization.CypherCommandRoutingUseInvalidClauseAfterGraph(token))
	}
	if next.Kind == "USE" {
		return useSyntaxError(localization.CypherCommandRoutingUseNotFirstClause())
	}
	if !inSubquery && next.administration {
		return useSyntaxError(localization.CypherCommandRoutingUseAdministrationCommand())
	}
	return nil
}

func (q *QueryStructure) validateUses(inSubquery bool) error {
	for index, clause := range q.Clauses {
		if clause.Kind == "USE" {
			if err := q.validateUseAt(index, inSubquery); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *structureReader) useReferenceStart(node grammar.IFabricUseClauseContext) int {
	if call := node.FunctionInvocation(); call != nil {
		return r.start(call.GetStart())
	}
	return r.start(node.AllSymbol()[0].GetStart())
}
