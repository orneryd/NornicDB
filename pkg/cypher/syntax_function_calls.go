package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// syntaxFunctionCall is one function call as the grammar reads it: a
// functionInvocation (including trim's FROM form) or a reduce / allReduce
// expression. Offsets are into the parsed source.
type syntaxFunctionCall struct {
	name, normalized string
	open, close      int // the call's "(" and ")"
	distinct         bool
	trimFrom         bool
	reduce           bool
	node             cypherantlr.SyntaxNode
	arguments        []cypherantlr.SyntaxNode
}

// syntaxFunctionCalls is the one reader that decides which source tokens are
// function calls: the grammar's invocation nodes under root, in source order.
// A procedure a CALL names, a USE graph selector, a pattern or a type in a
// type predicate is not a functionInvocation, so none of them is a call.
func syntaxFunctionCalls(root cypherantlr.SyntaxNode, visit func(syntaxFunctionCall) error) error {
	var failure error
	root.Walk(func(node cypherantlr.SyntaxNode) func() {
		if failure != nil {
			return nil
		}
		call, ok := syntaxFunctionCallAt(node)
		if ok {
			failure = visit(call)
		}
		return nil
	})
	return failure
}

func syntaxFunctionCallAt(node cypherantlr.SyntaxNode) (syntaxFunctionCall, bool) {
	call := syntaxFunctionCall{node: node}
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_functionInvocation:
		if invocation := node.ChildRule(cypherantlr.CypherParserRULE_invocationName); invocation.Valid() {
			call.name = invocation.QualifiedName()
			if chain := node.ChildRule(cypherantlr.CypherParserRULE_expressionChain); chain.Valid() {
				call.arguments = syntaxExpressions(chain)
			}
		} else {
			call.name, call.trimFrom = "trim", true
			call.arguments = syntaxExpressions(node)
		}
		_, call.distinct = node.DirectToken(cypherantlr.CypherLexerDISTINCT)
	case cypherantlr.CypherParserRULE_reduceExpression:
		call.name, call.reduce = "reduce", true
	case cypherantlr.CypherParserRULE_allReduceExpression:
		call.name, call.reduce = "allReduce", true
	default:
		return call, false
	}
	if call.name == "" {
		return call, false
	}
	open, found := node.DirectToken(cypherantlr.CypherLexerLPAREN)
	closing, closed := node.DirectToken(cypherantlr.CypherLexerRPAREN)
	if !found || !closed {
		return call, false
	}
	call.open, _ = open.Span()
	call.close, _ = closing.Span()
	call.normalized = lowerASCII(normalizeProjectionColumnName(call.name))
	return call, true
}

// textFunctionCalls reads the calls of a statement, a clause or an
// expression through the grammar. Text the grammar does not accept has no
// calls here: its syntax error belongs to admission.
func textFunctionCalls(ctx context.Context, text string, visit func(syntaxFunctionCall) error) error {
	// The statement's own Cypher version is read first; a call reads the
	// same in both versions, so text only the other version accepts is
	// read in that one.
	cypher25 := cypherVersionFromContext(ctx) == "25"
	for _, version := range []bool{cypher25, !cypher25} {
		for _, entry := range []cypherantlr.SyntaxEntry{cypherantlr.StatementEntry, cypherantlr.FragmentEntry, cypherantlr.ExpressionEntry} {
			syntax, err := cypherantlr.ParseSyntax(ctx, text, entry, version, cypherantlr.AcceptedSource)
			if cancelled := ctx.Err(); cancelled != nil {
				return cancelled
			}
			if err == nil && syntax.Valid() {
				return syntaxFunctionCalls(syntax.Root(), visit)
			}
		}
	}
	return nil
}

// syntaxArgumentTexts is the source text of each argument expression.
func syntaxArgumentTexts(call syntaxFunctionCall) []string {
	texts := make([]string, len(call.arguments))
	for i, argument := range call.arguments {
		texts[i] = argument.Code()
	}
	return texts
}
