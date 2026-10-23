package antlr

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// TransactionBatch is immutable syntax, independent of runtime row values.
type TransactionBatch struct {
	Expression            string
	LiteralKind           string
	Concurrent            bool
	ConcurrencyExpression string
	ErrorMode             string
	ReportVariable        string
	expressionSyntax      SyntaxNode
}

// ExpressionSyntax is the size expression in its original retained statement.
// The implicit default has no written node. Explicit consumers never re-lex it.
func (spec *TransactionBatch) ExpressionSyntax() (SyntaxNode, bool) {
	return spec.expressionSyntax, spec.expressionSyntax.Valid()
}

// CallClause is a compatibility view of an existing canonical CALL node.
type CallClause struct {
	Body           string
	After          string
	Batch          *TransactionBatch
	PlacementError string
}

// ReadCallClause is the standalone entry adapter. Statement consumers call
// DescribeCall on the retained node instead of parsing a second CALL fragment.
func ReadCallClause(ctx context.Context, query string) (CallClause, error) {
	syntax, err := ParseSyntax(ctx, query, FragmentEntry, false)
	if err != nil {
		return CallClause{}, err
	}
	call, found := syntax.LeadingCall()
	if !found || call.Rule() != CypherParserRULE_callSubquery {
		message := localization.CypherANTLRParseSyntaxError("expected a CALL subquery")
		return CallClause{}, localization.NewLocalizedError(string(message.ID), message, nil)
	}
	return DescribeCall(call), nil
}

// ReadTransactionalCalls consumes the one product and traversal. Placement
// state belongs to each enclosing query; independently committed writes are
// not writes of the containing query. No preliminary lexer or recursion remains.
func ReadTransactionalCalls(ctx context.Context, query string) ([]CallClause, error) {
	syntax, err := ParseSyntax(ctx, query, StatementEntry, false)
	if err != nil {
		return nil, err
	}
	candidate := false
	for index := 0; index < syntax.TokenCount(); index++ {
		if syntax.Token(index).Kind() == CypherLexerTRANSACTIONS {
			candidate = true
			break
		}
	}
	if !candidate {
		return nil, nil
	}
	type frame struct {
		writes bool
		union  bool
	}
	current := &frame{}
	nesting := 0
	var calls []CallClause
	syntax.Walk(func(node SyntaxNode) func() {
		switch node.Rule() {
		case CypherParserRULE_regularQuery:
			old := current.union
			current.union = node.ChildRule(CypherParserRULE_unionSt).Valid()
			return func() { current.union = old }
		case CypherParserRULE_callSubquery:
			call := DescribeCall(node)
			if call.Batch != nil {
				switch {
				case nesting != 0:
					call.PlacementError = "nested"
				case current.union:
					call.PlacementError = "union"
				case current.writes:
					call.PlacementError = "write"
				}
			}
			calls = append(calls, call)
			parent := current
			current = &frame{}
			nesting++
			return func() {
				if call.Batch == nil {
					parent.writes = parent.writes || current.writes
				}
				current = parent
				nesting--
			}
		case CypherParserRULE_existsSubquery, CypherParserRULE_subqueryExist,
			CypherParserRULE_countSubquery, CypherParserRULE_collectSubquery:
			nesting++
			return func() { nesting-- }
		case CypherParserRULE_updatingStatement:
			return func() { current.writes = true }
		}
		return nil
	})
	return calls, ctx.Err()
}

// DescribeCall reads an already parsed callSubquery rule. Its body, suffix and
// batch expressions remain slices of the original source, including Unicode.
func DescribeCall(call SyntaxNode) CallClause {
	open, _ := call.DirectToken(CypherLexerLBRACE)
	close, _ := call.DirectToken(CypherLexerRBRACE)
	_, bodyStart := open.Span()
	bodyEnd, _ := close.Span()
	_, end := call.Span()
	result := CallClause{
		Body:  strings.TrimSpace(call.data.source[bodyStart:bodyEnd]),
		After: strings.TrimSpace(call.data.source[end:]),
	}
	batch := call.ChildRule(CypherParserRULE_transactionBatch)
	if !batch.Valid() {
		return result
	}
	spec := &TransactionBatch{Expression: "1000", LiteralKind: "Integer"}
	afterOf := false
	for index := 0; index < batch.ChildCount(); index++ {
		child := batch.Child(index)
		switch {
		case child.IsToken(CypherLexerOF):
			afterOf = true
		case child.IsToken(CypherLexerCONCURRENT):
			spec.Concurrent = true
		case child.IsToken(CypherLexerCONTINUE), child.IsToken(CypherLexerBREAK), child.IsToken(CypherLexerFAIL):
			spec.ErrorMode = child.Text()
		case child.Rule() == CypherParserRULE_symbol:
			spec.ReportVariable, _ = child.Name()
		case child.Rule() == CypherParserRULE_expression:
			if afterOf {
				spec.Expression, spec.LiteralKind = child.Text(), batchLiteralKind(child)
				spec.expressionSyntax = child
			} else {
				spec.ConcurrencyExpression = child.Text()
			}
		}
	}
	result.Batch = spec
	return result
}

// batchLiteralKind follows only the existing literal/unary-parenthesis shape;
// computed expressions remain runtime-dependent, rather than guessed by value.
func batchLiteralKind(node SyntaxNode) string {
	for node.Rule() != CypherParserRULE_literal {
		var next SyntaxNode
		for index := 0; index < node.ChildCount(); index++ {
			child := node.Child(index)
			if child.Rule() < 0 {
				if !child.IsToken(CypherLexerPLUS) && !child.IsToken(CypherLexerSUB) && !child.IsToken(CypherLexerLPAREN) && !child.IsToken(CypherLexerRPAREN) {
					return ""
				}
			} else {
				if next.Valid() {
					return ""
				}
				next = child
			}
		}
		if !next.Valid() {
			return ""
		}
		node = next
	}
	if _, null := node.DirectToken(CypherLexerNULL_W); null {
		return "Null"
	}
	for _, kind := range []struct {
		rule int
		name string
	}{
		{CypherParserRULE_boolLit, "Boolean"}, {CypherParserRULE_stringLit, "String"},
		{CypherParserRULE_charLit, "String"}, {CypherParserRULE_listLit, "List"},
		{CypherParserRULE_mapLit, "Map"},
	} {
		if node.ChildRule(kind.rule).Valid() {
			return kind.name
		}
	}
	if number := node.ChildRule(CypherParserRULE_numLit); number.Valid() {
		if _, floating := number.DirectToken(CypherLexerFLOAT); floating {
			return "Float"
		}
		return "Integer"
	}
	return ""
}
