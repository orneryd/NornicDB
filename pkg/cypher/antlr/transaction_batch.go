package antlr

import (
	"context"
	"strings"

	antlr4 "github.com/antlr4-go/antlr/v4"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// TransactionBatch is syntax, independent of an execution's parameters.
// LiteralKind describes the expression's Cypher literal, through parentheses
// and a unary sign; a computed expression is not a literal.
type TransactionBatch struct {
	Expression            string
	LiteralKind           string
	Concurrent            bool
	ConcurrencyExpression string
	ErrorMode             string
	ReportVariable        string
}

// CallClause is the shared structural read used by execution and validation.
type CallClause struct {
	Body           string
	After          string
	Batch          *TransactionBatch
	PlacementError string
}

type batchReadRule uint8

const (
	batchReadStatement batchReadRule = iota
	batchReadCall
	batchReadClause
	batchReadExpression
)

// ReadCallClause reads one CALL prefix, leaving any following clause in After.
func ReadCallClause(ctx context.Context, query string) (call CallClause, err error) {
	err = readBatchTree(ctx, query, batchReadCall, func(tree antlr4.Tree, text []rune) {
		call = describeCall(tree.(*CallSubqueryContext), text)
	})
	return
}

// ReadTransactionalCalls reads enclosing query structure as well as the batch
// production. Default and ANTLR execution share these exact token boundaries.
func ReadTransactionalCalls(ctx context.Context, query string) (calls []CallClause, err error) {
	tokens, err := LexContext(ctx, query)
	if err != nil {
		return nil, err
	}
	candidate := false
	for _, token := range tokens {
		if token.GetTokenType() == CypherLexerTRANSACTIONS {
			candidate = true
			break
		}
	}
	if !candidate {
		return nil, nil
	}
	err = readBatchTree(ctx, query, batchReadStatement, func(tree antlr4.Tree, text []rune) {
		// Each recursion returns writes owned by its containing query. A
		// transactional CALL's writes belong to its independent batches.
		var walk func(antlr4.Tree, int, bool, bool) bool
		walk = func(node antlr4.Tree, nesting int, union, precedingWrite bool) bool {
			switch n := node.(type) {
			case *RegularQueryContext:
				union = len(n.AllUnionSt()) != 0
			case *CallSubqueryContext:
				call := describeCall(n, text)
				if call.Batch != nil {
					switch {
					case nesting != 0:
						call.PlacementError = "nested"
					case union:
						call.PlacementError = "union"
					case precedingWrite:
						call.PlacementError = "write"
					}
				}
				calls = append(calls, call)
				writes := walk(n.SubqueryBody(), nesting+1, false, false)
				return call.Batch == nil && writes
			case *ExistsSubqueryContext, *SubqueryExistContext, *CountSubqueryContext, *CollectSubqueryContext:
				nesting++
			}
			writes := false
			for _, child := range node.GetChildren() {
				writes = walk(child, nesting, union, precedingWrite || writes) || writes
			}
			if _, updating := node.(*UpdatingStatementContext); updating {
				return true
			}
			return writes
		}
		walk(tree, 0, false, false)
	})
	if err != nil {
		return nil, err
	}
	return calls, nil
}

func describeCall(call *CallSubqueryContext, text []rune) CallClause {
	result := CallClause{
		Body:  strings.TrimSpace(string(text[call.LBRACE().GetSymbol().GetStop()+1 : call.RBRACE().GetSymbol().GetStart()])),
		After: strings.TrimSpace(string(text[call.GetStop().GetStop()+1:])),
	}
	if batch, ok := call.TransactionBatch().(*TransactionBatchContext); ok {
		spec := &TransactionBatch{Expression: "1000", Concurrent: batch.CONCURRENT() != nil}
		if expression := batch.GetSize(); expression != nil {
			spec.Expression = sourceExpression(expression, text)
			spec.LiteralKind = batchLiteralKind(expression)
		} else {
			spec.LiteralKind = "Integer"
		}
		if expression := batch.GetConcurrency(); expression != nil {
			spec.ConcurrencyExpression = sourceExpression(expression, text)
		}
		if mode := batch.GetErrorMode(); mode != nil {
			spec.ErrorMode = mode.GetText()
		}
		if variable := batch.GetStatusVariable(); variable != nil {
			spec.ReportVariable = variable.GetText()
		}
		result.Batch = spec
	}
	return result
}

func sourceExpression(expression antlr4.ParserRuleContext, text []rune) string {
	return string(text[expression.GetStart().GetStart() : expression.GetStop().GetStop()+1])
}

func batchLiteralKind(node antlr4.Tree) string {
	if literal := batchLiteral(node); literal != nil {
		switch {
		case literal.NULL_W() != nil:
			return "Null"
		case literal.BoolLit() != nil:
			return "Boolean"
		case literal.StringLit() != nil, literal.CharLit() != nil:
			return "String"
		case literal.ListLit() != nil:
			return "List"
		case literal.MapLit() != nil:
			return "Map"
		case literal.NumLit() != nil:
			if literal.NumLit().(*NumLitContext).FLOAT() != nil {
				return "Float"
			}
			return "Integer"
		}
	}
	return ""
}

func batchLiteral(node antlr4.Tree) ILiteralContext {
	if literal, ok := node.(ILiteralContext); ok {
		return literal
	}
	var child antlr4.Tree
	for _, next := range node.GetChildren() {
		if terminal, ok := next.(antlr4.TerminalNode); ok {
			switch terminal.GetSymbol().GetTokenType() {
			case CypherLexerPLUS, CypherLexerSUB, CypherLexerLPAREN, CypherLexerRPAREN:
			default:
				return nil
			}
		} else {
			if child != nil {
				return nil
			}
			child = next
		}
	}
	if child == nil {
		return nil
	}
	return batchLiteral(child)
}

func readBatchTree(ctx context.Context, query string, rule batchReadRule, consume func(antlr4.Tree, []rune)) (err error) {
	defer recoverPredictionCancellation(&err)
	if err = ctx.Err(); err != nil {
		return err
	}
	lexer := NewCypherLexer(contextInputStream(ctx, query))
	listener := &parseErrorListener{}
	lexer.RemoveErrorListeners()
	lexer.AddErrorListener(listener)
	tokens := antlr4.NewCommonTokenStream(lexer, antlr4.TokenDefaultChannel)
	var stream antlr4.TokenStream = tokens
	if ctx.Done() != nil {
		stream = &contextTokenStream{TokenStream: tokens, ctx: ctx}
	}
	parser := NewCypherParser(stream)
	parser.RemoveErrorListeners()
	parser.AddErrorListener(listener)
	prediction := acquirePrediction()
	usePrediction(parser, prediction)
	defer func() { parser.Interpreter = nil; prediction.release() }()
	var tree antlr4.Tree
	switch rule {
	case batchReadCall:
		tree = parser.CallSubquery()
	case batchReadClause:
		tree = parser.SubqueryBody()
	case batchReadExpression:
		tree = parser.Expression()
	default:
		tree = parser.Script()
	}
	if (rule == batchReadClause || rule == batchReadExpression) && stream.LA(1) != antlr4.TokenEOF {
		listener.errors = append(listener.errors, "unexpected trailing input")
	}
	if len(listener.errors) != 0 {
		message := localization.CypherANTLRParseSyntaxError(strings.Join(listener.errors, "; "))
		return localization.NewLocalizedError(string(message.ID), message, nil)
	}
	consume(tree, []rune(query))
	return ctx.Err()
}
