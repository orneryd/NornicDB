// Author: Codex R17 implementation agent, 27 October 2026.
package antlr

import antlr4 "github.com/antlr4-go/antlr/v4"

type caseResultFrame struct {
	context antlr4.ParserRuleContext
	prior   int
}

// CASE value expressions need their enclosing context to distinguish their
// end from another CASE arm. Predict them with LL before resuming the outer
// SLL parse, avoiding a failed fast parse followed by a whole-source retry.
// The grammar-produced parent and terminal identify the boundary; this owner
// neither scans source text nor changes the grammar or error-retry contract.
type caseResultPrediction struct {
	parser *CypherParser
	mode   int
	frames []caseResultFrame
}

func (*caseResultPrediction) VisitTerminal(antlr4.TerminalNode) {}
func (*caseResultPrediction) VisitErrorNode(antlr4.ErrorNode)   {}

func (policy *caseResultPrediction) EnterEveryRule(context antlr4.ParserRuleContext) {
	if context.GetRuleIndex() != CypherParserRULE_expression {
		return
	}
	parent, ok := context.GetParent().(antlr4.ParserRuleContext)
	if !ok || parent.GetRuleIndex() != CypherParserRULE_caseExpression {
		return
	}
	children := parent.GetChildren()
	if len(children) < 2 {
		return
	}
	terminal, ok := children[len(children)-2].(antlr4.TerminalNode)
	if !ok {
		return
	}
	kind := terminal.GetSymbol().GetTokenType()
	if kind != CypherParserTHEN && kind != CypherParserELSE {
		return
	}
	policy.frames = append(policy.frames, caseResultFrame{context: context, prior: policy.mode})
	policy.mode = antlr4.PredictionModeLL
	policy.parser.Interpreter.SetPredictionMode(policy.mode)
}

func (policy *caseResultPrediction) ExitEveryRule(context antlr4.ParserRuleContext) {
	if len(policy.frames) == 0 {
		return
	}
	index := len(policy.frames) - 1
	frame := policy.frames[index]
	if frame.context != context {
		return
	}
	policy.frames[index] = caseResultFrame{}
	policy.frames = policy.frames[:index]
	policy.mode = frame.prior
	policy.parser.Interpreter.SetPredictionMode(policy.mode)
}
