package cypher

import (
	"errors"

	"github.com/orneryd/nornicdb/pkg/fabric"
)

// lowerPatternAdmission handles the native descriptor's explicit decline.
// It invokes the existing pattern lowerer, never treats a parse failure as
// partial binding facts, and leaves local expression helpers after routing.
func lowerPatternAdmission(source string, cypher25 bool, supplied ...*fabric.QueryStructure) (string, *queryRewrite, error) {
	var input *fabric.QueryStructure
	if len(supplied) > 0 {
		input = supplied[0]
	} else {
		input = logicalStatementSyntax(source)
	}
	var syntax *fabric.QuerySyntaxError
	if !errors.As(input.Err, &syntax) || !syntax.PatternLowering {
		return source, nil, nil
	}
	rewriter := &labelExpressionRewriter{query: source, cypher25: cypher25, admissionPatterns: true}
	if err := rewriter.statement(0, len(source)); err != nil {
		return source, nil, err
	}
	if len(rewriter.edits) == 0 {
		return source, nil, nil
	}
	lowered, restore := applyLabelRewriteEdits(source, rewriter.edits, true)
	return lowered, restore, nil
}
