package cypher

import (
	"github.com/orneryd/nornicdb/pkg/fabric"
	"strings"
)

// admittedClauseOperand is an immutable source view plus the current semantic
// text. The latter can mask already-admitted iteration predicates while the
// native input retains exact spans and local-scope provenance.
type admittedClauseOperand struct {
	fabric.QueryOperand
	expression string
	frame      staticTypeScope
}

// scope binds this selected view without reconstructing its native interval or
// allocating a second frame merely to apply the already-admitted mask.
func (operand admittedClauseOperand) scope(input staticTypeScope) staticTypeScope {
	input = input.withNativeInput(operand.Input)
	if input.native != nil {
		input.native.text = operand.expression
	}
	return input
}

type admittedOperandSelection struct {
	native   bool
	framed   bool
	operands []admittedClauseOperand
}

// An admitted legacy selection is different from a missing selection and from
// an admitted empty native operand list. It has no per-query allocation.
var legacyOperandSelection = &admittedOperandSelection{}

func admitClauseOperands(clause pipelineClause) pipelineClause {
	if clause.admission != nil {
		return clause
	}
	clause.admission = legacyOperandSelection
	if clause.syntax == nil && clause.input.Valid() && !clause.noLocalScopes {
		needs := false
		_ = forEachClauseOperatorExpression(clause, func(expression string, _ bool) error {
			needs = needs || fabric.QuerySyntaxDemand(expression).Has(fabric.NativeLocalScopes)
			return nil
		})
		if needs {
			clause.syntax = clause.input.NativeClause()
		}
	}
	if clause.syntax == nil || !clause.syntax.Input.Valid() || clause.kind == pipelineClauseCall || clause.kind == pipelineClauseCallSubquery {
		return clause
	}
	clause.admission = &admittedOperandSelection{native: true}
	for _, operand := range clause.syntax.Operands() {
		expression := operand.Input.Text()
		if start, end, ok := operand.Input.RangeIn(clause.syntax.Input); ok && len(clause.text) == len(clause.syntax.Input.Text()) {
			expression = clause.text[start:end]
		}
		if strings.TrimSpace(expression) == "" {
			continue
		}
		if operand.Projected && !operand.Predicate && clause.syntax.Projection != nil {
			items := clause.syntax.Projection.Items
			item := func(index int) (string, string) { return items[index].Expression, items[index].Column }
			if orderedProjectionExpressions([]orderByTerm{{column: expression}}, len(items), item) != nil {
				continue
			}
		}
		clause.admission.operands = append(clause.admission.operands, admittedClauseOperand{QueryOperand: operand, expression: expression})
	}
	return clause
}

// admittedProjection is produced with the statement descriptor. Native
// declarations are borrowed; a deliberately absent native tree still has the
// physical parser's complete WITH declaration facts. Nil never means empty.
func admittedProjection(clause pipelineClause) *fabric.QueryProjection {
	if clause.projection != nil {
		return clause.projection
	}
	if clause.syntax != nil && clause.syntax.Projection != nil {
		return clause.syntax.Projection
	}
	keyword := "WITH"
	if clause.kind == pipelineClauseReturn {
		keyword = "RETURN"
	}
	clause = physicalClauseFacts(clause)
	body, tail := clause.projectionBody, clause.projectionTail
	_, distinct := cutDistinct(strings.TrimSpace(clause.text[len(keyword):]))
	projection := &fabric.QueryProjection{Text: body, Tail: tail, Modifiers: tail, Distinct: distinct}
	for _, raw := range splitTopLevelComma(body) {
		var item fabric.QueryProjectionItem
		expression, column := projectionExprAliasFacts(strings.TrimSpace(raw), &item)
		if expression == "*" {
			projection.Star = true
			continue
		}
		if expression == "" || column == "" {
			continue
		}
		projection.Items = append(projection.Items, item)
	}
	return projection
}

func projectClauseTypeScope(types typeScope, input staticTypeScope, clause pipelineClause) typeScope {
	return projectNativeTypeScope(types, input, admittedProjection(clause))
}

// admitOperandFrames is the sole producer of typed operand views. Both static
// validators borrow these frames, including enclosing iteration declarations.
func admitOperandFrames(clause pipelineClause, scope staticTypeScope, projected func() staticTypeScope) pipelineClause {
	if !clause.admission.native || clause.admission.framed {
		return clause
	}
	root := scope
	if root.native == nil || root.native.input != clause.input {
		root = root.withClauseInput(clause)
	}
	if root.native != nil && root.native.text != clause.text {
		root = root.withNativeMask(clause.text)
	}
	for index := range clause.admission.operands {
		operand := &clause.admission.operands[index]
		view := scope
		if operand.Projected && projected != nil {
			view = projected()
		}
		view.native, view.noLocalScopes = root.native, root.noLocalScopes
		if at, _, ok := operand.Input.RangeIn(clause.input); ok {
			view = withPatternComprehensionBindings(withLocalTypes(view, clause.text, at), clause.text, at)
		}
		operand.frame = operand.scope(view)
	}
	clause.admission.framed = true
	return clause
}
