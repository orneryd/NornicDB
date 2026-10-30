// Author: Codex R17 implementation agent, 29 October 2026; occurrence-form
// helpers: Claude R17 implementation agent, 30 October 2026.
package cypher

import (
	"context"
	"strings"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// These adapters preserve existing source-fixture assertions while the internal
// readers now require an explicitly admitted occurrence. Public execution tests
// continue to enter Execute without these test-only adapters.
func retainedClauseForTest(ctx context.Context, text string) pipelineClause {
	clause, admitted := fixtureClauseForTest(ctx, text)
	if !admitted {
		panic("test fixture is not an admitted pipeline clause: " + text)
	}
	return clause
}

// fixtureClauseForTest reads a test's clause text as a subquery body and
// returns its first clause. Production readers never enter this adapter: they
// address a clause of a statement they hold (retainedClauseAt).
func fixtureClauseForTest(ctx context.Context, text string) (pipelineClause, bool) {
	syntax, err := cypherantlr.ParseSyntax(ctx, text, cypherantlr.ClauseEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil || !syntax.Valid() {
		var absent pipelineClause
		return absent, false
	}
	body := readQueryBody(syntax.Root())
	if len(body.branches) > 0 && len(body.branches[0].clauses) > 0 {
		return body.branches[0].clauses[0].pipelineClause, true
	}
	var absent pipelineClause
	return absent, false
}

// returnPlanForTest is the projection plan of a RETURN or WITH fixture clause,
// or the absent plan when the text is not one.
func returnPlanForTest(ctx context.Context, text string) *returnProjectionPlan {
	clause, ok := fixtureClauseForTest(ctx, text)
	if !ok || clause.kind != pipelineClauseReturn && clause.kind != pipelineClauseWith {
		return absentProjectionPlan()
	}
	return projectionPlanOf(ctx, clause.syntax)
}

// returnItemsForTest lists the items of a RETURN or WITH fixture clause.
func returnItemsForTest(ctx context.Context, text string) []returnItem {
	return returnItemsOfPlan(returnPlanForTest(ctx, text))
}
func queryBodyForTest(ctx context.Context, text string) queryBody {
	return readQueryBodyText(ctx, text, 0, len(text))
}
func queryBodiesForTest(ctx context.Context, texts []string) []queryBody {
	var bodies []queryBody
	for _, text := range texts {
		bodies = append(bodies, queryBodyForTest(ctx, text))
	}
	return bodies
}
func callClauseBodySpanForTest(ctx context.Context, clause queryBodyClause) (int, int, bool) {
	_, ok := callClauseBody(clause)
	if !ok {
		return 0, 0, false
	}
	open, a := clause.syntax.DirectToken(cypherantlr.CypherLexerLBRACE)
	close, b := clause.syntax.DirectToken(cypherantlr.CypherLexerRBRACE)
	if !a || !b {
		return 0, 0, false
	}
	_, start := open.Span()
	end, _ := close.Span()
	return start, end, true
}
func (e *StorageExecutor) pipelineApplyReturnTextSourceForTest(ctx context.Context, rows []pipelineRow, text string, source pipelineRowSource, validated bool, groups ...[]*pipelineAggregateGroup) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnPlan(ctx, rows, returnPlanForTest(ctx, text), source, validated, groups...)
}

// pipelineApplyReturnTextForTest projects rows through a RETURN fixture clause.
func (e *StorageExecutor) pipelineApplyReturnTextForTest(ctx context.Context, rows []pipelineRow, text string) (*ExecuteResult, bool) {
	return e.pipelineApplyReturnPlan(ctx, rows, returnPlanForTest(ctx, text), pipelineRowsSource(rows), false)
}

// pipelineApplyWithTextForTest projects rows through a WITH fixture clause.
func (e *StorageExecutor) pipelineApplyWithTextForTest(ctx context.Context, rows []pipelineRow, text string, source pipelineRowSource, validated bool) ([]pipelineRow, bool) {
	clause, ok := fixtureClauseForTest(ctx, text)
	if !ok || clause.kind != pipelineClauseWith {
		return nil, false
	}
	return e.pipelineApplyWithPlan(ctx, rows, clause, projectionPlanOf(ctx, clause.syntax), source, validated)
}

// projectMergeReturnForTest projects rows through a RETURN fixture statement.
func (e *StorageExecutor) projectMergeReturnForTest(ctx context.Context, rows []pipelineRow, statement string) (*ExecuteResult, error) {
	return e.projectMergeReturn(ctx, rows, statement, queryGapEnd(statement, 0))
}

// Independent source fixtures for the typed branch consumers.
func unscopedTextBranchLocals(ctx context.Context, source string, names []string) []string {
	return unscopedBranchLocals(ctx, readQueryBodyText(ctx, source, 0, len(source)), names)
}
func textBranchDeclaresVariable(ctx context.Context, source, name string) bool {
	return branchDeclaresVariable(ctx, readQueryBodyText(ctx, source, 0, len(source)), name)
}

// resolveReturnItem projects one item over a single node binding. The test
// writes its RETURN statement as a client would; production projections read
// their statement's own RETURN occurrence.
func (e *StorageExecutor) resolveReturnItem(ctx context.Context, item returnItem, variable string, node *storage.Node) interface{} {
	row := e.mergeBindingRow(ctx, map[string]*storage.Node{variable: node}, nil)
	return firstProjectedValueForTest(e.projectMergeReturnForTest(ctx, []pipelineRow{row}, "RETURN "+item.expr))
}

// resolveReturnExprFromVarMap projects one expression over a joined binding
// row, as the former OPTIONAL MATCH helper did, through a client-written RETURN.
func (e *StorageExecutor) resolveReturnExprFromVarMap(ctx context.Context, expr string, varMap map[string]interface{}, targetVar, relVar string, targetNode *storage.Node, targetEdge *storage.Edge) interface{} {
	row := e.mergeBindingRow(ctx, map[string]*storage.Node{targetVar: targetNode}, map[string]*storage.Edge{relVar: targetEdge})
	for name, value := range varMap {
		if name != targetVar && name != relVar {
			row[name] = value
		}
	}
	return firstProjectedValueForTest(e.projectMergeReturnForTest(ctx, []pipelineRow{row}, "RETURN "+strings.TrimSpace(expr)))
}

func firstProjectedValueForTest(projected *ExecuteResult, err error) interface{} {
	if err != nil || len(projected.Rows) == 0 || len(projected.Rows[0]) == 0 {
		return nil
	}
	return projected.Rows[0][0]
}

// clauseEffectsForTest walks a fixture clause's effects with the production
// effect visitor; production reads statement effects (pipelineStatementEffectsFor).
func clauseEffectsForTest(ctx context.Context, text string) (pipelineClauseEffects, error) {
	syntax, err := cypherantlr.ParseSyntax(ctx, text, cypherantlr.ClauseEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err != nil {
		return pipelineClauseEffects{}, err
	}
	var effects pipelineClauseEffects
	syntax.Walk(pipelineEffectVisitor(&effects))
	return effects, ctx.Err()
}

// countReturnPlanForTest is the RETURN plan of a fixture clause, nil for an
// absent RETURN ("").
func countReturnPlanForTest(ctx context.Context, text string) *returnProjectionPlan {
	if strings.TrimSpace(text) == "" {
		return nil
	}
	return returnPlanForTest(ctx, text)
}

// projectionItemForTest is the retained projection item of a one-item
// RETURN fixture, or the absent item (no AS offset) when the fixture is not
// exactly one admitted item.
func projectionItemForTest(ctx context.Context, item string) projectionItemSource {
	absent := readProjectionItemSource(cypherantlr.SyntaxNode{})
	clause, ok := fixtureClauseForTest(ctx, "RETURN "+item)
	if !ok || clause.kind != pipelineClauseReturn {
		return absent
	}
	items := projectionItemsFromSyntax(clause.syntax)
	if len(items) != 1 {
		return absent
	}
	return items[0]
}

// projectTransactionReturnForTest projects through the RETURN a transaction
// script writes; an empty projection is RETURN *.
func (e *StorageExecutor) projectTransactionReturnForTest(ctx context.Context, input *ExecuteResult, projection string) (*ExecuteResult, error) {
	if strings.TrimSpace(projection) == "" {
		projection = "*"
	}
	return e.projectTransactionReturn(ctx, input, "RETURN "+projection)
}
