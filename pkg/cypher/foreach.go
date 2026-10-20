package cypher

import (
	"github.com/orneryd/nornicdb/pkg/fabric"
	"strings"
)

func parsePipelineForeach(clause string, syntax ...*fabric.QueryClause) (string, string, []pipelineClause, error) {
	invalid := func() error {
		return newSemanticError("Neo.ClientError.Statement.SyntaxError", "InvalidForeach", "invalid or unsupported FOREACH update")
	}
	native, err := statementNativeClause(clause, "FOREACH", syntax...)
	if err != nil {
		return "", "", nil, err
	}
	if native == nil {
		return "", "", nil, invalid()
	}
	scope, ok := native.Input.RootScope()
	if !ok || scope.Kind != fabric.ExpressionIterationScope || len(scope.Declarations) != 1 {
		return "", "", nil, invalid()
	}
	text := native.Input.Text()
	declaration := scope.Declarations[0]
	listExpression := strings.TrimSpace(text[scope.Input.Start:scope.Input.End])
	updates, supported := splitPipelineClauses(strings.TrimSpace(text[scope.Visible+1 : scope.Close]))
	if !supported || len(updates) == 0 {
		return "", "", nil, invalid()
	}
	for _, update := range updates {
		switch update.kind {
		case pipelineClauseCreate, pipelineClauseSet, pipelineClauseMerge, pipelineClauseRemove, pipelineClauseDelete, pipelineClauseForeach:
		default:
			return "", "", nil, invalid()
		}
	}
	return declaration.Name, listExpression, updates, nil
}
