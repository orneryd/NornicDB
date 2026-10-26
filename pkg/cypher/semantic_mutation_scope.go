package cypher

import (
	"context"
	"maps"
)

// Mutation rules consume the admitted clause's incoming bindings. They do not
// maintain another transfer walk that can omit CALL or procedure exports.
func (e *StorageExecutor) boundMutationClauses(ctx context.Context, query string, cypher25 bool) ([]pipelineClause, error) {
	clauses, ok := splitPipelineClausesAllowingProcedureCalls(ctx, query)
	if !ok {
		return nil, nil
	}
	program := e.statementProjectionProgram(ctx, query, e.resultIncomingScope(), cypher25, e.participantSchemaForStatement(ctx, query))
	if !program.complete {
		if program.err != nil {
			return nil, program.err
		}
		return nil, unresolvedStatementError()
	}
	if program.bound == nil {
		return nil, nil
	}
	for index := range clauses {
		if step, exists := program.bound.scopes[clauses[index].syntax.ID()]; exists {
			step.Before.Visit(func(names []string) {
				clauses[index].validationScope = make(map[string]struct{}, len(names))
				for _, name := range names {
					clauses[index].validationScope[name] = struct{}{}
				}
			}, func() {})
		}
	}
	return clauses, nil
}

func clauseBindingScope(clause pipelineClause, fallback *semanticBindingScope) *semanticBindingScope {
	if clause.validationScope != nil {
		return &semanticBindingScope{names: maps.Clone(clause.validationScope)}
	}
	if fallback != nil {
		return fallback
	}
	return newSemanticBindingScope()
}
