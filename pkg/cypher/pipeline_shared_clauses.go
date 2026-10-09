package cypher

import (
	"context"
	"strings"
)

func cypherGrammarVersion(query string) string {
	version := "5"
	rest := strings.TrimSpace(query)
	for rest != "" {
		if startsWithKeywordFold(rest, "EXPLAIN") || startsWithKeywordFold(rest, "PROFILE") {
			rest = strings.TrimSpace(rest[len("EXPLAIN"):])
			continue
		}
		if !startsWithKeywordFold(rest, "CYPHER") {
			break
		}
		rest = strings.TrimSpace(rest[len("CYPHER"):])
		if len(rest) > 0 && isDigitByte(rest[0]) {
			end := 0
			for end < len(rest) && isDigitByte(rest[end]) {
				end++
			}
			version = rest[:end]
			rest = strings.TrimSpace(rest[end:])
		}
		for rest != "" && !startsWithClauseKeyword(rest) {
			end := strings.IndexAny(rest, " \t\r\n")
			if end < 0 {
				rest = ""
				break
			}
			rest = strings.TrimSpace(rest[end:])
		}
	}
	return version
}

func usesSharedGrammarClauses(query string) bool {
	if isSchemaCommandStatement(query) {
		return false
	}
	clauses, ok := pipelineClausesFor(query)
	if !ok {
		return false
	}
	for _, clause := range clauses {
		if clause.kind == pipelineClauseLet || clause.kind == pipelineClauseFilter ||
			clause.kind == pipelineClauseUnwind && startsWithKeywordFold(clause.text, "FOR") {
			return true
		}
		if clause.kind == pipelineClauseCallSubquery {
			start, end := strings.Index(clause.text, "{"), strings.LastIndex(clause.text, "}")
			if start >= 0 && end > start && usesSharedGrammarClauses(clause.text[start+1:end]) {
				return true
			}
		}
	}
	return false
}

func parsePipelineIteration(clause string) (expression, alias string, ok bool) {
	if !startsWithKeywordFold(strings.TrimSpace(clause), "FOR") {
		return splitUnwindBody(pipelineClauseBody(clause, "UNWIND"))
	}
	body := pipelineClauseBody(clause, "FOR")
	name, end, found := scanSymbolicName(body, 0)
	if !found {
		return "", "", false
	}
	rest := strings.TrimSpace(body[end:])
	if !startsWithKeywordFold(rest, "IN") {
		return "", "", false
	}
	expression = strings.TrimSpace(rest[len("IN"):])
	return expression, name, expression != ""
}

func parsePipelineLet(clause string) ([]pipelineRowProjection, error) {
	body := pipelineClauseBody(clause, "LET")
	var projections []pipelineRowProjection
	for _, item := range splitTopLevelComma(body) {
		item = strings.TrimSpace(item)
		name, end, found := scanSymbolicName(item, 0)
		if !found {
			return nil, sharedClauseSyntaxError("LET requires a variable = expression binding")
		}
		rest := strings.TrimSpace(item[end:])
		if len(rest) < 2 || rest[0] != '=' {
			return nil, sharedClauseSyntaxError("LET requires a variable = expression binding")
		}
		expression := strings.TrimSpace(rest[1:])
		if expression == "" || pipelineExpressionContainsAggregate(expression) {
			return nil, sharedClauseSyntaxError("LET requires a non-aggregate expression")
		}
		projections = append(projections, pipelineRowProjection{expression: expression, alias: normalizeProjectionColumnName(name)})
	}
	if len(projections) == 0 {
		return nil, sharedClauseSyntaxError("LET requires at least one binding")
	}
	return projections, nil
}

func pipelineFilterExpression(clause string) string {
	body := pipelineClauseBody(clause, "FILTER")
	if startsWithKeywordFold(body, "WHERE") {
		body = strings.TrimSpace(body[len("WHERE"):])
	}
	return body
}

func sharedClauseSyntaxError(message string) error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", message)
}

func bindPipelineLet(scope *semanticBindingScope, clause string) error {
	projections, err := parsePipelineLet(clause)
	if err != nil {
		return err
	}
	for _, projection := range projections {
		scope.bind(projection.alias)
	}
	return nil
}

func (e *StorageExecutor) validateSharedClause(scope matchSemanticScope, values map[string]string, clause pipelineClause) error {
	input := staticTypeScope{kinds: scope, values: values, complete: true}
	if clause.kind == pipelineClauseFilter {
		expression := pipelineFilterExpression(clause.text)
		if expression == "" {
			return sharedClauseSyntaxError("FILTER requires a predicate")
		}
		if err := undefinedExpressionVariable(scope, expression); err != nil {
			return err
		}
		return e.validateStaticClauseTypes(clause, input)
	}
	projections, err := parsePipelineLet(clause.text)
	if err != nil {
		return err
	}
	for _, projection := range projections {
		if err := projectionItemTermError(projection.expression); err != nil {
			return err
		}
		if err := undefinedExpressionVariable(scope, projection.expression); err != nil {
			return err
		}
		if err := validateStaticFunctionVariables(projection.expression, input); err != nil {
			return err
		}
	}
	return e.validateStaticOperatorTypes(clause, input, nil, nil)
}

func (e *StorageExecutor) pipelineSharedClauseSource(ctx context.Context, input pipelineRowSource, clause pipelineClause) (pipelineRowSource, error) {
	if clause.kind == pipelineClauseFilter {
		expression := pipelineFilterExpression(clause.text)
		if expression == "" {
			return nil, sharedClauseSyntaxError("FILTER requires a predicate")
		}
		return func(yield func(pipelineRow) bool) bool {
			valid := true
			completed := input(func(row pipelineRow) bool {
				if err := ctx.Err(); err != nil {
					recordExpressionFailure(ctx, err)
					valid = false
					return false
				}
				accepted := e.evaluateWithWhereCondition(ctx, expression, row)
				if getExpressionFailure(ctx) != nil {
					valid = false
					return false
				}
				return !accepted || yield(row)
			})
			return completed && valid
		}, nil
	}
	projections, err := parsePipelineLet(clause.text)
	if err != nil {
		return nil, err
	}
	plan := pipelineRowWith{star: true, projections: projections}
	return e.pipelineWithRowSource(ctx, input, plan), nil
}
