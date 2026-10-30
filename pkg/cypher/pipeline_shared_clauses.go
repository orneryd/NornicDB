package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
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
			end := strings.IndexFunc(rest, cyphertext.IsWhitespace)
			if end < 0 {
				rest = ""
				break
			}
			rest = strings.TrimSpace(rest[end:])
		}
	}
	return version
}

func usesSharedGrammarClauses(ctx context.Context, query string) bool {
	if isSchemaCommandStatement(query) {
		return false
	}
	clauses, ok := pipelineClausesFor(ctx, query)
	if !ok {
		return false
	}
	for _, clause := range clauses {
		if clause.kind == pipelineClauseLet || clause.kind == pipelineClauseFilter ||
			clause.kind == pipelineClauseUnwind && startsWithKeywordFold(clause.text, "FOR") {
			return true
		}
		if clause.kind == pipelineClauseCallSubquery {
			start, end := firstLiveBrace(clause.text), lastLiveBraceClose(clause.text)
			if start >= 0 && end > start && usesSharedGrammarClauses(ctx, clause.text[start+1:end]) {
				return true
			}
		}
	}
	return false
}

func parsePipelineIteration(clause braceView) (expression, alias string, ok bool) {
	view, alias, ok := parsePipelineIterationSource(clause)
	return view.text(), alias, ok
}

// parsePipelineIterationSource carries the extent selected by iteration grammar.
func parsePipelineIterationSource(clause braceView) (expression braceView, alias string, ok bool) {
	clause = clause.trim()
	if cyphertext.KeywordEnd(clause.text(), 0, "FOR", cyphertext.CypherWord) == 3 {
		body := clause.clauseBody("FOR")
		name, end, found := scanSymbolicName(body.text(), 0)
		if !found {
			return braceView{}, "", false
		}
		rest := body.window(end, len(body.text())).trim()
		if !(cyphertext.KeywordEnd(rest.text(), 0, "IN", cyphertext.CypherWord) == 2) {
			return braceView{}, "", false
		}
		expression = rest.clauseBody("IN")
		return expression, name, !expression.empty()
	}
	product, found := splitUnwindBody(clause)
	if !found {
		return braceView{}, "", false
	}
	return clause.spanWindow(product.expressionSpan), product.aliasTail, true
}

func projectionSemanticSourceAndTail(clause braceView, keyword string) (body, tail braceView) {
	clause = clause.trim()
	cut := len(clause.text())
	for _, next := range []string{"WHERE", "ORDER BY", "SKIP", "LIMIT"} {
		if at := topLevelKeywordIndex(clause, next); at >= len(keyword) && at < cut {
			cut = at
		}
	}
	body = clause.window(0, cut).clauseBody(keyword)
	body, _ = cutDistinct(body)
	return body, clause.window(cut, len(clause.text()))
}

func parsePipelineLet(clause braceView) ([]pipelineRowProjection, error) {
	body := clause.clauseBody("LET")
	var projections []pipelineRowProjection
	for _, itemView := range body.commaViews() {
		item := itemView.text()
		name, end, found := scanSymbolicName(item, 0)
		if !found {
			return nil, sharedClauseSyntaxError("LET requires a variable = expression binding")
		}
		restView := itemView.window(end, len(item)).trim()
		rest := restView.text()
		if len(rest) < 2 || rest[0] != '=' {
			return nil, sharedClauseSyntaxError("LET requires a variable = expression binding")
		}
		expressionView := restView.window(1, len(rest)).trim()
		expression := expressionView.text()
		if expression == "" || pipelineExpressionContainsAggregate(expressionView) {
			return nil, sharedClauseSyntaxError("LET requires a non-aggregate expression")
		}
		projections = append(projections, pipelineRowProjection{expression: expression, alias: normalizeProjectionColumnName(name), view: itemView.window(len(item)-len(expression), len(item))})
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

func bindPipelineLet(scope *semanticBindingScope, clause braceView) error {
	projections, err := parsePipelineLet(clause)
	if err != nil {
		return err
	}
	for _, projection := range projections {
		scope.bind(projection.alias)
	}
	return nil
}

func (e *StorageExecutor) validateSharedClause(ctx context.Context, scope matchSemanticScope, values map[string]string, clause pipelineClause, cypher25 bool) error {
	input := staticTypeScope{kinds: scope, values: values, complete: true, cypher25: cypher25}
	if clause.kind == pipelineClauseFilter {
		expression := clause.view.filterExpression()
		if expression.empty() {
			return sharedClauseSyntaxError("FILTER requires a predicate")
		}
		if pipelineExpressionContainsAggregate(expression) {
			return sharedClauseSyntaxError("Aggregation is not allowed in FILTER")
		}
		if err := undefinedExpressionVariable(scope, expression); err != nil {
			return err
		}
		return e.validateStaticClauseTypes(ctx, clause, input)
	}
	projections, err := parsePipelineLet(clause.input(ctx))
	if err != nil {
		return err
	}
	for _, projection := range projections {
		if err := projectionItemTermError(projection.expression); err != nil {
			return err
		}
		if err := undefinedExpressionVariable(scope, projection.view); err != nil {
			return err
		}
		if err := validateStaticFunctionVariables(projection.view, input); err != nil {
			return err
		}
	}
	return e.validateStaticOperatorTypes(ctx, clause, input, nil, nil)
}

func (e *StorageExecutor) pipelineSharedClauseSource(ctx context.Context, input pipelineRowSource, clause pipelineClause) (pipelineRowSource, error) {
	if clause.kind == pipelineClauseFilter {
		expression := clause.view.filterExpression()
		if expression.empty() {
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
	projections, err := parsePipelineLet(clause.input(ctx))
	if err != nil {
		return nil, err
	}
	plan := pipelineRowWith{star: true, projections: projections}
	return e.pipelineWithRowSource(ctx, input, plan), nil
}
