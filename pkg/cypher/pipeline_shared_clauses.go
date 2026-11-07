package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
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
		if clause.kind == pipelineClauseLet || clause.kind == pipelineClauseFilter || forIterationClause(clause) {
			return true
		}
		if clause.kind == pipelineClauseCallSubquery {
			start, end := strings.Index(clause.text, "{"), strings.LastIndex(clause.text, "}")
			if start >= 0 && end > start && usesSharedGrammarClauses(ctx, clause.text[start+1:end]) {
				return true
			}
		}
	}
	return false
}

// iterationClause is one UNWIND or shared-grammar FOR clause's binding, read
// from the retained grammar node the clause was split from: the list
// expression's own source bytes and the decoded name the clause binds.
// trailing names the keyword an UNWIND form leaves after its variable, which
// Neo4j rejects (UNWIND takes no WHERE).
type iterationClause struct {
	expression string
	variable   string
	trailing   string
}

// iterationClauseFor reads an iteration clause's binding from its retained
// grammar node. It is the one reader of a parsed iteration clause; the native
// scanner (splitUnwindBody) reads raw statement text only, for the top-level
// dispatch that runs before a statement is split into clauses.
func iterationClauseFor(clause pipelineClause) (iterationClause, bool) {
	if clause.kind != pipelineClauseUnwind || !clause.syntax.Valid() {
		return iterationClause{}, false
	}
	node := clause.syntax
	symbol := node.ChildRule(cypherantlr.CypherParserRULE_symbol)
	if !symbol.Valid() {
		return iterationClause{}, false
	}
	variable, symbolic := symbol.Name()
	if !symbolic {
		return iterationClause{}, false
	}
	source := node.Source()
	iteration := iterationClause{variable: variable}
	switch node.Rule() {
	case cypherantlr.CypherParserRULE_unwindSt:
		if _, found := node.DirectToken(cypherantlr.CypherLexerUNWIND); !found {
			return iterationClause{}, false
		}
		as, found := node.DirectToken(cypherantlr.CypherLexerAS)
		if !found {
			return iterationClause{}, false
		}
		list := node.ChildRule(cypherantlr.CypherParserRULE_expression)
		if !list.Valid() {
			return iterationClause{}, false
		}
		start, _ := list.Span()
		boundary, _ := as.Span()
		if boundary < start || boundary > len(source) {
			return iterationClause{}, false
		}
		iteration.expression = strings.TrimSpace(source[start:boundary])
		if node.ChildRule(cypherantlr.CypherParserRULE_where).Valid() {
			iteration.trailing = "WHERE"
		}
	case cypherantlr.CypherParserRULE_forSt:
		keyword, found := node.DirectToken(cypherantlr.CypherLexerIN)
		if !found {
			return iterationClause{}, false
		}
		_, start := keyword.Span()
		_, end := node.Span()
		if end < start || end > len(source) {
			return iterationClause{}, false
		}
		iteration.expression = strings.TrimSpace(source[start:end])
	default:
		return iterationClause{}, false
	}
	if iteration.expression == "" {
		return iterationClause{}, false
	}
	return iteration, true
}

// forIterationClause reports the shared-grammar FOR form of an iteration
// clause from its retained node.
func forIterationClause(clause pipelineClause) bool {
	return clause.kind == pipelineClauseUnwind && clause.syntax.Valid() &&
		clause.syntax.Rule() == cypherantlr.CypherParserRULE_forSt
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

func (e *StorageExecutor) validateSharedClause(ctx context.Context, scope matchSemanticScope, values map[string]string, clause pipelineClause, cypher25 bool) error {
	input := staticTypeScope{kinds: scope, values: values, complete: true, cypher25: cypher25}
	if clause.kind == pipelineClauseFilter {
		expression := pipelineFilterExpression(clause.text)
		if expression == "" {
			return sharedClauseSyntaxError("FILTER requires a predicate")
		}
		if pipelineExpressionContainsAggregate(expression) {
			return sharedClauseSyntaxError("Aggregation is not allowed in FILTER")
		}
		if err := undefinedExpressionVariable(ctx, scope, expression); err != nil {
			return err
		}
		return e.validateStaticClauseTypes(ctx, clause, input)
	}
	projections, err := parsePipelineLet(clause.text)
	if err != nil {
		return err
	}
	// LET binds its items in order: an item's expression reads the items
	// before it (LET a = 1, b = a + 1), never its own alias or a later one.
	// Every check of an item therefore runs against the bindings reached
	// before it, and the item's own static type then joins the scope the next
	// item and the clause's operator check read.
	types := typeScopeOf(input)
	for _, projection := range projections {
		if err := e.validateSubqueryPatternBindings(ctx, input, projection.expression); err != nil {
			return err
		}
		if err := projectionItemTermError(projection.expression); err != nil {
			return err
		}
		if err := undefinedExpressionVariable(ctx, input.kinds, projection.expression); err != nil {
			return err
		}
		if err := validateStaticFunctionVariables(ctx, projection.expression, input); err != nil {
			return err
		}
		types[projection.alias] = projectedOperand(ctx, types, input, projection.expression)
		next := types.static(input.cypher25)
		next.params, next.complete = input.params, input.complete
		input = next
	}
	return e.validateStaticOperatorTypes(ctx, clause, input, nil, nil)
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
	// LET binds its items in order, so an item's expression reads the values
	// the items before it produced. One WITH row projection per item, keeping
	// the row's variables (star), is that order: each step evaluates one item
	// against the row the steps before it built.
	source := input
	for _, projection := range projections {
		plan := pipelineRowWith{star: true, projections: []pipelineRowProjection{projection}}
		source = e.pipelineWithRowSource(ctx, source, plan)
	}
	return source, nil
}
