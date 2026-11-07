package cypher

import (
	"context"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

// boundCommand is one catalog command clause this compilation composed from the
// statement's retained syntax: the result schema the command declares and the
// columns its tail's own expressions read. A command clause reads no graph and
// resolves nothing at run time, so it is not one of the graph clause kinds.
type boundCommand struct {
	outputs     resultmetadata.Names
	expressions []string
}

// newBoundCompilation owns the scope transfers of one statement's parse product.
func newBoundCompilation(compiler *projectionCompilation, syntax cypherantlr.StatementSyntax) *boundCompilation {
	return &boundCompilation{
		owner:       compiler,
		events:      map[int]*boundEvent{},
		product:     &boundStatementProgram{syntax: syntax, projections: map[int]*returnProjectionPlan{}, effects: map[int]pipelineClauseEffects{}, scalars: map[int]scalarExpressionSyntax{}, columns: map[int]resultmetadata.Columns{}},
		locals:      map[localIdentityKey]statementBindingID{},
		clauses:     map[int]queryBodyClause{},
		localSyntax: map[int][]syntaxLocalBinding{},
		commands:    map[int]*boundCommand{},
	}
}

// compileShowCommand answers a SHOW statement from the result schema its own
// listing declares, and reports whether this owner answered it. Without a tail
// the statement is that declaration; with a retained tail the declaration is the
// body's first clause and the tail's own query (YIELD's items, or the query that
// continues over them) composes with it, so a valid YIELD projection, alias or
// RETURN reports the columns it produces through the same projection owner every
// other query uses. No listing runs: the schema is declared, not observed, and
// a rejected column is the error execution reports for it.
func (compiler *projectionCompilation) compileShowCommand(ctx context.Context, syntax cypherantlr.StatementSyntax, show cypherantlr.SyntaxNode, start, end int, incoming, retained resultmetadata.Names) (*projectedScopeFacts, bool) {
	declared, declaredHere, err := showCommandResult(ctx, show, compiler.cypher25)
	if !declaredHere {
		return nil, false
	}
	if err != nil {
		return programFailure(resultmetadata.ReasonUnsupportedSyntax, start, err), true
	}
	tail := show.ChildRule(cypherantlr.CypherParserRULE_showTail)
	if !tail.Valid() {
		return fixedCommandScopeFacts(start, end, incoming, retained, declared.defaultColumns(), resultmetadata.ClauseOther, commandResultScope), true
	}
	// The tail's query continues the statement over the command's columns: the
	// YIELD's own RETURN, or the clauses a Cypher 25 statement composes after it.
	// A tail that yokes several queries with UNION composes none of them with the
	// command's declaration, and keeps the contract this owner had for it.
	continued := queryBody{}
	if tailQuery := tail.ChildRule(cypherantlr.CypherParserRULE_regularQuery); tailQuery.Valid() {
		continued = queryBodyFromSyntax(tailQuery)
		if continued.status != queryBodyRead || len(continued.branches) > 1 {
			return nil, false
		}
	}
	hasContinuation := len(continued.branches) == 1 && len(continued.branches[0].clauses) > 0
	outputs, expressions, position, err := declared.tailProjection(tail)
	if err != nil {
		return programFailure(resultmetadata.ReasonUndefinedVariable, position, err), true
	}
	command := &boundCommand{outputs: resultmetadata.Known(outputs), expressions: expressions}
	commandEnd := end
	if hasContinuation {
		commandEnd = continued.branches[0].clauses[0].start
	}
	branch := queryBodyBranch{start: start, end: end, useStart: -1, useEnd: -1, finishStart: -1, finishEnd: -1}
	branch.clauses = append(branch.clauses, queryBodyClause{
		pipelineClause: pipelineClause{kind: pipelineClauseCommand, syntax: show, text: show.Code()},
		start:          start,
		end:            commandEnd,
		syntax:         show,
	})
	if hasContinuation {
		branch.clauses = append(branch.clauses, continued.branches[0].clauses...)
	}
	body := queryBody{status: queryBodyRead, branches: []queryBodyBranch{branch}}

	compilation := newBoundCompilation(compiler, syntax)
	compilation.commands[show.ID()] = command
	compiled := compilation.prepareBody(ctx, body, show, start, end)
	return compilation.completeBody(ctx, compiled, syntax, incoming, retained), true
}

// enterCommand completes a declared command clause. The command declares the
// columns it lists, so it reads and resolves nothing: its own clause is finished
// here, and the branch carries the columns the clauses after it read. The tail's
// expressions still bind against the declaration's columns and the YIELD aliases
// (clause.after), which is what execution lets them read.
func (compilation *boundCompilation) enterCommand(clause *boundClause, command *boundCommand) {
	outputs := projectedNameMap(command.outputs)
	retained := copyBindingNames(clause.before)
	for name := range outputs {
		retained[name] = struct{}{}
	}
	clause.step.Kind = resultmetadata.ClauseOther
	clause.step.After = projectionScope(projectedMapNames(retained))
	clause.outputs = command.outputs
	clause.step.Projection = &resultmetadata.Projection{Names: command.outputs}
	clause.branch.statement.Clauses = append(clause.branch.statement.Clauses, clause.step)
	clause.branch.scope = retained
	clause.branch.outputs = command.outputs
	clause.branch.columns = projectionColumns(command.outputs)
	for _, name := range command.expressions {
		clause.after[name] = struct{}{}
	}
}
