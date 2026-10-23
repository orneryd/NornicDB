package cypher

import (
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

var _ resultmetadata.FactsProvider = (*StorageExecutor)(nil)

// StatementFacts exposes the one cached execution projection program. Scope,
// imports, classification and ordered columns are established together. The
// owner's answer is complete or a terminal error; it never declines execution.
func (e *StorageExecutor) StatementFacts(query string, incoming resultmetadata.Scope, cypher25 bool) resultmetadata.Facts {
	return e.StatementFactsWith(query, incoming, cypher25, nil)
}

// StatementFactsWith completes participant-owned signatures during compilation.
// Signature answers are not cached under query text: a participant may change
// its procedure registry independently of this executor's registry generation.
func (e *StorageExecutor) StatementFactsWith(query string, incoming resultmetadata.Scope, cypher25 bool, schema resultmetadata.ParticipantSchema) resultmetadata.Facts {
	if schema == nil {
		program := e.statementProjectionProgram(query, incoming, cypher25)
		if program.complete {
			return program.facts
		}
		return resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: program.reason, Position: program.position, Err: program.err})
	}
	ensureBuiltInProceduresRegistered()
	names := resultmetadata.Unknown()
	incoming.Visit(func(known []string) { names = resultmetadata.Known(known) }, func() { names = resultmetadata.Unknown() })
	compiler := projectionCompilation{executor: e, source: query, cypher25: cypher25, schema: schema}
	program := compiler.root(names, resultmetadata.Known(nil))
	if program.err == nil && program.statement != nil {
		facts, err := resultmetadata.KnownFacts(*program.statement)
		if err == nil {
			return facts
		}
	}
	return resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: program.reason, Position: program.position, Err: program.err})
}

func (e *StorageExecutor) statementProjectionProgram(query string, incoming resultmetadata.Scope, cypher25 bool) *projectedScopeFacts {
	ensureBuiltInProceduresRegistered()
	names := resultmetadata.Unknown()
	incoming.Visit(func(values []string) { names = resultmetadata.Known(values) }, func() { names = resultmetadata.Unknown() })
	return e.projectedStatementScope(query, names, resultmetadata.Known(nil), cypher25)
}

func projectionScope(names resultmetadata.Names) resultmetadata.Scope {
	scope := resultmetadata.UnknownScope()
	names.Visit(func(values []string) { scope = resultmetadata.KnownScope(values) }, func() { scope = resultmetadata.UnknownScope() })
	return scope
}

func projectionColumns(names resultmetadata.Names) resultmetadata.Columns {
	columns := resultmetadata.UnknownColumns()
	names.Visit(func(values []string) { columns = resultmetadata.KnownColumns(values) }, func() { columns = resultmetadata.UnknownColumns() })
	return columns
}

func programFailure(reason resultmetadata.UnresolvedReason, position int, err error) *projectedScopeFacts {
	return &projectedScopeFacts{reason: reason, position: position, err: err}
}

// compileStatementProgram compiles the execution clause operators and their
// scope transfers. WITH and RETURN use the same cached projection plans that
// execute rows; CALL stores its nested program rather than re-inferring exports.
func (e *StorageExecutor) compileStatementProgram(source string, incoming, retained resultmetadata.Names, cypher25 bool) *projectedScopeFacts {
	compiler := projectionCompilation{executor: e, source: source, cypher25: cypher25}
	return compiler.root(incoming, retained)
}

type projectionCompilation struct {
	executor *StorageExecutor
	source   string
	cypher25 bool
	schema   resultmetadata.ParticipantSchema
	syntax   cypherantlr.StatementSyntax
}

type projectionGraph struct {
	start, end int
	selected   bool
	outer      resultmetadata.Names
	outerIDs   map[string]statementBindingID
}

func (compiler *projectionCompilation) root(incoming, retained resultmetadata.Names) *projectedScopeFacts {
	return compiler.compileBound(incoming, retained)
}
func statementHasReturningOperator(statement resultmetadata.Statement) bool {
	if len(statement.Branches) > 0 {
		for _, branch := range statement.Branches {
			if statementHasReturningOperator(branch) {
				return true
			}
		}
		return false
	}
	return len(statement.Clauses) > 0 && statement.Clauses[len(statement.Clauses)-1].Kind == resultmetadata.ClauseReturn
}

func projectionDescriptor(plan *returnProjectionPlan) resultmetadata.Projection {
	expressions := make([]string, 0, len(plan.projections))
	for _, item := range plan.projections {
		expressions = append(expressions, item.expr)
	}
	return resultmetadata.Projection{Expressions: expressions, Names: resultmetadata.Known(plan.columns), Distinct: plan.distinct, Modifiers: plan.modifiers}
}

func statementUsesParticipant(statement resultmetadata.Statement) bool {
	for _, clause := range statement.Clauses {
		if clause.ParticipantResolved || clause.Call != nil && clause.Call.Resolution == resultmetadata.ResolvedByParticipant {
			return true
		}
	}
	for _, branch := range statement.Branches {
		if statementUsesParticipant(branch) {
			return true
		}
	}
	return false
}
