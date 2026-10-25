package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

var _ resultmetadata.FactsProvider = (*StorageExecutor)(nil)

// StatementFacts exposes the one cached execution projection program. Scope,
// imports, classification and ordered columns are established together. The
// owner's answer is complete or a terminal error; it never declines execution.
func (e *StorageExecutor) StatementFacts(query string, incoming resultmetadata.Scope, cypher25 bool, stage SourceStage) resultmetadata.Facts {
	return e.StatementFactsWith(query, incoming, cypher25, nil, stage)
}

// StatementFactsWith completes participant-owned signatures during compilation.
// Signature answers are not cached under query text: a participant may change
// its procedure registry independently of this executor's registry generation.
func (e *StorageExecutor) StatementFactsWith(query string, incoming resultmetadata.Scope, cypher25 bool, schema ParticipantSchema, stage SourceStage) resultmetadata.Facts {
	source := cyphertext.NewSource(context.Background(), query, stage)
	if source.Err != nil {
		return resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: resultmetadata.ReasonParserDecline, Err: source.Err})
	}
	query = source.Text
	if schema != nil && source.Text != source.Original {
		participant := schema
		schema = func(obligation resultmetadata.SchemaObligation) (ParticipantProcedure, error) {
			if obligation.Start >= 0 && obligation.End >= 0 {
				span, ok := source.OriginalSpan(cyphertext.Span{Start: obligation.Start, End: obligation.End}, cyphertext.SourceExtent)
				if !ok {
					return ParticipantProcedure{}, unresolvedStatementError()
				}
				obligation.Start, obligation.End = span.Start, span.End
			}
			if obligation.UseStart >= 0 && obligation.UseEnd >= 0 {
				span, ok := source.OriginalSpan(cyphertext.Span{Start: obligation.UseStart, End: obligation.UseEnd}, cyphertext.SourceExtent)
				if !ok {
					return ParticipantProcedure{}, unresolvedStatementError()
				}
				obligation.UseStart, obligation.UseEnd = span.Start, span.End
			}
			return participant(obligation)
		}
	}
	return e.statementProjectionProgram(context.Background(), query, incoming, cypher25, schema).facts.SourceView(source)
}

func (e *StorageExecutor) statementProjectionProgram(ctx context.Context, query string, incoming resultmetadata.Scope, cypher25 bool, schema ParticipantSchema) *projectedScopeFacts {
	query = statementAdmissionSource(ctx, query)
	ensureBuiltInProceduresRegistered()
	names := resultmetadata.Unknown()
	incoming.Visit(func(values []string) { names = resultmetadata.Known(values) }, func() { names = resultmetadata.Unknown() })
	var program *projectedScopeFacts
	if schema == nil {
		program = e.projectedStatementScope(ctx, query, names, resultmetadata.Known(nil), cypher25, semanticBodyFromContext(ctx))
	} else {
		compiler := projectionCompilation{executor: e, source: query, cypher25: cypher25, schema: schema, bindings: semanticBodyFromContext(ctx), scalarKind: scalarCompilationKind(ctx, query)}
		program = compiler.root(ctx, names, resultmetadata.Known(nil))
	}
	// Cached programs are immutable; participant-dependent compilation never
	// enters that cache. Both paths expose the same terminal Facts contract.
	if program.complete && ctx.Err() == nil {
		return program
	}
	immutable := *program
	if err := ctx.Err(); err != nil {
		immutable.complete, immutable.err = false, err
	}
	if immutable.err == nil && immutable.statement != nil {
		if facts, err := resultmetadata.KnownFacts(*immutable.statement); err == nil {
			immutable.complete, immutable.facts = true, facts
		}
	}
	if !immutable.complete {
		immutable.facts = resultmetadata.UnresolvedFacts(resultmetadata.Unresolved{Reason: immutable.reason, Position: immutable.position, Err: immutable.err})
	}
	return &immutable
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
func (e *StorageExecutor) compileStatementProgram(ctx context.Context, source string, incoming, retained resultmetadata.Names, cypher25 bool, bodies ...*semanticBodyScope) *projectedScopeFacts {
	compiler := projectionCompilation{executor: e, source: source, cypher25: cypher25, scalarKind: scalarCompilationKind(ctx, source)}
	if len(bodies) != 0 {
		compiler.bindings = bodies[0]
	}
	return compiler.root(ctx, incoming, retained)
}

type projectionCompilation struct {
	names      *quotedVariableNames
	bindings   *semanticBodyScope
	scalarKind string

	executor *StorageExecutor
	source   string
	cypher25 bool
	schema   ParticipantSchema
	syntax   cypherantlr.StatementSyntax
}

type projectionGraph struct {
	start, end int
	selected   bool
	outer      resultmetadata.Names
	outerIDs   map[string]statementBindingID
	routing    resultmetadata.Scope
	selection  *resultmetadata.GraphSelection
}

func (compiler *projectionCompilation) root(ctx context.Context, incoming, retained resultmetadata.Names) *projectedScopeFacts {
	return compiler.compileBound(ctx, incoming, retained)
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
	passThrough := len(plan.projections) > 0
	for _, item := range plan.projections {
		expressions = append(expressions, item.expr)
		passThrough = passThrough && item.direct && item.variable == item.alias
	}
	return resultmetadata.Projection{Expressions: expressions, Names: resultmetadata.Known(plan.columns), Distinct: plan.distinct, Modifiers: plan.modifiers, PassThrough: passThrough}
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

// presentationProjection selects the first UNION branch's admitted declaration
// view. Public spelling stays with the existing naming/presentation owner.
func (program *boundStatementProgram) presentationProjection() resultmetadata.Projection {
	if program == nil || program.root == nil || program.root.program == nil || program.root.program.statement == nil {
		return resultmetadata.Projection{}
	}
	statement := program.root.program.statement
	for len(statement.Branches) != 0 {
		statement = &statement.Branches[0]
	}
	if len(statement.Clauses) == 0 {
		return resultmetadata.Projection{}
	}
	last := statement.Clauses[len(statement.Clauses)-1]
	if last.Kind != resultmetadata.ClauseReturn || last.Projection == nil {
		return resultmetadata.Projection{}
	}
	return *last.Projection
}
