package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// ResultColumns is the strict ordered-header view of the execution program.
// Unknown facts are an error, never an empty successful header.
func (e *StorageExecutor) ResultColumns(ctx context.Context, query string, incoming resultmetadata.Scope, cypher25 bool, stage SourceStage) ([]string, error) {
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return nil, source.Err
	}
	query = source.Text
	var columns []string
	var failure error
	program := e.statementProjectionProgram(ctx, query, incoming, cypher25, nil)
	if !program.complete {
		if program.err != nil {
			return nil, program.err
		}
		return nil, unresolvedStatementError()
	}
	program.statement.Columns.Visit(func(known []string) { columns = known }, func() { failure = unresolvedStatementError() })
	return columns, failure
}

func unresolvedStatementError() error {
	return localizedStatusError("Neo.ClientError.Statement.FeatureNotSupported", "UnresolvedStatement", localization.CypherResultMetadataUnresolved())
}

// verifyResultColumns checks stream metadata against the established schema.
// Stream fields are observations; they never replace or reconstruct the owner.
func verifyResultColumns(actual, expected []string) error {
	return resultmetadata.KnownColumns(expected).Verify(actual)
}

func (e *StorageExecutor) resultIncomingScope() resultmetadata.Scope {
	names := make([]string, 0, len(e.fabricRecordBindings))
	for name := range e.fabricRecordBindings {
		names = append(names, name)
	}
	return resultmetadata.KnownScope(names)
}

// StatementColumns is the legacy names-only public view. A nil result denotes
// unresolved or invalid facts and must never be treated as a successful header.
// Execution and rewriting consumers use ResultColumns or StatementFacts.
func (e *StorageExecutor) StatementColumns(query string) []string {
	columns, err := e.StatementResultColumns(query, RawSource)
	if err != nil {
		return nil
	}
	return columns
}

// StatementResultColumns is the strict public header view with this executor's
// incoming record scope. Rewriters and transaction adapters preserve its error.
func (e *StorageExecutor) StatementResultColumns(query string, stage SourceStage) ([]string, error) {
	return e.ResultColumns(context.Background(), query, e.resultIncomingScope(), classifyStatement(query).version == "25", stage)
}

// StatementResultColumnsContext is the public execution view, using the same
// participant-signature resolver as Fabric planning before any effects.
func (e *StorageExecutor) StatementResultColumnsContext(ctx context.Context, query string, stage SourceStage) ([]string, error) {
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return nil, source.Err
	}
	query = source.Text
	var columns []string
	var failure error
	cypher25 := cypherVersionFromContext(ctx) == "25" || classifyStatement(query).version == "25"
	e.statementProjectionProgram(ctx, query, e.resultIncomingScope(), cypher25, e.participantSchemaForStatement(ctx, query)).facts.Visit(func(statement resultmetadata.Statement) {
		statement.Columns.Visit(func(known []string) { columns = known }, func() { failure = unresolvedStatementError() })
	}, func() { failure = unresolvedStatementError() }, func(reason resultmetadata.Unresolved) { failure = reason.Err })
	return columns, failure
}

// knownResultColumns is an optional cached hint for row fast paths. A missing
// hint leaves actual execution responsible; this is not a result-schema source.
func knownResultColumns(names resultmetadata.Names) []string {
	var columns []string
	names.Visit(func(known []string) { columns = known }, func() { columns = nil })
	return columns
}

// CachedResultColumns is a legacy cache-only hint for the Fabric consumers
// being replaced by planned Facts. It never compiles or guesses a cold header.
func (e *StorageExecutor) CachedResultColumns(ctx context.Context, query string, incoming []string) resultmetadata.Names {
	statement := classifyStatement(query)
	cypher25 := cypherVersionFromContext(ctx) == "25" || statement.version == "25"
	names := make(map[string]struct{}, len(incoming)+len(e.fabricRecordBindings))
	for _, name := range incoming {
		names[name] = struct{}{}
	}
	for name := range e.fabricRecordBindings {
		names[name] = struct{}{}
	}
	key := projectionKey(query, projectedMapNames(names), resultmetadata.Known(nil), cypher25)
	if facts, found := projectionFactsCache.get(key); found && facts.generation == globalProcedureRegistry.generation.Load() && facts.complete {
		names := resultmetadata.Unknown()
		facts.statement.Columns.Visit(func(header []string) { names = resultmetadata.Known(header) }, func() {})
		return names
	}
	return resultmetadata.Unknown()
}

// statementProducesColumns is framing's presence-only view of the canonical
// clause readers. Ordinary UNION execution does not compile a scope program.
func (e *StorageExecutor) statementProducesColumns(ctx context.Context, query string) bool {
	start, end, _, ok := queryStatementWindow(ctx, query)
	return ok && queryBodyProducesColumns(ctx, query, readQueryBody(ctx, query, start, end), false)
}

func queryBodyProducesColumns(ctx context.Context, source string, body queryBody, directReturnsOnly bool) bool {
	for _, branch := range body.branches {
		if branch.declined || branch.finished || len(branch.clauses) == 0 {
			continue
		}
		last := branch.clauses[len(branch.clauses)-1]
		switch last.kind {
		case pipelineClauseReturn:
			return true
		case pipelineClauseCall:
			if directReturnsOnly {
				continue
			}
			produced := false
			projectedProcedureNames(ctx, last.text).Visit(func(names []string) { produced = len(names) > 0 }, func() { produced = false })
			if produced {
				return true
			}
		case pipelineClauseCallSubquery:
			if directReturnsOnly {
				continue
			}
			start, end, ok := callClauseBody(ctx, last)
			if ok && queryBodyProducesColumns(ctx, source, readQueryBody(ctx, source, start, end), false) {
				return true
			}
		}
	}
	return false
}

// ResultProjection is the execution descriptor from the owner program. It
// never expands a compound wildcard from the statement's entry scope.
func (e *StorageExecutor) ResultProjection(ctx context.Context, query string, incoming []string, stage SourceStage) resultmetadata.Projection {
	source := cyphertext.NewSource(ctx, query, stage)
	if source.Err != nil {
		return resultmetadata.Projection{}
	}
	query = source.Text
	var projection resultmetadata.Projection
	e.statementProjectionProgram(ctx, query, resultmetadata.KnownScope(incoming), cypherVersionFromContext(ctx) == "25" || classifyStatement(query).version == "25", nil).facts.Visit(func(statement resultmetadata.Statement) {
		if len(statement.Branches) != 0 || len(statement.Clauses) == 0 {
			return
		}
		last := statement.Clauses[len(statement.Clauses)-1]
		if last.Kind == resultmetadata.ClauseReturn && last.Projection != nil {
			projection = *last.Projection
		}
	}, func() { projection = resultmetadata.Projection{} }, func(resultmetadata.Unresolved) { projection = resultmetadata.Projection{} })
	return projection
}
