package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

type boundScopeAdmission struct {
	graph   bool
	command bool
	columns resultmetadata.Columns
	program *boundStatementProgram
}

func (admission boundScopeAdmission) verifyColumns(actual []string) error {
	if admission.graph {
		return admission.columns.Verify(actual)
	}
	if admission.command {
		return nil
	} // The typed command continues in its command owner.
	return unresolvedStatementError()
}

// admitBoundStatement is execution's adapter into the shared scope program.
// Administrative commands have a typed command continuation; graph statements
// cannot decline binding or substitute successful empty facts.
func (e *StorageExecutor) admitBoundStatement(ctx context.Context, source string) (boundScopeAdmission, error) {
	// Commands have a real command continuation with its own syntax and
	// result contract; the statement kind is classifyStatement's.
	if class := classifyStatement(source); class.commandOwned || class.command != commandNone {
		return boundScopeAdmission{command: true}, nil
	}
	if _, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource); err != nil {
		if err := ctx.Err(); err != nil {
			return boundScopeAdmission{}, err
		}
		return boundScopeAdmission{}, syntaxAdmissionError(ctx, source, AcceptedSource, err)
	} else if err := ctx.Err(); err != nil {
		return boundScopeAdmission{}, err
	}
	incoming := make([]string, 0, len(e.fabricRecordBindings)+len(valueBindingsFromContext(ctx)))
	for name := range e.fabricRecordBindings {
		incoming = append(incoming, name)
	}
	for name := range valueBindingsFromContext(ctx) {
		incoming = append(incoming, name)
	}
	schema := e.participantSchemaForStatement(ctx, source)
	if schema == nil {
		var retained []string
		for name := range pipelineImports(ctx) {
			retained = append(retained, name)
		}
		program := e.projectedStatementScope(ctx, source, resultmetadata.Known(incoming), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25")
		if !program.complete {
			if program.err != nil {
				return boundScopeAdmission{}, program.err
			}
			return boundScopeAdmission{}, unresolvedStatementError()
		}
		return boundScopeAdmission{graph: true, columns: program.statement.Columns, program: program.bound}, nil
	}
	admission := boundScopeAdmission{}
	var err error
	program := e.statementProjectionProgram(ctx, source, resultmetadata.KnownScope(incoming), cypherVersionFromContext(ctx) == "25", schema)
	if err := ctx.Err(); err != nil {
		return boundScopeAdmission{}, err
	}
	program.facts.Visit(func(statement resultmetadata.Statement) {
		admission = boundScopeAdmission{graph: true, columns: statement.Columns}
	}, func() { err = unresolvedStatementError() }, func(reason resultmetadata.Unresolved) { err = reason.Err })
	return admission, err
}
