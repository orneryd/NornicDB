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
	// Extensions have a real command continuation with its own syntax and
	// result contract. The graph grammar must not reject them before routing.
	if isNornicExtensionStatement(source) {
		return boundScopeAdmission{command: true}, nil
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25")
	if err != nil {
		return boundScopeAdmission{}, &classifiedCypherError{cause: err, code: "Neo.ClientError.Statement.SyntaxError", detail: "UnexpectedSyntax"}
	}
	query := statementSyntaxQuery(syntax)
	if !query.Valid() {
		return boundScopeAdmission{command: true}, nil
	}
	if !query.ChildRule(cypherantlr.CypherParserRULE_regularQuery).Valid() && !query.ChildRule(cypherantlr.CypherParserRULE_standaloneCall).Valid() {
		return boundScopeAdmission{command: true}, nil
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
		program := e.projectedStatementScope(source, resultmetadata.Known(incoming), resultmetadata.Known(retained), cypherVersionFromContext(ctx) == "25")
		if !program.complete {
			if program.err != nil {
				return boundScopeAdmission{}, program.err
			}
			return boundScopeAdmission{}, unresolvedStatementError()
		}
		return boundScopeAdmission{graph: true, columns: program.statement.Columns, program: program.bound}, nil
	}
	admission := boundScopeAdmission{}
	e.StatementFactsWith(source, resultmetadata.KnownScope(incoming), cypherVersionFromContext(ctx) == "25", schema).Visit(func(statement resultmetadata.Statement) {
		admission = boundScopeAdmission{graph: true, columns: statement.Columns}
	}, func() { err = unresolvedStatementError() }, func(reason resultmetadata.Unresolved) { err = reason.Err })
	return admission, err
}
