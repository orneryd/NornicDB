package cypher

import (
	"context"
	"errors"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"strings"
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

// errParticipantSignatureDeferred is the participant schema seam's answer when
// the selected participant owns a procedure's implicit signature and this caller
// cannot read it. It is not a statement diagnostic: the graph the CALL runs on
// resolves the procedure and its columns when that participant compiles the
// fragment it runs. Only the Fabric preflight supplies this answer, and only
// for a selection whose registry the coordinator cannot read before execution.
var errParticipantSignatureDeferred error = deferredParticipantSignatureError{}

type deferredParticipantSignatureError struct{}

func (deferredParticipantSignatureError) Error() string {
	return "the selected participant owns this procedure's signature"
}

// fabricAdmission supplies the selected participant's schema resolver. Binding
// and type validation both consume the original statement, preserving USE
// boundaries and the exact syntax addressed by its retained binding program.
// The zero value uses the local registry.
type fabricAdmission struct {
	schema resultmetadata.ParticipantSchema
}

// admitStatement is execution's adapter into the shared scope program.
// Administrative commands have a typed command continuation; graph statements
// cannot decline binding or substitute successful empty facts.
func (e *StorageExecutor) admitStatement(ctx context.Context, source string, fabric fabricAdmission) (admission boundScopeAdmission, err error) {
	schema := e.participantSchemaForStatement(ctx, source)
	if fabric.schema != nil {
		schema = fabric.schema
	}
	// Binding establishes exact membership and headers before type validation.
	// Both phases finish before any execution owner can perform effects. A
	// deferred participant signature is not a statement diagnostic: the scope
	// program stops where the selected participant owns it, and the semantic
	// and type rules still run on the statement the engine validates.
	defer func() {
		if err != nil && !errors.Is(err, errParticipantSignatureDeferred) {
			return
		}
		if admission.program != nil {
			ctx = withBoundStatementProgram(ctx, admission.program)
		}
		if typeErr := e.validateStatementTypes(ctx, source); typeErr != nil {
			err = typeErr
		}
	}()
	// Extensions have a real command continuation with its own syntax and
	// result contract. The graph grammar must not reject them before routing.
	if isNornicExtensionStatement(ctx, source) || isSchemaCommandStatement(source) || isMetadataCommand(ctx, source, metadataCatalogExecution) {
		return boundScopeAdmission{command: true}, nil
	}
	syntax, err := cypherantlr.ParseSyntax(ctx, source, cypherantlr.StatementEntry, cypherVersionFromContext(ctx) == "25", cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return boundScopeAdmission{}, err
	}
	if err != nil {
		// Non-graph commands retain their command owner's terminal diagnostic.
		if startsWithKeywordFold(strings.TrimSpace(source), "SHOW") || startsWithKeywordFold(strings.TrimSpace(source), "DROP") {
			return boundScopeAdmission{command: true}, nil
		}
		return boundScopeAdmission{}, syntaxAdmissionError(ctx, source, AcceptedSource, err)
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
	admission = boundScopeAdmission{}
	program := e.statementProjectionProgram(ctx, source, resultmetadata.KnownScope(incoming), cypherVersionFromContext(ctx) == "25", schema)
	if err := ctx.Err(); err != nil {
		return boundScopeAdmission{}, err
	}
	program.facts.Visit(func(statement resultmetadata.Statement) {
		admission = boundScopeAdmission{graph: true, columns: statement.Columns, program: program.bound}
	}, func() { err = unresolvedStatementError() }, func(reason resultmetadata.Unresolved) { err = reason.Err })
	return admission, err
}
