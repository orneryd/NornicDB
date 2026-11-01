package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
)

func declaredCommandProgram(start, end int, incoming, retained, after resultmetadata.Names, columns resultmetadata.Columns, kind resultmetadata.ClauseKind) *projectedScopeFacts {
	statement := resultmetadata.Statement{
		Start: start, End: end, Incoming: projectionScope(incoming), Retained: projectionScope(retained),
		After: projectionScope(after), Columns: columns,
		Clauses: []resultmetadata.Clause{{Kind: kind, Start: start, End: end, Before: projectionScope(incoming), After: projectionScope(after)}},
	}
	var names []string
	columns.Visit(func(values []string) { names = values }, func() {})
	return &projectedScopeFacts{after: after, outputs: resultmetadata.Known(names), statement: &statement}
}

// compileCommandResult applies the native argument owner before its metadata
// continuation. Only already supported commands declare a result here.
func (compiler *projectionCompilation) compileCommandResult(ctx context.Context, query, body cypherantlr.SyntaxNode, incoming, retained resultmetadata.Names) (*projectedScopeFacts, bool) {
	start, end := query.Span()
	bodyStart, bodyEnd := body.Span()
	source := compiler.source[bodyStart:bodyEnd]
	class := classifyStatementBody(source)
	if use := query.ChildRule(cypherantlr.CypherParserRULE_useClause); use.Valid() {
		useStart, _ := use.Span()
		if _, _, _, err := admitUseClause(ctx, compiler.source[useStart:end], false, AcceptedSource); err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, useStart, err), true
		}
	}
	if body.Rule() == cypherantlr.CypherParserRULE_regularQuery || body.Rule() == cypherantlr.CypherParserRULE_standaloneCall {
		return nil, false
	}
	commandContext := statementSourceWindow(ctx, compiler.source, cyphertext.Span{Start: bodyStart, End: bodyEnd})
	if body.Rule() == cypherantlr.CypherParserRULE_schemaCommand {
		_, err := compiler.executor.prepareSchemaCommand(commandContext, source)
		if err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, bodyStart, err), true
		}
		// Native schema mutations return no columns. The same validated plan
		// supplies EXPLAIN/PROFILE metadata without running any stored-data
		// validation or mutation continuation.
		return declaredCommandProgram(start, end, incoming, retained, incoming, resultmetadata.KnownColumns(nil), resultmetadata.ClauseWrite), true
	}
	if body.Rule() == cypherantlr.CypherParserRULE_privilegeCommand {
		if _, _, err := parseProcedurePrivilegeCommand(source); err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, bodyStart, err), true
		}
		return declaredCommandProgram(start, end, incoming, retained, incoming, resultmetadata.KnownColumns(nil), resultmetadata.ClauseWrite), true
	}
	if (class.command == commandShowUsers || class.command == commandShowCurrentUser) &&
		!body.ChildRule(cypherantlr.CypherParserRULE_showTail).Valid() {
		return declaredCommandProgram(start, end, incoming, retained, resultmetadata.Known(showUsersColumns), resultmetadata.KnownColumns(showUsersColumns), resultmetadata.ClauseOther), true
	}
	if command, supported, err := compiler.executor.prepareCompositeCommand(commandContext, source, class.command); supported {
		if err != nil {
			return programFailure(resultmetadata.ReasonUnsupportedSyntax, bodyStart, err), true
		}
		return declaredCommandProgram(start, end, incoming, retained, resultmetadata.Known(command.columns()), resultmetadata.KnownColumns(command.columns()), resultmetadata.ClauseWrite), true
	}
	return nil, false
}
