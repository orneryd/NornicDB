package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// Native command kinds are execution-plan operators. The selector contains no
// executable callbacks; admission can inspect the operation without reaching
// execution, cache hints or declaration bodies. Dispatch consumes the same kind.
type nativeCommandKind uint8

const (
	nativeShow nativeCommandKind = iota + 1
	nativeProcedurePrivilege
	nativeUnsupportedAdministration
	nativeCreateProcedure
	nativeDropProcedure
	nativeSchema
	nativeKnowledgePolicy
	nativeCreateComposite
	nativeReplaceDatabase
	nativeCreateDatabase
	nativeCreateAlias
	nativeDropComposite
	nativeDropDatabase
	nativeDropAlias
	nativeAlterComposite
	nativeAlterDatabase
	nativeInvalidDrop
	nativePipeline
	nativeCall
	nativeReturn
	nativeLoadCSV
	nativeUnsupported
)

type nativeCommandExecution struct {
	kind         nativeCommandKind
	continuation string
	priority     bool
}

func readNativeStatementCommand(query string) (nativeCommandExecution, bool) {
	command := nativeCommandExecution{}
	switch {
	case startsWithKeywordFold(query, "SHOW"), startsWithKeywordFold(query, "TERMINATE"):
		command.kind, command.priority = nativeShow, true
		command.continuation = showExecutionContinuation(query)
	case isProcedurePrivilegeCommand(query):
		command.kind = nativeProcedurePrivilege
	case startsWithKeywordFold(query, "GRANT"), startsWithKeywordFold(query, "REVOKE"), startsWithKeywordFold(query, "DENY"), isDropRoleCommand(query):
		command.kind = nativeUnsupportedAdministration
	case isCreateProcedureCommand(query):
		command.kind = nativeCreateProcedure
	case isDropProcedureCommand(query):
		command.kind = nativeDropProcedure
	case isOrdinarySchemaDDL(query):
		command.kind = nativeSchema
	case isKnowledgePolicyDDLStatement(query):
		command.kind = nativeKnowledgePolicy
	case startsWithKeywords(query, "CREATE", "COMPOSITE DATABASE"):
		command.kind = nativeCreateComposite
	case isCreateOrReplaceDatabaseQuery(query):
		command.kind = nativeReplaceDatabase
	case startsWithKeywords(query, "CREATE", "DATABASE"):
		command.kind = nativeCreateDatabase
	case startsWithKeywords(query, "CREATE", "ALIAS"):
		command.kind = nativeCreateAlias
	case startsWithKeywords(query, "DROP", "COMPOSITE DATABASE"):
		command.kind = nativeDropComposite
	case startsWithKeywords(query, "DROP", "DATABASE"):
		command.kind = nativeDropDatabase
	case startsWithKeywords(query, "DROP", "ALIAS"):
		command.kind = nativeDropAlias
	case startsWithKeywords(query, "ALTER", "COMPOSITE DATABASE"):
		command.kind = nativeAlterComposite
	case startsWithKeywords(query, "ALTER", "DATABASE"):
		command.kind, command.priority = nativeAlterDatabase, true
	case startsWithKeywordFold(query, "DROP"):
		command.kind = nativeInvalidDrop
	default:
		return command, false
	}
	// The graph planner has precedence over metadata handlers for CREATE
	// patterns whose path variable is spelled like a command keyword. Every
	// consumer of the native domain must make the same selection as dispatch.
	if !command.priority && startsWithKeywordFold(query, "CREATE") {
		if _, planned := canExecuteAsPipeline(query); planned {
			return nativeCommandExecution{}, false
		}
	}
	return command, true
}

func (command nativeCommandExecution) execute(e *StorageExecutor, ctx context.Context, query string) (*ExecuteResult, error) {
	switch command.kind {
	case nativeShow:
		if result, handled, err := e.executeShowStatement(ctx, query); handled {
			return result, err
		}
		return nil, unsupportedNativeStatement(query)
	case nativeProcedurePrivilege:
		return e.executeProcedurePrivilegeCommand(ctx, query)
	case nativeUnsupportedAdministration:
		return nil, unsupportedAdministrationCommandError(query)
	case nativeCreateProcedure:
		return e.executeCreateProcedure(ctx, query)
	case nativeDropProcedure:
		return e.executeDropProcedure(ctx, query)
	case nativeSchema:
		return e.executeSchemaCommand(ctx, query)
	case nativeKnowledgePolicy:
		return e.executeKnowledgePolicyDDL(ctx, query)
	case nativeCreateComposite:
		return e.executeCreateCompositeDatabase(ctx, query)
	case nativeReplaceDatabase:
		return e.executeCreateOrReplaceDatabase(ctx, query)
	case nativeCreateDatabase:
		return e.executeCreateDatabase(ctx, query)
	case nativeCreateAlias:
		return e.executeCreateAlias(ctx, query)
	case nativeDropComposite:
		return e.executeDropCompositeDatabase(ctx, query)
	case nativeDropDatabase:
		return e.executeDropDatabase(ctx, query)
	case nativeDropAlias:
		return e.executeDropAlias(ctx, query)
	case nativeAlterComposite:
		return e.executeAlterCompositeDatabase(ctx, query)
	case nativeAlterDatabase:
		return e.executeAlterDatabase(ctx, query)
	case nativeInvalidDrop:
		return nil, newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "invalid DROP clause: "+truncateQuery(query, 80))
	case nativePipeline:
		return e.executeRequiredPipeline(ctx, query)
	case nativeCall:
		return e.executeCall(ctx, query)
	case nativeReturn:
		return e.executeReturn(ctx, query)
	case nativeLoadCSV:
		return e.executeLoadCSV(ctx, query)
	}
	return nil, unsupportedNativeStatement(query)
}

// This is the native router's final physical operator selection. A statement
// rejected by its pipeline planner keeps that planner's terminal diagnostic;
// it never receives a guessed read/write mode from a failed grammar parse.
func readNativeGraphCommand(query string) nativeCommandExecution {
	kind := nativeUnsupported
	switch {
	case startsWithKeywords(query, "OPTIONAL", "MATCH"), startsWithKeywords(query, "OPTIONAL", "CALL"), startsWithKeywordFold(query, "MATCH"),
		startsWithKeywordFold(query, "CREATE"), findKeywordIndex(query, "WITH") == 0, findKeywordIndex(query, "UNWIND") == 0, findKeywordIndex(query, "FOREACH") == 0:
		kind = nativePipeline
	case findKeywordIndex(query, "CALL") == 0:
		kind = nativeCall
	case findKeywordIndex(query, "RETURN") == 0:
		kind = nativeReturn
	case findKeywordIndex(query, "LOAD CSV") == 0:
		kind = nativeLoadCSV
	}
	return nativeCommandExecution{kind: kind}
}

func unplannedNativePipelineError() error {
	return newSemanticError("Neo.ClientError.Statement.SyntaxError", "UnexpectedSyntax", "query could not be planned as a clause pipeline")
}

// Preserve the terminal owner's existing public classification and message;
// graph admission cannot replace an unknown native command's diagnostics.
func unsupportedNativeStatement(query string) error {
	words := strings.Fields(query)
	first := ""
	if len(words) > 0 {
		first = upperASCII(words[0])
	}
	return &classifiedCypherError{
		cause: localizedError(localization.CypherTransactionsQueryTypeUnsupported(first), nil),
		code:  "Neo.ClientError.Statement.SyntaxError", detail: "UnexpectedSyntax",
	}
}
