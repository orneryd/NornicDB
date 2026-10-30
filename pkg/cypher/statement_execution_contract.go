package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/fabric"
)

// Statement execution modes come only from grammar-owned clauses and the live
// procedure registry. QueryInfo's substring hints belong to cache optimization;
// neither admission nor resolved execution may consume them.
type statementExecutionContract struct {
	writes, transaction, calls, dynamic bool
	unknownProcedure                    bool
}

func readStatementExecutionContract(ctx context.Context, query string) (statementExecutionContract, error) {
	mode, bodyView := executionModeView(statementBraceView(ctx, query))
	body := bodyView.text()
	if mode == ModeExplain {
		return statementExecutionContract{}, nil
	}
	if startsWithKeywordFold(body, "SHOW") || isNornicExtensionStatement(bodyView) || isSchemaCommandStatement(body) || bareTransactionCommand(body) != "" {
		return statementExecutionContract{}, nil
	}
	if fabric.IsAdministrationCommand(body) {
		// Administration keeps its existing routing and error owner. CREATE
		// commands not handled by the system router retain the implicit
		// wrapper used before procedure admission was introduced.
		writes := startsWithKeywordFold(body, "CREATE")
		return statementExecutionContract{writes: writes, transaction: writes}, nil
	}
	cypher25 := cypherVersionFromContext(ctx) == "25"
	structure, err := cypherantlr.ParseSyntax(ctx, body, cypherantlr.FragmentEntry, cypher25, cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return statementExecutionContract{}, err
	}
	if err != nil {
		return statementExecutionContract{}, syntaxAdmissionError(ctx, body, AcceptedSource, err)
	}
	ensureBuiltInProceduresRegistered()
	effects := structure.Effects()
	contract := statementExecutionContract{writes: effects.Writes(), transaction: effects.Writes(), calls: effects.CallCount() > 0}
	for i := 0; i < effects.CallCount(); i++ {
		if _, known := globalProcedureRegistry.Get(effects.Call(i).Name()); !known {
			contract.unknownProcedure = true
		}
	}
	visitStatementProcedureSyntax(structure, func(spec ProcedureSpec, _ string) bool {
		// SCHEMA procedures keep their existing schema-transaction owner;
		// this contract admits registered data WRITE procedures.
		fixedWrite := spec.Mode == ProcedureModeWrite
		contract.writes = contract.writes || fixedWrite
		contract.dynamic = contract.dynamic || spec.ExecutesStatements
		contract.transaction = contract.transaction || !spec.OwnsTransactions &&
			(fixedWrite || spec.ExecutesStatements || spec.Name == "tx.setMetaData")
		return false
	})
	return contract, nil
}
