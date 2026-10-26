// Author of retained syntax inspection: Codex R17 implementation agent.
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
}

func readStatementExecutionContract(ctx context.Context, query string) (statementExecutionContract, error) {
	mode, body := parseExecutionMode(query)
	if mode == ModeExplain {
		return statementExecutionContract{}, nil
	}
	if startsWithKeywordFold(body, "SHOW") || isNornicExtensionStatement(body) || isSchemaCommandStatement(body) || bareTransactionCommand(body) != "" {
		return statementExecutionContract{}, nil
	}
	if fabric.IsAdministrationCommand(body) {
		// Administration keeps its existing routing and error owner. CREATE
		// commands not handled by the system router retain the implicit
		// wrapper used before procedure admission was introduced.
		writes := startsWithKeywordFold(body, "CREATE")
		return statementExecutionContract{writes: writes, transaction: writes}, nil
	}
	structure, err := cypherantlr.InspectSyntax(ctx, body, cypherVersionFromContext(ctx) == "25")
	if err := ctx.Err(); err != nil {
		return statementExecutionContract{}, err
	}
	if err != nil {
		return statementExecutionContract{}, syntaxAdmissionError(body, err)
	}
	contract := statementExecutionContract{}
	visitStatementProcedureSyntax(structure, func(node cypherantlr.SyntaxNode) {
		switch node.Rule() {
		case cypherantlr.CypherParserRULE_updatingStatement:
			contract.writes, contract.transaction = true, true
		case cypherantlr.CypherParserRULE_queryCallSt, cypherantlr.CypherParserRULE_standaloneCall:
			contract.calls = true
		}
	}, func(spec ProcedureSpec, _ string) bool {
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
