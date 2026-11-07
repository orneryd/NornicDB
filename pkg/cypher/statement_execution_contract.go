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
	selectsDatabase                     bool
	// stateChange covers catalog/extension mutations whose existing owner
	// completes them without the data-write transaction wrapper.
	stateChange bool
}

// mayWrite is the conservative effect used by optimization and routing owners.
// Catalog changes and unresolved procedures must not acquire a read-only path;
// transaction and permission owners retain their separate data-write flags.
func (c statementExecutionContract) mayWrite() bool {
	return c.writes || c.stateChange || c.dynamic || c.unknownProcedure
}

func readStatementExecutionContract(ctx context.Context, query string) (statementExecutionContract, error) {
	mode, body := parseExecutionMode(query)
	if mode == ModeExplain {
		return statementExecutionContract{}, nil
	}
	if startsWithKeywordFold(body, "SHOW") || bareTransactionCommand(body) != "" {
		return statementExecutionContract{}, nil
	}
	if isNornicExtensionStatement(ctx, body) {
		return statementExecutionContract{stateChange: true}, nil
	}
	if isOrdinarySchemaDDL(ctx, body) {
		// Ordinary schema DDL is a write whose effects the implicit
		// transaction's schema view stages and publishes (schema_transaction.go):
		// the declared index or constraint and its fill from the stored nodes
		// become visible together. Catalog commands (CREATE / DROP DATABASE,
		// ALIAS, ...) keep their own contract below.
		return statementExecutionContract{writes: true, transaction: true}, nil
	}
	if isSchemaCommandStatement(body) {
		return statementExecutionContract{stateChange: true}, nil
	}
	if fabric.IsAdministrationCommand(body) {
		// Administration keeps its existing routing and error owner. CREATE
		// commands not handled by the system router retain the implicit
		// wrapper used before procedure admission was introduced.
		writes := startsWithKeywordFold(body, "CREATE")
		return statementExecutionContract{writes: writes, transaction: writes, stateChange: true}, nil
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
		contract.stateChange = contract.stateChange || spec.writes()
		contract.dynamic = contract.dynamic || spec.ExecutesStatements
		contract.transaction = contract.transaction || !spec.OwnsTransactions &&
			(fixedWrite || spec.ExecutesStatements || spec.Name == "tx.setMetaData")
		return false
	})
	// Only transaction-bearing static statements need a selection owner. A
	// retained USE token is merely a cheap admission hint: property/label names
	// can use that keyword, so only a grammar-owned clause selects the owner.
	if contract.transaction && !contract.dynamic {
		for i := 0; i < structure.TokenCount(); i++ {
			if structure.Token(i).Kind() == cypherantlr.CypherParserUSE {
				structure.Walk(func(node cypherantlr.SyntaxNode) func() {
					if node.Rule() == cypherantlr.CypherParserRULE_useClause {
						contract.selectsDatabase = true
					}
					return nil
				})
				break
			}
		}
	}
	return contract, nil
}
