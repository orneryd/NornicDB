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
	nodeImages                          ProcedureNodeImages
	functions                           cypherantlr.StatementEffects
	selectsDatabase                     bool
	// stateChange covers catalog/extension mutations whose existing owner
	// completes them without the data-write transaction wrapper.
	stateChange bool
}

// statementEffect is this contract's effect classification.
//
// statementEffectWrite is a statement whose writes are fixed by its text and
// the live registry: grammar updating clauses, a registered WRITE or SCHEMA
// procedure, ordinary schema DDL, a catalog/administration mutation or an
// unresolved procedure.
//
// statementEffectRuntime is a statement that runs statement text supplied at
// execution time and fixes no write of its own. Whether it writes is decided
// when that text is resolved against the caller's parameters, so an owner that
// needs a proven read must resolve the policy instead of refusing the
// statement; an owner that only needs "no read-only path" still treats it as a
// write.
type statementEffect int

const (
	statementEffectRead statementEffect = iota
	statementEffectWrite
	statementEffectRuntime
)

// effect is the one owner of this contract's effect classification.
func (c statementExecutionContract) effect() statementEffect {
	switch {
	case c.writes || c.stateChange || c.unknownProcedure:
		return statementEffectWrite
	case c.dynamic:
		return statementEffectRuntime
	}
	return statementEffectRead
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
	contract := statementExecutionContract{writes: effects.Writes(), transaction: effects.Writes(), calls: effects.CallCount() > 0, nodeImages: MetadataNodeImages, functions: effects}
	for i := 0; i < effects.CallCount(); i++ {
		if _, known := globalProcedureRegistry.Get(effects.Call(i).Name()); !known {
			contract.unknownProcedure = true
			contract.nodeImages = FullNodeImages
		}
	}
	visitStatementProcedureSyntax(structure, func(spec ProcedureSpec, _ string) bool {
		if spec.NodeImages == FullNodeImages {
			contract.nodeImages = FullNodeImages
		}
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
