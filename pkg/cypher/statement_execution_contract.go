package cypher

import (
	"context"
	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
)

// Statement execution modes come only from grammar-owned clauses and the live
// procedure registry. QueryInfo's substring hints belong to cache optimization;
// neither admission nor resolved execution may consume them.
//
// writes is a data write (grammar mutation effects or a registered WRITE
// procedure); authorization maps it to the WRITE privilege. stateChange is a
// command's schema or administration change. SHOW changes nothing itself.
type statementExecutionContract struct {
	writes, transaction, calls, dynamic bool
	unknownProcedure                    bool
	// stateChange: a command's own change (schema or administration), from
	// its row in statementCommandFacts.
	stateChange bool
}

// changesState is the one answer to "does this statement change state": a
// data write (grammar mutations or a registered WRITE procedure) or a
// command's schema or administration change.
func (c statementExecutionContract) changesState() bool { return c.writes || c.stateChange }

// statementChangesState is the conservative effect answer for optimizations:
// text the owner cannot read is treated as a write.
func statementChangesState(ctx context.Context, query string) bool {
	contract, err := readStatementExecutionContract(ctx, query)
	return err != nil || contract.changesState()
}

func readStatementExecutionContract(ctx context.Context, query string) (statementExecutionContract, error) {
	if classifyStatement(query).prefixMode == prefixExplain {
		// EXPLAIN plans the statement and runs nothing.
		return statementExecutionContract{}, nil
	}
	_, body := parseExecutionMode(query)
	// The statement's own CYPHER preamble sets its language version, as at
	// the execution entry; a nested statement keeps its parent's.
	ctx = withCypherVersion(ctx, body)
	class := classifyStatement(body)
	// A command's effect is its row's change. Administration CREATE commands
	// keep the implicit write wrapper used before procedure admission. A SHOW
	// command changes nothing itself but may continue with updating clauses,
	// whose effects are the grammar's, as are a graph query's.
	command := statementExecutionContract{
		writes:      class.change == changeAdministrationCreate,
		transaction: class.change == changeAdministrationCreate,
		stateChange: class.change != changeNone,
	}
	if class.change != changeNone {
		return command, nil
	}
	cypher25 := cypherVersionFromContext(ctx) == "25"
	structure, err := cypherantlr.ParseSyntax(ctx, body, cypherantlr.FragmentEntry, cypher25, cypherantlr.AcceptedSource)
	if err := ctx.Err(); err != nil {
		return statementExecutionContract{}, err
	}
	if err != nil {
		if class.command != commandNone && class.command != commandBeginScript {
			// A command's owner reads and diagnoses its own syntax.
			return command, nil
		}
		return statementExecutionContract{}, syntaxAdmissionError(ctx, body, AcceptedSource, err)
	}
	// Graph queries and SHOW commands, which may continue with updating
	// clauses, take the grammar's effects.
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
		// this contract admits registered data WRITE procedures. Both change
		// state (ProcedureSpec.writes).
		fixedWrite := spec.Mode == ProcedureModeWrite
		contract.writes = contract.writes || fixedWrite
		contract.stateChange = contract.stateChange || spec.writes()
		contract.dynamic = contract.dynamic || spec.ExecutesStatements
		contract.transaction = contract.transaction || !spec.OwnsTransactions &&
			(fixedWrite || spec.ExecutesStatements || spec.Name == "tx.setMetaData")
		return false
	})
	return contract, nil
}
