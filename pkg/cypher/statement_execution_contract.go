package cypher

import "github.com/orneryd/nornicdb/pkg/fabric"

// Statement execution modes come only from grammar-owned clauses and the live
// procedure registry. QueryInfo's substring hints belong to cache optimization;
// neither admission nor resolved execution may consume them.
type statementExecutionContract struct {
	writes, transaction, calls, dynamic bool
}

func readStatementExecutionContract(query string) (statementExecutionContract, error) {
	structure := fabric.ReadQueryStructure(query)
	if structure.Err != nil {
		// The same selected native handler owns both execution and applicability
		// of graph admission. Stored procedure text and metadata options are not
		// graph clauses. This domain boundary adds no grammar exception list.
		if mode, inner := parseExecutionMode(query); mode != ModeNormal {
			if mode == ModeExplain {
				return statementExecutionContract{}, nil
			}
			return readStatementExecutionContract(inner)
		}
		// UNION ownership is the execution router's structural reader as well;
		// a native command in its first branch cannot hide a later graph write.
		if branches, _, _, union := parseTopLevelUnionBranches(query); union && len(branches) > 1 {
			var contract statementExecutionContract
			for _, branch := range branches {
				part, err := readStatementExecutionContract(branch)
				if err != nil {
					return statementExecutionContract{}, err
				}
				contract.writes = contract.writes || part.writes
				contract.transaction = contract.transaction || part.transaction
				contract.calls = contract.calls || part.calls
				contract.dynamic = contract.dynamic || part.dynamic
			}
			return contract, nil
		}
		if command, native := readNativeStatementCommand(query); native {
			if command.continuation != "" {
				return readStatementExecutionContract(command.continuation)
			}
			return statementExecutionContract{}, nil
		}
		if command := readNativeGraphCommand(query); command.kind == nativePipeline {
			if _, planned := canExecuteAsPipeline(query); !planned {
				return statementExecutionContract{}, unplannedNativePipelineError()
			}
		}
		return statementExecutionContract{}, fabricGraphReferenceError(structure.Err)
	}
	if structure.First("EXPLAIN") != nil {
		return statementExecutionContract{}, nil
	}
	contract := statementExecutionContract{writes: structure.Writes(), transaction: structure.Writes()}
	var visit func(*fabric.QueryStructure)
	visit = func(scope *fabric.QueryStructure) {
		for _, clause := range scope.Clauses {
			if clause.Nested != nil {
				visit(clause.Nested)
			}
			if clause.Kind != "CALL" || clause.Nested != nil {
				continue
			}
			contract.calls = true
		}
	}
	visit(structure)
	visitStructuredProcedureCalls(structure, func(spec ProcedureSpec, _ string) bool {
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
