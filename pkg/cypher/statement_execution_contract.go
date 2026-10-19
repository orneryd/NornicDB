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
		return statementExecutionContract{}, structure.Err
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
		contract.writes = contract.writes || spec.writes()
		contract.dynamic = contract.dynamic || spec.ExecutesStatements
		contract.transaction = contract.transaction || !spec.OwnsTransactions &&
			(spec.writes() || spec.ExecutesStatements || spec.Name == "tx.setMetaData")
		return false
	})
	return contract, nil
}
