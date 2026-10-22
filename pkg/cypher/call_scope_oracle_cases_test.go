package cypher

import "github.com/orneryd/nornicdb/internal/callscopetest"

type callScopeOracleCase = callscopetest.Case

func generatedCallScopeOracleCases() []callScopeOracleCase {
	return callscopetest.OracleCases()
}

func callScopeResponsibilityCases() []callScopeOracleCase {
	return callscopetest.ResponsibilityCases()
}
