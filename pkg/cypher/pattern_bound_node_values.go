package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/localization"
)

// patternNodeVariables caches the node variables of a MATCH body's pattern
// (matchPatternStructure), by body text.
var patternNodeVariables = newBoundedCache[string, []string](1024, boundedCacheStringShardIndex)

// validateBoundPatternNodes is Neo4j's run-time check of the node variables
// of a MATCH or OPTIONAL MATCH pattern that the input rows already bind: a
// value that is neither null nor a node is a TypeError
// (WITH x.node AS m MATCH (m) where m is a string). A variable whose type is
// known before the statement runs is rejected then (projectedOperand); a
// relationship position bound to another value only matches nothing, as in
// Neo4j. Ordinary maps remain maps even when their keys resemble metadata;
// typed graph values and values of no Cypher type retain their own handling.
func validateBoundPatternNodes(ctx context.Context, rows []pipelineRow, body string) error {
	if len(rows) == 0 || len(rows) == 1 && len(rows[0]) == 0 {
		return nil
	}
	variables, cached := patternNodeVariables.get(body)
	if !cached {
		variables = extractNodeVariables(matchPatternStructure(body))
		patternNodeVariables.put(body, variables)
	}
	ctx = withPipelineCancellation(ctx)
	probe := pipelineCancellationForContext(ctx)
	for _, row := range rows {
		if !probe.check() {
			return probe.err
		}
		for _, variable := range variables {
			if value, bound := row[variable]; bound {
				if err := boundPatternNodeValueError(variable, value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// boundPatternNodeValueError is the TypeError for a pattern's node variable
// bound to value, or nil when value is a node, null, or a value of no Cypher
// type. User maps cannot acquire node identity through metadata-shaped keys.
func boundPatternNodeValueError(variable string, value interface{}) error {
	switch cypherValueKindOf(value) {
	case valueKindNode, valueKindNull, valueKindOther:
		return nil
	}
	return localizedStatusError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType",
		localization.CypherCorePatternNodeValueType(variable, cypherTypeName(value)))
}
