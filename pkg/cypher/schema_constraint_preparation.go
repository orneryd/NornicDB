package cypher

import (
	"context"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// preparedCreateConstraint contains syntax only. Preparing it neither evaluates
// option expressions nor validates or changes stored data, so EXPLAIN and
// execution can use the same declaration owner.
type preparedCreateConstraint struct {
	source      string
	ifNotExists bool
	contract    *parsedCreateConstraintContractDDL
	property    *parsedConstraintPropertyDeclaration
	cardinality *parsedCardinalityConstraintDDL
	policy      *parsedPolicyConstraintDDL
}

func (e *StorageExecutor) prepareCreateConstraint(ctx context.Context, source string) (preparedCreateConstraint, error) {
	onPosition := keywordIndexFrom(ctx, source, "ON", 0, defaultKeywordScanOpts())
	forPosition := keywordIndexFrom(ctx, source, "FOR", 0, defaultKeywordScanOpts())
	if onPosition >= 0 && (forPosition < 0 || onPosition < forPosition) {
		return preparedCreateConstraint{}, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
	}
	canonical, err := e.canonicalConstraintRequirement(ctx, source)
	if err != nil {
		return preparedCreateConstraint{}, err
	}
	prepared := preparedCreateConstraint{source: canonical, ifNotExists: strings.Contains(upperASCII(canonical), "IF NOT EXISTS")}
	if contract, err := e.parseCreateConstraintContractDDL(ctx, canonical); err == nil {
		prepared.contract = &contract
		return prepared, nil
	}
	if declaration, recognized, err := e.parseCreateConstraintPropertyDDL(ctx, canonical); recognized {
		if err != nil {
			return preparedCreateConstraint{}, err
		}
		prepared.property = &declaration
		return prepared, nil
	}
	if cardinality, err := e.parseCreateConstraintCardinalityDDL(ctx, canonical); err == nil {
		prepared.cardinality = &cardinality
		return prepared, nil
	} else if isCreateConstraintStatement(ctx, canonical) && strings.Contains(upperASCII(canonical), "REQUIRE MAX COUNT") {
		return preparedCreateConstraint{}, err
	}
	if policy, err := e.parseCreateConstraintPolicyDDL(ctx, canonical); err == nil {
		prepared.policy = &policy
		return prepared, nil
	} else if isCreateConstraintStatement(ctx, canonical) && (strings.Contains(upperASCII(canonical), "REQUIRE ALLOWED") || strings.Contains(upperASCII(canonical), "REQUIRE DISALLOWED")) {
		return preparedCreateConstraint{}, err
	}
	return preparedCreateConstraint{}, localizedError(localization.CypherSchemaInvalidSyntax("CREATE CONSTRAINT"), nil)
}
