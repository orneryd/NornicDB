package cypher

import (
	"github.com/orneryd/nornicdb/pkg/localization"
)

func validateYieldColumnsExist(columns []string, yield *yieldClause) error {
	if yield == nil || yield.yieldAll || len(yield.items) == 0 {
		return nil
	}
	available := make(map[string]struct{}, len(columns))
	for _, c := range columns {
		available[c] = struct{}{}
	}
	for _, item := range yield.items {
		if _, ok := available[item.name]; !ok {
			return localizedError(localization.CypherCommandRoutingUnknownYieldColumn(item.name), nil)
		}
	}
	return nil
}
