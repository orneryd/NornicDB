package fabric

import (
	"strings"

	"github.com/orneryd/nornicdb/internal/cyphertext"
)

// applyLookupProjectionFacts retains the selected concrete item payload and
// its aliases. Collect retains scalar expression facts, not a synthetic row.
type applyLookupProjectionFacts struct {
	countItems        []fabricCountReturnItem
	returnItems       []fabricReturnItem
	aliases           []string
	collectExpression string
	collectAlias      string
}

// readApplyLookupProjection owns one CSV/item/alias pass for all batched
// projection shapes. Expression admission remains specific to the result kind.
func readApplyLookupProjection(shape applyLookupShape, source, matchVar string) (applyLookupProjectionFacts, bool) {
	var facts applyLookupProjectionFacts
	switch shape {
	case applyLookupShapeCount, applyLookupShapeRows:
	case applyLookupShapeCollect:
		source = strings.TrimSpace(source)
	default:
		return facts, false
	}
	items := splitTopLevelCSV(source)
	if len(items) == 0 || shape == applyLookupShapeCollect && len(items) != 1 {
		return facts, false
	}
	for _, raw := range items {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return applyLookupProjectionFacts{}, false
		}
		expression, alias := raw, raw
		as := lastAsIndexFold(raw)
		if as >= 0 {
			expression = strings.TrimSpace(raw[:as])
			alias = strings.TrimSpace(raw[as+4:])
		}
		if !cyphertext.IsIdentifier(alias) {
			return applyLookupProjectionFacts{}, false
		}
		switch shape {
		case applyLookupShapeCount:
			if !isSimpleCountExpr(expression) {
				return applyLookupProjectionFacts{}, false
			}
			if facts.countItems == nil {
				facts.countItems = make([]fabricCountReturnItem, 0, len(items))
			}
			facts.countItems = append(facts.countItems, fabricCountReturnItem{expr: expression, alias: alias})
		case applyLookupShapeRows:
			variable, property, ok := parseFabricVarProp(expression)
			if !ok || !strings.EqualFold(variable, matchVar) {
				return applyLookupProjectionFacts{}, false
			}
			if facts.returnItems == nil {
				facts.returnItems = make([]fabricReturnItem, 0, len(items))
			}
			facts.returnItems = append(facts.returnItems, fabricReturnItem{expr: expression, prop: property, alias: alias})
		case applyLookupShapeCollect:
			if as < 0 || !startsWithFold(expression, "collect(") || !strings.HasSuffix(expression, ")") {
				return applyLookupProjectionFacts{}, false
			}
			inner := strings.TrimSpace(expression[len("collect(") : len(expression)-1])
			if inner == "" {
				return applyLookupProjectionFacts{}, false
			}
			return applyLookupProjectionFacts{collectExpression: inner, collectAlias: alias}, true
		}
		if facts.aliases == nil {
			facts.aliases = make([]string, 0, len(items))
		}
		facts.aliases = append(facts.aliases, alias)
	}
	return facts, true
}
