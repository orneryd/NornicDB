package cypher

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// withExpressionBinding scopes an evaluated value without rendering it as
// Cypher text. Names avoid both client parameters and identifiers in the input.
func withExpressionBinding(ctx context.Context, expression string, value interface{}) (context.Context, string) {
	probe := contextPipelineProbe(ctx)
	params := getParamsFromContext(ctx)
	for index := 0; ; index++ {
		name := fmt.Sprintf("__nornic_expression_%d", index)
		if _, exists := params[name]; exists || strings.Contains(expression, name) {
			continue
		}
		bound := make(map[string]interface{}, len(params)+1)
		for key, original := range params {
			if probe.stopped() {
				return nil, ""
			}
			bound[key] = original
		}
		bound[name], _ = normalizeParameterValueWithCancellation(value, contextPipelineProbe(ctx))
		return withQueryParams(ctx, bound), "$" + name
	}
}

func dateTimeFromGoTime(value time.Time) CypherDateTime {
	zoneID := value.Location().String()
	if zoneID == "UTC" || zoneID == "Local" {
		zoneID = ""
	} else if _, ok := loadPinnedTemporalLocation(zoneID); !ok {
		zoneID = ""
	}
	return CypherDateTime{Time: value, ZoneID: zoneID}
}
