package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFabricLogicalBaseConsumers(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		count       int
	}{
		{"numeric_union_native", "RETURN 1 AS v UNION RETURN 1.0 AS v", 1},
		{"numeric_union_fabric", "USE comp.one RETURN 1 AS v UNION USE comp.one RETURN 1.0 AS v", 1},
		{"list_union_fabric", "USE comp.one RETURN [1] AS v UNION USE comp.one RETURN [1.0] AS v", 1},
		{"map_union_fabric", "USE comp.one RETURN {k:1} AS v UNION USE comp.one RETURN {k:1.0} AS v", 1},
		{"optional", "WITH 1 AS x OPTIONAL CALL { USE comp.one MATCH (n:ReviewNoNode) RETURN n } RETURN x,n", 1},
		{"named_retained", "WITH 11 AS x CALL (x) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", 1},
		{"star_retained", "WITH 11 AS x CALL (*) { USE comp.one WITH 1 AS y RETURN x AS v } RETURN v", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			exec := fabricLogicalTypeExecutor(t)
			result, err := exec.Execute(context.Background(), tc.query, nil)
			if assert.NoError(t, err, tc.query) {
				assert.Len(t, result.Rows, tc.count, "query=%s rows=%v", tc.query, result.Rows)
			}
		})
	}
}
