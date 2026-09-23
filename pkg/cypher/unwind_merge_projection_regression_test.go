package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestUnwindMergeMapUpdateIsIndependentOfReturnProjection(t *testing.T) {
	tests := []struct {
		name       string
		projection string
	}{
		{name: "entity properties", projection: "item.id AS id, item.name AS name"},
		{name: "aggregate count", projection: "count(item) AS count"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor, ctx := newUnitExecutor(t)
			result, err := executor.Execute(ctx, `
				UNWIND $rows AS row
				MERGE (item:Item {id: row.id})
				SET item += {name: row.name + 'x'}
				RETURN `+test.projection, map[string]interface{}{
				"rows": []interface{}{
					map[string]interface{}{"id": int64(1), "name": "n1"},
					map[string]interface{}{"id": int64(2), "name": "n2"},
				},
			})
			require.NoError(t, err)
			if test.name == "aggregate count" {
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			}

			stored, err := executor.Execute(ctx, "MATCH (item:Item) RETURN item.id, item.name ORDER BY item.id", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1), "n1x"}, {int64(2), "n2x"}}, stored.Rows)
		})
	}
}
