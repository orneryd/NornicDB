package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDynamicLabelPredicatePreservesUserArguments(t *testing.T) {
	for _, expression := range []string{`x:$("__nornic_haslabels")`, `x IS $("__nornic_haslabels")`, `(x):$("__nornic_haslabels")`, `x:$(m.__nornic_haslabels)`, `x IS $(m.__nornic_haslabels)`, `(x):$(m.__nornic_haslabels)`, `x:$(__nornic_haslabels)`, `x IS $(__nornic_haslabels)`, `(x):$(__nornic_haslabels)`} {
		t.Run(expression, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "label_argument"))
			query := `CYPHER 25 CREATE (x:__nornic_haslabels) WITH x, {__nornic_haslabels:'__nornic_haslabels'} AS m, '__nornic_haslabels' AS __nornic_haslabels WHERE ` + expression + ` RETURN count(x) AS v`
			result, err := e.Execute(context.Background(), query, nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
		})
	}
}
