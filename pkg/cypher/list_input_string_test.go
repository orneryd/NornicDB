package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputStringConsumers(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "string_lists")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	for _, input := range listinputs.Inputs() {
		if input.Literal != "['c','a','c']" {
			continue
		}
		t.Run(input.Name, func(t *testing.T) {
			_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"StringList"}, Properties: map[string]interface{}{"name": input.Name, "xs": input.PropertyValue()}})
			require.NoError(t, err)
			params := map[string]interface{}{"name": input.Name, "xs": input.Value}
			for _, template := range []string{"string.join(%s,',')", "split('c:a:c',%s)"} {
				control, err := exec.Execute(context.Background(), listinputs.Query(template, input.Literal), nil)
				require.NoError(t, err)
				for _, form := range []struct{ prefix, value string }{
					{"", "$xs"}, {"WITH $xs AS xs ", "xs"}, {"MATCH (n:StringList {name:$name}) ", "n.xs"},
				} {
					t.Run(fmt.Sprintf("%s/%s", template, form.value), func(t *testing.T) {
						result, err := exec.Execute(context.Background(), form.prefix+listinputs.Query(template, form.value), params)
						require.NoError(t, err)
						require.Equal(t, control.Rows, result.Rows)
					})
				}
			}
		})
	}
}
