package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDbRetrieveDecodedWhitespaceFilterKeys(t *testing.T) {
	for _, key := range []string{" ", "  ", "\t"} {
		t.Run(fmt.Sprintf("key_%q", key), func(t *testing.T) {
			ctx := context.Background()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retrieve-keys")
			exec := NewStorageExecutor(store)
			for _, node := range []*storage.Node{
				{ID: "allowed", Labels: []string{"R3Whitespace"}, Properties: map[string]interface{}{"id": "allowed", key: "allow"}},
				{ID: "denied", Labels: []string{"R3Whitespace"}, Properties: map[string]interface{}{"id": "denied", key: "deny"}},
				{ID: "missing", Labels: []string{"R3Whitespace"}, Properties: map[string]interface{}{"id": "missing"}},
				{ID: "other-label", Labels: []string{"Other"}, Properties: map[string]interface{}{"id": "other-label", key: "allow"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, alias := range []string{"filters", "propertyFilters", "property_filters"} {
				t.Run(alias, func(t *testing.T) {
					check := func(filter, want string, count int64) {
						t.Helper()
						query := "CALL db.retrieve({mode:'id', types:['R3Whitespace'], " + alias + ":" + filter + ", n:10})"
						result, err := exec.Execute(ctx, query, nil)
						require.NoError(t, err)
						require.Len(t, result.Rows, 1)
						page := result.Rows[0][0].(map[string]interface{})
						require.Equal(t, count, page["returned"], query)
						hits := page["results"].([]interface{})
						require.Len(t, hits, int(count))
						if want != "" {
							node := hits[0].(map[string]interface{})["node"].(*storage.Node)
							require.Equal(t, want, node.Properties["id"])
							require.Equal(t, "allow", node.Properties[key])
						}
					}
					check("{`"+key+"`:['allow']}", "allowed", 1)
					check("{`"+key+"`:[]}", "", 0)
					// The exact empty-key sentinel drops its filter; it does not
					// stand for any of the nonempty whitespace property keys.
					result, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{"request": map[string]interface{}{
						"mode": "id", "types": []interface{}{"R3Whitespace"}, alias: map[string]interface{}{"": []interface{}{"allow"}}, "n": int64(10),
					}})
					require.NoError(t, err)
					require.Equal(t, int64(3), result.Rows[0][0].(map[string]interface{})["returned"])
				})
			}
		})
	}
}

func TestDecodedNameRetrievalTypesAndVectorProperty(t *testing.T) {
	for _, name := range []string{" ", "  ", "\t"} {
		t.Run(fmt.Sprintf("name_%q", name), func(t *testing.T) {
			ctx := context.Background()
			store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "retrieve-types")
			exec := NewStorageExecutor(store)
			for _, node := range []*storage.Node{
				{ID: "exact", Labels: []string{name}}, {ID: "unrelated", Labels: []string{"Other"}},
			} {
				_, err := store.CreateNode(node)
				require.NoError(t, err)
			}
			for _, alias := range []string{"types", "labels"} {
				result, err := exec.Execute(ctx, "CALL db.retrieve($request)", map[string]interface{}{"request": map[string]interface{}{
					"mode": "id", alias: []interface{}{name}, "n": int64(10),
				}})
				require.NoError(t, err)
				page := result.Rows[0][0].(map[string]interface{})
				require.Equal(t, int64(1), page["returned"])
				hit := page["results"].([]interface{})[0].(map[string]interface{})["node"].(*storage.Node)
				require.Equal(t, []string{name}, hit.Labels)
			}
			_, property, _, _, admitted := parseCosineReturnShape([]returnItem{{expr: "vector.similarity.cosine(n.`" + name + "`, $v)", alias: "score"}}, "n")
			require.True(t, admitted)
			require.Equal(t, name, property)
		})
	}
}
