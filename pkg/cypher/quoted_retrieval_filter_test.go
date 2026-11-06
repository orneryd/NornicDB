package cypher

import (
	"fmt"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// quotedRetrievalFilterQuery is the searchable content of both fixture nodes:
// retrieval matches them equally, so only the property filters can tell them
// apart.
const quotedRetrievalFilterQuery = "quoted whitespace filter specimen"

// quotedRetrievalFilterNodes seeds two documents with the same searchable
// content and different properties for a whitespace-only key (" "), a
// two-space key ("  ") and a key whose leading space is part of its name
// (" key", which the second document keeps as the distinct bare key "key").
func quotedRetrievalFilterNodes() []*storage.Node {
	return []*storage.Node{
		{
			ID:     "space-a",
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"content": quotedRetrievalFilterQuery,
				" ":       "a-space",
				"  ":      "a-two-space",
				" key":    "a-leading",
			},
		},
		{
			ID:     "space-b",
			Labels: []string{"Document"},
			Properties: map[string]interface{}{
				"content": quotedRetrievalFilterQuery,
				" ":       "b-space",
				"  ":      "b-two-space",
				"key":     "b-bare",
			},
		},
	}
}

// quotedRetrievalFilterIDs runs a retrieval procedure through the public
// Execute path with the request, params-supplied as a driver sends it, and
// returns the returned node IDs sorted.
func quotedRetrievalFilterIDs(t *testing.T, exec *StorageExecutor, procedure string, request map[string]interface{}) []string {
	t.Helper()
	result, err := exec.Execute(t.Context(), fmt.Sprintf("CALL %s($request)", procedure), map[string]interface{}{
		"request": request,
	})
	require.NoError(t, err)
	ids := make([]string, 0, len(result.Rows))
	for index, row := range result.Rows {
		node, ok := row[0].(*storage.Node)
		require.Truef(t, ok, "row %d: %s node column is %T", index, procedure, row[0])
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	return ids
}

func quotedRetrievalSorted(ids []string) []string {
	sorted := make([]string, len(ids))
	copy(sorted, ids)
	sort.Strings(sorted)
	return sorted
}

// TestQuotedRetrievalFilterKeepsExactPropertyIdentity covers NOR46: a property
// filter key names a node property exactly, including keys that are only
// whitespace or that carry boundary spaces. parseRetrievalFilters used to drop
// every key whose strings.TrimSpace was empty, so {" ": [...]} constrained
// nothing and db.retrieve silently returned every match. The controls keep the
// two neighbouring behaviours: an empty value list still matches nothing
// (#938), and a truly empty key ("") still constrains nothing.
func TestQuotedRetrievalFilterKeepsExactPropertyIdentity(t *testing.T) {
	withCypherParsers(t, func(t *testing.T) {

		for _, procedure := range []string{"db.retrieve", "db.rretrieve"} {
			t.Run(procedure, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
				for _, node := range quotedRetrievalFilterNodes() {
					_, err := store.CreateNode(t.Context(), node)
					require.NoError(t, err)
				}
				svc := search.NewService(store, search.DefaultVectorDimensions, nil)
				require.NoError(t, svc.BuildIndexes(t.Context()))
				t.Cleanup(func() { require.NoError(t, svc.Close()) })
				exec := NewStorageExecutor(store)
				exec.SetSearchService(svc)

				// Control: both documents are equally searchable, so a case
				// that returns one of them is the filter, not the query.
				require.Equal(t, []string{"space-a", "space-b"}, quotedRetrievalFilterIDs(t, exec, procedure, map[string]interface{}{
					"query": quotedRetrievalFilterQuery,
					"limit": 10,
				}))

				for _, testCase := range []struct {
					name    string
					filters map[string]interface{}
					want    []string
				}{
					{
						name:    "space key keeps its own node",
						filters: map[string]interface{}{" ": []interface{}{"a-space"}},
						want:    []string{"space-a"},
					},
					{
						name:    "space key keeps the other node",
						filters: map[string]interface{}{" ": []interface{}{"b-space"}},
						want:    []string{"space-b"},
					},
					{
						name:    "two-space key keeps its own node",
						filters: map[string]interface{}{"  ": []interface{}{"a-two-space"}},
						want:    []string{"space-a"},
					},
					{
						name:    "two-space value does not satisfy the space key",
						filters: map[string]interface{}{" ": []interface{}{"a-two-space"}},
					},
					{
						name:    "leading-space key keeps the exact name",
						filters: map[string]interface{}{" key": []interface{}{"a-leading"}},
						want:    []string{"space-a"},
					},
					{
						name:    "leading-space key does not match the bare key",
						filters: map[string]interface{}{" key": []interface{}{"b-bare"}},
					},
					{
						name:    "bare key does not match the leading-space key",
						filters: map[string]interface{}{"key": []interface{}{"a-leading"}},
					},
					{
						name:    "bare key keeps its own node",
						filters: map[string]interface{}{"key": []interface{}{"b-bare"}},
						want:    []string{"space-b"},
					},
					{
						name:    "empty value list matches nothing",
						filters: map[string]interface{}{" ": []interface{}{}},
					},
					{
						name:    "empty property key constrains nothing",
						filters: map[string]interface{}{"": []interface{}{"absent"}},
						want:    []string{"space-a", "space-b"},
					},
					{
						name: "every whitespace key narrows together",
						filters: map[string]interface{}{
							" ":  []interface{}{"a-space"},
							"  ": []interface{}{"a-two-space"},
						},
						want: []string{"space-a"},
					},
				} {
					t.Run(testCase.name, func(t *testing.T) {
						request := map[string]interface{}{
							"query": quotedRetrievalFilterQuery,
							"limit": 10,
						}
						if testCase.filters != nil {
							request["filters"] = testCase.filters
						}
						require.Equal(t, quotedRetrievalSorted(testCase.want), quotedRetrievalFilterIDs(t, exec, procedure, request))
					})
				}
			})
		}
	})
}
