package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPropertyReadersShareReceiverMeaning(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	for _, tc := range []struct {
		name  string
		value interface{}
		want  interface{}
	}{
		{"ordinary precedence", map[string]interface{}{"name": "top", "properties": map[string]interface{}{"name": "nested"}}, "top"},
		{"ordinary missing", map[string]interface{}{"properties": map[string]interface{}{"name": "nested"}}, nil},
		{"remote", storage.RemoteEntityValue{"elementId": "r", "labels": []string{"Doc"}, "properties": map[string]interface{}{"name": "Remote"}}, "Remote"},
		{"node", &storage.Node{ID: "n", Properties: map[string]interface{}{"name": "Node"}}, "Node"},
		{"relationship", &storage.Edge{ID: "e", Properties: map[string]interface{}{"name": "Edge"}}, "Edge"},
		{"null", nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, expr := range []string{"n.name", "n['name']"} {
				t.Run(expr, func(t *testing.T) {
					fixtureAdmission826Context, fixtureAdmission826 := expressionTestContext(context.Background(), expr)
					_ = fixtureAdmission826
					got := e.evaluateExpressionFromValuesContext(fixtureAdmission826, fixtureAdmission826Context, expr, map[string]interface{}{"n": tc.value})
					require.Equal(t, tc.want, got)
				})
			}
			require.Equal(t, tc.want, extractPropertyFromValue(tc.value, "name"), "CALL reader")
			require.Equal(t, tc.want, getNodeProperty(tc.value, "name"), "collection reader")
			want := tc.want
			if want == nil {
				want = "fallback"
			}
			require.Equal(t, want, e.evaluateCoalesceInContext("coalesce(n.name,'fallback')", nil, nil, map[string]interface{}{"n": tc.value}), "COALESCE reader")
		})
	}
}

func TestMaterializedAccessUsesTypedIdentity(t *testing.T) {
	for _, key := range []string{"_nodeId", "_edgeId"} {
		t.Run(key, func(t *testing.T) {
			ordinary := map[string]interface{}{key: "invented", "labels": []string{"L"}}
			require.False(t, materializedValue(nil, ordinary))
			ordinary["nested"] = &storage.Node{ID: "actual"}
			r := &recordingAccessRecorder{}
			require.True(t, materializedValue(r, ordinary))
			require.Equal(t, []string{"actual"}, r.ids)
		})
	}
	remote := storage.RemoteEntityValue{"elementId": "remote", "labels": []string{"Doc"}, "properties": map[string]interface{}{}}
	r := &recordingAccessRecorder{}
	require.True(t, materializedValue(r, remote))
	require.Equal(t, []string{"remote"}, r.ids)
}

func TestTypedNilRemoteProjectionIsNull(t *testing.T) {
	e := NewStorageExecutor(newTestMemoryEngine(t))
	var n storage.RemoteEntityValue
	result, err := e.Execute(context.Background(), "WITH $n AS n RETURN n{.*} AS v", map[string]interface{}{"n": n})
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{nil}}, result.Rows)
}
