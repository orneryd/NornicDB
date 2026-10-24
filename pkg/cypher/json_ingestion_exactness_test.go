package cypher

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestJSONIngestionLoadExactNumbers(t *testing.T) {
	const document = `{"v":9007199254740993,"decimal":1.25,"whole":2.0,"exponent":2e0,"nested":[{"v":-9007199254740993},true,null,"9007199254740993"]}`
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(document)) }))
	defer server.Close()
	path := filepath.Join(t.TempDir(), "numbers.json")
	require.NoError(t, os.WriteFile(path, []byte(document), 0600))
	for _, source := range []string{path, "file://" + path, "https://example.com/numbers.json"} {
		t.Run(source, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			exec.SetAllowLocalAPOCFileAccess(true)
			exec.allowRemoteAPOCURLAccess = true
			exec.apocRemoteURLAllowlist = []string{"example.com"}
			allowPublicAPOCTestHost(exec)
			exec.apocRemoteHTTPClient = &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				request = request.Clone(request.Context())
				request.URL.Host = server.Listener.Addr().String()
				return server.Client().Transport.RoundTrip(request)
			})}
			result, err := exec.Execute(context.Background(), "CALL apoc.load.json($url) YIELD value RETURN value.v, value", map[string]interface{}{"url": source})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			require.Equal(t, int64(9007199254740993), result.Rows[0][0])
			value := result.Rows[0][1].(map[string]interface{})
			require.Equal(t, 1.25, value["decimal"])
			require.Equal(t, float64(2), value["whole"])
			require.Equal(t, float64(2), value["exponent"])
			require.Equal(t, []interface{}{map[string]interface{}{"v": int64(-9007199254740993)}, true, nil, "9007199254740993"}, value["nested"])
		})
	}
}

func TestJSONIngestionCDCExactValues(t *testing.T) {
	exec, store := newCDCExecutor(t, storage.ChangeCaptureFull)
	from := cdcID(t, exec, "db.cdc.current")
	_, err := store.CreateNode(&storage.Node{ID: "exact", Labels: []string{"ExactCDC"}, Properties: map[string]interface{}{
		"v": int64(9007199254740993), "unsigned": uint64(math.MaxUint64), "decimal": 1.25,
		"nested": []interface{}{uint64(math.MaxUint64), int64(-9007199254740993)},
	}})
	require.NoError(t, err)
	rows := cdcRows(t, exec, from)
	require.Len(t, rows, 1)
	properties := stateOf(rows[0], "after")["properties"].(map[string]interface{})
	require.Equal(t, int64(9007199254740993), properties["v"])
	require.Equal(t, uint64(math.MaxUint64), properties["unsigned"])
	require.Equal(t, 1.25, properties["decimal"])
	require.Equal(t, []interface{}{uint64(math.MaxUint64), int64(-9007199254740993)}, properties["nested"])
	from = cdcID(t, exec, "db.cdc.current")
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "exact-r", StartNode: "exact", EndNode: "exact", Type: "EXACT", Properties: map[string]interface{}{"v": uint64(math.MaxUint64), "d": 2.5}}))
	rows = cdcRows(t, exec, from, map[string]interface{}{"select": "r", "key": map[string]interface{}{"v": uint64(math.MaxUint64)}})
	require.Len(t, rows, 1)
	properties = stateOf(rows[0], "after")["properties"].(map[string]interface{})
	require.Equal(t, uint64(math.MaxUint64), properties["v"])
	require.Equal(t, 2.5, properties["d"])
}

func TestJSONIngestionCDCNumericSelectors(t *testing.T) {
	exec, _ := newCDCExecutor(t, storage.ChangeCaptureFull)
	ctx := context.Background()
	from := cdcID(t, exec, "db.cdc.current")
	_, err := exec.Execute(ctx, "CREATE (:ExactCDC {v:9007199254740992})", nil)
	require.NoError(t, err)
	for _, value := range []interface{}{int64(9007199254740992), float64(9007199254740992)} {
		rows := cdcRows(t, exec, from, map[string]interface{}{"select": "n", "key": map[string]interface{}{"v": value}})
		require.Len(t, rows, 1, "selector %T", value)
	}
	require.Empty(t, cdcRows(t, exec, from, map[string]interface{}{"select": "n", "key": map[string]interface{}{"v": int64(9007199254740993)}}))
	from = cdcID(t, exec, "db.cdc.current")
	_, err = exec.Execute(ctx, "MATCH (n:ExactCDC) SET n.v = $v", map[string]interface{}{"v": float64(9007199254740992)})
	require.NoError(t, err)
	rows := cdcRows(t, exec, from, map[string]interface{}{"select": "n", "changesTo": []interface{}{"v"}})
	require.Len(t, rows, 1, "INTEGER to FLOAT remains a change")
	require.Equal(t, int64(9007199254740992), stateOf(rows[0], "before")["properties"].(map[string]interface{})["v"])
	require.Equal(t, float64(9007199254740992), stateOf(rows[0], "after")["properties"].(map[string]interface{})["v"])
}

func TestJSONIngestionCDCSelectorNullControl(t *testing.T) {
	require.True(t, holdsValues(map[string]interface{}{"v": nil}, map[string]interface{}{"v": nil}))
	require.True(t, holdsValues(map[string]interface{}{"v": []interface{}{nil, int64(2)}}, map[string]interface{}{"v": []interface{}{nil, int64(2)}}))
	require.False(t, holdsValues(map[string]interface{}{}, map[string]interface{}{"v": nil}))
	require.False(t, holdsValues(map[string]interface{}{"v": "2"}, map[string]interface{}{"v": int64(2)}))
}
