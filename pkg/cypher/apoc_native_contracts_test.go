package cypher

import (
	"context"
	apocnode "github.com/orneryd/nornicdb/apoc/node"
	apocnodes "github.com/orneryd/nornicdb/apoc/nodes"
	apocpath "github.com/orneryd/nornicdb/apoc/path"
	apocrel "github.com/orneryd/nornicdb/apoc/rel"
	apoctime "github.com/orneryd/nornicdb/apoc/temporal"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func nativeAPOCFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	withPluginFunctions(t, map[string]interface{}{
		"apoc.node.id": apocnode.ID, "apoc.node.labels": apocnode.Labels, "apoc.node.degree": apocnode.Degree,
		"apoc.rel.id": apocrel.ID, "apoc.rel.type": apocrel.Type, "apoc.rel.startNode": apocrel.StartNode, "apoc.rel.endNode": apocrel.EndNode,
		"apoc.nodes.connected": apocnodes.Connected, "apoc.path.elements": apocpath.Elements, "apoc.path.slice": apocpath.Slice, "apoc.path.combine": apocpath.Combine, "apoc.temporal.format": apoctime.Format,
	})
	s := storage.NewNamespacedEngine(newTestMemoryEngine(t), "native_apoc")
	for _, id := range []storage.NodeID{"a", "b", "c"} {
		_, err := s.CreateNode(&storage.Node{ID: id, Labels: []string{"NativeAPOC"}, Properties: map[string]interface{}{"key": string(id)}})
		require.NoError(t, err)
	}
	require.NoError(t, s.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "APOC_R"}))
	require.NoError(t, s.CreateEdge(&storage.Edge{ID: "bc", StartNode: "b", EndNode: "c", Type: "APOC_S"}))
	return NewStorageExecutor(s)
}

func TestAPOCNativeEntityAndTemporalContracts(t *testing.T) {
	e := nativeAPOCFixture(t)
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"MATCH (a:NativeAPOC {key:'a'}) RETURN apoc.node.id(a)", "a"},
		{"MATCH (a:NativeAPOC {key:'a'}) RETURN apoc.node.labels(a)", []interface{}{"NativeAPOC"}},
		{"MATCH (b:NativeAPOC {key:'b'}) RETURN apoc.node.degree(b)", int64(2)},
		{"MATCH ()-[r:APOC_R]->() RETURN apoc.rel.id(r)", "ab"},
		{"MATCH ()-[r:APOC_R]->() RETURN apoc.rel.type(r)", "APOC_R"},
		{"MATCH ()-[r:APOC_R]->() RETURN apoc.rel.startNode(r).key", "a"},
		{"MATCH ()-[r:APOC_R]->() RETURN apoc.rel.endNode(r).key", "b"},
		{"MATCH (a:NativeAPOC {key:'a'}),(b:NativeAPOC {key:'b'}) RETURN apoc.nodes.connected(a,b,'APOC_R>')", true},
		{"MATCH (a:NativeAPOC {key:'a'}),(b:NativeAPOC {key:'b'}) RETURN apoc.nodes.connected(b,a,'APOC_R>')", false},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->()-[:APOC_S]->() RETURN size(apoc.path.elements(p))", int64(5)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->()-[:APOC_S]->() RETURN length(apoc.path.slice(p,0,1))", int64(1)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->()-[:APOC_S]->() RETURN length(apoc.path.slice(p,1))", int64(1)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->()-[:APOC_S]->() RETURN length(apoc.path.slice(p,5,1))", int64(0)},
		{"MATCH p=(a:NativeAPOC {key:'a'})-[:APOC_R]->(b), q=(b)-[:APOC_S]->() RETURN length(apoc.path.combine(p,q))", int64(2)},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'yyyy-MM-dd')", "2020-01-02"},
		{"RETURN apoc.temporal.format(date('2020-01-02'))", "2020-01-02"},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r, err := e.Execute(context.Background(), tc.query, nil)
			require.NoError(t, err)
			require.Len(t, r.Rows, 1)
			require.EqualValues(t, tc.want, r.Rows[0][0])
		})
	}
}

func TestAPOCNativeContractsBoundaries(t *testing.T) {
	e := nativeAPOCFixture(t)
	ctx := context.Background()
	for _, q := range []string{"RETURN apoc.node.id(null)", "RETURN apoc.node.labels(null)", "RETURN apoc.rel.id(null)", "RETURN apoc.rel.type(null)", "RETURN apoc.rel.startNode(null)", "RETURN apoc.rel.endNode(null)", "RETURN apoc.path.elements(null)", "RETURN apoc.path.slice(null)", "RETURN apoc.path.combine(null,null)"} {
		t.Run(q, func(t *testing.T) {
			r, err := e.Execute(ctx, q, nil)
			require.NoError(t, err)
			require.Nil(t, r.Rows[0][0])
		})
	}
	for _, q := range []string{"RETURN apoc.node.degree(null)", "RETURN apoc.temporal.format(null)", "MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->() RETURN apoc.path.combine(p,p)"} {
		t.Run(q, func(t *testing.T) {
			_, err := e.Execute(ctx, q, nil)
			requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
		})
	}
	for _, q := range []string{"RETURN apoc.rel.startnode(null)", "RETURN apoc.Rel.type(null)"} {
		_, err := e.Execute(ctx, q, nil)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
	for _, tc := range []struct {
		q    string
		want interface{}
	}{
		{"RETURN apoc.nodes.connected(null,null)", false},
		{"MATCH (a:NativeAPOC {key:'a'}) RETURN apoc.node.degree(a,'APOC_S>')", int64(0)},
		{"MATCH (b:NativeAPOC {key:'b'}) RETURN apoc.node.degree(b,'<APOC_R')", int64(1)},
		{"MATCH (a:NativeAPOC {key:'a'}),(b:NativeAPOC {key:'b'}) RETURN apoc.nodes.connected(b,a,'<APOC_R')", true},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->() RETURN length(apoc.path.slice(p,-1,-2))", int64(0)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->() RETURN length(apoc.path.combine(null,p))", int64(1)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->() RETURN length(apoc.path.combine(p,null))", int64(1)},
		{"MATCH p=(:NativeAPOC {key:'a'})-[:APOC_R]->() WHERE size(apoc.path.elements(p))=3 RETURN length(apoc.path.slice(p))", int64(1)},
		{"RETURN apoc.temporal.format('unchanged','yyyy-MM-dd')", "unchanged"},
		{"RETURN apoc.temporal.format(1.0,'yyyy')", "1.0"},
	} {
		t.Run(tc.q, func(t *testing.T) {
			r, err := e.Execute(ctx, tc.q, nil)
			require.NoError(t, err)
			require.Len(t, r.Rows, 1)
			require.EqualValues(t, tc.want, r.Rows[0][0])
		})
	}
	// Self-relationships contribute twice to BOTH degree, once per directed degree.
	require.NoError(t, e.storage.CreateEdge(&storage.Edge{ID: "loop", StartNode: "a", EndNode: "a", Type: "APOC_LOOP"}))
	r, err := e.Execute(ctx, "MATCH (a:NativeAPOC {key:'a'}) RETURN apoc.node.degree(a),apoc.node.degree(a,'APOC_LOOP>')", nil)
	require.NoError(t, err)
	require.EqualValues(t, []interface{}{int64(3), int64(1)}, r.Rows[0])
}

func TestAPOCNativeOverrideAndTransactionalReads(t *testing.T) {
	e := nativeAPOCFixture(t)
	withPluginFunctions(t, map[string]interface{}{"apoc.node.id": func(*storage.Node) string { return "override" }})
	r, err := e.Execute(context.Background(), "MATCH (a:NativeAPOC {key:'a'}) RETURN apoc.node.id(a)", nil)
	require.NoError(t, err)
	require.Equal(t, "override", r.Rows[0][0])
	// Removing a plugin must not remove the native current-graph implementations.
	withPluginFunctions(t, nil)
	r, err = e.Execute(context.Background(), "MATCH (a:NativeAPOC {key:'a'}),(c:NativeAPOC {key:'c'}) CREATE (c)-[:ADDED]->(a) RETURN apoc.node.degree(a,'<ADDED')", nil)
	require.NoError(t, err)
	require.EqualValues(t, int64(1), r.Rows[0][0])
}

func TestAPOCNativeTemporalOracle(t *testing.T) {
	e := nativeAPOCFixture(t)
	for _, tc := range []struct{ expression, want string }{
		{"apoc.temporal.format(duration('P1DT2H3M4.005S'),'dd HH:mm:ss.SSS')", "01 02:03:04.500"},
		{"apoc.temporal.format(duration('PT0.005S'),'S SS SSS SSSSSSSSS')", "5 50 500 005000000"},
		{"apoc.temporal.format(duration('P1Y14M15DT25H61M61.005S'),'yyyy MM dd ww qq HH mm ss SSS A N')", "0002 02 15 02 00 26 02 01 500 93721005 N"},
		{"apoc.temporal.format(duration('P1DT2H'),'yyyy-MM-dd HH:mm:ss')", "0000-00-01 02:00:00"},
		{"apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_date_time')", "2020-01-02T03:04:05Z"},
		{"apoc.temporal.format(date('2020-01-02'),'iso_date')", "2020-01-02"},
		{"apoc.temporal.format([1,2],'yyyy')", "[1, 2]"},
		{"apoc.temporal.format({a:1},'yyyy')", "{a=1}"},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			r, err := e.Execute(context.Background(), "RETURN "+tc.expression, nil)
			require.NoError(t, err)
			require.Equal(t, tc.want, r.Rows[0][0])
		})
	}
	_, err := e.Execute(context.Background(), "RETURN apoc.temporal.format('hello','not-pattern')", nil)
	requireStatusCode(t, err, "Neo.ClientError.Procedure.ProcedureCallFailed")
}

func TestAPOCNativeRemoteTransportPaths(t *testing.T) {
	// Genuine HTTP graph metadata, including a recorded path, mints the typed
	// values. Ordinary maps never acquire graph provenance through these aliases.
	wire, err := os.ReadFile("../storage/testdata/remote_http_empty_entities.json")
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(wire)
	}))
	t.Cleanup(server.Close)
	remote, err := storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: server.URL, Database: "remote"})
	require.NoError(t, err)
	t.Cleanup(func() { _ = remote.Close() })
	_, rows, err := remote.QueryCypher(context.Background(), "RETURN recorded_fixture", nil)
	require.NoError(t, err)
	path := rows[0][5].(storage.RemotePathValue)
	e := nativeAPOCFixture(t)
	for _, tc := range []struct {
		expression string
		want       interface{}
	}{
		{"size(apoc.path.elements($p))", int64(3)},
		{"length(apoc.path.slice($p,1))", int64(0)},
		{"length(apoc.path.combine(apoc.path.slice($p,0,0),$p))", int64(1)},
		{"apoc.node.labels($node)", []interface{}{"RemoteEmptyFixture"}},
	} {
		t.Run(tc.expression, func(t *testing.T) {
			r, err := e.Execute(context.Background(), "RETURN "+tc.expression, map[string]interface{}{"p": path, "node": rows[0][1]})
			require.NoError(t, err)
			if tc.expression == "apoc.node.labels($node)" {
				require.Equal(t, path[0]["labels"], r.Rows[0][0])
			} else {
				require.EqualValues(t, tc.want, r.Rows[0][0])
			}
		})
	}
	_, err = e.Execute(context.Background(), "RETURN apoc.path.elements($p)", map[string]interface{}{"p": []interface{}{path[0]}})
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
}

func TestAPOCNativeNamedTemporalOracle(t *testing.T) {
	e := nativeAPOCFixture(t)
	for _, tc := range []struct{ query, want, code string }{
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'basic_iso_date')", "20200102Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_date')", "2020-01-02Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_local_date')", "2020-01-02", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_offset_date')", "2020-01-02Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_date_time')", "2020-01-02T03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_local_date_time')", "2020-01-02T03:04:05", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_offset_date_time')", "2020-01-02T03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_zoned_date_time')", "2020-01-02T03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_instant')", "2020-01-02T03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_time')", "03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_local_time')", "03:04:05", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_offset_time')", "03:04:05Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_ordinal_date')", "2020-002Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'iso_week_date')", "2020-W01-4Z", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'rfc_1123_date_time')", "Thu, 2 Jan 2020 03:04:05 GMT", ""},
		{"RETURN apoc.temporal.format(datetime('2020-01-02T03:04:05Z'),'year_month')", "2020-01", ""},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_date_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_local_date_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_offset_date')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_local_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_offset_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_instant')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(date('2020-01-02'),'iso_zoned_date_time')", "", "Neo.ClientError.Procedure.ProcedureCallFailed"},
		{"RETURN apoc.temporal.format(duration('P1DT2H3M4.005S'),'iso_date_time')", "0000-00-01T02:03:04.5", ""},
		{"RETURN apoc.temporal.format(duration('P1DT2H3M4.005S'),'iso_time')", "02:03:04.5", ""},
	} {
		t.Run(tc.query, func(t *testing.T) {
			r, err := e.Execute(context.Background(), tc.query, nil)
			if tc.code != "" {
				requireStatusCode(t, err, tc.code)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, r.Rows[0][0])
		})
	}
}

func TestAPOCNativeCheckedOwnerErrors(t *testing.T) {
	e := nativeAPOCFixture(t)
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		args []interface{}
	}{
		{"apoc.node.degree", nil}, {"apoc.node.degree", []interface{}{&storage.Node{ID: "a"}, int64(1)}},
		{"apoc.node.degree", []interface{}{map[string]interface{}{"id": "a"}}},
		{"apoc.node.id", []interface{}{&storage.Edge{ID: "ab"}}},
		{"apoc.rel.id", []interface{}{&storage.Node{ID: "a"}}},
		{"apoc.node.labels", []interface{}{int64(1)}},
		{"apoc.nodes.connected", []interface{}{&storage.Node{ID: "a"}, int64(1)}},
		{"apoc.path.elements", []interface{}{map[string]interface{}{"length": 1}}},
		{"apoc.path.slice", []interface{}{int64(1)}},
		{"apoc.path.slice", []interface{}{PathResult{Nodes: []*storage.Node{{ID: "a"}}}, "invalid"}},
		{"apoc.path.combine", []interface{}{PathResult{Nodes: []*storage.Node{{ID: "a"}}}, int64(1)}},
		{"apoc.temporal.format", []interface{}{nil}},
		{"apoc.temporal.format", []interface{}{int64(1), int64(1)}},
	} {
		_, handled, err := e.evaluateNativeAPOCFunction(ctx, tc.name, tc.args)
		require.True(t, handled)
		require.Error(t, err, tc.name)
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()
	_, handled, err := e.evaluateNativeAPOCFunction(cancelCtx, "apoc.node.id", []interface{}{nil})
	require.True(t, handled)
	require.ErrorIs(t, err, context.Canceled)
	_, handled, err = e.evaluateNativeAPOCFunction(ctx, "unregistered", nil)
	require.False(t, handled)
	require.NoError(t, err)
	// Native Go time values must use the same formatter as parsed temporal
	// values, not the optional plugin's simplified Java-to-Go replacement.
	stamp := time.Date(2020, 1, 2, 3, 4, 5, 123000000, time.UTC)
	r, err := e.Execute(ctx, "RETURN apoc.temporal.format($stamp,'yyyy-MM-dd HH:mm:ss.SSS')", map[string]interface{}{"stamp": stamp})
	require.NoError(t, err)
	require.Equal(t, "2020-01-02 03:04:05.123", r.Rows[0][0])
}
