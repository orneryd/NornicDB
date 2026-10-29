// Author: Lane #1644 (Codex). Public outcome classes use real issued
// storage, with one stable failing operation and a healthy control per query.
package cypher_test

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"testing"
)

type foundationConsumerReadFault struct {
	storage.Engine
	seam   string
	cause  error
	visits int
}

func (v *foundationConsumerReadFault) Namespace() string { return storage.DatabaseOf(v.Engine) }
func (v *foundationConsumerReadFault) fail(seam string) error {
	if v.seam == seam {
		v.visits++
		return v.cause
	}
	return nil
}
func (v *foundationConsumerReadFault) AllNodes() ([]*storage.Node, error) {
	if e := v.fail("all-nodes"); e != nil {
		return nil, e
	}
	return v.Engine.AllNodes()
}
func (v *foundationConsumerReadFault) GetAllNodes() []*storage.Node {
	nodes, _ := v.AllNodes()
	return nodes
}
func (v *foundationConsumerReadFault) AllEdges() ([]*storage.Edge, error) {
	if e := v.fail("all-edges"); e != nil {
		return nil, e
	}
	return v.Engine.AllEdges()
}
func (v *foundationConsumerReadFault) GetNodesByLabel(label string) ([]*storage.Node, error) {
	if e := v.fail("label"); e != nil {
		return nil, e
	}
	return v.Engine.GetNodesByLabel(label)
}
func (v *foundationConsumerReadFault) GetNode(id storage.NodeID) (*storage.Node, error) {
	if e := v.fail("point"); e != nil {
		return nil, e
	}
	return v.Engine.GetNode(id)
}
func (v *foundationConsumerReadFault) GetEdge(id storage.EdgeID) (*storage.Edge, error) {
	if e := v.fail("edge"); e != nil {
		return nil, e
	}
	return v.Engine.GetEdge(id)
}
func (v *foundationConsumerReadFault) GetOutgoingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e := v.fail("adjacency"); e != nil {
		return nil, e
	}
	return v.Engine.GetOutgoingEdges(id)
}
func (v *foundationConsumerReadFault) GetIncomingEdges(id storage.NodeID) ([]*storage.Edge, error) {
	if e := v.fail("adjacency"); e != nil {
		return nil, e
	}
	return v.Engine.GetIncomingEdges(id)
}
func (v *foundationConsumerReadFault) NodeCount() (int64, error) {
	if e := v.fail("count"); e != nil {
		return 0, e
	}
	return v.Engine.NodeCount()
}
func TestFoundationPublicReadOutcomeConsumers(t *testing.T) {
	cases := []struct{ name, seam, query string }{
		{"node-properties", "all-nodes", "CALL db.schema.nodeProperties()"},
		{"relationship-properties", "all-edges", "CALL db.schema.relProperties()"},
		{"property-keys", "all-edges", "CALL db.propertyKeys()"},
		{"statistics-count", "count", "CALL nornicdb.stats()"},
		{"database-info-count", "count", "CALL db.info()"},
		{"id-seek", "point", "MATCH (n:Doc) WHERE id(n) IN $ids RETURN n"},
		{"export", "all-edges", "CALL apoc.export.json.all('')"},
		{"whole-graph-input", "all-nodes", "CALL apoc.algo.pageRank() YIELD node,score RETURN node,score"},
		{"selected-graph-input", "label", "CALL apoc.algo.pageRank('Doc') YIELD node,score RETURN node,score"},
		{"page-rank", "adjacency", "CALL apoc.algo.pageRank('Doc') YIELD node,score RETURN node,score"},
		{"betweenness", "adjacency", "CALL apoc.algo.betweenness('Doc') YIELD node,score RETURN node,score"},
		{"louvain", "adjacency", "CALL apoc.algo.louvain('Doc') YIELD node,community RETURN node,community"},
		{"path-algorithm", "adjacency", "CALL apoc.algo.dijkstra('a','b','R>','weight') YIELD path,weight RETURN path,weight"},
		{"a-star", "point", "CALL apoc.algo.aStar('a','b','R>','weight') YIELD path,weight RETURN path,weight"},
		{"bound-degree-expression", "adjacency", "MATCH (n:Doc) RETURN COUNT { MATCH (n)-->() } AS degree"},
		{"gds-point", "point", "RETURN gds.util.asNode('a') AS node"},
		{"gds-batch", "point", "RETURN gds.util.asNodes(['a','b']) AS nodes"},
		{"relationship-endpoint", "point", "RETURN startNode($edge).name AS name"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			base := storage.NewMemoryEngine()
			defer base.Close()
			tenant := storage.NewNamespacedEngine(base, "tenant")
			for _, id := range []storage.NodeID{"a", "b"} {
				_, e := tenant.CreateNode(&storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"name": string(id), "lat": float64(1), "lon": float64(1)}})
				require.NoError(t, e)
			}
			require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "ab", StartNode: "a", EndNode: "b", Type: "R", Properties: map[string]any{"weight": float64(1)}}))
			cause := errors.New("unrecovered required consumer read failed")
			view := &foundationConsumerReadFault{Engine: tenant, cause: cause}
			executor := cypher.NewStorageExecutor(view)
			ctx := context.Background()
			edge, edgeErr := tenant.GetEdge("ab")
			require.NoError(t, edgeErr)
			params := map[string]any{"edge": edge, "ids": []string{"a", "b"}}
			healthy, err := executor.Execute(ctx, tc.query, params)
			require.NoError(t, err, "healthy control must execute this public query")
			require.NotNil(t, healthy)
			require.NotEmpty(t, healthy.Rows)
			view.seam = tc.seam
			executor = cypher.NewStorageExecutor(view)
			result, err := executor.Execute(ctx, tc.query, params)
			require.Positive(t, view.visits, "fault must reach the actual selected public consumer")
			require.ErrorIs(t, err, cause, "required read failure must not appear as a successful result: %#v", result)
		})
	}
}
