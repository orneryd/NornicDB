package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestReleasePropertySuppressionQueryMaterialization(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		check       func(*testing.T, interface{})
	}{
		{"endpoint property", "MATCH (:Doc)-[r:LINK]->() RETURN startNode(r).body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"coalesce property", "MATCH (d:Doc) RETURN coalesce(d.body, 'fallback')", func(t *testing.T, v interface{}) { require.Equal(t, "fallback", v) }},
		{"optional traversal property", "MATCH (d:Doc) OPTIONAL MATCH (d)-[r:LINK]->() RETURN d.body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"node aggregate property", "MATCH (d:Doc) RETURN sum(d.secretNumber)", func(t *testing.T, v interface{}) { require.EqualValues(t, 0, v) }},
		{"relationship aggregate property", "MATCH (:Doc)-[r:LINK]->() RETURN sum(r.secretWeight)", func(t *testing.T, v interface{}) { require.EqualValues(t, 0, v) }},
		{"order hidden property", "MATCH (:Doc)-[r:LINK]->() RETURN r.title ORDER BY r.secretWeight, r.title LIMIT 1", func(t *testing.T, v interface{}) { require.Equal(t, "a", v) }},
		{"aliased property", "MATCH (d:Doc) WITH d AS n RETURN n.body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"subscript property", "MATCH (d:Doc) RETURN d['body']", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"WHERE property", "MATCH (d:Doc) WHERE d.body IS NULL RETURN d.title", func(t *testing.T, v interface{}) { require.Equal(t, "visible", v) }},
		{"mutating alias retains hidden storage", "MATCH (d:Doc) WITH d AS n SET n.title='updated' RETURN n", func(t *testing.T, v interface{}) {
			require.NotContains(t, v.(*storage.Node).Properties, "body")
			require.Equal(t, "updated", v.(*storage.Node).Properties["title"])
		}},
		{"collected entities", "MATCH (d:Doc) RETURN collect(d)", assertVisibleQueryEntities},
		{"path entity", "MATCH p=(d:Doc)-[:LINK]->() RETURN p", assertVisibleQueryEntities},
		{"path scalar", "MATCH p=(d:Doc)-[:LINK]->() RETURN [n IN nodes(p) | n.body]", func(t *testing.T, v interface{}) { require.Equal(t, []interface{}{nil, nil}, v) }},
		{"relationship scalar", "MATCH (:Doc)-[r:LINK]->() RETURN r.summary", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"relationship entity", "MATCH (:Doc)-[r:LINK]->() RETURN r", assertVisibleQueryEntities},
		{"relationship properties", "MATCH (:Doc)-[r:LINK]->() RETURN properties(r)", func(t *testing.T, v interface{}) { require.NotContains(t, v.(map[string]interface{}), "summary") }},
		{"relationship keys", "MATCH (:Doc)-[r:LINK]->() RETURN keys(r)", func(t *testing.T, v interface{}) { require.NotContains(t, v, "summary") }},
		{"relationship projection", "MATCH (:Doc)-[r:LINK]->() RETURN r {.*}", func(t *testing.T, v interface{}) { require.NotContains(t, v.(map[string]interface{}), "summary") }},
		{"elapsed cache", "MATCH (d:Doc) RETURN d.body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"cached raw read", "MATCH (d:Doc) RETURN d.body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"property", "MATCH (d:Doc) RETURN d.body", func(t *testing.T, v interface{}) { require.Nil(t, v) }},
		{"entity", "MATCH (d:Doc) RETURN d", func(t *testing.T, v interface{}) { require.NotContains(t, v.(*storage.Node).Properties, "body") }},
		{"properties", "MATCH (d:Doc) RETURN properties(d)", func(t *testing.T, v interface{}) { require.NotContains(t, v.(map[string]interface{}), "body") }},
		{"keys", "MATCH (d:Doc) RETURN keys(d)", func(t *testing.T, v interface{}) { require.NotContains(t, v, "body") }},
		{"projection", "MATCH (d:Doc) RETURN d {.*}", func(t *testing.T, v interface{}) { require.NotContains(t, v.(map[string]interface{}), "body") }},
	} {
		for _, route := range []struct {
			name   string
			scoped bool
		}{{"raw", false}, {"namespaced", true}} {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				be, err := storage.NewBadgerEngineInMemory()
				require.NoError(t, err)
				t.Cleanup(func() { _ = be.Close() })
				schema := be.GetSchemaForNamespace("testns")
				require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "doc_decay", Scope: knowledgepolicy.ScopeNode, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "doc_binding", ProfileRef: "doc_decay", TargetLabels: []string{"Doc"}, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "body", HalfLifeSeconds: 1}, {PropertyPath: "secretNumber", HalfLifeSeconds: 1}}}))
				require.NoError(t, schema.CreateDecayProfileBundle(knowledgepolicy.DecayProfileBundle{Name: "link_decay", Scope: knowledgepolicy.ScopeEdge, Function: knowledgepolicy.DecayFunctionExponential, HalfLifeSeconds: 3600 * 24 * 365, VisibilityThreshold: 0.1, ScoreFrom: knowledgepolicy.ScoreFromCreated, Enabled: true, DecayEnabled: true}))
				require.NoError(t, schema.CreateDecayProfileBinding(knowledgepolicy.DecayProfileBinding{Name: "link_binding", ProfileRef: "link_decay", TargetEdgeType: "LINK", IsEdge: true, PropertyRules: []knowledgepolicy.DecayProfilePropertyRule{{PropertyPath: "summary", HalfLifeSeconds: 1}, {PropertyPath: "secretWeight", HalfLifeSeconds: 1}}}))
				be.SetDecayEnabled(true)
				old := time.Now().Add(-time.Hour)
				if tc.name == "elapsed cache" {
					old = time.Now()
				}
				_, err = be.CreateNode(&storage.Node{ID: "testns:doc", Labels: []string{"Doc"}, Properties: map[string]interface{}{"title": "visible", "body": "hidden", "secretNumber": int64(41)}, CreatedAt: old, UpdatedAt: old})
				require.NoError(t, err)
				_, err = be.CreateNode(&storage.Node{ID: "testns:target", Labels: []string{"Target"}, Properties: map[string]interface{}{"title": "target"}, CreatedAt: old, UpdatedAt: old})
				require.NoError(t, err)
				require.NoError(t, be.CreateEdge(&storage.Edge{ID: "testns:link", StartNode: "testns:doc", EndNode: "testns:target", Type: "LINK", Properties: map[string]interface{}{"weight": int64(7), "summary": "edge-hidden", "secretWeight": int64(13), "title": "a"}, CreatedAt: old, UpdatedAt: old}))
				if tc.name == "order hidden property" {
					require.NoError(t, be.CreateEdge(&storage.Edge{ID: "testns:link-second", StartNode: "testns:doc", EndNode: "testns:target", Type: "LINK", Properties: map[string]interface{}{"secretWeight": int64(1), "title": "b"}, CreatedAt: old, UpdatedAt: old}))
				}
				var engine storage.Engine = be
				if route.scoped {
					engine = storage.NewNamespacedEngine(be, "testns")
				}
				exec := NewStorageExecutorWithQueryCachePolicy(engine, 100, time.Minute)
				if tc.name == "entity" {
					raw, err := be.GetNode("testns:doc")
					require.NoError(t, err)
					projected := exec.projectVisiblePipelineNodeRead(context.Background(), []*storage.Node{raw}, []returnItem{{expr: "d", alias: "d"}, {expr: "d.body", alias: "body"}}, "d")
					require.NotContains(t, projected.Rows[0][0].(*storage.Node).Properties, "body")
					require.Nil(t, projected.Rows[0][1])
					require.Equal(t, "hidden", raw.Properties["body"])
				}
				if tc.name == "cached raw read" || tc.name == "elapsed cache" {
					if tc.name == "cached raw read" {
						be.SetDecayEnabled(false)
					}
					warm, err := exec.Execute(context.Background(), tc.query, nil)
					require.NoError(t, err)
					require.Len(t, warm.Rows, 1)
					require.Equal(t, "hidden", warm.Rows[0][0])
					if tc.name == "cached raw read" {
						hitsBefore, _, _, _, _ := exec.cache.Stats()
						_, err = exec.Execute(context.Background(), tc.query, nil)
						require.NoError(t, err)
						hitsAfter, _, _, _, _ := exec.cache.Stats()
						require.Greater(t, hitsAfter, hitsBefore, "ordinary reads still hit cache while decay is disabled")
						be.SetDecayEnabled(true)
					} else {
						require.Eventually(t, func() bool {
							return be.FilterPropertyByDecay("testns:doc", []string{"Doc"}, "body", old.UnixNano(), old.UnixNano(), time.Now().UnixNano())
						}, 8*time.Second, 50*time.Millisecond, "property must cross its actual visibility threshold without a graph mutation")
					}
				}
				result, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				tc.check(t, result.Rows[0][0])
				raw, err := be.GetNode("testns:doc")
				require.NoError(t, err)
				require.Equal(t, "hidden", raw.Properties["body"])
				rawEdge, err := be.GetEdge("testns:link")
				require.NoError(t, err)
				require.Equal(t, "edge-hidden", rawEdge.Properties["summary"])
				require.Equal(t, int64(13), rawEdge.Properties["secretWeight"])

				revealed, err := exec.Execute(context.Background(), "MATCH (d:Doc) RETURN reveal(d)", nil)
				require.NoError(t, err)
				require.Len(t, revealed.Rows, 1)
				require.Equal(t, "hidden", revealed.Rows[0][0].(*storage.Node).Properties["body"])
				again, err := exec.Execute(context.Background(), tc.query, nil)
				require.NoError(t, err)
				require.Len(t, again.Rows, 1)
				tc.check(t, again.Rows[0][0])
			})
		}
	}
}

func assertVisibleQueryEntities(t *testing.T, value interface{}) {
	t.Helper()
	entities := 0
	var walk func(interface{})
	walk = func(value interface{}) {
		switch v := value.(type) {
		case *storage.Node:
			entities++
			require.NotContains(t, v.Properties, "body")
		case *storage.Edge:
			entities++
			require.NotContains(t, v.Properties, "summary")
		case *PathResult:
			for _, n := range v.Nodes {
				walk(n)
			}
			for _, r := range v.Relationships {
				walk(r)
			}
		case map[string]interface{}:
			for _, x := range v {
				walk(x)
			}
		case []interface{}:
			for _, x := range v {
				walk(x)
			}
		case []*storage.Node:
			for _, x := range v {
				walk(x)
			}
		case []*storage.Edge:
			for _, x := range v {
				walk(x)
			}
		}
	}
	walk(value)
	require.Positive(t, entities, "result must contain an entity whose properties were checked")
}

// Optional matches and collected values can carry typed nil containers. Projection
// preserves their shape instead of turning a missing value into an empty value.
func TestReleasePropertyProjectionMissingValues(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	for _, value := range []interface{}{(*storage.Node)(nil), (*storage.Edge)(nil), (*PathResult)(nil), []interface{}(nil), []*storage.Node(nil), []*storage.Edge(nil), map[string]interface{}(nil)} {
		require.Equal(t, value, exec.projectVisibleQueryValue(value))
		require.Nil(t, exec.queryEntityProperties(value))
	}
	edge := &storage.Edge{ID: "r", Properties: map[string]interface{}{"visible": true}}
	got := exec.projectVisibleQueryValue([]*storage.Edge{edge}).([]*storage.Edge)
	require.Equal(t, edge, got[0])
	require.NotSame(t, edge, got[0])
	require.Nil(t, exec.queryEntityProperties(42))
	node := &storage.Node{ID: "n", Properties: map[string]interface{}{"visible": true}}
	result := exec.projectVisiblePipelineNodeRead(context.Background(), []*storage.Node{node}, []returnItem{{expr: "n", alias: "n"}}, "n")
	require.Same(t, node, result.Rows[0][0])
}
