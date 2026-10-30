package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineWithProjectionCases(t *testing.T) {
	type step struct {
		name, query     string
		input           []pipelineRow
		recognized      bool
		wantRows        []pipelineRow
		sameInput       bool
		unchanged       []pipelineRow
		count           int
		fields, numeric pipelineRow
	}
	n := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	starNode := &storage.Node{ID: "n1", Properties: map[string]interface{}{"name": "A"}}
	projectionNode := &storage.Node{ID: "n1", Labels: []string{"N"}, Properties: map[string]interface{}{"name": "alice"}}
	for _, group := range []struct {
		name, namespace string
		input           []pipelineRow
		steps           []step
	}{
		{name: "scalar fallback", namespace: "merge_proj_cov", input: []pipelineRow{{"n": n, "score": int64(7)}}, steps: []step{
			{name: "node and scalar aliases", query: "WITH n AS nodeAlias, score AS s", recognized: true, wantRows: []pipelineRow{{"nodeAlias": n, "s": int64(7)}}, unchanged: []pipelineRow{{"n": n, "score": int64(7)}}},
		}},
		{name: "star and aliases", namespace: "merge_with_proj_cov", input: []pipelineRow{{"n": starNode, "score": int64(7)}}, steps: []step{
			{name: "star", query: "WITH *", recognized: true, sameInput: true},
			{name: "aliases", query: "WITH n AS m, score AS s", recognized: true, wantRows: []pipelineRow{{"m": starNode, "s": int64(7)}}},
		}},
		{name: "literal aliases", namespace: "test", input: []pipelineRow{{}}, steps: []step{
			{name: "four scalar aliases", query: `WITH 'entity-single' AS entity_id, 'calls' AS relation_type, 'state-single' AS state_id, 'commit-single-row' AS commit_hash`, recognized: true, wantRows: []pipelineRow{{"entity_id": "entity-single", "relation_type": "calls", "state_id": "state-single", "commit_hash": "commit-single-row"}}},
		}},
		{name: "projection branches", input: []pipelineRow{{"n": projectionNode, "m": map[string]interface{}{"score": int64(9)}}}, steps: []step{
			{name: "empty WITH", query: "WITH", recognized: true, sameInput: true},
			{name: "aggregate", query: "WITH count(*) AS c", input: []pipelineRow{{"x": 1}, {"x": 2}}, recognized: true, count: 1, numeric: pipelineRow{"c": int64(2)}},
			{name: "node map and literal", query: "WITH n.name AS name, m.score AS score, 42 AS answer", recognized: true, count: 1, fields: pipelineRow{"name": "alice"}, numeric: pipelineRow{"score": int64(9), "answer": int64(42)}},
			{name: "nested literals", query: "WITH [x, [], {}] AS values, [x, []][1] AS nested", input: []pipelineRow{{"x": int64(7)}}, recognized: true, count: 1, fields: pipelineRow{"values": []interface{}{int64(7), []interface{}{}, map[string]interface{}{}}, "nested": []interface{}{}}},
			{name: "count without alias", query: "WITH count(*)", recognized: false},
			{name: "unknown expression", query: "WITH unknownExpr AS x", recognized: false},
		}},
	} {
		t.Run(group.name, func(t *testing.T) {
			var store storage.Engine = newTestMemoryEngine(t)
			if group.namespace != "" {
				store = storage.NewNamespacedEngine(store, group.namespace)
			}
			exec := NewStorageExecutor(store)
			for _, tc := range group.steps {
				t.Run(tc.name, func(t *testing.T) {
					input := group.input
					if tc.input != nil {
						input = tc.input
					}
					out, ok := exec.pipelineApplyWith(context.Background(), input, originalBraceView(tc.query), nil, false)
					require.Equal(t, tc.recognized, ok)
					if !tc.recognized {
						require.Nil(t, out)
						return
					}
					if tc.sameInput {
						require.Equal(t, input, out)
					}
					if tc.wantRows != nil {
						require.Equal(t, tc.wantRows, out)
					}
					if tc.unchanged != nil {
						require.Equal(t, tc.unchanged, input)
					}
					if tc.count > 0 {
						require.Len(t, out, tc.count)
					}
					for key, want := range tc.fields {
						require.Equal(t, want, out[0][key])
					}
					for key, want := range tc.numeric {
						require.EqualValues(t, want, out[0][key])
					}
				})
			}
		})
	}
}

func TestPipelineMatchCases(t *testing.T) {
	base := newTestMemoryEngine(t)
	store := storage.NewNamespacedEngine(base, "pipeline_match_cov")
	n := &storage.Node{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"id": "p1"}}
	_, err := store.CreateNode(n)
	require.NoError(t, err)
	require.NoError(t, store.CreateEdge(&storage.Edge{ID: "e1", Type: "R", StartNode: "n1", EndNode: "n1"}))
	unusedCalls := 0
	rows := []pipelineRow{{
		"node":          n,
		"edge":          &storage.Edge{ID: "e2", Type: "R", StartNode: "n1", EndNode: "n1"},
		"m":             map[string]interface{}{"id": "p1"},
		"mapWithUnused": map[string]interface{}{"unused": countedLiteralStringer{calls: &unusedCalls}},
		"x":             int64(1), "unused": countedLiteralStringer{calls: &unusedCalls},
	}}
	matchErr := errors.New("match lookup failed")
	for _, tc := range []struct {
		name, query string
		failure     error
	}{
		{name: "scalar bindings", query: "MATCH (p:Person {id: m.id})"},
		{name: "lookup error", query: "MATCH (p:Person)", failure: matchErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var engine storage.Engine = store
			if tc.failure != nil {
				engine = &matchErrEngine{Engine: store, err: tc.failure}
			}
			exec := NewStorageExecutor(engine)
			out, ok, err := exec.pipelineApplyMatch(context.Background(), rows, originalBraceView(tc.query))
			require.True(t, ok)
			if tc.failure != nil {
				require.Error(t, err)
				require.ErrorIs(t, err, tc.failure)
				require.Nil(t, out)
			} else {
				require.NoError(t, err)
				require.Len(t, out, 1)
				require.NotNil(t, out[0]["p"])
				require.Zero(t, unusedCalls)
			}
		})
	}
}
