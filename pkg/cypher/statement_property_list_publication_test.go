package cypher

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

func TestStreamedPropertyListsDoNotReachFutureRows(t *testing.T) {
	for _, decay := range []bool{false, true} {
		mode := "plain"
		if decay {
			mode = "decay"
		}
		t.Run(mode, func(t *testing.T) {
			for _, testCase := range []struct{ name, setup, query string }{
				{"node", "MATCH (d:Doc {title:'a'}) SET d.tags=['original']", "MATCH (d:Doc {title:'a'}) UNWIND [1,2,3,4,5,6] AS i RETURN d, d.tags[0] AS observed"},
				{"relationship", "MATCH (:Doc {title:'a'})-[r:LINK]->() SET r.tags=['original']", "MATCH (:Doc {title:'a'})-[r:LINK]->() UNWIND [1,2,3,4,5,6] AS i RETURN r, r.tags[0] AS observed"},
			} {
				t.Run(testCase.name, func(t *testing.T) {
					exec, be := newPublicConsumerViewExecutor(t, 1)
					be.SetDecayEnabled(decay)
					_, err := exec.Execute(t.Context(), testCase.setup, nil)
					require.NoError(t, err)
					ctx, cancel := context.WithCancel(t.Context())
					defer cancel()
					if decay {
						requireStatementViewAdmits(t, exec, testCase.query)
					} else {
						require.Nil(t, exec.withStatementPropertyViews(t.Context(), testCase.query))
					}
					run := executeStreamed(ctx, exec, testCase.query, 1)
					defer func() { cancel(); <-run.stream.Done() }()
					require.True(t, run.stream.streamed())
					properties := func(value interface{}) map[string]interface{} {
						switch entity := value.(type) {
						case *storage.Node:
							return entity.Properties
						case *storage.Edge:
							return entity.Properties
						default:
							t.Fatalf("unexpected entity type %T", value)
							return nil
						}
					}
					first, done := run.stream.Next(1)
					require.False(t, done)
					require.Len(t, first, 1)
					tags := properties(first[0][0])["tags"].([]string)
					require.Equal(t, "original", tags[0])
					tags[0] = "consumer changed"
					rest, done := run.stream.Next(-1)
					require.True(t, done)
					require.Len(t, rest, 5)
					<-run.stream.Done()
					require.NoError(t, run.err)
					for _, row := range rest {
						require.Equal(t, "original", properties(row[0])["tags"].([]string)[0], "a delivered row must not alter future row values")
						require.Equal(t, "original", row[1], "the ongoing statement must read its original value")
					}
				})
			}
		})
	}

}

func TestPublishedPropertyViewsDetachMutableFieldsAtBothBudgetPaths(t *testing.T) {
	for _, oversized := range []bool{false, true} {
		name := "memoized"
		if oversized {
			name = "over_budget"
		}
		t.Run(name, func(t *testing.T) {
			exec, be := newPublicConsumerViewExecutor(t, 1)
			scoped := exec.withStatementPropertyViews(t.Context(), "MATCH (d:Doc) RETURN d")
			require.NotNil(t, scoped)
			node, err := be.GetNode(t.Context(), "nor310:a")
			require.NoError(t, err)
			edge, err := be.GetEdge(t.Context(), "nor310:linka")
			require.NoError(t, err)
			node.Properties["tags"] = []string{"node value"}
			edge.Properties["tags"] = []string{"edge value"}
			node.NamedEmbeddings = map[string][]float32{"title": {1, 2}}
			node.ChunkEmbeddings = [][]float32{{3, 4}}
			node.EmbedMeta = map[string]interface{}{"tags": []string{"metadata"}}
			if oversized {
				padding := strings.Repeat("x", statementPropertyViewBytes/2)
				node.Properties["padding"], edge.Properties["padding"] = padding, padding
			}
			originalNode, originalEdge := storage.CopyNode(node), storage.CopyEdge(edge)
			_ = scoped.queryVisibleValue(node)
			_ = scoped.queryVisibleValue(edge)
			if oversized {
				require.Empty(t, scoped.statementPropertyViews.nodes)
				require.Empty(t, scoped.statementPropertyViews.edges)
			} else {
				require.NotEmpty(t, scoped.statementPropertyViews.nodes)
				require.NotEmpty(t, scoped.statementPropertyViews.edges)
			}
			row, _ := scoped.publishQueryRow([]interface{}{node, edge})
			publishedNode, publishedEdge := row[0].(*storage.Node), row[1].(*storage.Edge)
			publishedNode.Properties["tags"].([]string)[0] = "caller changed"
			publishedEdge.Properties["tags"].([]string)[0] = "caller changed"
			publishedNode.Labels[0] = "Other"
			publishedNode.NamedEmbeddings["title"][0] = 9
			publishedNode.ChunkEmbeddings[0][0] = 9
			publishedNode.EmbedMeta["tags"].([]string)[0] = "caller changed"
			require.Equal(t, originalNode, node, "publication must not expose any retained read-image field")
			require.Equal(t, originalEdge, edge)
			nextNode := scoped.queryVisibleValue(node).(*storage.Node)
			nextEdge := scoped.queryVisibleValue(edge).(*storage.Edge)
			require.Equal(t, []string{"node value"}, nextNode.Properties["tags"])
			require.Equal(t, []string{"edge value"}, nextEdge.Properties["tags"])
		})
	}
}
