package storage

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/stretchr/testify/require"
)

func TestRelationshipEmitterProperty(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			calls := 0
			check := func(text string) {
				got, err := cyphernames.ReadType(text)
				require.NoError(t, err, text)
				require.Equal(t, name, got, text)
				calls++
			}
			r := &RemoteEngine{transport: &fakeRemoteTransport{
				queryFn: func(_ context.Context, text string, _ map[string]any) ([]string, [][]any, error) {
					check(text)
					if strings.Contains(text, "count(r)") || strings.Contains(text, "CREATE") || strings.Contains(text, "MERGE") {
						return []string{"value"}, [][]any{{int64(1)}}, nil
					}
					return nil, nil, nil
				},
				queryBatchFn: func(_ context.Context, statements []remoteStatement) error {
					for _, statement := range statements {
						check(statement.Statement)
					}
					return nil
				},
			}}
			edge := &Edge{Type: name, StartNode: "a", EndNode: "b"}
			require.NoError(t, r.CreateEdge(edge))
			edge.ID = "edge"
			require.NoError(t, r.CreateEdge(edge))
			require.NoError(t, r.BulkCreateEdges([]*Edge{edge}))
			_, err := r.GetEdgesByType(name)
			require.NoError(t, err)
			r.GetEdgeBetween("a", "b", name)
			_, err = r.EdgeCountByType(name)
			require.NoError(t, err)
			_, err = r.EdgeCountByStartLabel(name, name)
			require.NoError(t, err)
			_, err = r.EdgeCountByEndLabel(name, name)
			require.NoError(t, err)
			require.Equal(t, 8, calls, "every remote relationship emitter must run")
		})
	}
}
