package bolt

import (
	"context"
	"fmt"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixture "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestIssue1327BoltMapAndPathProvenance(t *testing.T) {
	prior := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(prior) })
	base := fixture.Own(t, fixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, port := startBoltIntegrationServer(t, storage.NewNamespacedEngine(base, "neo4j"), boltIntegrationExplicitTx)
	ctx := context.Background()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", port), neo4j.NoAuth())
	require.NoError(t, err)
	t.Cleanup(func() { _ = driver.Close(ctx) })
	setup := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
	setupResult, err := setup.Run(ctx, "CREATE (:Issue1327A)-[:ISSUE1327]->(:Issue1327B)", nil)
	require.NoError(t, err)
	_, err = setupResult.Consume(ctx)
	require.NoError(t, err)
	require.NoError(t, setup.Close(ctx))
	for _, parser := range []string{"nornic", "antlr"} {
		config.SetParserType(parser)
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", parser, explicit), func(t *testing.T) {
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "neo4j"})
				defer session.Close(ctx)
				var tx neo4j.ExplicitTransaction
				if explicit {
					var e error
					tx, e = session.BeginTransaction(ctx)
					require.NoError(t, e)
					defer tx.Rollback(ctx)
				}
				run := func(query string, params map[string]any) any {
					var result neo4j.ResultWithContext
					var e error
					if explicit {
						result, e = tx.Run(ctx, query, params)
					} else {
						result, e = session.Run(ctx, query, params)
					}
					require.NoError(t, e)
					require.True(t, result.Next(ctx))
					value := result.Record().Values[0]
					require.False(t, result.Next(ctx))
					require.NoError(t, result.Err())
					return value
				}
				for _, marker := range []any{true, false, int64(17), nil} {
					t.Run(fmt.Sprintf("marker=%v", marker), func(t *testing.T) {
						value := run("RETURN {_pathResult:$marker, keep:'value'} AS m", map[string]any{"marker": marker})
						require.Equal(t, map[string]any{"_pathResult": marker, "keep": "value"}, value)
					})
				}
				for _, marker := range []any{true, false, int64(17), nil} {
					t.Run(fmt.Sprintf("graph marker=%v", marker), func(t *testing.T) {
						value := run("MATCH (a:Issue1327A)-[r:ISSUE1327]->(b:Issue1327B) RETURN {_pathResult:$marker, nodes:[a,b], rels:[r]} AS m", map[string]any{"marker": marker})
						m, ok := value.(map[string]any)
						require.True(t, ok, "got %T", value)
						require.Equal(t, marker, m["_pathResult"])
						require.Len(t, m["nodes"], 2)
						require.Len(t, m["rels"], 1)
					})
				}
				for _, query := range []string{"MATCH p=(a:Issue1327A)-[:ISSUE1327]->(b:Issue1327B) RETURN p", "MATCH p=(b:Issue1327B)<-[:ISSUE1327]-(a:Issue1327A) RETURN p"} {
					t.Run(query, func(t *testing.T) {
						value := run(query, nil)
						path, ok := value.(neo4j.Path)
						require.True(t, ok, "got %T", value)
						require.Len(t, path.Nodes, 2)
						require.Len(t, path.Relationships, 1)
					})
				}
			})
		}
	}
}

func TestIssue1327PackstreamMapAndTypedPath(t *testing.T) {
	node := &storage.Node{ID: "test:one", Labels: []string{"Node"}}
	path := cypher.PathResult{Nodes: []*storage.Node{node}}
	for _, bolt5 := range []bool{false, true} {
		for _, utc := range []bool{false, true} {
			for _, marker := range []any{true, false, int64(17), nil} {
				ordinary := map[string]any{"_pathResult": marker, "nested": map[string]any{"_pathResult": marker}}
				wire, err := encodeRecordValueInto(t.Context(), nil, ordinary, utc, bolt5, "test")
				require.NoError(t, err)
				value, used, err := decodePackStreamValue(wire, 0)
				require.NoError(t, err)
				require.Equal(t, len(wire), used)
				require.Equal(t, ordinary, value)
			}
			for _, carrier := range []any{path, &path} {
				wire, err := encodeRecordValueInto(t.Context(), nil, map[string]any{"_pathResult": carrier}, utc, bolt5, "test")
				require.NoError(t, err)
				require.Equal(t, []byte{0xB3, 0x50}, wire[:2])
			}
		}
	}
}
