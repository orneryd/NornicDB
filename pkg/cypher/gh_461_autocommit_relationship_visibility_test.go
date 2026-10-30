package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestGH461_AutocommitCreatedRelationshipVisibleAcrossTransactionsAndReopen(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "badger")
	engine, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dataDir}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
		}
	})

	ctx := context.Background()
	store := storage.NewNamespacedEngine(engine, "nornic")
	exec := NewStorageExecutor(store)
	_, err = exec.Execute(ctx, "CREATE (o:O {id:2})-[:HAS {n:5}]->(i:I {sku:'a1'})", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	for _, testCase := range []struct {
		query string
		want  [][]interface{}
	}{
		{"MATCH (o:O)-[h:HAS]->(i:I) RETURN h.n AS n", [][]interface{}{{int64(5)}}},
		{"MATCH (o)-[h:HAS]->(i) RETURN h.n AS n", [][]interface{}{{int64(5)}}},
		{"MATCH (o:O)-[h]->(i) RETURN h.n AS n", [][]interface{}{{int64(5)}}},
		{"MATCH (o)-[h]->(i:I) RETURN h.n AS n", [][]interface{}{{int64(5)}}},
		{"MATCH (o:O)-->(i:I) RETURN count(*) AS c", [][]interface{}{{int64(1)}}},
		{"MATCH ()-[h:HAS]->() RETURN count(h) AS c", [][]interface{}{{int64(1)}}},
	} {
		result, runErr := exec.Execute(ctx, testCase.query, nil)
		require.NoError(t, runErr, testCase.query)
		require.Equal(t, testCase.want, result.Rows, testCase.query)
	}
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	updated, err := exec.Execute(ctx, "MATCH (:O {id:2})-[h:HAS]->(i:I) SET h.m = 'x' RETURN h.m AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"x"}}, updated.Rows)
	readback, err := exec.Execute(ctx, "MATCH (:O {id:2})-[h:HAS]->(i:I) RETURN h.n AS n, h.m AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5), "x"}}, readback.Rows)

	require.NoError(t, engine.Close())
	engine = nil
	engine, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dataDir}))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	exec = NewStorageExecutor(storage.NewNamespacedEngine(engine, "nornic"))
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	reopened, err := exec.Execute(ctx, "MATCH (o:O)-[h:HAS]->(i:I) RETURN h.n AS n, h.m AS m", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(5), "x"}}, reopened.Rows)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}
