package bolt

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type canceledSearchEmbedder struct{}

func (canceledSearchEmbedder) Embed(context.Context, string) ([]float32, error) {
	return nil, context.Canceled
}

func (canceledSearchEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}

func TestBoltRetrieveCanceledEmbeddingReturnsFailure(t *testing.T) {
	base := storage.NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	store := storage.NewNamespacedEngine(base, "nornic")
	server, port := startBoltIntegrationServer(t, store)
	exec := server.executor.(*cypherQueryExecutor).executor
	exec.SetEmbedder(canceledSearchEmbedder{})
	svc := search.NewService(store)
	require.NoError(t, svc.BuildIndexes(context.Background()))
	exec.SetSearchService(svc)
	conn := openBoltTestConn(t, port)
	require.NoError(t, SendRun(t, conn, "CALL db.retrieve({query: 'needle'}) YIELD node RETURN node", nil, nil))
	_, message, err := AssertFailure(t, conn)
	require.NoError(t, err)
	require.Contains(t, message, "canceled")
}
