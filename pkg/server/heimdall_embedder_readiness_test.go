package server

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHeimdallQuerySeesEmbedderReadiness(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	_, err := router.Query(context.Background(), "nornic", "RETURN 1", nil)
	require.NoError(t, err)
	embedder := &countingEmbedder{dims: 2}
	server.db.GetCypherExecutor().SetEmbedder(embedder)
	const query = "CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding"
	rows, err := router.Query(context.Background(), "nornic", query, nil)
	require.NoError(t, err, "Heimdall must see the base executor's ready provider")
	require.Len(t, rows[0]["embedding"], 2)
	server.db.GetCypherExecutor().SetEmbedder(&countingEmbedder{dims: 3})
	for _, current := range []*heimdallDBRouter{router, newHeimdallDBRouter(server.db, server.dbManager, nil)} {
		rows, err := current.Query(context.Background(), "nornic", query, nil)
		require.NoError(t, err)
		require.Len(t, rows[0]["embedding"], 3, "cached and new executors must use the replacement")
	}
}

func TestHeimdallQueryConcurrentEmbedderPublication(t *testing.T) {
	server, _ := setupTestServer(t)
	router := newHeimdallDBRouter(server.db, server.dbManager, nil)
	base := server.db.GetCypherExecutor()
	first, second := &countingEmbedder{dims: 2}, &countingEmbedder{dims: 3}
	base.SetEmbedder(first)
	start := make(chan struct{})
	failures := make(chan error, 50)
	var workers sync.WaitGroup
	workers.Go(func() {
		<-start
		for range 50 {
			base.SetEmbedder(first)
			base.SetEmbedder(second)
		}
	})
	workers.Go(func() {
		<-start
		for range 50 {
			_, err := router.Query(context.Background(), "nornic", "CALL db.index.vector.embed('alpha') YIELD embedding RETURN embedding", nil)
			if err != nil {
				failures <- err
			}
		}
	})
	close(start)
	workers.Wait()
	close(failures)
	for err := range failures {
		require.NoError(t, err)
	}
}
