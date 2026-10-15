package main

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type readyTestEmbedder struct{ calls atomic.Int64 }

func (e *readyTestEmbedder) Embed(context.Context, string) ([]float32, error) {
	e.calls.Add(1)
	return []float32{1, 0}, nil
}
func (e *readyTestEmbedder) EmbedBatch(ctx context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i, text := range texts {
		vectors[i], _ = e.Embed(ctx, text)
	}
	return vectors, nil
}
func (*readyTestEmbedder) ChunkText(text string, _, _ int) ([]string, error) {
	return []string{text}, nil
}
func (*readyTestEmbedder) Dimensions() int { return 2 }
func (*readyTestEmbedder) Model() string   { return "ready-test" }
func (*readyTestEmbedder) Backend() string { return "cpu" }

func TestHybridSearchPicksUpEmbedderAfterStartup(t *testing.T) {
	for _, searchDuringStartup := range []bool{false, true} {
		name := "early_statement"
		if searchDuringStartup {
			name = "early_search"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			cfg := nornicdb.DefaultConfig()
			cfg.Memory.DecayEnabled = false
			cfg.Memory.AutoLinksEnabled = false
			cfg.Memory.EmbeddingEnabled = true
			cfg.Memory.EmbeddingDimensions = 2
			cfg.Database.AsyncWritesEnabled = false
			db, err := nornicdb.Open(t.TempDir(), cfg)
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.GetStorage().CreateNode(&storage.Node{
				ID: "doc", Labels: []string{"Document"},
				Properties:      map[string]interface{}{"content": "alpha"},
				ChunkEmbeddings: [][]float32{{1, 0}},
			})
			require.NoError(t, err)
			svc, err := db.GetOrCreateSearchService("nornic", db.GetStorage())
			require.NoError(t, err)
			require.NoError(t, svc.BuildIndexes(ctx))
			require.Nil(t, db.GetCypherExecutor().GetEmbedder())

			manager, err := multidb.NewDatabaseManager(db.GetBaseStorageForManager(), nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = db.Close()
				_ = manager.Close()
			})
			server := bolt.NewWithDatabaseManager(bolt.DefaultConfig(), NewDBQueryExecutor(db), manager)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			t.Cleanup(func() {
				require.NoError(t, server.Close())
				require.NoError(t, <-served)
			})
			newSession := func() neo4j.SessionWithContext {
				t.Helper()
				// Separate drivers guarantee a new connection, bypassing pool reuse.
				driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
				require.NoError(t, err)
				t.Cleanup(func() { _ = driver.Close(ctx) })
				session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
				t.Cleanup(func() { _ = session.Close(ctx) })
				return session
			}
			run := func(session neo4j.SessionWithContext, statement string) []*neo4j.Record {
				t.Helper()
				result, err := session.Run(ctx, statement, nil)
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				return records
			}
			session := newSession()
			run(session, "RETURN 1")
			queries := []string{
				"CALL db.rretrieve({query: 'alpha', limit: 5}) YIELD search_method, fallback_triggered, fallback_reason RETURN search_method, fallback_triggered, fallback_reason",
				"CALL db.retrieveGraph({query: 'alpha', limit: 5}) YIELD search_method, fallback_triggered, fallback_reason RETURN search_method, fallback_triggered, fallback_reason",
			}
			if searchDuringStartup {
				for _, query := range queries {
					records := run(session, query)
					require.NotEmpty(t, records)
					require.Equal(t, true, records[0].Values[1])
					require.Equal(t, "no_embedder", records[0].Values[2])
				}
			}

			// Use the same readiness hook as the server after its health check.
			embedder := &readyTestEmbedder{}
			db.SetEmbedder(embedder)
			for index, current := range []neo4j.SessionWithContext{session, newSession()} {
				for _, query := range queries {
					before := embedder.calls.Load()
					statement := query
					if index > 0 {
						// Use another statement key to prove this connection embeds;
						// a cached hybrid result may legitimately skip embedding.
						statement += ", 1 AS new_session"
					}
					records := run(current, statement)
					require.NotEmpty(t, records)
					require.Contains(t, records[0].Values[0], "hybrid")
					require.Equal(t, false, records[0].Values[1])
					require.Equal(t, "", records[0].Values[2])
					require.Greater(t, embedder.calls.Load(), before)
				}
				// An explicit transaction also uses the ready shared provider.
				tx, err := current.BeginTransaction(ctx)
				require.NoError(t, err)
				result, err := tx.Run(ctx, fmt.Sprintf("CALL db.index.vector.embed('transaction %d') YIELD embedding RETURN embedding", index), nil)
				require.NoError(t, err)
				records, err := result.Collect(ctx)
				require.NoError(t, err)
				require.Len(t, records, 1)
				require.NoError(t, tx.Rollback(ctx))
			}
		})
	}
}
