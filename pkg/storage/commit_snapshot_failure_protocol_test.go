package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These tests use the actual storage decoder and transaction owner. The
// fixture changes stored bytes; it never supplies a preconstructed error.
func snapshotFailureServer(t *testing.T) (*server.Server, string) {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Auth.Enabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.AsyncWritesEnabled = false
	config.Database.MVCCLifecycleEnabled = false
	config.Database.WALAutoCompactionEnabled = false
	config.Database.MVCCRetentionMaxVersions = 100
	db, err := nornicdb.Open(t.TempDir(), config)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	serverConfig := server.DefaultConfig()
	serverConfig.Port = 0
	serverConfig.Headless = true
	serverConfig.EmbeddingEnabled = false
	serverConfig.ProcessConfig = config
	srv, err := server.New(db, nil, serverConfig)
	require.NoError(t, err)
	require.NoError(t, srv.Start())
	t.Cleanup(func() { require.NoError(t, srv.Stop(context.Background())) })
	store, err := srv.GetDatabaseManager().GetStorage("nornic")
	require.NoError(t, err)
	_, err = cypher.NewStorageExecutor(store).Execute(context.Background(), "CREATE CONSTRAINT FOR (n:Doc) REQUIRE n.id IS UNIQUE", nil)
	require.NoError(t, err)
	_, err = store.CreateNode(&storage.Node{ID: "doc", Labels: []string{"Doc"}, Properties: map[string]any{"id": "original"}})
	require.NoError(t, err)
	base := db.GetBaseStorageForManager()
	for {
		wrapper, ok := base.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		base = wrapper.GetInnerEngine()
	}
	badgerEngine, ok := base.(*storage.BadgerEngine)
	require.True(t, ok, "fixture requires real Badger storage, got %T", base)
	cause := badgerEngine.CommitSnapshotFailureFixture(t, "nornic:doc")
	return srv, cause
}

func TestSnapshotCommitFailureBolt(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "RUN"
		if explicit {
			name = "COMMIT"
		}
		t.Run(name, func(t *testing.T) {
			srv, cause := snapshotFailureServer(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, srv.GetDatabaseManager())
			done := make(chan error, 1)
			go func() { done <- peer.Serve(listener) }()
			t.Cleanup(func() { require.NoError(t, peer.Close()); require.NoError(t, <-done) })
			ctx := context.Background()
			driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
			require.NoError(t, err)
			defer driver.Close(ctx)
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(ctx)
			if explicit {
				tx, beginErr := session.BeginTransaction(ctx)
				require.NoError(t, beginErr)
				defer tx.Close(ctx)
				result, runErr := tx.Run(ctx, snapshotFailureQuery, nil)
				require.NoError(t, runErr)
				_, runErr = result.Consume(ctx)
				require.NoError(t, runErr, "failure must be at COMMIT")
				err = tx.Commit(ctx)
			} else {
				result, runErr := session.Run(ctx, snapshotFailureQuery, nil)
				err = runErr
				if err == nil {
					_, err = result.Consume(ctx)
				}
			}
			var failure *neo4j.Neo4jError
			require.ErrorAs(t, err, &failure)
			require.Contains(t, failure.Msg, "failed to read transaction snapshot")
			require.Contains(t, failure.Msg, cause)
			require.Equal(t, snapshotFailureStatus, failure.Code)
			require.False(t, failure.IsRetriable())
			t.Logf("%s: %s: %s", name, failure.Code, failure.Msg)
		})
	}
}

func TestSnapshotCommitFailureHTTP(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			srv, cause := snapshotFailureServer(t)
			client := &http.Client{Timeout: 15 * time.Second}
			post := func(path string, queries ...string) server.TransactionResponse {
				t.Helper()
				statements := make([]map[string]any, 0, len(queries))
				for _, query := range queries {
					statements = append(statements, map[string]any{"statement": query})
				}
				payload, err := json.Marshal(map[string]any{"statements": statements})
				require.NoError(t, err)
				response, err := client.Post("http://"+srv.Addr()+path, "application/json", bytes.NewReader(payload))
				require.NoError(t, err)
				defer response.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
				var decoded server.TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&decoded))
				return decoded
			}
			var response server.TransactionResponse
			if explicit {
				opened := post("/db/nornic/tx", snapshotFailureQuery)
				require.Empty(t, opened.Errors, "statement must succeed before COMMIT")
				require.NotEmpty(t, opened.Commit)
				request, err := http.NewRequest(http.MethodPost, opened.Commit, bytes.NewBufferString("{\"statements\":[]}"))
				require.NoError(t, err)
				request.Header.Set("Content-Type", "application/json")
				reply, err := client.Do(request)
				require.NoError(t, err)
				defer reply.Body.Close()
				require.Equal(t, http.StatusOK, reply.StatusCode)
				require.NoError(t, json.NewDecoder(reply.Body).Decode(&response))
			} else {
				response = post("/db/nornic/tx/commit", snapshotFailureQuery)
			}
			require.Len(t, response.Errors, 1)
			failure := response.Errors[0]
			require.Contains(t, failure.Message, "failed to read transaction snapshot")
			require.Contains(t, failure.Message, cause)
			require.Equal(t, snapshotFailureStatus, failure.Code)
			require.False(t, strings.HasPrefix(failure.Code, "Neo.TransientError."))
			t.Logf("%s: %s: %s", name, failure.Code, failure.Message)
		})
	}
}
