package storage_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/bolt"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/server"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const pr1655ProtocolUpdate = "MATCH (n:Doc {id: 'original'}) SET n.summary_input = 'changed'"

// The protocol fixtures share the actual head corruption and metadata-boundary
// handler used by Execute tests; they neither replace the error nor add hooks.
func openPR1655ProtocolFixture(t *testing.T, writeBehind ...bool) (*server.Server, *pr1655ExecuteFault) {
	t.Helper()
	fault := &pr1655ExecuteFault{}
	config := nornicdb.DefaultConfig()
	config.Auth.Enabled = false
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	config.Database.AsyncWritesEnabled = len(writeBehind) > 0 && writeBehind[0]
	config.Database.MVCCLifecycleEnabled = false
	config.Database.WALAutoCompactionEnabled = false
	config.Storage.Mode = "low"
	config.Logger = slog.New(&pr1655ExecuteHandler{fault: fault, next: slog.DiscardHandler})
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil && db != nil {
		_ = db.Close()
	}
	require.NoError(t, err)
	base := db.GetBaseStorageForManager()
	for {
		wrapper, ok := base.(storage.EngineUnwrapper)
		if !ok {
			break
		}
		base = wrapper.GetInnerEngine()
	}
	native, ok := base.(*storage.BadgerEngine)
	if !ok {
		_ = db.Close()
		t.Fatalf("real Badger fixture required, got %T", base)
	}
	fixtureowner.Track(t, native, storage.ErrStorageClosed)
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
	fault.engine = native
	t.Cleanup(func() { fault.restore(t) })
	store, err := srv.GetDatabaseManager().GetStorage("nornic")
	require.NoError(t, err)
	_, err = cypher.NewStorageExecutor(store).Execute(context.Background(), "CREATE (:Doc {id:'original', summary_input:'before'})", nil)
	require.NoError(t, err)
	require.NoError(t, native.FlushWriteBehind())
	fault.nodeID, err = pr1655SingleDocID(native)
	require.NoError(t, err)
	return srv, fault
}

func assertPR1655ProtocolBodyUnchanged(t *testing.T, fault *pr1655ExecuteFault) {
	t.Helper()
	require.True(t, fault.fired.Load(), "the real commit boundary was never reached")
	require.True(t, fault.installed.Load(), "the fixture failed to corrupt the actual head")
	require.NoError(t, fault.setupError())
	fault.armed.Store(false)
	fault.restore(t)
	require.NoError(t, fault.engine.FlushWriteBehind())
	node, err := fault.engine.GetStoredNode(fault.nodeID)
	require.NoError(t, err)
	require.Equal(t, "before", node.Properties["summary_input"], "failed commit published its pending body")
}

func TestPR1655BoltCommitReadFailure(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "RUN"
		if explicit {
			name = "COMMIT"
		}
		t.Run(name, func(t *testing.T) {
			srv, fault := openPR1655ProtocolFixture(t)
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)
			peer := bolt.NewWithDatabaseManager(&bolt.Config{ReadBufferSize: 8192, WriteBufferSize: 8192}, nil, srv.GetDatabaseManager())
			done := make(chan error, 1)
			go func() { done <- peer.Serve(listener) }()
			t.Cleanup(func() {
				require.NoError(t, peer.Close())
				select {
				case err := <-done:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Error("Bolt Serve did not terminate")
				}
			})
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			driver, err := neo4j.NewDriverWithContext("bolt://"+listener.Addr().String(), neo4j.NoAuth())
			require.NoError(t, err)
			defer driver.Close(context.Background())
			session := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
			defer session.Close(context.Background())
			metadata := neo4j.WithTxMetadata(map[string]any{"fixture_node": string(fault.nodeID)})
			if explicit {
				tx, beginErr := session.BeginTransaction(ctx, metadata)
				require.NoError(t, beginErr)
				defer tx.Close(context.Background())
				result, runErr := tx.Run(ctx, pr1655ProtocolUpdate, nil)
				require.NoError(t, runErr)
				_, runErr = result.Consume(ctx)
				require.NoError(t, runErr, "statement must stage before the commit fault")
				require.False(t, fault.fired.Load())
				fault.armed.Store(true)
				err = tx.Commit(ctx)
			} else {
				fault.armed.Store(true)
				result, runErr := session.Run(ctx, pr1655ProtocolUpdate, nil, metadata)
				err = runErr
				if err == nil {
					_, err = result.Consume(ctx)
				}
			}
			var failure *neo4j.Neo4jError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, pr1655CommitFailedStatus, failure.Code)
			require.NotNil(t, fault.decodeErr)
			require.Contains(t, failure.Msg, fault.decodeErr.Error())
			require.False(t, failure.IsRetriable(), "infrastructure corruption is not an automatic retry")
			assertPR1655ProtocolBodyUnchanged(t, fault)
		})
	}
}

func TestPR1655HTTPCommitReadFailure(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		name := "implicit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			srv, fault := openPR1655ProtocolFixture(t)
			client := &http.Client{Timeout: 15 * time.Second}
			t.Cleanup(client.CloseIdleConnections)
			post := func(url string, statements []map[string]any, metadata map[string]any) server.TransactionResponse {
				t.Helper()
				payload, err := json.Marshal(map[string]any{"statements": statements, "tx_metadata": metadata})
				require.NoError(t, err)
				response, err := client.Post(url, "application/json", bytes.NewReader(payload))
				require.NoError(t, err)
				defer response.Body.Close()
				require.Contains(t, []int{http.StatusOK, http.StatusCreated}, response.StatusCode)
				var result server.TransactionResponse
				require.NoError(t, json.NewDecoder(response.Body).Decode(&result))
				return result
			}
			var response server.TransactionResponse
			metadata := map[string]any{"fixture_node": string(fault.nodeID)}
			if explicit {
				opened := post("http://"+srv.Addr()+"/db/nornic/tx", []map[string]any{{"statement": pr1655ProtocolUpdate}}, metadata)
				require.Empty(t, opened.Errors, "statement must stage before the commit fault")
				require.NotEmpty(t, opened.Commit)
				require.False(t, fault.fired.Load())
				fault.armed.Store(true)
				response = post(opened.Commit, []map[string]any{}, nil)
			} else {
				// HTTP BEGIN carries tx_metadata; an implicit statement can set the
				// same metadata through the existing procedure in its own atomic frame.
				query := "CALL tx.setMetaData($metadata) WITH 1 AS ignored " + pr1655ProtocolUpdate
				fault.armed.Store(true)
				response = post("http://"+srv.Addr()+"/db/nornic/tx/commit", []map[string]any{{"statement": query, "parameters": map[string]any{"metadata": metadata}}}, nil)
			}
			require.Len(t, response.Errors, 1)
			require.Equal(t, pr1655CommitFailedStatus, response.Errors[0].Code)
			require.NotNil(t, fault.decodeErr)
			require.Contains(t, response.Errors[0].Message, fault.decodeErr.Error())
			assertPR1655ProtocolBodyUnchanged(t, fault)
		})
	}
}
