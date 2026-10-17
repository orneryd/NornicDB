package bolt

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// compositeRoutingManager is the part of the database manager composite
// routing uses; the rest of the interface is left nil.
type compositeRoutingManager struct {
	cypher.DatabaseManagerInterface
	manager *multidb.DatabaseManager
}

func (a compositeRoutingManager) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = routingDatabaseInfo{db}
	}
	return out
}
func (a compositeRoutingManager) Exists(name string) bool { return a.manager.Exists(name) }
func (a compositeRoutingManager) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}
func (a compositeRoutingManager) ResolveDatabase(name string) (string, error) {
	return a.manager.ResolveDatabase(name)
}
func (a compositeRoutingManager) ListAliases(name string) map[string]string {
	return a.manager.ListAliases(name)
}
func (a compositeRoutingManager) GetDatabaseLimits(name string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(name)
}
func (a compositeRoutingManager) GetStorageForUse(name, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}
func (a compositeRoutingManager) GetCompositeConstituents(name string) ([]interface{}, error) {
	constituents, err := a.manager.GetCompositeConstituents(name)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(constituents))
	for i, c := range constituents {
		out[i] = c
	}
	return out, nil
}

type routingDatabaseInfo struct{ info *multidb.DatabaseInfo }

func (d routingDatabaseInfo) Name() string         { return d.info.Name }
func (d routingDatabaseInfo) Type() string         { return d.info.Type }
func (d routingDatabaseInfo) Status() string       { return d.info.Status }
func (d routingDatabaseInfo) IsDefault() bool      { return d.info.IsDefault }
func (d routingDatabaseInfo) CreatedAt() time.Time { return d.info.CreatedAt }

// A composite explicit transaction whose remote participant, a NornicDB
// server reached over Bolt, MERGEd a unique key rolls that participant back
// when it fails, so the remote server releases the key at once: a competing
// MERGE there completes while the failed transaction is still open. Ending
// the failed transaction then ends nothing a second time (the competitor
// keeps its key), and the key's node exists once.
func TestCompositeExplicitTx_FailureRollsBackRemoteParticipant(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		for _, end := range []string{"ROLLBACK", "COMMIT"} {
			t.Run(mode+"/"+end, func(t *testing.T) {
				previous := config.GetParserType()
				config.SetParserType(mode)
				t.Cleanup(func() { config.SetParserType(previous) })

				_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(storage.NewMemoryEngine(), "remote"))
				runBoltQueryAndCollectRecords(t, openBoltTestConn(t, port), "CREATE CONSTRAINT rk_u FOR (u:U) REQUIRE u.k IS UNIQUE")

				mgr, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), &multidb.Config{
					DefaultDatabase: "nornic",
					SystemDatabase:  "system",
					RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
						return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken, User: ref.User, Password: ref.Password})
					},
				})
				require.NoError(t, err)
				t.Cleanup(func() { _ = mgr.Close() })
				require.NoError(t, mgr.CreateCompositeDatabase("cmp_rk", []multidb.ConstituentRef{
					{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read_write", URI: fmt.Sprintf("bolt://127.0.0.1:%d", port), AuthMode: "oidc_forwarding"},
				}))
				cmpStore, err := mgr.GetStorageWithAuth("cmp_rk", "Bearer remote-token")
				require.NoError(t, err)
				exec := cypher.NewStorageExecutor(cmpStore)
				exec.SetDatabaseManager(compositeRoutingManager{manager: mgr})
				ctx := context.Background()
				_, err = exec.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				t.Cleanup(func() { _, _ = exec.Execute(ctx, "ROLLBACK", nil) })
				_, err = exec.Execute(ctx, "CALL { USE cmp_rk.r MERGE (u:U {k: 1}) RETURN u.k AS k } RETURN k", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, "RETURN (", nil)
				require.Error(t, err)

				// The competitor, an explicit transaction on the remote server.
				competitor := openBoltTestConn(t, port)
				requireNoError(t, SendBegin(t, competitor, nil))
				requireNoError(t, ReadSuccess(t, competitor))
				select {
				case err := <-mergeRemoteKey(t, competitor):
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("a remote MERGE of the failed transaction's key waits for the failed transaction to end")
				}

				third := mergeRemoteKey(t, openBoltTestConn(t, port))
				_, err = exec.Execute(ctx, end, nil)
				if end == "COMMIT" {
					require.ErrorContains(t, err, "rolled back because a statement in it failed")
				} else {
					require.NoError(t, err)
				}
				select {
				case err := <-third:
					t.Fatalf("ending the failed transaction released the competitor's remote key (MERGE returned %v)", err)
				case <-time.After(300 * time.Millisecond):
				}
				requireNoError(t, SendCommit(t, competitor))
				requireNoError(t, ReadSuccess(t, competitor))
				select {
				case err := <-third:
					require.NoError(t, err)
				case <-time.After(10 * time.Second):
					t.Fatal("a remote MERGE of the key waits after its holder committed")
				}

				rows := runBoltQueryAndCollectRecords(t, openBoltTestConn(t, port), "MATCH (u:U {k: 1}) RETURN count(u) AS c")
				require.Equal(t, [][]any{{int64(1)}}, rows)
			})
		}
	}
}

// mergeRemoteKey MERGEs key 1 over conn in the background and reports the
// outcome; the caller waits on it with a bound.
func mergeRemoteKey(t *testing.T, conn net.Conn) <-chan error {
	done := make(chan error, 1)
	go func() {
		err := SendRun(t, conn, "MERGE (u:U {k: 1}) RETURN u.k AS k", nil, nil)
		if err == nil {
			err = ReadSuccess(t, conn)
		}
		if err == nil {
			err = SendPull(t, conn, nil)
		}
		for err == nil {
			var msgType byte
			msgType, _, err = ReadMessage(conn)
			if err == nil && msgType == MsgSuccess {
				break
			}
			if err == nil && msgType != MsgRecord {
				err = fmt.Errorf("unexpected Bolt message 0x%02X", msgType)
			}
		}
		done <- err
	}()
	return done
}
