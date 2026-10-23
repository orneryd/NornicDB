package bolt

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"io"
	"net"
	"sync"
	"sync/atomic"
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

				_, port := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "remote"))
				runBoltQueryAndCollectRecords(t, openBoltTestConn(t, port), "CREATE CONSTRAINT rk_u FOR (u:U) REQUIRE u.k IS UNIQUE")

				mgr, err := multidb.NewDatabaseManager(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), &multidb.Config{
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

// stallingProxy forwards TCP to a server until stall is called; from then on
// it stops passing the server's replies on and keeps every connection open:
// a remote that accepts requests but never answers them.
type stallingProxy struct {
	listener net.Listener
	target   string
	stalled  atomic.Bool
	mu       sync.Mutex
	conns    []net.Conn
}

func startStallingProxy(t *testing.T, targetPort int) (*stallingProxy, int) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	p := &stallingProxy{listener: listener, target: fmt.Sprintf("127.0.0.1:%d", targetPort)}
	go p.serve()
	t.Cleanup(p.close)
	return p, listener.Addr().(*net.TCPAddr).Port
}

func (p *stallingProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, client, upstream)
		p.mu.Unlock()
		go func() {
			_, _ = io.Copy(upstream, client)
			_ = upstream.Close()
		}()
		go func() {
			buf := make([]byte, 32*1024)
			for {
				n, err := upstream.Read(buf)
				if n > 0 && !p.stalled.Load() {
					if _, err := client.Write(buf[:n]); err != nil {
						return
					}
				}
				if err != nil {
					_ = client.Close()
					return
				}
			}
		}()
	}
}

// close drops every connection, which unblocks a client still waiting.
func (p *stallingProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
}

// A composite explicit transaction that fails while its remote participant
// doesn't answer releases its local MERGE key at once and returns the
// statement's failure by the statement's deadline: the remote rollback is
// bounded, the participant records the deadline (the later ROLLBACK reports
// it), and the remote server ends its sub-transaction when the driver drops
// the connection.
func TestCompositeExplicitTx_FailureDoesNotWaitOnUnresponsiveRemote(t *testing.T) {
	for _, mode := range []string{"nornic", "antlr"} {
		t.Run(mode, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(mode)
			t.Cleanup(func() { config.SetParserType(previous) })
			ctx := context.Background()

			_, remotePort := startBoltIntegrationServerWithExplicitTx(t, storage.NewNamespacedEngine(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), "remote"))
			proxy, port := startStallingProxy(t, remotePort)
			mgr, err := multidb.NewDatabaseManager(fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)), &multidb.Config{
				DefaultDatabase: "nornic",
				SystemDatabase:  "system",
				RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
					return storage.NewRemoteEngine(storage.RemoteEngineConfig{URI: ref.URI, Database: ref.DatabaseName, AuthToken: authToken})
				},
			})
			require.NoError(t, err)
			t.Cleanup(func() { _ = mgr.Close() })
			require.NoError(t, mgr.CreateDatabase("st_local"))
			local, err := mgr.GetStorage("st_local")
			require.NoError(t, err)
			_, err = cypher.NewStorageExecutor(local).Execute(ctx, "CREATE CONSTRAINT st_u FOR (u:U) REQUIRE u.k IS UNIQUE", nil)
			require.NoError(t, err)
			require.NoError(t, mgr.CreateCompositeDatabase("cmp_st", []multidb.ConstituentRef{
				{Alias: "l", DatabaseName: "st_local", Type: "local", AccessMode: "read_write"},
				{Alias: "r", DatabaseName: "remote", Type: "remote", AccessMode: "read_write", URI: fmt.Sprintf("bolt://127.0.0.1:%d", port), AuthMode: "oidc_forwarding"},
			}))
			cmpStore, err := mgr.GetStorageWithAuth("cmp_st", "Bearer remote-token")
			require.NoError(t, err)
			exec := cypher.NewStorageExecutor(cmpStore)
			exec.SetDatabaseManager(compositeRoutingManager{manager: mgr})
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CALL { USE cmp_st.l MERGE (u:U {k: 1}) RETURN u.k AS k } RETURN k", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, "CALL { USE cmp_st.r MATCH (n) RETURN count(n) AS c } RETURN c", nil)
			require.NoError(t, err)

			merged := make(chan error, 1)
			go func() {
				_, err := cypher.NewStorageExecutor(local).Execute(ctx, "MERGE (u:U {k: 1}) RETURN u.k AS k", nil)
				merged <- err
			}()
			proxy.stalled.Store(true)
			statementCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()
			failed := make(chan error, 1)
			go func() {
				_, err := exec.Execute(statementCtx, "RETURN (", nil)
				failed <- err
			}()

			select {
			case err := <-merged:
				require.NoError(t, err)
			case <-time.After(time.Second):
				proxy.close()
				<-failed
				t.Fatal("the local MERGE key waits for the remote participant's rollback")
			}
			select {
			case err := <-failed:
				require.Error(t, err)
				require.NotContains(t, err.Error(), "deadline", "the statement reports its own failure")
			case <-time.After(10 * time.Second):
				proxy.close()
				<-failed
				t.Fatal("the failed statement waits for an unresponsive remote's rollback")
			}
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.ErrorContains(t, err, "deadline")
		})
	}
}
