package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestCompositeExplicitTx_SecondWriteShardRejected(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("shard_a"))
	require.NoError(t, mgr.CreateDatabase("shard_b"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
		{Alias: "b", DatabaseName: "shard_b", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()
	t.Logf("composite executor storage type: %T", exec.storage)

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	require.NotNil(t, exec.txContext)
	require.IsType(t, &fabric.FabricTransaction{}, exec.txContext.tx)

	firstWrite := "CALL { USE cmp.a CREATE (n:W {id: '1'}) RETURN count(n) AS c } RETURN c"
	secondWrite := "CALL { USE cmp.b CREATE (n:W {id: '2'}) RETURN count(n) AS c } RETURN c"
	require.Equal(t, "cmp.a", extractFirstUseGraph(firstWrite))
	require.Equal(t, "cmp.b", extractFirstUseGraph(secondWrite))

	_, err = exec.Execute(ctx, firstWrite, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, secondWrite, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Writing to more than one database per transaction is not allowed")

	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestCompositeExplicitTx_BeginCommitLifecycle(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("shard_c"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp2", []multidb.ConstituentRef{
		{Alias: "c", DatabaseName: "shard_c", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp2")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "CALL { USE cmp2.c CREATE (n:T {id: 'ok'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)
}

func TestGh683_CompositeTerminalContext(t *testing.T) {
	for _, canceledCommit := range []bool{false, true} {
		t.Run(fmt.Sprint(canceledCommit), func(t *testing.T) {
			manager, err := multidb.NewDatabaseManager(storage.NewMemoryEngine(), nil)
			require.NoError(t, err)
			defer manager.Close()
			require.NoError(t, manager.CreateDatabase("share_shard"))
			require.NoError(t, manager.CreateCompositeDatabase("cmp_sh", []multidb.ConstituentRef{{Alias: "sh", DatabaseName: "share_shard", Type: "local", AccessMode: "read_write"}}))
			store, err := manager.GetStorage("cmp_sh")
			require.NoError(t, err)
			exec := NewStorageExecutor(store)
			exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
			ctx := context.Background()
			_, err = exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			runCtx, cancelRun := context.WithCancel(ctx)
			_, err = exec.Execute(runCtx, "USE cmp_sh.sh CREATE (:TxShare {id:1})", nil)
			require.NoError(t, err)
			cancelRun()
			commitCtx, cancelCommit := context.WithCancel(ctx)
			defer cancelCommit()
			if canceledCommit {
				cancelCommit()
			}
			_, err = exec.Execute(commitCtx, "COMMIT", nil)
			if canceledCommit {
				require.ErrorIs(t, err, context.Canceled)
				_, err = exec.Execute(ctx, "ROLLBACK", nil)
				require.NoError(t, err)
			} else {
				require.NoError(t, err)
			}
			shard, err := manager.GetStorage("share_shard")
			require.NoError(t, err)
			nodes, err := shard.GetNodesByLabel("TxShare")
			require.NoError(t, err)
			if canceledCommit {
				require.Empty(t, nodes)
			} else {
				require.Len(t, nodes, 1)
			}
		})
	}
}

func TestCompositeExplicitTx_DocumentationExamples(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("nornic_tr"))
	require.NoError(t, mgr.CreateDatabase("nornic_txt"))
	require.NoError(t, mgr.CreateCompositeDatabase("nornic_cmp_tx", []multidb.ConstituentRef{
		{Alias: "tr", DatabaseName: "nornic_tr", Type: "local", AccessMode: "read_write"},
		{Alias: "txt", DatabaseName: "nornic_txt", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("nornic_cmp_tx")
	require.NoError(t, err)
	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	// Docs example: one write shard per explicit transaction succeeds.
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE nornic_cmp_tx.tr
  CREATE (t:Translation {id: "tr-tx-1", textKey: "ORDERS_WHERE"})
  RETURN count(t) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	// Docs example: second write shard in same explicit transaction is rejected.
	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE nornic_cmp_tx.tr
  CREATE (t:Translation {id: "tr-tx-2"})
  RETURN count(t) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, `
CALL {
  USE nornic_cmp_tx.txt
  CREATE (tt:TranslationText {translationId: "tr-tx-2", locale: "en-US", value: "Where are my orders?"})
  RETURN count(tt) AS c
}
RETURN c
`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Writing to more than one database per transaction is not allowed")
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestCompositeExplicitTx_RollbackRevertsWrites(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("rollback_shard"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp_rb", []multidb.ConstituentRef{
		{Alias: "rb", DatabaseName: "rollback_shard", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp_rb")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CALL {
  USE cmp_rb.rb
  CREATE (n:TxRollback {id: 'rb-1'})
  RETURN count(n) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	verify := NewStorageExecutor(cmpStore)
	verify.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	res, err := verify.Execute(ctx, "CALL { USE cmp_rb.rb MATCH (n:TxRollback {id: 'rb-1'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(0), res.Rows[0][0])
}

func TestCompositeExplicitTx_CommitPersistsWrites(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("commit_shard"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp_cm", []multidb.ConstituentRef{
		{Alias: "cm", DatabaseName: "commit_shard", Type: "local", AccessMode: "read_write"},
	}))

	cmpStore, err := mgr.GetStorage("cmp_cm")
	require.NoError(t, err)

	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, `
CALL {
  USE cmp_cm.cm
  CREATE (n:TxCommit {id: 'cm-1'})
  RETURN count(n) AS c
}
RETURN c
`, nil)
	require.NoError(t, err)

	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	verify := NewStorageExecutor(cmpStore)
	verify.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	res, err := verify.Execute(ctx, "CALL { USE cmp_cm.cm MATCH (n:TxCommit {id: 'cm-1'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, int64(1), res.Rows[0][0])
}

func TestCompositeExplicitTx_RemoteParticipantCommitLifecycle(t *testing.T) {
	var (
		openCount     int
		execCount     int
		commitCount   int
		rollbackCount int
	)
	var remote *httptest.Server
	remote = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote/tx"):
			openCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  remote.URL + "/db/tenant_remote/tx/1/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote/tx/1"):
			execCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"c"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote/tx/1/commit"):
			commitCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant_remote/tx/1"):
			rollbackCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer remote.Close()

	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{
				URI:       ref.URI,
				Database:  ref.DatabaseName,
				AuthToken: authToken,
				User:      ref.User,
				Password:  ref.Password,
			})
		},
	})
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateCompositeDatabase("cmp_remote", []multidb.ConstituentRef{
		{Alias: "r", DatabaseName: "tenant_remote", Type: "remote", AccessMode: "read_write", URI: remote.URL, AuthMode: "oidc_forwarding"},
	}))

	cmpStore, err := mgr.GetStorageWithAuth("cmp_remote", "Bearer tx-token")
	require.NoError(t, err)
	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL { USE cmp_remote.r CREATE (n:RemoteTx {id: 'r1'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "COMMIT", nil)
	require.NoError(t, err)

	require.Equal(t, 1, openCount)
	require.Equal(t, 1, execCount)
	require.Equal(t, 1, commitCount)
	require.Equal(t, 0, rollbackCount)
}

func TestCompositeExplicitTx_RemoteParticipantRollbackLifecycle(t *testing.T) {
	var (
		openCount     int
		execCount     int
		commitCount   int
		rollbackCount int
	)
	var remote *httptest.Server
	remote = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote_rb/tx"):
			openCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{},
				"errors":  []any{},
				"commit":  remote.URL + "/db/tenant_remote_rb/tx/2/commit",
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote_rb/tx/2"):
			execCount++
			_ = json.NewEncoder(w).Encode(map[string]any{
				"results": []any{
					map[string]any{
						"columns": []string{"c"},
						"data":    []any{map[string]any{"row": []any{1}}},
					},
				},
				"errors": []any{},
			})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/db/tenant_remote_rb/tx/2/commit"):
			commitCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		case r.Method == http.MethodDelete && strings.HasSuffix(r.URL.Path, "/db/tenant_remote_rb/tx/2"):
			rollbackCount++
			_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{}, "errors": []any{}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer remote.Close()

	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, &multidb.Config{
		DefaultDatabase: "nornic",
		SystemDatabase:  "system",
		RemoteEngineFactory: func(ref multidb.ConstituentRef, authToken string) (storage.Engine, error) {
			return storage.NewRemoteEngine(storage.RemoteEngineConfig{
				URI:       ref.URI,
				Database:  ref.DatabaseName,
				AuthToken: authToken,
				User:      ref.User,
				Password:  ref.Password,
			})
		},
	})
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateCompositeDatabase("cmp_remote_rb", []multidb.ConstituentRef{
		{Alias: "r", DatabaseName: "tenant_remote_rb", Type: "remote", AccessMode: "read_write", URI: remote.URL, AuthMode: "oidc_forwarding"},
	}))

	cmpStore, err := mgr.GetStorageWithAuth("cmp_remote_rb", "Bearer tx-token")
	require.NoError(t, err)
	exec := NewStorageExecutor(cmpStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	ctx := context.Background()

	_, err = exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CALL { USE cmp_remote_rb.r CREATE (n:RemoteTx {id: 'r2'}) RETURN count(n) AS c } RETURN c", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)

	require.Equal(t, 1, openCount)
	require.Equal(t, 1, execCount)
	require.Equal(t, 0, commitCount)
	require.Equal(t, 1, rollbackCount)
}

// TestCompositeExplicitTx_StatementsShareShardTransaction: the statements of
// an explicit transaction on a composite database run in one transaction per
// constituent. A statement sees what earlier ones wrote, and COMMIT keeps
// every statement's writes, as in Neo4j; the relationship counters (count by
// type and by label and type) stay equal to a scan.
func TestCompositeExplicitTx_StatementsShareShardTransaction(t *testing.T) {
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.CreateDatabase("share_shard"))
	require.NoError(t, mgr.CreateCompositeDatabase("cmp_sh", []multidb.ConstituentRef{
		{Alias: "sh", DatabaseName: "share_shard", Type: "local", AccessMode: "read_write"},
	}))
	cmpStore, err := mgr.GetStorage("cmp_sh")
	require.NoError(t, err)
	ctx := context.Background()
	count := func(exec *StorageExecutor) interface{} {
		res, err := exec.Execute(ctx, "USE cmp_sh.sh MATCH (n:TxShare) RETURN count(n) AS c", nil)
		require.NoError(t, err)
		require.Len(t, res.Rows, 1)
		return res.Rows[0][0]
	}
	// requireRelationshipCounts compares the counter-served relationship
	// counts with a scan that must read every relationship.
	requireRelationshipCounts := func(exec *StorageExecutor, want int64) {
		for _, query := range []string{
			"USE cmp_sh.sh MATCH ()-[r:TS]->() RETURN count(r) AS c",
			"USE cmp_sh.sh MATCH (:TxShare)-[r:TS]->() RETURN count(r) AS c",
			"USE cmp_sh.sh MATCH ()-[r:TS]->() WHERE r.missing IS NULL RETURN count(r) AS c",
		} {
			res, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			require.Equal(t, want, res.Rows[0][0], query)
		}
	}

	for _, end := range []string{"COMMIT", "ROLLBACK"} {
		exec := NewStorageExecutor(cmpStore)
		exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		_, err = exec.Execute(ctx, "USE cmp_sh.sh CREATE (:TxShare {id: 1})-[:TS]->(:TxEnd)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(1), count(exec), end)
		_, err = exec.Execute(ctx, "USE cmp_sh.sh CREATE (:TxShare {id: 2})-[:TS]->(:TxEnd)", nil)
		require.NoError(t, err)
		require.Equal(t, int64(2), count(exec), end)
		_, err = exec.Execute(ctx, end, nil)
		require.NoError(t, err)

		verify := NewStorageExecutor(cmpStore)
		verify.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
		if end == "COMMIT" {
			require.Equal(t, int64(2), count(verify))
			requireRelationshipCounts(verify, 2)
			_, err = verify.Execute(ctx, "USE cmp_sh.sh MATCH (n) DETACH DELETE n", nil)
			require.NoError(t, err)
		} else {
			require.Equal(t, int64(0), count(verify))
		}
		requireRelationshipCounts(verify, 0)
	}
}
