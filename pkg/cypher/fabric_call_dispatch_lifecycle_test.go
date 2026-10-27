package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func fabricDispatchRows(t *testing.T, rows [][]interface{}, want string) {
	t.Helper()
	encoded, err := json.Marshal(append(make([][]interface{}, 0, len(rows)), rows...))
	require.NoError(t, err)
	require.JSONEq(t, want, string(encoded))
}

func fabricDispatchRemoteExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI")
	if uri == "" {
		t.Skip("requires --ref neo-ref")
	}
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	mgr := exec.dbManager.(*testDatabaseManagerAdapter).manager
	require.NoError(t, mgr.AddConstituent(t.Context(), "comp", multidb.ConstituentRef{
		Alias: "remote", DatabaseName: "neo4j", Type: "remote", AccessMode: "read_write", URI: uri,
	}))
	return exec
}

// Participant completion must outlive both the APPLY batch and the statement
// which first enlisted it. Canceling the statement after it returns must not
// cancel a later COMMIT/ROLLBACK of the transaction opened with a live context.
func TestFabricRemoteApplyCompletion(t *testing.T) {
	exec := fabricDispatchRemoteExecutor(t)
	for _, keys := range []int{1, 513, 1025} {
		for _, shape := range []struct{ name, body, want string }{
			{"collect", "RETURN collect(n.key) AS ks } RETURN count(*) AS rows, sum(size(ks)) AS matches", fmt.Sprintf("[[%d,0]]", keys)},
			{"count", "RETURN count(n) AS c } RETURN count(*) AS rows, sum(c) AS matches", fmt.Sprintf("[[%d,0]]", keys)},
			{"materialized", "RETURN n.key AS key ORDER BY key } RETURN count(*) AS rows", "[[0]]"},
			{"streamed", "RETURN n.key AS key } RETURN count(*) AS rows", "[[0]]"},
		} {
			for _, mode := range []string{"autocommit", "COMMIT", "ROLLBACK"} {
				t.Run(fmt.Sprintf("%s/%d/%s", shape.name, keys, mode), func(t *testing.T) {
					ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer cancel()
					runExec := exec
					if mode != "autocommit" {
						runExec = fabricCallRowsCompositeExecutor(t, exec)
						_, err := runExec.Execute(ctx, "BEGIN", nil)
						require.NoError(t, err)
						defer func() { _, _ = runExec.Execute(context.Background(), "ROLLBACK", nil) }()
					}
					query := fmt.Sprintf("USE comp CALL { USE comp.one UNWIND range(1,%d) AS k RETURN k } CALL { WITH k USE comp.remote MATCH (n:FabricDispatchMissing) WHERE n.key = k ", keys) + shape.body
					statementCtx, cancelStatement := context.WithCancel(ctx)
					result, err := runExec.Execute(statementCtx, query, nil)
					cancelStatement()
					require.NoError(t, err, "statement")
					fabricDispatchRows(t, result.Rows, shape.want)
					if mode != "autocommit" {
						_, err := runExec.Execute(ctx, mode, nil)
						require.NoError(t, err, "participant completion")
					}
				})
			}
		}
	}
}

func TestFabricUnionApplyTrace(t *testing.T) {
	exec := newFabricCallRowsExecutor(t)
	exec.cache = nil
	branch := "CALL { USE comp.one UNWIND range(1,513) AS k RETURN k } CALL { WITH k USE comp.two MATCH (n:FabricDispatchMissing) WHERE n.key = k RETURN n.key AS key } RETURN key"
	for _, shape := range []struct{ name, query, want string }{
		{"nested", "USE comp CALL { " + branch + " UNION ALL " + branch + " } RETURN count(*) AS c", "[[0]]"},
		{"top_level", "USE comp " + branch + " UNION ALL USE comp " + branch, "[]"},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", shape.name, explicit), func(t *testing.T) {
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()
				runExec := exec
				if explicit {
					runExec = fabricCallRowsCompositeExecutor(t, exec)
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					defer func() { _, _ = runExec.Execute(context.Background(), "ROLLBACK", nil) }()
				}
				result, err := runExec.Execute(ctx, shape.query, nil)
				require.NoError(t, err)
				fabricDispatchRows(t, result.Rows, shape.want)
				require.True(t, runExec.LastHotPathTrace().FabricBatchedApplyRows)
			})
		}
	}
}

// Participant completion belongs to the protocol's transaction owner. An ended
// statement must permit later COMMIT/ROLLBACK; an expired owner must reject it.
// Successful COMMIT persists the remote write and ROLLBACK removes it.
func TestFabricRemoteCompletionContextLifetime(t *testing.T) {
	exec := fabricDispatchRemoteExecutor(t)
	for _, scope := range []struct {
		name        string
		requestOnly bool
		actions     []string
		ends        []string
	}{
		{name: "ended_request", requestOnly: true, actions: []string{"COMMIT", "ROLLBACK"}, ends: []string{"cancel", "deadline"}},
		{name: "ended_owner", actions: []string{"COMMIT"}, ends: []string{"deadline"}},
	} {
		for _, action := range scope.actions {
			for _, end := range scope.ends {
				t.Run(scope.name+"/"+action+"/"+end, func(t *testing.T) {
					id := fmt.Sprintf("%s-%s-%s-%d", scope.name, action, end, time.Now().UnixNano())
					params := map[string]interface{}{"id": id}
					runExec := fabricCallRowsCompositeExecutor(t, exec)
					ctx, cancel := context.WithCancel(context.Background())
					if end == "deadline" {
						timeout := 5 * time.Second
						if scope.requestOnly {
							timeout = 2 * time.Second
						}
						ctx, cancel = context.WithTimeout(context.Background(), timeout)
					}
					defer cancel()
					if scope.requestOnly {
						owner, cancelOwner := context.WithCancel(context.Background())
						defer cancelOwner()
						ctx = fabric.WithTransactionLifetime(ctx, owner)
					}
					_, err := runExec.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					query := "USE comp.remote RETURN 1 AS k"
					if scope.requestOnly {
						query = "USE comp.remote CREATE (:FabricCompletion {id: $id})"
					}
					_, err = runExec.Execute(ctx, query, params)
					require.NoError(t, err)
					if end == "cancel" {
						cancel()
					}
					<-ctx.Done()
					_, err = runExec.Execute(context.Background(), action, nil)
					if !scope.requestOnly {
						require.Error(t, err)
						require.Contains(t, strings.ToLower(err.Error()), "deadline")
						return
					}
					require.NoError(t, err, "participant completion")
					result, err := exec.Execute(context.Background(), "USE comp.remote MATCH (n:FabricCompletion {id: $id}) RETURN count(n) AS c", params)
					require.NoError(t, err)
					want := "[[1]]"
					if action == "ROLLBACK" {
						want = "[[0]]"
					}
					fabricDispatchRows(t, result.Rows, want)
					_, err = exec.Execute(context.Background(), "USE comp.remote MATCH (n:FabricCompletion {id: $id}) DETACH DELETE n", params)
					require.NoError(t, err)
				})
			}
		}
	}
}

// The transaction owner's cancellation must still stop remote completion, even
// when COMMIT itself is submitted with a fresh context.
func TestFabricRemoteCompletionTransactionCancellation(t *testing.T) {
	exec := fabricDispatchRemoteExecutor(t)
	for _, action := range []string{"COMMIT", "ROLLBACK"} {
		t.Run(action, func(t *testing.T) {
			runExec := fabricCallRowsCompositeExecutor(t, exec)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			_, err := runExec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = runExec.Execute(ctx, "USE comp.remote RETURN 1 AS k", nil)
			require.NoError(t, err)
			cancel()
			_, err = runExec.Execute(context.Background(), action, nil)
			require.Error(t, err)
			require.True(t, strings.Contains(strings.ToLower(err.Error()), "cancel"), "%v", err)
		})
	}
}
