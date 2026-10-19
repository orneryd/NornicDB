package server

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestNamespaceDropRemoteFabricLogicalBegin(t *testing.T) {
	for _, opened := range []bool{false, true} {
		t.Run(fmt.Sprintf("participant-opened=%v", opened), func(t *testing.T) {
			s, authenticator := setupTestServer(t)
			remote := httptest.NewServer(s.buildRouter())
			defer remote.Close()
			ctx := cypher.WithAuthToken(context.Background(), "Bearer "+getAuthToken(t, authenticator, "admin"))
			admin, err := s.newExecutorForDatabase("nornic")
			require.NoError(t, err)
			run := func(exec *cypher.StorageExecutor, query string) *cypher.ExecuteResult {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				return result
			}
			run(admin, "CREATE DATABASE remote_lifetime")
			require.NoError(t, s.dbManager.CreateCompositeDatabase("remote_coordinator", []multidb.ConstituentRef{{Alias: "r", DatabaseName: "remote_lifetime", Type: "remote", URI: remote.URL, AccessMode: "read_write", AuthMode: "oidc_forwarding"}}))
			cmp, err := s.newExecutorForDatabase("remote_coordinator")
			require.NoError(t, err)
			run(cmp, "BEGIN")
			defer func() {
				if cmp.HasActiveTransaction() {
					_, _ = cmp.Execute(ctx, "ROLLBACK", nil)
				}
			}()
			if opened {
				run(cmp, "CALL { USE remote_coordinator.r RETURN 1 AS n } RETURN n")
			}
			run(admin, "DROP DATABASE remote_lifetime")
			run(admin, "CREATE DATABASE remote_lifetime")
			_, err = cmp.Execute(ctx, "CALL { USE remote_coordinator.r CREATE (n:StaleRemote) RETURN count(n) AS n } RETURN n", nil)
			if err == nil {
				_, err = cmp.Execute(ctx, "COMMIT", nil)
			}
			require.Error(t, err, "outer BEGIN must reject a remote namespace recreated before a lazy physical participant opens")
			fresh, err := s.newExecutorForDatabase("remote_coordinator")
			require.NoError(t, err)
			run(fresh, "BEGIN")
			run(fresh, "CALL { USE remote_coordinator.r CREATE (n:FreshRemote) RETURN count(n) AS n } RETURN n")
			run(fresh, "COMMIT")
			leaf, err := s.newExecutorForDatabase("remote_lifetime")
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(0)}}, run(leaf, "MATCH (n:StaleRemote) RETURN count(n) AS n").Rows)
			require.Equal(t, [][]interface{}{{int64(1)}}, run(leaf, "MATCH (n:FreshRemote) RETURN count(n) AS n").Rows)
		})
	}
}
