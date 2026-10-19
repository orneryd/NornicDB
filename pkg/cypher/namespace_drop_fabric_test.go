package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/stretchr/testify/require"
)

func TestSolR3FabricLazyLifetime(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, bindBefore := range []bool{false, true} {
			t.Run(fmt.Sprintf("buffered=%v/bindBefore=%v", buffered, bindBefore), func(t *testing.T) {
				f := openTypedCountScope(t, t.TempDir(), buffered)
				typedCountRun(t, f.admin, "CREATE DATABASE lifetime_a")
				require.NoError(t, f.manager.CreateCompositeDatabase("lifetime_cmp", []multidb.ConstituentRef{
					{Alias: "a", DatabaseName: "lifetime_a", Type: "local", AccessMode: "read_write"},
				}))
				cmp := f.executor(t, "lifetime_cmp")
				typedCountRun(t, cmp, "BEGIN")
				defer func() {
					if cmp.HasActiveTransaction() {
						_, _ = cmp.Execute(context.Background(), "ROLLBACK", nil)
					}
				}()
				if bindBefore {
					typedCountRun(t, cmp, "CALL { USE lifetime_cmp.a MATCH (n) RETURN count(n) AS c } RETURN c")
				}
				typedCountRun(t, f.admin, "DROP DATABASE lifetime_a")
				typedCountRun(t, f.admin, "CREATE DATABASE lifetime_a")
				_, writeErr := cmp.Execute(context.Background(), "CALL { USE lifetime_cmp.a CREATE (n:StaleFabric) RETURN count(n) AS c } RETURN c", nil)
				if writeErr != nil {
					t.Logf("old Fabric transaction rejected at write: %v", writeErr)
					return
				}
				_, commitErr := cmp.Execute(context.Background(), "COMMIT", nil)
				if commitErr == nil {
					result := typedCountRun(t, f.executor(t, "lifetime_a"), "MATCH (n:StaleFabric) RETURN count(n) AS c")
					t.Logf("pre-DROP outer BEGIN committed into recreated A: %v", result.Rows)
				}
				require.Error(t, commitErr, "outer BEGIN predates DROP even when its physical participant opens lazily")
			})
		}
	}
}

func TestNamespaceDropRetiresCompositeCoordinator(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%v", empty), func(t *testing.T) {
			f := openTypedCountScope(t, t.TempDir(), true)
			typedCountRun(t, f.admin, "CREATE DATABASE coordinator_leaf")
			var refs []multidb.ConstituentRef
			if !empty {
				refs = []multidb.ConstituentRef{{Alias: "a", DatabaseName: "coordinator_leaf", Type: "local", AccessMode: "read_write"}}
			}
			require.NoError(t, f.manager.CreateCompositeDatabase("coordinator", refs))
			cmp := f.executor(t, "coordinator")
			typedCountRun(t, cmp, "BEGIN")
			typedCountRun(t, f.admin, "DROP COMPOSITE DATABASE coordinator")
			require.NoError(t, f.manager.CreateCompositeDatabase("coordinator", refs))
			if !empty {
				_, err := cmp.Execute(context.Background(), "CALL { USE coordinator.a CREATE (n:OldCoordinator) RETURN count(n) AS c } RETURN c", nil)
				if err != nil {
					return
				}
			}
			_, err := cmp.Execute(context.Background(), "COMMIT", nil)
			require.Error(t, err, "the coordinator itself has a logical lifetime, even with no local constituents")
			typedCountWant(t, f.executor(t, "coordinator_leaf"), "MATCH (n:OldCoordinator) RETURN count(n) AS n", 0)
		})
	}
}
