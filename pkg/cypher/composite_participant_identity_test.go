package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestCompositePhysicalParticipantIdentity(t *testing.T) {
	for _, rootFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "alias_first", true: "root_first"}[rootFirst], func(t *testing.T) {
			exec := fabricCallRowsCompositeExecutor(t, newFabricCallRowsExecutor(t))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			t.Cleanup(func() {
				if exec.txContext != nil && exec.txContext.active {
					_, err := exec.Execute(ctx, "ROLLBACK", nil)
					require.NoError(t, err)
				}
			})
			alias := "USE comp.one CREATE (n:IdentityProbe {k:1}) RETURN n.k AS k"
			root := "USE comp CREATE (n:IdentityProbe {database_id:'one', k:2}) RETURN n.k AS k"
			first, second := alias, root
			if rootFirst {
				first, second = root, alias
			}
			_, err = exec.Execute(ctx, first, nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, second, nil)
			require.NoError(t, err)
			for _, query := range []string{"USE comp.one MATCH (n:IdentityProbe) RETURN count(n) AS c", "USE comp MATCH (n:IdentityProbe) RETURN count(n) AS c"} {
				result, err := exec.Execute(ctx, query, nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(2)}}, result.Rows)
			}
			_, err = exec.Execute(ctx, "USE comp CREATE (n:IdentityProbe {database_id:'two', k:3}) RETURN n.k", nil)
			require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			for _, db := range []string{"one", "two"} {
				result, err := exec.Execute(ctx, "USE comp."+db+" MATCH (n:IdentityProbe) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			}
		})
	}
}

func TestCompositeUnionExplicitBranchOwners(t *testing.T) {
	for _, second := range []string{"one", "two"} {
		t.Run(second, func(t *testing.T) {
			exec := fabricCallRowsCompositeExecutor(t, newFabricCallRowsExecutor(t))
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "USE comp.one CREATE (n:UnionOwnerProbe) RETURN 1 AS k UNION ALL USE comp."+second+" CREATE (m:UnionOwnerProbe) RETURN 2 AS k", nil)
			if second == "one" {
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}, {int64(2)}}, result.Rows)
			} else {
				require.ErrorIs(t, err, fabric.ErrSecondWriteShard)
			}
			_, err = exec.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, err)
			for _, db := range []string{"one", "two"} {
				result, err := exec.Execute(ctx, "USE comp."+db+" MATCH (n:UnionOwnerProbe) RETURN count(n) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(0)}}, result.Rows)
			}
		})
	}
}
