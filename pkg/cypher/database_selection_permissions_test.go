package cypher

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDatabaseSelectionUsesStatementPermissions(t *testing.T) {
	for _, access := range []struct {
		name        string
		read, write bool
	}{
		{"write only", false, true},
		{"read only", true, false},
		{"no access", false, false},
	} {
		for _, route := range []struct{ name, prefix, database string }{
			{"direct", "", "nornic"},
			{"USE", "USE nornic ", "nornic"},
			{":USE", ":USE nornic\n", "nornic"},
			{":USE with params", ":USE nornic\n:params clear\n", "nornic"},
			{"USE other", "USE other ", "other"},
		} {
			t.Run(access.name+"/"+route.name, func(t *testing.T) {
				exec, stores := nestedUseRouterExecutor(t, "nornic")
				ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
					if database != route.database {
						return true
					}
					// The database is selectable; only its read and write vary.
					return permission == "access" || permission == "read" && access.read || permission == "write" && access.write
				})
				for _, statement := range []string{
					"CREATE (:Loaded)",
					"CREATE (a:Loaded)-[:NEXT]->(:Loaded) SET a.done = true",
					"UNWIND [1, 2] AS n CREATE (:Loaded {n: n})",
				} {
					result, err := exec.Execute(ctx, route.prefix+statement, nil)
					if access.write {
						require.NoError(t, err, statement)
					} else {
						var denied *PermissionDeniedError
						require.ErrorAs(t, err, &denied, statement)
						require.Equal(t, "write", denied.Permission, statement)
						require.Nil(t, result)
					}
				}
				for database, store := range stores {
					nodes, err := store.GetNodesByLabel("Loaded")
					require.NoError(t, err)
					want := 0
					if access.write && database == route.database {
						want = 5
					}
					require.Len(t, nodes, want, database)
				}
			})
		}
	}
}

func TestFabricDatabaseSelectionUsesFragmentPermissions(t *testing.T) {
	for _, access := range []struct {
		name        string
		read, write bool
	}{
		{"write only", false, true},
		{"read only", true, false},
		{"no access", false, false},
	} {
		t.Run(access.name, func(t *testing.T) {
			exec := newCompositeFixture(t)
			ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
				if database != "pother" {
					return true
				}
				// The database is selectable; only its read and write vary.
				return permission == "access" || permission == "read" && access.read || permission == "write" && access.write
			})
			for _, prefix := range []string{"USE pcomp.other ", "USE graph.byName('pcomp.other') ", ":USE pcomp.other\n"} {
				result, err := exec.Execute(ctx, prefix+"CREATE (:Loaded)", nil)
				if access.write {
					require.NoError(t, err, prefix)
				} else {
					var denied *PermissionDeniedError
					require.ErrorAs(t, err, &denied, prefix)
					require.Equal(t, "write", denied.Permission)
					require.Nil(t, result)
				}
			}
			result, err := exec.Execute(context.Background(), "USE pcomp.other MATCH (n:Loaded) RETURN count(n) AS c", nil)
			require.NoError(t, err)
			want := int64(0)
			if access.write {
				want = 3
			}
			require.Equal(t, [][]interface{}{{want}}, result.Rows)
		})
	}

	t.Run("read one database and write another", func(t *testing.T) {
		exec := newCompositeFixture(t)
		ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
			return database != "pother" || permission == "read"
		})
		result, err := exec.Execute(ctx, "CALL { USE pcomp.other MATCH (n:EI) RETURN n.v AS v } CALL { USE pcomp.third WITH v CREATE (n:Loaded {v: v}) RETURN n.v AS loaded } RETURN loaded", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"o"}}, result.Rows)
	})
}
