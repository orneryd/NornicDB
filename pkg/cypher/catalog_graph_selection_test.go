package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestDynamicCatalogSelectionDeniesBeforeLookup(t *testing.T) {
	exec := newCompositeFixture(t)
	ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
		return database == "pcomp"
	})
	for _, graph := range []string{"pcomp.other", "pcomp.absent"} {
		t.Run(graph, func(t *testing.T) {
			result, err := exec.Execute(ctx, "USE graph.byName($graph) RETURN 1 AS n", map[string]interface{}{"graph": graph})
			var denied *PermissionDeniedError
			require.ErrorAs(t, err, &denied)
			require.Nil(t, result)
		})
	}
}

func TestCatalogVisibilityTracksLivePermissionChanges(t *testing.T) {
	exec := newCatalogVisibilityExecutor(t)
	allowed := false
	ctx := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		if permission != "access" {
			return true
		}
		return database == "nornic" || allowed
	})
	query := "SHOW DATABASES YIELD name RETURN name"
	for _, grant := range []bool{false, true, false} {
		allowed = grant
		result, err := exec.Execute(ctx, query, nil)
		require.NoError(t, err)
		if grant {
			require.Len(t, result.Rows, 3)
		} else {
			require.Equal(t, [][]interface{}{{"nornic"}}, result.Rows)
		}
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	_, err := exec.Execute(canceled, query, nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestCompositeCatalogFunctionsUseSelectionVisibility(t *testing.T) {
	exec := newCompositeFixture(t)
	ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool { return database == "pcomp" })
	result, err := exec.Execute(ctx, "RETURN graph.names() AS graphs", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{[]interface{}{}}}, result.Rows)
	for _, name := range []string{"pcomp.other", "pcomp.absent"} {
		_, err := exec.Execute(ctx, "RETURN graph.propertiesByName($name) AS properties", map[string]interface{}{"name": name})
		requireStatusCode(t, err, "Neo.ClientError.Database.DatabaseNotFound")
	}
}

func TestCatalogAliasesAndCompositeNamesUseVisibility(t *testing.T) {
	exec := newCompositeFixture(t)
	require.NoError(t, exec.dbManager.CreateAlias(context.Background(), "hidden_alias", "pthird"))
	require.NoError(t, exec.dbManager.CreateAlias(context.Background(), "visible_alias", "pother"))
	ctx := auth.WithRequestDatabaseAccessMode(context.Background(), catalogVisibilityAccessMode{"pother": true})
	result, err := exec.Execute(ctx, "SHOW ALIASES", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"visible_alias", "pother"}}, result.Rows)
	composite := newCompositeFixture(t)
	ctx = WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool { return permission != "access" || database == "nornic" })
	result, err = composite.Execute(ctx, "SHOW COMPOSITE DATABASES", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	for _, name := range []string{"pcomp", "absent"} {
		_, err = composite.Execute(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE "+name, nil)
		var denied *PermissionDeniedError
		require.ErrorAs(t, err, &denied)
	}
}

func TestCatalogConstituentRowsUseSelectionVisibility(t *testing.T) {
	exec := newCompositeFixture(t)
	ctx := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool { return permission != "access" || database == "pcomp" })
	result, err := exec.Execute(ctx, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE pcomp", nil)
	require.NoError(t, err)
	require.Empty(t, result.Rows)
	allowed, err := exec.Execute(context.Background(), "SHOW CONSTITUENTS FOR COMPOSITE DATABASE pcomp", nil)
	require.NoError(t, err)
	require.Len(t, allowed.Rows, 2)
}
