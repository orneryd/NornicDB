package cypher

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestDatabaseVisibilityHasOneOwner pins that every SHOW form that names
// databases lists only those the principal may access, with or without a tail.
// A context without a principal's resolver lists all of them.
func TestDatabaseVisibilityHasOneOwner(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, manager.CreateAlias(t.Context(), "hidden_alias", "private"))
	require.NoError(t, manager.CreateAlias(t.Context(), "visible_alias", "nornic"))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "hidden_composite", []multidb.ConstituentRef{
		{Alias: "p", DatabaseName: "private", Type: "local", AccessMode: "read_write"},
	}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "visible_composite", []multidb.ConstituentRef{
		{Alias: "v", DatabaseName: "nornic", Type: "local", AccessMode: "read_write"},
		{Alias: "h", DatabaseName: "private", Type: "local", AccessMode: "read_write"},
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})

	principal := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		return permission != "access" || database == "nornic" || database == "visible_composite" || database == "visible_composite.v"
	})
	names := func(ctx context.Context, statement string) []interface{} {
		result, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
		rows := []interface{}{}
		for _, row := range result.Rows {
			rows = append(rows, row[0])
		}
		return rows
	}

	for _, statement := range []string{
		"SHOW DATABASES",
		"SHOW DATABASES YIELD name",
		"SHOW DATABASES YIELD name RETURN name",
		"SHOW DATABASES /* c */ YIELD name",
		"SHOW DATABASES YIELD *",
		"SHOW DATABASE nornic",
		"SHOW DATABASE nornic YIELD name",
		"SHOW DATABASE /* c */ `nornic`",
		"SHOW DATABASE",
		"SHOW DATABASE /* c */",
		"SHOW DATABASE YIELD name",
		"SHOW DATABASES nornic",
	} {
		require.Contains(t, names(principal, statement), "nornic", statement)
		require.NotContains(t, names(principal, statement), "private", statement)
		require.NotContains(t, names(principal, statement), "hidden_composite", statement)
	}
	for _, statement := range []string{"SHOW DATABASES", "SHOW DATABASE", "SHOW DATABASE system", "SHOW DATABASES YIELD name"} {
		// Neo4j: the system database "is always visible".
		require.Contains(t, names(principal, statement), "system", statement)
	}
	for _, statement := range []string{
		"SHOW DATABASES WHERE name = 'private'",
		"SHOW DATABASES YIELD name WHERE name = 'private'",
		"SHOW DATABASE private",
		"SHOW DATABASE private YIELD name",
		"SHOW DATABASE private WHERE name = 'private'",
		"SHOW DATABASE /* c */ private",
		"SHOW DATABASE `private`",
		"SHOW DATABASE missing",
		"SHOW DATABASES private",
		"SHOW DATABASES missing",
		"SHOW ALIASES FOR DATABASE private",
		"SHOW ALIASES FOR DATABASE missing",
	} {
		require.Empty(t, names(principal, statement), statement)
	}
	require.Equal(t, []interface{}{"visible_alias"}, names(principal, "SHOW ALIASES"))
	require.Equal(t, []interface{}{"visible_composite"}, names(principal, "SHOW COMPOSITE DATABASES"))
	require.Equal(t, []interface{}{"visible_composite"}, names(principal, "SHOW COMPOSITE DATABASES YIELD name"))
	require.Equal(t, []interface{}{"v"}, names(principal, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE visible_composite"))

	// Hidden is indistinguishable from absent, for a composite and for a
	// database that is not one.
	_, absent := exec.Execute(principal, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE missing", nil)
	require.Error(t, absent)
	for _, statement := range []string{
		"SHOW CONSTITUENTS FOR COMPOSITE DATABASE hidden_composite",
		"SHOW CONSTITUENTS FOR COMPOSITE DATABASE private",
	} {
		_, hidden := exec.Execute(principal, statement, nil)
		require.EqualError(t, hidden, absent.Error(), statement)
	}
	count, err := exec.Execute(principal, "SHOW DATABASES YIELD name RETURN count(name) AS c", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(3)}}, count.Rows)

	unrestricted := context.Background()
	require.Subset(t, names(unrestricted, "SHOW DATABASES YIELD name"), []interface{}{"nornic", "private"})
	require.Equal(t, []interface{}{"private"}, names(unrestricted, "SHOW DATABASE private"))
	require.Subset(t, names(unrestricted, "SHOW DATABASE"), []interface{}{"nornic", "private"})
	require.Subset(t, names(unrestricted, "SHOW COMPOSITE DATABASES"), []interface{}{"hidden_composite", "visible_composite"})
	require.Len(t, names(unrestricted, "SHOW CONSTITUENTS FOR COMPOSITE DATABASE hidden_composite"), 1)
	require.Equal(t, []interface{}{"hidden_alias"}, names(unrestricted, "SHOW ALIASES FOR DATABASE private"))
}

// TestGraphNamesListsOnlyAccessibleGraphs pins the Cypher Manual: "graph.names()
// only returns the name of a graph reference if the user has access to it". A
// hidden graph answers as an absent one. The unrestricted run first shows that
// the answer does not outlive one principal.
func TestGraphNamesListsOnlyAccessibleGraphs(t *testing.T) {
	exec := newCompositeFixture(t)
	principal := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
		return permission != "access" || database == "pcomp" || database == "pcomp.other"
	})
	for _, statement := range []string{
		"RETURN graph.names() AS g",
		"WITH 1 AS x RETURN graph.names() AS g",
		"WITH graph.names() AS g RETURN g",
	} {
		all, err := exec.Execute(context.Background(), statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other", "pcomp.third"}}}, all.Rows, statement)
		filtered, err := exec.Execute(principal, statement, nil)
		require.NoError(t, err, statement)
		require.Equal(t, [][]interface{}{{[]interface{}{"pcomp.other"}}}, filtered.Rows, statement)
	}
	unwound, err := exec.Execute(principal, "UNWIND graph.names() AS g RETURN g", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"pcomp.other"}}, unwound.Rows)
	_, err = exec.Execute(principal, "RETURN graph.propertiesByName('pcomp.other') AS p", nil)
	require.NoError(t, err)
	_, hidden := exec.Execute(principal, "RETURN graph.propertiesByName('pcomp.third') AS p", nil)
	_, absent := exec.Execute(principal, "RETURN graph.propertiesByName('pcomp.absent') AS p", nil)
	require.Error(t, absent)
	require.Error(t, hidden)
	require.Equal(t, strings.ReplaceAll(absent.Error(), "pcomp.absent", "NAME"), strings.ReplaceAll(hidden.Error(), "pcomp.third", "NAME"))
}

// TestHiddenGraphAnswersAsAbsent compares a hidden graph with an absent one on
// each route that resolves a name, as a principal who may access only pcomp and
// pcomp.other: static and dynamic USE, shell USE, a nested USE, an element id,
// SHOW LIMITS and a read-only constituent's mode.
func TestHiddenGraphAnswersAsAbsent(t *testing.T) {
	exec := newCompositeFixture(t)
	principal := WithDatabasePermissionResolver(context.Background(), "pcomp", func(database, permission string) bool {
		// As PrincipalAccess.Allows answers: every permission on a graph the
		// principal may access, none on any other name.
		return database == "pcomp" || database == "pcomp.other" || database == "pother"
	})
	normalize := func(err error) string {
		require.Error(t, err)
		return strings.ReplaceAll(strings.ReplaceAll(err.Error(), "pcomp.third", "NAME"), "pcomp.absent", "NAME")
	}
	for _, form := range []string{
		"USE %s RETURN 1 AS x",
		"USE %s MATCH (n) RETURN n",
		"USE %s CREATE (n)",
		"CALL { USE %s RETURN 1 AS x } RETURN x",
		"USE graph.byName('%s') RETURN 1 AS x",
		"USE graph.byName('%s') CREATE (n)",
		"USE pcomp CALL { USE graph.byName('%s') RETURN 1 AS x } RETURN x",
		"USE pcomp CALL { USE %s CREATE (n) } RETURN 1 AS x",
		":USE %s\nRETURN 1 AS x",
	} {
		_, hidden := exec.Execute(principal, fmt.Sprintf(form, "pcomp.third"), nil)
		_, absent := exec.Execute(principal, fmt.Sprintf(form, "pcomp.absent"), nil)
		require.Equal(t, normalize(absent), normalize(hidden), form)
		var denied *PermissionDeniedError
		require.ErrorAs(t, hidden, &denied, form)
	}
	_, hidden := exec.Execute(principal, "USE graph.byElementId('4:pthird:1') RETURN 1 AS x", nil)
	_, absent := exec.Execute(principal, "USE graph.byElementId('4:absent:1') RETURN 1 AS x", nil)
	var deniedHidden, deniedAbsent *PermissionDeniedError
	require.ErrorAs(t, hidden, &deniedHidden)
	require.ErrorAs(t, absent, &deniedAbsent)
	require.Equal(t, deniedAbsent.Permission, deniedHidden.Permission)

	// A principal with access still learns that a graph is absent.
	allowed := WithDatabasePermissionResolver(context.Background(), "pcomp", func(string, string) bool { return true })
	_, missing := exec.Execute(allowed, "USE graph.byName('pcomp.absent') RETURN 1 AS x", nil)
	require.Error(t, missing)
	var denied *PermissionDeniedError
	require.False(t, errors.As(missing, &denied))
}

// TestHiddenCompositeAndLimitsAnswerAsAbsent compares a hidden composite, its
// constituents, a hidden read-only constituent and SHOW LIMITS with absent names.
func TestHiddenCompositeAndLimitsAnswerAsAbsent(t *testing.T) {
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: "private"}))
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "hidden_composite", []multidb.ConstituentRef{
		{Alias: "ro", DatabaseName: "private", Type: "local", AccessMode: "read"},
	}))
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	principal := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		return database == "nornic"
	})
	for _, pair := range [][2]string{
		{"USE hidden_composite RETURN 1 AS x", "USE absent_composite RETURN 1 AS x"},
		{"USE hidden_composite.ro RETURN 1 AS x", "USE absent_composite.ro RETURN 1 AS x"},
		{"USE hidden_composite.ro CREATE (n)", "USE absent_composite.ro CREATE (n)"},
		{"USE hidden_composite.absent RETURN 1 AS x", "USE absent_composite.absent RETURN 1 AS x"},
		{"USE private RETURN 1 AS x", "USE absent_composite RETURN 1 AS x"},
		{"SHOW LIMITS FOR DATABASE private", "SHOW LIMITS FOR DATABASE absent_composite"},
		{"SHOW CONSTITUENTS FOR COMPOSITE DATABASE hidden_composite", "SHOW CONSTITUENTS FOR COMPOSITE DATABASE absent_composite"},
	} {
		_, hidden := exec.Execute(principal, pair[0], nil)
		_, absent := exec.Execute(principal, pair[1], nil)
		require.Error(t, hidden, pair[0])
		require.Error(t, absent, pair[1])
		normalize := func(message string) string {
			for _, name := range []string{"hidden_composite", "absent_composite", "private"} {
				message = strings.ReplaceAll(message, name, "NAME")
			}
			return message
		}
		require.Equal(t, normalize(absent.Error()), normalize(hidden.Error()), pair[0])
	}
	// With access, SHOW LIMITS reads the database.
	limits, err := exec.Execute(context.Background(), "SHOW LIMITS FOR DATABASE private", nil)
	require.NoError(t, err)
	require.NotNil(t, limits)
}

// TestCatalogReadersAreReviewed keeps the population of functions in this package
// that read the catalog closed: a new reader can reveal a hidden database, so it
// must be reviewed here, with the reason it cannot, or go through canSeeDatabase.
func TestCatalogReadersAreReviewed(t *testing.T) {
	const (
		owner    = "filters through canSeeDatabase"
		admin    = "reached only with the admin permission, checked before it runs"
		afterUse = "runs after authorizeSelectedDatabase or the access check for the same name"
		internal = "never reaches a response: routing, caching or identity of the selected database"
	)
	reviewed := map[string]string{
		"authorizeSelectedDatabase":      "is the authorizer",
		"resolveGraphSelection":          "is the authorizer",
		"callDbCdcQuery":                 internal,
		"databaseChangeTag":              internal,
		"callInfo":                       "names only the current and system databases",
		"systemDatabaseName":             internal,
		"prepareCreateCompositeDatabase": admin,
		"execute":                        admin,
		"executeShowCompositeDatabases":  owner,
		"executeShowConstituents":        owner,
		"executeDatabaseDrop":            admin,
		"alterDatabaseOption":            admin,
		"databaseOptionsValue":           owner,
		"NewDatabaseScopedExecutor":      afterUse,
		"buildFabricCatalog":             "planning and dispatch authorize a name before answering for it",
		"executeViaFabricWithTx":         afterUse,
		"fabricProcedureWritePlan":       internal,
		"fabricResultCacheKey":           internal,
		"useTargetRequiresFabric":        "picks a route; every route authorizes the name first",
		"executeAlterDatabase":           admin,
		"executeCreateAlias":             admin,
		"executeDatabaseCreate":          admin,
		"executeDropAlias":               admin,
		"executeShowAliases":             owner,
		"executeShowDatabases":           owner,
		"executeShowLimits":              owner,
		"showDatabaseCreatedAt":          owner,
		"showDatabaseRow":                owner,
		"CompositeGraphs":                owner,
		"resolveCompositeConstituent":    afterUse,
		"resolveCompositeStorage":        afterUse,
		"scopedExecutorForUse":           afterUse,
		"graphFunctionStorage":           afterUse,
		"systemStorage":                  internal,
		"bindStatementCacheScope":        internal,
		"cacheDependencyScopes":          internal,
		"recordStatementReference":       internal,
		"UseDatabaseStatementCaches":     internal,
		"DatabaseStatementCaches":        internal,
	}
	catalogReads := map[string]bool{
		"ListDatabases": true, "ListDatabaseNames": true, "ListAliases": true, "ListCompositeDatabases": true,
		"GetCompositeConstituents": true, "GetDatabaseLimits": true, "Exists": true, "ExistsOrIsConstituent": true,
		"ResolveDatabase": true, "IsCompositeDatabase": true, "DatabaseOptions": true, "DatabaseIdentity": true,
		"GetDatabase": true, "GetStorage": true, "GetStorageForUse": true, "GetStorageWithAuth": true,
		"RemoteConstituentForDisplay": true, "GetStorageSize": true, "StatementCacheScope": true, "DatabaseStatementCaches": true,
		"SystemDatabaseName": true, "ServerIdentity": true,
	}
	files := token.NewFileSet()
	packages, err := parser.ParseDir(files, ".", func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
	require.NoError(t, err)
	for _, pkg := range packages {
		for _, file := range pkg.Files {
			for _, declaration := range file.Decls {
				function, ok := declaration.(*ast.FuncDecl)
				if !ok || function.Body == nil {
					continue
				}
				ast.Inspect(function.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if !ok {
						return true
					}
					selector, ok := call.Fun.(*ast.SelectorExpr)
					if !ok || !catalogReads[selector.Sel.Name] {
						return true
					}
					switch receiver := selector.X.(type) {
					case *ast.Ident:
						if receiver.Name != "manager" && receiver.Name != "typed" && receiver.Name != "legacy" && receiver.Name != "provider" {
							return true
						}
					case *ast.SelectorExpr:
						if receiver.Sel.Name != "dbManager" {
							return true
						}
					default:
						return true
					}
					if reviewed[function.Name.Name] == "" {
						t.Errorf("%s: %s reads the catalog (%s) and is not reviewed", files.Position(call.Pos()), function.Name.Name, selector.Sel.Name)
					}
					return true
				})
			}
		}
	}
}
