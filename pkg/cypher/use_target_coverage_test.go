package cypher

import (
	"context"
	"errors"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// accessTestManager is a database manager for AccessDatabases: composite
// cmp with constituents a → dba (a ConstituentRef) and B → dbb (the map
// form), the alias al → target, and the plain databases in plain. When
// constituentsErr is set, the composite's constituents can't be read.
type accessTestManager struct {
	useAuthDBManager
	constituentsErr error
}

func (m *accessTestManager) IsCompositeDatabase(name string) bool { return name == "cmp" }
func (m *accessTestManager) GetCompositeConstituents(name string) ([]interface{}, error) {
	if m.constituentsErr != nil {
		return nil, m.constituentsErr
	}
	return []interface{}{
		multidb.ConstituentRef{Alias: "a", DatabaseName: "dba", Type: "local"},
		map[string]interface{}{"alias": "B", "database_name": "dbb", "type": "local"},
		"not a constituent",
	}, nil
}
func (m *accessTestManager) ResolveDatabase(name string) (string, error) {
	switch name {
	case "al":
		return "target", nil
	case "plain", "target", "cmp", "dba", "dbb":
		return name, nil
	}
	return "", errors.New("database not found")
}

// TestAccessDatabases pins which databases a graph name needs (#738): a
// composite constituent needs the composite and the constituent's database,
// an alias needs itself and its target, anything else only itself.
func TestAccessDatabases(t *testing.T) {
	manager := &accessTestManager{}
	for name, want := range map[string][]string{
		"cmp.a":     {"cmp", "dba"},
		"cmp.b":     {"cmp", "dbb"},    // alias matched case-insensitively (map form)
		"cmp.zz":    {"cmp", "cmp.zz"}, // no such constituent: the full name
		"al":        {"al", "target"},
		"plain":     {"plain"},
		"cmp":       {"cmp"},
		"nosuch":    {"nosuch"},
		"nosuch.a":  {"nosuch.a"}, // not a composite: an ordinary name
		".leading":  {".leading"},
		"plain.dot": {"plain.dot"},
	} {
		got, err := AccessDatabases(manager, name)
		require.NoError(t, err, name)
		require.Equal(t, want, got, name)
	}

	got, err := AccessDatabases(nil, "cmp.a")
	require.NoError(t, err)
	require.Equal(t, []string{"cmp.a"}, got, "no database manager: the name alone")

	lookupErr := errors.New("constituents unavailable")
	got, err = AccessDatabases(&accessTestManager{constituentsErr: lookupErr}, "cmp.a")
	require.ErrorIs(t, err, lookupErr)
	require.Equal(t, []string{"cmp"}, got, "constituents unreadable: the composite, with the error")
}

// TestAuthorizeSelectedDatabase: every database AccessDatabases lists must
// be allowed; a failed constituent lookup is reported after the composite
// is checked.
func TestAuthorizeSelectedDatabase(t *testing.T) {
	recording := func(refused ...string) (*[]string, DatabasePermissionResolver) {
		var asked []string
		return &asked, func(database, permission string) bool {
			asked = append(asked, database+":"+permission)
			for _, name := range refused {
				if database == name {
					return false
				}
			}
			return true
		}
	}
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	exec.SetDatabaseManager(&accessTestManager{})

	// Without a resolver in the context nothing is checked.
	require.NoError(t, exec.authorizeSelectedDatabase(context.Background(), "cmp.a"))

	asked, resolver := recording()
	require.NoError(t, exec.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "cmp.a"))
	require.Equal(t, []string{"cmp:read", "dba:read"}, *asked)

	// Refusing the constituent's database refuses the constituent.
	asked, resolver = recording("dba")
	err := exec.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "cmp.a")
	var denied *PermissionDeniedError
	require.ErrorAs(t, err, &denied)
	require.Equal(t, "read", denied.Permission)
	requireStatusCode(t, err, "Neo.ClientError.Security.Forbidden")
	require.Equal(t, []string{"cmp:read", "dba:read"}, *asked)

	// An alias needs its target too.
	asked, resolver = recording("target")
	err = exec.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "al")
	require.ErrorAs(t, err, &denied)
	require.Equal(t, []string{"al:read", "target:read"}, *asked)

	// Constituents that can't be read: the composite is checked, then the
	// lookup's error is returned; a refused composite is refused first.
	lookupErr := errors.New("constituents unavailable")
	failing := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	failing.SetDatabaseManager(&accessTestManager{constituentsErr: lookupErr})
	asked, resolver = recording()
	err = failing.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "cmp.a")
	require.ErrorIs(t, err, lookupErr)
	require.Equal(t, []string{"cmp:read"}, *asked)
	asked, resolver = recording("cmp")
	err = failing.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "cmp.a")
	require.ErrorAs(t, err, &denied)
	require.NotErrorIs(t, err, lookupErr)
	require.Equal(t, []string{"cmp:read"}, *asked)

	// No database manager: the name alone.
	embedded := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	asked, resolver = recording()
	require.NoError(t, embedded.authorizeSelectedDatabase(WithDatabasePermissionResolver(context.Background(), "nornic", resolver), "cmp.a"))
	require.Equal(t, []string{"cmp.a:read"}, *asked)
}

// badEngineDBManager serves a value that is not a storage engine.
type badEngineDBManager struct{ standardDBManager }

func (m *badEngineDBManager) GetStorageForUse(string, string) (interface{}, error) {
	return struct{}{}, nil
}

// newUseTargetFixture is an executor on nornic (one :Home {v: 1} node) with
// the standard database other, the alias other_alias → other and the alias
// ghost → a database whose storage can't be opened.
func newUseTargetFixture(t *testing.T) *StorageExecutor {
	t.Helper()
	base := newTestMemoryEngine(t)
	home := wrappedDatabaseEngine{Engine: storage.NewNamespacedEngine(base, "nornic"), namespace: "nornic"}
	other := wrappedDatabaseEngine{Engine: storage.NewNamespacedEngine(base, "other"), namespace: "other"}
	exec := NewStorageExecutor(home)
	exec.SetDatabaseManager(&standardDBManager{
		engines: map[string]storage.Engine{"nornic": home, "other": other},
		aliases: map[string]string{"other_alias": "other", "ghost": "ghostdb"},
	})
	_, err := exec.Execute(context.Background(), "CREATE (:Home {v: 1})", nil)
	require.NoError(t, err)
	return exec
}

// TestUseTargetAuthorizationOnEveryRoute: a USE clause's database is
// authorized on the top-level route, in CALL subqueries and in a statement
// apoc.cypher.run executes (#738); an alias needs its target.
func TestUseTargetAuthorizationOnEveryRoute(t *testing.T) {
	exec := newUseTargetFixture(t)
	refuseOther := WithDatabasePermissionResolver(context.Background(), "nornic", func(database, permission string) bool {
		return database != "other"
	})
	for _, query := range []string{
		"USE other RETURN 1 AS x",
		"USE other_alias RETURN 1 AS x",
		"MATCH (n:Home) CALL { USE other RETURN 2 AS c } RETURN c",
		"CALL { USE other RETURN 2 AS c } RETURN c",
		"UNWIND [1] AS x CALL { USE other RETURN 2 AS c } RETURN c",
		"CALL apoc.cypher.run('USE other RETURN 1 AS x', {}) YIELD value RETURN value",
	} {
		_, err := exec.Execute(refuseOther, query, nil)
		var denied *PermissionDeniedError
		require.ErrorAs(t, err, &denied, query)
		require.Equal(t, "read", denied.Permission, query)
	}

	allowAll := WithDatabasePermissionResolver(context.Background(), "nornic", func(string, string) bool { return true })
	result, err := exec.Execute(allowAll, "CALL apoc.cypher.run('USE other RETURN 1 AS x', {}) YIELD value RETURN value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{map[string]interface{}{"x": int64(1)}}}, result.Rows)
	result, err = exec.Execute(allowAll, "USE other_alias RETURN 1 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

// TestUseTargetStorageFailures: a USE target whose storage can't be opened,
// or isn't a storage engine, fails the statement with the target named.
func TestUseTargetStorageFailures(t *testing.T) {
	exec := newUseTargetFixture(t)
	_, err := exec.Execute(context.Background(), "USE ghost RETURN 1 AS x", nil)
	require.EqualError(t, err, "USE ghostdb failed: database not found")

	base := newTestMemoryEngine(t)
	bad := NewStorageExecutor(wrappedDatabaseEngine{Engine: storage.NewNamespacedEngine(base, "nornic"), namespace: "nornic"})
	bad.SetDatabaseManager(&badEngineDBManager{standardDBManager{
		engines: map[string]storage.Engine{"other": storage.NewNamespacedEngine(base, "other")},
	}})
	_, err = bad.Execute(context.Background(), "USE other RETURN 1 AS x", nil)
	require.EqualError(t, err, "USE other failed: storage engine has unexpected type")
}

// TestSubqueryUseClauseErrors: a CALL subquery's USE clause is read with the
// one USE grammar on every subquery route, and a dynamic graph reference
// outside a composite database is Neo4j's SyntaxError (#738).
func TestSubqueryUseClauseErrors(t *testing.T) {
	exec := newUseTargetFixture(t)
	const mustConclude = "Query must conclude with a RETURN clause, a FINISH clause, an update clause, a unit subquery call, or a procedure call with no YIELD."
	const dynamic = "Dynamic graph lookup not allowed here. This feature is only available on composite databases.\nAttempted to access graph graph.byName(\"other\")"
	for query, message := range map[string]string{
		"MATCH (n:Home) CALL { USE other } RETURN n.v AS v":                         mustConclude,
		"CALL { USE other } RETURN 1 AS x":                                          mustConclude,
		"MATCH (n:Home) CALL { USE graph.byName('other') RETURN 1 AS c } RETURN c":  dynamic,
		"UNWIND [1] AS x CALL { USE graph.byName('other') RETURN 1 AS c } RETURN c": dynamic,
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.EqualError(t, err, message, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

// TestCompositeUseInTransaction: in an explicit transaction on a composite
// database, :USE of the composite itself stays on the transaction's
// database and runs.
func TestCompositeUseInTransaction(t *testing.T) {
	exec := newCompositeFixture(t)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	result, err := exec.Execute(ctx, ":USE pcomp\nRETURN 1 AS x", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
}

// TestCompositeGraphFunctionEdgeCases: graph.byName with an argument that
// can't be evaluated, and graph.propertiesByName errors, on a composite
// database.
func TestCompositeGraphFunctionEdgeCases(t *testing.T) {
	exec := newCompositeFixture(t)
	ctx := context.Background()

	// An undefined variable is reported as Neo4j reports it anywhere.
	_, err := exec.Execute(ctx, "USE graph.byName(nosuch) MATCH (n:EI) RETURN n.v AS v", nil)
	require.EqualError(t, err, "Variable `nosuch` not defined")
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	// An argument that is no expression is the invalid argument.
	_, err = exec.Execute(ctx, "USE graph.byName(1 +) MATCH (n:EI) RETURN n.v AS v", nil)
	require.Error(t, err)
	requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")

	// A name that isn't one of the composite's graphs, through the
	// expression evaluator (UNWIND, list and function arguments).
	for _, query := range []string{
		"UNWIND graph.propertiesByName('nope') AS g RETURN g",
		"UNWIND [graph.propertiesByName('nope')] AS g RETURN g",
		"UNWIND keys(graph.propertiesByName('nope')) AS g RETURN g",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.EqualError(t, err, "Graph not found: nope", query)
		requireStatusCode(t, err, "Neo.ClientError.Database.DatabaseNotFound")
	}

	// A name that isn't a string is a TypeError.
	for query, message := range map[string]string{
		"WITH 5 AS x RETURN graph.propertiesByName(x) AS g":     "graph.propertiesbyname() received an invalid Integer argument",
		"WITH ['a'] AS x RETURN graph.propertiesByName(x) AS g": "graph.propertiesbyname() received an invalid List argument",
	} {
		_, err := exec.Execute(ctx, query, nil)
		require.Error(t, err, query)
		require.Equal(t, "Neo.ClientError.Statement.TypeError: "+message, statusText(err), query)
	}

	// An argument whose evaluation fails fails the statement.
	_, err = exec.Execute(ctx, "WITH 1 AS x RETURN graph.propertiesByName(1/0) AS g", nil)
	require.EqualError(t, err, "/ by zero")
	requireStatusCode(t, err, "Neo.ClientError.Statement.ArithmeticError")
}

// TestCompositeGraphFunctionParameterCounts: on a composite database a
// graph function called with the wrong number of arguments is Neo4j's
// SyntaxError "Too many parameters for function '<name>'" / "Insufficient
// parameters for function '<name>'", the same through either evaluator
// (a literal statement, and rows from WITH / UNWIND).
func TestCompositeGraphFunctionParameterCounts(t *testing.T) {
	exec := newCompositeFixture(t)
	for query, message := range map[string]string{
		"RETURN graph.names(1) AS g":                   "Too many parameters for function 'graph.names'",
		"WITH 1 AS x RETURN graph.names(x) AS g":       "Too many parameters for function 'graph.names'",
		"UNWIND [1] AS x RETURN graph.names(x) AS g":   "Too many parameters for function 'graph.names'",
		"RETURN graph.propertiesByName() AS p":         "Insufficient parameters for function 'graph.propertiesByName'",
		"RETURN graph.propertiesByName('a', 'b') AS p": "Too many parameters for function 'graph.propertiesByName'",
	} {
		_, err := exec.Execute(context.Background(), query, nil)
		require.EqualError(t, err, message, query)
		requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
	}
}

// TestGraphAccess: a request's or session's graph is allowed when every
// database AccessDatabases lists is, and its data database is the last one;
// an unreadable composite refuses access; a nil check allows all.
func TestGraphAccess(t *testing.T) {
	manager := &accessTestManager{}
	allow := func(allowed ...string) func(string) bool {
		return func(database string) bool {
			for _, name := range allowed {
				if name == database {
					return true
				}
			}
			return false
		}
	}
	data, allowed := GraphAccess(manager, "cmp.a", allow("cmp", "dba"))
	require.True(t, allowed)
	require.Equal(t, "dba", data)
	data, allowed = GraphAccess(manager, "cmp.a", allow("dba"))
	require.False(t, allowed, "the composite isn't allowed")
	require.Equal(t, "dba", data)
	data, allowed = GraphAccess(manager, "al", nil)
	require.True(t, allowed)
	require.Equal(t, "target", data)
	data, allowed = GraphAccess(&accessTestManager{constituentsErr: errors.New("unavailable")}, "cmp.a", nil)
	require.False(t, allowed, "an unreadable composite refuses access")
	require.Equal(t, "cmp.a", data)
}

// TestUndefinedVariableError: an expression that starts with a name that
// isn't a function call names an undefined variable.
func TestUndefinedVariableError(t *testing.T) {
	for expression, want := range map[string]string{"nosuch": "nosuch", "x.y": "x", "g + 1": "g"} {
		err, undefined := undefinedVariableError(expression)
		require.True(t, undefined, expression)
		require.EqualError(t, err, "Variable `"+want+"` not defined", expression)
	}
	for _, expression := range []string{"f(1)", "toUpper ('a')", "1", "'s'", ""} {
		_, undefined := undefinedVariableError(expression)
		require.False(t, undefined, expression)
	}
}

// TestCypherTypeSystemNameWithArticle: Neo4j's "but it was …" type names.
func TestCypherTypeSystemNameWithArticle(t *testing.T) {
	for value, want := range map[interface{}]string{nil: "NULL", int64(1): "an INTEGER", 1.5: "a FLOAT", true: "a BOOLEAN", "s": "a STRING"} {
		require.Equal(t, want, cypherTypeSystemNameWithArticle(value), "%#v", value)
	}
	require.Equal(t, "a LIST", cypherTypeSystemNameWithArticle([]interface{}{1}))
	require.Equal(t, "a MAP", cypherTypeSystemNameWithArticle(map[string]interface{}{}))
}

// TestRowGraphFunctionArguments: in the row evaluator, a graph function's
// argument error is the statement's error, and an argument the row can't
// resolve leaves the call unresolved; a well-formed call returns the value.
func TestRowGraphFunctionArguments(t *testing.T) {
	exec := newCompositeFixture(t)
	value, resolved, err := exec.evaluateRowGraphFunction("graph.propertiesByName", "1 / 0", map[string]interface{}{})
	require.Error(t, err)
	require.False(t, resolved)
	require.Nil(t, value)
	value, resolved, err = exec.evaluateRowGraphFunction("graph.propertiesByName", "missing", map[string]interface{}{})
	require.NoError(t, err)
	require.False(t, resolved)
	require.Nil(t, value)
	graphs, composite := exec.CompositeGraphs()
	require.True(t, composite)
	value, resolved, err = exec.evaluateRowGraphFunction("graph.propertiesByName", "g", map[string]interface{}{"g": graphs[0]})
	require.NoError(t, err)
	require.True(t, resolved)
	require.Equal(t, map[string]interface{}{}, value)
}
