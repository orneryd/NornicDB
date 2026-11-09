package cypher

// NOR-387: a restricted principal's catalog commands report only the databases
// it may see, so an existing but invisible database is indistinguishable from
// an absent one. These are regression tests for the public
// StorageExecutor.Execute path with the request's per-database access mode
// (auth.WithRequestDatabaseAccessMode), the policy Bolt and HTTP attach.
//
// The catalog holds nornic, system and hidden; the restricted principal sees
// nornic only. A principal granted all three shows that hidden exists and is
// listed when it is visible, so the restricted assertions prove visibility, not
// the database's absence, is what removes it.

import (
	"context"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// catalogVisibilityAccessMode is the request DatabaseAccessMode the protocol
// layer attaches for a principal: the databases it may see and access.
// Databases outside the set are neither visible nor accessible.
type catalogVisibilityAccessMode map[string]bool

func (m catalogVisibilityAccessMode) CanSeeDatabase(database string) bool { return m[database] }

func (m catalogVisibilityAccessMode) CanAccessDatabase(database string) bool { return m[database] }

var (
	catalogVisibilityNornicOnly = catalogVisibilityAccessMode{"nornic": true}
	catalogVisibilityEverywhere = catalogVisibilityAccessMode{"nornic": true, "system": true, "hidden": true}
)

// newCatalogVisibilityExecutor returns an executor on the nornic graph whose
// database manager holds nornic, system and hidden.
func newCatalogVisibilityExecutor(t *testing.T) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	manager := newMockDatabaseManager(t)
	for _, name := range []string{"nornic", "system", "hidden"} {
		require.NoError(t, manager.CreateDatabase(context.Background(), multidb.DatabaseCreateRequest{Name: name}))
	}
	exec.SetDatabaseManager(manager)
	return exec
}

// catalogVisibilityTextValues returns the sorted string values of column in
// result, so assertions do not depend on the catalog's iteration order.
func catalogVisibilityTextValues(t *testing.T, result *ExecuteResult, column string) []string {
	t.Helper()
	require.NotNil(t, result)
	index := -1
	for i, name := range result.Columns {
		if name == column {
			index = i
			break
		}
	}
	require.NotEqual(t, -1, index, "column %q is missing from %v", column, result.Columns)
	values := make([]string, 0, len(result.Rows))
	for _, row := range result.Rows {
		require.Greater(t, len(row), index, "row %v has no %q value", row, column)
		value, ok := row[index].(string)
		require.True(t, ok, "row %v has a non-string %q value", row, column)
		values = append(values, value)
	}
	sort.Strings(values)
	return values
}

// requireIndistinguishableCatalogDenials requires the refusal of an existing but
// invisible database and the refusal of an absent database to be the same
// refusal: same status code and no result rows either way.
func requireIndistinguishableCatalogDenials(t *testing.T, invisibleErr, absentErr error, invisibleResult, absentResult *ExecuteResult) {
	t.Helper()
	require.Error(t, invisibleErr, "an existing but invisible database must be refused")
	require.Error(t, absentErr, "an absent database must be refused")
	require.Equal(t, statusText(absentErr), statusText(invisibleErr),
		"an invisible database and an absent one must be refused identically")
	require.Nil(t, invisibleResult, "a refused statement returns no rows for the invisible database")
	require.Nil(t, absentResult, "a refused statement returns no rows for the absent database")
}

// TestCatalogVisibilityShowDatabases covers SHOW DATABASES and its YIELD
// projection, alias and aggregation tails: every form lists only the databases
// the principal may see, before projection or aggregation.
func TestCatalogVisibilityShowDatabases(t *testing.T) {
	exec := newCatalogVisibilityExecutor(t)
	restricted := auth.WithRequestDatabaseAccessMode(context.Background(), catalogVisibilityNornicOnly)
	visible := auth.WithRequestDatabaseAccessMode(context.Background(), catalogVisibilityEverywhere)

	t.Run("plain SHOW lists only visible databases", func(t *testing.T) {
		restrictedResult, err := exec.Execute(restricted, "SHOW DATABASES", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"nornic"}, catalogVisibilityTextValues(t, restrictedResult, "name"))

		visibleResult, err := exec.Execute(visible, "SHOW DATABASES", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"hidden", "nornic", "system"}, catalogVisibilityTextValues(t, visibleResult, "name"))
	})

	t.Run("YIELD name projects only visible databases", func(t *testing.T) {
		restrictedResult, err := exec.Execute(restricted, "SHOW DATABASES YIELD name", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name"}, restrictedResult.Columns)
		require.Equal(t, [][]interface{}{{"nornic"}}, restrictedResult.Rows)

		visibleResult, err := exec.Execute(visible, "SHOW DATABASES YIELD name", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"hidden", "nornic", "system"}, catalogVisibilityTextValues(t, visibleResult, "name"))
	})

	t.Run("YIELD name alias renames the visible column", func(t *testing.T) {
		restrictedResult, err := exec.Execute(restricted, "SHOW DATABASES YIELD name AS database", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"database"}, restrictedResult.Columns)
		require.Equal(t, [][]interface{}{{"nornic"}}, restrictedResult.Rows)

		visibleResult, err := exec.Execute(visible, "SHOW DATABASES YIELD name AS database", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"database"}, visibleResult.Columns)
		require.Equal(t, []string{"hidden", "nornic", "system"}, catalogVisibilityTextValues(t, visibleResult, "database"))
	})

	t.Run("YIELD projection carries no invisible database metadata", func(t *testing.T) {
		restrictedResult, err := exec.Execute(restricted, "SHOW DATABASES YIELD name, type", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"name", "type"}, restrictedResult.Columns)
		require.Equal(t, [][]interface{}{{"nornic", "standard"}}, restrictedResult.Rows)

		visibleResult, err := exec.Execute(visible, "SHOW DATABASES YIELD name, type", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"hidden", "nornic", "system"}, catalogVisibilityTextValues(t, visibleResult, "name"))
		require.Equal(t, []string{"standard", "standard", "standard"}, catalogVisibilityTextValues(t, visibleResult, "type"))
	})

	t.Run("count aggregation counts only visible databases", func(t *testing.T) {
		restrictedResult, err := exec.Execute(restricted, "SHOW DATABASES YIELD name RETURN count(*) AS total", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"total"}, restrictedResult.Columns)
		require.Equal(t, [][]interface{}{{int64(1)}}, restrictedResult.Rows)

		visibleResult, err := exec.Execute(visible, "SHOW DATABASES YIELD name RETURN count(*) AS total", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(3)}}, visibleResult.Rows)
	})
}

// TestCatalogVisibilityShowLimits covers SHOW LIMITS FOR DATABASE: a database
// the principal cannot see is refused exactly like a database that does not
// exist, and a visible database reports its limits.
func TestCatalogVisibilityShowLimits(t *testing.T) {
	exec := newCatalogVisibilityExecutor(t)
	restricted := auth.WithRequestDatabaseAccessMode(context.Background(), catalogVisibilityNornicOnly)
	visible := auth.WithRequestDatabaseAccessMode(context.Background(), catalogVisibilityEverywhere)

	t.Run("invisible and absent databases are refused identically", func(t *testing.T) {
		invisibleResult, invisibleErr := exec.Execute(restricted, "SHOW LIMITS FOR DATABASE hidden", nil)
		absentResult, absentErr := exec.Execute(restricted, "SHOW LIMITS FOR DATABASE absent", nil)
		requireIndistinguishableCatalogDenials(t, invisibleErr, absentErr, invisibleResult, absentResult)
	})

	t.Run("visible database reports its limits", func(t *testing.T) {
		result, err := exec.Execute(restricted, "SHOW LIMITS FOR DATABASE nornic", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"database", "limit", "value", "description"}, result.Columns)
		require.Equal(t, []string{"nornic"}, catalogVisibilityTextValues(t, result, "database"))
	})

	t.Run("visible database reports the limits hidden from others", func(t *testing.T) {
		result, err := exec.Execute(visible, "SHOW LIMITS FOR DATABASE hidden", nil)
		require.NoError(t, err)
		require.Equal(t, []string{"hidden"}, catalogVisibilityTextValues(t, result, "database"))
	})
}
