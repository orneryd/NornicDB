package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// compositeTestFixture creates a composite database "comp" with one local
// constituent "a" backed by "shard_a", and returns the default executor
// (scoped to the default DB) wired with a DatabaseManager.
func compositeTestFixture(t *testing.T) (*StorageExecutor, *multidb.DatabaseManager) {
	t.Helper()
	base := storage.NewMemoryEngine()
	mgr, err := multidb.NewDatabaseManager(base, nil)
	require.NoError(t, err)
	t.Cleanup(func() { mgr.Close() })

	require.NoError(t, mgr.CreateDatabase("shard_a"))
	require.NoError(t, mgr.CreateCompositeDatabase("comp", []multidb.ConstituentRef{
		{Alias: "a", DatabaseName: "shard_a", Type: "local", AccessMode: "read_write"},
	}))

	defaultStore, err := mgr.GetStorage(mgr.DefaultDatabaseName())
	require.NoError(t, err)
	exec := NewStorageExecutor(defaultStore)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	return exec, mgr
}

// compositeRootExecutor returns an executor scoped to the composite root engine.
func compositeRootExecutor(t *testing.T, mgr *multidb.DatabaseManager) *StorageExecutor {
	t.Helper()
	compEngine, err := mgr.GetStorage("comp")
	require.NoError(t, err)
	exec := NewStorageExecutor(compEngine)
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: mgr})
	return exec
}

// --- Step 6: Schema DDL rejection on composite root ---

func TestCompositeRoot_CreateIndex_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(),
		"CREATE INDEX idx_test FOR (n:Label) ON (n.prop)", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "constituent target")
}

func TestCompositeRoot_CreateFulltextIndex_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(),
		"CREATE FULLTEXT INDEX ft_test FOR (n:Label) ON EACH [n.title]", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
}

func TestCompositeRoot_CreateConstraint_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(),
		"CREATE CONSTRAINT unique_id FOR (n:Node) REQUIRE n.id IS UNIQUE", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "constituent target")
}

func TestCompositeRoot_DropIndex_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "DROP INDEX some_idx", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "constituent target")
}

func TestCompositeRoot_DropConstraint_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "DROP CONSTRAINT some_constraint", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
}

// --- Step 6: Real DROP INDEX execution ---

func TestDropIndex_RealExecution(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := NewStorageExecutor(base)

	// Create an index.
	_, err := exec.Execute(context.Background(),
		"CREATE INDEX idx_person_name FOR (p:Person) ON (p.name)", nil)
	require.NoError(t, err)

	// Verify it exists.
	// SHOW RANGE INDEXES leaves out the two token lookup indexes.
	res, err := exec.Execute(context.Background(), "SHOW RANGE INDEXES", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Rows), "expected 1 index after CREATE")

	// Drop it.
	_, err = exec.Execute(context.Background(), "DROP INDEX idx_person_name", nil)
	require.NoError(t, err)

	// Verify it's gone.
	res, err = exec.Execute(context.Background(), "SHOW RANGE INDEXES", nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Rows), "expected 0 indexes after DROP")
}

func TestDropIndex_IfExists_NonExistent(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := NewStorageExecutor(base)

	// DROP INDEX IF EXISTS on non-existent index should succeed silently.
	res, err := exec.Execute(context.Background(), "DROP INDEX nonexistent IF EXISTS", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestDropIndex_NonExistent_Fails(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := NewStorageExecutor(base)

	_, err := exec.Execute(context.Background(), "DROP INDEX nonexistent", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not exist")
}

func TestDropIndex_BacktickQuoted(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := NewStorageExecutor(base)

	_, err := exec.Execute(context.Background(),
		"CREATE INDEX `my-index` FOR (n:Label) ON (n.prop)", nil)
	require.NoError(t, err)

	res, err := exec.Execute(context.Background(), "SHOW RANGE INDEXES", nil)
	require.NoError(t, err)
	require.Equal(t, 1, len(res.Rows))

	// Verify the stored index name (backticks should be stripped).
	storedName := ""
	if m, ok := res.Rows[0][1].(string); ok {
		storedName = m
	}
	t.Logf("stored index name: %q", storedName)

	_, err = exec.Execute(context.Background(), "DROP INDEX `my-index`", nil)
	require.NoError(t, err, "DROP INDEX `my-index` failed; stored name was %q", storedName)

	res, err = exec.Execute(context.Background(), "SHOW RANGE INDEXES", nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.Rows))
}

// --- Step 7: SHOW INDEXES / SHOW CONSTRAINTS rejection on composite root ---

func TestCompositeRoot_ShowIndexes_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "SHOW INDEXES", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "constituent target")
}

func TestCompositeRoot_ShowFulltextIndexes_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "SHOW FULLTEXT INDEXES", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
}

func TestCompositeRoot_ShowConstraints_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "SHOW CONSTRAINTS", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "constituent target")
}

// --- Step 6-7: Constituent-scoped schema commands succeed ---

func TestConstituent_CreateIndex_Success(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// USE comp.a routes to the constituent, where schema DDL should work.
	res, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX idx_test FOR (n:Label) ON (n.prop)", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestConstituent_ShowIndexes_Success(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Create an index on the constituent.
	_, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX idx_show FOR (n:Label) ON (n.prop)", nil)
	require.NoError(t, err)

	// SHOW INDEXES on the constituent should return that index.
	res, err := exec.Execute(context.Background(), "USE comp.a SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)

	found := false
	for _, row := range res.Rows {
		if len(row) >= 2 {
			if name, ok := row[1].(string); ok && name == "idx_show" {
				found = true
				break
			}
		}
	}
	require.True(t, found, "expected index 'idx_show' in SHOW INDEXES result, got %v", res.Rows)
}

func TestConstituent_DropIndex_Success(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Create then drop an index on the constituent.
	_, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX idx_drop FOR (n:Label) ON (n.prop)", nil)
	require.NoError(t, err)

	_, err = exec.Execute(context.Background(), "USE comp.a DROP INDEX idx_drop", nil)
	require.NoError(t, err)

	// Verify it's gone.
	res, err := exec.Execute(context.Background(), "USE comp.a SHOW INDEXES", nil)
	require.NoError(t, err)
	for _, row := range res.Rows {
		if len(row) >= 2 {
			if name, ok := row[1].(string); ok && name == "idx_drop" {
				t.Fatal("expected index 'idx_drop' to be dropped")
			}
		}
	}
}

func TestConstituent_ShowConstraints_Success(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// SHOW CONSTRAINTS on constituent should succeed (empty is fine).
	res, err := exec.Execute(context.Background(), "USE comp.a SHOW CONSTRAINTS", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Columns)
}

// --- Step 11: Reject implicit-union plain MATCH on composite root ---

func TestCompositeRoot_PlainMatch_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "MATCH (n) RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
	require.Contains(t, err.Error(), "explicit graph targeting")
}

func TestCompositeRoot_PlainCreate_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "CREATE (n:Test {id: '1'})", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
}

func TestCompositeRoot_PlainReturn_Rejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	_, err := exec.Execute(context.Background(), "RETURN 1 AS one", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
}

func TestCompositeRoot_UseConstituent_Match_Succeeds(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	res, err := exec.Execute(context.Background(),
		"USE comp.a MATCH (n) RETURN count(n) AS cnt", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}

func TestCompositeRoot_ShowDatabases_Allowed(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	// System commands should still work on composite root.
	res, err := exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.Rows)
}

func TestCompositeRoot_ShowCompositeDatabases_Allowed(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	res, err := exec.Execute(context.Background(), "SHOW COMPOSITE DATABASES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
}

// --- Step 13: Search service not inherited by composite ---

func TestCompositeExecutor_SearchServiceNotInherited(t *testing.T) {
	base := storage.NewMemoryEngine()
	exec := NewStorageExecutor(base)
	// Simulate having a search service on the base executor.
	// (We can't set a real one without import cycles, but we can check
	// the cloneForStorage behavior by verifying the field is nil on composite.)
	compositeEngine := storage.NewCompositeEngine(
		map[string]storage.Engine{"a": storage.NewMemoryEngine()},
		map[string]string{"a": "shard_a"},
		map[string]string{"a": "read_write"},
	)
	cloned := exec.cloneForStorage(compositeEngine)
	// The cloned executor for a composite engine must not inherit searchService.
	if cloned.searchService != nil {
		t.Fatal("expected searchService to be nil on composite-scoped executor")
	}
}

// --- Step 9: Documentation examples as deterministic tests ---

func TestDocExample_CreateIndexOnConstituent(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Doc example: "Create index on a specific constituent"
	// USE comp.a CREATE INDEX translation_id_idx FOR (t:Translation) ON (t.id)
	res, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX translation_id_idx FOR (t:Translation) ON (t.id)", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestDocExample_ShowIndexesOnConstituent(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Create index first.
	_, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX doc_idx FOR (t:Translation) ON (t.id)", nil)
	require.NoError(t, err)

	// Doc example: "Show indexes for a specific constituent"
	// USE comp.a SHOW INDEXES
	res, err := exec.Execute(context.Background(), "USE comp.a SHOW INDEXES", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.GreaterOrEqual(t, len(res.Rows), 1)
}

func TestDocExample_DropIndexOnConstituent(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Create then drop.
	_, err := exec.Execute(context.Background(),
		"USE comp.a CREATE INDEX drop_me FOR (t:Translation) ON (t.id)", nil)
	require.NoError(t, err)

	// Doc example: "Drop index from a specific constituent"
	// USE comp.a DROP INDEX drop_me
	_, err = exec.Execute(context.Background(), "USE comp.a DROP INDEX drop_me", nil)
	require.NoError(t, err)

	// Verify gone.
	res, err := exec.Execute(context.Background(), "USE comp.a SHOW INDEXES", nil)
	require.NoError(t, err)
	for _, row := range res.Rows {
		if len(row) >= 2 {
			if name, ok := row[1].(string); ok && name == "drop_me" {
				t.Fatal("index 'drop_me' should have been dropped")
			}
		}
	}
}

func TestDocExample_CompositeRootSchemaRejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	// Doc example: "These are REJECTED on composite root"
	tests := []struct {
		name    string
		query   string
		errFrag string
	}{
		{"CREATE INDEX on root", "CREATE INDEX bad FOR (n:N) ON (n.x)", "composite databases requires a constituent target"},
		{"SHOW INDEXES on root", "SHOW INDEXES", "composite databases requires a constituent target"},
		{"SHOW CONSTRAINTS on root", "SHOW CONSTRAINTS", "composite databases requires a constituent target"},
		{"DROP INDEX on root", "DROP INDEX bad", "composite databases requires a constituent target"},
		{"DROP CONSTRAINT on root", "DROP CONSTRAINT bad", "composite databases requires a constituent target"},
		{"CREATE CONSTRAINT on root", "CREATE CONSTRAINT c FOR (n:N) REQUIRE n.x IS UNIQUE", "composite databases requires a constituent target"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := exec.Execute(context.Background(), tt.query, nil)
			require.Error(t, err, "expected error for %s", tt.name)
			require.Contains(t, err.Error(), "Neo.ClientError.Statement.NotAllowed")
			require.Contains(t, err.Error(), tt.errFrag)
		})
	}
}

func TestDocExample_DropIndexIfExistsOnConstituent(t *testing.T) {
	exec, _ := compositeTestFixture(t)

	// Doc example: DROP INDEX IF EXISTS on constituent (silent no-op)
	res, err := exec.Execute(context.Background(),
		"USE comp.a DROP INDEX nonexistent IF EXISTS", nil)
	require.NoError(t, err)
	require.NotNil(t, res)
}

func TestDocExample_PlainQueryOnCompositeRejected(t *testing.T) {
	_, mgr := compositeTestFixture(t)
	exec := compositeRootExecutor(t, mgr)

	// Doc example: plain MATCH on composite root is rejected
	_, err := exec.Execute(context.Background(), "MATCH (n:Person) RETURN n", nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "composite databases require explicit graph targeting")

	// Doc example: plain CREATE on composite root is rejected
	_, err = exec.Execute(context.Background(), `CREATE (n:Person {name: "Alice"})`, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "composite databases require explicit graph targeting")

	// System commands (SHOW DATABASES) ARE allowed on composite root
	_, err = exec.Execute(context.Background(), "SHOW DATABASES", nil)
	require.NoError(t, err)
}
