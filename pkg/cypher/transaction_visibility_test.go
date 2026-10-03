package cypher

import (
	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestExplicitTransactionSchemaRollbackIsolation(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "schema_rollback")
	executor := NewStorageExecutor(store)
	peer := NewStorageExecutor(store)
	ctx := context.Background()
	_, err = executor.Execute(ctx, "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor.txContext != nil && executor.txContext.active {
			_, _ = executor.handleRollback()
		}
	})
	_, err = executor.Execute(ctx, "CREATE INDEX staged_index FOR (n:Account) ON (n.accountID)", nil)
	require.NoError(t, err)
	staged, err := executor.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'staged_index' RETURN name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"staged_index"}}, staged.Rows)
	visible, err := peer.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'staged_index' RETURN name", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	rolledBack, err := peer.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'staged_index' RETURN name", nil)
	require.NoError(t, err)
	require.Empty(t, visible.Rows, "uncommitted schema must be private")
	require.Empty(t, rolledBack.Rows, "rollback must discard schema mutations")
}

func TestExplicitTransactionSchemaCommitPublishesBackfill(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "schema_commit")
	executor := NewStorageExecutor(store)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE (:Account {accountID: 'seed', name: 'existing'})",
		"CREATE INDEX retained_index FOR (n:Account) ON (n.name)",
		"BEGIN",
		"CREATE INDEX staged_index FOR (n:Account) ON (n.accountID)",
		"COMMIT",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	visible, err := executor.Execute(ctx, "SHOW INDEXES YIELD name WHERE name = 'staged_index' RETURN name", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"staged_index"}}, visible.Rows)
	for _, property := range []string{"accountID", "name"} {
		indexed := store.GetSchema().PropertyIndexLookup("Account", property, map[string]string{"accountID": "seed", "name": "existing"}[property])
		require.Len(t, indexed, 1, property)
	}
}

func TestExplicitTransactionSchemaBackfillConcurrentWrite(t *testing.T) {
	base, err := storage.NewBadgerEngineInMemory()
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	store := storage.NewNamespacedEngine(base, "schema_concurrent")
	executor := NewStorageExecutor(store)
	peer := NewStorageExecutor(store)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE (:Account {accountID: 'seed'})",
		"BEGIN",
		"CREATE INDEX concurrent_index FOR (n:Account) ON (n.accountID)",
	} {
		_, err := executor.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	_, err = peer.Execute(ctx, "CREATE (:Account {accountID: 'concurrent'})", nil)
	require.NoError(t, err)
	_, err = executor.Execute(ctx, "COMMIT", nil)
	if err != nil {
		require.ErrorIs(t, err, storage.ErrConflict)
		return
	}
	indexed := store.GetSchema().PropertyIndexLookup("Account", "accountID", "concurrent")
	require.Len(t, indexed, 1, "schema commit must not publish a stale backfill")
}

func TestExplicitTransactionVectorSchemaRuntimeLifetime(t *testing.T) {
	for _, procedure := range []bool{false, true} {
		for _, dropping := range []bool{false, true} {
			for _, commit := range []bool{false, true} {
				t.Run(map[bool]string{false: "ddl", true: "procedure"}[procedure]+"/"+map[bool]string{false: "create", true: "drop"}[dropping]+"/"+map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
					store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "vector_schema")
					executor := NewStorageExecutor(store)
					ctx := context.Background()
					create := "CREATE VECTOR INDEX staged_vector FOR (n:Account) ON (n.embedding) OPTIONS {indexConfig: {`vector.dimensions`: 3, `vector.similarity_function`: 'cosine'}}"
					if procedure {
						create = "CALL db.index.vector.createNodeIndex('staged_vector', 'Account', 'embedding', 3, 'cosine')"
					}
					statement := create
					if dropping {
						_, err := executor.Execute(ctx, create, nil)
						require.NoError(t, err)
						statement = "DROP INDEX staged_vector"
						if procedure {
							statement = "CALL db.index.vector.drop('staged_vector')"
						}
					}
					_, err := executor.Execute(ctx, "BEGIN", nil)
					require.NoError(t, err)
					_, err = executor.Execute(ctx, statement, nil)
					require.NoError(t, err)
					_, stagedRuntimeVisible := executor.vectorIndexSpaces["staged_vector"]
					_, err = executor.Execute(ctx, map[bool]string{false: "ROLLBACK", true: "COMMIT"}[commit], nil)
					require.NoError(t, err)
					require.Equal(t, dropping, stagedRuntimeVisible, "staged DDL must not alter the public vector registry")
					key, finalRuntimeVisible := executor.vectorIndexSpaces["staged_vector"]
					require.Equal(t, dropping != commit, finalRuntimeVisible)
					if finalRuntimeVisible {
						_, exists := executor.GetVectorRegistry().GetSpace(key)
						require.True(t, exists)
					}
				})
			}
		}
	}
}

func TestExplicitTransactionSchemaProcedureAdmission(t *testing.T) {
	for _, procedure := range []struct{ name, query, entity string }{
		{"node_vector", "CALL db.index.vector.createNodeIndex('schema_procedure', 'Account', 'embedding', 3, 'cosine')", "NODE"},
		{"relationship_vector", "CALL db.index.vector.createRelationshipIndex('schema_procedure', 'ACCOUNT', 'embedding', 3, 'cosine')", "RELATIONSHIP"},
		{"node_fulltext", "CALL db.index.fulltext.createNodeIndex('schema_procedure', ['Account'], ['name'])", "NODE"},
		{"relationship_fulltext", "CALL db.index.fulltext.createRelationshipIndex('schema_procedure', ['ACCOUNT'], ['name'])", "RELATIONSHIP"},
	} {
		for _, commit := range []bool{false, true} {
			t.Run(procedure.name+"/"+map[bool]string{false: "rollback", true: "commit"}[commit], func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "schema_procedure")
				executor := NewStorageExecutor(store)
				peer := NewStorageExecutor(store)
				ctx := context.Background()
				_, err := executor.Execute(ctx, "BEGIN", nil)
				require.NoError(t, err)
				_, err = executor.Execute(ctx, procedure.query, nil)
				require.NoError(t, err)
				show := "SHOW INDEXES YIELD name, entityType WHERE name = 'schema_procedure' RETURN name, entityType"
				staged, err := executor.Execute(ctx, show, nil)
				require.NoError(t, err)
				visible, err := peer.Execute(ctx, show, nil)
				require.NoError(t, err)
				_, err = executor.Execute(ctx, map[bool]string{false: "ROLLBACK", true: "COMMIT"}[commit], nil)
				require.NoError(t, err)
				require.Empty(t, visible.Rows, "uncommitted procedure schema must be private")
				require.Equal(t, [][]interface{}{{"schema_procedure", procedure.entity}}, staged.Rows)
				stored, err := peer.Execute(ctx, show, nil)
				require.NoError(t, err)
				if commit {
					require.Equal(t, staged.Rows, stored.Rows)
				} else {
					require.Empty(t, stored.Rows)
				}
			})
		}
	}
}

func TestExplicitTransactionRejectsMixedSchemaAndData(t *testing.T) {
	for _, schemaFirst := range []bool{false, true} {
		t.Run(map[bool]string{false: "data_then_schema", true: "schema_then_data"}[schemaFirst], func(t *testing.T) {
			base, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			t.Cleanup(func() { _ = base.Close() })
			store := storage.NewNamespacedEngine(base, "mixed_schema")
			executor := NewStorageExecutor(store)
			ctx := context.Background()
			_, err = executor.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			statements := []string{"CREATE (:Account {accountID: 'mixed'})", "CREATE INDEX mixed_index FOR (n:Account) ON (n.accountID)"}
			if schemaFirst {
				statements[0], statements[1] = statements[1], statements[0]
			}
			_, err = executor.Execute(ctx, statements[0], nil)
			require.NoError(t, err)
			_, mixErr := executor.Execute(ctx, statements[1], nil)
			_, rollbackErr := executor.Execute(ctx, "ROLLBACK", nil)
			require.NoError(t, rollbackErr)
			require.ErrorContains(t, mixErr, "ForbiddenDueToTransactionType")
			_, exists := store.GetSchema().GetPropertyIndex("Account", "accountID")
			require.False(t, exists)
			nodes, err := store.GetNodesByLabel("Account")
			require.NoError(t, err)
			require.Empty(t, nodes)
		})
	}
}

func TestExplicitTransactionSeesAcknowledgedAsyncWrite(t *testing.T) {
	base := newTestMemoryEngine(t)
	async := storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
		FlushInterval:    time.Hour,
		MaxNodeCacheSize: 1000,
		MaxEdgeCacheSize: 1000,
	})
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(async, "visibility"))

	created, err := executor.Execute(context.Background(),
		"CREATE (:Account {accountID: 'acknowledged'})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, created.Stats.NodesCreated)
	require.True(t, async.HasPendingWrites(), "the reproduction requires an unflushed acknowledged write")

	_, err = executor.handleBegin()
	require.NoError(t, err)
	t.Cleanup(func() {
		if executor.txContext != nil && executor.txContext.active {
			_, _ = executor.handleRollback()
		}
	})

	matched, err := executor.Execute(context.Background(),
		"MATCH (account:Account {accountID: 'acknowledged'}) RETURN account.accountID", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{"acknowledged"}}, matched.Rows)
}

func TestExplicitTransactionCreatesDependentWritesFromAcknowledgedState(t *testing.T) {
	base := newTestMemoryEngine(t)
	async := storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
		FlushInterval:    time.Hour,
		MaxNodeCacheSize: 1000,
		MaxEdgeCacheSize: 1000,
	})
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	executor := NewStorageExecutor(storage.NewNamespacedEngine(async, "dependent"))

	_, err := executor.Execute(context.Background(),
		"CREATE (:Account {accountID: 'source'})", nil)
	require.NoError(t, err)
	_, err = executor.handleBegin()
	require.NoError(t, err)

	created, err := executor.Execute(context.Background(), `
		MATCH (source:Account {accountID: 'source'})
		CREATE (target:Account {accountID: 'target'})
		CREATE (source)-[:LINKS_TO]->(target)
	`, nil)
	require.NoError(t, err)
	require.Equal(t, 1, created.Stats.RelationshipsCreated)
	_, err = executor.handleCommit()
	require.NoError(t, err)

	result, err := executor.Execute(context.Background(),
		"MATCH (:Account {accountID: 'source'})-[r:LINKS_TO]->(:Account {accountID: 'target'}) RETURN count(r)", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)
}

func TestOpenExplicitTransactionDoesNotBlockAnotherBeginAfterAcknowledgedWrite(t *testing.T) {
	base := newTestMemoryEngine(t)
	async := storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
		FlushInterval:    time.Hour,
		MaxNodeCacheSize: 1000,
		MaxEdgeCacheSize: 1000,
	})
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	store := storage.NewNamespacedEngine(async, "concurrent_begin")
	first := NewStorageExecutor(store)
	second := NewStorageExecutor(store)

	_, err := first.handleBegin()
	require.NoError(t, err)
	firstOpen := true
	t.Cleanup(func() {
		if firstOpen {
			_, _ = first.handleRollback()
		}
		if second.txContext != nil && second.txContext.active {
			_, _ = second.handleRollback()
		}
	})

	_, err = async.CreateNode(&storage.Node{
		ID:     "concurrent_begin:pending",
		Labels: []string{"Pending"},
	})
	require.NoError(t, err)
	require.True(t, async.HasPendingWrites(), "the reproduction requires a pending acknowledged write")

	beginDone := make(chan error, 1)
	go func() {
		_, beginErr := second.handleBegin()
		beginDone <- beginErr
	}()

	var beginErr error
	beginCompletedPromptly := false
	select {
	case beginErr = <-beginDone:
		beginCompletedPromptly = true
	case <-time.After(250 * time.Millisecond):
	}

	// Always release the first transaction before asserting so a regression
	// cannot strand the second BEGIN goroutine or the async engine cleanup.
	_, rollbackErr := first.handleRollback()
	require.NoError(t, rollbackErr)
	firstOpen = false
	if !beginCompletedPromptly {
		select {
		case beginErr = <-beginDone:
		case <-time.After(time.Second):
			t.Fatal("second BEGIN remained blocked after the first transaction rolled back")
		}
	}
	require.True(t, beginCompletedPromptly, "second BEGIN blocked for the lifetime of an unrelated explicit transaction")
	require.NoError(t, beginErr)
}

func TestCountReadDoesNotStallWhileAnotherTransactionBegins(t *testing.T) {
	base := newTestMemoryEngine(t)
	async := storage.NewAsyncEngine(base, &storage.AsyncEngineConfig{
		FlushInterval:    time.Hour,
		MaxNodeCacheSize: 1000,
		MaxEdgeCacheSize: 1000,
	})
	t.Cleanup(func() { require.NoError(t, async.Close()) })
	store := storage.NewNamespacedEngine(async, "concurrent_count")
	first := NewStorageExecutor(store)
	second := NewStorageExecutor(store)

	_, err := first.handleBegin()
	require.NoError(t, err)
	t.Cleanup(func() {
		if first.txContext != nil && first.txContext.active {
			_, _ = first.handleRollback()
		}
		if second.txContext != nil && second.txContext.active {
			_, _ = second.handleRollback()
		}
	})

	_, err = async.CreateNode(&storage.Node{
		ID:     "concurrent_count:pending",
		Labels: []string{"Pending"},
	})
	require.NoError(t, err)

	beginDone := make(chan error, 1)
	go func() {
		_, beginErr := second.handleBegin()
		beginDone <- beginErr
	}()

	countDone := make(chan error, 1)
	go func() {
		_, countErr := async.NodeCount()
		countDone <- countErr
	}()

	select {
	case countErr := <-countDone:
		require.NoError(t, countErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("count read stalled behind transaction snapshot admission")
	}
	select {
	case beginErr := <-beginDone:
		require.NoError(t, beginErr)
	case <-time.After(250 * time.Millisecond):
		t.Fatal("second BEGIN stalled behind an open transaction")
	}
}
