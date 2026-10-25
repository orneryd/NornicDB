package cypher

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNamePositionalCountKeys(t *testing.T) {
	dir := t.TempDir()
	engine, err := storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			require.NoError(t, engine.Close())
		}
	})
	ctx := context.Background()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "count_keys"))
	leftLabel, rightType := cyphertext.QuoteName("A\x00B"), cyphertext.QuoteName("B\x00C")
	for _, query := range []string{
		"CREATE (a:" + leftLabel + ")-[:C]->(b:" + leftLabel + ")",
		"CREATE (:A)",
		"CREATE (:Other)-[:" + rightType + "]->(:Other)",
	} {
		_, err = executor.Execute(ctx, query, nil)
		require.NoError(t, err, query)
	}
	require.NoError(t, engine.FlushWriteBehind())
	verify := func(t *testing.T) {
		for _, tc := range []struct {
			name, pattern string
			count         int64
		}{
			{"start left", "(n:" + leftLabel + ")-[:C]->()", 1},
			{"start distinct tuple", "(n:A)-[:" + rightType + "]->()", 0},
			{"end left", "()-[:C]->(n:" + leftLabel + ")", 1},
			{"end distinct tuple", "()-[:" + rightType + "]->(n:A)", 0},
			{"ordinary control", "(n:Other)-[:" + rightType + "]->()", 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				query := "MATCH " + tc.pattern + " RETURN count(*) AS c"
				result, err := executor.Execute(ctx, query, nil)
				require.NoError(t, err, query)
				require.Equal(t, [][]interface{}{{tc.count}}, result.Rows, query)
			})
		}
	}
	t.Run("flushed", verify)
	require.NoError(t, engine.Close())
	engine, err = storage.NewBadgerEngine(dir)
	require.NoError(t, err)
	executor = NewStorageExecutor(storage.NewNamespacedEngine(engine, "count_keys"))
	t.Run("reopened", verify)
}

func TestDecodedNameCountDeltaFraming(t *testing.T) {
	dir := t.TempDir()
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: false})
	require.NoError(t, err)
	t.Cleanup(func() {
		if engine != nil {
			require.NoError(t, engine.Close())
		}
	})
	ctx := context.Background()
	executor := NewStorageExecutor(storage.NewNamespacedEngine(engine, "delta_names"))
	names := []string{"DeltaName", "DeltaName\x00part", "DeltaName\x00part\x00last", "OrdinaryControl"}
	for index, name := range names {
		for number := 0; number <= index; number++ {
			query := "CREATE (:" + cyphertext.QuoteName(name) + ")-[:" + cyphertext.QuoteName(name) + "]->()"
			_, err = executor.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
	}
	for _, phase := range []string{"committed deltas", "reopened"} {
		t.Run(phase, func(t *testing.T) {
			for index, name := range names {
				for _, carrier := range []struct{ name, pattern string }{
					{"label", "(n:" + cyphertext.QuoteName(name) + ")"},
					{"type", "()-[:" + cyphertext.QuoteName(name) + "]->()"},
				} {
					t.Run(carrier.name+"/"+name, func(t *testing.T) {
						query := "MATCH " + carrier.pattern + " RETURN count(*) AS c"
						result, err := executor.Execute(ctx, query, nil)
						require.NoError(t, err, query)
						require.Equal(t, [][]interface{}{{int64(index + 1)}}, result.Rows, query)
					})
				}
			}
		})
		if phase == "committed deltas" {
			require.NoError(t, engine.Close())
			engine, err = storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: false})
			require.NoError(t, err)
			executor = NewStorageExecutor(storage.NewNamespacedEngine(engine, "delta_names"))
		}
	}
}

func TestDecodedNameNamespaceCountIdentity(t *testing.T) {
	engine, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: false})
	require.NoError(t, err)
	manager, err := multidb.NewDatabaseManager(t.Context(), engine, nil)
	if err != nil {
		require.NoError(t, engine.Close())
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	ctx := context.Background()
	type subject struct {
		database, name string
		count          int64
		executor       *StorageExecutor
	}
	subjects := []subject{{database: "db\x00scope", name: "X", count: 1}, {database: "db", name: "scope\x00X", count: 2}, {database: "ordinary_db", name: "OrdinaryControl", count: 3}}
	for index := range subjects {
		subject := &subjects[index]
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: subject.database}))
		store, err := manager.GetStorage(subject.database)
		require.NoError(t, err)
		subject.executor = NewStorageExecutor(store)
		for number := int64(0); number < subject.count; number++ {
			query := "CREATE (:" + cyphertext.QuoteName(subject.name) + ")-[:" + cyphertext.QuoteName(subject.name) + "]->()"
			_, err := subject.executor.Execute(ctx, query, nil)
			require.NoError(t, err, query)
		}
	}
	verify := func(t *testing.T, subject subject) {
		for _, carrier := range []struct{ name, pattern string }{
			{"label", "(n:" + cyphertext.QuoteName(subject.name) + ")"},
			{"type", "()-[:" + cyphertext.QuoteName(subject.name) + "]->()"},
			{"positional", "(n:" + cyphertext.QuoteName(subject.name) + ")-[:" + cyphertext.QuoteName(subject.name) + "]->()"},
		} {
			t.Run(carrier.name, func(t *testing.T) {
				result, err := subject.executor.Execute(ctx, "MATCH "+carrier.pattern+" RETURN count(*) AS c", nil)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{subject.count}}, result.Rows, "a different namespace/name tuple must own separate counts")
			})
		}
	}
	for index, subject := range subjects {
		t.Run(fmt.Sprintf("before drop/%d", index), func(t *testing.T) { verify(t, subject) })
	}
	require.NoError(t, manager.DropDatabase(t.Context(), "db"))
	for _, index := range []int{0, 2} {
		t.Run(fmt.Sprintf("after drop/%d", index), func(t *testing.T) { verify(t, subjects[index]) })
	}
}
