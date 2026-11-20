package nornicdb

// v1.5.2 verification: reproductions built from the text of three Plane items,
// through the public embedded API (Open, ExecuteCypher, Backup, Restore).

import (
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func v152TextConfig(buffered bool) *Config {
	cfg := DefaultConfig()
	cfg.Database.AsyncWritesEnabled = buffered
	cfg.Database.StrictDurability = !buffered
	if buffered {
		cfg.Database.AsyncFlushInterval = time.Minute
	}
	cfg.Database.WALAutoCompactionEnabled = false
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	return cfg
}

func v152Count(t *testing.T, db *DB, query string) int64 {
	t.Helper()
	result, err := db.ExecuteCypher(t.Context(), query, nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	switch v := result.Rows[0][0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	case float64:
		return int64(v)
	}
	t.Fatalf("count returned %T", result.Rows[0][0])
	return 0
}

// N15-14 (NOR-147): with write buffering on, a UNIQUE constraint created before
// a restart must still reject a second buffered CREATE of the same value
// before flush. Expected: the second CREATE fails; one node remains.
func TestV152TextUniqueConstraintAfterRestart(t *testing.T) {
	for _, buffered := range []bool{true, false} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "data")
			db, err := Open(root, v152TextConfig(buffered))
			require.NoError(t, err)
			_, err = db.ExecuteCypher(t.Context(), "CREATE CONSTRAINT v152_doc_id FOR (d:V152Doc) REQUIRE d.id IS UNIQUE", nil)
			require.NoError(t, err)
			require.NoError(t, db.Close())
			db, err = Open(root, v152TextConfig(buffered))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, first := db.ExecuteCypher(t.Context(), "CREATE (:V152Doc {id:'k'})", nil)
			_, second := db.ExecuteCypher(t.Context(), "CREATE (:V152Doc {id:'k'})", nil)
			count := v152Count(t, db, "MATCH (d:V152Doc {id:'k'}) RETURN count(d)")
			t.Logf("V152TEXT NOR-147 buffered=%v first=%v second=%v count=%d", buffered, first, second, count)
			require.NoError(t, first)
			require.Error(t, second, "a second CREATE of an existing UNIQUE value was acknowledged after restart")
			require.EqualValues(t, 1, count)
		})
	}
}

// N15-132 (NOR-356): after a restore, a repeated query must not answer with
// the result cached from before the restore.
func TestV152TextQueryCacheAfterRestore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "data")
	db, err := Open(root, v152TextConfig(false))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	const read = "MATCH (p:V152Restore {k:'a'}) RETURN p.v AS v"
	_, err = db.ExecuteCypher(t.Context(), "CREATE (:V152Restore {k:'a', v:1})", nil)
	require.NoError(t, err)
	backup := filepath.Join(t.TempDir(), "backup.bak")
	require.NoError(t, db.Backup(t.Context(), backup))
	_, err = db.ExecuteCypher(t.Context(), "MATCH (p:V152Restore {k:'a'}) SET p.v = 2", nil)
	require.NoError(t, err)
	before, err := db.ExecuteCypher(t.Context(), read, nil)
	require.NoError(t, err)
	t.Logf("V152TEXT NOR-356 before restore rows=%v", before.Rows)
	require.NoError(t, db.Restore(t.Context(), backup))
	after, err := db.ExecuteCypher(t.Context(), read, nil)
	require.NoError(t, err)
	control, err := db.ExecuteCypher(t.Context(), read+" ", nil)
	require.NoError(t, err)
	t.Logf("V152TEXT NOR-356 after restore rows=%v; uncached-text control rows=%v", after.Rows, control.Rows)
	require.Equal(t, control.Rows, after.Rows, "the repeated query returned its pre-restore cached result")
}

// N15-8 (NOR-139): repeatedly MATCH an existing node by key and increment it
// while other writers create, update and delete other nodes of the same label.
// Expected: every increment matches one row and the final count is exact.
func TestV152TextCounterUnderConcurrentWrites(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprintf("buffered=%v", buffered), func(t *testing.T) {
			db, err := Open(filepath.Join(t.TempDir(), "data"), v152TextConfig(buffered))
			require.NoError(t, err)
			t.Cleanup(func() { _ = db.Close() })
			_, err = db.ExecuteCypher(t.Context(), "CREATE INDEX v152_counter_id FOR (n:V152Counter) ON (n.id)", nil)
			require.NoError(t, err)
			_, err = db.ExecuteCypher(t.Context(), "CREATE (:V152Counter {id:'target', c:0})", nil)
			require.NoError(t, err)
			const incrementers, increments, churners = 4, 250, 4
			var zeroRows, failed, acknowledged atomic.Int64
			stop := make(chan struct{})
			var churn sync.WaitGroup
			for w := 0; w < churners; w++ {
				churn.Add(1)
				go func(w int) {
					defer churn.Done()
					for i := 0; ; i++ {
						select {
						case <-stop:
							return
						default:
						}
						key := fmt.Sprintf("other-%d-%d", w, i%20)
						for _, q := range []string{
							"MERGE (n:V152Counter {id:$k}) SET n.c = coalesce(n.c, 0) + 1",
							"MATCH (n:V152Counter {id:$k}) SET n.id = $k2",
							"MATCH (n:V152Counter {id:$k2}) DETACH DELETE n",
						} {
							_, _ = db.ExecuteCypher(t.Context(), q, map[string]interface{}{"k": key, "k2": key + "-renamed"})
						}
					}
				}(w)
			}
			var inc sync.WaitGroup
			for w := 0; w < incrementers; w++ {
				inc.Add(1)
				go func() {
					defer inc.Done()
					for i := 0; i < increments; i++ {
						result, err := db.ExecuteCypher(t.Context(), "MATCH (n:V152Counter {id:'target'}) SET n.c = n.c + 1 RETURN n.c", nil)
						switch {
						case err != nil:
							failed.Add(1)
						case len(result.Rows) == 0:
							zeroRows.Add(1)
						default:
							acknowledged.Add(1)
						}
					}
				}()
			}
			inc.Wait()
			close(stop)
			churn.Wait()
			final := v152Count(t, db, "MATCH (n:V152Counter {id:'target'}) RETURN n.c")
			t.Logf("V152TEXT NOR-139 buffered=%v acknowledged=%d zeroRows=%d failed=%d final=%d", buffered, acknowledged.Load(), zeroRows.Load(), failed.Load(), final)
			require.Zero(t, zeroRows.Load(), "MATCH on the existing target returned no row")
			require.EqualValues(t, acknowledged.Load(), final, "acknowledged increments are missing from the final count")
		})
	}
}
