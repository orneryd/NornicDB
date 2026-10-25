package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// A temporal value read from storage carries a zone of its own. An indexed
// equality on it finds the node right after the write and after a restart,
// when the index is filed from stored values, as the unindexed scan does.
func TestTemporalIndexedEqualitySurvivesRestart(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		dir := t.TempDir()
		open := func() (*storage.BadgerEngine, *StorageExecutor) {
			base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			return base, NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
		}
		base, exec := open()
		ctx := context.Background()
		run := func(query string) [][]interface{} {
			t.Helper()
			result, err := exec.Execute(ctx, query, nil)
			require.NoError(t, err, query)
			return result.Rows
		}
		run("CREATE INDEX p_d FOR (p:P) ON (p.d)")
		run("CREATE INDEX p_t FOR (p:P) ON (p.t)")
		for _, label := range []string{"P", "Q"} {
			run("CREATE (:" + label + " {id: 1, d: date('2020-01-02'), t: datetime('2020-01-02T03:04:05+01:00')})")
			run("CREATE (:" + label + " {id: 2, d: date('2020-01-03'), t: datetime('2020-01-03T03:04:05+01:00')})")
		}
		check := func(stage string) {
			t.Helper()
			for _, where := range []string{"p.d = date('2020-01-02')", "p.t = datetime('2020-01-02T03:04:05+01:00')", "p.d >= date('2020-01-03')"} {
				indexed := run("MATCH (p:P) WHERE " + where + " RETURN p.id ORDER BY p.id")
				plain := run("MATCH (p:Q) WHERE " + where + " RETURN p.id ORDER BY p.id")
				require.Equal(t, plain, indexed, "%s write-behind=%v: %s", stage, buffered, where)
				require.NotEmpty(t, indexed, "%s write-behind=%v: %s", stage, buffered, where)
			}
		}
		check("after write")
		require.NoError(t, base.Close())
		base, exec = open()
		check("after restart")
		require.NoError(t, base.Close())
	}
}
