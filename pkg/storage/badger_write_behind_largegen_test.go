package storage

// Regression controls for large-generation write-behind flush and token draining.

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// flushLargeWriteBehindGeneration stages one large generation through
// write-behind and flushes it. Every commit is an implicit, namespaced
// single-node commit, so both generation cases share the exact staging,
// flush and drain sequence; nodeFor supplies each case's own node, which is
// the only difference between them (no properties vs. a fresh property-key
// token per commit).
func flushLargeWriteBehindGeneration(t *testing.T, engine *BadgerEngine, n int, nodeFor func(i int) *Node) {
	t.Helper()

	for i := 0; i < n; i++ {
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, tx.SetImplicit(true))
		require.NoError(t, tx.SetNamespace("test"))
		_, err = tx.CreateNode(nodeFor(i))
		require.NoError(t, err)
		require.NoError(t, tx.Commit())
	}
	require.NoError(t, engine.FlushWriteBehind(nil))
	require.Zero(t, engine.writeBehind.PendingOps())
}

func TestWriteBehind_LargeGenerationFlushSpansBadgerBatches(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	const n = 25000
	flushLargeWriteBehindGeneration(t, engine, n, func(i int) *Node {
		return &Node{
			ID:         NodeID("test:zz" + strconv.Itoa(i)),
			Labels:     []string{"ZZ"},
			Properties: nil,
		}
	})
}

// TestWriteBehind_LargeGenerationFlushWithPropertyTokens reproduces the
// production deadlock: a generation whose flush turns into a large commit
// (the flusher holds the commit gate exclusively) ALSO staged property-key
// tokens, so per-commit token persistence flushed a db-targeted batch that
// re-entered the commit gate's RLock and deadlocked against the flusher's
// own exclusive Lock. The token batches must ride the large commit.
func TestWriteBehind_LargeGenerationFlushWithPropertyTokens(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)

	const n = 25000
	flushLargeWriteBehindGeneration(t, engine, n, func(i int) *Node {
		// Each commit introduces a fresh property name: the property-key
		// drain is non-empty for every commit in the generation.
		return &Node{
			ID:     NodeID("test:tok" + strconv.Itoa(i)),
			Labels: []string{"Tok"},
			Properties: map[string]any{
				"prop_" + strconv.Itoa(i): int64(i),
			},
		}
	})
}

// Sync-path control: one explicit transaction creating the same 30K nodes,
// write-behind disabled entirely.
func TestWriteBehind_SyncLargeCommitControl(t *testing.T) {
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir()}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })

	const n = 25000
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetNamespace("test"))
	for i := 0; i < n; i++ {
		_, err = tx.CreateNode(&Node{
			ID:         NodeID("test:zz" + strconv.Itoa(i)),
			Labels:     []string{"ZZ"},
			Properties: nil,
		})
		require.NoError(t, err)
	}
	require.NoError(t, tx.Commit())
}
