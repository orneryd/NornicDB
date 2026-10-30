package storage

import (
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// Review r3 P1 (#1655): an explicit commit publishes to storage before it
// updates the node cache, so a blind write staged in between reads the old
// body from the cache. Its commit must compare the key image with the node's
// current body, or it overwrites the acknowledged key change.
func TestPR1655StaleCachedBodyIsReadAgainAtCommit(t *testing.T) {
	for _, implicit := range []bool{true, false} {
		t.Run(fmt.Sprintf("implicit=%t", implicit), func(t *testing.T) {
			b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: true, WriteBehind: true, WriteBehindInterval: time.Hour}))
			require.NoError(t, err)
			defer b.Close()
			require.NoError(t, b.GetSchemaForNamespace("test").AddUniqueConstraint("doc_id", "Doc", "id"))
			doc := func(id string, extra ...string) *Node {
				node := &Node{ID: "test:x", Labels: []string{"Doc"}, Properties: map[string]interface{}{"id": id}}
				if len(extra) > 0 {
					node.Properties["note"] = extra[0]
				}
				return node
			}
			seed, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, seed.SetNamespace("test"))
			_, err = seed.CreateNode(doc("a"))
			require.NoError(t, err)
			require.NoError(t, seed.Commit())
			require.NoError(t, b.FlushWriteBehind())

			rename, err := b.BeginTransaction()
			require.NoError(t, err)
			require.NoError(t, rename.SetNamespace("test"))
			require.NoError(t, rename.UpdateGraphNode(NodeUpdateRequest{After: doc("b"), Policy: ClientNodeEdits}))
			require.NoError(t, rename.Commit())
			require.NoError(t, b.FlushWriteBehind())
			// The cache still holds the body from before the rename, as between
			// the rename's publication and its cache update.
			b.nodeCacheMu.Lock()
			b.nodeCache["test:x"] = doc("a")
			b.nodeCacheMu.Unlock()

			blind, err := b.BeginTransaction()
			require.NoError(t, err)
			defer blind.Rollback()
			require.NoError(t, blind.SetImplicit(implicit))
			require.NoError(t, blind.SetNamespace("test"))
			if err := blind.UpdateGraphNode(NodeUpdateRequest{After: doc("a", "x"), Policy: ClientNodeEdits}); err != nil {
				require.ErrorIs(t, err, ErrConflict, "a staging read that sees the rename refuses the write")
			} else {
				require.ErrorIs(t, blind.Commit(), ErrConflict, "the rename was published before the blind write")
			}
			require.NoError(t, b.FlushWriteBehind())
			// Drop the planted entry so the check reads what storage holds.
			b.nodeCacheMu.Lock()
			delete(b.nodeCache, "test:x")
			b.nodeCacheMu.Unlock()
			current, err := b.GetNode("test:x")
			require.NoError(t, err)
			require.Equal(t, "b", current.Properties["id"], "the acknowledged rename is kept")
		})
	}
}
