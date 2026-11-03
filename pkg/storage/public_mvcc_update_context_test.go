package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type mvccUpdateContextKey struct{}

func TestPublicMVCCUpdateHonorsRequestContext(t *testing.T) {
	for _, route := range []string{"write_head", "delete_version"} {
		t.Run(route, func(t *testing.T) {
			engine := newTestMemoryEngine(t)
			node := &Node{ID: "optional:n", Properties: map[string]any{"value": int64(1)}}
			_, err := engine.CreateNode(t.Context(), node)
			require.NoError(t, err)
			number, ok := engine.idDict.nodeForwardLookup(node.ID)
			require.True(t, ok)
			logical := mvccLogicalKey(prefixMVCCNode, number)
			original, err := engine.ReadMVCCHead(context.Background(), logical)
			require.NoError(t, err)
			older := original.Version
			older.CommitTimestamp = older.CommitTimestamp.Add(-time.Minute)
			if route == "delete_version" {
				require.NoError(t, engine.AppendNodeVersion(node, older))
				// Append advances the head too; restore the newer live fence so
				// this archived version is eligible for lifecycle deletion.
				require.NoError(t, engine.WriteMVCCHead(context.Background(), logical, original))
			}
			target := original
			target.Version.CommitTimestamp = target.Version.CommitTimestamp.Add(time.Minute)
			target.Version.CommitSequence++
			operation := func(ctx context.Context) error {
				if route == "write_head" {
					return engine.WriteMVCCHead(ctx, logical, target)
				}
				return engine.DeleteMVCCVersion(ctx, logical, older)
			}
			hasOlder := func() bool {
				found := false
				require.NoError(t, engine.IterateMVCCVersions(context.Background(), logical, func(version MVCCVersion, _ bool, _ int64) error {
					found = found || version.Compare(older) == 0
					return nil
				}))
				return found
			}
			if route == "delete_version" {
				require.True(t, hasOlder())
			}
			marker := new(int)
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), mvccUpdateContextKey{}, marker))
			defer cancel()
			engine.db.commitGate.Lock()
			held := true
			defer func() {
				if held {
					engine.db.commitGate.Unlock()
				}
			}()
			observed := make(chan context.Context, 1)
			restore := setAdmissionWaitObserver(func(owner any, actual context.Context) {
				if owner == &engine.db.commitGate && actual != nil && actual.Value(mvccUpdateContextKey{}) == marker {
					select {
					case observed <- actual:
					default:
					}
				}
			})
			defer restore()
			done := make(chan error, 1)
			go func() { done <- operation(ctx) }()
			select {
			case actual := <-observed:
				require.True(t, actual == ctx, "request identity must reach admission")
			case <-time.After(10 * time.Second):
				t.Fatal("public update never reached cancellable commit admission")
			}
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
			case <-time.After(10 * time.Second):
				t.Fatal("cancellation did not release blocked public update")
			}
			engine.db.commitGate.Unlock()
			held = false
			after, err := engine.ReadMVCCHead(context.Background(), logical)
			require.NoError(t, err)
			require.Zero(t, after.Version.Compare(original.Version), "canceled update must not publish")
			if route == "delete_version" {
				require.True(t, hasOlder())
			}
			require.NoError(t, operation(t.Context()), "healthy retry after cancellation publishes")
			if route == "write_head" {
				after, err = engine.ReadMVCCHead(context.Background(), logical)
				require.NoError(t, err)
				require.Zero(t, after.Version.Compare(target.Version))
			} else {
				require.False(t, hasOlder(), "healthy retry must remove the actual archived version")
			}
		})
	}
}
