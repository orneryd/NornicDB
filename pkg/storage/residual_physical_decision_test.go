package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

// Observe the actual physical record and revocation inside the callback frame.
// The large route's intent deletion identifies its final physical action even
// when a body was already written by one of its invisible preparation batches.
func TestResidualDecisionContainsPhysicalAction(t *testing.T) {
	for _, route := range []string{"ordinary", "large", "ordinary-conflict"} {
		t.Run(route, func(t *testing.T) {
			b, err := fixtureowner.OwnResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), LowMemory: route == "large"}))
			require.NoError(t, err)
			t.Cleanup(func() { _ = b.Close() })
			_, err = b.CreateNode(&Node{ID: "test:physical", Properties: map[string]any{"v": int64(1)}})
			require.NoError(t, err)
			tx, err := b.BeginTransaction()
			require.NoError(t, err)
			t.Cleanup(func() { _ = tx.Rollback() })
			require.NoError(t, tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: "test:physical", Properties: map[string]any{"v": int64(2)}}, Policy: ClientNodeEdits}))
			if route == "large" {
				stageLargeCreate(t, tx, 6000)
			}
			if route == "ordinary-conflict" {
				overwriteRaw(t, b, nodeKey("test:physical"))
			}
			physical := func(key []byte) ([]byte, uint64, error) {
				read := b.db.DB.NewTransactionAt(b.db.DB.MaxVersion(), false)
				defer read.Discard()
				item, err := read.Get(key)
				if err != nil {
					return nil, 0, err
				}
				value, err := item.ValueCopy(nil)
				return value, item.Version(), err
			}
			calls, boundaries := 0, 0
			t.Cleanup(setDurableCacheHook(func(uint64) { boundaries++ }))
			var observed error
			ctx := WithPublicationDecision(context.Background(), func(_ context.Context, _ PublicationIdentity, action func() error) error {
				calls++
				key := nodeKey("test:physical")
				if route == "large" {
					key = largeCommitIntentKey
				}
				before, version, readErr := physical(key)
				cut := b.db.oracle.published.Load()
				b.durableCaches.mu.Lock()
				floorBefore := b.durableCaches.nodeFloor("test:physical")
				b.durableCaches.mu.Unlock()
				boundariesBefore := boundaries
				actionErr := action()
				after, nextVersion, afterErr := physical(key)
				b.durableCaches.mu.Lock()
				floorAfter := b.durableCaches.nodeFloor("test:physical")
				b.durableCaches.mu.Unlock()
				if readErr != nil || b.db.oracle.published.Load() != cut {
					observed = fmt.Errorf("physical frame lost its prior record or exposed the unfinished oracle: %v", readErr)
				}
				if route == "ordinary-conflict" {
					if !errors.Is(actionErr, badger.ErrConflict) || !bytes.Equal(before, after) || nextVersion != version || boundaries != boundariesBefore || floorAfter != floorBefore {
						observed = fmt.Errorf("failed physical action changed its record or issued revocation: %v", actionErr)
					}
				} else if actionErr != nil || boundaries != boundariesBefore+1 || floorAfter < b.db.DB.MaxVersion() {
					observed = fmt.Errorf("physical action/revocation did not complete inside the decision: %v", actionErr)
				} else if route == "large" && !errors.Is(afterErr, badger.ErrKeyNotFound) {
					observed = fmt.Errorf("large final action did not delete its durable intent: %v", afterErr)
				} else if route != "large" && (afterErr != nil || nextVersion <= version || bytes.Equal(before, after)) {
					observed = fmt.Errorf("ordinary physical body did not change inside the decision: %v", afterErr)
				}
				return actionErr
			})
			err = tx.CommitContext(ctx)
			if route == "ordinary-conflict" {
				require.ErrorIs(t, err, badger.ErrConflict)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, 1, calls)
			require.NoError(t, observed)
			stored, err := b.GetStoredNode("test:physical")
			require.NoError(t, err)
			want := int64(2)
			if route == "ordinary-conflict" {
				want = 1
			}
			require.Equal(t, want, stored.Properties["v"])
		})
	}
}
