package storage

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCountNodesWithLabelCanceledMetadataAdmission(t *testing.T) {
	for _, route := range []string{"native", "namespace", "wal_namespace", "plain_scan", "wal_scan", "namespace_scan", "native_buffer", "namespace_buffer"} {
		t.Run(route, func(t *testing.T) {
			engine := newReadSnapshotTestEngine(t, true)
			// Acknowledged writes stay in the write-behind buffer, so every
			// route below counts committed buffer state rather than a landed
			// generation.
			create := func(namespace string, node *Node) {
				tx, err := engine.BeginTransaction(t.Context())
				require.NoError(t, err)
				defer tx.Rollback()
				require.NoError(t, tx.SetImplicit(true))
				require.NoError(t, tx.SetNamespace(namespace))
				_, err = tx.CreateNode(t.Context(), node)
				require.NoError(t, err)
				require.NoError(t, tx.Commit())
			}
			create("alpha", &Node{ID: "alpha:a", Labels: []string{"Person"}})
			create("beta", &Node{ID: "beta:b", Labels: []string{"Person"}})
			create("alpha", &Node{ID: "alpha:other", Labels: []string{"Other"}})
			var store Engine = engine
			expected := int64(2)
			var admissionLock sync.Locker = &engine.labelCountWriteMu
			var admissionOwner any = &engine.labelCountWriteMu
			// The buffer routes keep the public named-count metadata path and
			// differ only in which wait the request is held at: the buffer's
			// own read admission rather than the engine's count-family hold.
			bufferCountRoute := route == "native_buffer" || route == "namespace_buffer"
			bufferedAdmission := bufferCountRoute
			if route == "plain_scan" || route == "wal_scan" || route == "namespace_scan" {
				store = NewTracedEngine(engine)
				bufferedAdmission = true
			}
			if bufferedAdmission {
				admissionLock, admissionOwner = &engine.writeBehind.mu, &engine.writeBehind.mu
			}
			if route == "namespace_scan" {
				store = NewNamespacedEngine(store, "alpha")
				expected = 1
			}
			if route == "wal_scan" {
				wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, wal.Close()) })
				store = NewWALEngine(store, wal)
			}
			if route == "namespace" || route == "namespace_buffer" {
				store = NewNamespacedEngine(engine, "alpha")
				expected = 1
			}
			if route == "wal_namespace" {
				wal, err := NewWAL(t.TempDir(), DefaultWALConfig())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, wal.Close()) })
				store = NewNamespacedEngine(NewWALEngine(engine, wal), "alpha")
				expected = 1
			}
			type countContextKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), countContextKey{}, "count-request"))
			waiting := make(chan context.Context, 1)
			t.Cleanup(setAdmissionWaitObserver(func(owner any, seen context.Context) {
				if owner == admissionOwner {
					select {
					case waiting <- seen:
					default:
					}
				}
			}))
			admissionLock.Lock()
			var unlockOnce sync.Once
			unlock := func() { unlockOnce.Do(admissionLock.Unlock) }
			t.Cleanup(unlock)
			if bufferCountRoute {
				// The merge this read is waiting to perform has real
				// acknowledged counts: the buffer still owns them.
				require.False(t, engine.writeBehind.active.countsApplied.Load(), "buffer must still own the acknowledged counts")
				var pending int64
				for _, commit := range engine.writeBehind.active.commits {
					for key, delta := range commit.labelDeltas {
						if key.label != "Person" || route == "namespace_buffer" && key.namespace != "alpha" {
							continue
						}
						pending += delta
					}
				}
				require.Equal(t, expected, pending, "the blocked merge must have an acknowledged buffer delta to add")
			}
			type outcome struct {
				count int64
				err   error
			}
			done := make(chan outcome, 1)
			joined := make(chan struct{})
			t.Cleanup(func() {
				cancel()
				unlock()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("count worker did not join")
				}
			})
			go func() {
				defer close(joined)
				count, err := CountNodesWithLabel(ctx, store, "Person")
				done <- outcome{count, err}
			}()
			var admission context.Context
			select {
			case admission = <-waiting:
			case <-time.After(5 * time.Second):
				t.Fatal("public count did not reach held native count admission")
			}
			cancel()
			select {
			case got := <-done:
				require.ErrorIs(t, got.err, context.Canceled)
				require.Zero(t, got.count, "canceled metadata admission returns no count")
			case <-time.After(time.Second):
				t.Fatal("public count discarded cancellation while waiting for label metadata")
			}
			require.True(t, admission == ctx, "count admission must retain actual request identity and values")
			require.Equal(t, "count-request", admission.Value(countContextKey{}))
			unlock()
			select {
			case <-joined:
			case <-time.After(5 * time.Second):
				t.Fatal("count caller did not join after result")
			}
			count, err := CountNodesWithLabel(t.Context(), store, "Person")
			require.NoError(t, err)
			require.Equal(t, expected, count, "healthy retry preserves native/global or namespace truth")
			count, err = CountNodesWithLabel(t.Context(), store, "Missing")
			require.NoError(t, err)
			require.Zero(t, count)
		})
	}
}
