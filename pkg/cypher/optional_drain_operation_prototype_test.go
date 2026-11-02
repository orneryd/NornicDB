package cypher

import (
	"context"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	nerr "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This fixture observes the real BEGIN admission owner without unwrapping it.
// The underlying engine still owns snapshots, namespace priming and drains.
type optionalDrainProbe struct {
	storage.Engine
	native  *storage.BadgerEngine
	events  []string
	observe bool
	begins  int
	onDrain func(context.Context) error
}

func (p *optionalDrainProbe) EnsureNamespaceMVCC(namespace string) error {
	if p.observe {
		p.events = append(p.events, "prime")
	}
	return p.native.EnsureNamespaceMVCC(namespace)
}
func (p *optionalDrainProbe) FlushWriteBehind(ctx context.Context) error {
	if p.observe {
		p.events = append(p.events, "drain")
	}
	if p.onDrain != nil {
		return p.onDrain(ctx)
	}
	return p.native.FlushWriteBehind(ctx)
}
func (p *optionalDrainProbe) BeginTransaction(ctx context.Context) (*storage.BadgerTransaction, error) {
	p.begins++
	if p.observe {
		p.events = append(p.events, "begin")
	}
	return p.native.BeginTransaction(ctx)
}
func newOptionalDrainProbe(t testing.TB) (*StorageExecutor, *optionalDrainProbe) {
	t.Helper()
	base := newTestMemoryEngine(t)
	probe := &optionalDrainProbe{Engine: base, native: base.BadgerEngine, observe: true}
	return NewStorageExecutor(storage.NewNamespacedEngine(probe, "bound_drain")), probe
}

func TestOptionalDrainRequestRejectionBeforeSnapshot(t *testing.T) {
	for _, kind := range []string{"cancel_in_public_drain", "expired_deadline_public", "expired_deadline_handler"} {
		t.Run(kind, func(t *testing.T) {
			exec, probe := newOptionalDrainProbe(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			want := context.Canceled
			if kind != "cancel_in_public_drain" {
				var stop context.CancelFunc
				ctx, stop = context.WithDeadline(context.Background(), time.Unix(1, 0))
				defer stop()
				want = context.DeadlineExceeded
			}
			type key struct{}
			ctx = context.WithValue(ctx, key{}, "request")
			original := ctx
			probe.onDrain = func(seen context.Context) error {
				require.Equal(t, "request", seen.Value(key{}))
				require.Equal(t, original.Done(), seen.Done())
				if kind == "cancel_in_public_drain" {
					cancel()
				}
				return seen.Err()
			}
			var err error
			if kind == "expired_deadline_handler" {
				_, err = exec.handleBegin(ctx)
			} else {
				_, err = exec.Execute(ctx, "BEGIN", nil)
			}
			require.ErrorIs(t, err, want)
			require.Zero(t, probe.begins)
			require.Nil(t, exec.txContext)
			if kind == "expired_deadline_public" {
				require.Empty(t, probe.events)
			} else {
				require.Equal(t, []string{"prime", "drain"}, probe.events)
				code, _ := nerr.Neo4jStatus(err)
				require.Equal(t, "Neo.DatabaseError.Transaction.TransactionStartFailed", code)
			}
		})
	}
}

func TestOptionalDrainSnapshotLifetimeAndMetadata(t *testing.T) {
	exec, probe := newOptionalDrainProbe(t)
	_, err := exec.Execute(context.Background(), "CREATE (:BoundDrain {value: 7})", nil)
	require.NoError(t, err)
	require.NotContains(t, probe.events, "drain", "implicit admission has no optional explicit-BEGIN drain")
	probe.events = nil
	probe.begins = 0
	request, cancel := context.WithCancel(context.Background())
	defer cancel()
	request = WithTransactionMetadata(request, map[string]any{"request": "drain_only"})
	probe.onDrain = func(seen context.Context) error {
		require.Equal(t, request.Done(), seen.Done())
		return probe.native.FlushWriteBehind(seen)
	}
	_, err = exec.Execute(request, "BEGIN", nil)
	require.NoError(t, err)
	require.Equal(t, []string{"prime", "drain", "begin"}, probe.events)
	require.Equal(t, 1, probe.begins)
	// Existing explicit BEGIN opens with its independent background lifetime;
	// the public BEGIN boundary still applies the original request metadata.
	require.Equal(t, map[string]any{"request": "drain_only"}, exec.txContext.tx.(*storage.BadgerTransaction).GetMetadata())
	cancel()
	result, err := exec.Execute(context.Background(), "MATCH (n:BoundDrain) RETURN n.value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

func TestOptionalDrainAcknowledgedBufferedWrite(t *testing.T) {
	base, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour}))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "buffered_ack"))
	created, err := exec.Execute(context.Background(), "CREATE (:BoundDrain {value: 11})", nil)
	require.NoError(t, err)
	require.Equal(t, 1, created.Stats.NodesCreated)
	_, err = exec.Execute(context.Background(), "BEGIN", nil)
	require.NoError(t, err)
	result, err := exec.Execute(context.Background(), "MATCH (n:BoundDrain) RETURN n.value", nil)
	require.NoError(t, err)
	require.Equal(t, [][]interface{}{{int64(11)}}, result.Rows)
	_, err = exec.Execute(context.Background(), "ROLLBACK", nil)
	require.NoError(t, err)
}

func BenchmarkOptionalDrainPublicBeginRoundTrip(b *testing.B) {
	exec, probe := newOptionalDrainProbe(b)
	probe.observe = false
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			b.Fatal(err)
		}
		if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
			b.Fatal(err)
		}
	}
}
