package storage

import (
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
)

// BenchmarkWALAcceptedCommit measures public preparation, WAL publication and
// native settlement for unique one-node commands alternating between databases.
// It includes pending-owner registration and removal; correctness is checked
// after timing. Fixed iteration counts bound the retained fixture size.
func BenchmarkWALAcceptedCommit(b *testing.B) {
	b.Cleanup(config.WithWALEnabled())
	native := fixtureowner.Own(b, fixtureowner.Track(b, NewMemoryEngine(), ErrStorageClosed))
	wal, err := NewWAL(b.TempDir(), &WALConfig{SyncMode: "none"})
	if err != nil {
		b.Fatal(err)
	}
	owner := NewWALEngine(native, wal)
	b.Cleanup(func() {
		if err := owner.Close(); err != nil {
			b.Error(err)
		}
	})
	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		namespace := "alpha"
		if i%2 != 0 {
			namespace = "beta"
		}
		id := fmt.Sprintf("commit-%d", i)
		tx, err := native.BeginTransaction(b.Context())
		if err != nil {
			b.Fatal(err)
		}
		if err := tx.SetNamespace(namespace); err != nil {
			b.Fatal(err)
		}
		if _, err := tx.CreateNode(b.Context(), &Node{ID: NodeID(namespace + ":" + id)}); err != nil {
			b.Fatal(err)
		}
		prepared, err := tx.PrepareEntityCommit(b.Context(), EntityCommitRequest{LogicalID: id})
		if err != nil {
			b.Fatal(err)
		}
		receipt, err := owner.SubmitEntityCommit(b.Context(), prepared, nil)
		if err != nil {
			prepared.Abort()
			b.Fatal(err)
		}
		if err := receipt.Wait(b.Context()); err != nil {
			b.Fatal(err)
		}
		prepared.Abort()
	}
	b.StopTimer()
	count, err := native.NodeCount(b.Context())
	if err != nil || count != int64(b.N) {
		b.Fatalf("stored nodes = %d, err = %v; want %d", count, err, b.N)
	}
}
