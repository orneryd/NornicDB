package storage

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

// This is an allocation count for four affected entrances, not a timing or
// speed comparison. Setup and assertions stay outside the measured calls.
func TestLabelReaderAllocationFootprint(t *testing.T) {
	b := createTestBadgerEngine(t)
	for i := 0; i < 8; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("test:%d", i)), Labels: []string{"Target"}, Properties: map[string]interface{}{"a": int64(i)}})
		require.NoError(t, err)
	}
	n := NewNamespacedEngine(b, "test")
	var readErr error
	for _, tc := range []struct {
		name string
		read func()
	}{
		{"id_first", func() { readErr = n.ForEachNodeIDByLabel("Target", func(NodeID) bool { return false }) }},
		{"absent", func() { _, readErr = b.GetFirstNodeByLabelInScope("test:", "Missing") }},
		{"snapshot_first", func() {
			tx, err := b.BeginTransaction()
			if err != nil {
				readErr = err
				return
			}
			defer tx.Rollback()
			if err = tx.SetNamespace("test"); err != nil {
				readErr = err
				return
			}
			readErr = tx.StreamNodesByLabelProjected("Target", []string{"a"}, func(*Node) error { return ErrIterationStopped })
			if readErr == ErrIterationStopped {
				readErr = nil
			}
		}},
		{"transaction_index", func() {
			tx, err := b.BeginTransaction()
			if err != nil {
				readErr = err
				return
			}
			defer tx.Rollback()
			_, readErr = tx.GetNodesByLabel("Target")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			allocs := testing.AllocsPerRun(20, tc.read)
			require.NoError(t, readErr)
			t.Logf("%s: %.0f allocations/call", tc.name, allocs)
		})
	}
	t.Logf("ready numeric reader: %d bytes", unsafe.Sizeof(nodeIndexReader{}))
}
