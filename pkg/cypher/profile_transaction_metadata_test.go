package cypher

// tx.setMetaData validates caller metadata against the unchanged
// 2048-byte limit and retains an owned copy of it. Its size check formats
// every caller value and its retention copies them, so the statement invokes
// caller-controlled methods: a fmt.Stringer, a fmt.Formatter, or the supported
// CallerCopy protocol. That caller code can read storage itself, and no
// stdlib formatting dispatch reports such a read to the executor. A PROFILE of
// the statement therefore keeps the procedure callback explicitly unobserved
// instead of certifying zero database reads.
//
// The controls below run the public statement with values whose own methods
// read a test-owned store and count that read, and hold the plan to its
// unobserved callback.

import (
	"context"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// metadataCallerValue carries the text the statement measures and the caller's
// own work that runs when the statement invokes one of its value methods.
type metadataCallerValue struct {
	text     string
	onInvoke func()
}

func (v metadataCallerValue) measuredText() string {
	v.onInvoke()
	return v.text
}

// metadataStringerValue answers the size check's formatting through its own
// String method.
type metadataStringerValue struct{ metadataCallerValue }

func (v metadataStringerValue) String() string { return v.measuredText() }

// metadataFormatterValue answers it through its own Format method, which
// fmt.Sprint dispatches before String.
type metadataFormatterValue struct{ metadataCallerValue }

func (v metadataFormatterValue) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, v.measuredText())
}

// metadataCopierValue hands metadata retention its own copy, and performs the
// caller's work while that copy is made.
type metadataCopierValue struct{ metadataCallerValue }

func (v metadataCopierValue) CallerCopy() any {
	v.onInvoke()
	return v
}

// TestProfileTxSetMetadataLeavesCallerValueReadsUnobserved runs the public
// statement under PROFILE with a caller value whose own method reads the
// test-owned store. The read happens, and the plan reports the procedure
// callback as unobserved instead of certifying the statement's database access
// as a measured zero.
func TestProfileTxSetMetadataLeavesCallerValueReadsUnobserved(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, kind := range []string{"ordinary", "stringer", "formatter", "copy"} {
			t.Run(kind, func(t *testing.T) {
				store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx_metadata_caller_reads")
				exec := NewStorageExecutor(store)
				const nodeID = storage.NodeID("metadata-caller-read")
				_, err := store.CreateNode(t.Context(), &storage.Node{
					ID:         nodeID,
					Labels:     []string{"MetadataCallerRead"},
					Properties: map[string]interface{}{"name": "caller"},
				})
				require.NoError(t, err)

				var reads, refusedReads atomic.Int64
				value := metadataCallerValue{text: "caller text", onInvoke: func() {
					node, err := store.GetNode(t.Context(), nodeID)
					if err == nil && node != nil && node.ID == nodeID && node.Properties["name"] == "caller" {
						reads.Add(1)
					} else {
						refusedReads.Add(1)
					}
				}}
				metadata := map[string]interface{}{"app": "test-app"}
				switch kind {
				case "stringer":
					metadata["value"] = metadataStringerValue{value}
				case "formatter":
					metadata["value"] = metadataFormatterValue{value}
				case "copy":
					metadata["value"] = metadataCopierValue{value}
				}

				result, err := exec.Execute(t.Context(),
					"PROFILE CALL tx.setMetaData($metadata) RETURN 1 AS one",
					map[string]interface{}{"metadata": metadata})
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{int64(1)}}, result.Rows)

				require.Zero(t, refusedReads.Load(), "all caller reads reached the test-owned stored value")
				if kind == "ordinary" {
					require.Zero(t, reads.Load(), "ordinary metadata invokes no caller value method")
				} else {
					require.Positive(t, reads.Load(), "the caller value's own method read the test-owned store")
				}

				plan := executionPlanFromResult(t, result)
				require.Contains(t, plan.Unobserved, "database reads: procedure callback: tx.setMetaData",
					"the plan reports the procedure callback instead of certifying its complete access")
				require.False(t, plan.TotalDBHitsObserved, "the statement's database access cannot be certified")
				require.Zero(t, plan.TotalDBHits, "the caller value's read is not a measured database hit")
			})
		}
	})
}

// TestTxSetMetadataRetainsCallerValues keeps the statement's metadata contract:
// caller values are retained with their own types, and retention dispatches the
// caller's CallerCopy for the value that owns one.
func TestTxSetMetadataRetainsCallerValues(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx_metadata_retention")
	exec := NewStorageExecutor(store)
	_, err := exec.Execute(t.Context(), "BEGIN", nil)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })

	var copies atomic.Int64
	metadata := map[string]interface{}{
		"app":    "test-app",
		"userId": int64(12345),
		"copy":   metadataCopierValue{metadataCallerValue{text: "copied", onInvoke: func() { copies.Add(1) }}},
	}
	result, err := exec.Execute(t.Context(), "CALL tx.setMetaData($metadata)",
		map[string]interface{}{"metadata": metadata})
	require.NoError(t, err)
	require.Empty(t, result.Columns)
	require.Empty(t, result.Rows)

	tx, ok := exec.txContext.tx.(*storage.BadgerTransaction)
	require.True(t, ok, "the explicit transaction is the metadata owner")
	retained := tx.GetMetadata()
	require.Equal(t, "test-app", retained["app"])
	require.Equal(t, int64(12345), retained["userId"])
	retainedCopy, ok := retained["copy"].(metadataCopierValue)
	require.True(t, ok, "retention keeps the caller's own value type: %T", retained["copy"])
	require.Equal(t, "copied", retainedCopy.text)
	require.Positive(t, copies.Load(), "retention dispatched the caller's CallerCopy")
}

// TestTxSetMetadataMeasuresCallerMethodTextAgainstTheLimit keeps the unchanged
// size accounting: the text a caller value's own method answers is what the
// 2048-byte limit measures.
func TestTxSetMetadataMeasuresCallerMethodTextAgainstTheLimit(t *testing.T) {
	for _, kind := range []string{"stringer", "formatter"} {
		t.Run(kind, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "tx_metadata_limit"))
			var measured atomic.Int64
			value := metadataCallerValue{
				text:     strings.Repeat("x", 2100),
				onInvoke: func() { measured.Add(1) },
			}
			var metadataValue interface{}
			if kind == "stringer" {
				metadataValue = metadataStringerValue{value}
			} else {
				metadataValue = metadataFormatterValue{value}
			}
			_, err := exec.Execute(t.Context(), "CALL tx.setMetaData($metadata)",
				map[string]interface{}{"metadata": map[string]interface{}{"data": metadataValue}})
			require.Error(t, err)
			require.Contains(t, err.Error(), "too large")
			require.Positive(t, measured.Load(), "the size check measured the text the value's own method answered")
		})
	}
}
