package cypher

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHotPathTrace_AllMarkers(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	exec.resetHotPathTrace()
	exec.setFabricBatchedApplyRowsUsed(false)
	exec.markOuterIndexTopKUsed()
	exec.setFabricBatchedApplyRowsUsed(true)
	exec.markSimpleMatchLimitFastPathUsed()
	exec.markCompoundQueryFastPathUsed()
	exec.markTraversalStartSeedTopKUsed()
	exec.markTraversalEndSeedTopKUsed()
	exec.markUnwindSimpleMergeBatchUsed()
	exec.markUnwindMergeChainBatchUsed()
	exec.markUnwindRelationshipMergeBatchUsed()
	exec.markUnwindFixedChainLinkBatchUsed()
	exec.markUnwindMultiMatchCreateBatchUsed()
	exec.markCallTailTraversalFastPathUsed()
	exec.markMergeSchemaLookupUsed()
	exec.markMergeScanFallbackUsed()

	trace := exec.LastHotPathTrace()
	require.True(t, trace.OuterIndexTopK)
	require.True(t, trace.FabricBatchedApplyRows)
	require.True(t, trace.SimpleMatchLimitFastPath)
	require.True(t, trace.CompoundQueryFastPath)
	require.True(t, trace.TraversalStartSeedTopK)
	require.True(t, trace.TraversalEndSeedTopK)
	require.True(t, trace.UnwindSimpleMergeBatch)
	require.True(t, trace.UnwindMergeChainBatch)
	require.True(t, trace.UnwindRelationshipMergeBatch)
	require.True(t, trace.UnwindFixedChainLinkBatch)
	require.True(t, trace.UnwindMultiMatchCreateBatch)
	require.True(t, trace.CallTailTraversalFastPath)
	require.True(t, trace.MergeSchemaLookupUsed)
	require.True(t, trace.MergeScanFallbackUsed)
}
