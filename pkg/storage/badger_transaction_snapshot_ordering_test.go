// SPDX-License-Identifier: MIT
package storage

import (
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTransactionSnapshotPhysicalStateDoesNotAdvancePastLogicalBoundary(t *testing.T) {
	engine := createTestBadgerEngine(t)
	nodeID := NodeID("test:snapshot-ordering")
	_, err := engine.CreateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"revision": 0}})
	require.NoError(t, err)

	engine.mvccByNamespaceMu.Lock()
	for i := 0; i < 4_096; i++ {
		engine.mvccByNamespace[fmt.Sprintf("snapshot-ordering-%d", i)] = &namespaceMVCCState{}
	}
	engine.mvccByNamespaceMu.Unlock()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopWriter := func() { stopOnce.Do(func() { close(stop) }) }
	t.Cleanup(stopWriter)
	writerErr := make(chan error, 1)
	go func() {
		for revision := 1; ; revision++ {
			select {
			case <-stop:
				writerErr <- nil
				return
			default:
			}
			if err := engine.UpdateNode(&Node{ID: nodeID, Properties: map[string]interface{}{"revision": revision}}); err != nil {
				writerErr <- err
				return
			}
			runtime.Gosched()
		}
	}()

	physicalAdvancedPastLogical := false
	for attempt := 0; attempt < 256 && !physicalAdvancedPastLogical; attempt++ {
		tx, beginErr := engine.BeginTransaction()
		require.NoError(t, beginErr)
		require.NoError(t, tx.SetNamespace("test"))

		head, headErr := engine.loadNodeMVCCHeadInTxn(tx.snapshotTx, nodeID)
		require.NoError(t, headErr)
		physicalAdvancedPastLogical = head.Version.CommitSequence > tx.readTS.CommitSequence
		require.NoError(t, tx.Rollback())
		runtime.Gosched()
	}
	stopWriter()
	require.NoError(t, <-writerErr)

	require.False(t, physicalAdvancedPastLogical,
		"the pinned physical snapshot must not include a commit newer than its logical namespace boundary")
}
