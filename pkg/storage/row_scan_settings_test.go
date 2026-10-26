package storage

import (
	"context"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// A committed-row scan with a setting left out fails instead of choosing a
// behaviour.
func TestRowScanRequiresEverySetting(t *testing.T) {
	engine := newRowScanFixture(t, false)
	complete := edgeRowScan{scope: allRows(), overlay: noOverlay[EdgeID](), decode: decodeErrorsSkip, visibility: scanVisible}
	for name, scan := range map[string]edgeRowScan{
		"scope":      {overlay: complete.overlay, decode: complete.decode, visibility: complete.visibility},
		"overlay":    {scope: complete.scope, decode: complete.decode, visibility: complete.visibility},
		"decode":     {scope: complete.scope, overlay: complete.overlay, visibility: complete.visibility},
		"visibility": {scope: complete.scope, overlay: complete.overlay, decode: complete.decode},
	} {
		err := engine.db.View(func(txn *badger.Txn) error {
			_, err := engine.scanEdgeRows(context.Background(), txn, scan, func(*Edge) error { return nil })
			return err
		})
		require.ErrorIs(t, err, errRowScanSettings, "edge scan without %s", name)
	}
	nodeComplete := nodeRowScan{scope: allRows(), overlay: noOverlay[NodeID](), decode: decodeErrorsSkip, visibility: scanVisible}
	for name, rows := range map[string]nodeRowScan{
		"scope":      {overlay: nodeComplete.overlay, decode: nodeComplete.decode, visibility: nodeComplete.visibility},
		"overlay":    {scope: nodeComplete.scope, decode: nodeComplete.decode, visibility: nodeComplete.visibility},
		"decode":     {scope: nodeComplete.scope, overlay: nodeComplete.overlay, visibility: nodeComplete.visibility},
		"visibility": {scope: nodeComplete.scope, overlay: nodeComplete.overlay, decode: nodeComplete.decode},
	} {
		err := engine.db.View(func(txn *badger.Txn) error {
			return engine.streamNodesWithOptionsInTxn(context.Background(), txn, rows, StreamNodesOptions{}, func(*Node) error { return nil })
		})
		require.ErrorIs(t, err, errRowScanSettings, "node scan without %s", name)
	}
}
