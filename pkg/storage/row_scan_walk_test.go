// Author: #1683 lane (Claude), 2026-10-31 (NOR-148).
package storage

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// scanRows is the walk of both entity scans: the scope, the resume key, a stop,
// an error and cancellation each end it the same way for either kind.
func TestScanRowsScopeResumeStopAndCancellation(t *testing.T) {
	b := newTestEngine(t)
	for _, id := range []NodeID{"walk:a", "walk:b", "walk:c", "zzz:d"} {
		_, err := b.CreateNode(&Node{ID: id})
		require.NoError(t, err)
	}
	require.NoError(t, b.FlushWriteBehind(context.Background()))
	prefix := append([]byte{prefixNode}, "walk:"...)
	walk := func(ctx context.Context, start []byte, stopAt int, failure error) (ids []string, stopped bool, err error) {
		err = b.withView(func(txn *badger.Txn) error {
			var walkErr error
			stopped, walkErr = scanRows(ctx, txn, prefix, start, func(item *badger.Item) error {
				ids = append(ids, string(item.Key()[1:]))
				if len(ids) == stopAt {
					if failure != nil {
						return failure
					}
					return ErrIterationStopped
				}
				return nil
			})
			return walkErr
		})
		return ids, stopped, err
	}

	ids, stopped, err := walk(context.Background(), nil, 0, nil)
	require.NoError(t, err)
	require.False(t, stopped)
	require.Equal(t, []string{"walk:a", "walk:b", "walk:c"}, ids)

	ids, _, err = walk(context.Background(), nodeKey("walk:b"), 0, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"walk:b", "walk:c"}, ids)

	ids, stopped, err = walk(context.Background(), nil, 2, nil)
	require.NoError(t, err)
	require.True(t, stopped)
	require.Equal(t, []string{"walk:a", "walk:b"}, ids)

	failure := errors.New("visit failed")
	_, stopped, err = walk(context.Background(), nil, 1, failure)
	require.ErrorIs(t, err, failure)
	require.False(t, stopped)

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ids, _, err = walk(cancelled, nil, 0, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, ids)
}

// Every part of a node scan is classified here. A hook that reads a row by ID must
// make the scan build the ID, or the scan would silently skip it; adding any field
// or hook to nodeRowScan or rowHooks fails this test until it is classified.
func TestNodeRowScanEveryIDHookChoosesByID(t *testing.T) {
	needsID := map[string]func(*nodeRowScan){
		"overlay":    func(s *nodeRowScan) { s.overlay = overlayReplaces(map[NodeID]bool{"replaced": true}) },
		"body":       func(s *nodeRowScan) { s.body = func(NodeID, []byte) (bool, error) { return false, nil } },
		"item":       func(s *nodeRowScan) { s.item = func(NodeID, *badger.Item) (bool, error) { return false, nil } },
		"cachedNode": func(s *nodeRowScan) { s.cachedNode = func(NodeID) *Node { return nil } },
	}
	hooksNeedID := map[string]func(*rowHooks[NodeID]){
		"before": func(h *rowHooks[NodeID]) { h.before = func(NodeID) (bool, error) { return false, nil } },
	}
	// These use no ID before decoding: settings, or code that reads the row's key itself.
	noID := map[string]bool{"scope": true, "decode": true, "visibility": true, "fullItem": true, "decoded": true}
	hooksNoID := map[string]bool{"decodeFailure": true}

	scanType := reflect.TypeOf(nodeRowScan{})
	for i := range scanType.NumField() {
		name := scanType.Field(i).Name
		if name == "hooks" || noID[name] {
			continue
		}
		set, ok := needsID[name]
		require.True(t, ok, "classify nodeRowScan.%s", name)
		var scan nodeRowScan
		set(&scan)
		require.True(t, scan.choosesByID(), "nodeRowScan.%s", name)
	}
	hooksType := reflect.TypeOf(rowHooks[NodeID]{})
	for i := range hooksType.NumField() {
		name := hooksType.Field(i).Name
		if hooksNoID[name] {
			continue
		}
		set, ok := hooksNeedID[name]
		require.True(t, ok, "classify rowHooks.%s", name)
		var scan nodeRowScan
		set(&scan.hooks)
		require.True(t, scan.choosesByID(), "rowHooks.%s", name)
	}
	require.False(t, nodeRowScan{scope: allRows(), overlay: noOverlay[NodeID](), decode: decodeErrorsFail, visibility: scanStored, fullItem: true}.choosesByID())
}
