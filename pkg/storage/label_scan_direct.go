package storage

import (
	"context"
	"strings"

	"github.com/dgraph-io/badger/v4"
)

// labelScanPointLookups is how many of a label's nodes a scan reads with one
// record lookup each before it may switch to a single pass over the node
// records. A scan stopped within this prefix (LIMIT) stays on point lookups.
const labelScanPointLookups = 1024

// labelScanMaxBuffered bounds the one-pass tail. Larger scans fall back to
// streaming point reads rather than retaining every remaining node body.
const labelScanMaxBuffered = 1024

// readNodeRecordsInOnePass calls read with the stored record of each of ids
// that exists, walking scope's node records once ("" is every database)
// instead of looking each record up. read's order is the records' key order;
// an error from read stops the walk and is returned. The walk is the engine's
// one node-row scan; read keeps its own versioned record reader.
func (b *BadgerEngine) readNodeRecordsInOnePass(txn *badger.Txn, scope string, ids []NodeID, read func(NodeID, *badger.Item) error) error {
	wanted := make(map[NodeID]struct{}, len(ids))
	for _, id := range ids {
		wanted[id] = struct{}{}
	}
	if len(wanted) == 0 {
		return nil
	}
	rows := nodeRowScan{scope: rowsWithPrefix(scope), overlay: noOverlay[NodeID](), decode: decodeErrorsFail, visibility: scanStored,
		hooks: rowHooks[NodeID]{before: func(id NodeID) (bool, error) {
			if len(wanted) == 0 {
				return true, ErrIterationStopped // every wanted record was read
			}
			_, ok := wanted[id]
			return !ok, nil
		}},
		item: func(id NodeID, item *badger.Item) (bool, error) {
			delete(wanted, id)
			return true, read(id, item)
		}}
	return b.streamNodesWithOptionsInTxn(context.Background(), txn, rows, StreamNodesOptions{}, func(*Node) error { return nil })
}

// labelCoversScope reports whether label is on at least half of the nodes in
// scope ("" is every database), by the stored node and label counts. Above
// that share a single pass over the node records costs less than a label
// index walk with one record lookup per node.
func (b *BadgerEngine) labelCoversScope(scope, label string) bool {
	var total, labelled int64
	var err error
	if scope == "" {
		total = b.nodeCount.Load()
		labelled, err = b.NodeCountByLabel(label)
	} else {
		if total, err = b.NodeCountByPrefix(scope); err == nil {
			namespace, _, _ := strings.Cut(scope, ":")
			labelled, err = b.NodeCountByLabelInNamespace(namespace, label)
		}
	}
	return err == nil && total > 0 && labelled*2 >= total
}
