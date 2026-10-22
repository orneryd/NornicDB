package cypher

import (
	"context"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// storageScan is how the executor consumes a storage stream or visitor: a
// storage call that hands it a population one item at a time through a
// callback. Every such callback starts by observing its scan (observe, or
// canceled for a callback that answers with a bool), which stops the storage
// iteration once the statement is canceled or expired; the adapter below then
// returns the statement's error. The storage stream and visitor APIs are
// called only from this file, and only with callbacks that start that way
// (TestStorageStreamCallSites).
//
// A scan observes its statement at the owner's stride: its first item and
// then once per rowCancellationInterval items, counted by the same
// materializedRowCheckpoint the owner's inner passes use, so an item costs an
// increment. The checkpoint comes from a free list (newStorageScan; the
// scan's function closes it), so a scan allocates nothing, and the callback
// holds a copy of the scan (one pointer). The free list is a buffered
// channel rather than a sync.Pool: a pool drops its entries at every GC and
// then allocates them, and its per-processor slots, again.
type storageScan struct {
	checkpoint *materializedRowCheckpoint
}

// storageScanCheckpoints holds the checkpoints of finished scans, up to the
// scans that can run at once; a scan beyond that allocates its own.
var storageScanCheckpoints = make(chan *materializedRowCheckpoint, 1024)

func newStorageScan(ctx context.Context) storageScan {
	var checkpoint *materializedRowCheckpoint
	select {
	case checkpoint = <-storageScanCheckpoints:
	default:
		checkpoint = new(materializedRowCheckpoint)
	}
	*checkpoint = materializedRowCheckpoint{ctx: ctx}
	return storageScan{checkpoint: checkpoint}
}

// ctx is the statement the scan observes, held by its checkpoint.
func (s storageScan) ctx() context.Context { return s.checkpoint.ctx }

// close returns the scan's checkpoint once its storage calls are over.
func (s storageScan) close() {
	*s.checkpoint = materializedRowCheckpoint{}
	select {
	case storageScanCheckpoints <- s.checkpoint:
	default:
	}
}

// observe is the first statement of an error-returning scan callback.
func (s storageScan) observe() error {
	if s.checkpoint.due() {
		if err := s.checkpoint.ctx.Err(); err != nil {
			recordExpressionFailure(s.checkpoint.ctx, err)
			return err
		}
	}
	return nil
}

// canceled is the first statement of a scan callback that returns false to
// stop its scan, or to decline the item when the storage call has no stop.
func (s storageScan) canceled() bool {
	return s.observe() != nil
}

// result is the scan's outcome: the statement's error once it is canceled or
// expired, whatever the storage call returned, else that call's error.
func (s storageScan) result(err error) error {
	if ctxErr := s.ctx().Err(); ctxErr != nil {
		recordExpressionFailure(s.ctx(), ctxErr)
		return ctxErr
	}
	return err
}

func streamNodesByLabelProjected(scan storageScan, reader storage.ProjectedLabelNodeReader, label string, properties []string, visit func(*storage.Node) error) error {
	return scan.result(reader.StreamNodesByLabelProjected(label, properties, visit))
}

func streamNodesWithOptions(scan storageScan, store storage.Engine, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	return scan.result(store.StreamNodesWithOptions(scan.ctx(), opts, visit))
}

func streamNodes(scan storageScan, streamer storage.StreamingEngine, visit func(*storage.Node) error) error {
	return scan.result(streamer.StreamNodes(scan.ctx(), visit))
}

func streamNodesWithFallback(scan storageScan, engine storage.Engine, chunkSize int, visit func(*storage.Node) error) error {
	return scan.result(storage.StreamNodesWithFallback(scan.ctx(), engine, chunkSize, visit))
}

func streamEdgesWithFallback(scan storageScan, engine storage.Engine, chunkSize int, visit func(*storage.Edge) error) error {
	return scan.result(storage.StreamEdgesWithFallback(scan.ctx(), engine, chunkSize, visit))
}

func visitPropertyIndexGroupsInRange(scan storageScan, schema *storage.SchemaManager, label, property string, descending bool, bounds storage.PropertyIndexBounds, visit func([]storage.NodeID) bool) (bool, error) {
	found := schema.VisitPropertyIndexGroupsInRange(label, property, descending, bounds, visit)
	return found, scan.result(nil)
}

// matchEdgesBetween's storage read takes no context and has no early stop:
// once the statement is canceled its callback declines every further
// relationship, and the call returns the statement's error.
func matchEdgesBetween(scan storageScan, engine storage.Engine, startID, endID storage.NodeID, edgeType string, properties []string, match func(*storage.Edge) bool) ([]*storage.Edge, error) {
	edges, err := storage.MatchEdgesBetween(engine, startID, endID, edgeType, properties, match)
	if err = scan.result(err); err != nil {
		return nil, err
	}
	return edges, nil
}

func visitWALEntriesAfterFromDir(scan storageScan, walDir string, afterSeq uint64, visit func(storage.WALEntry) error) error {
	return scan.result(storage.VisitWALEntriesAfterFromDir(walDir, afterSeq, visit))
}
