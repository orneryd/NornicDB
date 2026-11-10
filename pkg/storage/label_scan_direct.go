package storage

import (
	"context"
	"strings"
	"sync"

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
// an error from read stops the walk and is returned.
// Author: storage-format-owners lane (Codex), 1 November 2026.
func readNodeRecordsInOnePass(ctx context.Context, txn *badger.Txn, scope string, ids []NodeID, read func(NodeID, *badger.Item) error) error {
	wanted := make(map[NodeID]struct{}, len(ids))
	var cancel bulkReadCancellation
	for _, id := range ids {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		wanted[id] = struct{}{}
	}
	prefix := []byte{prefixNode}
	if scope != "" {
		prefix = nodeKey(NodeID(scope))
	}
	records := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
	defer records.Close()
	for records.Rewind(); records.Valid() && len(wanted) > 0; records.Next() {
		if err := cancel.check(ctx); err != nil {
			return err
		}
		item := records.Item()
		nodeID := NodeID(item.Key()[1:])
		if _, ok := wanted[nodeID]; !ok {
			continue
		}
		delete(wanted, nodeID)
		if err := read(nodeID, item); err != nil {
			return err
		}
	}
	return nil
}

// recentLabelScans remembers the labels of an engine's last few filtered
// scans, and for a label one of them walked whole, the label's nodes.
type recentLabelScans struct {
	mu sync.Mutex
	// scans holds the newest first.
	scans [4]recentLabelScan
}

// recentLabelScan is one remembered label. ids, once a scan with visibility
// has walked the whole label index, is the label's nodes in index order as
// that scan read them from a view at source. It is a copy of that walk, never
// an owner of label membership (BadgerEngine.rememberedLabelNodes).
type recentLabelScan struct {
	scope, label string
	ids          []NodeID
	source       uint64
	visibility   readVisibility
	// refused, when not 0, is the view of the last scan whose images of the
	// label the node read cache refused (BadgerEngine.labelImagesRefused).
	refused uint64
}

// repeats reports whether one of the remembered scans read scope's label, and
// remembers this scan when none did.
func (recent *recentLabelScans) repeats(scope, label string) bool {
	recent.mu.Lock()
	defer recent.mu.Unlock()
	for _, scan := range recent.scans {
		if scan.label == label && scan.scope == scope {
			return true
		}
	}
	copy(recent.scans[1:], recent.scans[:len(recent.scans)-1])
	recent.scans[0] = recentLabelScan{scope: scope, label: label}
	return false
}

// refusal returns the view of the last scan whose images of scope's label
// the node read cache refused, 0 when it refused none.
func (recent *recentLabelScans) refusal(scope, label string) uint64 {
	recent.mu.Lock()
	defer recent.mu.Unlock()
	for _, scan := range recent.scans {
		if scan.label == label && scan.scope == scope {
			return scan.refused
		}
	}
	return 0
}

// refuse remembers that the node read cache refused the images a scan of
// scope's label read from a view at view.
func (recent *recentLabelScans) refuse(scope, label string, view uint64) {
	recent.mu.Lock()
	defer recent.mu.Unlock()
	for index := range recent.scans {
		if scan := &recent.scans[index]; scan.label == label && scan.scope == scope {
			scan.refused = max(scan.refused, view)
			return
		}
	}
}

// labelImagesRefused reports whether the node read cache has refused the
// query images of scope's label, and the refusal still stands.
//
// The cache refuses a label's images when they are together more than its
// byte budget (cacheFillLabelNodes). That is a fact about the label's nodes
// as the refused scan read them, not about what the cache holds, so no
// eviction or invalidation changes it. It stands while the label's nodes are
// what they were: every commit that writes a node of the label, or changes
// which nodes carry it, raises the label's floor to its own timestamp
// (issueDurableCacheScope), and a refusal made at a view below that floor is
// forgotten. The label's next scan then offers its images again.
func (b *BadgerEngine) labelImagesRefused(scope, label string) bool {
	refused := b.filteredLabelScans.refusal(scope, label)
	if refused == 0 {
		return false
	}
	b.durableCaches.mu.Lock()
	floor := b.durableCaches.labelFloor(label)
	b.durableCaches.mu.Unlock()
	return refused >= floor
}

// population returns the nodes of scope's label as a scan with visibility v
// walked them, and the view that walk read from; nil when none did.
func (recent *recentLabelScans) population(scope, label string, v readVisibility) ([]NodeID, uint64) {
	recent.mu.Lock()
	defer recent.mu.Unlock()
	for _, scan := range recent.scans {
		if scan.label == label && scan.scope == scope && scan.ids != nil && scan.visibility == v {
			return scan.ids, scan.source
		}
	}
	return nil, 0
}

// keepPopulation remembers ids as the nodes of scope's label, walked whole by
// a scan with visibility v from a view at source. The walk of a label that is
// no longer remembered is not kept, nor one older than the walk it would
// replace.
func (recent *recentLabelScans) keepPopulation(scope, label string, v readVisibility, source uint64, ids []NodeID) {
	if ids == nil {
		ids = []NodeID{}
	}
	recent.mu.Lock()
	defer recent.mu.Unlock()
	for index := range recent.scans {
		if scan := &recent.scans[index]; scan.label == label && scan.scope == scope {
			if scan.ids == nil || scan.source <= source {
				scan.ids, scan.source, scan.visibility = ids, source, v
			}
			return
		}
	}
}

// rememberedLabelNodes returns the nodes of scope's label as an earlier scan
// walked them, when that walk is still what a read at view would walk.
//
// The label index is the one owner of label membership. Every commit that
// writes a node carrying the label before or after the write (a node created,
// deleted or rewritten, a label added or removed, a transaction's commit)
// raises the label's floor to its own timestamp, inside the commit and before
// any read can see it; a commit
// whose effects are not known, such as a dropped database or a restore, raises
// every floor (issueDurableCacheScope). A remembered walk is therefore the
// label's membership exactly while its view is not below that floor. It also
// has to be no newer than the read, which would otherwise see a later
// membership than its own view holds. A transaction never reads through here:
// it reads its own view of the label, with its pending writes.
func (b *BadgerEngine) rememberedLabelNodes(scope, label string, v readVisibility, view uint64) []NodeID {
	ids, source := b.filteredLabelScans.population(scope, label, v)
	if ids == nil || source > view {
		return nil
	}
	b.durableCaches.mu.Lock()
	floor := b.durableCaches.labelFloor(label)
	b.durableCaches.mu.Unlock()
	if source < floor {
		return nil
	}
	return ids
}

// labelCoversScope reports whether label is on at least half of the nodes in
// scope ("" is every database), by the stored node and label counts. Above
// that share a single pass over the node records costs less than a label
// index walk with one record lookup per node. labelled is how many of the
// scope's nodes carry label, or -1 when the counts could not be read.
func (b *BadgerEngine) labelCoversScope(ctx context.Context, scope, label string) (covers bool, labelled int64, err error) {
	var total int64
	if scope == "" {
		total = b.nodeCount.Load()
		labelled, err = b.namedCount(ctx, countDeltaKindLabel, "", label, false)
	} else {
		if total, err = b.entityCountByPrefix(ctx, scope, prefixNode); err == nil {
			namespace, _, _ := strings.Cut(scope, ":")
			labelled, err = b.namedCount(ctx, countDeltaKindLabel, namespace, label, true)
		}
	}
	// Failed count hints retain point lookups; cancellation still ends the read.
	if err != nil {
		return false, -1, ctx.Err()
	}
	return total > 0 && labelled*2 >= total, labelled, ctx.Err()
}
