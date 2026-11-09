package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// MVCC version pruning: the one owner of which historical versions may go.
//
// Readers never wait for a prune pass. A transaction reads only through its
// pinned physical snapshot (withSnapshotViewLocked): the managed oracle keeps
// the versions it reads until it ends, so a delete committed after its cut is
// invisible to it, and a delete committed before its cut removed only versions
// older than heads its boundary already covers. Its registration therefore
// only has to reach the next step's floors, and it registers without any
// prune lock. An as-of read resolves history through a fresh view instead; it
// registers under mvccPruneBatchMu shared (beginMVCCSnapshotRead), so no step
// that decided without it can commit after it is registered. It therefore
// waits for the step in flight, which scans the whole remaining history of
// the key it prunes: that wait grows with the length of the remaining chain.
//
// A pass walks the four MVCC version keyspaces in bounded batches. Each batch
// is committed in steps; a step reads the reader floors and commits its
// deletes while it holds mvccPruneBatchMu, and releases it before the next
// step. A step deletes up to one chunk of version records (mvccPruneChunk),
// plus the final tombstone marker when it reclaims a deleted entity, in
// transactions that each fit one Badger batch, so a key with a longer history
// is pruned over several steps, each committed: the kernel decides every step
// again from what is stored then, by the same rules. Each step scans the
// key's whole remaining history before it deletes the oldest chunk, so a very
// long chain can do roughly quadratic total scan work at a fixed chunk size.
// Keys that
// cannot have anything to delete are recognized in the read-only walk and open
// no write transaction. A budgeted pass (MVCCPruneOptions.MaxRuntime, the
// lifecycle cycle) carries its deadline into every admission, storage and
// conflict wait it makes, so a spent budget also ends a batch that is waiting
// for storage; reading the reader floors takes short engine locks without a
// deadline. The next budgeted pass resumes after the last committed decision.
// Found work waits for at most a fixed share of the budget of further
// scanning plus the scan of the key in progress: the first key a budgeted
// pass decides is committed at once, and later candidates are committed at
// the first key boundary after an eighth of the budget has passed since the
// first of them was found (or when a batch is full, or the walk ends). Work whose commit does not fit leaves the cursor before it, so it is
// the first key the next budgeted pass decides, with the whole budget. So a
// budgeted pass commits progress in any layout of candidates, unless that
// first key's own work (its scan and its deletes) exceeds the whole budget.
// An unbudgeted pass (an explicit prune) walks everything.

// mvccPruneBatchKeys bounds the keys one batch decides and commits.
const mvccPruneBatchKeys = 64

// mvccPrunePendingShare bounds how long found work waits: in a budgeted pass,
// candidates are committed at the first key boundary after
// MaxRuntime/mvccPrunePendingShare has passed since the first of them was
// found, so they wait at most that plus the scan of the key in progress.
const mvccPrunePendingShare = 8

// mvccPruneChunkRecords bounds the version records one prune step deletes,
// apart from the final tombstone marker of an entity the step reclaims. It is
// well inside one Badger batch in every engine profile (the smallest, the
// LowMemory profile's, admits about 13,000 entries); mvccPruneChunk lowers it
// for a smaller batch.
const mvccPruneChunkRecords = 4096

// mvccPruneChunk is the most version records one prune step deletes, apart
// from the final tombstone marker of an entity it reclaims: one Badger batch
// must hold them, with room for that marker and the step's floor, head and
// dictionary entries (otherwise badger.ErrTxnTooBig).
func (b *BadgerEngine) mvccPruneChunk() int {
	if limit := b.db.MaxBatchCount() / 2; limit < mvccPruneChunkRecords {
		return max(1, int(limit))
	}
	return mvccPruneChunkRecords
}

// mvccPruneCursor is where the next budgeted pass resumes: the keyspace index
// and the last logical key the previous budgeted pass decided (nil: from the
// keyspace's start). Guarded by mvccPruneMu.
type mvccPruneCursor struct {
	kind  int
	after []byte
}

// mvccPruneReaders is one step's view of the registered snapshot readers.
type mvccPruneReaders struct {
	// any: at least one reader is registered.
	any bool
	// unknown: readers whose namespaces and versions are not known (the
	// engine's own counter, used without a lifecycle registry): every key is
	// protected entirely.
	unknown bool
	// byNamespace holds every reader's snapshot per namespace, oldest first;
	// "" holds readers registered without a namespace, which apply to every
	// namespace.
	byNamespace map[string][]MVCCVersion
	// all holds every reader's snapshot, oldest first, for keys whose
	// namespace can no longer be resolved.
	all []MVCCVersion
}

// SnapshotVersionsByNamespace returns every reader's snapshot version per
// namespace, oldest first. It is the one reader-floor computation: the
// lifecycle registry and the engine's prune both read it. A namespace appears
// only when it has a reader; "" collects readers registered without a
// namespace.
func SnapshotVersionsByNamespace(readers []SnapshotReaderInfo) map[string][]MVCCVersion {
	out := make(map[string][]MVCCVersion)
	for _, reader := range readers {
		out[reader.Namespace] = append(out[reader.Namespace], reader.SnapshotVersion)
	}
	for _, versions := range out {
		sortMVCCVersions(versions)
	}
	return out
}

// OldestSnapshotVersionsByNamespace returns the oldest snapshot version per
// namespace (SnapshotVersionsByNamespace).
func OldestSnapshotVersionsByNamespace(readers []SnapshotReaderInfo) map[string]MVCCVersion {
	all := SnapshotVersionsByNamespace(readers)
	out := make(map[string]MVCCVersion, len(all))
	for namespace, versions := range all {
		out[namespace] = versions[0]
	}
	return out
}

// sortMVCCVersions orders versions oldest first, with zero (unknown)
// versions before every other.
func sortMVCCVersions(versions []MVCCVersion) {
	sort.Slice(versions, func(i, j int) bool {
		a, b := versions[i], versions[j]
		if a.IsZero() || b.IsZero() {
			return a.IsZero() && !b.IsZero()
		}
		return a.Compare(b) < 0
	})
}

// mvccPruneReaderFloors snapshots the registered readers for one step: the
// lifecycle registry when one is wired, and the engine's counter, which
// counts readers registered while lifecycle is disabled.
func (b *BadgerEngine) mvccPruneReaderFloors() mvccPruneReaders {
	var readers mvccPruneReaders
	if b.activeMVCCSnapshotReaders.Load() > 0 {
		readers.any, readers.unknown = true, true
		return readers
	}
	b.mu.RLock()
	controller := b.lifecycleController
	b.mu.RUnlock()
	if controller == nil {
		return readers
	}
	registry := controller.ReaderRegistry()
	if registry == nil {
		return readers
	}
	snapshot := registry.Snapshot()
	if len(snapshot) == 0 {
		if registry.ActiveCount() > 0 {
			readers.any, readers.unknown = true, true
		}
		return readers
	}
	readers.any = true
	readers.byNamespace = SnapshotVersionsByNamespace(snapshot)
	readers.all = make([]MVCCVersion, 0, len(snapshot))
	for _, reader := range snapshot {
		readers.all = append(readers.all, reader.SnapshotVersion)
	}
	sortMVCCVersions(readers.all)
	return readers
}

// keepFrom decides, for one logical key, which of its version records the
// registered readers can see. records holds the key's version records oldest
// first; head is its current version. Every reader sees the newest record at
// or before its snapshot, or the head when its snapshot is at or after the
// head; a reader older than every record sees none of them. keepFrom returns
// the index of the oldest record any applicable reader sees (len(records)
// when none sees a record) and whether any reader applies to the key: every
// record below that index is invisible to every reader. A reader of unknown
// (zero) version keeps the whole chain.
func (r *mvccPruneReaders) keepFrom(b *BadgerEngine, logical []byte, records []MVCCVersion, head MVCCVersion) (int, bool) {
	if r == nil || !r.any {
		return len(records), false
	}
	if r.unknown {
		return 0, true
	}
	var lists [2][]MVCCVersion
	if namespace, resolved := b.mvccLogicalNamespace(logical); resolved {
		lists = [2][]MVCCVersion{r.byNamespace[namespace], r.byNamespace[""]}
	} else {
		lists[0] = r.all
	}
	keep, applies := len(records), false
	for _, snapshots := range lists {
		if len(snapshots) == 0 {
			continue
		}
		applies = true
		if snapshots[0].IsZero() {
			return 0, true
		}
		if len(records) == 0 {
			continue
		}
		// The oldest reader that sees any record: the first snapshot at or
		// after the oldest record. Older readers see none; newer ones see
		// the same record or a newer one.
		first := sort.Search(len(snapshots), func(i int) bool { return snapshots[i].Compare(records[0]) >= 0 })
		if first == len(snapshots) || snapshots[first].Compare(head) >= 0 {
			continue
		}
		visible := sort.Search(len(records), func(i int) bool { return records[i].Compare(snapshots[first]) > 0 }) - 1
		if visible < keep {
			keep = visible
		}
	}
	return keep, applies
}

// mvccLogicalNamespace resolves the database of an MVCC logical key from the
// entities it belongs to (mvccLogicalEntities): the node or relationship
// itself, or an adjacency record's node.
func (b *BadgerEngine) mvccLogicalNamespace(logical []byte) (string, bool) {
	if b.idDict == nil {
		return "", false
	}
	entities := b.mvccLogicalEntities(logical)
	var id string
	switch {
	case len(entities.nodes) > 0:
		id = string(entities.nodes[0])
	case len(entities.edges) > 0:
		id = string(entities.edges[0])
	default:
		return "", false
	}
	namespace, _, prefixed := ParseDatabasePrefix(id)
	return namespace, prefixed
}

// mvccLogicalKeyLength is the length of the logical key that a version key
// of the walked keyspaces starts with, or 0 for a malformed key.
func mvccLogicalKeyLength(key []byte) int {
	switch {
	case len(key) == 1+8+16:
		return 1 + 8
	case len(key) == 1+8+8+16 && (key[0] == prefixMVCCOutgoingAdj || key[0] == prefixMVCCIncomingAdj):
		return 1 + 8 + 8
	default:
		return 0
	}
}

// beginMVCCSnapshotRead registers an as-of reader at version. It reads
// history through a fresh view, so its registration waits for a prune step
// in flight, which decided without it; that step's scan of the remaining
// history of its key grows with the chain's length. ctx bounds that wait.
func (b *BadgerEngine) beginMVCCSnapshotRead(ctx context.Context, version MVCCVersion) (func(), error) {
	if err := b.mvccPruneBatchMu.RLockContext(ctx); err != nil {
		return nil, err
	}
	defer b.mvccPruneBatchMu.RUnlock()
	return b.acquireSnapshotReader(SnapshotReaderInfo{
		SnapshotVersion: version,
		StartTime:       time.Now(),
	})
}

// beginMVCCHistoryRead is the one registration of a history read at version
// and selects its view. A nil view asks for a fresh engine view: such a read
// can see deletes committed after it registers, so it registers behind a
// prune step in flight (beginMVCCSnapshotRead). A transaction's pinned
// snapshot cannot see them, so its registration never waits.
func (b *BadgerEngine) beginMVCCHistoryRead(ctx context.Context, version MVCCVersion, view func(func(*badger.Txn) error) error) (func(), func(func(*badger.Txn) error) error, error) {
	if view == nil {
		release, err := b.beginMVCCSnapshotRead(ctx, version)
		return release, b.withView, err
	}
	release, err := b.acquireSnapshotReader(SnapshotReaderInfo{SnapshotVersion: version, StartTime: time.Now()})
	return release, view, err
}

// PruneMVCCVersions deletes the historical versions that retention no longer
// keeps and no registered snapshot can read. Passes are ordered against each
// other; readers and writers never wait for one (see the file comment).
//
// MaxRuntime bounds this engine's call. Waiting for another pass, storage
// admission, conflict retries and the walk over long chains all end when it
// is spent; the call then returns what it committed, and the next bounded
// call resumes after the last committed decision. Reading the reader floors
// takes short engine locks without a deadline. The first key the call
// decides is committed at once, and a later candidate waits for at most an
// eighth of MaxRuntime of further discovery plus the scan of the key in
// progress before it is committed, so the
// call commits progress unless that first key's own work exceeds MaxRuntime.
// A step's update that
// storage has already admitted runs to completion, because a commit is
// indivisible: a call can exceed MaxRuntime by that one commit, which deletes
// up to mvccPruneChunk records plus a final tombstone marker. The budget is
// per engine: a wrapper that prunes several engines (a composite database)
// gives each its own MaxRuntime.
func (b *BadgerEngine) PruneMVCCVersions(ctx context.Context, opts MVCCPruneOptions) (int64, error) {
	if !b.work.Enter() {
		return 0, ErrStorageClosed
	}
	defer b.work.Leave()
	work := ctx
	if opts.MaxRuntime > 0 {
		var cancel context.CancelFunc
		work, cancel = context.WithTimeout(ctx, opts.MaxRuntime)
		defer cancel()
	}
	if err := b.mvccPruneMu.LockContext(work); err != nil {
		if mvccPruneBudgetSpent(ctx, err) {
			return 0, nil
		}
		return 0, err
	}
	defer b.mvccPruneMu.Unlock()
	pass := mvccPrunePass{b: b, ctx: work, opts: b.effectiveMVCCPruneOptions(opts)}
	start := mvccPruneCursor{}
	if opts.MaxRuntime > 0 {
		start = b.mvccPruneResume
	}
	// The version keyspaces a pass walks, in cursor order.
	kinds := []byte{prefixMVCCNode, prefixMVCCEdge, prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj}
	for kind := start.kind; kind < len(kinds); kind++ {
		after := []byte(nil)
		if kind == start.kind {
			after = start.after
		}
		if err := pass.walk(kinds[kind], after); err != nil {
			if opts.MaxRuntime > 0 && mvccPruneBudgetSpent(ctx, err) {
				b.mvccPruneResume = mvccPruneCursor{kind: kind, after: pass.decided}
				return pass.deleted, nil
			}
			return pass.deleted, err
		}
	}
	b.mvccPruneResume = mvccPruneCursor{}
	return pass.deleted, nil
}

// mvccPruneBudgetSpent reports whether err ended a pass because its
// MaxRuntime was spent, not because its caller ended.
func mvccPruneBudgetSpent(caller context.Context, err error) bool {
	return caller.Err() == nil && errors.Is(err, context.DeadlineExceeded)
}

// mvccPrunePass is one PruneMVCCVersions call. ctx carries the call's
// MaxRuntime deadline into the walk and every admission, storage and
// conflict wait the pass makes.
type mvccPrunePass struct {
	b       *BadgerEngine
	ctx     context.Context
	opts    MVCCPruneOptions
	deleted int64
	// started: the pass has decided a key. Until then a candidate is the
	// front of the pass and is committed at once.
	started bool
	// decided is the last logical key of the current keyspace whose decision
	// is committed; every key before it is committed too.
	decided []byte
}

// walk decides every logical key of one keyspace after `after`. An error
// leaves p.decided at the last committed decision.
func (p *mvccPrunePass) walk(kind byte, after []byte) error {
	p.decided = bytes.Clone(after)
	prefix := []byte{kind}
	seek := prefix
	if after != nil {
		// Past every version key of after: they are after + 16 bytes.
		seek = append(bytes.Clone(after), bytes.Repeat([]byte{0xff}, 17)...)
	}
	return p.b.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badgerPrefixIteratorOptions(prefix))
		defer it.Close()
		var logical, last []byte
		size := 0
		// batch holds the candidates found and not yet committed;
		// pendingSince is when the first of them was found.
		var batch [][]byte
		var pendingSince time.Time
		share := p.opts.MaxRuntime / mvccPrunePendingShare
		decide := func() error {
			front := !p.started
			p.started = true
			if p.candidate(txn, logical, size, last) {
				if len(batch) == 0 {
					pendingSince = time.Now()
				}
				batch = append(batch, append([]byte(nil), logical...))
			}
			// Keep finding more only while the batch has room and, in a
			// budgeted pass, the found work is not the pass's front and has
			// waited less than its share of the budget.
			waited := share > 0 && (front || time.Since(pendingSince) >= share)
			if len(batch) > 0 && len(batch) < mvccPruneBatchKeys && !waited {
				return nil
			}
			if err := p.flush(batch); err != nil {
				return err
			}
			batch = batch[:0]
			p.decided = append(p.decided[:0], logical...)
			return nil
		}
		for it.Seek(seek); it.ValidForPrefix(prefix); it.Next() {
			// Also inside one long chain: the walk stops when the call's
			// budget or its caller ends. Found work not yet committed is
			// then the first key the next pass decides.
			if err := p.ctx.Err(); err != nil {
				return err
			}
			key := it.Item().Key()
			n := mvccLogicalKeyLength(key)
			if n == 0 {
				return fmt.Errorf("invalid mvcc key length: %d", len(key))
			}
			if size > 0 && bytes.Equal(key[:n], logical) {
				size++
				last = append(last[:0], key...)
				continue
			}
			if size > 0 {
				if err := decide(); err != nil {
					return err
				}
			}
			logical = append(logical[:0], key[:n]...)
			last = append(last[:0], key...)
			size = 1
		}
		if size > 0 {
			if err := decide(); err != nil {
				return err
			}
		}
		if err := p.flush(batch); err != nil {
			return err
		}
		if size > 0 {
			p.decided = append(p.decided[:0], logical...)
		}
		return nil
	})
}

// candidate reports, from the read-only walk, whether logical may have
// versions to delete. It is a necessary condition only: the kernel
// (pruneMVCCKeyspaceInTxn) decides. size is the number of version records;
// last is the newest one's key.
func (p *mvccPrunePass) candidate(txn *badger.Txn, logical []byte, size int, last []byte) bool {
	keep := p.opts.MaxVersionsPerKey
	if keep <= 0 || size > keep {
		return true
	}
	if p.opts.MinRetentionAge > 0 {
		// The retention rule keeps every record; only a tombstoned head with
		// no retention age reclaims more.
		return false
	}
	tombstoned, err := p.b.mvccPruneHeadTombstonedInTxn(txn, logical, last)
	return err != nil || tombstoned
}

// mvccPruneHeadTombstonedInTxn reports whether logical's head is a tombstone,
// resolving the head as the kernel does: the external head, or else the
// newest version record.
func (b *BadgerEngine) mvccPruneHeadTombstonedInTxn(txn *badger.Txn, logical, last []byte) (bool, error) {
	if len(logical) == 1+8 {
		head, err := b.loadMVCCHeadForLogicalKeyInTxn(txn, logical)
		if err == nil {
			return head.Tombstoned, nil
		}
		if err != ErrNotFound {
			return false, err
		}
	}
	item, err := txn.Get(last)
	if err != nil {
		return false, err
	}
	return mvccItemTombstoned(logical[0], item)
}

// flush commits batch, the pending candidate keys, in steps.
func (p *mvccPrunePass) flush(batch [][]byte) error {
	for len(batch) > 0 {
		done, err := p.step(batch)
		if err != nil {
			return err
		}
		batch = batch[done:]
	}
	return nil
}

// step decides batch's keys in order under one hold of mvccPruneBatchMu: the
// floors it reads and the deletes it commits form one unit that no as-of
// registration can split. It deletes up to mvccPruneChunk records, plus the
// final tombstone marker of an entity it reclaims; when that leaves records of
// a key to decide, the next step continues with that key,
// with fresh reader floors. step returns how many keys of batch it finished;
// each finished key's decision is committed.
func (p *mvccPrunePass) step(batch [][]byte) (int, error) {
	if err := p.b.mvccPruneBatchMu.LockContext(p.ctx); err != nil {
		return 0, err
	}
	defer p.b.mvccPruneBatchMu.Unlock()
	readers := p.b.mvccPruneReaderFloors()
	room := p.b.mvccPruneChunk()
	for i, logical := range batch {
		deleted, more, err := p.b.pruneMVCCLogicalKey(p.ctx, logical, p.opts, &readers, room)
		p.deleted += deleted
		if err != nil {
			return i, err
		}
		if more {
			return i, nil
		}
		p.decided = append(p.decided[:0], logical...)
		if room -= int(deleted); room <= 0 {
			return i + 1, nil
		}
	}
	return len(batch), nil
}

// pruneMVCCLogicalKey runs the kernel for one logical key in its own update,
// deleting at most limit version records, and retries a conflict with a
// writer of the same entity. more reports that the key has records left to
// decide.
func (b *BadgerEngine) pruneMVCCLogicalKey(ctx context.Context, logical []byte, opts MVCCPruneOptions, readers *mvccPruneReaders, limit int) (int64, bool, error) {
	backoff := 50 * time.Microsecond
	for {
		var deleted int64
		var more bool
		// Ordered after the entity's acknowledged write-behind commits: one
		// not yet applied is drained first, so the prune reads the entity's
		// latest head (a tombstone it would reclaim may have been written
		// over), and none lands over it afterwards.
		err := b.withUpdate(ctx, updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(update *badger.Txn) error {
			var pruneErr error
			deleted, more, pruneErr = b.pruneMVCCKeyspaceInTxn(ctx, update, logical, opts, readers, limit)
			return pruneErr
		}, b.mvccLogicalEntities(logical))
		if !errors.Is(err, badger.ErrConflict) {
			if err != nil {
				return 0, false, err
			}
			return deleted, more, nil
		}
		select {
		case <-ctx.Done():
			return 0, false, ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 2*time.Millisecond {
			backoff *= 2
		}
	}
}

// mvccItemTombstoned decodes whether a stored version record of the given
// keyspace is a tombstone.
func mvccItemTombstoned(kind byte, item *badger.Item) (bool, error) {
	var tombstoned bool
	err := item.Value(func(val []byte) error {
		var decodeErr error
		tombstoned, decodeErr = mvccRecordTombstoned(kind, val)
		return decodeErr
	})
	return tombstoned, err
}

// mvccRecordTombstoned decodes whether a version record of the given
// keyspace is a tombstone.
func mvccRecordTombstoned(kind byte, val []byte) (bool, error) {
	switch kind {
	case prefixMVCCNode:
		record, err := decodeMVCCNodeRecord(val)
		return record.Tombstoned, err
	case prefixMVCCEdge:
		record, err := decodeMVCCEdgeRecord(val)
		return record.Tombstoned, err
	case prefixMVCCOutgoingAdj, prefixMVCCIncomingAdj:
		record, err := decodeMVCCAdjacencyRecord(val)
		return record.Tombstoned, err
	default:
		return false, fmt.Errorf("unsupported mvcc prune prefix: %x", kind)
	}
}
