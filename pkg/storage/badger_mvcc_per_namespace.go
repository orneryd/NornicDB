package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"runtime"

	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"
)

// namespaceForIDs returns the shared namespace of a slice of node or edge IDs,
// or an error if the slice is empty / mixed-namespace.
func namespaceForIDs[T ~string](ids []T, namespaceOf func(T) string, kind string) (string, error) {
	var ns string
	for _, id := range ids {
		if id == "" {
			continue
		}
		other := namespaceOf(id)
		if other == "" {
			return "", fmt.Errorf("%s ID must be prefixed with namespace, got: %s", kind, id)
		}
		if ns == "" {
			ns = other
			continue
		}
		if ns != other {
			return "", fmt.Errorf("%w: batch contains both %q and %q",
				ErrCrossNamespaceTransaction, ns, other)
		}
	}
	if ns == "" {
		return "", fmt.Errorf("batch contains no usable IDs")
	}
	return ns, nil
}

// firstIDNamespace returns the namespace of the first non-empty ID, or "".
func firstIDNamespace[T ~string](ids []T) string {
	for _, id := range ids {
		if id != "" {
			ns, _, _ := ParseDatabasePrefix(string(id))
			return ns
		}
	}
	return ""
}

// namespaceForEdgeIDs returns the shared namespace of a slice of edge IDs.
func namespaceForEdgeIDs(ids []EdgeID) (string, error) {
	return namespaceForIDs(ids, namespaceForEdgeID, "edge")
}

// namespaceMVCCState keeps the last issued sequence apart from its durable
// reservation ceiling. Snapshot cuts use seq, so unused numbers in a block
// never advance a running transaction's snapshot. highWaterNanos clamps
// version timestamps against backward clock steps.
type namespaceMVCCState struct {
	seq             atomic.Uint64
	highWaterNanos  atomic.Int64
	persistKey      []byte
	reservedThrough atomic.Uint64
	refillMu        contextRWMutex
}

// namespaceMVCC seeds an unopened namespace from the durable sequence
// ceiling, the legacy global key and surviving entity heads. Older
// binaries stored the last committed sequence in the same eight bytes;
// it is still a valid lower bound when opening that data.
func (b *BadgerEngine) namespaceMVCC(namespace string) (*namespaceMVCCState, error) {
	if namespace == "" {
		return nil, fmt.Errorf("namespaceMVCC: empty namespace")
	}

	b.mvccByNamespaceMu.RLock()
	state, ok := b.mvccByNamespace[namespace]
	b.mvccByNamespaceMu.RUnlock()
	if ok {
		return state, nil
	}

	persisted, err := b.loadPersistedNamespaceSequence(namespace)
	if err != nil {
		return nil, err
	}
	recoveredSeq, recoveredHighWater, err := b.recoverNamespaceMVCCFloor(namespace)
	if err != nil {
		return nil, err
	}

	// A concurrent caller may have published the namespace's state since the
	// read check; only the first one is kept and persisted.
	b.mvccByNamespaceMu.Lock()
	state, ok = b.mvccByNamespace[namespace]
	if !ok {
		if b.mvccByNamespace == nil {
			b.mvccByNamespace = make(map[string]*namespaceMVCCState)
		}
		starting := max(persisted, b.mvccLegacyGlobalSeed, recoveredSeq)
		state = &namespaceMVCCState{
			persistKey: mvccNamespaceSequenceKey(namespace),
		}
		state.seq.Store(starting)
		state.reservedThrough.Store(persisted)
		if recoveredHighWater > 0 {
			state.highWaterNanos.Store(recoveredHighWater)
		}
		b.mvccByNamespace[namespace] = state
	}
	b.mvccByNamespaceMu.Unlock()
	return state, nil
}

// EnsureNamespaceMVCC eagerly loads the per-namespace MVCC state so a
// subsequent BeginTransaction snapshots the namespace's current sequence
// instead of treating it like a post-begin namespace.
func (b *BadgerEngine) EnsureNamespaceMVCC(namespace string) error {
	if namespace == "" {
		return nil
	}
	state, err := b.namespaceMVCC(namespace)
	if err != nil {
		return err
	}
	// Hot path: once a live engine has already advanced this namespace's
	// sequence and high-water mark, re-scanning all heads on every implicit
	// write transaction is pure overhead. The expensive recovery path is only
	// needed for cold/stale states (e.g. reopen after migration where the
	// namespace was initialized at seq=0 before recovery ran).
	if state.seq.Load() > 0 && state.highWaterNanos.Load() > 0 {
		return nil
	}
	recoveredSeq, recoveredHighWater, err := b.recoverNamespaceMVCCFloor(namespace)
	if err != nil {
		return err
	}
	wantSeq := state.seq.Load()
	if b.mvccLegacyGlobalSeed > wantSeq {
		wantSeq = b.mvccLegacyGlobalSeed
	}
	if recoveredSeq > wantSeq {
		wantSeq = recoveredSeq
	}
	for {
		current := state.seq.Load()
		if wantSeq <= current {
			break
		}
		if state.seq.CompareAndSwap(current, wantSeq) {
			break
		}
	}
	if recoveredHighWater > 0 {
		for {
			current := state.highWaterNanos.Load()
			if recoveredHighWater <= current {
				break
			}
			if state.highWaterNanos.CompareAndSwap(current, recoveredHighWater) {
				break
			}
		}
	}
	return nil
}

// snapshotNamespaceVersions captures the current (sequence, high-water
// timestamp) for every namespace the engine knows about. Called from
// BeginTransaction so a transaction's readTS, when later rebound at
// lazy pin time, points to the namespace's begin-time version rather
// than its current version. Without this, the lazy-pin path would let
// peer commits that landed between begin and pin become visible —
// silently violating snapshot isolation for any reader that pinned
// after a peer commit. The snapshot is a point-in-time copy, so peer
// commits after BeginTransaction do NOT mutate it.
func (b *BadgerEngine) snapshotNamespaceVersions(ctx context.Context) (map[string]MVCCVersion, error) {
	return b.snapshotNamespaceVersionsAt(ctx, time.Now().UTC())
}

// snapshotNamespaceVersionsAt is snapshotNamespaceVersions at the wall clock
// a transaction cut supplies (mvccReservations.cutTaken).
func (b *BadgerEngine) snapshotNamespaceVersionsAt(ctx context.Context, now time.Time) (map[string]MVCCVersion, error) {
	b.mvccByNamespaceMu.RLock()
	defer b.mvccByNamespaceMu.RUnlock()
	if len(b.mvccByNamespace) == 0 {
		return nil, ctx.Err()
	}
	out := make(map[string]MVCCVersion, len(b.mvccByNamespace))
	for ns, state := range b.mvccByNamespace {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if state == nil {
			continue
		}
		highWater := state.highWaterNanos.Load()
		ts := now
		if highWater > ts.UnixNano() {
			ts = time.Unix(0, highWater).UTC()
		}
		out[ns] = MVCCVersion{
			CommitTimestamp: ts,
			CommitSequence:  state.seq.Load(),
		}
	}
	return out, ctx.Err()
}

// loadPersistedNamespaceSequence reads the durable sequence ceiling for
// namespace from disk; returns 0 if the key is absent (fresh namespace).
func (b *BadgerEngine) loadPersistedNamespaceSequence(namespace string) (uint64, error) {
	var seq uint64
	err := b.withView(func(txn *badger.Txn) error {
		item, err := txn.Get(mvccNamespaceSequenceKey(namespace))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			if len(val) != 8 {
				return fmt.Errorf("invalid mvcc sequence length: %d", len(val))
			}
			seq = binary.BigEndian.Uint64(val)
			return nil
		})
	})
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// pdW161RecoverInflight counts concurrent recoveries (PD-W161-2 item 10 diagnostic, scratch only).
var pdW161RecoverInflight atomic.Int64

func (b *BadgerEngine) recoverNamespaceMVCCFloor(namespace string) (uint64, int64, error) {
	if namespace == "" {
		return 0, 0, nil
	}
	pdStart := time.Now()
	pdInflight := pdW161RecoverInflight.Add(1)
	var pdNodeKeys, pdEdgeKeys int
	pdCaller := "unknown"
	if pc, _, line, ok := runtime.Caller(1); ok {
		pdCaller = fmt.Sprintf("%s:%d", runtime.FuncForPC(pc).Name(), line)
	}
	defer func() {
		pdW161RecoverInflight.Add(-1)
		if elapsed := time.Since(pdStart); elapsed >= 5*time.Millisecond {
			fmt.Fprintf(os.Stderr, "PD_CAUSE recover_namespace_mvcc_floor namespace=%s caller=%s inflight_at_start=%d node_keys=%d edge_keys=%d start=%s end=%s elapsed_ms=%.3f\n",
				namespace, pdCaller, pdInflight, pdNodeKeys, pdEdgeKeys, pdStart.UTC().Format(time.RFC3339Nano),
				time.Now().UTC().Format(time.RFC3339Nano), float64(elapsed)/float64(time.Millisecond))
		}
	}()
	namespacePrefix := []byte(namespace + ":")
	var maxSeq uint64
	var maxTS int64

	track := func(version MVCCVersion) {
		if version.CommitSequence > maxSeq {
			maxSeq = version.CommitSequence
		}
		if ts := version.CommitTimestamp.UTC().UnixNano(); ts > maxTS {
			maxTS = ts
		}
	}

	err := b.withView(func(txn *badger.Txn) error {
		for _, prefix := range []byte{prefixNode, prefixEdge} {
			// Seek to this namespace so a first write never walks other
			// databases' primary keys while recovering its MVCC floors.
			scanPrefix := append([]byte{prefix}, namespacePrefix...)
			it := txn.NewIterator(badgerPrefixIteratorOptions(scanPrefix))
			for it.Seek(scanPrefix); it.ValidForPrefix(scanPrefix); it.Next() {
				key := it.Item().Key()
				if prefix == prefixNode {
					pdNodeKeys++
					head, err := b.loadNodeMVCCHeadInTxn(txn, NodeID(string(key[1:])))
					if errors.Is(err, ErrNotFound) {
						continue
					}
					if err != nil {
						it.Close()
						return err
					}
					track(head.Version)
					track(head.FloorVersion)
					continue
				}
				pdEdgeKeys++
				head, err := b.loadEdgeMVCCHeadInTxn(txn, EdgeID(string(key[1:])))
				if errors.Is(err, ErrNotFound) {
					continue
				}
				if err != nil {
					it.Close()
					return err
				}
				track(head.Version)
				track(head.FloorVersion)
			}
			it.Close()
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return maxSeq, maxTS, nil
}

// nextNamespaceMVCCSequence atomically advances the namespace's commit
// sequence and reports whether the counter has saturated. Saturation
// triggers the same timestamp-bumping fallback that the engine-global path
// used: highWaterNanos is force-advanced by 1ns instead of wrapping the
// uint64 to zero.
func (s *namespaceMVCCState) nextSequence() (uint64, bool) {
	for {
		cur := s.seq.Load()
		if cur == maxMVCCCommitSequence {
			return cur, true
		}
		next := cur + 1
		if s.seq.CompareAndSwap(cur, next) {
			return next, false
		}
	}
}

// reserveCommitTimestamp clamps the namespace's high-water mark to
// max(commitTime, current-high-water+forceAdvance) and returns the
// resulting commit timestamp. forceAdvance is set when the sequence has
// saturated, in which case we must guarantee timestamp strict ordering
// across versions that share the saturated sequence value.
func (s *namespaceMVCCState) reserveCommitTimestamp(commitTime time.Time, forceAdvance bool) (time.Time, error) {
	stampNanos := commitTime.UTC().UnixNano()
	for {
		cur := s.highWaterNanos.Load()
		next := stampNanos
		if forceAdvance {
			if cur == int64(^uint64(0)>>1) {
				return time.Time{}, fmt.Errorf("mvcc commit timestamp exhausted: %w", ErrExhausted)
			}
			if next <= cur {
				next = cur + 1
			}
		} else if next <= cur {
			return commitTime.UTC(), nil
		}
		if s.highWaterNanos.CompareAndSwap(cur, next) {
			return time.Unix(0, next).UTC(), nil
		}
	}
}
