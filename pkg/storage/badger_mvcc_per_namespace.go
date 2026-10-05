package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

// namespaceMVCCState holds the per-database MVCC sequence counter and the
// high-water clamp on commit timestamps. One instance lives in
// BadgerEngine.mvccByNamespace per active namespace.
//
// The two atomics are independent: seq is the strictly-increasing logical
// commit sequence used by snapshotIsolationConflict for total ordering, and
// highWaterNanos is the largest commit-timestamp ever stamped onto a version
// in this namespace. Reads clamp their sample to >= highWaterNanos so a
// backward NTP step cannot make a new transaction observe an earlier
// timestamp than something already committed.
//
// A persistKey is cached so allocate paths never re-encode the namespace
// segment for the on-disk sequence key on the hot path.
type namespaceMVCCState struct {
	seq            atomic.Uint64
	highWaterNanos atomic.Int64
	persistKey     []byte
}

// namespaceMVCC returns the per-namespace MVCC state, lazily loading the
// persisted sequence and seeding the high-water clamp from the legacy
// engine-global counter on first access.
//
// Backward compatibility: the legacy engine-global mvccSequenceKey is read
// once at engine open into b.mvccLegacyGlobalSeed. The first time any
// namespace is touched, its starting sequence is set to
// max(persisted-per-ns, legacyGlobalSeed) — guaranteeing that no version
// in any namespace can ever sit at or below a sequence previously emitted
// under the global counter, so MVCC head records persisted by older
// binaries remain strictly older than anything we mint going forward.
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
		if recoveredHighWater > 0 {
			state.highWaterNanos.Store(recoveredHighWater)
		}
		b.mvccByNamespace[namespace] = state
	}
	b.mvccByNamespaceMu.Unlock()
	if !ok && recoveredSeq > persisted {
		b.persistMVCCSequence(namespace)
	}
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
	persisted, err := b.loadPersistedNamespaceSequence(namespace)
	if err != nil {
		return err
	}
	recoveredSeq, recoveredHighWater, err := b.recoverNamespaceMVCCFloor(namespace)
	if err != nil {
		return err
	}
	wantSeq := state.seq.Load()
	if b.mvccLegacyGlobalSeed > wantSeq {
		wantSeq = b.mvccLegacyGlobalSeed
	}
	if persisted > wantSeq {
		wantSeq = persisted
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
	if wantSeq > persisted {
		b.persistMVCCSequence(namespace)
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
func (b *BadgerEngine) snapshotNamespaceVersions() map[string]MVCCVersion {
	b.mvccByNamespaceMu.RLock()
	defer b.mvccByNamespaceMu.RUnlock()
	if len(b.mvccByNamespace) == 0 {
		return nil
	}
	out := make(map[string]MVCCVersion, len(b.mvccByNamespace))
	now := time.Now().UTC()
	for ns, state := range b.mvccByNamespace {
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
	return out
}

// loadPersistedNamespaceSequence reads the persisted commit sequence for
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

func (b *BadgerEngine) recoverNamespaceMVCCFloor(namespace string) (uint64, int64, error) {
	if namespace == "" {
		return 0, 0, nil
	}
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
			opts := badgerIteratorOptions()
			opts.Prefix = []byte{prefix}
			it := txn.NewIterator(opts)
			for it.Rewind(); it.ValidForPrefix(opts.Prefix); it.Next() {
				key := it.Item().Key()
				if len(key) <= 1 || !bytes.HasPrefix(key[1:], namespacePrefix) {
					continue
				}
				if prefix == prefixNode {
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
