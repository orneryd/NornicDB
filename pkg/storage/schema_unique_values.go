package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// CheckUniqueConstraint checks if a value violates a unique constraint.
// Returns error if constraint is violated.
func (sm *SchemaManager) CheckUniqueConstraint(label, property string, value interface{}, excludeNode NodeID) error {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return nil // No constraint
	}

	constraint.mu.RLock()
	defer constraint.mu.RUnlock()

	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil
	}

	if existingNode, found := constraint.values[valueKey]; found {
		if existingNode != excludeNode {
			return localizedError(localization.StorageSchemaUniqueConstraintViolation(label, property, value), nil)
		}
	}

	return nil
}

// LookupUniqueConstraintValue returns the node currently registered for a
// single-property uniqueness constraint value. The second return value reports
// whether the value is present, and the third reports whether the unique
// constraint exists.
func (sm *SchemaManager) LookupUniqueConstraintValue(label, property string, value interface{}) (NodeID, bool, bool) {
	nodeID, found, exists, _ := sm.LookupUniqueConstraintValueForPlanning(label, property, value)
	return nodeID, found, exists
}

// LookupUniqueConstraintValueForPlanning returns the node currently registered
// for a single-property uniqueness constraint value, plus whether the values
// cache has been rebuilt from storage and can be trusted for misses. Planners
// may trust absence only when cacheComplete is true; otherwise they must retain
// a scan fallback because the cache may not have been rebuilt from storage yet.
func (sm *SchemaManager) LookupUniqueConstraintValueForPlanning(label, property string, value interface{}) (nodeID NodeID, valueFound bool, constraintExists bool, cacheComplete bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, exists, false
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return "", false, true, false
	}
	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, value, valueKey)
	return nodeID, valueFound, true, cacheComplete
}

func (sm *SchemaManager) lookupUniqueConstraintValueForValidation(label, property string, value interface{}) (nodeID NodeID, valueFound bool, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, false, exists
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return "", false, false, true
	}

	nodeID, valueFound, cacheComplete = sm.lookupUniqueValue(constraint, label, property, value, valueKey)
	return nodeID, valueFound, cacheComplete, true
}

// uniqueValueHolders returns the nodes holding value under label's unique
// constraint on property, merged with the pending writes: the registered
// holder unless a pending write supersedes it, and every pending node with
// the value. cacheComplete reports whether the registered values cover all
// stored nodes, so an empty result can be trusted.
func (sm *SchemaManager) uniqueValueHolders(label, property string, value interface{}) (holders []NodeID, cacheComplete bool, constraintExists bool) {
	sm.mu.RLock()
	constraint, exists := sm.uniqueConstraints[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists {
		return nil, false, false
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return nil, false, true
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	holder, found := constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && view.keep(holder) {
		holders = append(holders, holder)
	}
	holders = view.valueMatches(holders, label, property, valueKey)
	return holders, cacheComplete, true
}

// lookupUniqueValue returns the node holding valueKey under constraint,
// merged with the pending writes: a registered holder with a pending update
// or delete no longer counts, and a pending node with the value does.
func (sm *SchemaManager) lookupUniqueValue(constraint *UniqueConstraint, label, property string, value, valueKey interface{}) (nodeID NodeID, found bool, cacheComplete bool) {
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	nodeID, found = constraint.values[valueKey]
	cacheComplete = constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && !view.keep(nodeID) {
		nodeID, found = "", false
	}
	if pending := view.valueMatches(nil, label, property, valueKey); len(pending) > 0 {
		sort.Slice(pending, func(i, j int) bool { return string(pending[i]) < string(pending[j]) })
		nodeID, found = pending[0], true
	}
	return nodeID, found, cacheComplete
}

// uniqueConstraintLockKey identifies one (label, property, value) triple for
// the purpose of acquiring a commit-time mutex. Two transactions whose
// pending nodes touch the same (label, property, value) serialize at commit;
// transactions touching disjoint values commit in parallel. The granularity
// is per constrained value, not per constraint.
//
// The value is stored in its canonical comparable form returned by
// indexValueKey so semantically equal but type-distinct values
// (e.g. int and int64) use the same lock and serialize correctly. Values
// that are not comparable cannot acquire a lock; their constraint is still
// validated at commit but without commit-window serialization. (In practice
// every UNIQUE-constrained property in Eshu and Neo4j-compatible workloads
// uses comparable scalar types — strings, ints, floats, bools.)
//
// Lock granularity history: an earlier per-(label, property) design
// effectively serialized every writer touching any value of a constrained
// property. Under bootstrap-index Pass 2 fan-out (8 projector workers + the
// collector + the ingester all writing TerraformResource nodes with
// disjoint uids), this collapsed throughput to single-writer levels — a
// "serialization workaround" in disguise. Per-value locking lets disjoint
// writers commit concurrently while still preventing the silent-overwrite
// race that motivated the lock.
type uniqueConstraintLockKey struct {
	// entityKind zero retains the existing constraint value identity. Entity
	// authority shares this registry and the existing reentrant transaction owner.
	entityKind uint8
	// shared requests endpoint stability without changing key identity.
	shared   bool
	entityID string
	label    string
	property string
	value    interface{}
}

// uniqueConstraintCommitLock is one canonical constraint or entity key's
// authority. Exclusive owners serialize entity mutation and constrained values;
// relationship endpoint readers share the same identity and exclude writers.
// Owners may reacquire their holds. A channel exists only while there are
// waiters, and is closed when the key's eligibility changes.
type uniqueConstraintCommitLock struct {
	recoveryPending bool
	owner           string
	holds           int
	readerOwner     string
	readerHolds     int
	readers         map[string]int
	waitingWriters  int
	refs            int
	order           uint64
	released        chan struct{}
}

// Registry entries are private to active acquisitions. They return to the pool
// only after the last holder or waiter drops its reference.
var uniqueConstraintLockPool = sync.Pool{New: func() any { return new(uniqueConstraintCommitLock) }}

// Batch lock acquisition discards its deduplication map as soon as requests
// are assembled. Reuse bounded maps so large ingest batches do not allocate
// the same table on every commit; clearing drops all retained key references.
type uniqueConstraintSeenTable struct {
	keys map[uniqueConstraintLockKey]int
}

var uniqueConstraintSeenPool = sync.Pool{New: func() any {
	return &uniqueConstraintSeenTable{keys: make(map[uniqueConstraintLockKey]int)}
}}

type uniqueConstraintLockRequest struct {
	key    uniqueConstraintLockKey
	lock   *uniqueConstraintCommitLock
	shared bool
}

// appendConstraintValueKeys projects node and relationship VALUE keys for the
// canonical registry. Entity and endpoint authority belongs to the earlier
// write/prepare owner; unconstrained images append no keys.
func (sm *SchemaManager) appendConstraintValueKeys(keys []uniqueConstraintLockKey, nodes []*Node, edges []*Edge) []uniqueConstraintLockKey {
	if sm == nil {
		return keys
	}
	for _, node := range nodes {
		if node == nil || len(node.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels(node.Labels) {
			if c.EffectiveEntityType() != ConstraintEntityNode {
				continue
			}
			if key, ok := nodeConstraintLockKey(c, node); ok {
				if keys == nil {
					keys = make([]uniqueConstraintLockKey, 0, len(nodes))
				}
				keys = append(keys, key)
			}
		}
	}
	for _, edge := range edges {
		if edge == nil || len(edge.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels([]string{edge.Type}) {
			if c.EffectiveEntityType() != ConstraintEntityRelationship || (c.Type != ConstraintUnique && c.Type != ConstraintRelationshipKey) {
				continue
			}
			values, ok := relationshipConstraintValueKeys(c.Properties, edge.Properties, true)
			if !ok || len(values) == 0 {
				continue
			}
			var key interface{} = values[0]
			if len(values) > 1 {
				key = NewCompositeKey(values...).Hash
			}
			keys = append(keys, uniqueConstraintLockKey{label: "\x00relationship:" + edge.Type, property: relationshipPropertyKey(c.Properties), value: key})
		}
	}

	return keys
}

// nodeConstraintLockKey is the canonical node key projection for both commit
// and MERGE admission. Composite constraints lock one tuple, never its columns,
// under the hash the constraint's composite index files that tuple under.
func nodeConstraintLockKey(c Constraint, node *Node) (uniqueConstraintLockKey, bool) {
	var properties []string
	switch {
	case (c.Type == ConstraintUnique || c.Type == ConstraintNodeKey) && len(c.Properties) > 0:
		properties = c.Properties
	case c.Type == ConstraintTemporal && len(c.Properties) >= 3:
		properties = c.Properties[:len(c.Properties)-2]
	default:
		return uniqueConstraintLockKey{}, false
	}
	// Preserve TEMPORAL's raw whole-key hash for composite list-bearing keys.
	if c.Type == ConstraintTemporal && len(properties) > 1 {
		if spec, ok := splitTemporalKeySpec(c.Properties); ok {
			value, missing := spec.keyValue(node.Properties)
			if missing == "" && spec.composite() && spec.listBearing(value) {
				return uniqueConstraintLockKey{label: c.Label, property: string(c.Type) + ":" + strings.Join(properties, ","), value: spec.keyHash(value)}, true
			}
		}
	}
	if len(properties) == 1 {
		value, ok := constraintLockValue(node, properties[0])
		return uniqueConstraintLockKey{label: c.Label, property: properties[0], value: value}, ok
	}
	values := make([]interface{}, len(properties))
	for i, property := range properties {
		value, ok := constraintLockValue(node, property)
		if !ok {
			return uniqueConstraintLockKey{}, false
		}
		values[i] = value
	}
	return uniqueConstraintLockKey{label: c.Label, property: string(c.Type) + ":" + strings.Join(properties, ","), value: constraintCompositeKey(values)}, true
}

// constraintLockValue is node's canonical value of prop for a constraint key
// lock; ok is false when the property is missing, null or not comparable (no
// key to lock).
func constraintLockValue(node *Node, prop string) (interface{}, bool) {
	rawValue, has := node.Properties[prop]
	if !has || rawValue == nil {
		return nil, false
	}
	return indexValueKey(rawValue)
}

// acquireUniqueConstraintCommitLocks acquires exact UNIQUE value locks for
// owner in a deterministic order and returns a release function.
// Deterministic ordering eliminates the AB-BA deadlock risk when two
// batches both touch overlapping sets of constrained values. A lock owner
// already holds (a MERGE key its transaction locked, #961) is taken again
// without waiting, and released with this release.
//
// Duplicate keys in the input are deduplicated. An empty input returns a
// no-op release function so callers can safely defer the result regardless
// of whether locks were acquired.
//
// Exclusive value locks guard the entire commit window for their value —
// validateAllConstraints, badgerTx.Commit, and the RegisterUniqueValue call
// that publishes the committed value to the constraint cache — so a
// subsequent transaction touching the same value always observes a coherent
// cache. Transactions touching disjoint values always acquire disjoint locks.
// Registry entries count both holders and waiters and are evicted when that
// count reaches zero, bounding memory by active commit demand rather than the
// historical graph cardinality.
//
// A wait that would close a cycle of owners waiting for each other fails with
// ErrDeadlock, and a wait ends with ctx's error when ctx ends. The locks
// taken so far are released either way.
func (sm *SchemaManager) acquireUniqueConstraintCommitLocks(ctx context.Context, owner string, keys []uniqueConstraintLockKey) (func(), error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if len(keys) == 0 {
		return func() {}, nil
	}
	requests := make([]uniqueConstraintLockRequest, 0, len(keys))
	const minPooledKeys, maxPooledKeys = 64, 16384
	pooled := len(keys) >= minPooledKeys && len(keys) <= maxPooledKeys
	var seen map[uniqueConstraintLockKey]int
	var table *uniqueConstraintSeenTable
	if pooled {
		table = uniqueConstraintSeenPool.Get().(*uniqueConstraintSeenTable)
		seen = table.keys
	} else {
		seen = make(map[uniqueConstraintLockKey]int, len(keys))
	}
	for _, k := range keys {
		shared := k.shared
		k.shared = false // mode is not part of the identity: writes conflict with reads.
		if !lockableUniqueConstraintKey(k) {
			continue
		}
		if index, dup := seen[k]; dup {
			// One owner requesting both modes takes exclusive ownership.
			requests[index].shared = requests[index].shared && shared
			continue
		}
		seen[k] = len(requests)
		requests = append(requests, uniqueConstraintLockRequest{key: k, shared: shared})
	}
	if pooled {
		clear(table.keys)
		uniqueConstraintSeenPool.Put(table)
	}
	if len(requests) == 0 {
		return func() {}, nil
	}

	sm.uniqueConstraintCommitLocksMu.Lock()
	if sm.uniqueConstraintCommitLocks == nil {
		sm.uniqueConstraintCommitLocks = make(map[uniqueConstraintLockKey]*uniqueConstraintCommitLock, len(requests))
	}
	if len(sm.uniqueConstraintCommitLocks) == 0 {
		sm.uniqueConstraintCommitOrder = 0
	}
	for i := range requests {
		lock := sm.uniqueConstraintCommitLocks[requests[i].key]
		if lock == nil {
			sm.uniqueConstraintCommitOrder++
			if sm.uniqueConstraintCommitOrder == 0 {
				panic("UNIQUE commit lock order overflow")
			}
			lock = uniqueConstraintLockPool.Get().(*uniqueConstraintCommitLock)
			lock.order = sm.uniqueConstraintCommitOrder
			sm.uniqueConstraintCommitLocks[requests[i].key] = lock
		}
		lock.refs++
		requests[i].lock = lock
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	sort.Slice(requests, func(i, j int) bool {
		return requests[i].lock.order < requests[j].lock.order
	})

	for i := range requests {
		if err := sm.waitForUniqueConstraintLock(ctx, owner, requests[i]); err != nil {
			sm.releaseUniqueConstraintLocks(owner, requests[:i], requests[i:])
			return nil, err
		}
	}
	var releaseOnce sync.Once
	return func() { releaseOnce.Do(func() { sm.releaseUniqueConstraintLocks(owner, requests, nil) }) }, nil
}

// lockableUniqueConstraintKey reports whether k can key a constraint lock: its
// value is comparable and equal to itself.
func lockableUniqueConstraintKey(k uniqueConstraintLockKey) bool {
	valueType := reflect.TypeOf(k.value)
	if valueType != nil && !valueType.Comparable() {
		return false
	}
	// Non-reflexive comparable values such as NaN never conflict under
	// equality and cannot be safely used as registry map keys: a lookup or
	// delete with the same value would never find them.
	return k.value == nil || k.value == k.value
}

// Endpoint keys usually have a single reader. Keep that owner inline and
// allocate the extra-reader map only when commits actually overlap on a key.
func (lock *uniqueConstraintCommitLock) readerCount() int {
	n := len(lock.readers)
	if lock.readerOwner != "" {
		n++
	}
	return n
}

func (lock *uniqueConstraintCommitLock) readerHoldsFor(owner string) int {
	if lock.readerOwner == owner {
		return lock.readerHolds
	}
	return lock.readers[owner]
}

func (lock *uniqueConstraintCommitLock) addReader(owner string) {
	if lock.readerOwner == owner {
		lock.readerHolds++
		return
	}
	if n := lock.readers[owner]; n > 0 {
		lock.readers[owner] = n + 1
		return
	}
	if lock.readerOwner == "" {
		lock.readerOwner, lock.readerHolds = owner, 1
		return
	}
	if lock.readers == nil {
		lock.readers = make(map[string]int)
	}
	lock.readers[owner]++
}

func (lock *uniqueConstraintCommitLock) removeReader(owner string) bool {
	if lock.readerOwner == owner {
		lock.readerHolds--
		if lock.readerHolds == 0 {
			lock.readerOwner = ""
			return true
		}
		return false
	}
	if n := lock.readers[owner]; n > 1 {
		lock.readers[owner] = n - 1
	} else if n == 1 {
		delete(lock.readers, owner)
		return true
	}
	return false
}

// signalUniqueConstraintLock wakes existing waiters while the registry is held.
func signalUniqueConstraintLock(lock *uniqueConstraintCommitLock) {
	if lock.released != nil {
		close(lock.released)
		lock.released = nil
	}
}

// waitForUniqueConstraintLock owns one key in exclusive or shared mode. The
// latter is only used for relationship endpoints, which must be stable against
// node mutation but can be read concurrently by independent relationships.
func (sm *SchemaManager) waitForUniqueConstraintLock(ctx context.Context, owner string, request uniqueConstraintLockRequest) error {
	lock := request.lock
	sm.uniqueConstraintCommitLocksMu.Lock()
	waiting := false
	clearWait := func() {
		if !waiting {
			return
		}
		delete(sm.uniqueConstraintLockWaits, owner)
		delete(sm.uniqueConstraintLockWaitShared, owner)
		if !request.shared {
			lock.waitingWriters--
			if lock.waitingWriters == 0 {
				signalUniqueConstraintLock(lock)
			}
		}
		waiting = false
	}
	for {
		ownReader := lock.readerHoldsFor(owner) > 0
		ownWriter := lock.owner == owner
		canAcquire := false
		if request.shared {
			canAcquire = (lock.owner == "" || ownWriter) && (lock.waitingWriters == 0 || ownReader || ownWriter)
		} else {
			canAcquire = ownWriter || (lock.owner == "" && (lock.readerCount() == 0 || lock.readerCount() == 1 && ownReader))
		}
		if lock.recoveryPending && !ownWriter && !(request.shared && ownReader) {
			clearWait()
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ErrConflict
		}
		if canAcquire {
			clearWait()
			if request.shared {
				lock.addReader(owner)
			} else {
				lock.owner = owner
				lock.holds++
			}
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return nil
		}
		if !waiting {
			if sm.uniqueConstraintLockWaits == nil {
				sm.uniqueConstraintLockWaits = make(map[string]*uniqueConstraintCommitLock)
			}
			if sm.uniqueConstraintLockWaitShared == nil {
				sm.uniqueConstraintLockWaitShared = make(map[string]bool)
			}
			sm.uniqueConstraintLockWaits[owner] = lock
			sm.uniqueConstraintLockWaitShared[owner] = request.shared
			if !request.shared {
				lock.waitingWriters++
			}
			waiting = true
		}
		if sm.uniqueConstraintLockWaitClosesCycleLocked(owner, lock, request.shared) {
			clearWait()
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return localizedError(localization.StorageTransactionDeadlockDetected(request.key.label, request.key.property), ErrDeadlock)
		}
		if lock.released == nil {
			lock.released = make(chan struct{})
		}
		released := lock.released
		sm.uniqueConstraintCommitLocksMu.Unlock()
		if hook := uniqueKeyLockWaitHook.Load(); hook != nil {
			(*hook)()
		}
		select {
		case <-released:
		case <-ctx.Done():
			sm.uniqueConstraintCommitLocksMu.Lock()
			clearWait()
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ctx.Err()
		}
		sm.uniqueConstraintCommitLocksMu.Lock()
	}
}

// uniqueConstraintLockWaitClosesCycleLocked follows every conflicting owner,
// including queued writers that prevent new readers from starving a writer.
func (sm *SchemaManager) uniqueConstraintLockWaitClosesCycleLocked(owner string, lock *uniqueConstraintCommitLock, shared bool) bool {
	seen := make(map[string]bool)
	var visitOwner func(string) bool
	var visitLock func(string, *uniqueConstraintCommitLock, bool) bool
	visitOwner = func(holder string) bool {
		if holder == owner {
			return true
		}
		if seen[holder] {
			return false
		}
		seen[holder] = true
		if next := sm.uniqueConstraintLockWaits[holder]; next != nil {
			return visitLock(holder, next, sm.uniqueConstraintLockWaitShared[holder])
		}
		return false
	}
	visitLock = func(waiter string, target *uniqueConstraintCommitLock, read bool) bool {
		if target.owner != "" && target.owner != waiter && visitOwner(target.owner) {
			return true
		}
		if !read {
			if reader := target.readerOwner; reader != "" && reader != waiter && visitOwner(reader) {
				return true
			}
			for reader := range target.readers {
				if reader != waiter && visitOwner(reader) {
					return true
				}
			}
		} else if target.owner == "" && target.readerHoldsFor(waiter) == 0 && target.waitingWriters > 0 {
			for writer, pending := range sm.uniqueConstraintLockWaits {
				if writer != waiter && pending == target && !sm.uniqueConstraintLockWaitShared[writer] && visitOwner(writer) {
					return true
				}
			}
		}
		return false
	}
	return visitLock(owner, lock, shared)
}

// releaseUniqueConstraintLocks releases owner's hold on each of held and drops
// the registry references of held and unacquired.
func (sm *SchemaManager) releaseUniqueConstraintLocks(owner string, held, unacquired []uniqueConstraintLockRequest) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for i := len(held) - 1; i >= 0; i-- {
		request := held[i]
		lock := request.lock
		changed := false
		if request.shared {
			changed = lock.removeReader(owner)
		} else if lock.owner == owner && lock.holds > 0 {
			lock.holds--
			if lock.holds == 0 {
				lock.owner = ""
				changed = true
			}
		}
		if lock.owner == "" && lock.readerCount() == 0 {
			lock.recoveryPending = false
		}
		if changed {
			signalUniqueConstraintLock(lock)
		}
	}
	for _, requests := range [2][]uniqueConstraintLockRequest{held, unacquired} {
		for _, request := range requests {
			request.lock.refs--
			if request.lock.refs == 0 && sm.uniqueConstraintCommitLocks[request.key] == request.lock {
				delete(sm.uniqueConstraintCommitLocks, request.key)
				*request.lock = uniqueConstraintCommitLock{}
				uniqueConstraintLockPool.Put(request.lock)
			}
		}
	}
}

// engineWriteLockOwners numbers the lock owners of direct engine writes,
// which aren't transactions.
var engineWriteLockOwners atomic.Uint64

// newEngineWriteLockOwner returns a lock owner for one direct engine write.
func newEngineWriteLockOwner() string {
	return "engine-write-" + strconv.FormatUint(engineWriteLockOwners.Add(1), 10)
}

// mergeAdmissionKey couples the canonical commit lock with the constraint
// whose current equality index supplies the node to catch up to. A composite
// lock's value is the tuple's full key hash in that index.
type mergeAdmissionKey struct {
	lock       uniqueConstraintLockKey
	properties []string
}

func (sm *SchemaManager) uniqueMergeKeys(labels []string, props map[string]interface{}) []mergeAdmissionKey {
	if sm == nil || len(props) == 0 {
		return nil
	}
	var keys []mergeAdmissionKey
	node := &Node{Properties: props}
	for _, c := range sm.GetConstraintsForLabels(labels) {
		if c.EffectiveEntityType() != ConstraintEntityNode || (c.Type != ConstraintUnique && c.Type != ConstraintNodeKey) {
			continue
		}
		key, ok := nodeConstraintLockKey(c, node)
		if !ok || !lockableUniqueConstraintKey(key) {
			continue
		}
		keys = append(keys, mergeAdmissionKey{lock: key, properties: c.Properties})
	}
	return keys
}

// HasUniqueValueTracking reports whether any unique constraint exists whose
// values are tracked in the unique-value cache. When false, callers can skip
// per-value RegisterUniqueValue/UnregisterUniqueValue work entirely.
func (sm *SchemaManager) HasUniqueValueTracking() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return len(sm.uniqueConstraints) > 0
}

// RegisterUniqueValue registers a value for a unique constraint.
func (sm *SchemaManager) RegisterUniqueValue(label, property string, value interface{}, nodeID NodeID) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	constraint.values[valueKey] = nodeID
	constraint.mu.Unlock()
}

// UnregisterUniqueValue removes a value from a unique constraint.
func (sm *SchemaManager) UnregisterUniqueValue(label, property string, value interface{}) {
	sm.mu.RLock()
	key := fmt.Sprintf("%s:%s", label, property)
	constraint, exists := sm.uniqueConstraints[key]
	sm.mu.RUnlock()

	if !exists {
		return
	}
	valueKey, ok := indexValueKey(value)
	if !ok {
		return
	}

	constraint.mu.Lock()
	delete(constraint.values, valueKey)
	constraint.mu.Unlock()
}

// markCommitOwnerRecoveryPending wakes the existing registry's waiters. They
// must relinquish lifecycle admission instead of blocking safe repair behind
// a durable command whose native attempt already rolled back.
func (sm *SchemaManager) markCommitOwnerRecoveryPending(owner string) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for _, lock := range sm.uniqueConstraintCommitLocks {
		if (lock.owner == owner || lock.readerHoldsFor(owner) > 0) && !lock.recoveryPending {
			lock.recoveryPending = true
			signalUniqueConstraintLock(lock)
		}
	}
}
