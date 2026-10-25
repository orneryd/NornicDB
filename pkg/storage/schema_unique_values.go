package storage

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/pkg/localization"
	"reflect"
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
func (sm *SchemaManager) LookupUniqueConstraintValueForPlanning(label, property string, value interface{}) (NodeID, bool, bool, bool) {
	id, found, exists, complete, _ := sm.LookupUniqueConstraintValueForPlanningWithContext(context.Background(), label, property, value)
	return id, found, exists, complete
}

// LookupUniqueConstraintValueForPlanningWithContext observes pending holders
// before reporting the cache answer. It does not change claim/lock ownership.
func (sm *SchemaManager) LookupUniqueConstraintValueForPlanningWithContext(ctx context.Context, label, property string, value interface{}) (NodeID, bool, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, false, false, err
	}
	sm.mu.RLock()
	constraint, exists := sm.uniqueConstraints[fmt.Sprintf("%s:%s", label, property)]
	sm.mu.RUnlock()
	if !exists || value == nil {
		return "", false, exists, false, nil
	}
	key, ok := indexValueKey(value)
	if !ok {
		return "", false, true, false, nil
	}
	id, found, complete, err := sm.lookupUniqueValueWithContext(ctx, constraint, label, property, key)
	return id, found, true, complete, err
}

// The compatibility entrance is retained for the post-conflict commit diagnostic.
func (sm *SchemaManager) lookupUniqueConstraintValueForValidation(label, property string, value interface{}) (NodeID, bool, bool, bool) {
	id, found, complete, exists, _ := sm.lookupUniqueConstraintValueForValidationWithContext(context.Background(), label, property, value)
	return id, found, complete, exists
}

func (sm *SchemaManager) lookupUniqueConstraintValueForValidationWithContext(ctx context.Context, label, property string, value interface{}) (NodeID, bool, bool, bool, error) {
	id, found, exists, complete, err := sm.LookupUniqueConstraintValueForPlanningWithContext(ctx, label, property, value)
	return id, found, complete, exists, err
}

func (sm *SchemaManager) lookupUniqueValueWithContext(ctx context.Context, constraint *UniqueConstraint, label, property string, valueKey interface{}) (NodeID, bool, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, false, err
	}
	view, source := sm.beginPendingRead()
	defer endPendingRead(source)
	constraint.mu.RLock()
	id, found := constraint.values[valueKey]
	complete := constraint.valuesCacheComplete
	constraint.mu.RUnlock()
	if found && !view.keep(id) {
		id, found = "", false
	}
	// Selection needs the smallest holder, not a sorted materialized copy.
	// Enumerate once and keep only the minimum namespace-visible identity.
	var pendingID NodeID
	pendingFound := false
	for _, candidate := range view.valueMatchSet(label, property, valueKey) {
		if err := ctx.Err(); err != nil {
			return "", false, complete, err
		}
		if view.inNamespace(candidate) && (!pendingFound || candidate < pendingID) {
			pendingID, pendingFound = candidate, true
		}
	}
	if pendingFound {
		id, found = pendingID, true
	}
	return id, found, complete, ctx.Err()
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
	entityID   string
	label      string
	property   string
	value      interface{}
}

// uniqueConstraintCommitLock is one constraint key's lock. It belongs to one
// owner at a time (a transaction ID, or a direct engine write's own ID); the
// owner may take it again (holds counts its acquisitions), and released is
// closed whenever it becomes free, waking its waiters (#961).
type uniqueConstraintCommitLock struct {
	recoveryPending bool
	owner           string
	holds           int
	refs            int
	order           uint64
	released        chan struct{}
}

type uniqueConstraintLockRequest struct {
	key  uniqueConstraintLockKey
	lock *uniqueConstraintCommitLock
}

// appendConstraintValueKeys projects node and relationship VALUE keys for the
// canonical registry. Entity and endpoint authority belongs to the earlier
// write/prepare owner; unconstrained images append no keys.
func (sm *SchemaManager) appendConstraintValueKeys(keys []uniqueConstraintLockKey, nodes []*Node, edges []*Edge) []uniqueConstraintLockKey {
	keys, _ = sm.appendConstraintValueKeysWithContext(context.Background(), keys, nodes, edges)
	return keys
}

// appendConstraintValueKeysWithContext is the same value projection during
// cancellable commit preparation. Context-free point writes and accepted
// prepared-commit recovery use the compatibility entrance above.
func (sm *SchemaManager) appendConstraintValueKeysWithContext(ctx context.Context, keys []uniqueConstraintLockKey, nodes []*Node, edges []*Edge) ([]uniqueConstraintLockKey, error) {
	if sm == nil {
		return keys, ctx.Err()
	}
	for _, node := range nodes {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if node == nil || len(node.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels(node.Labels) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if edge == nil || len(edge.Properties) == 0 {
			continue
		}
		for _, c := range sm.GetConstraintsForLabels([]string{edge.Type}) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
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

	return keys, ctx.Err()
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
// Each lock guards the entire commit window for its specific value —
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
	seen := make(map[uniqueConstraintLockKey]struct{}, len(keys))
	for _, k := range keys {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if !lockableUniqueConstraintKey(k) {
			continue
		}
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		requests = append(requests, uniqueConstraintLockRequest{key: k})
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
			lock = &uniqueConstraintCommitLock{order: sm.uniqueConstraintCommitOrder, released: make(chan struct{})}
			sm.uniqueConstraintCommitLocks[requests[i].key] = lock
		}
		lock.refs++
		requests[i].lock = lock
	}
	sm.uniqueConstraintCommitLocksMu.Unlock()
	if err := sortIndexCandidatesWithContext(ctx, requests, func(a, b uniqueConstraintLockRequest) bool { return a.lock.order < b.lock.order }); err != nil {
		sm.releaseUniqueConstraintLocks(owner, nil, requests)
		return nil, err
	}

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

// waitForUniqueConstraintLock takes request's lock for owner, waiting while
// another owner holds it.
func (sm *SchemaManager) waitForUniqueConstraintLock(ctx context.Context, owner string, request uniqueConstraintLockRequest) error {
	lock := request.lock
	sm.uniqueConstraintCommitLocksMu.Lock()
	for {
		if lock.owner == "" || lock.owner == owner {
			lock.owner = owner
			lock.holds++
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return nil
		}
		if lock.recoveryPending {
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ErrConflict
		}
		if sm.uniqueConstraintLockWaitClosesCycleLocked(owner, lock) {
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return localizedError(localization.StorageTransactionDeadlockDetected(request.key.label, request.key.property), ErrDeadlock)
		}
		if sm.uniqueConstraintLockWaits == nil {
			sm.uniqueConstraintLockWaits = make(map[string]*uniqueConstraintCommitLock)
		}
		sm.uniqueConstraintLockWaits[owner] = lock
		released := lock.released
		sm.uniqueConstraintCommitLocksMu.Unlock()
		if hook := uniqueKeyLockWaitHook.Load(); hook != nil {
			(*hook)()
		}
		select {
		case <-released:
		case <-ctx.Done():
			sm.uniqueConstraintCommitLocksMu.Lock()
			delete(sm.uniqueConstraintLockWaits, owner)
			sm.uniqueConstraintCommitLocksMu.Unlock()
			return ctx.Err()
		}
		sm.uniqueConstraintCommitLocksMu.Lock()
	}
}

// uniqueConstraintLockWaitClosesCycleLocked reports whether owner waiting for
// lock would close a cycle: lock's owner waits, directly or through other
// owners, for a lock owner holds.
func (sm *SchemaManager) uniqueConstraintLockWaitClosesCycleLocked(owner string, lock *uniqueConstraintCommitLock) bool {
	holder := lock.owner
	for steps := 0; holder != "" && steps <= len(sm.uniqueConstraintLockWaits); steps++ {
		if holder == owner {
			return true
		}
		next, waiting := sm.uniqueConstraintLockWaits[holder]
		if !waiting {
			return false
		}
		holder = next.owner
	}
	return false
}

// releaseUniqueConstraintLocks releases owner's hold on each of held and drops
// the registry references of held and unacquired.
func (sm *SchemaManager) releaseUniqueConstraintLocks(owner string, held, unacquired []uniqueConstraintLockRequest) {
	sm.uniqueConstraintCommitLocksMu.Lock()
	defer sm.uniqueConstraintCommitLocksMu.Unlock()
	for i := len(held) - 1; i >= 0; i-- {
		lock := held[i].lock
		if lock.owner != owner || lock.holds == 0 {
			continue
		}
		lock.holds--
		if lock.holds == 0 {
			lock.owner = ""
			lock.recoveryPending = false
			close(lock.released)
			lock.released = make(chan struct{})
		}
	}
	for _, requests := range [2][]uniqueConstraintLockRequest{held, unacquired} {
		for _, request := range requests {
			request.lock.refs--
			if request.lock.refs == 0 && sm.uniqueConstraintCommitLocks[request.key] == request.lock {
				delete(sm.uniqueConstraintCommitLocks, request.key)
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
	keys, _ := sm.uniqueMergeKeysWithContext(context.Background(), labels, props)
	return keys
}

func (sm *SchemaManager) uniqueMergeKeysWithContext(ctx context.Context, labels []string, props map[string]interface{}) ([]mergeAdmissionKey, error) {
	if sm == nil || len(props) == 0 {
		return nil, ctx.Err()
	}
	var keys []mergeAdmissionKey
	node := &Node{Properties: props}
	constraints, err := sm.GetConstraintsForLabelsWithContext(ctx, labels)
	if err != nil {
		return nil, err
	}
	for _, c := range constraints {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if c.EffectiveEntityType() != ConstraintEntityNode || (c.Type != ConstraintUnique && c.Type != ConstraintNodeKey) {
			continue
		}
		key, ok := nodeConstraintLockKey(c, node)
		if !ok || !lockableUniqueConstraintKey(key) {
			continue
		}
		keys = append(keys, mergeAdmissionKey{lock: key, properties: c.Properties})
	}
	return keys, ctx.Err()
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
		if lock.owner == owner && !lock.recoveryPending {
			lock.recoveryPending = true
			close(lock.released)
			lock.released = make(chan struct{})
		}
	}
}
