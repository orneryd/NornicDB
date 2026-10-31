package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dgraph-io/badger/v4"

	"github.com/orneryd/nornicdb/pkg/callerowned"
)

// Change data capture (#937). In a database whose txLogEnrichment is DIFF or
// FULL, every committed change of a node or relationship is a ChangeEvent,
// written in the same Badger commit as the change, by BadgerTransaction.Commit
// alone: the commit's states (newCommitStates) give each entity's state
// before and after. A write that would bypass a transaction runs in one for
// such a database (BadgerEngine.inCaptureTransaction). Events are keyed in
// the database's commit order: commits of one capturing database take its
// change log in turn, from before their writes until they end.

// prefixChangeEvent keys change events: [0x26]["db:"][commitSeq][seq] ->
// ChangeEvent. 0x26 is the first byte no key family has used.
const prefixChangeEvent = byte(0x26)

// ChangeCaptureMode is a database's txLogEnrichment.
type ChangeCaptureMode uint8

const (
	// ChangeCaptureOff captures nothing (Neo4j's default).
	ChangeCaptureOff ChangeCaptureMode = iota
	// ChangeCaptureDiff records the changed properties and labels of an update.
	ChangeCaptureDiff
	// ChangeCaptureFull records the whole state before and after.
	ChangeCaptureFull
)

// ParseChangeCaptureMode reads a txLogEnrichment value; anything but DIFF or
// FULL is OFF.
func ParseChangeCaptureMode(value string) ChangeCaptureMode {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "DIFF":
		return ChangeCaptureDiff
	case "FULL":
		return ChangeCaptureFull
	}
	return ChangeCaptureOff
}

func (m ChangeCaptureMode) String() string {
	switch m {
	case ChangeCaptureDiff:
		return "DIFF"
	case ChangeCaptureFull:
		return "FULL"
	}
	return "OFF"
}

// ChangeState is an entity's state in a change event: its properties, and a
// node's labels.
type ChangeState struct {
	Properties map[string]any `msgpack:"p"`
	Labels     []string       `msgpack:"l,omitempty"`
}

// ChangeEvent is one committed change of a node or relationship.
type ChangeEvent struct {
	// CommitSeq is the transaction's place in its database's commit order
	// (Neo4j's txId); Seq the change's place in the transaction.
	CommitSeq  uint64            `msgpack:"c"`
	Seq        uint32            `msgpack:"s"`
	StartTime  int64             `msgpack:"ts"` // the transaction's start, Unix nanoseconds
	CommitTime int64             `msgpack:"tc"` // its commit, Unix nanoseconds
	Mode       ChangeCaptureMode `msgpack:"m"`
	TxMetadata map[string]any    `msgpack:"md,omitempty"`
	// Node is true for a node's change, false for a relationship's.
	Node bool `msgpack:"n"`
	// Operation is 'c', 'u' or 'd'.
	Operation byte `msgpack:"o"`
	// ID is the entity's ID without its database prefix.
	ID string `msgpack:"i"`
	// Labels are a node's labels: after the change, or before a delete.
	Labels []string `msgpack:"l,omitempty"`
	// Type, Start and End are a relationship's type and endpoints (IDs
	// without the database prefix), with the endpoints' labels.
	Type        string       `msgpack:"t,omitempty"`
	Start       string       `msgpack:"a,omitempty"`
	End         string       `msgpack:"b,omitempty"`
	StartLabels []string     `msgpack:"al,omitempty"`
	EndLabels   []string     `msgpack:"bl,omitempty"`
	Before      *ChangeState `msgpack:"bf,omitempty"`
	After       *ChangeState `msgpack:"af,omitempty"`
	// Keys are the entity's key-constraint property values, as Neo4j's
	// events carry them: a node's per label (one map per NODE KEY
	// constraint it satisfies), a relationship's as a list (RELATIONSHIP
	// KEY), and each endpoint's per label. Taken from the entity's whole
	// state (after the change, or before a delete), so a DIFF update
	// carries them too.
	Keys      map[string][]map[string]any `msgpack:"k,omitempty"`
	EdgeKeys  []map[string]any            `msgpack:"ek,omitempty"`
	StartKeys map[string][]map[string]any `msgpack:"ak,omitempty"`
	EndKeys   map[string][]map[string]any `msgpack:"bk,omitempty"`
}

// ChangeCaptureEngine is implemented by engines that capture changes; the
// storage wrappers forward it.
type ChangeCaptureEngine interface {
	// SetChangeCapture sets a database's capture mode.
	SetChangeCapture(namespace string, mode ChangeCaptureMode)
	// ChangeCapture returns a database's capture mode.
	ChangeCapture(namespace string) ChangeCaptureMode
	// ChangeEvents returns up to limit of a database's events after
	// (commitSeq, seq), in commit order; limit <= 0 is no limit.
	ChangeEvents(namespace string, commitSeq uint64, seq uint32, limit int) ([]ChangeEvent, error)
	// ChangeBounds returns the commitSeq of a database's earliest retained
	// event and of its last committed one; 0, 0 when it has none.
	ChangeBounds(namespace string) (earliest, last uint64, err error)
	// SetChangeRetention sets how long change events are kept.
	SetChangeRetention(retention time.Duration)
}

// changeCapture is an engine's capture state.
type changeCapture struct {
	modes sync.Map // namespace -> ChangeCaptureMode
	logs  sync.Map // namespace -> *changeLog
	// retention is how long events are kept, in nanoseconds (0: the default);
	// the retention job runs once a database captures, until Close.
	retention atomic.Int64
	jobMu     sync.Mutex
	job       *changeRetentionJob
}

// changeLog orders a database's capturing commits: a commit holds it from
// before its writes until it ends, so commitSeq follows commit order.
type changeLog struct {
	mu     trackedRWMutex
	loaded bool
	last   uint64 // the last committed commitSeq
}

// SetChangeCapture sets a database's capture mode (its txLogEnrichment).
// The first capturing database starts the retention job.
func (b *BadgerEngine) SetChangeCapture(namespace string, mode ChangeCaptureMode) {
	if mode == ChangeCaptureOff {
		b.changes.modes.Delete(namespace)
		return
	}
	b.changes.modes.Store(namespace, mode)
	b.changes.jobMu.Lock()
	defer b.changes.jobMu.Unlock()
	if b.changes.job == nil {
		b.changes.job = newChangeRetentionJob(b, 0)
		b.changes.job.Start(context.Background())
	}
}

// SetChangeRetention sets how long change events are kept; 0 or less is
// the default, 2 days (Neo4j's default transaction log retention).
func (b *BadgerEngine) SetChangeRetention(retention time.Duration) {
	b.changes.retention.Store(int64(retention))
}

// stopChangeRetention stops the retention job, at Close.
func (b *BadgerEngine) stopChangeRetention() {
	b.changes.jobMu.Lock()
	job := b.changes.job
	b.changes.job = nil
	b.changes.jobMu.Unlock()
	if job != nil {
		job.Stop()
	}
}

// ChangeCapture returns a database's capture mode.
func (b *BadgerEngine) ChangeCapture(namespace string) ChangeCaptureMode {
	if mode, ok := b.changes.modes.Load(namespace); ok {
		return mode.(ChangeCaptureMode)
	}
	return ChangeCaptureOff
}

func changeEventPrefix(namespace string) []byte {
	key := make([]byte, 0, len(namespace)+2)
	key = append(key, prefixChangeEvent)
	key = append(key, namespace...)
	return append(key, ':')
}

func changeEventKey(namespace string, commitSeq uint64, seq uint32) []byte {
	key := changeEventPrefix(namespace)
	key = binary.BigEndian.AppendUint64(key, commitSeq)
	return binary.BigEndian.AppendUint32(key, seq)
}

// changeEventPosition reads (commitSeq, seq) from an event key.
func changeEventPosition(key []byte) (uint64, uint32, bool) {
	if len(key) < 12 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-12:]), binary.BigEndian.Uint32(key[len(key)-4:]), true
}

func (b *BadgerEngine) changeLogFor(namespace string) *changeLog {
	log, _ := b.changes.logs.LoadOrStore(namespace, &changeLog{})
	return log.(*changeLog)
}

// ChangeBounds returns the commitSeq of a database's earliest retained event
// and of its last committed one; 0, 0 when it has none.
func (b *BadgerEngine) ChangeBounds(namespace string) (earliest, last uint64, err error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return 0, 0, err
	}

	prefix := changeEventPrefix(namespace)
	err = b.withView(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		it.Seek(prefix)
		if it.ValidForPrefix(prefix) {
			earliest, _, _ = changeEventPosition(it.Item().Key())
		}
		it.Close()
		opts.Reverse = true
		reverse := txn.NewIterator(opts)
		defer reverse.Close()
		reverse.Seek(append(append([]byte(nil), prefix...), 0xFF))
		if reverse.ValidForPrefix(prefix) {
			last, _, _ = changeEventPosition(reverse.Item().Key())
		}
		return nil
	})
	return earliest, last, err
}

// ChangeEvents returns up to limit of a database's events after
// (commitSeq, seq), in commit order; limit <= 0 is no limit.
func (b *BadgerEngine) ChangeEvents(namespace string, commitSeq uint64, seq uint32, limit int) ([]ChangeEvent, error) {
	if err := b.pendingNamespaceConflict(namespace); err != nil {
		return nil, err
	}

	prefix := changeEventPrefix(namespace)
	after := changeEventKey(namespace, commitSeq, seq)
	var events []ChangeEvent
	err := b.withView(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(after); it.ValidForPrefix(prefix); it.Next() {
			if bytes.Equal(it.Item().Key(), after) {
				continue
			}
			var event ChangeEvent
			if err := it.Item().Value(func(value []byte) error { return decodeValue(value, &event) }); err != nil {
				return fmt.Errorf("decoding change event: %w", err)
			}
			events = append(events, event)
			if limit > 0 && len(events) >= limit {
				return nil
			}
		}
		return nil
	})
	return events, err
}

// txChangeCapture is a capturing commit's hold on its database's change log.
type txChangeCapture struct {
	log       *changeLog
	mode      ChangeCaptureMode
	commitSeq uint64
	wrote     bool
}

// beginChangeCaptureLocked takes the database's change log for a commit in
// a capturing database, before any of the commit's writes and locks (so a
// large commit's publication locks are always taken after it), and
// allocates the commit's place in the change order.
func (tx *BadgerTransaction) beginChangeCaptureLocked() error {
	mode := tx.engine.ChangeCapture(tx.namespace)
	if mode == ChangeCaptureOff {
		return nil
	}
	log := tx.engine.changeLogFor(tx.namespace)
	if err := log.mu.LockContext(tx.commitCtx); err != nil {
		return err
	}
	if !log.loaded {
		_, last, err := tx.engine.ChangeBounds(tx.namespace)
		if err != nil {
			log.mu.Unlock()
			return fmt.Errorf("reading the change log: %w", err)
		}
		log.last, log.loaded = last, true
	}
	tx.changeCapture = &txChangeCapture{log: log, mode: mode, commitSeq: log.last + 1}
	return nil
}

// endChangeCaptureLocked releases the change log when the commit ends; a
// commit that wrote events and committed takes its place.
func (tx *BadgerTransaction) endChangeCaptureLocked(committed bool) {
	capture := tx.changeCapture
	if capture == nil {
		return
	}
	tx.changeCapture = nil
	if committed && capture.wrote {
		capture.log.last = capture.commitSeq
	}
	capture.log.mu.Unlock()
}

// writeChangeEventsLocked writes the commit's change events through w: one
// per node and relationship whose state the transaction changed, in the
// order the transaction first touched them; the relationships a node delete
// removes come before it. It runs before the commit's buffered writes, so a
// cascaded relationship's stored record is still its state before.
func (tx *BadgerTransaction) writeChangeEventsLocked(w *batchWriter, operations []Operation, commitTime time.Time) error {
	capture := tx.changeCapture
	if capture == nil {
		return nil
	}
	b := tx.engine
	states := newCommitStates(operations)
	base := ChangeEvent{
		CommitSeq: capture.commitSeq, StartTime: tx.StartTime.UnixNano(), CommitTime: commitTime.UnixNano(),
		Mode: capture.mode, TxMetadata: copyChangeMetadata(tx.Metadata),
	}
	seq := uint32(0)
	write := func(event ChangeEvent) error {
		event.Seq = seq
		seq++
		value, err := encodeValue(&event)
		if err != nil {
			return fmt.Errorf("encoding change event: %w", err)
		}
		key := changeEventKey(tx.namespace, event.CommitSeq, event.Seq)
		capture.wrote = true
		return w.write(func(txn *badger.Txn) error { return txn.Set(key, value) })
	}
	labels := newChangeEndpointLabels(b, states)
	keys := changeKeys{schema: b.GetSchemaForNamespace(tx.namespace)}
	seenNodes := make(map[NodeID]bool)
	seenEdges := make(map[EdgeID]bool)
	edgeEvent := func(id EdgeID, before, after *Edge) error {
		if seenEdges[id] {
			return nil
		}
		seenEdges[id] = true
		event, ok := edgeChangeEvent(base, before, after, labels)
		if !ok {
			return nil
		}
		current := after
		if current == nil {
			current = before
		}
		event.EdgeKeys = keys.ofEdge(current)
		event.StartKeys = keys.ofNode(labels.node(current.StartNode))
		event.EndKeys = keys.ofNode(labels.node(current.EndNode))
		return write(event)
	}
	for _, op := range operations {
		switch op.Type {
		case OpCreateNode, OpUpdateNode, OpDeleteNode:
			id := op.NodeID
			if op.Node != nil {
				id = op.Node.ID
			}
			if op.Type == OpDeleteNode {
				for _, edgeID := range op.DeletedEdgeIDs {
					if seenEdges[edgeID] {
						continue
					}
					var edge *Edge
					err := w.write(func(txn *badger.Txn) error {
						var loadErr error
						edge, loadErr = b.loadEdgeForAdjacencyTombstoneInTxn(txn, edgeID)
						if loadErr == ErrNotFound {
							loadErr = nil
						}
						return loadErr
					})
					if err != nil {
						return err
					}
					if edge != nil {
						if err := edgeEvent(edgeID, edge, nil); err != nil {
							return err
						}
					}
				}
			}
			if seenNodes[id] {
				continue
			}
			seenNodes[id] = true
			if event, ok := nodeChangeEvent(base, states.committedNodes[id], states.finalNodes[id]); ok {
				whole := states.finalNodes[id]
				if whole == nil {
					whole = states.committedNodes[id]
				}
				event.Keys = keys.ofNode(whole)
				if err := write(event); err != nil {
					return err
				}
			}
		case OpCreateEdge, OpUpdateEdge, OpDeleteEdge:
			id := op.EdgeID
			if op.Edge != nil {
				id = op.Edge.ID
			}
			if err := edgeEvent(id, states.committedEdges[id], states.finalEdges[id]); err != nil {
				return err
			}
		}
	}
	return nil
}

// nodeChangeEvent is a node's change from before to after (nil for none);
// false when the transaction left it as it was, or created and deleted it.
func nodeChangeEvent(base ChangeEvent, before, after *Node) (ChangeEvent, bool) {
	event := base
	event.Node = true
	switch {
	case before == nil && after == nil:
		return event, false
	case before == nil:
		event.Operation, event.ID, event.Labels = 'c', localEntityID(string(after.ID)), copyStrings(after.Labels)
		event.After = &ChangeState{Properties: copyChangeProperties(after.Properties), Labels: copyStrings(after.Labels)}
	case after == nil:
		event.Operation, event.ID, event.Labels = 'd', localEntityID(string(before.ID)), copyStrings(before.Labels)
		event.Before = &ChangeState{Properties: copyChangeProperties(before.Properties), Labels: copyStrings(before.Labels)}
	default:
		changed := changedProperties(before.Properties, after.Properties)
		removedLabels, addedLabels := labelChanges(before.Labels, after.Labels)
		if len(changed) == 0 && len(removedLabels) == 0 && len(addedLabels) == 0 {
			return event, false
		}
		event.Operation, event.ID, event.Labels = 'u', localEntityID(string(after.ID)), copyStrings(after.Labels)
		if base.Mode == ChangeCaptureFull {
			event.Before = &ChangeState{Properties: copyChangeProperties(before.Properties), Labels: copyStrings(before.Labels)}
			event.After = &ChangeState{Properties: copyChangeProperties(after.Properties), Labels: copyStrings(after.Labels)}
		} else {
			event.Before = &ChangeState{Properties: pickProperties(before.Properties, changed), Labels: removedLabels}
			event.After = &ChangeState{Properties: pickProperties(after.Properties, changed), Labels: addedLabels}
		}
	}
	return event, true
}

// edgeChangeEvent is a relationship's change from before to after; false
// when the transaction left it as it was, or created and deleted it.
func edgeChangeEvent(base ChangeEvent, before, after *Edge, labels *changeEndpointLabels) (ChangeEvent, bool) {
	event := base
	current := after
	switch {
	case before == nil && after == nil:
		return event, false
	case before == nil:
		event.Operation = 'c'
		event.After = &ChangeState{Properties: copyChangeProperties(after.Properties)}
	case after == nil:
		event.Operation, current = 'd', before
		event.Before = &ChangeState{Properties: copyChangeProperties(before.Properties)}
	default:
		changed := changedProperties(before.Properties, after.Properties)
		if len(changed) == 0 {
			return event, false
		}
		event.Operation = 'u'
		if base.Mode == ChangeCaptureFull {
			event.Before = &ChangeState{Properties: copyChangeProperties(before.Properties)}
			event.After = &ChangeState{Properties: copyChangeProperties(after.Properties)}
		} else {
			event.Before = &ChangeState{Properties: pickProperties(before.Properties, changed)}
			event.After = &ChangeState{Properties: pickProperties(after.Properties, changed)}
		}
	}
	event.ID, event.Type = localEntityID(string(current.ID)), current.Type
	event.Start, event.End = localEntityID(string(current.StartNode)), localEntityID(string(current.EndNode))
	event.StartLabels, event.EndLabels = labels.of(current.StartNode), labels.of(current.EndNode)
	return event, true
}

// changeEndpointLabels finds a relationship endpoint's labels: its state
// at the end of the transaction when the transaction touched it, else its
// stored node, read once per commit outside the commit's own transaction
// (which would make the read part of its conflict set).
type changeEndpointLabels struct {
	engine *BadgerEngine
	states commitStates
	read   map[NodeID]*Node
}

func newChangeEndpointLabels(engine *BadgerEngine, states commitStates) *changeEndpointLabels {
	return &changeEndpointLabels{engine: engine, states: states, read: make(map[NodeID]*Node)}
}

func (l *changeEndpointLabels) of(id NodeID) []string {
	if node := l.node(id); node != nil {
		return copyStrings(node.Labels)
	}
	return nil
}

// node is the endpoint's state: at the end of the transaction when the
// transaction touched it (before its delete when it deleted it), else as
// stored; nil when there is none.
func (l *changeEndpointLabels) node(id NodeID) *Node {
	if node, touched := l.states.finalNodes[id]; touched {
		if node == nil {
			node = l.states.committedNodes[id]
		}
		if node != nil {
			return node
		}
	}
	if node, ok := l.read[id]; ok {
		return node
	}
	node, err := l.engine.GetNodeWithoutEmbeddings(id)
	if err != nil {
		node = nil
	}
	l.read[id] = node
	return node
}

// changeKeys reads an entity's key-constraint property values from its
// database's schema (ChangeEvent.Keys).
type changeKeys struct {
	schema *SchemaManager
}

// ofNode is a node's key values per label: for each NODE KEY constraint on
// one of its labels, in constraint-name order, the constrained properties'
// values. Nil when it has none.
func (k changeKeys) ofNode(node *Node) map[string][]map[string]any {
	if node == nil || k.schema == nil {
		return nil
	}
	var keys map[string][]map[string]any
	for _, constraint := range k.schema.GetConstraintsForLabels(node.Labels) {
		if constraint.Type != ConstraintNodeKey || constraint.EffectiveEntityType() != ConstraintEntityNode {
			continue
		}
		if values := keyValues(constraint.Properties, node.Properties); values != nil {
			if keys == nil {
				keys = make(map[string][]map[string]any)
			}
			keys[constraint.Label] = append(keys[constraint.Label], values)
		}
	}
	return keys
}

// ofEdge is a relationship's key values: for each RELATIONSHIP KEY
// constraint on its type, in constraint-name order, the constrained
// properties' values. Nil when it has none.
func (k changeKeys) ofEdge(edge *Edge) []map[string]any {
	if edge == nil || k.schema == nil {
		return nil
	}
	constraints := k.schema.GetAllConstraints()
	sort.Slice(constraints, func(i, j int) bool { return constraints[i].Name < constraints[j].Name })
	var keys []map[string]any
	for _, constraint := range constraints {
		if constraint.Type != ConstraintRelationshipKey || constraint.Label != edge.Type {
			continue
		}
		if values := keyValues(constraint.Properties, edge.Properties); values != nil {
			keys = append(keys, values)
		}
	}
	return keys
}

// keyValues is the named properties' values, or nil when one is missing.
func keyValues(names []string, properties map[string]any) map[string]any {
	if len(names) == 0 {
		return nil
	}
	values := make(map[string]any, len(names))
	for _, name := range names {
		value, ok := properties[name]
		if !ok || value == nil {
			return nil
		}
		values[name] = value
	}
	return values
}

// changedProperties returns the keys whose value differs between before and
// after, set on one side only included, sorted.
func changedProperties(before, after map[string]any) []string {
	var changed []string
	for key, value := range before {
		if other, ok := after[key]; !ok || !reflect.DeepEqual(value, other) {
			changed = append(changed, key)
		}
	}
	for key := range after {
		if _, ok := before[key]; !ok {
			changed = append(changed, key)
		}
	}
	sort.Strings(changed)
	return changed
}

func pickProperties(properties map[string]any, keys []string) map[string]any {
	picked := make(map[string]any, len(keys))
	for _, key := range keys {
		if value, ok := properties[key]; ok {
			picked[key] = value
		}
	}
	return picked
}

func labelChanges(before, after []string) (removed, added []string) {
	in := func(labels []string, label string) bool {
		for _, candidate := range labels {
			if candidate == label {
				return true
			}
		}
		return false
	}
	for _, label := range before {
		if !in(after, label) {
			removed = append(removed, label)
		}
	}
	for _, label := range after {
		if !in(before, label) {
			added = append(added, label)
		}
	}
	return removed, added
}

func copyChangeProperties(properties map[string]any) map[string]any {
	if properties == nil {
		return make(map[string]any)
	}
	// Change events are handed to every subscriber: each gets its own values.
	return callerowned.Map(properties)
}

func copyChangeMetadata(metadata map[string]interface{}) map[string]any {
	if len(metadata) == 0 {
		return nil
	}
	return copyChangeProperties(metadata)
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	return append([]string(nil), values...)
}

// localEntityID is an entity ID without its database prefix.
func localEntityID(id string) string {
	if _, local, ok := ParseDatabasePrefix(id); ok {
		return local
	}
	return id
}

// PruneChangeEvents deletes a database's events of transactions committed
// before cutoff, returning how many it deleted. The last transaction's
// events are always kept: they are where db.cdc.current() stands.
func (b *BadgerEngine) PruneChangeEvents(namespace string, cutoff time.Time) (int, error) {
	prefix := changeEventPrefix(namespace)
	_, last, err := b.ChangeBounds(namespace)
	if err != nil {
		return 0, err
	}
	var expired [][]byte
	err = b.withView(func(txn *badger.Txn) error {
		opts := badgerIteratorOptions()
		opts.Prefix = prefix
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var event ChangeEvent
			if err := it.Item().Value(func(value []byte) error { return decodeValue(value, &event) }); err != nil {
				return err
			}
			if event.CommitTime >= cutoff.UnixNano() || event.CommitSeq == last {
				return nil
			}
			expired = append(expired, it.Item().KeyCopy(nil))
		}
		return nil
	})
	if err != nil || len(expired) == 0 {
		return 0, err
	}
	for start := 0; start < len(expired); start += 1000 {
		end := min(start+1000, len(expired))
		if err := b.withUpdate(updateRequest{scope: cacheWriteScope{known: true}, metadata: true}, func(txn *badger.Txn) error {
			for _, key := range expired[start:end] {
				if err := txn.Delete(key); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return start, err
		}
	}
	return len(expired), nil
}

// changeRetentionJob deletes the change events older than the retention
// period of every capturing database, periodically.
type changeRetentionJob struct {
	engine   *BadgerEngine
	interval time.Duration
	mu       sync.Mutex
	cancel   context.CancelFunc
	done     chan struct{}
}

// defaultChangeRetention is Neo4j's default transaction log retention.
const defaultChangeRetention = 48 * time.Hour

// newChangeRetentionJob creates a retention job for engine, checking every
// interval (a minute when not positive); events are kept for the engine's
// retention (SetChangeRetention).
func newChangeRetentionJob(engine *BadgerEngine, interval time.Duration) *changeRetentionJob {
	if interval <= 0 {
		interval = time.Minute
	}
	return &changeRetentionJob{engine: engine, interval: interval}
}

// changeRetention is how long the engine keeps change events.
func (b *BadgerEngine) changeRetention() time.Duration {
	if retention := time.Duration(b.changes.retention.Load()); retention > 0 {
		return retention
	}
	return defaultChangeRetention
}

// Start runs the job until Stop or ctx ends.
func (j *changeRetentionJob) Start(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.cancel != nil {
		return
	}
	ctx, j.cancel = context.WithCancel(ctx)
	j.done = make(chan struct{})
	go j.run(ctx)
}

// Stop stops the job and waits for it.
func (j *changeRetentionJob) Stop() {
	j.mu.Lock()
	cancel, done := j.cancel, j.done
	j.cancel = nil
	j.mu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
}

func (j *changeRetentionJob) run(ctx context.Context) {
	defer close(j.done)
	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			j.RunOnce(time.Now())
		}
	}
}

// RunOnce deletes the expired events of every capturing database.
func (j *changeRetentionJob) RunOnce(now time.Time) {
	cutoff := now.Add(-j.engine.changeRetention())
	j.engine.changes.modes.Range(func(key, _ any) bool {
		namespace := key.(string)
		if n, err := j.engine.PruneChangeEvents(namespace, cutoff); err != nil {
			j.engine.log.Warn("change event retention failed", "component", "cdc", "database", namespace, "error", err)
		} else if n > 0 {
			j.engine.log.Debug("change events expired", "component", "cdc", "database", namespace, "events", n)
		}
		return true
	})
}

// ChangeCaptureOf returns the change-capturing engine under engine's
// wrappers (namespace, async, WAL, tracing), if it has one.
func ChangeCaptureOf(engine Engine) (ChangeCaptureEngine, bool) {
	for engine != nil {
		if capture, ok := engine.(ChangeCaptureEngine); ok {
			return capture, true
		}
		wrapper, ok := engine.(EngineUnwrapper)
		if !ok {
			return nil, false
		}
		inner := wrapper.GetInnerEngine()
		if inner == engine {
			return nil, false
		}
		engine = inner
	}
	return nil, false
}
