// Package writeordertest is a deterministic model-based harness for the
// storage write-order contract. A Driver adapts one store (the storage engine
// itself, or a layer above it such as Cypher or replication) to a fixed set of
// steps; the harness runs generated or named programs of those steps on one
// goroutine and checks every outcome against a reference model of plain maps:
//
//   - acknowledged writes are visible and durable (store = model) once the
//     store is flushed or reopened,
//   - each entity's notifications follow commit order, and the last one is
//     the entity's final state,
//   - no relationship ever lacks an endpoint,
//   - a transaction whose final graph is valid, and that no committed write
//     has conflicted with since it began, is accepted,
//   - node and relationship counters equal the model,
//   - every live entity is manually suppressed exactly when the model says,
//   - with capacity limits (Options), no accepted write leaves more entities
//     than the limit.
//
// A layer above storage (replication, a quota-enforcing database manager)
// injects its own failures through a Faulter and the Fault step: a rejected
// replication, an apply that times out and may still land, a journal racing
// a commit. The model needs no knowledge of them: a rejected write must leave
// no trace, after a reopen too, and an indeterminate one either.
//
// A step's outcome is committed, rejected or indeterminate. Indeterminate
// (the write may or may not have taken effect, as for a replicated command
// whose acknowledgement was lost) is never treated as a rollback: the model
// accepts either result at the next quiescent check and adopts what the store
// holds.
package writeordertest

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/collection"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// Outcome is what a store reports for a step.
type Outcome int

const (
	Committed Outcome = iota
	Rejected
	Indeterminate
)

func (o Outcome) String() string {
	switch o {
	case Committed:
		return "committed"
	case Rejected:
		return "rejected"
	}
	return "indeterminate"
}

// OpKind is an entity write.
type OpKind string

const (
	CreateNode OpKind = "createN"
	UpdateNode OpKind = "updateN"
	DeleteNode OpKind = "deleteN"
	CreateEdge OpKind = "createE"
	UpdateEdge OpKind = "updateE"
	DeleteEdge OpKind = "deleteE"
)

// Op is an entity write. Node and edge names are the model's ("n0", "r1");
// a Driver maps them to the store's identifiers.
type Op struct {
	Kind       OpKind
	Node       string
	Edge       string
	Start, End string
}

func (op Op) String() string {
	switch op.Kind {
	case CreateNode, UpdateNode, DeleteNode:
		return fmt.Sprintf("%s(%s)", op.Kind, op.Node)
	case DeleteEdge:
		return fmt.Sprintf("%s(%s)", op.Kind, op.Edge)
	}
	return fmt.Sprintf("%s(%s %s->%s)", op.Kind, op.Edge, op.Start, op.End)
}

// StepKind is one action of a program.
type StepKind string

const (
	// Direct performs Op outside any transaction (Implicit: in its own
	// implicit transaction).
	Direct StepKind = "direct"
	// Begin opens transaction Slot (Implicit: an implicit transaction, which a
	// write-behind store acknowledges before it is applied).
	Begin StepKind = "begin"
	// Stage stages Op in transaction Slot.
	Stage StepKind = "stage"
	// Commit commits transaction Slot.
	Commit StepKind = "commit"
	// Flush applies every acknowledged write and delivers its notifications.
	Flush StepKind = "flush"
	// Reopen rolls back open transactions, closes the store and opens it again.
	Reopen StepKind = "reopen"
	// DeletePrefix deletes every entity of the namespace.
	DeletePrefix StepKind = "deletePrefix"
	// Backup takes backup Slot; Restore restores it.
	Backup  StepKind = "backup"
	Restore StepKind = "restore"
	// Suppress sets (Flag) or clears manual suppression of Op.Node or Op.Edge.
	Suppress StepKind = "suppress"
	// Sidecar rewrites Op.Node's embedding sidecar for its current body.
	Sidecar StepKind = "sidecar"
	// Arm makes the next notification for Op.Node or Op.Edge write back to
	// that entity once (Implicit: in an implicit transaction).
	Arm StepKind = "arm"
	// Fault arms the driver's named fault (Step.Fault) for the steps that
	// follow (Faulter); a driver without faults skips it.
	Fault StepKind = "fault"
)

// Step is one action of a program.
type Step struct {
	Kind     StepKind
	Op       Op
	Slot     int
	Implicit bool
	Flag     bool
	// Fault names the fault a Fault step arms.
	Fault string
}

func (st Step) target() string {
	if st.Op.Edge != "" {
		return st.Op.Edge
	}
	return st.Op.Node
}

func (st Step) String() string {
	switch st.Kind {
	case Direct:
		if st.Implicit {
			return "implicit " + st.Op.String()
		}
		return "direct " + st.Op.String()
	case Begin:
		return fmt.Sprintf("begin tx%d implicit=%v", st.Slot, st.Implicit)
	case Stage:
		return fmt.Sprintf("tx%d stage %s", st.Slot, st.Op)
	case Commit:
		return fmt.Sprintf("tx%d commit", st.Slot)
	case Suppress:
		return fmt.Sprintf("suppress %s %v", st.target(), st.Flag)
	case Sidecar:
		return "sidecar " + st.Op.Node
	case Arm:
		return fmt.Sprintf("arm listener on %s (implicit=%v)", st.target(), st.Implicit)
	case Fault:
		return "fault " + st.Fault
	case Backup, Restore:
		return fmt.Sprintf("%s b%d", st.Kind, st.Slot)
	}
	return string(st.Kind)
}

// EdgeBody is a relationship's state in the model.
type EdgeBody struct {
	Start, End string
	V          int64
}

// State is a graph: node name -> value, relationship name -> body.
type State struct {
	Nodes map[string]int64
	Edges map[string]EdgeBody
}

// NewState returns an empty graph.
func NewState() State {
	return State{Nodes: map[string]int64{}, Edges: map[string]EdgeBody{}}
}

func (s State) clone() State {
	c := NewState()
	for k, v := range s.Nodes {
		c.Nodes[k] = v
	}
	for k, v := range s.Edges {
		c.Edges[k] = v
	}
	return c
}

func (s State) deleteNode(id string) []string {
	delete(s.Nodes, id)
	var cascaded []string
	for eid, e := range s.Edges {
		if e.Start == id || e.End == id {
			delete(s.Edges, eid)
			cascaded = append(cascaded, eid)
		}
	}
	sort.Strings(cascaded)
	return cascaded
}

func (s State) dangling() (string, bool) {
	for eid, e := range s.Edges {
		if _, ok := s.Nodes[e.Start]; !ok {
			return eid, true
		}
		if _, ok := s.Nodes[e.End]; !ok {
			return eid, true
		}
	}
	return "", false
}

func (s State) String() string {
	var parts []string
	for _, id := range collection.SortedKeys(s.Nodes) {
		parts = append(parts, fmt.Sprintf("%s=%d", id, s.Nodes[id]))
	}
	for _, id := range collection.SortedKeys(s.Edges) {
		e := s.Edges[id]
		parts = append(parts, fmt.Sprintf("%s=%s->%s:%d", id, e.Start, e.End, e.V))
	}
	return "{" + strings.Join(parts, " ") + "}"
}

// valid reports whether op applies to s.
func (op Op) valid(s State) bool {
	switch op.Kind {
	case CreateNode:
		_, ok := s.Nodes[op.Node]
		return !ok
	case UpdateNode, DeleteNode:
		_, ok := s.Nodes[op.Node]
		return ok
	case CreateEdge, UpdateEdge:
		_, exists := s.Edges[op.Edge]
		_, a := s.Nodes[op.Start]
		_, b := s.Nodes[op.End]
		return a && b && exists == (op.Kind == UpdateEdge)
	case DeleteEdge:
		_, ok := s.Edges[op.Edge]
		return ok
	}
	return false
}

// apply applies op with value v to s and returns the entity keys it wrote.
// It is lenient: an op that does not apply does what a store that accepted
// it would (an update of a missing node creates it, a delete of a missing
// entity changes nothing).
func (op Op) apply(s State, v int64) []string {
	switch op.Kind {
	case CreateNode, UpdateNode:
		s.Nodes[op.Node] = v
		return []string{nodeKey(op.Node)}
	case DeleteNode:
		keys := []string{nodeKey(op.Node)}
		for _, eid := range s.deleteNode(op.Node) {
			keys = append(keys, edgeKey(eid))
		}
		return keys
	case CreateEdge, UpdateEdge:
		s.Edges[op.Edge] = EdgeBody{Start: op.Start, End: op.End, V: v}
		return []string{edgeKey(op.Edge)}
	case DeleteEdge:
		delete(s.Edges, op.Edge)
		return []string{edgeKey(op.Edge)}
	}
	return nil
}

// involved returns the keys whose state decides op's outcome against s.
func (op Op) involved(s State) []string {
	switch op.Kind {
	case CreateNode, UpdateNode:
		return []string{nodeKey(op.Node)}
	case DeleteNode:
		keys := []string{nodeKey(op.Node)}
		for eid, e := range s.Edges {
			if e.Start == op.Node || e.End == op.Node {
				keys = append(keys, edgeKey(eid))
			}
		}
		return keys
	case CreateEdge, UpdateEdge:
		return []string{edgeKey(op.Edge), nodeKey(op.Start), nodeKey(op.End)}
	}
	return []string{edgeKey(op.Edge)}
}

func nodeKey(id string) string { return "n:" + id }
func edgeKey(id string) string { return "e:" + id }

// Event is a notification the store delivered: Key is "n:<node>" or
// "e:<edge>"; a relationship event carries its endpoints.
type Event struct {
	Key        string
	V          int64
	Deleted    bool
	Start, End string
}

// NodeEvent and EdgeEvent build events from a store's notifications.
func NodeEvent(node string, v int64, deleted bool) Event {
	return Event{Key: nodeKey(node), V: v, Deleted: deleted}
}

func EdgeEvent(edge string, v int64, deleted bool, start, end string) Event {
	return Event{Key: edgeKey(edge), V: v, Deleted: deleted, Start: start, End: end}
}

// Observed is the store's state over the program's entity names.
type Observed struct {
	State State
	// NodeCount and EdgeCount are the store's counters, or -1 when the store
	// does not keep them.
	NodeCount, EdgeCount int64
	// HiddenNodes and HiddenEdges are the live entities manually suppressed,
	// or nil when the store does not report suppression.
	HiddenNodes, HiddenEdges map[string]bool
	// Usage is the store's accounted size, or nil when it keeps none.
	Usage *Usage
}

// Usage is a store's accounted size: the bytes its quota layer holds for
// what it stores, and an independent recount of the stored entities (hidden
// ones included). They must agree whenever every acknowledged write is
// applied, and the recount must stay within Options.MaxBytes.
type Usage struct {
	Cached, Recount int64
}

// Driver adapts a store to the harness. Every method runs on the harness's
// goroutine; Open's events callback is called on whatever goroutine the store
// delivers notifications on, and may call Write.
type Driver interface {
	// Open opens (or reopens) the store and subscribes events to its
	// notifications. The store's background apply, if any, must not run on
	// its own: Flush, Close and the steps that order behind acknowledged
	// writes are where acknowledged writes are applied.
	Open(events func(Event)) error
	// Close closes the store.
	Close() error
	// Supports reports whether the store implements a step kind.
	Supports(StepKind) bool
	// Write performs op with value v directly or in its own implicit
	// transaction.
	Write(op Op, v int64, implicit bool) (Outcome, error)
	Begin(slot int, implicit bool) error
	Stage(slot int, op Op, v int64) (Outcome, error)
	Commit(slot int) (Outcome, error)
	Rollback(slot int)
	Flush() error
	DeletePrefix() (Outcome, error)
	Backup(slot int) error
	Restore(slot int) (Outcome, error)
	// Suppress sets or clears manual suppression of a node (edge == false) or
	// relationship.
	Suppress(id string, edge, flag bool) (Outcome, error)
	// Sidecar rewrites node's embedding sidecar for its current body v.
	Sidecar(node string, v int64) (Outcome, error)
	// Observe reads the store's state of the given entities.
	Observe(nodes, edges []string) (Observed, error)
}

// Faulter is a Driver that injects faults. Fault arms the named fault for the
// store's following steps; their outcomes report it: Rejected when the write
// cannot take effect, Indeterminate when it still may.
type Faulter interface {
	Fault(name string) error
}

// FaultRejecter is a Faulter that attributes rejections: FaultRejected
// reports whether the step it just returned Rejected for was rejected by a
// fault it armed (a definitive external rejection, such as a replication
// the cluster refused), and clears that report. The harness then accepts the
// rejection of a valid step and leaves the model unchanged, so the store
// must hold no trace of it: its state, counters, suppressions and
// notifications are checked as after any step, after a reopen too. Any
// other rejection of a valid step is still a violation.
type FaultRejecter interface {
	Faulter
	FaultRejected() bool
}

// Options configure a run. MaxNodes and MaxEdges, when positive, are the
// store's capacity: a write that would exceed one may be rejected, and none
// the store accepts may leave more entities than it allows. MaxBytes, when
// positive, is its byte limit: the model cannot size entities, so the store
// reports its usage (Observed.Usage), which must stay within the limit, and
// a write the store refuses for the limit (LimitRejecter) must leave no
// trace.
type Options struct {
	MaxNodes, MaxEdges int
	MaxBytes           int64
}

// Expresser is a Driver that cannot express every write: Expresses reports
// whether it can perform op over state s (the state the write applies to). A
// Cypher route, for one, cannot move a relationship to other endpoints. The
// harness skips a write the driver cannot express, and says so in the trace;
// it is not a rejection.
type Expresser interface {
	Expresses(op Op, s State) bool
}

// LimitRejecter is a Driver that attributes rejections to a limit:
// LimitRejected reports whether the step it just returned Rejected for was
// refused because it would exceed one of the store's limits, and clears
// that report. With Options.MaxBytes set, the harness accepts such a
// rejection of a valid step and leaves the model unchanged; the store must
// hold no trace of it. A refusal for MaxNodes or MaxEdges needs no
// attribution: the model accepts it when the write would exceed the count in
// some state the outstanding indeterminate writes can leave, and reports it
// otherwise.
type LimitRejecter interface {
	LimitRejected() bool
}

// Names of the entities programs use.
const (
	Nodes = 4
	Edges = 3
)

func NodeName(i int) string { return fmt.Sprintf("n%d", i) }
func EdgeName(i int) string { return fmt.Sprintf("r%d", i) }

func universe() (nodes, edges []string) {
	for i := 0; i < Nodes; i++ {
		nodes = append(nodes, NodeName(i))
	}
	for i := 0; i < Edges; i++ {
		edges = append(edges, EdgeName(i))
	}
	return nodes, edges
}

type tx struct {
	implicit bool
	view     State
	begin    int64
	written  map[string]bool
	staged   []Op
	vals     []int64
	// diverged: a staging outcome differed from the view because state it
	// depends on changed since the transaction began.
	diverged bool
	// mayReject: the transaction staged a create over a hidden entity, which
	// the store may reject at commit as a conflict with the stored one.
	mayReject bool
	// deleted: the entities the transaction deleted (their suppression ends).
	deleted map[string]bool
}

type arm struct {
	key      string
	implicit bool
	fired    bool
}

type pendingWrite struct {
	op      Op
	v       int64
	outcome Outcome
}

type run struct {
	d       Driver
	model   State
	seq     int64
	last    map[string]int64
	v       int64
	txs     map[int]*tx
	backups map[int]State
	// hidden: the entities manually suppressed. A create over a hidden
	// entity keeps the stored entity: the store accepts it as a no-op or
	// rejects it, and the model changes nothing.
	hidden       map[string]bool
	backupHidden map[int]map[string]bool
	// dropSeq: the commit sequence of the last prefix delete. A transaction
	// open across it may be rejected, and its staging may diverge.
	dropSeq int64
	arms    []*arm
	events  map[string][]int64
	// eventFloor: the last value written before the last reset of events (a
	// prefix delete or a restore). The store delivers notifications in
	// commit order but not necessarily before the delete returns; one that
	// arrives after the reset with a value no newer is of a write before it.
	// A delete's notification carries no value (0) and is always kept.
	eventFloor int64
	// uncertain holds, per key, the values an indeterminate write may have
	// left (-1: absent); the next quiescent check adopts the store's.
	uncertain      map[string][]int64
	uncertainEdges map[string][]EdgeBody
	// listener writes run inside the notification that triggers them; the
	// model applies them after that notification's own write.
	pending []pendingWrite
	// closing: the store is closing; a write it delivers a notification to
	// may be rejected.
	closing bool
	// listenerWrites counts the writes listeners made; listening the ones
	// still in the store, which listened signals reaching zero.
	listenerWrites int
	listening      int
	listened       *sync.Cond
	// cb owns the state store callbacks share with steps: events, arms,
	// pending, listenerWrites, listening, trace and v (see log).
	cb sync.Mutex

	// opts are the run's options: its capacity.
	opts  Options
	trace []string
	fail  string
}

func (r *run) next() int64 {
	r.cb.Lock()
	defer r.cb.Unlock()
	r.v++
	return r.v
}

// The store delivers notifications on whatever goroutine it chooses (a
// write-behind worker drains and dispatches on its own), so event runs
// concurrently with steps. cb owns everything the two share; neither holds
// it across a call into the store, which may deliver a notification on the
// calling goroutine.

// log appends a line to the trace.
func (r *run) log(line string) {
	r.cb.Lock()
	r.trace = append(r.trace, line)
	r.cb.Unlock()
}

// traceLines is a copy of the trace.
func (r *run) traceLines() []string {
	r.cb.Lock()
	defer r.cb.Unlock()
	return append([]string(nil), r.trace...)
}

// resetEvents forgets the notifications published so far.
func (r *run) resetEvents() {
	r.cb.Lock()
	r.events = map[string][]int64{}
	r.eventFloor = r.v
	r.cb.Unlock()
}

// publishedEvents is a copy of the notifications published so far, per key.
func (r *run) publishedEvents() map[string][]int64 {
	r.cb.Lock()
	defer r.cb.Unlock()
	return collection.CloneSliceValues(r.events)
}

// takePending removes and returns the listener writes made so far, once
// none is still in the store: a write a listener started is accounted with
// the others, not after the check that follows.
func (r *run) takePending() []pendingWrite {
	r.cb.Lock()
	defer r.cb.Unlock()
	for r.listening > 0 {
		r.listened.Wait()
	}
	p := r.pending
	r.pending = nil
	return p
}

// listenerCount is the number of listener writes made so far.
func (r *run) listenerCount() int {
	r.cb.Lock()
	defer r.cb.Unlock()
	return r.listenerWrites
}

// addArm arms a listener write.
func (r *run) addArm(a *arm) {
	r.cb.Lock()
	r.arms = append(r.arms, a)
	r.cb.Unlock()
}

func (r *run) violate(class, format string, args ...any) {
	if r.fail == "" {
		r.fail = class + ": " + fmt.Sprintf(format, args...)
	}
}

func (r *run) commitKeys(keys []string) {
	r.seq++
	for _, k := range keys {
		r.last[k] = r.seq
	}
}

func (r *run) changedSince(begin int64, keys []string) bool {
	for _, k := range keys {
		if r.last[k] > begin {
			return true
		}
	}
	return false
}

func (r *run) allKeys() []string {
	var keys []string
	for id := range r.model.Nodes {
		keys = append(keys, nodeKey(id))
	}
	for id := range r.model.Edges {
		keys = append(keys, edgeKey(id))
	}
	return keys
}

func (r *run) open() bool {
	r.resetEvents()
	if err := r.d.Open(r.event); err != nil {
		r.violate("reopen", "the store failed to open: %v", err)
		return false
	}
	return true
}

// claimEvent records a notification and reserves all listener work before settlement
// can observe an idle harness. Claiming an arm and reserving its write are atomic.
func (r *run) claimEvent(ev Event) []*arm {
	v := ev.V
	if ev.Deleted {
		v = -1
	}
	r.cb.Lock()
	if ev.V == 0 || ev.V > r.eventFloor {
		r.events[ev.Key] = append(r.events[ev.Key], v)
	}
	var fire []*arm
	if !ev.Deleted {
		for _, a := range r.arms {
			if a.fired || a.key != ev.Key {
				continue
			}
			a.fired = true
			fire = append(fire, a)
		}
	}
	r.listenerWrites += len(fire)
	r.listening += len(fire)
	r.cb.Unlock()
	return fire
}

func (r *run) event(ev Event) {
	fire := r.claimEvent(ev)
	for _, a := range fire {
		op := Op{Kind: UpdateNode, Node: ev.Key[2:]}
		if ev.Key[0] == 'e' {
			op = Op{Kind: UpdateEdge, Edge: ev.Key[2:], Start: ev.Start, End: ev.End}
		}
		v := r.next()
		outcome, err := r.d.Write(op, v, a.implicit)
		r.cb.Lock()
		r.trace = append(r.trace, fmt.Sprintf("  listener %s v=%d implicit=%v -> %s %v", op, v, a.implicit, outcome, err))
		r.pending = append(r.pending, pendingWrite{op: op, v: v, outcome: outcome})
		r.listening--
		if r.listening == 0 {
			r.listened.Broadcast()
		}
		r.cb.Unlock()
	}
}

// settle applies, in order, the listener writes made since the last call.
func (r *run) settle() {
	for {
		pending := r.takePending()
		if len(pending) == 0 {
			return
		}
		for _, p := range pending {
			r.applyWrite(p.op, p.v, p.outcome, "listener "+p.op.String())
		}
	}
}

// hiddenCreate reports whether op creates an entity s holds that is hidden.
func (r *run) hiddenCreate(op Op, s State) bool {
	switch op.Kind {
	case CreateNode:
		_, ok := s.Nodes[op.Node]
		return ok && r.hidden[nodeKey(op.Node)]
	case CreateEdge:
		_, ok := s.Edges[op.Edge]
		return ok && r.hidden[edgeKey(op.Edge)]
	}
	return false
}

// unhide ends the suppression of entities a delete removed.
func (r *run) unhide(op Op, keys []string) {
	if op.Kind == DeleteNode || op.Kind == DeleteEdge {
		for _, k := range keys {
			delete(r.hidden, k)
		}
	}
}

// applyWrite applies a single-op write's outcome to the model.
func (r *run) applyWrite(op Op, v int64, outcome Outcome, what string) {
	if r.hiddenCreate(op, r.model) {
		return
	}
	valid := op.valid(r.model)
	fits := !r.exceeds(r.model, op, v)
	switch {
	case outcome == Indeterminate:
		r.uncertainOp(op, v)
	case outcome == Committed && valid && fits:
		keys := op.apply(r.model, v)
		r.unhide(op, keys)
		r.commitKeys(keys)
	case outcome == Committed && op.Kind == UpdateNode:
		// An update of a missing node may create it (an upserting store).
		r.commitKeys(op.apply(r.model, v))
	case outcome == Committed:
		r.violate("accepted-invalid", "%s accepted against %s", what, r.model)
	case valid && !r.mayExceed(r.model, op, v) && !r.closing:
		// A write may be refused for capacity when it would exceed it in
		// any state an indeterminate write can still leave.
		r.rejectedValid("%s rejected against %s", what, r.model)
	}
}

// rejectedValid reports the rejection of a valid step, unless the driver
// attributes it to a fault it armed (FaultRejecter): then the model stays as
// it is and the following checks verify that the store kept no trace.
func (r *run) rejectedValid(format string, args ...any) {
	if f, ok := r.d.(FaultRejecter); ok && f.FaultRejected() {
		r.log("  rejected by an armed fault")
		return
	}
	// Only the byte limit is the store's to judge: the model cannot size
	// entities. A refusal for a count limit is judged by the model
	// (mayExceed), not taken on the driver's word.
	if l, ok := r.d.(LimitRejecter); ok && r.opts.MaxBytes > 0 && l.LimitRejected() {
		r.log("  rejected by the byte limit")
		return
	}
	r.violate("rejected-valid", format, args...)
}

// expresses reports whether the driver can perform op over s.
func (r *run) expresses(op Op, s State) bool {
	if e, ok := r.d.(Expresser); ok && !e.Expresses(op, s) {
		r.log("  not expressible on this driver: skipped")
		return false
	}
	return true
}

// uncertainOp records that op may or may not have taken effect.
func (r *run) uncertainOp(op Op, v int64) {
	after := r.model.clone()
	keys := op.apply(after, v)
	r.uncertainState(after, keys)
}

func (r *run) uncertainState(after State, keys []string) {
	if r.uncertain == nil {
		r.uncertain = map[string][]int64{}
		r.uncertainEdges = map[string][]EdgeBody{}
	}
	for _, k := range keys {
		id := k[2:]
		if k[0] == 'n' {
			for _, s := range []State{r.model, after} {
				v, ok := s.Nodes[id]
				if !ok {
					v = -1
				}
				r.uncertain[k] = append(r.uncertain[k], v)
			}
		} else {
			for _, s := range []State{r.model, after} {
				e, ok := s.Edges[id]
				if !ok {
					e = EdgeBody{V: -1}
				}
				r.uncertainEdges[k] = append(r.uncertainEdges[k], e)
			}
		}
	}
	r.commitKeys(keys)
}

func (r *run) step(st Step) {
	r.log(st.String())
	switch st.Kind {
	case Direct:
		if !r.expresses(st.Op, r.model) {
			return
		}
		v := r.next()
		outcome, err := r.d.Write(st.Op, v, st.Implicit)
		r.applyWrite(st.Op, v, outcome, fmt.Sprintf("%s (%v)", st, err))
		r.settle()
	case Begin:
		if err := r.d.Begin(st.Slot, st.Implicit); err != nil {
			r.violate("rejected-valid", "begin tx%d: %v", st.Slot, err)
			return
		}
		r.txs[st.Slot] = &tx{implicit: st.Implicit, view: r.model.clone(), begin: r.seq, written: map[string]bool{}, deleted: map[string]bool{}}
	case Stage:
		x := r.txs[st.Slot]
		if x == nil || !r.expresses(st.Op, x.view) {
			return
		}
		v := r.next()
		if r.hiddenCreate(st.Op, x.view) {
			_, _ = r.d.Stage(st.Slot, st.Op, v)
			x.mayReject = true
			r.settle()
			break
		}
		if st.Op.Kind == UpdateNode && x.deleted[nodeKey(st.Op.Node)] {
			// A node the transaction deleted stays deleted: its update
			// changes nothing, accepted or not.
			_, _ = r.d.Stage(st.Slot, st.Op, v)
			r.settle()
			break
		}
		// Capacity is a property of the final image: a store may stage a write
		// over its capacity and refuse the commit (commitTx, checkStep check
		// what it accepts), or refuse it when it stages it.
		valid := st.Op.valid(x.view)
		overCapacity := r.mayExceed(x.view, st.Op, v)
		changed := x.diverged || x.begin < r.dropSeq || r.changedSince(x.begin, st.Op.involved(x.view))
		outcome, err := r.d.Stage(st.Slot, st.Op, v)
		accepted := outcome == Committed
		switch {
		case !accepted && valid && overCapacity:
		case accepted && (valid || st.Op.Kind == UpdateNode):
			for _, k := range st.Op.apply(x.view, v) {
				x.written[k] = true
				if st.Op.Kind == DeleteNode || st.Op.Kind == DeleteEdge {
					x.deleted[k] = true
				}
			}
			x.staged = append(x.staged, st.Op)
			x.vals = append(x.vals, v)
		case !accepted && !valid:
		case changed:
			// Staging read state committed since the transaction began.
			x.diverged = true
			if accepted {
				for _, k := range st.Op.apply(x.view, v) {
					x.written[k] = true
					if st.Op.Kind == DeleteNode || st.Op.Kind == DeleteEdge {
						x.deleted[k] = true
					}
				}
				x.staged = append(x.staged, st.Op)
				x.vals = append(x.vals, v)
			}
		case accepted:
			r.violate("accepted-invalid", "tx%d staged %s against %s", st.Slot, st.Op, x.view)
		default:
			r.rejectedValid("tx%d staging %s rejected: %v (view %s)", st.Slot, st.Op, err, x.view)
		}
		r.settle()
	case Commit:
		x := r.txs[st.Slot]
		if x == nil {
			return
		}
		delete(r.txs, st.Slot)
		r.commitTx(st.Slot, x)
		r.settle()
	case Flush:
		// A listener a flush delivers may acknowledge a write of its own,
		// which only a later flush applies: flush until none does.
		for round := 0; ; round++ {
			before := r.listenerCount()
			if err := r.d.Flush(); err != nil {
				r.violate("flush", "flush failed: %v", err)
				return
			}
			r.settle()
			if r.listenerCount() == before || round == 10 {
				break
			}
		}
		r.checkQuiescent("after flush")
	case Reopen:
		r.rollbackAll()
		r.closing = true
		err := r.d.Close()
		r.settle()
		r.closing = false
		if err != nil {
			r.violate("close", "close failed: %v", err)
			return
		}
		if !r.open() {
			return
		}
		r.checkQuiescent("after reopen")
	case DeletePrefix:
		outcome, err := r.d.DeletePrefix()
		switch outcome {
		case Committed:
			keys := r.allKeys()
			r.model = NewState()
			r.commitKeys(keys)
			r.dropSeq = r.seq
			r.hidden = map[string]bool{}
			r.resetEvents()
		case Indeterminate:
			r.uncertainState(NewState(), r.allKeys())
		default:
			r.rejectedValid("DeletePrefix rejected: %v", err)
		}
		r.settle()
	case Backup:
		if err := r.d.Backup(st.Slot); err != nil {
			r.violate("rejected-valid", "backup: %v", err)
			return
		}
		r.backups[st.Slot] = r.model.clone()
		hidden := map[string]bool{}
		for k, h := range r.hidden {
			hidden[k] = h
		}
		r.backupHidden[st.Slot] = hidden
		// The backup's flush delivered notifications; their listeners'
		// writes follow the backup.
		r.settle()
	case Restore:
		snap, ok := r.backups[st.Slot]
		if !ok {
			return
		}
		r.rollbackAll()
		outcome, err := r.d.Restore(st.Slot)
		if outcome == Rejected {
			r.rejectedValid("restore with no transaction open: %s %v", outcome, err)
			return
		}
		if outcome != Committed {
			r.violate("rejected-valid", "restore with no transaction open: %s %v", outcome, err)
			return
		}
		keys := r.allKeys()
		r.model = snap.clone()
		r.commitKeys(append(keys, r.allKeys()...))
		r.hidden = map[string]bool{}
		for k, h := range r.backupHidden[st.Slot] {
			r.hidden[k] = h
		}
		r.resetEvents()
		r.settle()
	case Suppress:
		edge := st.Op.Edge != ""
		var key string
		var exists bool
		if edge {
			key = edgeKey(st.Op.Edge)
			_, exists = r.model.Edges[st.Op.Edge]
		} else {
			key = nodeKey(st.Op.Node)
			_, exists = r.model.Nodes[st.Op.Node]
		}
		outcome, err := r.d.Suppress(st.target(), edge, st.Flag)
		switch {
		case outcome == Committed && exists:
			// A suppression rewrite changes the entity's version, not its value.
			r.hidden[key] = st.Flag
			r.commitKeys([]string{key})
		case outcome == Rejected && exists:
			r.rejectedValid("suppression of an existing entity rejected: %v", err)
		}
		r.settle()
	case Sidecar:
		cur, exists := r.model.Nodes[st.Op.Node]
		if !exists {
			return
		}
		outcome, err := r.d.Sidecar(st.Op.Node, cur)
		switch outcome {
		case Committed:
			r.commitKeys([]string{nodeKey(st.Op.Node)})
		case Rejected:
			r.rejectedValid("sidecar for the current body rejected: %v", err)
		}
		r.settle()
	case Fault:
		if f, ok := r.d.(Faulter); ok {
			if err := f.Fault(st.Fault); err != nil {
				r.violate("fault", "arming %s: %v", st.Fault, err)
			}
		}
	case Arm:
		key := nodeKey(st.Op.Node)
		if st.Op.Edge != "" {
			key = edgeKey(st.Op.Edge)
		}
		r.addArm(&arm{key: key, implicit: st.Implicit})
	}
	r.checkStep()
}

func (r *run) rollbackAll() {
	slots := make([]int, 0, len(r.txs))
	for s := range r.txs {
		slots = append(slots, s)
	}
	sort.Ints(slots)
	for _, s := range slots {
		r.d.Rollback(s)
		delete(r.txs, s)
	}
}

// commitTx commits a transaction and checks the outcome against the model.
func (r *run) commitTx(slot int, x *tx) {
	keys := collection.SortedKeys(x.written)
	conflicted := x.diverged || x.mayReject || x.begin < r.dropSeq || r.changedSince(x.begin, keys)
	var after State
	if x.diverged {
		// Staging followed state committed since begin: replay the staged ops
		// over the current state, as the store applied them.
		after = r.model.clone()
		for i, op := range x.staged {
			op.apply(after, x.vals[i])
		}
	} else {
		// The final image: every entity the transaction wrote, as its view
		// holds it, over the current state.
		after = r.model.clone()
		for _, k := range keys {
			id := k[2:]
			if k[0] == 'n' {
				if v, ok := x.view.Nodes[id]; ok {
					after.Nodes[id] = v
				} else {
					delete(after.Nodes, id)
				}
			} else if e, ok := x.view.Edges[id]; ok {
				after.Edges[id] = e
			} else {
				delete(after.Edges, id)
			}
		}
		// Endpoints a final relationship uses must not have changed under it.
		for _, k := range keys {
			if k[0] != 'e' {
				continue
			}
			if e, ok := x.view.Edges[k[2:]]; ok && r.changedSince(x.begin, []string{nodeKey(e.Start), nodeKey(e.End)}) {
				conflicted = true
			}
		}
	}
	_, invalid := after.dangling()
	full := r.mayOverLimit(after)
	outcome, err := r.d.Commit(slot)
	switch {
	case outcome == Indeterminate:
		r.uncertainState(after, keys)
	case outcome == Committed && invalid:
		r.violate("dangling", "tx%d committed a final graph with a relationship lacking an endpoint: %s", slot, after)
	case outcome == Committed:
		if conflicted && !x.diverged {
			// State the transaction depends on changed since it began: the
			// store may have applied its final image or, reading newer
			// state when it staged, its staged writes over the current
			// state. The next quiescent check adopts the one it holds.
			replay := r.model.clone()
			for i, op := range x.staged {
				op.apply(replay, x.vals[i])
			}
			r.model = replay
			r.uncertainState(after, differingKeys(replay, after))
		} else {
			r.model = after
		}
		for k := range x.deleted {
			delete(r.hidden, k)
		}
		r.commitKeys(keys)
	case !conflicted && !invalid && !full:
		r.rejectedValid("tx%d with a valid final graph %s and no conflicting commit since it began was rejected: %v (staged %v)", slot, after, err, x.staged)
	}
}

// differingKeys returns the keys whose value differs between a and b.
func differingKeys(a, b State) []string {
	var keys []string
	for _, s := range [...]State{a, b} {
		for id := range s.Nodes {
			av, aok := a.Nodes[id]
			bv, bok := b.Nodes[id]
			if aok != bok || av != bv {
				keys = append(keys, nodeKey(id))
			}
		}
		for id := range s.Edges {
			ae, aok := a.Edges[id]
			be, bok := b.Edges[id]
			if aok != bok || ae != be {
				keys = append(keys, edgeKey(id))
			}
		}
	}
	sort.Strings(keys)
	return uniqueSorted(keys)
}

func uniqueSorted(keys []string) []string {
	out := keys[:0]
	for i, k := range keys {
		if i == 0 || k != keys[i-1] {
			out = append(out, k)
		}
	}
	return out
}

// overLimit reports whether s holds more entities than the capacity allows.
func (r *run) overLimit(s State) bool {
	return (r.opts.MaxNodes > 0 && len(s.Nodes) > r.opts.MaxNodes) || (r.opts.MaxEdges > 0 && len(s.Edges) > r.opts.MaxEdges)
}

// exceeds reports whether op applied to s would exceed the capacity.
func (r *run) exceeds(s State, op Op, v int64) bool {
	if r.opts.MaxNodes <= 0 && r.opts.MaxEdges <= 0 {
		return false
	}
	after := s.clone()
	op.apply(after, v)
	return r.overLimit(after)
}

// mayOverLimit reports whether s, with every indeterminate write that may
// still have created an entity counted as created, holds more entities than
// the capacity allows: a store may refuse a write that fits s while a write
// it acknowledged as indeterminate may still take capacity.
func (r *run) mayOverLimit(s State) bool {
	if r.opts.MaxNodes <= 0 && r.opts.MaxEdges <= 0 {
		return false
	}
	nodes, edges := len(s.Nodes), len(s.Edges)
	for k, vs := range r.uncertain {
		if _, ok := s.Nodes[k[2:]]; ok || k[0] != 'n' {
			continue
		}
		for _, v := range vs {
			if v >= 0 {
				nodes++
				break
			}
		}
	}
	for k, bs := range r.uncertainEdges {
		if _, ok := s.Edges[k[2:]]; ok {
			continue
		}
		for _, b := range bs {
			if b.V >= 0 {
				edges++
				break
			}
		}
	}
	return (r.opts.MaxNodes > 0 && nodes > r.opts.MaxNodes) || (r.opts.MaxEdges > 0 && edges > r.opts.MaxEdges)
}

// mayExceed is exceeds with indeterminate writes counted (mayOverLimit).
func (r *run) mayExceed(s State, op Op, v int64) bool {
	if r.opts.MaxNodes <= 0 && r.opts.MaxEdges <= 0 {
		return false
	}
	after := s.clone()
	op.apply(after, v)
	return r.mayOverLimit(after)
}

// checkStep checks what holds after every step.
func (r *run) checkStep() {
	if r.overLimit(r.model) {
		r.violate("quota", "accepted writes leave %d nodes / %d relationships, over the capacity %d / %d", len(r.model.Nodes), len(r.model.Edges), r.opts.MaxNodes, r.opts.MaxEdges)
	}
	if id, ok := r.model.dangling(); ok {
		r.violate("dangling", "model relationship %s lacks an endpoint", id)
	}
	events := r.publishedEvents()
	for _, key := range collection.SortedKeys(events) {
		vs := events[key]
		lastV := int64(-1)
		for _, v := range vs {
			if v >= 0 && v < lastV {
				r.violate("publication-order", "%s published %v: a value older than one already published", key, vs)
				return
			}
			if v >= 0 {
				lastV = v
			}
		}
	}
}

// checkQuiescent compares the store with the model once every acknowledged
// write is applied and every notification delivered.
func (r *run) checkQuiescent(when string) {
	if r.fail != "" {
		return
	}
	nodes, edges := universe()
	obs, err := r.d.Observe(nodes, edges)
	if err != nil {
		r.violate("observe", "%s: %v", when, err)
		return
	}
	r.resolveUncertain(obs.State)
	for _, id := range nodes {
		got, gotOK := obs.State.Nodes[id]
		want, wantOK := r.model.Nodes[id]
		if gotOK != wantOK || got != want {
			r.violate("durability", "%s: node %s is %s, acknowledged %s", when, id, nodeValue(got, gotOK), nodeValue(want, wantOK))
			return
		}
	}
	for _, id := range edges {
		got, gotOK := obs.State.Edges[id]
		want, wantOK := r.model.Edges[id]
		if gotOK != wantOK || got != want {
			r.violate("durability", "%s: relationship %s is %s, acknowledged %s", when, id, edgeValue(got, gotOK), edgeValue(want, wantOK))
			return
		}
	}
	if id, ok := obs.State.dangling(); ok {
		r.violate("dangling", "%s: stored relationship %s lacks an endpoint", when, id)
		return
	}
	events := r.publishedEvents()
	for _, key := range collection.SortedKeys(events) {
		vs := events[key]
		lastV := vs[len(vs)-1]
		id := key[2:]
		want := int64(-1)
		if key[0] == 'n' {
			if v, ok := r.model.Nodes[id]; ok {
				want = v
			}
		} else if e, ok := r.model.Edges[id]; ok {
			want = e.V
		}
		if lastV != want {
			r.violate("publication-order", "%s: the last notification for %s carries %d, its state is %d (published %v)", when, key, lastV, want, vs)
			return
		}
	}
	if u := obs.Usage; u != nil {
		if u.Cached != u.Recount {
			r.violate("quota-usage", "%s: the quota holds %d bytes, a recount of the stored entities %d", when, u.Cached, u.Recount)
			return
		}
		if r.opts.MaxBytes > 0 && u.Recount > r.opts.MaxBytes {
			r.violate("quota", "%s: accepted writes store %d bytes, over the limit %d", when, u.Recount, r.opts.MaxBytes)
			return
		}
	}
	if obs.NodeCount >= 0 && (obs.NodeCount != int64(len(r.model.Nodes)) || obs.EdgeCount != int64(len(r.model.Edges))) {
		r.violate("counters", "%s: counters %d nodes / %d relationships, model %d / %d", when, obs.NodeCount, obs.EdgeCount, len(r.model.Nodes), len(r.model.Edges))
		return
	}
	if obs.HiddenNodes == nil {
		return
	}
	for _, id := range collection.SortedKeys(r.model.Nodes) {
		if got, want := obs.HiddenNodes[id], r.hidden[nodeKey(id)]; got != want {
			r.violate("suppression", "%s: node %s suppressed=%v, model %v", when, id, got, want)
			return
		}
	}
	for _, id := range collection.SortedKeys(r.model.Edges) {
		if got, want := obs.HiddenEdges[id], r.hidden[edgeKey(id)]; got != want {
			r.violate("suppression", "%s: relationship %s suppressed=%v, model %v", when, id, got, want)
			return
		}
	}
}

// resolveUncertain adopts, for every key an indeterminate write touched, the
// store's value if it is one the write could have left.
func (r *run) resolveUncertain(got State) {
	for _, k := range collection.SortedKeys(r.uncertain) {
		id := k[2:]
		v, ok := got.Nodes[id]
		if !ok {
			v = -1
		}
		for _, alt := range r.uncertain[k] {
			if alt == v {
				if ok {
					r.model.Nodes[id] = v
				} else {
					r.model.deleteNode(id)
				}
				break
			}
		}
	}
	for _, k := range collection.SortedKeys(r.uncertainEdges) {
		id := k[2:]
		e, ok := got.Edges[id]
		if !ok {
			e = EdgeBody{V: -1}
		}
		for _, alt := range r.uncertainEdges[k] {
			if alt == e {
				if ok {
					r.model.Edges[id] = e
				} else {
					delete(r.model.Edges, id)
				}
				break
			}
		}
	}
	r.uncertain, r.uncertainEdges = nil, nil
}

func nodeValue(v int64, ok bool) string {
	if !ok {
		return "absent"
	}
	return fmt.Sprintf("v=%d", v)
}

func edgeValue(e EdgeBody, ok bool) string {
	if !ok {
		return "absent"
	}
	return fmt.Sprintf("%s->%s v=%d", e.Start, e.End, e.V)
}

// Run runs steps against d and returns the first violation ("class: detail",
// empty if none) and the trace of steps and listener writes.
func Run(d Driver, steps []Step) (fail string, trace []string) {
	return RunOptions(d, steps, Options{})
}

// RunOptions is Run with opts.
func RunOptions(d Driver, steps []Step, opts Options) (fail string, trace []string) {
	r := &run{d: d, model: NewState(), last: map[string]int64{}, txs: map[int]*tx{}, backups: map[int]State{},
		hidden: map[string]bool{}, backupHidden: map[int]map[string]bool{}, opts: opts}
	r.listened = sync.NewCond(&r.cb)
	if !r.open() {
		return r.fail, r.traceLines()
	}
	defer func() {
		r.rollbackAll()
		_ = d.Close()
	}()
	for _, st := range steps {
		if !d.Supports(st.Kind) {
			continue
		}
		watchdog := time.AfterFunc(StepTimeout, func() { hung(r.traceLines()) })
		r.step(st)
		watchdog.Stop()
		if r.fail != "" {
			break
		}
	}
	return r.fail, r.traceLines()
}

// StepTimeout bounds one step. A step still running then is a deadlock: the
// program's steps and every goroutine's stack are printed and the process
// exits, instead of the package's whole timeout passing in silence.
var StepTimeout = 2 * time.Minute

func hung(trace []string) {
	buf := make([]byte, 64<<20)
	buf = buf[:runtime.Stack(buf, true)]
	fmt.Fprintf(os.Stderr, "writeordertest: a step ran longer than %v (deadlock)\nsteps:\n  %s\n\n%s\n", StepTimeout, strings.Join(trace, "\n  "), buf)
	os.Exit(2)
}

// Class returns a violation's class.
func Class(fail string) string {
	if i := strings.Index(fail, ":"); i >= 0 {
		return fail[:i]
	}
	return fail
}

// Populate is the start every generated program and schedule shares: every
// node, then relationships n0->n1, n1->n2, n2->n3, flushed.
func Populate() []Step {
	var steps []Step
	for i := 0; i < Nodes; i++ {
		steps = append(steps, Step{Kind: Direct, Op: Op{Kind: CreateNode, Node: NodeName(i)}})
	}
	for i := 0; i < Edges; i++ {
		steps = append(steps, Step{Kind: Direct, Op: Op{Kind: CreateEdge, Edge: EdgeName(i), Start: NodeName(i), End: NodeName((i + 1) % Nodes)}})
	}
	return append(steps, Step{Kind: Flush})
}

// Generate returns a deterministic program of about length steps from seed.
func Generate(seed int64, length int) []Step {
	rng := rand.New(rand.NewSource(seed))
	node := func() string { return NodeName(rng.Intn(Nodes)) }
	edge := func() string { return EdgeName(rng.Intn(Edges)) }
	op := func() Op {
		switch rng.Intn(6) {
		case 0:
			return Op{Kind: CreateNode, Node: node()}
		case 1:
			return Op{Kind: UpdateNode, Node: node()}
		case 2:
			return Op{Kind: DeleteNode, Node: node()}
		case 3:
			return Op{Kind: CreateEdge, Edge: edge(), Start: node(), End: node()}
		case 4:
			return Op{Kind: UpdateEdge, Edge: edge(), Start: node(), End: node()}
		default:
			return Op{Kind: DeleteEdge, Edge: edge()}
		}
	}
	steps := Populate()
	open := map[int]bool{}
	openSlots := func() []int {
		slots := make([]int, 0, len(open))
		for s := range open {
			slots = append(slots, s)
		}
		sort.Ints(slots)
		return slots
	}
	backups := 0
	for len(steps) < length {
		switch k := rng.Intn(100); {
		case k < 22:
			steps = append(steps, Step{Kind: Direct, Op: op(), Implicit: rng.Intn(3) == 0})
		case k < 40:
			slot := rng.Intn(3)
			if open[slot] {
				steps = append(steps, Step{Kind: Commit, Slot: slot})
				delete(open, slot)
				continue
			}
			open[slot] = true
			steps = append(steps, Step{Kind: Begin, Slot: slot, Implicit: rng.Intn(2) == 0})
		case k < 62:
			if slots := openSlots(); len(slots) > 0 {
				steps = append(steps, Step{Kind: Stage, Slot: slots[rng.Intn(len(slots))], Op: op()})
			}
		case k < 70:
			if slots := openSlots(); len(slots) > 0 {
				s := slots[rng.Intn(len(slots))]
				steps = append(steps, Step{Kind: Commit, Slot: s})
				delete(open, s)
			}
		case k < 77:
			steps = append(steps, Step{Kind: Flush})
		case k < 80:
			open = map[int]bool{}
			steps = append(steps, Step{Kind: Reopen})
		case k < 82:
			steps = append(steps, Step{Kind: DeletePrefix})
		case k < 84:
			steps = append(steps, Step{Kind: Backup, Slot: backups})
			backups++
		case k < 86:
			if backups > 0 {
				open = map[int]bool{}
				steps = append(steps, Step{Kind: Restore, Slot: rng.Intn(backups)})
			}
		case k < 90:
			st := Step{Kind: Suppress, Flag: rng.Intn(3) != 0}
			if rng.Intn(2) == 0 {
				st.Op.Node = node()
			} else {
				st.Op.Edge = edge()
			}
			steps = append(steps, st)
		case k < 93:
			steps = append(steps, Step{Kind: Sidecar, Op: Op{Node: node()}})
		default:
			st := Step{Kind: Arm, Implicit: rng.Intn(2) == 0}
			if rng.Intn(2) == 0 {
				st.Op.Node = node()
			} else {
				st.Op.Edge = edge()
			}
			steps = append(steps, st)
		}
	}
	for _, s := range openSlots() {
		steps = append(steps, Step{Kind: Commit, Slot: s})
	}
	return append(steps, Step{Kind: Flush}, Step{Kind: Reopen})
}

// RunPrograms runs the generated programs for seeds against fresh drivers
// from newDriver and fails t once per violation class (at most two programs
// reported per class), naming the seed and its trace.
func RunPrograms(t testing.TB, label string, newDriver func() Driver, seeds []int64, length int) {
	t.Helper()
	classes := map[string]int{}
	for _, seed := range seeds {
		fail, trace := Run(newDriver(), Generate(seed, length))
		if fail == "" {
			continue
		}
		class := Class(fail)
		classes[class]++
		line := fail
		if len(line) > 240 {
			line = line[:240]
		}
		t.Logf("WRITE_ORDER_SEED %s seed=%d %s", label, seed, line)
		if classes[class] <= 2 {
			t.Errorf("%s seed=%d %s\nsteps:\n  %s", label, seed, fail, strings.Join(trace, "\n  "))
		}
	}
	if len(classes) > 0 {
		t.Logf("WRITE_ORDER_MODEL %s failing programs by class: %v (of %d)", label, classes, len(seeds))
	}
}

// Schedule is a named program for one contract case.
type Schedule struct {
	Name  string
	Steps []Step
}

func node(i int) string { return NodeName(i) }

// StorageSchedules are the storage-owned named cases. Each starts from
// Populate (n0..n3; r0 n0->n1, r1 n1->n2, r2 n2->n3).
func StorageSchedules() []Schedule {
	with := func(steps ...Step) []Step { return append(Populate(), steps...) }
	direct := func(op Op) Step { return Step{Kind: Direct, Op: op} }
	implicit := func(op Op) Step { return Step{Kind: Direct, Op: op, Implicit: true} }
	stage := func(slot int, op Op) Step { return Step{Kind: Stage, Slot: slot, Op: op} }
	begin := func(slot int, implicit bool) Step { return Step{Kind: Begin, Slot: slot, Implicit: implicit} }
	commit := func(slot int) Step { return Step{Kind: Commit, Slot: slot} }
	flush, reopen := Step{Kind: Flush}, Step{Kind: Reopen}
	var out []Schedule
	for _, implicitTx := range []bool{false, true} {
		suffix := "Explicit"
		if implicitTx {
			suffix = "Implicit"
		}
		// The final relationship no longer uses the node the transaction deletes.
		out = append(out,
			Schedule{Name: "MovedNewRelationshipThenDeletedFormerEndpoint" + suffix, Steps: with(
				direct(Op{Kind: DeleteEdge, Edge: "r1"}),
				begin(0, implicitTx),
				stage(0, Op{Kind: CreateEdge, Edge: "r1", Start: node(0), End: node(2)}),
				stage(0, Op{Kind: UpdateEdge, Edge: "r1", Start: node(0), End: node(3)}),
				stage(0, Op{Kind: DeleteNode, Node: node(2)}),
				commit(0), flush, reopen)},
			Schedule{Name: "MovedStoredRelationshipThenDeletedFormerEndpoint" + suffix, Steps: with(
				begin(0, implicitTx),
				stage(0, Op{Kind: UpdateEdge, Edge: "r0", Start: node(0), End: node(2)}),
				stage(0, Op{Kind: UpdateEdge, Edge: "r0", Start: node(0), End: node(3)}),
				stage(0, Op{Kind: DeleteEdge, Edge: "r1"}),
				stage(0, Op{Kind: DeleteEdge, Edge: "r2"}),
				stage(0, Op{Kind: DeleteNode, Node: node(2)}),
				commit(0), flush, reopen)},
		)
	}
	out = append(out,
		// An acknowledged create, still buffered, is deleted by a later prefix delete.
		Schedule{Name: "PrefixDeleteRetiresAcknowledgedCreate", Steps: append(with(
			direct(Op{Kind: DeleteNode, Node: node(3)}), flush,
			implicit(Op{Kind: CreateNode, Node: node(3)}),
			Step{Kind: DeletePrefix}), flush, reopen)},
		Schedule{Name: "PrefixDeleteRetiresAcknowledgedUpdate", Steps: with(
			implicit(Op{Kind: UpdateNode, Node: node(1)}),
			Step{Kind: DeletePrefix}, flush, reopen)},
		// A restore replaces an acknowledged update still buffered.
		Schedule{Name: "RestoreRetiresAcknowledgedUpdate", Steps: with(
			Step{Kind: Backup, Slot: 0},
			implicit(Op{Kind: UpdateNode, Node: node(1)}),
			Step{Kind: Restore, Slot: 0}, flush, reopen)},
		// A listener of the drained update writes a newer body before the
		// suppression's own notification.
		Schedule{Name: "ManualSuppressionPublishesAfterDrainedListenerWriteNode", Steps: with(
			implicit(Op{Kind: UpdateNode, Node: node(1)}),
			Step{Kind: Arm, Op: Op{Node: node(1)}},
			Step{Kind: Suppress, Op: Op{Node: node(1)}, Flag: true}, flush, reopen)},
		Schedule{Name: "ManualSuppressionPublishesAfterDrainedListenerWriteEdge", Steps: with(
			implicit(Op{Kind: UpdateEdge, Edge: "r1", Start: node(1), End: node(2)}),
			Step{Kind: Arm, Op: Op{Edge: "r1"}},
			Step{Kind: Suppress, Op: Op{Edge: "r1"}, Flag: true}, flush, reopen)},
		// Close's final drain delivers a listener that acknowledges a write:
		// it must survive the reopen (or be rejected).
		Schedule{Name: "CloseKeepsListenerAcknowledgedWrite", Steps: with(
			implicit(Op{Kind: UpdateNode, Node: node(1)}),
			Step{Kind: Arm, Op: Op{Node: node(1)}, Implicit: true},
			reopen)},
		// A direct relationship create sees acknowledged, still buffered endpoints.
		Schedule{Name: "AcknowledgedEndpointsAvailableToDirectCreateEdge", Steps: append(with(
			direct(Op{Kind: DeleteEdge, Edge: "r0"}),
			direct(Op{Kind: DeleteNode, Node: node(0)}), flush,
			implicit(Op{Kind: CreateNode, Node: node(0)}),
			direct(Op{Kind: CreateEdge, Edge: "r0", Start: node(0), End: node(1)})), flush, reopen)},
	)
	return append(out, minimizedSchedules()...)
}

// minimizedSchedules are generated programs the minimizer reduced, each from
// an empty store, and the cases they led to.
func minimizedSchedules() []Schedule {
	direct := func(op Op) Step { return Step{Kind: Direct, Op: op} }
	implicit := func(op Op) Step { return Step{Kind: Direct, Op: op, Implicit: true} }
	stage := func(slot int, op Op) Step { return Step{Kind: Stage, Slot: slot, Op: op} }
	begin := func(slot int, implicit bool) Step { return Step{Kind: Begin, Slot: slot, Implicit: implicit} }
	commit := func(slot int) Step { return Step{Kind: Commit, Slot: slot} }
	suppress := func(op Op, flag bool) Step { return Step{Kind: Suppress, Op: op, Flag: flag} }
	flush, reopen := Step{Kind: Flush}, Step{Kind: Reopen}
	cn := func(i int) Op { return Op{Kind: CreateNode, Node: node(i)} }
	un := func(i int) Op { return Op{Kind: UpdateNode, Node: node(i)} }
	dn := func(i int) Op { return Op{Kind: DeleteNode, Node: node(i)} }
	ce := func(edge string, start, end int) Op {
		return Op{Kind: CreateEdge, Edge: edge, Start: node(start), End: node(end)}
	}
	ue := func(edge string, start, end int) Op {
		return Op{Kind: UpdateEdge, Edge: edge, Start: node(start), End: node(end)}
	}
	de := func(edge string) Op { return Op{Kind: DeleteEdge, Edge: edge} }
	out := []Schedule{
		// A restore brings back the backup's counters with its graph.
		{Name: "RestoredCountersSurviveDelete", Steps: []Step{
			direct(cn(2)), direct(cn(3)), direct(ce("r2", 2, 3)),
			{Kind: Backup, Slot: 0}, reopen, {Kind: Restore, Slot: 0},
			direct(dn(2)), reopen}},
		// A transaction moves a relationship that a buffered node delete
		// cascades to first: its commit re-creates it, and counts it.
		{Name: "CommitOverBufferedCascadeCountsRelationship", Steps: []Step{
			direct(cn(0)), direct(cn(1)), direct(cn(2)), direct(ce("r1", 1, 2)),
			begin(0, true), stage(0, ue("r1", 0, 0)),
			implicit(dn(2)), commit(0), flush}},
		// A node delete cascades to the relationships attached when it
		// commits, not to one an acknowledged commit moved away.
		{Name: "SynchronousDeleteKeepsRelationshipMovedAway", Steps: []Step{
			direct(cn(0)), direct(cn(1)), direct(cn(3)), direct(ce("r0", 0, 1)),
			begin(0, true), stage(0, ue("r0", 3, 1)), commit(0),
			begin(0, false), stage(0, dn(0)), commit(0), reopen}},
		{Name: "BufferedDeleteKeepsRelationshipMovedAway", Steps: []Step{
			direct(cn(0)), direct(cn(2)), direct(cn(3)), direct(ce("r2", 2, 3)),
			implicit(ue("r2", 0, 2)), implicit(dn(3)), reopen}},
		// A transaction deletes a node and writes it again: the counters keep it.
		{Name: "DeleteThenUpdateNodeInOneTransactionCounts", Steps: []Step{
			direct(cn(3)), begin(1, false), direct(cn(2)),
			stage(1, dn(3)), stage(1, un(3)), commit(1), flush}},
		// A listener's write delivered after a prefix delete finds nothing to update.
		{Name: "ListenerWriteAfterPrefixDelete", Steps: []Step{
			direct(cn(1)), direct(cn(2)), direct(ce("r1", 1, 2)),
			{Kind: Arm, Op: Op{Edge: "r1"}, Implicit: true},
			direct(cn(0)), begin(1, true), stage(1, ue("r1", 1, 0)), commit(1),
			{Kind: Backup, Slot: 1}, {Kind: DeletePrefix}, flush, reopen}},
		// An acknowledged re-create of a deleted node is an endpoint for the
		// next transaction.
		{Name: "RecreatedNodeIsEndpointForNextTransaction", Steps: []Step{
			direct(cn(1)), direct(dn(1)), implicit(cn(1)), implicit(ce("r0", 1, 1)), flush, reopen}},
		// A listener of the commit a sidecar write drains writes the same node.
		{Name: "SidecarDeliversAfterReleasingItsOrder", Steps: []Step{
			direct(cn(1)), flush, implicit(un(1)),
			{Kind: Arm, Op: Op{Node: node(1)}, Implicit: true},
			{Kind: Sidecar, Op: Op{Node: node(1)}}, flush, reopen}},
		// A delete ends its entity's suppression, durably: the entity created
		// again after a reopen is not hidden.
		{Name: "SuppressionEndsWithItsNodeAcrossReopen", Steps: []Step{
			direct(cn(0)), suppress(Op{Node: node(0)}, true), direct(dn(0)), reopen,
			direct(cn(0)), flush, reopen}},
		{Name: "SuppressionEndsWithItsRelationshipAcrossReopen", Steps: []Step{
			direct(cn(0)), direct(cn(1)), direct(ce("r0", 0, 1)), suppress(Op{Edge: "r0"}, true),
			direct(de("r0")), reopen, direct(ce("r0", 0, 1)), flush, reopen}},
		// A prefix delete ends the suppression of every entity it deletes.
		{Name: "SuppressionEndsWithPrefixDelete", Steps: []Step{
			direct(cn(0)), direct(cn(1)), direct(ce("r0", 0, 1)),
			suppress(Op{Node: node(0)}, true), suppress(Op{Edge: "r0"}, true), {Kind: DeletePrefix},
			direct(cn(0)), direct(cn(1)), direct(ce("r0", 0, 1)), flush, reopen}},
	}
	// The end of a deleted entity's suppression never removes the
	// suppression of the entity created again with its ID.
	node0 := []Step{direct(cn(0))}
	edge0 := []Step{direct(cn(0)), direct(cn(1)), direct(ce("r0", 0, 1))}
	for i := 0; i < 25; i++ {
		node0 = append(node0, suppress(Op{Node: node(0)}, true), direct(dn(0)), direct(cn(0)), suppress(Op{Node: node(0)}, true))
		edge0 = append(edge0, suppress(Op{Edge: "r0"}, true), direct(de("r0")), direct(ce("r0", 0, 1)), suppress(Op{Edge: "r0"}, true))
	}
	return append(out,
		Schedule{Name: "ResuppressedNodeKeepsSuppression", Steps: append(node0, flush, reopen)},
		Schedule{Name: "ResuppressedRelationshipKeepsSuppression", Steps: append(edge0, flush, reopen)})
}

// RunSchedules runs each schedule as a subtest against a fresh driver.
func RunSchedules(t *testing.T, schedules []Schedule, newDriver func() Driver) {
	t.Helper()
	for _, s := range schedules {
		s := s
		t.Run(s.Name, func(t *testing.T) {
			if fail, trace := Run(newDriver(), s.Steps); fail != "" {
				t.Errorf("%s\nsteps:\n  %s", fail, strings.Join(trace, "\n  "))
			}
		})
	}
}

// Minimize returns a shortest program it finds, by removing steps, that
// still fails with fail's class, and that failure.
func Minimize(newDriver func() Driver, steps []Step, fail string) ([]Step, string) {
	class := Class(fail)
	for chunk := len(steps) / 2; chunk >= 1; chunk /= 2 {
		for i := 0; i+chunk <= len(steps); {
			trial := append(append([]Step(nil), steps[:i]...), steps[i+chunk:]...)
			if f, _ := Run(newDriver(), trial); f != "" && Class(f) == class {
				steps, fail = trial, f
				continue
			}
			i += chunk
		}
	}
	return steps, fail
}
