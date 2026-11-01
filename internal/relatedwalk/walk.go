// Package relatedwalk owns the single bounded breadth-first traversal used to
// discover nodes related to a search result.
//
// Three read surfaces publish related nodes: the MCP discover tool, the server
// Heimdall database router and the Heimdall query executor. Each carried its own
// copy of the same walk. The server router and the query executor only checked
// their result cap between adjacency reads, so one wide node could publish more
// records than the caller's limit; the MCP copy already enforced its 50-record
// cap inside both adjacency loops. This package is the one owner of the queue,
// visited set, depth bound, result cap and cancellation polling. Consumers keep
// their own neighborhood model (storage engine or semantic searcher) through
// Source, and their own node loading, projection and acceptance rules through
// Acceptor, so no graph is converted or materialized as an intermediate
// representation.
//
// Example usage:
//
//	walk := relatedwalk.Walk[heimdall.RelatedNode]
//	records, err := walk(ctx, relatedwalk.StorageSource{Engine: engine}, accept,
//		nodeID, relatedwalk.Options{MaxHops: depth, Limit: 50})
package relatedwalk

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cancelprobe"
)

// Direction identifies which side of the discovering relationship a neighbor
// sits on: Outgoing edges leave the expanded node, Incoming edges enter it.
type Direction uint8

const (
	Outgoing Direction = iota
	Incoming
)

// String returns the wire label shared by every related-node response.
func (d Direction) String() string {
	if d == Incoming {
		return "incoming"
	}
	return "outgoing"
}

// Neighbor is the first discovery of one node reached from the traversal
// frontier.
//
// The walk reuses a single Neighbor per walk and passes its address to
// Acceptor.Discover, so an acceptor must copy the fields it keeps and must not
// retain the pointer. Path is nil unless Options.Paths is set; when set it holds
// the ids from the start node through ID, both included, in Options.PathID's
// output space. Path slices are immutable once published, so an acceptor may
// retain one in a result record but must not modify it.
type Neighbor struct {
	// ID is the neighbor identity in the Source's id space.
	ID string
	// Relationship is the type of the edge that discovered the neighbor.
	Relationship string
	// Direction is the side of that edge the neighbor sits on.
	Direction Direction
	// Distance is the number of hops from the start node.
	Distance int
	// Path is the traversal path as described on the type.
	Path []string
}

// Options configures one walk.
type Options struct {
	// MaxHops bounds the distance of published neighbors. MaxHops < 1
	// publishes nothing.
	MaxHops int
	// Limit caps the number of published records. The walk stops reading
	// adjacency and nodes as soon as the cap is reached, including inside a
	// single adjacency. Limit < 1 publishes nothing.
	Limit int
	// Paths enables Neighbor.Path. Consumers that never publish a path leave
	// it false so no path slice is allocated.
	Paths bool
	// PathID maps a Source id to the id published in Neighbor.Path. A nil
	// PathID keeps Source ids. It is only consulted when Paths is set.
	PathID func(string) string
}

// Source enumerates the incident edges of one node in the order the consumer's
// storage contract defines. Implementations use ctx for every read and stop
// enumerating as soon as visit.Step reports false.
type Source interface {
	// EachIncidentEdge reports every edge incident on nodeID through visit.
	//
	// A record the implementation cannot interpret, such as a nil edge or an
	// edge without a usable endpoint, is still reported, with an empty
	// neighborID: the walk counts every examined record for its cancellation
	// poll, so an adjacency made of unusable records must not let the
	// implementation loop without observing a cancellation. Records that carry
	// an endpoint are reported with that endpoint, the edge type and the
	// direction of the list they came from.
	//
	// A returned error reports an adjacency read failure. The walk treats a
	// non-cancellation failure as best-effort: the neighbors reported before the
	// failure are kept and the node contributes nothing further. An
	// implementation must aim to report the rows an errored read returned when
	// its consumer contract keeps them.
	EachIncidentEdge(ctx context.Context, nodeID string, visit StepVisitor) error
}

// StepVisitor receives one examined incident record at a time from a Source.
// Step returns false when the walk must stop enumerating records, either because
// the result cap was reached or because the caller's context is done.
type StepVisitor interface {
	Step(neighborID, relationship string, direction Direction) bool
}

// Acceptor is the consumer boundary of a walk. Discover is called once for the
// first discovery of each neighbor, after that neighbor is marked visited and
// before it is expanded, and owns node loading plus consumer projection.
//
// It returns the record to publish, whether to publish it and whether the
// neighbor is expanded. A missing node is consumer policy: some callers publish
// a placeholder record and still expand, others skip it entirely.
type Acceptor[R any] interface {
	Discover(ctx context.Context, n *Neighbor) (record R, emit bool, expand bool)
}

// cancelPollMask is the amortized cancellation interval inside one adjacency:
// the walk observes the caller's context after every cancelPollMask+1 examined
// records and at every traversal boundary.
//
// The poll cannot be left to the reads alone. A record that is already visited,
// has no usable neighbor, or is rejected by the consumer never performs a
// context-aware read, so one wide adjacency of those records would examine and
// load every node before the walk noticed a cancellation; the result cap does
// not bound that work either, because nothing is published. Polling on the
// cached Done channel every 256 records costs one increment, one bitwise and and
// one predictable branch, while a select on every record is measurable on the
// related-node workloads. 256 examined records bounds the work performed after a
// cancellation to a fixed, documented interval.
const cancelPollMask = 255

func identityPathID(id string) string { return id }

// frontierNode is one queued node of the traversal. path is nil when paths are
// disabled.
type frontierNode struct {
	id       string
	distance int
	path     []string
}

type walk[R any, A Acceptor[R], S Source] struct {
	ctx      context.Context
	done     <-chan struct{}
	source   S
	accept   A
	maxHops  int
	limit    int
	paths    bool
	pathID   func(string) string
	visited  map[string]struct{}
	queue    []frontierNode
	records  []R
	reserve  int
	examined uint
	frame    frame[R, A, S]
	canceled bool
}

// frame is the single reusable StepVisitor and Neighbor buffer of one walk. It
// carries the cursor of the node whose adjacency is being enumerated, so the
// walk needs no per-node closure, visitor or neighbor allocation.
type frame[R any, A Acceptor[R], S Source] struct {
	walk     *walk[R, A, S]
	distance int
	path     []string
	neighbor Neighbor
}

// Step implements StepVisitor.
func (f *frame[R, A, S]) Step(neighborID, relationship string, direction Direction) bool {
	w := f.walk
	if w.canceled {
		return false
	}
	// Every examined record advances the poll, including empty, duplicate,
	// rejected and visited ones: those are exactly the records that would
	// otherwise let a wide adjacency run without observing a cancellation.
	w.examined++
	if w.examined&cancelPollMask == 0 && w.observeCancel() {
		return false
	}
	if neighborID == "" {
		return true
	}
	if _, seen := w.visited[neighborID]; seen {
		return true
	}
	w.visited[neighborID] = struct{}{}

	var path []string
	if w.paths {
		path = make([]string, len(f.path)+1)
		copy(path, f.path)
		path[len(f.path)] = w.pathID(neighborID)
	}

	n := &f.neighbor
	n.ID = neighborID
	n.Relationship = relationship
	n.Direction = direction
	n.Distance = f.distance + 1
	n.Path = path

	record, emit, expand := w.accept.Discover(w.ctx, n)
	if emit {
		if w.records == nil {
			w.records = make([]R, 0, w.reserve)
		}
		w.records = append(w.records, record)
	}
	if expand && n.Distance < w.maxHops {
		w.queue = append(w.queue, frontierNode{id: neighborID, distance: n.Distance, path: path})
	}
	return len(w.records) < w.limit
}

// observeCancel reports whether the caller's context is done. The done channel is
// the one the context returned when the walk started, so the poll never allocates
// and never calls ctx.Err per record; a nil done channel means the context can
// never be canceled, which keeps healthy background walks free of the select.
func (w *walk[R, A, S]) observeCancel() bool {
	if w.canceled {
		return true
	}
	if cancelprobe.Ready(w.done) {
		w.canceled = true
		return true
	}
	return false
}

// Walk performs the canonical bounded breadth-first traversal from start and
// returns the records accepted by accept in discovery order.
//
// The traversal is breadth-first with a first-discovery-wins visited set, so
// cycles, self loops and re-discovered nodes are reported exactly once, at the
// distance of their first discovery. Records are published while the walk runs;
// the cap is enforced inside each adjacency, so a consumer never sees more than
// Options.Limit records, and no adjacency or node is read once the cap is
// reached.
//
// ctx must be non-nil, like every ordinary context-taking function; the walk uses
// it for every source read and every acceptor node load. Cancellation is observed
// before the first source read, at every traversal boundary and, amortized, inside
// each adjacency. When the caller's context is canceled or its deadline expires,
// Walk returns no records and ctx.Err(), so errors.Is distinguishes
// context.Canceled from context.DeadlineExceeded and a consumer never publishes a
// partial collection.
func Walk[R any, A Acceptor[R], S Source](ctx context.Context, source S, accept A, start string, opts Options) ([]R, error) {
	if start == "" || opts.MaxHops < 1 || opts.Limit < 1 {
		return nil, nil
	}

	// Small reservations cover an ordinary local neighborhood without
	// reserving the whole cap for the many walks that stop after a few nodes.
	reserve := opts.Limit
	if reserve > 16 {
		reserve = 16
	}
	w := &walk[R, A, S]{
		ctx:     ctx,
		done:    ctx.Done(),
		source:  source,
		accept:  accept,
		maxHops: opts.MaxHops,
		limit:   opts.Limit,
		paths:   opts.Paths,
		pathID:  opts.PathID,
		visited: make(map[string]struct{}, reserve+1),
		queue:   make([]frontierNode, 1, reserve),
		reserve: reserve,
	}
	if w.paths && w.pathID == nil {
		w.pathID = identityPathID
	}
	w.frame.walk = w
	w.queue[0] = frontierNode{id: start}
	w.visited[start] = struct{}{}
	if w.paths {
		w.queue[0].path = []string{w.pathID(start)}
	}

	for len(w.queue) > 0 {
		if len(w.records) >= w.limit {
			break
		}
		// Before any read of the next node: this is also the check that runs
		// before the very first source read.
		if w.observeCancel() {
			return nil, w.ctx.Err()
		}
		current := w.queue[0]
		w.queue = w.queue[1:]
		if current.distance >= w.maxHops {
			continue
		}
		w.frame.distance = current.distance
		w.frame.path = current.path
		// A source read failure is best-effort by contract: the node
		// contributes the neighbors it reported and the walk continues.
		_ = w.source.EachIncidentEdge(ctx, current.id, &w.frame)
	}
	// Before publication: a walk canceled after its last adjacency still
	// publishes nothing.
	if w.observeCancel() {
		return nil, w.ctx.Err()
	}
	return w.records, nil
}
