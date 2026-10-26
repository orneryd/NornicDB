// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package search

import (
	"context"
	"maps"
	"strings"
	"sync"
	"weak"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// FulltextDocument is an immutable document supplied by a query dialect. The
// service owns its maintenance; the dialect owns its existing matching/scoring.
// Text is the canonical scoring body, and Value holds the dialect's field view.
type FulltextDocument struct {
	Text   string
	Tokens int
	Value  any
	// Unscored excludes an entry from scoring statistics and evaluation.
	// Ordered row consumers do not construct scoring entries.
	Unscored bool
}

// FulltextDocumentBuilder adapts a stored node or relationship to a dialect's
// document. Exactly one argument is non-nil. A nil result excludes the entity.
type FulltextDocumentBuilder func(*storage.Node, *storage.Edge) *FulltextDocument

// FulltextDocumentScope describes one schema projection and its read view.
// Fresh selects a request-scoped view for time-dependent visibility. Retain
// tells the service which named schema projections still exist.
type FulltextDocumentScope struct {
	Name, Definition string
	Relationships    bool
	Fresh            bool
	Ordered          bool
	// BeforeBuild runs after successful acquisition and before construction.
	// False completes the request without constructing or admitting a corpus.
	BeforeBuild func() (bool, error)
	Retain      func(string) bool
}

type fulltextDocumentView struct {
	engine        storage.Engine
	definition    string
	relationships bool
	build         FulltextDocumentBuilder
	rows          []*FulltextDocument
	documents     map[string]*FulltextDocument
	postings      map[uint32]map[string]struct{}
	tokens        int64
	bodyBytes     int64
	retiredWeight int64
	revision      storage.SnapshotReadRevision
	reader        storage.SnapshotReader
	events        uint64
	initialized   bool
	covered       bool
	indexed       int
	order         []string
	orderDirty    bool
}

// FulltextDocuments presents either leased complete statistics or an ordered
// acquisition for this request. Callers must not retain it or mutate documents.
type FulltextDocuments struct {
	view        *fulltextDocumentView
	ctx         context.Context
	acquisition fulltextAcquisition
}

// Count and AverageLength describe the complete scoped corpus, before limits
// and eligibility filtering, as required by the dialect's score calculation.
func (d FulltextDocuments) Count() int { return d.view.indexed }
func (d FulltextDocuments) AverageLength() float64 {
	if d.Count() == 0 {
		return 100
	}
	return float64(d.view.tokens) / float64(d.Count())
}

// Each supplies every scoring document to the dialect's existing evaluator.
// Body postings are never a substitute for the evaluator's field population.
func (d FulltextDocuments) Each(visit func(string, *FulltextDocument)) {
	if d.view.documents == nil {
		for i, doc := range d.view.rows {
			if d.ctx.Err() != nil {
				return
			}
			if !doc.Unscored {
				visit(d.view.order[i], doc)
			}
		}
		return
	}
	if !d.view.orderDirty {
		for _, id := range d.view.order {
			if d.ctx.Err() != nil {
				return
			}
			if doc := d.view.documents[id]; doc != nil && !doc.Unscored {
				visit(id, doc)
			}
		}
		return
	}
	for id, doc := range d.view.documents {
		if d.ctx.Err() != nil {
			return
		}
		if !doc.Unscored {
			visit(id, doc)
		}
	}
}

// VisitNodes consumes the materialized node reader. VisitEdges instead carries
// the consumer's stop directly into the issued native/fallback edge reader.
func (d FulltextDocuments) VisitNodes(visit func(*storage.Node) error) error {
	return d.acquisition.visitNodes(d.ctx, visit)
}
func (d FulltextDocuments) VisitEdges(visit func(*storage.Edge) error) error {
	return d.acquisition.visitEdges(d.ctx, visit)
}

// BodyDocumentFrequency counts literal substrings in the scoring body only.
// Trigram lookup is followed by the release's exact strings.Contains predicate.
func (d FulltextDocuments) BodyDocumentFrequency(term string) int {
	var smallest map[string]struct{}
	if d.view.postings != nil {
		for i := 0; i+3 <= len(term); i++ {
			posting := d.view.postings[fulltextGram(term, i)]
			if len(posting) == 0 {
				return 0
			}
			if smallest == nil || len(posting) < len(smallest) {
				smallest = posting
			}
		}
	}
	result := 0
	if smallest == nil {
		d.Each(func(_ string, doc *FulltextDocument) {
			if strings.Contains(doc.Text, term) {
				result++
			}
		})
	} else {
		for id := range smallest {
			if d.ctx.Err() != nil {
				return result
			}
			if strings.Contains(d.view.documents[id].Text, term) {
				result++
			}
		}
	}
	return result
}

func fulltextGram(text string, i int) uint32 {
	return uint32(text[i])<<16 | uint32(text[i+1])<<8 | uint32(text[i+2])
}

func (v *fulltextDocumentView) replace(id string, doc *FulltextDocument) {
	if previous := v.documents[id]; previous != nil {
		if !previous.Unscored {
			v.indexed--
		}
		v.tokens -= int64(previous.Tokens)
		v.bodyBytes -= int64(len(previous.Text))
		v.retiredWeight += int64(len(previous.Text)) + 1
		for i := 0; i+3 <= len(previous.Text); i++ {
			gram := fulltextGram(previous.Text, i)
			delete(v.postings[gram], id)
			if len(v.postings[gram]) == 0 {
				delete(v.postings, gram)
			}
		}
		delete(v.documents, id)
	}
	if doc == nil {
		return
	}
	if v.documents == nil {
		v.rows = append(v.rows, doc)
		v.order = append(v.order, id)
	} else {
		v.documents[id] = doc
	}
	if !doc.Unscored {
		v.indexed++
	}
	v.tokens += int64(doc.Tokens)
	v.bodyBytes += int64(len(doc.Text))
	if v.postings != nil {
		for i := 0; i+3 <= len(doc.Text); i++ {
			gram := fulltextGram(doc.Text, i)
			if v.postings[gram] == nil {
				v.postings[gram] = make(map[string]struct{})
			}
			v.postings[gram][id] = struct{}{}
		}
	}
}

type fulltextDocumentProjection struct {
	// A projection lease is cancellable and never serializes another index's
	// population. Only its holder accesses view; changes protects count/events.
	lease  chan struct{}
	view   *fulltextDocumentView
	count  int
	events uint64
}

type fulltextDocumentOwner struct {
	// changes protects the journal and projection registry/retention metadata.
	// No storage scan or query evaluation runs under this mutex.
	changes          sync.Mutex
	views            map[string]*fulltextDocumentProjection
	nodes            map[storage.NodeID]uint64
	edges            map[storage.EdgeID]uint64
	events           uint64
	reset            uint64
	dirtyLimit       int
	subscribed       bool
	subscriptionOnce sync.Once
}

func newFulltextDocumentOwner() *fulltextDocumentOwner {
	return &fulltextDocumentOwner{views: make(map[string]*fulltextDocumentProjection)}
}

func (o *fulltextDocumentOwner) subscribe(engine storage.Engine) {
	o.subscriptionOnce.Do(func() {
		// Storage subscriptions outlive a retired service. They must not retain its
		// documents or its storage view after the database drops that service.
		ref := weak.Make(o)
		node := func(id storage.NodeID) {
			if live := ref.Value(); live != nil {
				live.changes.Lock()
				live.events++
				if live.nodes == nil {
					live.nodes = make(map[storage.NodeID]uint64)
				}
				live.nodes[id] = live.events
				live.boundChanges()
				live.changes.Unlock()
			}
		}
		edge := func(id storage.EdgeID) {
			if live := ref.Value(); live != nil {
				live.changes.Lock()
				live.events++
				if live.edges == nil {
					live.edges = make(map[storage.EdgeID]uint64)
				}
				live.edges[id] = live.events
				live.boundChanges()
				live.changes.Unlock()
			}
		}
		o.subscribed = storage.SubscribeEvents(engine, storage.EventHandlers{
			NodeCreated: func(n *storage.Node) { node(n.ID) }, NodeUpdated: func(n *storage.Node) { node(n.ID) }, NodeDeleted: node,
			EdgeCreated: func(e *storage.Edge) { edge(e.ID) }, EdgeUpdated: func(e *storage.Edge) { edge(e.ID) }, EdgeDeleted: edge,
			NamespaceReset: func(string) {
				if live := ref.Value(); live != nil {
					live.changes.Lock()
					live.events++
					live.reset = live.events
					live.changes.Unlock()
				}
			},
		}) == nil
	})
}

// ReadFulltextDocuments serves a named fulltext projection from this search
// service, maintaining it from storage changes independently of query text.
// Definition is the schema's identity, including ordered fields and scope;
// changing or recreating a named index replaces its old projection.
//
// A transactional view without read snapshots is request-scoped. A committed
// wrapper can reuse its own corpus even when the service was created with a
// different wrapper. Each named projection retains only its most recently
// used view, and never serves that view's rows through a different engine.
// A revision admits reuse; an admitted event epoch also permits incremental
// maintenance. Otherwise population uses one snapshot without retrying it.
func (s *Service) ReadFulltextDocuments(ctx context.Context, engine storage.Engine, scope FulltextDocumentScope, build FulltextDocumentBuilder, read func(FulltextDocuments) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Ordered consumers own a traversal, not complete corpus statistics. Their
	// stop must reach acquisition on every call, including fresh/buffered views.
	if scope.Ordered {
		acquisition, close, err := openFulltextAcquisition(ctx, engine, scope)
		if err != nil {
			return err
		}
		defer close()
		if err := read(FulltextDocuments{ctx: ctx, acquisition: acquisition}); err != nil {
			return err
		}
		return ctx.Err()
	}
	prepare := func() (bool, error) {
		if scope.BeforeBuild != nil {
			return scope.BeforeBuild()
		}
		return true, nil
	}
	s.fulltextDocumentsOnce.Do(func() { s.fulltextDocuments = newFulltextDocumentOwner() })
	o := s.fulltextDocuments
	o.changes.Lock()
	if scope.Retain != nil {
		for name := range o.views {
			if !scope.Retain(name) {
				delete(o.views, name)
			}
		}
	}
	projection := o.views[scope.Name]
	if projection == nil {
		projection = &fulltextDocumentProjection{lease: make(chan struct{}, 1)}
		o.views[scope.Name] = projection
	}
	o.changes.Unlock()
	select {
	case projection.lease <- struct{}{}:
		defer func() { <-projection.lease }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	acquisition, close, err := openFulltextAcquisition(ctx, engine, scope)
	if err != nil {
		return err
	}
	defer close()
	querySnapshot, reader := acquisition.snapshot, acquisition.reader
	if scope.Fresh || querySnapshot == nil {
		// Transactional/unsupported views and dynamic visibility are never
		// installed as the committed projection's baseline.
		local := newFulltextDocumentView(scope.Definition, scope.Relationships, build)
		local.reader = reader
		o.changes.Lock()
		if projection.view == nil && o.views[scope.Name] == projection {
			delete(o.views, scope.Name)
		}
		o.changes.Unlock()
		if err := acquisition.materialize(ctx); err != nil {
			return err
		}
		if ready, err := prepare(); err != nil || !ready {
			return err
		}
		if err := local.populate(ctx, &acquisition, true); err != nil {
			return err
		}
		if err := read(FulltextDocuments{view: local, ctx: ctx}); err != nil {
			return err
		}
		return ctx.Err()
	}
	// Only a retained view using this service's journal needs a subscription.
	// Request-local services must not leave permanent weak callback records in
	// an engine every time a transient resolver has no managed service.
	if engine == s.engine {
		o.subscribe(s.engine)
	}
	view := projection.view
	if view == nil || view.engine != engine || view.definition != scope.Definition || view.relationships != scope.Relationships || view.reader != reader {
		view = newFulltextDocumentView(scope.Definition, scope.Relationships, build)
		view.engine = engine
		view.reader = reader
		projection.view = view
	}
	captured, capturedOK := querySnapshot.CapturedRevision()
	settled := engine == s.engine && o.subscribed && storage.EventNotificationsSettled(engine)
	o.changes.Lock()
	events, reset := o.events, o.reset
	nodes := make([]storage.NodeID, 0)
	edges := make([]storage.EdgeID, 0)
	for id, at := range o.nodes {
		if at > view.events {
			nodes = append(nodes, id)
		}
	}
	for id, at := range o.edges {
		if at > view.events {
			edges = append(edges, id)
		}
	}
	o.changes.Unlock()
	// An epoch is certified only across this short capture boundary, never by
	// waiting for a scan-length quiet interval. An unproved capture still serves
	// its coherent snapshot; it simply cannot be reused as a revision baseline.
	end, endOK := querySnapshot.CurrentRevision()
	stable := capturedOK && endOK && end == captured
	covered := settled && stable
	// Revisions also account for visible writes whose notifications are
	// still queued. Only an exactly covered interval may refresh by ID;
	// otherwise read the authoritative store, never a stale projection.
	unchanged := stable && view.initialized && captured.SameReaderView(view.revision)
	graph, previousGraph := captured.GraphMutationVersion(), view.revision.GraphMutationVersion()
	incremental := stable && view.initialized && view.covered && settled && reset <= view.events && events > view.events && graph > previousGraph && graph-previousGraph == events-view.events && captured.SingleReaderCommitAfter(view.revision)
	populated := unchanged
	prepared := false
	if !populated && incremental {
		changes, err := view.acquireChanges(ctx, engine, nodes, edges)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A read failure is the outcome, as it is for a full scan. Only a
		// revision that moved while the changes were read falls back to the
		// full population, because the by-ID reads may then mix revisions.
		if err != nil {
			return err
		}
		end, ok := querySnapshot.CurrentRevision()
		if ok && end == captured {
			if ready, err := prepare(); err != nil || !ready {
				return err
			}
			prepared = true
			if err := view.refresh(ctx, changes); err != nil {
				return err
			}
			populated = true
		}
	}
	if !populated {
		if err := acquisition.materialize(ctx); err != nil {
			return err
		}
		if ready, err := prepare(); err != nil || !ready {
			return err
		}
		prepared = true
		if err := view.populate(ctx, &acquisition, false); err != nil {
			return err
		}
	}
	if !prepared {
		if ready, err := prepare(); err != nil || !ready {
			return err
		}
	}
	view.revision, view.events, view.initialized, view.covered = captured, events, stable, covered || (unchanged && view.covered) || (incremental && populated)
	o.pruneChanges(projection)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := read(FulltextDocuments{view: view, ctx: ctx}); err != nil {
		return err
	}
	return ctx.Err()
}

func newFulltextDocumentView(definition string, relationships bool, build FulltextDocumentBuilder) *fulltextDocumentView {
	return &fulltextDocumentView{definition: definition, relationships: relationships, build: build}
}

func (v *fulltextDocumentView) populate(ctx context.Context, acquisition *fulltextAcquisition, local bool) error {
	v.initialized = false
	v.resetDocuments(0, !local)
	v.order, v.orderDirty = nil, false
	return acquisition.each(ctx, func(node *storage.Node, edge *storage.Edge) error {
		id := ""
		if node != nil {
			id = string(node.ID)
		} else {
			id = string(edge.ID)
		}
		doc := v.build(node, edge)
		v.replace(id, doc)
		if doc != nil && !local {
			v.order = append(v.order, id)
		}
		return nil
	})
}

func (v *fulltextDocumentView) refresh(ctx context.Context, changes []fulltextDocumentChange) (err error) {
	defer func() {
		if err != nil {
			v.initialized = false
		}
	}()
	if len(changes) > 0 {
		v.orderDirty = true
		v.order = nil
	}
	for _, change := range changes {
		if err := ctx.Err(); err != nil {
			return err
		}
		var doc *FulltextDocument
		if change.node != nil || change.edge != nil {
			doc = v.build(change.node, change.edge)
		}
		v.replace(change.id, doc)
	}
	return v.compact(ctx)
}

// compact releases hash-table capacity left by removed/replaced documents and
// grams. Retired work bounds both document and posting deletions; rebuilding
// after four resident weights keeps capacity proportional to resident data,
// including when a fixed-size corpus changes its vocabulary repeatedly. It
// reuses the sole representation writer and never rebuilds fields or reads
// storage, so the admitted rows, score inputs and order are unchanged.
func (v *fulltextDocumentView) compact(ctx context.Context) error {
	if v.retiredWeight < 4*(int64(len(v.documents))+v.bodyBytes+1) {
		return nil
	}
	documents := v.documents
	v.resetDocuments(len(documents), true)
	for id, document := range documents {
		if err := ctx.Err(); err != nil {
			return err
		}
		v.replace(id, document)
	}
	return nil
}

func (v *fulltextDocumentView) resetDocuments(capacity int, retained bool) {
	v.rows, v.documents, v.postings = nil, nil, nil
	if retained {
		v.documents, v.postings = make(map[string]*FulltextDocument, capacity), make(map[uint32]map[string]struct{})
	}
	v.tokens, v.bodyBytes, v.retiredWeight, v.indexed = 0, 0, 0, 0
}

func (o *fulltextDocumentOwner) pruneChanges(projection *fulltextDocumentProjection) {
	o.changes.Lock()
	defer o.changes.Unlock()
	projection.events, projection.count = projection.view.events, len(projection.view.documents)
	minimum := o.events
	previousLimit := o.dirtyLimit
	o.dirtyLimit = 1
	for _, view := range o.views {
		if view.events < minimum {
			minimum = view.events
		}
	}
	for _, view := range o.views {
		o.dirtyLimit += view.count
	}
	for id, at := range o.nodes {
		if at <= minimum {
			delete(o.nodes, id)
		}
	}
	for id, at := range o.edges {
		if at <= minimum {
			delete(o.edges, id)
		}
	}
	if len(o.nodes) == 0 {
		o.nodes = nil
	} else if previousLimit > 2*o.dirtyLimit {
		compacted := make(map[storage.NodeID]uint64, len(o.nodes))
		maps.Copy(compacted, o.nodes)
		o.nodes = compacted
	}
	if len(o.edges) == 0 {
		o.edges = nil
	} else if previousLimit > 2*o.dirtyLimit {
		compacted := make(map[storage.EdgeID]uint64, len(o.edges))
		maps.Copy(compacted, o.edges)
		o.edges = compacted
	}
}

// A quiet reader must not retain every ID ever deleted. Once the journal is
// larger than the resident corpus, invalidate its baseline and release the
// journal; the next read rebuilds it. This bounds maintenance metadata by
// retained documents without limiting matches or changing a query result.
func (o *fulltextDocumentOwner) boundChanges() {
	if len(o.nodes)+len(o.edges) <= o.dirtyLimit {
		return
	}
	o.reset = o.events
	o.nodes, o.edges = nil, nil
}
