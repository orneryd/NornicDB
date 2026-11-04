// Author: Codex implementation lane, 2026-10-26 (NOR-148).
// Author of change relevance: #1683 lane (Claude), 2026-10-30 (NOR-148).
// Author of change relevance: #1683 review-correction lane (DeepSeek), 2026-11-02 (NOR-148).
package search

import (
	"context"
	"errors"
	"iter"
	"maps"
	"slices"
	"strings"
	"sync"
	"weak"

	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// FulltextDocument is an immutable document supplied by a query dialect. The
// service owns its maintenance; the dialect owns its existing matching/scoring.
// Text is the canonical scoring body, and Value holds the dialect's field view.
type FulltextDocument struct {
	Text   string
	Tokens int
	Value  any
	// RetainedBytes forecasts the dialect field representation, including Text.
	// Zero uses the body plus a document header for generic consumers.
	RetainedBytes int64
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
	// EntityRows asks for request-scoped output images, independently of the
	// retained scoring representation. Images never survive the request lease.
	EntityRows bool
	Retain     func(string) bool
	// Candidate reports whether an entity image can be one of the
	// projection's documents; the builder returns nil for every other image.
	// Changes to entities that are neither candidates nor current documents
	// of any projection are not journaled. Nil journals every change.
	Candidate func(*storage.Node, *storage.Edge) bool
}

type fulltextDocumentView struct {
	engine        storage.Engine
	definition    string
	relationships bool
	build         FulltextDocumentBuilder
	rows          []*FulltextDocument
	documents     map[string]*FulltextDocument
	// frequencies holds only the most recent requested term set's body
	// document counts, so its size is one query's unique terms. Any document
	// replacement or reset drops it.
	frequencies   []fulltextFrequency
	tokens        int64
	bodyBytes     int64
	retiredWeight int64
	retainedBytes int64
	graph         uint64
	events        uint64
	initialized   bool
	covered       bool
	order         []string
	orderDirty    bool
	images        map[*FulltextDocument]fulltextDocumentChange
	captureImages bool
}

// FulltextDocuments presents either leased complete statistics or an ordered
// acquisition for this request. The service owns the read context it accepts
// and supplies it to the read callback; callers must not retain this value or
// mutate documents.
type FulltextDocuments struct {
	view        *fulltextDocumentView
	acquisition fulltextAcquisition
	snapshot    storage.EntityReadSnapshot
}

// Count and AverageLength describe the complete scoped corpus, before limits
// and eligibility filtering, as required by the dialect's score calculation.
func (d FulltextDocuments) Count() int { return d.view.documentCount() }
func (d FulltextDocuments) AverageLength() float64 {
	if d.Count() == 0 {
		return 100
	}
	return float64(d.view.tokens) / float64(d.Count())
}

// Each supplies every scoring document to the dialect's existing evaluator on
// the caller's context, and returns its cancellation or the visit's error.
// Body counts are never a substitute for the evaluator's field population.
func (d FulltextDocuments) Each(ctx context.Context, visit func(string, *FulltextDocument) error) error {
	return untilCancelled(ctx, d.view.scoringDocuments(), visit)
}

// scoringDocuments enumerates the view's representation: temporary rows, the
// retained corpus in its original order, or the retained map after an
// incremental change, whose order is no longer known. An ID in the original
// order may have lost its document during population.
func (v *fulltextDocumentView) scoringDocuments() iter.Seq2[string, *FulltextDocument] {
	if !v.temporary() && v.orderDirty {
		return maps.All(v.documents)
	}
	return func(yield func(string, *FulltextDocument) bool) {
		for i, id := range v.order {
			doc := v.documents[id]
			if v.temporary() {
				doc = v.rows[i]
			}
			if doc != nil && !yield(id, doc) {
				return
			}
		}
	}
}

// VisitNodes and VisitEdges carry the caller's context and stop directly into
// the issued native/fallback reader.
func (d FulltextDocuments) VisitNodes(ctx context.Context, visit func(*storage.Node) error) error {
	return d.acquisition.visitNodes(ctx, visit)
}
func (d FulltextDocuments) VisitEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return d.acquisition.visitEdges(ctx, visit)
}

// fulltextFrequency is one requested term's body document frequency.
type fulltextFrequency struct {
	term  string
	count int
}

// BodyDocumentFrequencies counts the scoring documents whose body literally
// contains each requested term, exactly as the release's strings.Contains
// predicate does. Every unique term is counted in one pass over the bodies,
// with one cancellation check per document, so a term shorter than any index
// unit keeps that predicate's meaning, the empty term included. A failure
// returns no counts at all.
//
// The view keeps the most recent term set's counts, bounded by one query's
// unique terms, and reuses them only for exactly that term set. The returned
// map belongs to the caller; the retained copy is private, and changing the
// result never changes it.
func (d FulltextDocuments) BodyDocumentFrequencies(ctx context.Context, terms []string) (map[string]int, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	requested, counts := make([]string, 0, len(terms)), make(map[string]int, len(terms))
	for _, term := range terms {
		if _, duplicate := counts[term]; duplicate {
			continue
		}
		counts[term] = 0
		requested = append(requested, term)
	}
	if len(d.view.frequencies) == len(requested) {
		sameTerms := true
		for _, entry := range d.view.frequencies {
			if _, exists := counts[entry.term]; !exists {
				sameTerms = false
				break
			}
		}
		if sameTerms {
			for _, entry := range d.view.frequencies {
				counts[entry.term] = entry.count
			}
			return counts, nil
		}
	}
	if err := untilCancelled(ctx, d.view.scoringDocuments(), func(_ string, document *FulltextDocument) error {
		for _, term := range requested {
			if strings.Contains(document.Text, term) {
				counts[term]++
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	// Retain the counts only with a view this service keeps, and clone the terms
	// so that set never keeps a larger query buffer alive.
	if !d.view.temporary() {
		frequencies := make([]fulltextFrequency, 0, len(requested))
		for _, term := range requested {
			frequencies = append(frequencies, fulltextFrequency{term: strings.Clone(term), count: counts[term]})
		}
		d.view.frequencies = frequencies
	}
	return counts, nil
}

// fulltextFrequencyBytes estimates a retained frequency set: one entry per term
// (a string header and a count) plus the cloned term text.
func fulltextFrequencyBytes(frequencies []fulltextFrequency) int64 {
	var bytes int64
	for _, entry := range frequencies {
		bytes += int64(len(entry.term)) + 24
	}
	return bytes
}

// temporary reports a request-scoped view: rows in order, with no retained
// documents.
func (v *fulltextDocumentView) temporary() bool { return v.documents == nil }

// documentCount is the number of documents of either representation.
func (v *fulltextDocumentView) documentCount() int {
	if v.temporary() {
		return len(v.rows)
	}
	return len(v.documents)
}

func (v *fulltextDocumentView) replace(ctx context.Context, id string, doc *FulltextDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Every replacement changes the corpus, so any retained term set described
	// the previous corpus and is not these documents' counts.
	v.frequencies = nil
	if previous := v.documents[id]; previous != nil {
		v.retainedBytes -= fulltextDocumentBytes(previous)
		v.tokens -= int64(previous.Tokens)
		v.bodyBytes -= int64(len(previous.Text))
		v.retiredWeight += int64(len(previous.Text)) + 1
		delete(v.documents, id)
	}
	if doc == nil {
		return nil
	}
	if v.temporary() {
		v.rows = append(v.rows, doc)
	} else {
		v.documents[id] = doc
	}
	v.tokens += int64(doc.Tokens)
	v.retainedBytes += fulltextDocumentBytes(doc)
	v.bodyBytes += int64(len(doc.Text))
	return nil
}

type fulltextDocumentProjection struct {
	// A projection lease is cancellable and never serializes another index's
	// population. Only its holder accesses view; changes protects count/events.
	lease  chan struct{}
	view   *fulltextDocumentView
	name   string
	count  int
	events uint64
	// candidate and relationships come from the scope; resident holds the IDs
	// of view's documents as of its holder's last publication (pruneChanges).
	candidate     func(*storage.Node, *storage.Edge) bool
	relationships bool
	resident      map[string]struct{}
	// reading is set while the lease holder's read is in flight: its view may
	// make an ID resident that resident does not hold yet. A change that is
	// neither a candidate nor resident waits in deferred until the read
	// publishes its residency (pruneChanges), then is journaled only if it is
	// resident there. A read that publishes nothing drops them: the residency
	// they were checked against still stands. deferred holds at most
	// fulltextDeferredLimit changes; past it the read releases them and sets
	// deferredOverflow, and its published view is not refreshed by ID.
	reading          bool
	deferred         []fulltextDeferredChange
	deferredOverflow bool
}

// fulltextDeferredFloor is the least number of changes one in-flight read may
// defer (40 bytes each, so 160 KB), so a first population, whose published
// corpus is still empty, keeps ordinary churn from forcing a rebuild.
const fulltextDeferredFloor = 4096

// fulltextDeferredChange is a change waiting for an in-flight read's residency.
type fulltextDeferredChange struct {
	id     string
	at     uint64
	isEdge bool
}

type fulltextDocumentOwner struct {
	// Retention needs shutdown observation and reporting, not access to the
	// service's independently owned mutable vector-serving state.
	closed           <-chan struct{}
	reportTransition func(FulltextRetentionStatus, bool)
	transitions      []fulltextRetentionTransition
	reporting        bool
	cache            *memory.RetainedCache
	status           map[string]FulltextRetentionStatus
	// changes protects the journal and projection registry/retention metadata.
	// No storage scan or query evaluation runs under this mutex. The journal
	// (nodes, edges) holds only changes that can alter a projection, so its
	// bound is sized by index-relevant changes, in flight or not. A read in
	// flight also holds at most deferredLimit unresolved changes until it
	// returns; a read that would exceed it costs its view one repopulation.
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
	return &fulltextDocumentOwner{views: make(map[string]*fulltextDocumentProjection), status: make(map[string]FulltextRetentionStatus)}
}

func (o *fulltextDocumentOwner) subscribe(engine storage.Engine) {
	o.subscriptionOnce.Do(func() {
		// Storage subscriptions outlive a retired service. They must not retain its
		// documents or its storage view after the database drops that service.
		ref := weak.Make(o)
		// A retired owner (nil) ignores every event.
		live := func(apply func(*fulltextDocumentOwner)) {
			if owner := ref.Value(); owner != nil {
				apply(owner)
			}
		}
		node := func(id storage.NodeID, image *storage.Node) {
			live(func(owner *fulltextDocumentOwner) { owner.observe(string(id), image, nil, false) })
		}
		edge := func(id storage.EdgeID, image *storage.Edge) {
			live(func(owner *fulltextDocumentOwner) { owner.observe(string(id), nil, image, true) })
		}
		o.subscribed = storage.SubscribeEvents(engine, storage.EventHandlers{
			NodeCreated: func(n *storage.Node) { node(n.ID, n) }, NodeUpdated: func(n *storage.Node) { node(n.ID, n) }, NodeDeleted: func(id storage.NodeID) { node(id, nil) },
			EdgeCreated: func(e *storage.Edge) { edge(e.ID, e) }, EdgeUpdated: func(e *storage.Edge) { edge(e.ID, e) }, EdgeDeleted: func(id storage.EdgeID) { edge(id, nil) },
			NamespaceReset: func(string) { live((*fulltextDocumentOwner).observeReset) },
		}) == nil
	})
}

// observe is the one transition for a storage change of either kind. Every
// change counts as an event, so event coverage stays exact; only changes that
// can affect a projection are journaled by ID.
func (o *fulltextDocumentOwner) observe(id string, node *storage.Node, edge *storage.Edge, isEdge bool) {
	at := o.beginEvent()
	if o.affects(id, node, edge, isEdge, at) {
		o.journal(id, isEdge, at)
		o.boundChanges()
	}
	o.changes.Unlock()
}

// invalidateBefore makes every view that has not seen event at repopulate.
func (o *fulltextDocumentOwner) invalidateBefore(at uint64) { o.reset = at }

// beginEvent is the one count of a delivered storage event, the epoch event
// coverage is certified by. It returns with changes held for the event's effect.
func (o *fulltextDocumentOwner) beginEvent() uint64 {
	o.changes.Lock()
	o.events++
	return o.events
}

// observeReset counts a namespace reset as an event that invalidates every
// view.
func (o *fulltextDocumentOwner) observeReset() {
	o.invalidateBefore(o.beginEvent())
	o.changes.Unlock()
}

// ReadFulltextDocuments serves a named fulltext projection from this search
// service, maintaining it from storage changes independently of query text.
// Definition is the schema's identity, including ordered fields and scope;
// changing or recreating a named index replaces its old projection.
//
// Only a committed view of the service's own database (storage.SameEventView:
// the same publisher, namespace and rows, reached only through forwarders) has a
// retained corpus, whichever view built the service. Every other view, and a
// scope with dynamic visibility, is request-scoped.
//
// Documents come from the view's own readers, so a read serves what the release
// reader serves. A commit moves the graph revision only after its data is
// visible, so rows read after sampling a revision include every write that
// revision names, and an equal revision later reuses them. A write during the
// population moves the revision afterwards. A baseline sampled with the event
// queue settled, and a revision and event count that moved together since, refresh
// by ID from the journal; anything else populates again.
//
// read receives the read context this service accepted for it, so cancellation
// and shutdown stay owned here and reach every follow-up read the consumer
// issues on these documents.
func (s *Service) ReadFulltextDocuments(ctx context.Context, engine storage.Engine, scope FulltextDocumentScope, build FulltextDocumentBuilder, read func(context.Context, FulltextDocuments) error) error {
	ctx, release, err := s.beginRead(ctx)
	if err != nil {
		return err
	}
	defer release()
	// Ordered consumers own a traversal, not complete corpus statistics. Their
	// stop must reach acquisition on every call, including fresh/buffered views.
	if scope.Ordered {
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{acquisition: fulltextAcquisition{engine: engine, relationships: scope.Relationships}})
	}
	acquisition := fulltextAcquisition{engine: engine, relationships: scope.Relationships}
	provider, versioned := engine.(graphMutationVersionProvider)
	if scope.Fresh || !versioned || !storage.SameEventView(engine, s.engine) {
		return readFulltextOriginal(ctx, &acquisition, scope, build, read)
	}
	o := s.documentOwner()
	if o == nil {
		return storage.ErrStorageClosed
	}
	o.changes.Lock()
	if scope.Retain != nil {
		for name := range o.views {
			if !scope.Retain(name) {
				delete(o.views, name)
			}
		}
		for name := range o.status {
			if !scope.Retain(name) {
				delete(o.status, name)
			}
		}
	}
	projection := o.views[scope.Name]
	if projection == nil {
		projection = &fulltextDocumentProjection{name: scope.Name, lease: make(chan struct{}, 1)}
		o.views[scope.Name] = projection
	}
	projection.candidate, projection.relationships = scope.Candidate, scope.Relationships
	o.changes.Unlock()
	defer func() {
		o.changes.Lock()
		// Cancellation may win before the lease, or immediately after it.
		// Do not pin journal history for a projection that published nothing.
		// A sibling's active read owns its view; reading is checked first while
		// holding changes, before inspecting that lease-protected field.
		if o.views[scope.Name] == projection && !projection.reading && projection.view == nil {
			delete(o.views, scope.Name)
		}
		o.changes.Unlock()
	}()
	select {
	case projection.lease <- struct{}{}:
		defer func() { <-projection.lease }()
	case <-ctx.Done():
		return ctx.Err()
	}
	o.changes.Lock()
	projection.reading = true
	o.changes.Unlock()
	defer func() {
		o.changes.Lock()
		projection.endRead()
		o.changes.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return err
	}
	// The journal holds the events of the service's own view. It covers a query
	// view only when storage proves both expose the same events, IDs and rows.
	// Only the service's own engine is ever subscribed, so a transient wrapper
	// never leaves a permanent weak callback record in storage.
	retained := !scope.Fresh && versioned && storage.SameEventView(engine, s.engine)
	budget, permitsRetention := o.cache.Admission(0, o.released(scope.Name))
	if !permitsRetention {
		o.releaseDocuments(budget)
	}
	o.changes.Lock()
	registered := o.views[scope.Name] == projection
	o.changes.Unlock()
	retained = retained && permitsRetention && registered
	if retained {
		o.subscribe(s.engine)
	}
	if !retained || !o.subscribed {
		o.changes.Lock()
		if projection.view == nil && o.views[scope.Name] == projection {
			delete(o.views, scope.Name)
		}
		o.changes.Unlock()
		return readFulltextOriginal(ctx, &acquisition, scope, build, read)
	}
	view := projection.view
	if view == nil || !storage.SameEventView(view.engine, engine) || view.definition != scope.Definition || view.relationships != scope.Relationships {
		view = newFulltextDocumentView(scope.Definition, scope.Relationships, build)
		view.engine = engine
		projection.view = view
	}
	settled := storage.EventNotificationsSettled(engine)
	graph, graphOK := provider.GraphMutationVersion()

	view.captureImages = scope.EntityRows
	defer func() { view.images = nil }()
	o.changes.Lock()
	events, reset := o.events, o.reset
	nodes, edges := changedSince(o.nodes, view.events), changedSince(o.edges, view.events)
	o.changes.Unlock()
	atGraph := func() bool {
		end, ok := provider.GraphMutationVersion()
		return ok && end == graph
	}
	stable := graphOK && atGraph()
	// Event coverage is certified only across this short sampling boundary, never
	// by waiting for a scan-length quiet interval.
	certified := settled && stable
	unchanged := stable && storage.ReadPublicationSettled(engine) && view.initialized && reset <= view.events && graph == view.graph
	// Every event since the baseline is journaled and every graph mutation moves
	// the revision, so equal movement says the journal names all that changed.
	incremental := certified && view.initialized && view.covered && reset <= view.events && events > view.events && graph > view.graph && graph-view.graph == events-view.events
	// The canonical capture is request-local. A short revision certificate
	// admits retained scoring inputs for it; no timestamp or entity survives
	// this read. Full populations still use the original native/fallback reader.
	var snapshot storage.EntityReadSnapshot
	if scope.EntityRows && (unchanged || incremental) {
		var err error
		snapshot, err = storage.OpenEntityReadSnapshot(ctx, engine, scope.Relationships)
		if err != nil {
			o.forgetProjection(scope.Name, projection)
			if errors.Is(err, storage.ErrReadSnapshotUnsupported) {
				return readFulltextOriginal(ctx, &acquisition, scope, build, read)
			}
			return err
		}
		defer snapshot.Close()
	}
	// A commit during capture selects one original population immediately;
	// it never turns population into a wait for a corpus-length quiet interval.
	if snapshot != nil && (!storage.ReadPublicationSettled(engine) || !atGraph()) {
		unchanged, incremental = false, false
	}

	refreshed := false
	var changes []fulltextDocumentChange
	// resident is how this read changes the published document IDs: every
	// ID after a full population, the refreshed IDs after a refresh.
	var resident fulltextResidency
	if !unchanged && incremental {
		var err error
		changes, err = view.acquireChanges(ctx, engine, nodes, edges)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A read failure is the outcome, as it is for a full scan. Only a
		// revision that moved while the changes were read falls back to the
		// full population, because the by-ID reads may then mix revisions.
		if err != nil {
			return err
		}
		if atGraph() && storage.EventNotificationsSettled(engine) {
			refreshed = true
			resident.changed = changes
		}
	}
	resident.full = !unchanged && !refreshed
	err = view.bringUp(ctx, &acquisition, fulltextBringUp{populate: resident.full, changes: resident.changed}, func() error {
		view.graph, view.events, view.initialized = graph, events, stable
		if !unchanged {
			view.covered = certified
		}
		o.pruneChanges(projection, resident)
		if err := ctx.Err(); err != nil {
			return err
		}
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{view: view, snapshot: snapshot})
	})
	if err == errFulltextRowUnavailable {
		// A selected body that the captured reader cannot supply invalidates
		// reuse. One original population produces the outcome, with no retry
		// loop or new point-reader error policy in the public procedure.
		view.initialized = false
		o.forgetProjection(scope.Name, projection)
		return readFulltextOriginal(ctx, &acquisition, scope, build, read)
	}
	if err == nil {
		o.admitDocuments(scope.Name, projection, view)
	} else {
		o.forgetProjection(scope.Name, projection)
	}
	return err
}

func readFulltextOriginal(ctx context.Context, acquisition *fulltextAcquisition, scope FulltextDocumentScope, build FulltextDocumentBuilder, read func(context.Context, FulltextDocuments) error) error {
	view := newFulltextDocumentView(scope.Definition, scope.Relationships, build)
	view.captureImages = scope.EntityRows
	return view.bringUp(ctx, acquisition, fulltextBringUp{populate: true, local: true}, func() error {
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{view: view})
	})
}

// deliverFulltextDocuments hands docs and the accepted read context to read. A
// read error is returned as is; a read that returns normally still reports the
// query's cancellation.
func deliverFulltextDocuments(ctx context.Context, read func(context.Context, FulltextDocuments) error, docs FulltextDocuments) error {
	if err := read(ctx, docs); err != nil {
		return err
	}
	return ctx.Err()
}

// fulltextBringUp says what a read does to its view: populate it from the
// acquisition (local: as temporary rows), or else apply changes, which are none
// when the view is already at the read's revision.
type fulltextBringUp struct {
	populate, local bool
	changes         []fulltextDocumentChange
}

// bringUp is the one order of a read: populate or refresh, then deliver.
func (v *fulltextDocumentView) bringUp(ctx context.Context, acquisition *fulltextAcquisition, plan fulltextBringUp, deliver func() error) error {
	var err error
	if plan.populate {
		err = v.populate(ctx, acquisition, plan.local)
	} else {
		err = v.refresh(ctx, plan.changes)
	}
	if err != nil {
		return err
	}
	return deliver()
}

func newFulltextDocumentView(definition string, relationships bool, build FulltextDocumentBuilder) *fulltextDocumentView {
	return &fulltextDocumentView{definition: definition, relationships: relationships, build: build}
}

func (v *fulltextDocumentView) populate(ctx context.Context, acquisition *fulltextAcquisition, local bool) error {
	v.initialized = false
	v.images = nil
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
		if err := v.replace(ctx, id, doc); err != nil {
			return err
		}
		if doc != nil {
			v.order = append(v.order, id)
			v.captureImage(doc, id, node, edge)
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
	err = untilCancelled(ctx, slices.All(changes), func(_ int, change fulltextDocumentChange) error {
		var doc *FulltextDocument
		if fulltextImageExists(change.node, change.edge) {
			doc = v.build(change.node, change.edge)
		}
		if err := v.replace(ctx, change.id, doc); err != nil {
			return err
		}
		if doc != nil {
			v.captureImage(doc, change.id, change.node, change.edge)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return v.compact(ctx)
}

// compact releases hash-table capacity left by removed or replaced documents.
// Retired work bounds the deletions; rebuilding after four resident weights
// keeps capacity proportional to resident data, including when a fixed-size
// corpus changes its vocabulary repeatedly. It reuses the sole representation
// writer and never rebuilds fields or reads storage, so the admitted rows,
// score inputs and order are unchanged. A retained frequency set describes the
// documents it counted, so this rebuild drops it and the next counting read
// fills it again.
func (v *fulltextDocumentView) compact(ctx context.Context) error {
	if v.retiredWeight < 4*(int64(len(v.documents))+v.bodyBytes+1) {
		return nil
	}
	documents := v.documents
	v.resetDocuments(len(documents), true)
	return untilCancelled(ctx, maps.All(documents), func(id string, document *FulltextDocument) error {
		return v.replace(ctx, id, document)
	})
}

func (v *fulltextDocumentView) resetDocuments(capacity int, retained bool) {
	v.rows, v.documents = nil, nil
	if retained {
		v.documents = make(map[string]*FulltextDocument, capacity)
	}
	v.frequencies = nil
	v.tokens, v.bodyBytes, v.retiredWeight = 0, 0, 0
	v.retainedBytes = 0
}

// fulltextResidency is the change a read made to its view's document IDs.
type fulltextResidency struct {
	full    bool
	changed []fulltextDocumentChange
}

func (o *fulltextDocumentOwner) pruneChanges(projection *fulltextDocumentProjection, resident fulltextResidency) {
	o.changes.Lock()
	defer o.changes.Unlock()
	if o.views[projection.name] != projection {
		projection.endRead()
		return
	}
	projection.events, projection.count = projection.view.events, len(projection.view.documents)
	if resident.full || projection.resident == nil {
		projection.resident = make(map[string]struct{}, len(projection.view.documents))
		for id := range projection.view.documents {
			projection.resident[id] = struct{}{}
		}
	} else {
		for _, change := range resident.changed {
			if projection.view.documents[change.id] != nil {
				projection.resident[change.id] = struct{}{}
			} else {
				delete(projection.resident, change.id)
			}
		}
	}
	// The read's residency is published: changes it deferred count now only if
	// they touch one of its documents. A read that released them cannot tell,
	// so its view is not refreshed by ID; the next read populates it again.
	if projection.deferredOverflow {
		projection.view.covered = false
	}
	for _, change := range projection.deferred {
		if projection.holds(change.id) {
			o.journal(change.id, change.isEdge, change.at)
		}
	}
	projection.endRead()
	minimum := o.events
	previousLimit := o.dirtyLimit
	o.dirtyLimit = 1
	for _, view := range o.views {
		if view.events < minimum {
			minimum = view.events
		}
		o.dirtyLimit += view.count
	}
	compact := previousLimit > 2*o.dirtyLimit
	o.nodes, o.edges = pruneJournal(o.nodes, minimum, compact), pruneJournal(o.edges, minimum, compact)
}

// The journal keeps one map per entity kind (nodes, edges); these three
// functions are its only operations on either map.

// recordChange records change at for id; a later change to an ID keeps the
// later event.
func recordChange[K comparable](journal map[K]uint64, id K, at uint64) map[K]uint64 {
	if journal == nil {
		journal = make(map[K]uint64)
	}
	if at > journal[id] {
		journal[id] = at
	}
	return journal
}

// changedSince lists the IDs changed after event events.
func changedSince[K comparable](journal map[K]uint64, events uint64) []K {
	ids := make([]K, 0)
	for id, at := range journal {
		if at > events {
			ids = append(ids, id)
		}
	}
	return ids
}

// pruneJournal drops changes every view has seen (at <= minimum), releases an
// empty map, and when compact copies the rest into a right-sized map.
func pruneJournal[K comparable](journal map[K]uint64, minimum uint64, compact bool) map[K]uint64 {
	for id, at := range journal {
		if at <= minimum {
			delete(journal, id)
		}
	}
	if len(journal) == 0 {
		return nil
	}
	if compact {
		compacted := make(map[K]uint64, len(journal))
		maps.Copy(compacted, journal)
		return compacted
	}
	return journal
}

// affects reports whether change at can alter a projection: its new image can
// be one of the projection's documents, or the projection holds a document for
// it. A projection whose read is in flight may be about to hold a document for
// it, so that read decides when it publishes (fulltextDocumentProjection.reading).
func (o *fulltextDocumentOwner) affects(id string, node *storage.Node, edge *storage.Edge, isEdge bool, at uint64) bool {
	for _, projection := range o.views {
		if projection.relationships != isEdge {
			continue
		}
		if projection.candidate == nil {
			return true
		}
		if fulltextImageExists(node, edge) && projection.candidate(node, edge) {
			return true
		}
		if projection.holds(id) {
			return true
		}
	}
	for _, projection := range o.views {
		if !projection.reading || projection.relationships != isEdge || projection.deferredOverflow {
			continue
		}
		if len(projection.deferred) >= o.deferredLimit() {
			projection.deferred, projection.deferredOverflow = nil, true
			continue
		}
		projection.deferred = append(projection.deferred, fulltextDeferredChange{id: id, at: at, isEdge: isEdge})
	}
	return false
}

// fulltextImageExists reports whether a change carries an entity image; a
// change without one is a deletion.
func fulltextImageExists(node *storage.Node, edge *storage.Edge) bool {
	return node != nil || edge != nil
}

// holds reports that id is resident as of the holder's last publication.
func (p *fulltextDocumentProjection) holds(id string) bool {
	_, ok := p.resident[id]
	return ok
}

// endRead ends the in-flight state of the lease holder's read.
func (p *fulltextDocumentProjection) endRead() {
	p.reading, p.deferred, p.deferredOverflow = false, nil, false
}

// deferredLimit bounds one in-flight read's deferred changes by the same
// resident-derived size as the journal (dirtyLimit), and at least
// fulltextDeferredFloor.
func (o *fulltextDocumentOwner) deferredLimit() int {
	return max(o.dirtyLimit, fulltextDeferredFloor)
}

// journal records change at for id in its kind's map (recordChange).
func (o *fulltextDocumentOwner) journal(id string, isEdge bool, at uint64) {
	if isEdge {
		o.edges = recordChange(o.edges, storage.EdgeID(id), at)
		return
	}
	o.nodes = recordChange(o.nodes, storage.NodeID(id), at)
}

// A quiet reader must not retain every ID ever deleted. Once the journal is
// larger than the resident corpus, invalidate its baseline and release the
// journal; the next read rebuilds it. This bounds maintenance metadata by
// retained documents without limiting matches or changing a query result.
func (o *fulltextDocumentOwner) boundChanges() {
	if len(o.nodes)+len(o.edges) <= o.dirtyLimit {
		return
	}
	o.invalidateBefore(o.events)
	o.nodes, o.edges = nil, nil
}
