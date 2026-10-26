// Author: Codex implementation lane, 2026-10-26 (NOR-148).
// Author of change relevance: #1683 lane (Claude), 2026-10-30 (NOR-148).
package search

import (
	"context"
	"iter"
	"maps"
	"slices"
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
	postings      map[uint32]map[string]struct{}
	tokens        int64
	bodyBytes     int64
	retiredWeight int64
	revision      storage.SnapshotReadRevision
	reader        storage.SnapshotReader
	events        uint64
	initialized   bool
	covered       bool
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
func (d FulltextDocuments) Count() int { return d.view.documentCount() }
func (d FulltextDocuments) AverageLength() float64 {
	if d.Count() == 0 {
		return 100
	}
	return float64(d.view.tokens) / float64(d.Count())
}

// Each supplies every scoring document to the dialect's existing evaluator.
// Body postings are never a substitute for the evaluator's field population.
func (d FulltextDocuments) Each(visit func(string, *FulltextDocument)) {
	_ = untilCancelled(d.ctx, d.view.scoringDocuments(), func(id string, doc *FulltextDocument) error {
		visit(id, doc)
		return nil
	})
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
	if !d.view.temporary() {
		for i := range fulltextGramCount(term) {
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
	count := func(doc *FulltextDocument) {
		if strings.Contains(doc.Text, term) {
			result++
		}
	}
	if smallest == nil {
		d.Each(func(_ string, doc *FulltextDocument) { count(doc) })
		return result
	}
	_ = untilCancelled(d.ctx, maps.All(smallest), func(id string, _ struct{}) error {
		count(d.view.documents[id])
		return nil
	})
	return result
}

// fulltextGramCount is the number of trigrams of text; none when it is shorter.
func fulltextGramCount(text string) int { return len(text) - 2 }

func fulltextGram(text string, i int) uint32 {
	return uint32(text[i])<<16 | uint32(text[i+1])<<8 | uint32(text[i+2])
}

// temporary reports a request-scoped view: rows in order, with no retained
// documents or postings.
func (v *fulltextDocumentView) temporary() bool { return v.documents == nil }

// documentCount is the number of documents of either representation.
func (v *fulltextDocumentView) documentCount() int {
	if v.temporary() {
		return len(v.rows)
	}
	return len(v.documents)
}

func (v *fulltextDocumentView) replace(id string, doc *FulltextDocument) {
	if previous := v.documents[id]; previous != nil {
		v.tokens -= int64(previous.Tokens)
		v.bodyBytes -= int64(len(previous.Text))
		v.retiredWeight += int64(len(previous.Text)) + 1
		for i := range fulltextGramCount(previous.Text) {
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
	if v.temporary() {
		v.rows = append(v.rows, doc)
	} else {
		v.documents[id] = doc
	}
	v.tokens += int64(doc.Tokens)
	v.bodyBytes += int64(len(doc.Text))
	if !v.temporary() {
		for i := range fulltextGramCount(doc.Text) {
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
	return &fulltextDocumentOwner{views: make(map[string]*fulltextDocumentProjection)}
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
// A transactional view without read snapshots is request-scoped. A committed
// view of the service's own database (storage.SameEventView: the same
// publisher, namespace and rows, reached only through forwarders) shares the
// service's journal and retained corpus, whichever view built the service.
// Any other committed view keeps a corpus of its own without the journal, and
// a projection never serves one view's rows through an unproven engine.
// A revision admits reuse; an admitted event epoch also permits incremental
// maintenance. Otherwise population uses one snapshot without retrying it.
func (s *Service) ReadFulltextDocuments(ctx context.Context, engine storage.Engine, scope FulltextDocumentScope, build FulltextDocumentBuilder, read func(FulltextDocuments) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	// Ordered consumers own a traversal, not complete corpus statistics. Their
	// stop must reach acquisition on every call, including fresh/buffered views.
	if scope.Ordered {
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{ctx: ctx, acquisition: orderedFulltextAcquisition(engine, scope)})
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
	projection.candidate, projection.relationships = scope.Candidate, scope.Relationships
	o.changes.Unlock()
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
		return local.bringUp(ctx, &acquisition, fulltextBringUp{populate: true, local: true}, prepare, func() error {
			return deliverFulltextDocuments(ctx, read, FulltextDocuments{view: local, ctx: ctx})
		})
	}
	// The journal holds the events of the service's own view. It covers a query
	// view only when storage proves both expose the same events, IDs and rows.
	// Only the service's own engine is ever subscribed, so a transient wrapper
	// never leaves a permanent weak callback record in storage.
	journal := storage.SameEventView(engine, s.engine)
	if journal {
		o.subscribe(s.engine)
	}
	view := projection.view
	if view == nil || !storage.SameEventView(view.engine, engine) || view.definition != scope.Definition || view.relationships != scope.Relationships || view.reader != reader {
		view = newFulltextDocumentView(scope.Definition, scope.Relationships, build)
		view.engine = engine
		view.reader = reader
		projection.view = view
	}
	captured, capturedOK := querySnapshot.CapturedRevision()
	settled := journal && o.subscribed && storage.EventNotificationsSettled(engine)
	o.changes.Lock()
	events, reset := o.events, o.reset
	nodes, edges := changedSince(o.nodes, view.events), changedSince(o.edges, view.events)
	o.changes.Unlock()
	// An epoch is certified only across this short capture boundary, never by
	// waiting for a scan-length quiet interval. An unproved capture still serves
	// its coherent snapshot; it simply cannot be reused as a revision baseline.
	atCaptured := func() bool {
		end, ok := querySnapshot.CurrentRevision()
		return ok && end == captured
	}
	stable := capturedOK && atCaptured()
	covered := settled && stable
	// Revisions also account for visible writes whose notifications are
	// still queued. Only an exactly covered interval may refresh by ID;
	// otherwise read the authoritative store, never a stale projection.
	unchanged := stable && view.initialized && captured.SameReaderView(view.revision)
	graph, previousGraph := captured.GraphMutationVersion(), view.revision.GraphMutationVersion()
	incremental := stable && view.initialized && view.covered && settled && reset <= view.events && events > view.events && graph > previousGraph && graph-previousGraph == events-view.events && captured.SingleReaderCommitAfter(view.revision)
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
		if atCaptured() {
			refreshed = true
			resident.changed = changes
		}
	}
	resident.full = !unchanged && !refreshed
	return view.bringUp(ctx, &acquisition, fulltextBringUp{populate: resident.full, changes: resident.changed}, prepare, func() error {
		view.revision, view.events, view.initialized, view.covered = captured, events, stable, covered || (unchanged && view.covered) || refreshed
		o.pruneChanges(projection, resident)
		if err := ctx.Err(); err != nil {
			return err
		}
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{view: view, ctx: ctx})
	})
}

// deliverFulltextDocuments hands docs to read. A read error is returned as is;
// a read that returns normally still reports the query's cancellation.
func deliverFulltextDocuments(ctx context.Context, read func(FulltextDocuments) error, docs FulltextDocuments) error {
	if err := read(docs); err != nil {
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

// bringUp is the one order of a read: load a population's entities, run the
// scope's before-build check, construct, then run deliver. A failed load is
// returned before the check runs, and a declined build constructs and delivers
// nothing.
func (v *fulltextDocumentView) bringUp(ctx context.Context, acquisition *fulltextAcquisition, plan fulltextBringUp, prepare func() (bool, error), deliver func() error) error {
	if plan.populate {
		if err := acquisition.materialize(ctx); err != nil {
			return err
		}
	}
	if ready, err := prepare(); err != nil || !ready {
		return err
	}
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
		if doc != nil {
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
	err = untilCancelled(ctx, slices.All(changes), func(_ int, change fulltextDocumentChange) error {
		var doc *FulltextDocument
		if fulltextImageExists(change.node, change.edge) {
			doc = v.build(change.node, change.edge)
		}
		v.replace(change.id, doc)
		return nil
	})
	if err != nil {
		return err
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
	return untilCancelled(ctx, maps.All(documents), func(id string, document *FulltextDocument) error {
		v.replace(id, document)
		return nil
	})
}

func (v *fulltextDocumentView) resetDocuments(capacity int, retained bool) {
	v.rows, v.documents, v.postings = nil, nil, nil
	if retained {
		v.documents, v.postings = make(map[string]*FulltextDocument, capacity), make(map[uint32]map[string]struct{})
	}
	v.tokens, v.bodyBytes, v.retiredWeight = 0, 0, 0
}

// fulltextResidency is the change a read made to its view's document IDs.
type fulltextResidency struct {
	full    bool
	changed []fulltextDocumentChange
}

func (o *fulltextDocumentOwner) pruneChanges(projection *fulltextDocumentProjection, resident fulltextResidency) {
	o.changes.Lock()
	defer o.changes.Unlock()
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
