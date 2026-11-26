package search

import (
	"context"
	"encoding/hex"
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Keep only a few complete policy views. Each view is also subject to the
// search service's existing retained-memory admission; the cap prevents a
// large number of principals from retaining one whole corpus apiece.
const fulltextPolicyViewLimit = 4

// readPolicyFulltextDocuments retains a projected corpus only for a stable
// policy and graph revision. A scoped graph is not a SameEventView of the raw
// graph because its rows differ, so the ordinary journal cannot refresh it.
// A changed graph revision instead rebuilds this policy's view from the scoped
// reader. Returned entities are always hydrated through the current request.
func (s *Service) readPolicyFulltextDocuments(ctx context.Context, acquisition *fulltextAcquisition, scope FulltextDocumentScope, build FulltextDocumentBuilder, read func(context.Context, FulltextDocuments) error) (bool, error) {
	request := scope.PolicyRequest
	if request.Fingerprint == "" || request.DatabaseID == nil || !searchVisibilityCacheSafe(scope.PolicySource) {
		return false, nil
	}
	provider, ok := scope.PolicySource.(storage.GraphMutationVersionProvider)
	if !ok {
		return false, nil
	}
	graph, supported := provider.GraphMutationVersion()
	if !supported || !storage.ReadPublicationSettled(scope.PolicySource) {
		return false, nil
	}
	hash, err := request.AccessHash()
	if err != nil {
		return true, err
	}
	key := scope.Name + "\x00fgac:" + request.Fingerprint + ":" + hex.EncodeToString(hash[:])
	o := s.documentOwner()
	if o == nil {
		return true, storage.ErrStorageClosed
	}
	o.changes.Lock()
	projection := o.views[key]
	if projection == nil {
		if len(o.policyOrder) == fulltextPolicyViewLimit {
			oldest := o.policyOrder[0]
			delete(o.views, oldest)
			o.policyOrder = o.policyOrder[1:]
		}
		projection = &fulltextDocumentProjection{name: key, lease: make(chan struct{}, 1)}
		// Policy projections are refreshed by graph revision, not the shared
		// mutation journal. Their candidate predicate must not journal raw rows.
		projection.candidate = func(*storage.Node, *storage.Edge) bool { return false }
		projection.relationships = scope.Relationships
		o.views[key] = projection
	} else {
		for index, old := range o.policyOrder {
			if old == key {
				o.policyOrder = append(o.policyOrder[:index], o.policyOrder[index+1:]...)
				break
			}
		}
	}
	o.policyOrder = append(o.policyOrder, key)
	o.changes.Unlock()
	defer func() {
		if projection.view == nil {
			o.forgetProjection(key, projection)
		}
	}()
	select {
	case projection.lease <- struct{}{}:
		defer func() { <-projection.lease }()
	case <-ctx.Done():
		return true, ctx.Err()
	}
	budget, allowed := o.cache.Admission(0, false)
	if !allowed {
		o.releaseDocuments(budget)
		return false, nil
	}
	stable := func() bool {
		current, ok := provider.GraphMutationVersion()
		return ok && current == graph && storage.ReadPublicationSettled(scope.PolicySource)
	}
	if !stable() {
		return false, nil
	}
	view := projection.view
	rebuild := view == nil || !view.initialized || view.graph != graph ||
		!storage.SameEventView(view.engine, scope.PolicySource) || view.definition != scope.Definition ||
		view.relationships != scope.Relationships || view.queryNodes != scope.QueryNodes || !slices.Equal(view.nodeLabels, scope.NodeLabels)
	if rebuild {
		view = newFulltextDocumentView(scope.Definition, scope.Relationships, build)
		view.engine = scope.PolicySource
		view.queryNodes = scope.QueryNodes
		view.nodeLabels = slices.Clone(scope.NodeLabels)
		projection.view = view
	}
	view.captureImages = scope.EntityRows
	defer func() { view.images = nil }()
	deliver := func() error {
		if !stable() {
			return errFulltextRowUnavailable
		}
		return deliverFulltextDocuments(ctx, read, FulltextDocuments{
			view: view, acquisition: *acquisition, policyGraph: provider, policyVersion: graph,
		})
	}
	if rebuild {
		err = view.bringUp(ctx, acquisition, fulltextBringUp{populate: true}, deliver)
	} else {
		err = deliver()
	}
	if err == errFulltextRowUnavailable || err == nil && !stable() {
		o.forgetProjection(key, projection)
		return false, nil
	}
	if err != nil {
		o.forgetProjection(key, projection)
		return true, err
	}
	view.graph, view.initialized = graph, true
	if budget, allowed := o.cache.Admission(0, false); !allowed {
		o.releaseDocuments(budget)
	}
	return true, nil
}
