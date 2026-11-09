// Author: #1683 implementation lane (Codex), 2026-11-01.
package search

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/memory"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// FulltextRetentionStatus describes an optional procedure corpus. Released
// indexes remain searchable: the same acquisition reads their data from storage.
type FulltextRetentionStatus struct {
	Index         string `json:"index"`
	Relationships bool   `json:"relationships"`
	Retained      bool   `json:"retained"`
	StorageReads  bool   `json:"storage_reads"`
	Documents     int    `json:"documents"`
	Bytes         int64  `json:"estimated_bytes"`
	MemoryUsed    int64  `json:"memory_used_bytes"`
	MemoryLimit   int64  `json:"memory_limit_bytes"`
	MemorySource  string `json:"memory_source"`
}

func (s *Service) documentOwner() *fulltextDocumentOwner {
	s.fulltextDocumentsMu.Lock()
	defer s.fulltextDocumentsMu.Unlock()
	if s.Closed() {
		return nil
	}
	if s.fulltextDocuments == nil {
		o := newFulltextDocumentOwner()
		o.closed = s.lifecycleCtx.Done()
		o.reportTransition = s.reportFulltextTransition
		o.cache = memory.RegisterRetainedCache(s.memoryPolicy, o.releaseDocuments)
		s.fulltextDocuments = o
	}
	return s.fulltextDocuments
}

// FulltextRetention returns the current optional-copy state for the existing
// database search status owner. Reading status never initializes an index.
func (s *Service) FulltextRetention() []FulltextRetentionStatus {
	if s == nil {
		return nil
	}
	s.fulltextDocumentsMu.Lock()
	o := s.fulltextDocuments
	s.fulltextDocumentsMu.Unlock()
	if o == nil {
		return nil
	}
	o.changes.Lock()
	result := make([]FulltextRetentionStatus, 0, len(o.status))
	for _, status := range o.status {
		result = append(result, status)
	}
	o.changes.Unlock()
	sort.Slice(result, func(i, j int) bool {
		if result[i].Index == result[j].Index {
			return !result[i].Relationships && result[j].Relationships
		}
		return result[i].Index < result[j].Index
	})
	return result
}

func (o *fulltextDocumentOwner) released(name string) bool {
	o.changes.Lock()
	defer o.changes.Unlock()
	return o.status[name].StorageReads
}

// releaseDocuments detaches whole generations under the metadata mutex, without
// waiting for a reader or mutating its view. That reader now owns request data
// only: registry identity prevents it from reinstalling the detached generation.
func (o *fulltextDocumentOwner) releaseDocuments(budget memory.Budget) {
	o.changes.Lock()
	if len(o.views) == 0 {
		o.changes.Unlock()
		return
	}
	report := false
	for name := range o.views {
		status := o.status[name]
		if !status.StorageReads {
			index, relationships := fulltextRetentionIdentity(name)
			status = FulltextRetentionStatus{Index: index, Relationships: relationships, StorageReads: true,
				MemoryUsed: budget.Used, MemoryLimit: budget.Limit, MemorySource: budget.Source}
			o.status[name] = status
			report = o.queueTransition(status, false) || report
		}
		// Do not touch projection.view: an active lease may still be evaluating
		// it. Removing registry ownership is the cache's complete retirement.
	}
	o.retireDocumentsLocked()
	o.changes.Unlock()
	if report {
		o.reportTransitions()
	}
}

func (o *fulltextDocumentOwner) forgetProjection(name string, projection *fulltextDocumentProjection) {
	o.changes.Lock()
	if o.views[name] == projection {
		delete(o.views, name)
		delete(o.status, name)
	}
	o.changes.Unlock()
}

func (o *fulltextDocumentOwner) retireDocuments() {
	o.changes.Lock()
	o.retireDocumentsLocked()
	o.status = nil
	o.changes.Unlock()
}

func (o *fulltextDocumentOwner) retireDocumentsLocked() {
	o.views = make(map[string]*fulltextDocumentProjection)
	o.nodes, o.edges = nil, nil
	o.dirtyLimit = 1
	o.invalidateBefore(o.events)
}

func (o *fulltextDocumentOwner) admitDocuments(name string, projection *fulltextDocumentProjection, view *fulltextDocumentView) {
	// The estimate is the resident documents plus their hash table and the one
	// retained frequency set this view may hold.
	bytes := view.retainedBytes + int64(len(view.documents))*192 + fulltextFrequencyBytes(view.frequencies)
	// The read reaches here after it materialized this generation, so the
	// physical usage the budget observes already contains it. Admitting the
	// corpus as forecast growth would charge it twice and demand room for a
	// second corpus: a released corpus near the limit could then never refill,
	// and every read would repopulate the whole corpus, be denied, and release
	// it again. Admit the resident copy against current usage and keep bytes as
	// the reported estimate. Refill keeps its own hysteresis: released(name)
	// still selects the stricter recovered-capacity threshold.
	budget, allowed := o.cache.Admission(0, o.released(name))
	if !allowed {
		o.releaseDocuments(budget)
		return
	}
	o.changes.Lock()
	if o.views[name] != projection {
		o.changes.Unlock()
		return
	}
	select {
	case <-o.closed:
		o.changes.Unlock()
		return
	default:
	}
	previous := o.status[name]
	index, relationships := fulltextRetentionIdentity(name)
	status := FulltextRetentionStatus{Index: index, Relationships: relationships, Retained: true,
		Documents: view.documentCount(), Bytes: bytes, MemoryUsed: budget.Used, MemoryLimit: budget.Limit, MemorySource: budget.Source}
	o.status[name] = status
	report := previous.StorageReads && o.queueTransition(status, true)
	o.changes.Unlock()
	if report {
		o.reportTransitions()
	}
}

func fulltextRetentionIdentity(name string) (string, bool) {
	kind, index, qualified := strings.Cut(name, ":")
	if !qualified {
		return name, false
	}
	return index, kind == "relationships"
}

type fulltextRetentionTransition struct {
	status   FulltextRetentionStatus
	refilled bool
}

// queueTransition is called with changes held. One caller drains committed
// transitions in order, outside all owner locks. Reentrant logger consumers can
// query status or search without recursively acquiring a held owner mutex.
func (o *fulltextDocumentOwner) queueTransition(status FulltextRetentionStatus, refilled bool) bool {
	o.transitions = append(o.transitions, fulltextRetentionTransition{status: status, refilled: refilled})
	if o.reporting {
		return false
	}
	o.reporting = true
	return true
}

func (o *fulltextDocumentOwner) reportTransitions() {
	for {
		o.changes.Lock()
		if len(o.transitions) == 0 {
			o.transitions, o.reporting = nil, false
			o.changes.Unlock()
			return
		}
		next := o.transitions[0]
		o.transitions[0] = fulltextRetentionTransition{}
		o.transitions = o.transitions[1:]
		o.changes.Unlock()
		o.reportTransition(next.status, next.refilled)
	}
}

func (s *Service) reportFulltextTransition(status FulltextRetentionStatus, refilled bool) {
	database := s.cacheNamespace
	if database == "" {
		if namespaced, ok := s.engine.(interface{ Namespace() string }); ok {
			database = namespaced.Namespace()
		}
	}
	if database == "" {
		database = "unscoped"
	}
	message := "Full-text retained copies released: database=%q index=%q relationships=%t memory_used_bytes=%d memory_limit_bytes=%d memory_source=%s; searches on this index now read from storage"
	level := slog.LevelWarn
	if refilled {
		message = "Full-text retained copies refilled: database=%q index=%q relationships=%t memory_used_bytes=%d memory_limit_bytes=%d memory_source=%s; searches on this index reuse the maintained corpus"
		level = slog.LevelInfo
	}
	s.logEvent(context.Background(), level, localization.SearchOperatorEvent(message,
		database, status.Index, status.Relationships, status.MemoryUsed, status.MemoryLimit, status.MemorySource))
}

func fulltextDocumentBytes(doc *FulltextDocument) int64 {
	if doc.RetainedBytes > 0 {
		return doc.RetainedBytes
	}
	return int64(len(doc.Text)) + 128
}

func (v *fulltextDocumentView) captureImage(doc *FulltextDocument, id string, node *storage.Node, edge *storage.Edge) {
	if !v.captureImages {
		return
	}
	if v.images == nil {
		v.images = make(map[*FulltextDocument]fulltextDocumentChange)
	}
	v.images[doc] = fulltextDocumentChange{id: id, node: node, edge: edge}
}

var errFulltextRowUnavailable = errors.New("search: admitted full-text row unavailable")

// Entity materializes an emitted row on the caller's context. A population
// already holds its original image for this request; a reuse hydrates from its
// short-certified committed capture. It never obtains a later live image. A
// callback may be reevaluated once through the original population if a
// captured row is unavailable.
func (d FulltextDocuments) Entity(ctx context.Context, id string, document *FulltextDocument) (*storage.Node, *storage.Edge, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	if image, exists := d.view.images[document]; exists {
		return image.node, image.edge, nil
	}
	if d.snapshot == nil {
		return nil, nil, errFulltextRowUnavailable
	}
	var node *storage.Node
	var edge *storage.Edge
	var err error
	if d.view.relationships {
		edge, err = d.snapshot.ReadEdge(ctx, storage.EdgeID(id))
	} else {
		node, err = d.snapshot.ReadNode(ctx, storage.NodeID(id))
	}
	if err != nil {
		if ctx.Err() != nil {
			return nil, nil, ctx.Err()
		}
		if errors.Is(err, storage.ErrNotFound) {
			// Absence is the native stream's visibility/decode disposition;
			// reacquire the population instead of serving an uncertified row.
			return nil, nil, errFulltextRowUnavailable
		}
		return nil, nil, err
	}
	return node, edge, nil
}
