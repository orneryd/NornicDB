// Plan 04-05-06: cmd/nornicdb wiring for EmbedMetrics + SearchMetrics.
//
// embedQueueProbe + searchServiceProbe are thin adapters that satisfy
// observability.EmbedProbe / observability.SearchProbe by routing into
// the *EmbedQueue (alias for *EmbedWorker) and *search.Service.
//
// attachLocalGGUFEmbedderMetrics walks the cached_embedder wrapper chain
// to find an underlying *embed.LocalGGUFEmbedder and calls AttachMetrics
// on it (D-09 FFI panic counter wiring). The walk is type-assertion
// based and tolerates the !localllm stub (which has its own no-op
// AttachMetrics so this code path is build-tag-symmetric).
package main

import (
	"github.com/orneryd/nornicdb/pkg/embed"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/search"
)

// embedQueueProbe satisfies observability.EmbedProbe by delegating to
// the EmbedQueue's QueueLen accessor (Plan 04-05 D-15b).
type embedQueueProbe struct {
	q *nornicdb.EmbedQueue
}

func (p embedQueueProbe) QueueLen() int {
	if p.q == nil {
		return 0
	}
	return p.q.QueueLen()
}

// searchServiceProbe satisfies observability.SearchProbe by delegating
// to the search.Service's IndexSizeBytes accessor (Plan 04-05 D-15b).
type searchServiceProbe struct {
	svc *search.Service
}

func (p searchServiceProbe) IndexSizeBytes(kind string) uint64 {
	if p.svc == nil {
		return 0
	}
	return p.svc.IndexSizeBytes(kind)
}

// attachEmbedMetricsToEmbedder walks the embedder reference looking for
// an underlying *embed.LocalGGUFEmbedder and calls AttachMetrics on it
// (Plan 04-05-03 D-09 FFI panic counter wiring). Returns true when a
// LocalGGUFEmbedder was found and attached.
//
// The input is `any` to tolerate both pkg/embed.Embedder and the
// pkg/cypher.QueryEmbedder minimal interface — the cypher executor
// returns the latter to avoid an import cycle, so a type switch lets
// us recover the concrete type when it's a LocalGGUFEmbedder.
func attachEmbedMetricsToEmbedder(e any, m *observability.EmbedMetrics) bool {
	if e == nil || m == nil {
		return false
	}
	// Direct LocalGGUFEmbedder.
	if local, ok := e.(*embed.LocalGGUFEmbedder); ok {
		local.AttachMetrics(m)
		return true
	}
	// CachedEmbedder wraps; the AttachMetrics surface is exposed only
	// on the LocalGGUF type today. Traversal into the cached layer is
	// deferred (CachedEmbedder.Base() not yet exported).
	return false
}
