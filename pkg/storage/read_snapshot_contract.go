// Author: Codex implementation lane, 2026-10-27 (NOR-148).
package storage

import "context"

// SnapshotReader names the acquisition contract a captured view must preserve.
// Zero keeps the existing stored build snapshot; the other values preserve the
// corresponding public reader, including its error scope and fallback policy.
type SnapshotReader uint8

const (
	SnapshotStored SnapshotReader = iota
	SnapshotAllNodes
	SnapshotAllEdges
	SnapshotStreamEdges
)

type snapshotReaderKey struct{}

// WithSnapshotReader requests a public-reader contract from OpenReadSnapshot.
// It does not discover through semantic decorators or create a second capture.
func WithSnapshotReader(ctx context.Context, reader SnapshotReader) context.Context {
	return context.WithValue(ctx, snapshotReaderKey{}, reader)
}

func requestedSnapshotReader(ctx context.Context) SnapshotReader {
	reader, _ := ctx.Value(snapshotReaderKey{}).(SnapshotReader)
	return reader
}

// SnapshotStreamFallbackContext preserves an issued stream forwarder's existing
// AllEdges fallback. A native stream error never selects this fallback.
func SnapshotStreamFallbackContext(ctx context.Context, inner Engine) context.Context {
	if requestedSnapshotReader(ctx) == SnapshotStreamEdges {
		if _, native := inner.(StreamingEngine); !native {
			return WithSnapshotReader(ctx, SnapshotAllEdges)
		}
	}
	return ctx
}

// SnapshotReaderOwner certifies the exact issued reader. An inherited snapshot
// method alone cannot certify a decorator that overrides materialized reads.
// Semantic decorators without this explicit contract use their original reader.
type SnapshotReaderOwner interface {
	OwnsSnapshotReader(Engine) bool
}

func (b *BadgerEngine) OwnsSnapshotReader(issued Engine) bool     { return issued == b }
func (m *MemoryEngine) OwnsSnapshotReader(issued Engine) bool     { return issued == m }
func (n *NamespacedEngine) OwnsSnapshotReader(issued Engine) bool { return issued == n }
func (w *WALEngine) OwnsSnapshotReader(issued Engine) bool        { return issued == w }
func (t *TracedEngine) OwnsSnapshotReader(issued Engine) bool     { return issued == t }

// SnapshotReadRevision is an opaque proof from the canonical commit oracle.
// It describes the captured physical point and graph revision, not wall time.
type SnapshotReadRevision struct {
	oracle                       *commitOracle
	timestamp, graph             uint64
	readerLatest, readerPrevious uint64
	reader                       SnapshotReader
}

// SingleReaderCommitAfter proves exactly one reader publication separates the
// views: one write, with all of its commits, batches and follow-up commits.
// Event coverage independently proves that publication's complete graph delta.
func (r SnapshotReadRevision) SingleReaderCommitAfter(previous SnapshotReadRevision) bool {
	return r.oracle == previous.oracle && r.reader == previous.reader && r.readerLatest > previous.readerLatest && r.readerPrevious == previous.readerLatest
}

// SameReaderView proves no reader publication separates the views.
func (r SnapshotReadRevision) SameReaderView(previous SnapshotReadRevision) bool {
	return r.oracle == previous.oracle && r.reader == previous.reader && r.graph == previous.graph && r.readerLatest == previous.readerLatest
}

func (r SnapshotReadRevision) GraphMutationVersion() uint64 { return r.graph }

// QueryReadSnapshot preserves a named original reader at one committed point.
// AllNodes and AllEdges propagate materialized-read errors; StreamEdges retains
// its native streaming policy. Reader identifies any forwarding fallback taken.
type QueryReadSnapshot interface {
	VisibleReadSnapshot
	IterateAllEdges(context.Context, func(*Edge) error) error
	Reader() SnapshotReader
	CapturedRevision() (SnapshotReadRevision, bool)
	CurrentRevision() (SnapshotReadRevision, bool)
}

func (b *BadgerEngine) snapshotReadRevision(reader SnapshotReader) (SnapshotReadRevision, bool) {
	if b.pendingNamespaceConflict("") != nil {
		return SnapshotReadRevision{}, false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed || b.db == nil {
		return SnapshotReadRevision{}, false
	}
	o := b.db.oracle
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.readsHeld || o.failed != nil || !o.readerPairCurrentLocked() {
		return SnapshotReadRevision{}, false
	}
	return SnapshotReadRevision{oracle: o, timestamp: o.published.Load(), graph: b.graphMutationVersions.read(""), reader: reader, readerLatest: o.readerLatest, readerPrevious: o.readerPrevious}, true
}

func (s *badgerReadSnapshot) Reader() SnapshotReader { return s.reader }
func (s *badgerReadSnapshot) CapturedRevision() (SnapshotReadRevision, bool) {
	return s.revision, s.revisionValid
}
func (s *badgerReadSnapshot) CurrentRevision() (SnapshotReadRevision, bool) {
	r, ok := s.b.snapshotReadRevision(s.reader)
	return r, ok && r.oracle == s.view.db.oracle
}

func (s *namespacedReadSnapshot) Reader() SnapshotReader { return s.inner.(QueryReadSnapshot).Reader() }
func (s *namespacedReadSnapshot) CapturedRevision() (SnapshotReadRevision, bool) {
	return s.inner.(QueryReadSnapshot).CapturedRevision()
}
func (s *namespacedReadSnapshot) CurrentRevision() (SnapshotReadRevision, bool) {
	return s.inner.(QueryReadSnapshot).CurrentRevision()
}

// IterateAllEdges preserves materialization errors before namespace filtering.
func (s *namespacedReadSnapshot) IterateAllEdges(ctx context.Context, visit func(*Edge) error) error {
	return s.inner.(QueryReadSnapshot).IterateAllEdges(ctx, func(edge *Edge) error {
		if edge == nil || !s.n.hasEdgePrefix(edge.ID) {
			return nil
		}
		return visit(s.n.toUserEdge(edge))
	})
}
