package storage

import (
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/observability"
)

// accessIncrementor is the subset of knowledgepolicy.AccessAccumulator needed
// by the decay filter to record access events on surviving nodes.
type accessIncrementor interface {
	IncrementAccess(entityID string)
}

// SetDecayEnabled enables or disables knowledge-layer decay scoring on read paths.
func (b *BadgerEngine) SetDecayEnabled(enabled bool) {
	b.decayEnabled = enabled
}

// SetAccessAccumulator wires the P-local accumulator so read paths can record
// access events for nodes that survive visibility filtering.
func (b *BadgerEngine) SetAccessAccumulator(acc accessIncrementor) {
	b.accumulator = acc
}

// SetRevealAll disables decay suppression for all entities in the current query.
// Call ClearRevealAll after the query completes.
func (b *BadgerEngine) SetRevealAll(reveal bool) {
	b.revealAll.Store(reveal)
}

// BeginQueryRevealScope guards a query's reveal mode against concurrent queries
// on the same engine. Reveal queries take an exclusive scope while normal
// queries take a shared scope so they cannot observe revealAll=true.
func (b *BadgerEngine) BeginQueryRevealScope(reveal bool) func() {
	if reveal {
		b.revealQueryMu.Lock()
		b.revealAll.Store(true)
		return func() {
			b.revealAll.Store(false)
			b.revealQueryMu.Unlock()
		}
	}

	b.revealQueryMu.RLock()
	return func() {
		b.revealQueryMu.RUnlock()
	}
}

// readVisibility is whether a read applies the visibility layer: on-demand
// suppression, knowledge-policy decay and the index tombstones decay leaves
// (filterNodeByDecay, filterEdgeByDecay, hasIndexTombstone). User reads do,
// unless a reveal scope is open. Integrity work reads what storage holds: a
// node delete's cascade and connected check, constraint checks, count
// bookkeeping, index backfills, replay, snapshots, backups and data-subject
// requests. The layer hides entities from users without removing them, so a
// hidden relationship still connects its endpoints and a hidden value still
// occupies its unique key; integrity work that skipped them left dangling
// relationships, admitted duplicates or lost data.
type readVisibility bool

const (
	// visibleRead applies the visibility layer: user-facing reads.
	visibleRead readVisibility = false
	// storedRead returns every stored entity: integrity work
	// (StoredEntityReader).
	storedRead readVisibility = true
)

// hidesNode reports whether a read with visibility v leaves node out.
func (b *BadgerEngine) hidesNode(v readVisibility, node *Node, nowNanos int64) bool {
	return v == visibleRead && b.filterNodeByDecay(node, nowNanos)
}

// hidesEdge reports whether a read with visibility v leaves edge out.
func (b *BadgerEngine) hidesEdge(v readVisibility, edge *Edge, nowNanos int64) bool {
	return v == visibleRead && b.filterEdgeByDecay(edge, nowNanos)
}

// checksIndexTombstones reports whether a read with visibility v skips index
// entries decay tombstoned.
func (b *BadgerEngine) checksIndexTombstones(v readVisibility) bool {
	return v == visibleRead && b.decayEnabled && !b.revealAll.Load()
}

// filterEntityByDecay is the shared kernel behind filterNodeByDecay and
// filterEdgeByDecay. It resolves the namespace scorer, applies the
// reveal-all scope, scores the entity, and decides suppression. The
// entity-specific scoring call is selected by isEdge so the hot scan path
// stays allocation-free.
func (b *BadgerEngine) filterEntityByDecay(
	scorer *knowledgepolicy.Scorer,
	kind string,
	entityID string,
	visibilitySuppressed bool,
	labels []string,
	entityType string,
	entityProps map[string]interface{},
	createdAtNanos, versionAtNanos, nowNanos int64,
	isEdge bool,
) bool {
	if scorer == nil {
		if visibilitySuppressed {
			if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
				kp.IncReadFilterDropped(kind, "")
				kp.IncSuppression(kind, "explicit_flag", "")
			}
			return true
		}
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(entityID); err == nil {
		accessMeta = meta
	}

	var res knowledgepolicy.ScoringResolution
	if isEdge {
		res = scorer.ScoreEdgeWithProperties(entityID, entityType, entityProps, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
	} else {
		res = scorer.ScoreNodeWithProperties(entityID, labels, entityProps, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
	}

	if res.NoDecay && visibilitySuppressed {
		if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
			kp.IncReadFilterDropped(kind, "")
			kp.IncSuppression(kind, "explicit_flag", "")
		}
		return true
	}
	if res.SuppressionEligible {
		if kp := observability.GetKnowledgePolicyMetrics(); kp != nil {
			kp.IncReadFilterDropped(kind, "")
		}
		return true
	}
	return false
}

// filterNodeByDecay returns true if the node should be suppressed from results.
// It only decides visibility. Access recording is deferred until a query
// actually materializes the entity into the final result set.
func (b *BadgerEngine) filterNodeByDecay(node *Node, nowNanos int64) bool {
	if node == nil || b.revealAll.Load() {
		return false
	}
	if b.manuallySuppressed(manualSuppressionNode, string(node.ID)) {
		return true
	}
	if !b.decayEnabled {
		return false
	}
	return b.filterEntityByDecay(
		b.getScorerForNode(node.ID),
		"node",
		string(node.ID),
		node.VisibilitySuppressed,
		node.Labels,
		"",
		node.Properties,
		node.CreatedAt.UnixNano(),
		node.UpdatedAt.UnixNano(),
		nowNanos,
		false,
	)
}

// filterEdgeByDecay returns true if the edge should be suppressed from results.
func (b *BadgerEngine) filterEdgeByDecay(edge *Edge, nowNanos int64) bool {
	if edge == nil || b.revealAll.Load() {
		return false
	}
	if b.manuallySuppressed(manualSuppressionEdge, string(edge.ID)) {
		return true
	}
	if !b.decayEnabled {
		return false
	}
	return b.filterEntityByDecay(
		b.getScorerForEdge(edge.ID),
		"edge",
		string(edge.ID),
		edge.VisibilitySuppressed,
		nil,
		edge.Type,
		edge.Properties,
		edge.CreatedAt.UnixNano(),
		edge.UpdatedAt.UnixNano(),
		nowNanos,
		true,
	)
}

// getScorerForNode resolves a Scorer from the node's namespace SchemaManager.
func (b *BadgerEngine) getScorerForNode(nodeID NodeID) *knowledgepolicy.Scorer {
	return b.getScorerForNamespace(extractNamespaceFromID(string(nodeID)))
}

// getScorerForEdge resolves a Scorer from the edge's namespace SchemaManager.
func (b *BadgerEngine) getScorerForEdge(edgeID EdgeID) *knowledgepolicy.Scorer {
	return b.getScorerForNamespace(extractNamespaceFromID(string(edgeID)))
}

// getScorerForNamespace builds a Scorer from the namespace's BindingTable.
func (b *BadgerEngine) getScorerForNamespace(namespace string) *knowledgepolicy.Scorer {
	b.schemasMu.RLock()
	sm := b.schemas[namespace]
	b.schemasMu.RUnlock()

	if sm == nil {
		return nil
	}

	bt := sm.GetBindingTable()
	if bt == nil {
		return nil
	}

	return knowledgepolicy.NewScorer(
		knowledgepolicy.NewResolver(bt, nil),
		true,
	)
}

// DecayScoringTime returns a frozen nanosecond timestamp for use as the scoring
// time across a single query. Call once per query and pass to all filter calls.
func DecayScoringTime() int64 {
	return time.Now().UnixNano()
}

// filterPropertyByDecay is the shared kernel behind FilterPropertyByDecay and
// FilterEdgePropertyByDecay. The entity-specific scoring call is selected by
// isEdge so the property-read path stays allocation-free.
func (b *BadgerEngine) filterPropertyByDecay(
	scorer *knowledgepolicy.Scorer,
	entityID string,
	labels []string,
	entityType, propKey string,
	accessMeta *knowledgepolicy.AccessMetaEntry,
	createdAtNanos, versionAtNanos, nowNanos int64,
	isEdge bool,
) bool {
	var res knowledgepolicy.ScoringResolution
	if isEdge {
		res = scorer.ScoreEdgeProperty(entityID, entityType, propKey, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
	} else {
		res = scorer.ScoreProperty(entityID, labels, propKey, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
	}
	return res.SuppressionEligible
}

// FilterPropertyByDecay returns true if the property should be hidden from results.
func (b *BadgerEngine) FilterPropertyByDecay(nodeID NodeID, labels []string, propKey string, createdAtNanos, versionAtNanos, nowNanos int64) bool {
	return b.filterNodePropertyByDecay(nodeID, labels, propKey, createdAtNanos, versionAtNanos, nowNanos, true)
}

func (b *BadgerEngine) filterNodePropertyByDecay(nodeID NodeID, labels []string, propKey string, createdAtNanos, versionAtNanos, nowNanos int64, honorReveal bool) bool {
	if !b.decayEnabled {
		return false
	}
	if honorReveal && b.revealAll.Load() {
		return false
	}

	scorer := b.getScorerForNode(nodeID)
	if scorer == nil {
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(string(nodeID)); err == nil {
		accessMeta = meta
	}

	return b.filterPropertyByDecay(scorer, string(nodeID), labels, "", propKey, accessMeta, createdAtNanos, versionAtNanos, nowNanos, false)
}

// FilterEdgePropertyByDecay returns true if the edge property should be hidden from results.
func (b *BadgerEngine) FilterEdgePropertyByDecay(edgeID EdgeID, edgeType, propKey string, createdAtNanos, versionAtNanos, nowNanos int64) bool {
	return b.filterRelationshipPropertyByDecay(edgeID, edgeType, propKey, createdAtNanos, versionAtNanos, nowNanos, true)
}

func (b *BadgerEngine) filterRelationshipPropertyByDecay(edgeID EdgeID, edgeType, propKey string, createdAtNanos, versionAtNanos, nowNanos int64, honorReveal bool) bool {
	if !b.decayEnabled {
		return false
	}
	if honorReveal && b.revealAll.Load() {
		return false
	}

	scorer := b.getScorerForEdge(edgeID)
	if scorer == nil {
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(string(edgeID)); err == nil {
		accessMeta = meta
	}

	return b.filterPropertyByDecay(scorer, string(edgeID), nil, edgeType, propKey, accessMeta, createdAtNanos, versionAtNanos, nowNanos, true)
}

// ScorerForNamespace returns a Scorer for the given namespace, or nil.
func (b *BadgerEngine) ScorerForNamespace(namespace string) *knowledgepolicy.Scorer {
	return b.getScorerForNamespace(namespace)
}

// IsDecayEnabled reports whether knowledge-layer decay scoring is active.
func (b *BadgerEngine) IsDecayEnabled() bool {
	return b.decayEnabled
}

// ExtractNamespaceFromID returns the namespace prefix of an entity ID.
func ExtractNamespaceFromID(id string) string {
	return extractNamespaceFromID(id)
}

func extractNamespaceFromID(id string) string {
	if idx := strings.IndexByte(id, ':'); idx >= 0 {
		return id[:idx]
	}
	return "nornic"
}
