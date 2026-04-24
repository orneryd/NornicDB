package storage

import (
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
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

// filterNodeByDecay returns true if the node should be suppressed from results.
// When a node survives filtering and an accumulator is set, it records the access.
func (b *BadgerEngine) filterNodeByDecay(node *Node, nowNanos int64) bool {
	if !b.decayEnabled || node == nil {
		return false
	}
	if b.revealAll.Load() {
		return false
	}

	scorer := b.getScorerForNode(node.ID)
	if scorer == nil {
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(string(node.ID)); err == nil {
		accessMeta = meta
	}

	input := knowledgepolicy.NodeScoringInput{
		EntityID:           string(node.ID),
		Labels:             node.Labels,
		CreatedAtNanos:     node.CreatedAt.UnixNano(),
		VersionAtNanos:     node.UpdatedAt.UnixNano(),
		LegacyAccessCount:  node.AccessCount,
		LegacyLastAccessed: node.LastAccessed.UnixNano(),
	}

	suppress, _ := knowledgepolicy.ShouldSuppressNode(scorer, input, accessMeta, nowNanos)
	if !suppress && b.accumulator != nil {
		b.accumulator.IncrementAccess(string(node.ID))
	}
	return suppress
}

// filterEdgeByDecay returns true if the edge should be suppressed from results.
func (b *BadgerEngine) filterEdgeByDecay(edge *Edge, nowNanos int64) bool {
	if !b.decayEnabled || edge == nil {
		return false
	}
	if b.revealAll.Load() {
		return false
	}

	scorer := b.getScorerForEdge(edge.ID)
	if scorer == nil {
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(string(edge.ID)); err == nil {
		accessMeta = meta
	}

	suppress, _ := knowledgepolicy.ShouldSuppressEdge(
		scorer, edge.Type, string(edge.ID), accessMeta,
		edge.CreatedAt.UnixNano(), nowNanos,
	)
	if !suppress && b.accumulator != nil {
		b.accumulator.IncrementAccess(string(edge.ID))
	}
	return suppress
}

// getScorerForNode resolves a Scorer from the node's namespace SchemaManager.
func (b *BadgerEngine) getScorerForNode(nodeID NodeID) *knowledgepolicy.Scorer {
	ns := extractNamespaceFromID(string(nodeID))
	return b.getScorerForNamespace(ns)
}

// getScorerForEdge resolves a Scorer from the edge's namespace SchemaManager.
func (b *BadgerEngine) getScorerForEdge(edgeID EdgeID) *knowledgepolicy.Scorer {
	ns := extractNamespaceFromID(string(edgeID))
	return b.getScorerForNamespace(ns)
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

// FilterPropertyByDecay returns true if the property should be hidden from results.
func (b *BadgerEngine) FilterPropertyByDecay(nodeID NodeID, labels []string, propKey string, createdAtNanos, versionAtNanos, nowNanos int64) bool {
	if !b.decayEnabled {
		return false
	}
	if b.revealAll.Load() {
		return false
	}

	ns := extractNamespaceFromID(string(nodeID))
	scorer := b.getScorerForNamespace(ns)
	if scorer == nil {
		return false
	}

	var accessMeta *knowledgepolicy.AccessMetaEntry
	if meta, err := b.GetAccessMeta(string(nodeID)); err == nil {
		accessMeta = meta
	}

	res := scorer.ScoreProperty(string(nodeID), labels, propKey, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
	return res.SuppressionEligible
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
