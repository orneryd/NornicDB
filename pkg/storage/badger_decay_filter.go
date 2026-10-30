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
// (filterEntityByDecay, hasIndexTombstone). User reads do,
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

// checksIndexTombstones reports whether a read with visibility v skips index
// entries decay tombstoned.
func (b *BadgerEngine) checksIndexTombstones(v readVisibility) bool {
	return v == visibleRead && b.decayEnabled && !b.revealAll.Load()
}

// decayPropertyRead selects property scoring without loading or copying its entity.
// storedRead ignores a query reveal scope for normal access-event projection;
// it still applies property policy, unlike stored entity reads.
type decayPropertyRead struct {
	id               string
	labels           []string
	edgeType         string
	property         string
	created, updated int64
	isEdge           bool
}

// filterEntityByDecay owns entity and property visibility for every native read.
// It only decides visibility; access recording belongs to final projection.
// Pointer inputs and property descriptors stay on the caller's stack.
func (b *BadgerEngine) filterEntityByDecay(v readVisibility, entity any, nowNanos int64) bool {
	// Most reads have no active policy. Resolve that case before inspecting
	// the entity: cached adjacency calls this once per relationship, and the
	// type/metadata dispatch otherwise adds cost even with decay disabled.
	if (v == visibleRead && !b.visibilityFiltered()) || (v == storedRead && !b.decayEnabled) {
		return false
	}
	var entityID, kind string
	var manualKind byte
	var node *Node
	var edge *Edge
	var propertyTarget *decayPropertyRead
	switch value := entity.(type) {
	case *Node:
		if value == nil {
			return false
		}
		node, entityID, kind, manualKind = value, string(value.ID), "node", manualSuppressionNode
	case *Edge:
		if value == nil {
			return false
		}
		edge, entityID, kind, manualKind = value, string(value.ID), "edge", manualSuppressionEdge
	case *decayPropertyRead:
		if value == nil {
			return false
		}
		propertyTarget, entityID = value, value.id
	default:
		return false
	}
	propertyRead := propertyTarget != nil
	if !propertyRead && v == storedRead {
		return false
	}
	if !propertyRead && b.manuallySuppressed(manualKind, entityID) {
		return true
	}
	if !b.decayEnabled {
		return false
	}

	var labels []string
	var entityType, property string
	var entityProps map[string]interface{}
	var visibilitySuppressed, isEdge bool
	var createdAtNanos, versionAtNanos int64
	switch {
	case node != nil:
		labels, entityProps, visibilitySuppressed = node.Labels, node.Properties, node.RuntimeMetadata().VisibilitySuppressed
		createdAtNanos, versionAtNanos = node.CreatedAt.UnixNano(), node.UpdatedAt.UnixNano()
	case edge != nil:
		entityType, entityProps, visibilitySuppressed, isEdge = edge.Type, edge.Properties, edge.VisibilitySuppressed, true
		createdAtNanos, versionAtNanos = edge.CreatedAt.UnixNano(), edge.UpdatedAt.UnixNano()
	default:
		labels, entityType, property, isEdge = propertyTarget.labels, propertyTarget.edgeType, propertyTarget.property, propertyTarget.isEdge
		createdAtNanos, versionAtNanos = propertyTarget.created, propertyTarget.updated
	}
	scorer := b.getScorerForNamespace(extractNamespaceFromID(entityID))
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
	if result, err := b.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{Mode: knowledgepolicy.AccessMetaReadPoint, EntityID: entityID}); err == nil {
		accessMeta = result.Entry
	}

	var res knowledgepolicy.ScoringResolution
	if propertyRead {
		if isEdge {
			res = scorer.ScoreEdgeProperty(entityID, entityType, property, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
		} else {
			res = scorer.ScoreProperty(entityID, labels, property, accessMeta, createdAtNanos, versionAtNanos, nowNanos)
		}
		return res.SuppressionEligible
	}
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
	return b.filterEntityByDecay(visibleRead, &decayPropertyRead{id: string(nodeID), labels: labels, property: propKey, created: createdAtNanos, updated: versionAtNanos}, nowNanos)
}

// FilterEdgePropertyByDecay returns true if the edge property should be hidden from results.
func (b *BadgerEngine) FilterEdgePropertyByDecay(edgeID EdgeID, edgeType, propKey string, createdAtNanos, versionAtNanos, nowNanos int64) bool {
	return b.filterEntityByDecay(visibleRead, &decayPropertyRead{id: string(edgeID), edgeType: edgeType, property: propKey, created: createdAtNanos, updated: versionAtNanos, isEdge: true}, nowNanos)
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
