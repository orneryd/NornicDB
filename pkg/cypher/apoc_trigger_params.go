package cypher

import (
	"sort"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// apocTriggerParams builds a trigger pass's transaction-state parameters
// from the change set, as APOC 5's TriggerMetadata.toMap does from the
// kernel's TransactionData:
//
//	$createdNodes, $deletedNodes                 lists of nodes
//	$createdRelationships, $deletedRelationships lists of relationships
//	$assignedLabels, $removedLabels              label -> list of nodes
//	$assignedNodeProperties                      key -> [{key, node, old, new}]
//	$removedNodeProperties                       key -> [{key, node, old}]
//	$assignedRelationshipProperties              key -> [{key, relationship, old, new}]
//	$removedRelationshipProperties               key -> [{key, relationship, old}]
//	$transactionId, $commitTime                  -1 before the commit (before, rollback)
//	$metaData                                    the transaction's metadata
//
// As in the kernel's data, a created entity's properties are assigned
// (old null) and its labels assigned; a deleted entity's are removed; an
// entity created and deleted in the transaction is in none; adding a label
// a node has is no assignment, and setting a property to the value it has
// is no assignment.
//
// The old and new values are the pass's; the entities are read live from
// the view each trigger runs over (APOC's entities are references), so a
// later trigger of the pass reads what an earlier one wrote. A deleted
// entity is its last state: reading its properties gives their old values
// (in Neo4j they can be read only with apoc.any.properties).
type apocTriggerParams struct {
	changes *storage.ChangeSet
	phase   apocTriggerPhase
}

func newApocTriggerParams(changes *storage.ChangeSet, phase apocTriggerPhase) apocTriggerParams {
	return apocTriggerParams{changes: changes, phase: phase}
}

// params returns a fresh parameter map for one trigger, its entities read
// through view.
func (p apocTriggerParams) params(view storage.Engine) map[string]any {
	changes := p.changes
	live := p.phase != apocTriggerRollback // a rolled-back transaction's entities are its last states
	nodes := make(map[storage.NodeID]*storage.Node, len(changes.Nodes))
	node := func(change storage.NodeChange) *storage.Node {
		if change.After == nil {
			return apocTriggerUserNode(change.Before)
		}
		id := apocTriggerLocalNodeID(change.ID)
		if cached, ok := nodes[id]; ok {
			return cached
		}
		var current *storage.Node
		if live {
			if read, err := view.GetNode(id); err == nil && read != nil {
				current = read
			}
		}
		if current == nil {
			current = apocTriggerUserNode(change.After)
		}
		nodes[id] = current
		return current
	}
	edge := func(change storage.EdgeChange) *storage.Edge {
		if change.After == nil {
			return apocTriggerUserEdge(change.Before)
		}
		if live {
			if read, err := view.GetEdge(apocTriggerLocalEdgeID(change.ID)); err == nil && read != nil {
				return read
			}
		}
		return apocTriggerUserEdge(change.After)
	}

	createdNodes, deletedNodes := []any{}, []any{}
	assignedLabels, removedLabels := map[string]any{}, map[string]any{}
	assignedNodeProperties, removedNodeProperties := map[string]any{}, map[string]any{}
	addTo := func(index map[string]any, key string, value any) {
		list, _ := index[key].([]any)
		index[key] = append(list, value)
	}
	for _, change := range changes.Nodes {
		entity := node(change)
		var before, after map[string]any
		var beforeLabels, afterLabels []string
		switch {
		case change.Before == nil:
			createdNodes = append(createdNodes, entity)
			after, afterLabels = change.After.Properties, change.After.Labels
		case change.After == nil:
			deletedNodes = append(deletedNodes, entity)
			before, beforeLabels = change.Before.Properties, change.Before.Labels
		default:
			before, beforeLabels = change.Before.Properties, change.Before.Labels
			after, afterLabels = change.After.Properties, change.After.Labels
		}
		removed, added := storage.LabelChanges(beforeLabels, afterLabels)
		for _, label := range added {
			addTo(assignedLabels, label, entity)
		}
		for _, label := range removed {
			addTo(removedLabels, label, entity)
		}
		apocTriggerPropertyEntries(before, after, "node", entity, addTo, assignedNodeProperties, removedNodeProperties)
	}

	createdRelationships, deletedRelationships := []any{}, []any{}
	assignedRelationshipProperties, removedRelationshipProperties := map[string]any{}, map[string]any{}
	for _, change := range changes.Edges {
		entity := edge(change)
		var before, after map[string]any
		switch {
		case change.Before == nil:
			createdRelationships = append(createdRelationships, entity)
			after = change.After.Properties
		case change.After == nil:
			deletedRelationships = append(deletedRelationships, entity)
			before = change.Before.Properties
		default:
			before, after = change.Before.Properties, change.After.Properties
		}
		apocTriggerPropertyEntries(before, after, "relationship", entity, addTo, assignedRelationshipProperties, removedRelationshipProperties)
	}

	transactionID, commitTime := int64(-1), int64(-1)
	if p.phase == apocTriggerAfter || p.phase == apocTriggerAfterAsync {
		transactionID = int64(changes.CommitSequence)
		if !changes.CommitTime.IsZero() {
			commitTime = changes.CommitTime.UnixMilli()
		}
	}
	metadata := make(map[string]any, len(changes.Metadata))
	for key, value := range changes.Metadata {
		metadata[key] = value
	}
	return map[string]any{
		"transactionId":                  transactionID,
		"commitTime":                     commitTime,
		"createdNodes":                   createdNodes,
		"createdRelationships":           createdRelationships,
		"deletedNodes":                   deletedNodes,
		"deletedRelationships":           deletedRelationships,
		"removedLabels":                  removedLabels,
		"removedNodeProperties":          removedNodeProperties,
		"removedRelationshipProperties":  removedRelationshipProperties,
		"assignedLabels":                 assignedLabels,
		"assignedNodeProperties":         assignedNodeProperties,
		"assignedRelationshipProperties": assignedRelationshipProperties,
		"metaData":                       metadata,
	}
}

// apocTriggerPropertyEntries adds an entity's property changes from before
// to after (nil for a created or deleted entity): {key, <kind>, old, new}
// for a key it has after, {key, <kind>, old} for one it lost.
func apocTriggerPropertyEntries(before, after map[string]any, kind string, entity any, addTo func(map[string]any, string, any), assigned, removed map[string]any) {
	for _, key := range apocTriggerChangedKeys(before, after) {
		newValue, has := after[key]
		if has {
			addTo(assigned, key, map[string]any{"key": key, kind: entity, "old": before[key], "new": newValue})
			continue
		}
		addTo(removed, key, map[string]any{"key": key, kind: entity, "old": before[key]})
	}
}

// apocTriggerChangedKeys is the change-capture property diff, all of a
// created or deleted entity's keys included, sorted.
func apocTriggerChangedKeys(before, after map[string]any) []string {
	if before != nil && after != nil {
		return storage.ChangedProperties(before, after)
	}
	whole := before
	if whole == nil {
		whole = after
	}
	keys := make([]string, 0, len(whole))
	for key := range whole {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func apocTriggerLocalNodeID(id storage.NodeID) storage.NodeID {
	if _, local, ok := storage.ParseDatabasePrefix(string(id)); ok {
		return storage.NodeID(local)
	}
	return id
}

func apocTriggerLocalEdgeID(id storage.EdgeID) storage.EdgeID {
	if _, local, ok := storage.ParseDatabasePrefix(string(id)); ok {
		return storage.EdgeID(local)
	}
	return id
}

// apocTriggerUserNode is a private copy of a change set's node with its
// database-local ID: the change set's nodes are the transaction's own.
func apocTriggerUserNode(node *storage.Node) *storage.Node {
	copied := storage.CopyNode(node)
	copied.ID = apocTriggerLocalNodeID(copied.ID)
	return copied
}

func apocTriggerUserEdge(edge *storage.Edge) *storage.Edge {
	copied := storage.CopyEdge(edge)
	copied.ID = apocTriggerLocalEdgeID(copied.ID)
	copied.StartNode = apocTriggerLocalNodeID(copied.StartNode)
	copied.EndNode = apocTriggerLocalNodeID(copied.EndNode)
	return copied
}
