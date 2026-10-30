package cypher

import "github.com/orneryd/nornicdb/pkg/storage"

type optimisticMutationMeta struct {
	CreatedNodeIDs         []string `json:"createdNodeIds,omitempty"`
	CreatedRelationshipIDs []string `json:"createdRelationshipIds,omitempty"`

	// seen sets make the add helpers O(1). The previous linear scan over the
	// accumulating ID slices made a statement that creates N entities
	// quadratic (N²/2 string comparisons): a single wiring pass of 48k edges
	// spent ~1.15 billion compares just de-duplicating already-unique IDs.
	// These maps are unexported so they never round-trip through JSON.
	seenNodeIDs         map[string]struct{}
	seenRelationshipIDs map[string]struct{}
}

func addOptimisticNodeID(result *ExecuteResult, id storage.NodeID) {
	if result == nil || id == "" {
		return
	}
	meta := ensureOptimisticMeta(result)
	idStr := string(id)
	if meta.seenNodeIDs == nil {
		meta.seenNodeIDs = make(map[string]struct{})
	}
	if _, exists := meta.seenNodeIDs[idStr]; exists {
		return
	}
	meta.seenNodeIDs[idStr] = struct{}{}
	meta.CreatedNodeIDs = append(meta.CreatedNodeIDs, idStr)
}

func addOptimisticRelationshipID(result *ExecuteResult, id storage.EdgeID) {
	if result == nil || id == "" {
		return
	}
	meta := ensureOptimisticMeta(result)
	idStr := string(id)
	if meta.seenRelationshipIDs == nil {
		meta.seenRelationshipIDs = make(map[string]struct{})
	}
	if _, exists := meta.seenRelationshipIDs[idStr]; exists {
		return
	}
	meta.seenRelationshipIDs[idStr] = struct{}{}
	meta.CreatedRelationshipIDs = append(meta.CreatedRelationshipIDs, idStr)
}

func ensureOptimisticMeta(result *ExecuteResult) *optimisticMutationMeta {
	if result.Metadata == nil {
		result.Metadata = make(map[string]interface{})
	}
	if raw, ok := result.Metadata["optimistic"]; ok {
		if typed, ok := raw.(*optimisticMutationMeta); ok && typed != nil {
			return typed
		}
		if typed, ok := raw.(optimisticMutationMeta); ok {
			copyVal := typed
			result.Metadata["optimistic"] = &copyVal
			return &copyVal
		}
	}
	meta := &optimisticMutationMeta{}
	result.Metadata["optimistic"] = meta
	return meta
}
