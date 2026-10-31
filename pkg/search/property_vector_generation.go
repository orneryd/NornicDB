package search

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// PrepareIndexBuild binds a build to the admission of its trigger. There
// remains one build implementation.
func (s *Service) PrepareIndexBuild(origin uint64) func(context.Context) error {
	return func(ctx context.Context) error { return s.buildIndexes(ctx, origin) }
}

// selectPropertyVectors is the installation boundary for descriptor retirement,
// shared by nodes, relationships and persisted populations. Rejected old work
// leaves a newer installed value intact, including properties absent from its
// image. An empty previous population (a baseline) cannot resurrect anything.
func (s *Service) selectPropertyVectors(entity string, labels []string, properties map[string]any, previous map[string][]float32, origin uint64) map[string]any {
	schema := s.engine.GetSchema()
	if schema == nil || schema.VectorIndexGeneration() <= origin {
		return properties
	}
	var selected map[string]any
	clone := func() {
		if selected != nil {
			return
		}
		selected = make(map[string]any, len(properties)+len(previous))
		for key, value := range properties {
			selected[key] = value
		}
	}
	for key := range properties {
		if !schema.VectorEntryGenerationCurrent(entity, labels, key, origin) {
			clone()
			if vector, ok := previous[key]; ok {
				selected[key] = vector
			} else {
				delete(selected, key)
			}
		}
	}
	for key, vector := range previous {
		if _, ok := properties[key]; !ok && !schema.VectorEntryGenerationCurrent(entity, labels, key, origin) {
			clone()
			selected[key] = vector
		}
	}
	if selected != nil {
		return selected
	}
	return properties
}

func (s *Service) selectNodePropertyVectorsLocked(node *storage.Node, origin uint64) *storage.Node {
	if node == nil {
		return node
	}
	schema := s.engine.GetSchema()
	if schema == nil || schema.VectorIndexGeneration() <= origin {
		return node
	}
	var previous map[string][]float32
	s.mu.RLock()
	if entries := s.nodePropVector[string(node.ID)]; len(entries) > 0 {
		previous = make(map[string][]float32, len(entries))
		for property, key := range entries {
			if vector, ok := s.getVectorForCypherLocked(key); ok {
				previous[property] = vector
			}
		}
	}
	s.mu.RUnlock()
	copyNode := *node
	copyNode.Properties = s.selectPropertyVectors("NODE", node.Labels, node.Properties, previous, origin)
	return &copyNode
}

// Restored metadata bypasses node streaming. Reconcile it at this same
// installation boundary before build completion, using no prior population.
func (s *Service) retireStalePropertyVectors(origin uint64) {
	s.indexMu.Lock()
	defer s.indexMu.Unlock()
	for id, entries := range s.nodePropVector {
		properties := make(map[string]any, len(entries))
		for property := range entries {
			properties[property] = true
		}
		selected := s.selectPropertyVectors("NODE", s.nodeLabels[id], properties, nil, origin)
		for property := range entries {
			if _, ok := selected[property]; !ok {
				s.removeNodePropertyVectorLocked(id, property)
			}
		}
	}
	for id, entries := range s.edgePropVector {
		properties := make(map[string]any, len(entries))
		for property := range entries {
			properties[property] = true
		}
		selected := s.selectPropertyVectors("RELATIONSHIP", []string{s.edgeTypes[id]}, properties, nil, origin)
		for property := range entries {
			if _, ok := selected[property]; !ok {
				delete(entries, property)
			}
		}
		if len(entries) == 0 {
			delete(s.edgePropVector, id)
			delete(s.edgeTypes, id)
		}
	}
}

// RemoveVectorIndex uses the existing ordered teardown owner for the exact
// descriptor. origin was captured by the DDL origin before schema publication.
func (s *Service) RemoveVectorIndex(index storage.VectorIndex, origin uint64) {
	if s == nil || index.Property == "" {
		return
	}
	s.orderWithBuild(func() { s.removePropertyVectorIndexOwned(index.Property, indexRemoval{index, origin}) })
}

type indexRemoval struct {
	index  storage.VectorIndex
	origin uint64
}

func (s *Service) vectorDescriptorApplies(removal indexRemoval, entity string, labels []string) bool {
	domain := string(removal.index.EntityType)
	if domain == "" {
		domain = "NODE"
	}
	if domain != entity {
		return false
	}
	matches := removal.index.Label == ""
	for _, label := range labels {
		if label == removal.index.Label {
			matches = true
		}
	}
	if !matches {
		return false
	}
	// Use the same schema decision as entry selection; a surviving sibling can
	// still own this canonical property entry (for example a multi-label node).
	selected := s.selectPropertyVectors(entity, labels, map[string]any{removal.index.Property: true}, nil, removal.origin)
	_, retained := selected[removal.index.Property]
	return !retained
}

func (s *Service) removeNodePropertyVectorLocked(nodeID, property string) {
	props := s.nodePropVector[nodeID]
	key, ok := props[property]
	if !ok {
		return
	}
	before := s.beginClusterNodeChangeLocked(nodeID)
	s.removeVectorLocked(key)
	if s.servingGPUIndex() != nil {
		_ = s.servingGPUIndex().Remove(key)
	}
	s.hnswRemoveLive(key)
	s.retireClusterVectorLocked(key)
	delete(props, property)
	if len(props) == 0 {
		delete(s.nodePropVector, nodeID)
	}
	s.publishNodeMembersLocked(nodeID)
	s.endClusterNodeChangeLocked(nodeID, before)
}
