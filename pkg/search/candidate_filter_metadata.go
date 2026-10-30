package search

import (
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// wantedTypesLower is the one construction of the search API's wanted-type
// membership set. It is computed once per request or scan, never per
// candidate, and holds exactly strings.ToLower of each requested type: the
// canonical loaded-node predicate (nodeMatchesType) compares lower
// forms, not EqualFold forms.
// An empty types list yields nil, which no predicate matches.
func wantedTypesLower(types []string) map[string]struct{} {
	if len(types) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(types))
	for _, wantedType := range types {
		wanted[strings.ToLower(wantedType)] = struct{}{}
	}
	return wanted
}

// filterByIndexedType applies the search API's label-or-type-property semantics
// from compact metadata maintained alongside the search indexes. It deliberately
// avoids loading nodes: document properties can be orders of magnitude larger
// than the labels needed by this predicate.
func (s *Service) filterByIndexedType(results []indexResult, types []string) ([]indexResult, bool) {
	s.indexMu.RLock()
	defer s.indexMu.RUnlock()
	for _, result := range results {
		if _, known := s.nodeLabels[result.ID]; !known {
			return nil, false
		}
	}

	// Candidate slices are pipeline-owned and discarded after filtering. Reuse
	// their backing array to keep this metadata-only path allocation-free.
	admit := s.indexedTypeAdmitter(types)
	filtered := results[:0]
	for _, result := range results {
		if admit(result.ID) {
			filtered = append(filtered, result)
		}
	}
	return filtered, true
}

// indexedTypeAdmitter returns the search API's label-or-type-property test
// for one node ID against types, from the compact metadata filterByIndexedType
// uses. A node with no metadata is admitted, and left to the full filter.
// The returned function reads s.nodeLabels and s.nodeTypes without locking:
// call it with s.indexMu held.
func (s *Service) indexedTypeAdmitter(types []string) func(id string) bool {
	return func(id string) bool {
		labels, known := s.nodeLabels[id]
		if !known {
			return true
		}
		for _, label := range labels {
			for _, nodeType := range types {
				if strings.EqualFold(label, nodeType) {
					return true
				}
			}
		}
		if indexedType, hasType := s.nodeTypes[id]; hasType {
			for _, nodeType := range types {
				if strings.EqualFold(indexedType, nodeType) {
					return true
				}
			}
		}
		return false
	}
}

// nodeMatchesType is the label-or-type-property predicate shared by batch
// candidate filtering and complete continuation scans. wanted must be
// wantedTypesLower(types); a nil or empty set matches nothing, which callers
// avoid by testing len(types) first.
func nodeMatchesType(node *storage.Node, wanted map[string]struct{}) bool {
	for _, label := range node.Labels {
		if _, ok := wanted[strings.ToLower(label)]; ok {
			return true
		}
	}
	nodeType, ok := node.Properties["type"].(string)
	if !ok {
		return false
	}
	_, ok = wanted[strings.ToLower(nodeType)]
	return ok
}
