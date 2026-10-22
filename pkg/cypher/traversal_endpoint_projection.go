package cypher

import (
	"slices"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// retainTraversalEndpointNode consumes the fresh caller-owned canonical view
// before it enters nodeCache or a path binding. Empty demand retains a header;
// nil demand preserves the full entity. Reuse the node and small property map.
func retainTraversalEndpointNode(node *storage.Node, properties []string) *storage.Node {
	if node == nil || properties == nil {
		return node
	}
	var selected map[string]interface{}
	if len(properties) > 0 {
		selected = node.Properties
		if len(selected) > 8 && len(selected) > 2*len(properties) {
			// Do not retain a wide body's map buckets for a narrow selection.
			selected = make(map[string]interface{}, len(properties))
			for _, property := range properties {
				if value, ok := node.Properties[property]; ok {
					selected[property] = value
				}
			}
		} else {
			for property := range selected {
				if !slices.Contains(properties, property) {
					delete(selected, property)
				}
			}
		}
	}
	var metadata map[string]interface{}
	if slices.Contains(properties, "has_embedding") {
		metadata = map[string]interface{}{"has_embedding": nodeHasEmbeddingValue(node)}
	}
	// Timestamps belong to property visibility. Vectors, embedding bodies and
	// storage snapshot owners are unrelated to an ID/label/scalar projection.
	*node = storage.Node{ID: node.ID, Labels: node.Labels, Properties: selected,
		CreatedAt: node.CreatedAt, UpdatedAt: node.UpdatedAt,
		VisibilitySuppressed: node.VisibilitySuppressed, EmbedMeta: metadata}
	return node
}

func traversalEndpointProperties(node nodePatternInfo, where string, tail []string) []string {
	properties := make([]string, 0, len(node.properties)+2)
	for property := range node.properties {
		properties = append(properties, property)
	}
	keep := func(property string) bool {
		if property == "" {
			properties = nil
			return false
		}
		if !slices.Contains(properties, property) {
			properties = append(properties, property)
		}
		return true
	}
	traversalVisitVariableProperties(where, node.variable, keep)
	for _, text := range tail {
		if properties == nil {
			return nil
		}
		traversalVisitVariableProperties(text, node.variable, keep)
	}
	return properties
}

func traversalEndpointTailReadsProperties(node nodePatternInfo, where string, tail []string) bool {
	if len(node.properties) != 0 || traversalVariablePropertiesRead(where, node.variable) {
		return true
	}
	for _, text := range tail {
		if traversalVariablePropertiesRead(text, node.variable) {
			return true
		}
	}
	return false
}
