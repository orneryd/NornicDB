package cypher

import (
	"context"
	"slices"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func (e *StorageExecutor) callDbSchemaVisualizationWithContext(ctx context.Context) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	nodes, err := store.AllNodes()
	if err != nil {
		return nil, err
	}
	edges, err := store.AllEdges()
	if err != nil {
		return nil, err
	}
	labels := make(map[string]bool)
	nodesByID := make(map[storage.NodeID]*storage.Node, len(nodes))
	for _, node := range nodes {
		nodesByID[node.ID] = node
		for _, label := range node.Labels {
			labels[label] = true
		}
	}
	virtualNodes := make([]*storage.Node, 0, len(labels))
	virtualByLabel := make(map[string]*storage.Node, len(labels))
	for _, label := range sortedStringSet(labels) {
		indexes, constraints := schemaVisualizationLabelMetadata(store.GetSchema(), label)
		node := &storage.Node{
			ID:         storage.NodeID("-" + strconv.Itoa(len(virtualNodes)+1)),
			Labels:     []string{label},
			Properties: map[string]interface{}{"name": label, "indexes": indexes, "constraints": constraints},
		}
		virtualNodes = append(virtualNodes, node)
		virtualByLabel[label] = node
	}
	startLabels := make(map[string]map[string]bool)
	endLabels := make(map[string]map[string]bool)
	types := make(map[string]bool)
	for _, edge := range edges {
		types[edge.Type] = true
		if startLabels[edge.Type] == nil {
			startLabels[edge.Type] = make(map[string]bool)
			endLabels[edge.Type] = make(map[string]bool)
		}
		if node := nodesByID[edge.StartNode]; node != nil {
			for _, label := range node.Labels {
				startLabels[edge.Type][label] = true
			}
		}
		if node := nodesByID[edge.EndNode]; node != nil {
			for _, label := range node.Labels {
				endLabels[edge.Type][label] = true
			}
		}
	}
	virtualEdges := make([]*storage.Edge, 0)
	for _, relationshipType := range sortedStringSet(types) {
		for _, start := range sortedStringSet(startLabels[relationshipType]) {
			for _, end := range sortedStringSet(endLabels[relationshipType]) {
				virtualEdges = append(virtualEdges, &storage.Edge{
					ID:         storage.EdgeID("-" + strconv.Itoa(len(virtualNodes)+len(virtualEdges)+1)),
					StartNode:  virtualByLabel[start].ID,
					EndNode:    virtualByLabel[end].ID,
					Type:       relationshipType,
					Properties: map[string]interface{}{"name": relationshipType},
				})
			}
		}
	}
	return &ExecuteResult{
		Columns: []string{"nodes", "relationships"},
		Rows:    [][]interface{}{{virtualNodes, virtualEdges}},
	}, nil
}

func schemaVisualizationLabelMetadata(schema *storage.SchemaManager, label string) ([]string, []string) {
	indexes := []string{}
	constraints := []string{}
	ownedIndexes := make(map[string]bool)
	for _, constraint := range schema.GetConstraintsForLabels([]string{label}) {
		if constraint.EffectiveEntityType() != storage.ConstraintEntityNode {
			continue
		}
		if statement, ok := showConstraintCreateStatement(constraint).(string); ok {
			constraints = append(constraints, statement)
		}
		ownedIndexes[constraint.OwnedIndex] = true
	}
	for _, raw := range schema.GetIndexes() {
		index, ok := raw.(map[string]interface{})
		if !ok || index["entityType"] == string(storage.ConstraintEntityRelationship) {
			continue
		}
		name, _ := index["name"].(string)
		if ownedIndexes[name] {
			continue
		}
		labels, _ := index["labels"].([]string)
		if index["label"] != label && !slices.Contains(labels, label) {
			continue
		}
		properties, _ := index["properties"].([]string)
		if property, ok := index["property"].(string); ok {
			properties = []string{property}
		}
		if len(properties) > 0 {
			indexes = append(indexes, strings.Join(properties, ","))
		}
	}
	slices.Sort(indexes)
	slices.Sort(constraints)
	return indexes, constraints
}
