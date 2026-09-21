package tck

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
)

func compareGraphSnapshots(left, right GraphSnapshot) error {
	leftValues, err := canonicalSnapshot(left)
	if err != nil {
		return err
	}
	rightValues, err := canonicalSnapshot(right)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(leftValues, rightValues) {
		return fmt.Errorf("graph snapshots differ: in transaction %v, after commit %v", leftValues, rightValues)
	}
	return nil
}

func canonicalSnapshot(snapshot GraphSnapshot) ([]string, error) {
	values := make([]string, 0, len(snapshot.Nodes)+len(snapshot.Relationships))
	for _, node := range snapshot.Nodes {
		canonical, err := canonicalValue(node, false)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal([]any{"node", node.Identity, canonical})
		if err != nil {
			return nil, err
		}
		values = append(values, string(encoded))
	}
	for _, relationship := range snapshot.Relationships {
		canonical, err := canonicalValue(relationship, false)
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal([]any{
			"relationship", relationship.Identity, relationship.StartIdentity,
			relationship.EndIdentity, canonical,
		})
		if err != nil {
			return nil, err
		}
		values = append(values, string(encoded))
	}
	sort.Strings(values)
	return values, nil
}

// CompareResults compares columns and rows using the ordering rules declared by
// the scenario. Unordered results are multisets, so duplicate rows are retained.
func CompareResults(actual, expected QueryResult, ordered, ignoreListOrder bool) error {
	if !reflect.DeepEqual(actual.Columns, expected.Columns) {
		return fmt.Errorf("columns differ: got %v, want %v", actual.Columns, expected.Columns)
	}
	if len(actual.Rows) != len(expected.Rows) {
		return fmt.Errorf("row count differs: got %d, want %d", len(actual.Rows), len(expected.Rows))
	}
	got, err := canonicalRows(actual.Rows, ignoreListOrder)
	if err != nil {
		return fmt.Errorf("canonicalize actual rows: %w", err)
	}
	want, err := canonicalRows(expected.Rows, ignoreListOrder)
	if err != nil {
		return fmt.Errorf("canonicalize expected rows: %w", err)
	}
	if !ordered {
		sort.Strings(got)
		sort.Strings(want)
	}
	if !reflect.DeepEqual(got, want) {
		return fmt.Errorf("rows differ: got %v, want %v", got, want)
	}
	return nil
}

func canonicalRows(rows [][]any, ignoreListOrder bool) ([]string, error) {
	result := make([]string, len(rows))
	for i, row := range rows {
		parts := make([]any, len(row))
		for j, value := range row {
			canonical, err := canonicalValue(value, ignoreListOrder)
			if err != nil {
				return nil, err
			}
			parts[j] = canonical
		}
		encoded, err := json.Marshal(parts)
		if err != nil {
			return nil, err
		}
		result[i] = string(encoded)
	}
	return result, nil
}

func canonicalValue(value any, ignoreListOrder bool) (any, error) {
	switch v := value.(type) {
	case nil, bool, string:
		return map[string]any{"type": fmt.Sprintf("%T", v), "value": v}, nil
	case int:
		return typedInteger(int64(v)), nil
	case int32:
		return typedInteger(int64(v)), nil
	case int64:
		return typedInteger(v), nil
	case float32:
		return typedFloat(float64(v)), nil
	case float64:
		return typedFloat(v), nil
	case []any:
		items := make([]any, len(v))
		for i := range v {
			item, err := canonicalValue(v[i], ignoreListOrder)
			if err != nil {
				return nil, err
			}
			items[i] = item
		}
		if ignoreListOrder {
			sort.Slice(items, func(i, j int) bool { return fmt.Sprint(items[i]) < fmt.Sprint(items[j]) })
		}
		return map[string]any{"type": "list", "value": items}, nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		entries := make([]any, 0, len(keys))
		for _, key := range keys {
			item, err := canonicalValue(v[key], ignoreListOrder)
			if err != nil {
				return nil, err
			}
			entries = append(entries, []any{key, item})
		}
		return map[string]any{"type": "map", "value": entries}, nil
	case NodeValue:
		labels := append([]string(nil), v.Labels...)
		sort.Strings(labels)
		properties, err := canonicalValue(v.Properties, ignoreListOrder)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "node", "labels": labels, "properties": properties}, nil
	case RelationshipValue:
		properties, err := canonicalValue(v.Properties, ignoreListOrder)
		if err != nil {
			return nil, err
		}
		return map[string]any{"type": "relationship", "relationshipType": v.Type, "properties": properties}, nil
	case PathValue:
		nodes := make([]any, len(v.Nodes))
		for i := range v.Nodes {
			node, err := canonicalValue(v.Nodes[i], ignoreListOrder)
			if err != nil {
				return nil, err
			}
			nodes[i] = node
		}
		segments := make([]any, len(v.Segments))
		for i := range v.Segments {
			relationship, err := canonicalValue(v.Segments[i].Relationship, ignoreListOrder)
			if err != nil {
				return nil, err
			}
			segments[i] = map[string]any{"forward": v.Segments[i].Forward, "relationship": relationship}
		}
		return map[string]any{"type": "path", "nodes": nodes, "segments": segments}, nil
	default:
		return nil, fmt.Errorf("unsupported result value %T", value)
	}
}

func typedInteger(value int64) map[string]any {
	return map[string]any{"type": "integer", "value": strconv.FormatInt(value, 10)}
}

func typedFloat(value float64) map[string]any {
	representation := strconv.FormatFloat(value, 'g', -1, 64)
	if math.IsNaN(value) {
		representation = "NaN"
	} else if math.IsInf(value, 1) {
		representation = "Inf"
	} else if math.IsInf(value, -1) {
		representation = "-Inf"
	}
	return map[string]any{"type": "float", "value": representation}
}

// ObserveSideEffects computes net observable changes rather than trusting
// mutation counters returned by an engine.
func ObserveSideEffects(before, after GraphSnapshot) (SideEffects, error) {
	beforeNodes, err := identitySetNodes(before.Nodes)
	if err != nil {
		return SideEffects{}, err
	}
	afterNodes, err := identitySetNodes(after.Nodes)
	if err != nil {
		return SideEffects{}, err
	}
	beforeRelationships, err := identitySetRelationships(before.Relationships)
	if err != nil {
		return SideEffects{}, err
	}
	afterRelationships, err := identitySetRelationships(after.Relationships)
	if err != nil {
		return SideEffects{}, err
	}
	addedNodes, removedNodes := setDelta(beforeNodes, afterNodes)
	addedRelationships, removedRelationships := setDelta(beforeRelationships, afterRelationships)
	beforeProperties, err := propertySet(before)
	if err != nil {
		return SideEffects{}, err
	}
	afterProperties, err := propertySet(after)
	if err != nil {
		return SideEffects{}, err
	}
	addedProperties, removedProperties := setDelta(beforeProperties, afterProperties)
	beforeLabels := labelSet(before.Nodes)
	afterLabels := labelSet(after.Nodes)
	addedLabels, removedLabels := setDelta(beforeLabels, afterLabels)
	return SideEffects{
		AddedNodes: addedNodes, RemovedNodes: removedNodes,
		AddedRelationships: addedRelationships, RemovedRelationships: removedRelationships,
		AddedProperties: addedProperties, RemovedProperties: removedProperties,
		AddedLabels: addedLabels, RemovedLabels: removedLabels,
	}, nil
}

func identitySetNodes(nodes []NodeValue) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node.Identity == "" {
			return nil, fmt.Errorf("snapshot node has empty identity")
		}
		if _, exists := set[node.Identity]; exists {
			return nil, fmt.Errorf("duplicate snapshot node identity %q", node.Identity)
		}
		set[node.Identity] = struct{}{}
	}
	return set, nil
}

func identitySetRelationships(relationships []RelationshipValue) (map[string]struct{}, error) {
	set := make(map[string]struct{}, len(relationships))
	for _, relationship := range relationships {
		if relationship.Identity == "" {
			return nil, fmt.Errorf("snapshot relationship has empty identity")
		}
		if _, exists := set[relationship.Identity]; exists {
			return nil, fmt.Errorf("duplicate snapshot relationship identity %q", relationship.Identity)
		}
		set[relationship.Identity] = struct{}{}
	}
	return set, nil
}

func propertySet(snapshot GraphSnapshot) (map[string]struct{}, error) {
	set := map[string]struct{}{}
	add := func(kind, identity string, properties map[string]any) error {
		for key, value := range properties {
			canonical, err := canonicalValue(value, false)
			if err != nil {
				return err
			}
			encoded, err := json.Marshal(canonical)
			if err != nil {
				return err
			}
			set[kind+"\x00"+identity+"\x00"+key+"\x00"+string(encoded)] = struct{}{}
		}
		return nil
	}
	for _, node := range snapshot.Nodes {
		if err := add("node", node.Identity, node.Properties); err != nil {
			return nil, err
		}
	}
	for _, relationship := range snapshot.Relationships {
		if err := add("relationship", relationship.Identity, relationship.Properties); err != nil {
			return nil, err
		}
	}
	return set, nil
}

func labelSet(nodes []NodeValue) map[string]struct{} {
	set := map[string]struct{}{}
	for _, node := range nodes {
		for _, label := range node.Labels {
			set[label] = struct{}{}
		}
	}
	return set
}

func setDelta(before, after map[string]struct{}) (added, removed int) {
	for value := range after {
		if _, exists := before[value]; !exists {
			added++
		}
	}
	for value := range before {
		if _, exists := after[value]; !exists {
			removed++
		}
	}
	return added, removed
}
