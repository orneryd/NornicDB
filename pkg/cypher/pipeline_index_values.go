package cypher

import (
	"sort"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type indexedPropertyValues struct {
	labels   []string
	property string
	values   []interface{}
}

// Complex index keys use the ordinary predicate route, whose recursive value
// readers carry cancellation. This seek only formats bounded scalar keys.
func boundedPipelineIndexValue(value interface{}) bool {
	switch value := value.(type) {
	case nil, bool, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return true
	case string:
		return len(value) <= 4096
	case CypherDate, *CypherDate, CypherLocalTime, *CypherLocalTime, CypherTime, *CypherTime,
		CypherLocalDateTime, *CypherLocalDateTime, CypherDateTime, *CypherDateTime,
		CypherDuration, *CypherDuration, CypherPoint, *CypherPoint, time.Time, *time.Time:
		return true
	default:
		return false
	}
}

// All list-index routes share ID collection, ordering and materialization.
// The optional probe belongs to their caller, including parameter preparation.
func (e *StorageExecutor) collectIndexedPropertyValues(nodePattern nodePatternInfo, anyOf []string, schema *storage.SchemaManager, seeks []indexedPropertyValues, probe *pipelineCancellation) ([]*storage.Node, bool, error) {
	idSet := make(map[storage.NodeID]struct{}, 256)
	for _, seek := range seeks {
		for _, label := range seek.labels {
			if probe != nil && !probe.check() {
				return nil, true, probe.err
			}
			for _, value := range seek.values {
				if probe != nil && !probe.check() {
					return nil, true, probe.err
				}
				if probe != nil && !boundedPipelineIndexValue(value) {
					return nil, false, nil
				}
				for _, id := range propertyIndexLookup(e.storage, schema, label, seek.property, value) {
					if probe != nil && !probe.check() {
						return nil, true, probe.err
					}
					idSet[id] = struct{}{}
				}
			}
		}
	}
	if len(idSet) == 0 {
		return []*storage.Node{}, true, nil
	}
	ids := make([]string, 0, len(idSet))
	for id := range idSet {
		if probe != nil && !probe.check() {
			return nil, true, probe.err
		}
		ids = append(ids, string(id))
	}
	if probe == nil {
		sort.Strings(ids)
	} else if !probe.sort(ids, func(i, j int) bool { return comparePipelineStrings(ids[i], ids[j], probe) < 0 }, false) {
		return nil, true, probe.err
	}
	nodes := make([]*storage.Node, 0, len(ids))
	for _, id := range ids {
		if probe != nil && !probe.check() {
			return nil, true, probe.err
		}
		node, err := e.getQueryNode(e.storage, storage.NodeID(id))
		if err != nil || node == nil {
			continue
		}
		if !seekCandidateHasLabels(node, nodePattern.labels, anyOf) {
			continue
		}
		nodes = append(nodes, node)
	}
	return nodes, true, nil
}
