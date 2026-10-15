package cypher

import (
	"sort"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// newGraphProjection creates the typed storage shared by both import forms.
func newGraphProjection(name, database string) *GraphProjection {
	return &GraphProjection{
		Name:                     name,
		Database:                 database,
		NodeLabels:               make(map[string][]string),
		NodeProperties:           make(projectedNodePropertyValueMap),
		NodePropertyTypes:        make(map[string]string),
		NodePropertyKeys:         make(map[string][]string),
		RelationshipPropertyKeys: make(map[string][]string),
		NodeProjection:           make(map[string]interface{}),
		RelationshipProjection:   make(map[string]interface{}),
		CreatedAt:                time.Now(),
	}
}

// indexNodes establishes the same deterministic node order for every reader.
func (p *GraphProjection) indexNodes() {
	sort.Strings(p.NodeIDs)
	p.nodeIndex = gdsNodeIndex(p.NodeIDs)
}

// finishCounts records catalog totals once each import has supplied its labels
// and types. Native projections retain requested empty labels/types; aggregation
// imports infer them only from their rows.
func (p *GraphProjection) finishCounts() {
	contexts := 0
	if len(p.RelationshipProjection) > 0 {
		contexts = len(p.Types)
		for _, raw := range p.RelationshipProjection {
			if spec, ok := raw.(map[string]interface{}); ok && spec["indexInverse"] == true {
				contexts++
			}
		}
	}
	p.relationshipTypeOrder = gdsRelationshipTypeOrder(p.Types, contexts)
	if len(p.relationshipTypeOrder) > 1 {
		p.relationshipTypeIndex = gdsNodeIndex(p.relationshipTypeOrder)
	}
	sort.Strings(p.Labels)
	sort.Strings(p.Types)
	p.NodeCount = len(p.NodeIDs)
	p.RelationshipCount = len(p.Relationships)
}

// gdsRelationshipTypeOrder reproduces the constituent order of GDS 2.13's
// relationship import maps and CSRGraphStore's mutable map copy. Every Java
// map transition goes through the shared iteration-order model.
func gdsRelationshipTypeOrder(types []string, nativeContexts int) []string {
	if len(types) < 2 {
		return types
	}
	ordered := types
	mode := gdsJavaConcurrentMap
	if nativeContexts > 0 {
		ordered = gdsJavaMapOrder(ordered, 31, nativeContexts, gdsJavaMapCompute)
		mode = gdsJavaMapPut
	}
	ordered = gdsJavaMapOrder(ordered, 31, 16, mode)
	return gdsJavaMapOrder(ordered, 31, len(types)*4/3+1, gdsJavaMapPut)
}

// registerGraphProjection owns the atomic catalog insertion for both imports.
// Only the duplicate-name message differs between GDS's two entry points.
func registerGraphProjection(p *GraphProjection, aggregation bool) error {
	key := gdsCatalogKey(p.Database, p.Name)
	projectionsMu.Lock()
	defer projectionsMu.Unlock()
	if _, exists := graphProjections[key]; exists {
		if aggregation {
			return localizedError(localization.CypherGraphProceduresGraphExistsAggregation(p.Name), nil)
		}
		return localizedError(localization.CypherGraphProceduresGraphAlreadyExists(p.Name), nil)
	}
	graphProjections[key] = p
	return nil
}

// resolveRelationshipTypes owns wildcard expansion and projected-type
// validation. Empty orientation/index lists mean none; algorithm lists mean all.
func (p *GraphProjection) resolveRelationshipTypes(types []string, allIfEmpty bool, purpose string) ([]string, error) {
	if containsString(types, "*") || allIfEmpty && len(types) == 0 {
		return p.Types, nil
	}
	if missing := missingNames(types, p.Types); len(missing) > 0 {
		projected := p.Types
		if len(projected) == 0 && purpose != "selection" {
			projected = []string{"__ALL__"}
		}
		switch purpose {
		case "undirected":
			return nil, localizedError(localization.CypherGraphProceduresUndirectedTypesNotProjected("["+strings.Join(missing, ", ")+"]", gdsNameList(projected)), nil)
		case "inverse":
			return nil, localizedError(localization.CypherGraphProceduresInverseTypesNotProjected("["+strings.Join(missing, ", ")+"]", gdsNameList(projected)), nil)
		default:
			return nil, localizedError(localization.CypherGraphProceduresRelationshipTypesNotInGraph(gdsNameList(missing), gdsNameList(projected)), nil)
		}
	}
	return types, nil
}

// gdsNodeIndex owns node-to-position maps for catalogs and selected topology.
func gdsNodeIndex(nodes []string) map[string]int {
	index := make(map[string]int, len(nodes))
	for i, id := range nodes {
		index[id] = i
	}
	return index
}

// gdsRelationshipDirections owns orientation for both projection imports.
func gdsRelationshipDirections(source, target, orientation string) [][2]string {
	switch orientation {
	case "REVERSE":
		return [][2]string{{target, source}}
	case "UNDIRECTED":
		return [][2]string{{source, target}, {target, source}}
	default:
		return [][2]string{{source, target}}
	}
}

// storeNodeProperty is the single typed node-value storage path. Importers
// supply only the exact value returned by gdsConvertPropertyValue.
func (p *GraphProjection) storeNodeProperty(id, key string, exact interface{}) {
	if p.NodeProperties[id] == nil {
		p.NodeProperties[id] = make(map[string]interface{})
	}
	p.NodeProperties[id][key] = exact
}

// matchesNodeLabels is shared by topology selection and the node-property
// utility. A label-qualified utility read of a node outside that label is null.
func (p *GraphProjection) matchesNodeLabels(id string, labels []string) bool {
	return len(labels) == 0 || containsString(labels, "*") || anyString(p.NodeLabels[id], labels)
}

// nodePropertyAvailability owns label-aware schema validation for algorithms.
// A usable property must be projected for every selected label. GDS's errors
// still list the union from the whole projection, including unselected labels.
func (p *GraphProjection) nodePropertyAvailability(labels []string) (selected, available []string) {
	if len(labels) == 0 || containsString(labels, "*") {
		labels = p.Labels
	}
	selected = commonKeys(p.NodePropertyKeys, labels)
	for _, keys := range p.NodePropertyKeys {
		for _, key := range keys {
			available = appendUnique(available, key)
		}
	}
	sort.Strings(available)
	return selected, available
}
