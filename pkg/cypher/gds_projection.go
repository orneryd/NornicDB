package cypher

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GraphProjection is a gds.graph.project graph (#1190): its nodes, in node
// ID order, and its relationships, oriented as projected. An UNDIRECTED
// relationship is held in both directions, as GDS holds and counts it.
// NodeProperties preserves each projected numeric type; direct Go consumers
// must handle int64, float64, []int64 and []float64 values rather than assume
// float64. For example, age, ok := projection.NodeProperties["alice"]["age"].(int64).
type GraphProjection struct {
	Name           string
	NodeIDs        []string
	NodeLabels     map[string][]string           // node -> the projected labels it carries
	Labels         []string                      // the projected labels ("__ALL__" for '*')
	NodeProperties projectedNodePropertyValueMap // node -> typed value (int64, float64, []int64 or []float64)
	// NodePropertyTypes is each projected property key's GDS type:
	// Integer, Float, List of Integer or List of Float.
	NodePropertyTypes        map[string]string
	NodePropertyKeys         map[string][]string // projected label -> its property keys
	Relationships            []ProjectedRelationship
	Types                    []string            // the projected relationship types ("__ALL__" for '*')
	RelationshipPropertyKeys map[string][]string // projected type -> its property keys
	NodeProjection           map[string]interface{}
	RelationshipProjection   map[string]interface{}
	NodeCount                int
	RelationshipCount        int
	CreatedAt                time.Time
	Database                 string
	Configuration            map[string]interface{} // as gds.graph.list reports it
	schema                   map[string]interface{} // GDS's schema, with each relationship type's direction
	nodeIndex                map[string]int
}

// ProjectedRelationship is one relationship of a projection, from Source
// to Target as projected.
type ProjectedRelationship struct {
	ID         string // the stored relationship's id
	Source     string
	Target     string
	Type       string             // the projected type's name
	Properties map[string]float64 // the projected properties that have a value
}

// Global graph projection store (thread-safe): one catalog per database, as
// GDS keeps it (#1161), keyed by gdsCatalogKey.
var (
	graphProjections = make(map[string]*GraphProjection)
	projectionsMu    sync.RWMutex
)

// gdsCatalogKey is a projection's key in the catalog: its database and name.
func gdsCatalogKey(database, name string) string {
	return database + "\x00" + name
}

// lookupGraphProjection is database's projection called name, or GDS's
// error for a graph the catalog doesn't hold.
func lookupGraphProjection(database, name string) (*GraphProjection, error) {
	projectionsMu.RLock()
	projection, exists := graphProjections[gdsCatalogKey(database, name)]
	projectionsMu.RUnlock()
	if !exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphNotFound(name, database), nil)
	}
	return projection, nil
}

// graphProjection is this database's projection called name.
func (e *StorageExecutor) graphProjection(name string) (*GraphProjection, error) {
	return lookupGraphProjection(e.databaseName(), name)
}

// databaseProjections is database's projections, by name.
func databaseProjections(database string) []*GraphProjection {
	projectionsMu.RLock()
	defer projectionsMu.RUnlock()
	var projections []*GraphProjection
	for _, projection := range graphProjections {
		if projection.Database == database {
			projections = append(projections, projection)
		}
	}
	sort.Slice(projections, func(i, j int) bool { return projections[i].Name < projections[j].Name })
	return projections
}

// gdsPropertySpec is one projected property: its key in the graph, the
// stored property it reads, its default value and, for a relationship
// property, its aggregation.
type gdsPropertySpec struct {
	key, property string
	defaultValue  interface{} // exact configured Integer or Float; nil when absent
	aggregation   string
}

// gdsElementSpec is one node or relationship projection entry.
type gdsElementSpec struct {
	name        string // the projected label or type
	element     string // the stored label or type; "*" for every one
	orientation string // relationships: NATURAL, REVERSE or UNDIRECTED
	aggregation string // relationships: DEFAULT, NONE, SINGLE, SUM, MIN, MAX or COUNT
	properties  []gdsPropertySpec
	// indexInverse is the relationships' inverse-index setting, recorded
	// and reported as given; NornicDB reads relationships both ways without
	// one.
	indexInverse bool
}

// merges reports whether the projection merges parallel relationships: an
// aggregation other than DEFAULT or NONE, the relationships' own or one of
// their properties', as GDS merges them (#1193 review).
func (spec gdsElementSpec) merges() bool {
	if spec.aggregation != "DEFAULT" && spec.aggregation != "NONE" {
		return true
	}
	for _, property := range spec.properties {
		if property.aggregation != "DEFAULT" && property.aggregation != "NONE" {
			return true
		}
	}
	return false
}

// callGdsGraphProject implements CALL gds.graph.project(graphName,
// nodeProjection, relationshipProjection) as GDS does (#1190). A projection
// is '*' (everything), a label or type name, a list of them, or a map from
// the projected name to the stored label or type, or to its settings
// (label / type, properties, and for relationships orientation, aggregation
// and indexInverse). A name text may also list several, separated by ':' or
// '|' ('Person:User', 'KNOWS|REFERENCES'): NornicDB's kept form. Labels,
// types and properties no stored element has, and a name already in use,
// are refused with GDS's messages.
func (e *StorageExecutor) callGdsGraphProject(args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.graph.project"
	started := time.Now()
	graphName, err := requiredProcedureString(procedure, args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	nodeSpecs, err := gdsProjectionSpecs(procedureArgument(args, 1), "node")
	if err != nil {
		return nil, err
	}
	relSpecs, err := gdsProjectionSpecs(procedureArgument(args, 2), "relationship")
	if err != nil {
		return nil, err
	}
	configuration, err := gdsProjectConfiguration(procedure, args, nodeSpecs, relSpecs)
	if err != nil {
		return nil, err
	}
	projection, err := e.buildGraphProjection(graphName, nodeSpecs, relSpecs, configuration["validateRelationships"] == true)
	if err != nil {
		return nil, err
	}
	configuration["nodeProjection"] = projection.NodeProjection
	configuration["relationshipProjection"] = projection.RelationshipProjection
	projection.Configuration = configuration
	projection.Database = e.databaseName()

	if err := registerGraphProjection(projection, false); err != nil {
		return nil, err
	}

	return &ExecuteResult{
		Columns: []string{"nodeProjection", "relationshipProjection", "graphName", "nodeCount", "relationshipCount", "projectMillis"},
		Rows: [][]any{{
			projection.NodeProjection, projection.RelationshipProjection, graphName,
			int64(projection.NodeCount), int64(projection.RelationshipCount), time.Since(started).Milliseconds(),
		}},
	}, nil
}

// gdsProjectConfiguration reads gds.graph.project's configuration map
// (#1161): readConcurrency, jobId, logProgress, sudo, validateRelationships,
// and nodeProperties / relationshipProperties, which every node or
// relationship projection then projects. It is the configuration as
// gds.graph.list reports it.
func gdsProjectConfiguration(procedure string, args []interface{}, nodeSpecs, relSpecs []gdsElementSpec) (map[string]interface{}, error) {
	options, err := optionalProcedureMap(procedure, args, 3, "configuration")
	if err != nil {
		return nil, err
	}
	c, err := newGDSConfig(options, "readConcurrency", "jobId", "logProgress", "sudo", "validateRelationships", "nodeProperties", "relationshipProperties")
	if err != nil {
		return nil, err
	}
	readConcurrency, err := c.integer("readConcurrency", "Integer", 1, 1<<31-1, 4, false)
	if err != nil {
		return nil, err
	}
	flags := map[string]bool{"logProgress": true, "sudo": false, "validateRelationships": false}
	for _, key := range []string{"logProgress", "sudo", "validateRelationships"} {
		value, given, err := c.boolean(key)
		if err != nil {
			return nil, err
		}
		if given {
			flags[key] = value
		}
	}
	for _, kind := range []struct {
		key   string
		specs []gdsElementSpec
	}{{"nodeProperties", nodeSpecs}, {"relationshipProperties", relSpecs}} {
		for i := range kind.specs {
			properties, err := gdsPropertySpecs(c.value(kind.key), kind.specs[i].aggregation)
			if err != nil {
				return nil, err
			}
			kind.specs[i].properties = append(kind.specs[i].properties, properties...)
		}
	}
	jobID, err := c.jobID()
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		"jobId": jobID, "readConcurrency": readConcurrency, "logProgress": flags["logProgress"], "sudo": flags["sudo"],
		"validateRelationships": flags["validateRelationships"], "nodeProperties": map[string]interface{}{}, "relationshipProperties": map[string]interface{}{},
	}, nil
}

// gdsProjectionSpecs reads a node or relationship projection argument.
func gdsProjectionSpecs(value interface{}, kind string) ([]gdsElementSpec, error) {
	nameKey := "label"
	if kind == "relationship" {
		nameKey = "type"
	}
	var specs []gdsElementSpec
	add := func(name, element string) {
		spec := gdsElementSpec{name: name, element: element, orientation: "NATURAL", aggregation: "DEFAULT"}
		if element == "*" {
			spec.name = "__ALL__"
		}
		specs = append(specs, spec)
	}
	addText := func(text string) {
		for _, name := range strings.FieldsFunc(text, func(r rune) bool { return r == ':' || r == '|' }) {
			if name = strings.TrimSpace(name); name != "" {
				add(name, name)
			}
		}
	}
	switch projection := value.(type) {
	case nil:
	case string:
		addText(projection)
	case map[string]interface{}:
		keys := make([]string, 0, len(projection))
		for key := range projection {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			spec := gdsElementSpec{name: key, element: key, orientation: "NATURAL", aggregation: "DEFAULT"}
			switch entry := projection[key].(type) {
			case nil:
			case string:
				spec.element = entry
			case map[string]interface{}:
				if err := spec.read(entry, kind, nameKey); err != nil {
					return nil, err
				}
			default:
				return nil, localizedError(localization.CypherGraphProceduresInvalidProjection(kind, gdsJavaClass(entry)), nil)
			}
			specs = append(specs, spec)
		}
	default:
		items, isList := cypherListValue(value)
		if !isList {
			return nil, localizedError(localization.CypherGraphProceduresInvalidProjection(kind, gdsJavaClass(value)), nil)
		}
		for _, item := range items {
			name, isString := item.(string)
			if !isString {
				return nil, localizedError(localization.CypherGraphProceduresInvalidProjection(kind, gdsJavaClass(item)), nil)
			}
			addText(name)
		}
	}
	if len(specs) == 0 {
		add("*", "*")
	}
	return specs, nil
}

// read applies a projection entry's settings map.
func (spec *gdsElementSpec) read(entry map[string]interface{}, kind, nameKey string) error {
	allowed := []string{nameKey, "properties"}
	if kind == "relationship" {
		allowed = append(allowed, "orientation", "aggregation", "indexInverse")
	}
	config, err := newGDSConfig(entry, allowed...)
	if err != nil {
		return err
	}
	if element, ok, err := config.text(nameKey); err != nil {
		return err
	} else if ok {
		spec.element = element
	}
	if kind == "relationship" {
		if orientation, ok, err := config.text("orientation"); err != nil {
			return err
		} else if ok {
			spec.orientation = strings.ToUpper(orientation)
			switch spec.orientation {
			case "NATURAL", "REVERSE", "UNDIRECTED":
			default:
				return localizedError(localization.CypherGraphProceduresUnsupportedOrientation(orientation), nil)
			}
		}
		if aggregation, ok, err := config.text("aggregation"); err != nil {
			return err
		} else if ok {
			if spec.aggregation, err = gdsAggregation(aggregation); err != nil {
				return err
			}
		}
		if spec.indexInverse, _, err = config.boolean("indexInverse"); err != nil {
			return err
		}
	}
	properties, err := gdsPropertySpecs(config.value("properties"), spec.aggregation)
	if err != nil {
		return err
	}
	spec.properties = properties
	return nil
}

// gdsAggregation reads a relationship or property aggregation.
func gdsAggregation(text string) (string, error) {
	switch upper := strings.ToUpper(text); upper {
	case "DEFAULT", "NONE", "SINGLE", "SUM", "MIN", "MAX", "COUNT":
		return upper, nil
	}
	return "", localizedError(localization.CypherGraphProceduresUnsupportedAggregation(text), nil)
}

// gdsPropertySpecs reads a projection entry's properties: a name, a list of
// names, or a map from the projected key to the stored property name or to
// its settings (property, defaultValue, aggregation).
func gdsPropertySpecs(value interface{}, aggregation string) ([]gdsPropertySpec, error) {
	specs, err := readGDSPropertySpecs(value, aggregation)
	if err != nil {
		return nil, err
	}
	for _, spec := range specs {
		// '*' counts parallel relationships, so it takes COUNT only.
		if spec.property == "*" && spec.aggregation != "COUNT" {
			return nil, localizedError(localization.CypherGraphProceduresStarPropertyRequiresCount(), nil)
		}
	}
	return specs, nil
}

func readGDSPropertySpecs(value interface{}, aggregation string) ([]gdsPropertySpec, error) {
	var specs []gdsPropertySpec
	add := func(name string) {
		specs = append(specs, gdsPropertySpec{key: name, property: name, aggregation: aggregation})
	}
	switch properties := value.(type) {
	case nil:
	case string:
		add(properties)
	case map[string]interface{}:
		keys := make([]string, 0, len(properties))
		for key := range properties {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			spec := gdsPropertySpec{key: key, property: key, aggregation: aggregation}
			switch entry := properties[key].(type) {
			case nil:
			case string:
				spec.property = entry
			case map[string]interface{}:
				config, err := newGDSConfig(entry, "property", "defaultValue", "aggregation")
				if err != nil {
					return nil, err
				}
				if property, ok, err := config.text("property"); err != nil {
					return nil, err
				} else if ok {
					spec.property = property
				}
				if config.value("defaultValue") != nil {
					value, _, ok, err := gdsConvertPropertyValue(config.value("defaultValue"), "", false)
					if err != nil {
						return nil, err
					}
					if !ok {
						return nil, config.typeError("defaultValue", "Double")
					}
					spec.defaultValue = value
				}
				if text, ok, err := config.text("aggregation"); err != nil {
					return nil, err
				} else if ok {
					if spec.aggregation, err = gdsAggregation(text); err != nil {
						return nil, err
					}
				}
			default:
				return nil, localizedError(localization.CypherGraphProceduresInvalidProjection("property", gdsJavaClass(entry)), nil)
			}
			specs = append(specs, spec)
		}
	default:
		items, isList := cypherListValue(value)
		if !isList {
			return nil, localizedError(localization.CypherGraphProceduresInvalidProjection("property", gdsJavaClass(value)), nil)
		}
		for _, item := range items {
			name, isString := item.(string)
			if !isString {
				return nil, localizedError(localization.CypherGraphProceduresInvalidProjection("property", gdsJavaClass(item)), nil)
			}
			add(name)
		}
	}
	return specs, nil
}

// buildGraphProjection reads the projected nodes and relationships from
// storage, streaming both.
func (e *StorageExecutor) buildGraphProjection(name string, nodeSpecs, relSpecs []gdsElementSpec, validateRelationships bool) (*GraphProjection, error) {
	projection := newGraphProjection(name, e.databaseName())
	ctx := context.Background()

	labelsSeen := make(map[string]bool)
	nodePropertySeen := make(map[string]bool)
	nodePropertyType := make(map[string]string) // one builder per projected key, shared across labels
	err := storage.StreamNodesWithFallback(ctx, e.storage, streamChunkSize, func(node *storage.Node) error {
		for _, label := range node.Labels {
			labelsSeen[label] = true
		}
		var labels []string
		for _, spec := range nodeSpecs {
			if spec.element != "*" && !nodeHasLabel(node, spec.element) {
				continue
			}
			labels = appendUnique(labels, spec.name)
			for _, property := range spec.properties {
				exact, kind, ok, err := gdsConvertPropertyValue(node.Properties[property.property], nodePropertyType[property.key], true)
				if err != nil {
					return err
				}
				if ok {
					nodePropertySeen[property.property] = true
					nodePropertyType[property.key] = kind
				} else if property.defaultValue != nil {
					exact, kind, ok, _ = gdsConvertPropertyValue(property.defaultValue, "", false)
				}
				if !ok {
					continue
				}
				id := string(node.ID)
				projection.storeNodeProperty(id, property.key, exact)
			}
		}
		if labels != nil {
			projection.NodeIDs = append(projection.NodeIDs, string(node.ID))
			projection.NodeLabels[string(node.ID)] = labels
		}
		return nil
	})
	if err != nil {
		var localized *localization.LocalizedError
		if errors.As(err, &localized) {
			return nil, err
		}
		return nil, localizedError(localization.CypherGraphProceduresStreamNodesFailed(err), err)
	}
	var requested, missing []string
	for _, spec := range nodeSpecs {
		if spec.element != "*" {
			requested = append(requested, spec.element)
			if !labelsSeen[spec.element] {
				missing = append(missing, spec.element)
			}
		}
	}
	if len(missing) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresNodeLabelsNotFound(strings.Join(requested, ", ")), nil)
	}
	// Node IDs in order, so the same graph gives the same rows (#1183).
	projection.indexNodes()

	typesSeen := make(map[string]bool)
	relPropertySeen := make(map[string]bool)
	type parallelKey struct {
		spec           int
		source, target string
	}
	merged := make(map[parallelKey]int) // aggregated relationships -> their index
	counts := make(map[int]float64)     // aggregated relationships -> how many were merged
	// The relationships in id order, as GDS reads them: a parallel
	// relationship's SINGLE value, and anything else that keeps the first
	// one, doesn't depend on the order storage streams them in. (NornicDB's
	// ids don't follow creation order, so "first" is the smallest id.)
	// Only projected relationships are kept for that (and, to refuse them
	// with validateRelationships, those with an end outside the projection),
	// so memory follows the projection, not the store.
	var edges []*storage.Edge
	err = storage.StreamEdgesWithFallback(ctx, e.storage, streamChunkSize, func(edge *storage.Edge) error {
		typesSeen[edge.Type] = true
		if !gdsTypeProjected(relSpecs, edge.Type) {
			return nil
		}
		_, hasStart := projection.nodeIndex[string(edge.StartNode)]
		_, hasEnd := projection.nodeIndex[string(edge.EndNode)]
		if hasStart && hasEnd || validateRelationships {
			edges = append(edges, edge)
		}
		return nil
	})
	if err != nil {
		return nil, localizedError(localization.CypherGraphProceduresStreamEdgesFailed(err), err)
	}
	sort.Slice(edges, func(i, j int) bool { return edges[i].ID < edges[j].ID })
	for _, edge := range edges {
		typesSeen[edge.Type] = true
		start, end := string(edge.StartNode), string(edge.EndNode)
		_, hasStart := projection.nodeIndex[start]
		_, hasEnd := projection.nodeIndex[end]
		if !hasStart || !hasEnd {
			// Kept only with validateRelationships, which refuses a projected
			// relationship whose end isn't projected, as GDS does.
			side, id := "source", start
			if hasStart {
				side, id = "target", end
			}
			return nil, localizedError(localization.CypherGraphProceduresRelationshipEndNotProjected(side, id), nil)
		}
		for specIndex, spec := range relSpecs {
			if spec.element != "*" && edge.Type != spec.element {
				continue
			}
			properties := make(map[string]float64, len(spec.properties))
			for _, property := range spec.properties {
				if property.property == "*" {
					continue // counted, by its COUNT aggregation
				}
				if exact, _, ok, err := gdsConvertPropertyValue(edge.Properties[property.property], "Relationship", true); err != nil {
					return nil, err
				} else if ok {
					value := exact.(float64)
					relPropertySeen[property.property] = true
					properties[property.key] = value
				} else if property.defaultValue != nil {
					exact, _, _, err := gdsConvertPropertyValue(property.defaultValue, "Relationship", false)
					if err != nil {
						return nil, err
					}
					properties[property.key] = exact.(float64)
				}
			}
			directions := gdsRelationshipDirections(start, end, spec.orientation)
			for _, direction := range directions {
				relationship := ProjectedRelationship{ID: string(edge.ID), Source: direction[0], Target: direction[1], Type: spec.name, Properties: properties}
				if !spec.merges() {
					projection.Relationships = append(projection.Relationships, relationship)
					continue
				}
				// An aggregation merges parallel relationships into one.
				key := parallelKey{specIndex, direction[0], direction[1]}
				index, seen := merged[key]
				if !seen {
					merged[key] = len(projection.Relationships)
					counts[len(projection.Relationships)] = 1
					relationship.Properties = make(map[string]float64, len(properties))
					for _, property := range spec.properties {
						if value, ok := properties[property.key]; ok {
							relationship.Properties[property.key] = value
						}
						if property.aggregation == "COUNT" {
							relationship.Properties[property.key] = 1
						}
					}
					projection.Relationships = append(projection.Relationships, relationship)
					continue
				}
				counts[index]++
				mergeParallelProperties(projection.Relationships[index].Properties, properties, spec.properties, counts[index])
			}
		}
	}
	requested, missing = nil, nil
	for _, spec := range relSpecs {
		if spec.element != "*" {
			requested = append(requested, spec.element)
			if !typesSeen[spec.element] {
				missing = append(missing, spec.element)
			}
		}
	}
	if len(missing) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresRelationshipTypesNotFound(strings.Join(requested, "|")), nil)
	}
	missing = nil
	for _, spec := range nodeSpecs {
		for _, property := range spec.properties {
			if !nodePropertySeen[property.property] && property.defaultValue == nil {
				missing = appendUnique(missing, property.property)
			}
		}
	}
	if len(missing) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresNodePropertiesNotFound(strings.Join(missing, ", ")), nil)
	}
	missing = nil
	for _, spec := range relSpecs {
		for _, property := range spec.properties {
			if !relPropertySeen[property.property] && property.defaultValue == nil && property.aggregation != "COUNT" && property.property != "*" {
				missing = appendUnique(missing, property.property)
			}
		}
	}
	if len(missing) > 0 {
		return nil, localizedError(localization.CypherGraphProceduresRelationshipPropertiesNotFound(strings.Join(missing, ", ")), nil)
	}

	for _, spec := range nodeSpecs {
		projection.Labels = appendUnique(projection.Labels, spec.name)
		properties := make(map[string]interface{}, len(spec.properties))
		for _, property := range spec.properties {
			projection.NodePropertyKeys[spec.name] = appendUnique(projection.NodePropertyKeys[spec.name], property.key)
			properties[property.key] = map[string]interface{}{"property": property.property, "defaultValue": gdsDefaultValue(property)}
		}
		projection.NodeProjection[spec.name] = map[string]interface{}{"label": spec.element, "properties": properties}
	}
	for _, spec := range relSpecs {
		projection.Types = appendUnique(projection.Types, spec.name)
		properties := make(map[string]interface{}, len(spec.properties))
		for _, property := range spec.properties {
			projection.RelationshipPropertyKeys[spec.name] = appendUnique(projection.RelationshipPropertyKeys[spec.name], property.key)
			properties[property.key] = map[string]interface{}{"property": property.property, "aggregation": property.aggregation, "defaultValue": gdsDefaultValue(property)}
		}
		projection.RelationshipProjection[spec.name] = map[string]interface{}{
			"type": spec.element, "orientation": spec.orientation, "aggregation": spec.aggregation, "properties": properties, "indexInverse": spec.indexInverse,
		}
	}
	projection.finishCounts()
	// Stored scalar values can widen a builder. A default must fit its
	// stored type; without stored values the default supplies that type.
	// Finalize each key before converting its earlier values.
	for _, spec := range nodeSpecs {
		for _, property := range spec.properties {
			kind := nodePropertyType[property.key]
			if kind == "" {
				// Without a stored value, the configured default supplies
				// its type. In particular, 2 and 2.0 remain distinct.
				_, kind, _, _ = gdsConvertPropertyValue(property.defaultValue, "", false)
				if kind == "" {
					kind = "Integer"
				}
			}
			nodePropertyType[property.key] = kind
		}
	}
	projection.NodePropertyTypes = nodePropertyType
	schemaTypes := make(map[string]string)
	for _, spec := range nodeSpecs {
		for _, property := range spec.properties {
			kind := nodePropertyType[property.key]
			if property.defaultValue != nil {
				if _, _, _, err := gdsConvertPropertyValue(property.defaultValue, "Default "+kind, false); err != nil {
					return nil, err
				}
			}
			schemaTypes[gdsPropertyTypeKey(spec.name, property.key)] = kind
		}
	}
	for _, properties := range projection.NodeProperties {
		for key, value := range properties {
			exact, _, _, err := gdsConvertPropertyValue(value, nodePropertyType[key], false)
			if err != nil {
				return nil, err
			}
			properties[key] = exact
		}
	}
	projection.schema = gdsGraphSchema(nodeSpecs, relSpecs, schemaTypes)
	return projection, nil
}

// mergeParallelProperties folds a parallel relationship's properties into
// the kept relationship's, by each property's aggregation: COUNT counts the
// count relationships merged, SUM adds, MIN and MAX keep the extreme, SINGLE
// keeps the first value; a missing value changes nothing.
func mergeParallelProperties(kept, properties map[string]float64, specs []gdsPropertySpec, count float64) {
	for _, property := range specs {
		value, ok := properties[property.key]
		previous, had := kept[property.key]
		switch {
		case property.aggregation == "COUNT":
			kept[property.key] = count
		case !ok:
		case !had:
			kept[property.key] = value
		case property.aggregation == "SUM":
			kept[property.key] = previous + value
		case property.aggregation == "MIN" && value < previous, property.aggregation == "MAX" && value > previous:
			kept[property.key] = value
		}
	}
}

// gdsGraphSchema is the graph's schema as gds.graph.list reports it: each
// label's and type's properties with their type and default, and each
// type's direction. A node property is an Integer when every stored value
// is one, a list a List of Integer or of Float (schema types are keyed
// by label and property); a relationship property is a Float, as GDS stores
// them.
func gdsGraphSchema(nodeSpecs, relSpecs []gdsElementSpec, nodePropertyTypes map[string]string) map[string]interface{} {
	return gdsProjectionSchema(nodeSpecs, relSpecs, nodePropertyTypes, "PERSISTENT")
}

// gdsProjectionSchema is the single schema renderer for both import forms.
// Aggregation relationships are TRANSIENT; native ones are PERSISTENT.
func gdsProjectionSchema(nodeSpecs, relSpecs []gdsElementSpec, nodePropertyTypes map[string]string, relationshipPersistence string) map[string]interface{} {
	nodes := make(map[string]interface{})
	for _, spec := range nodeSpecs {
		properties, _ := nodes[spec.name].(map[string]interface{})
		if properties == nil {
			properties = make(map[string]interface{})
		}
		for _, property := range spec.properties {
			kind := nodePropertyTypes[gdsPropertyTypeKey(spec.name, property.key)]
			switch {
			case kind == "List of Integer" || kind == "List of Float":
				properties[property.key] = kind + " (DefaultValue(null), PERSISTENT)"
			case kind == "Float":
				properties[property.key] = "Float (DefaultValue(" + gdsDefaultText(property.defaultValue, "NaN") + "), PERSISTENT)"
			default:
				defaultText := "-9223372036854775808"
				if property.defaultValue != nil {
					value, _, _, _ := gdsConvertPropertyValue(property.defaultValue, "Integer", false)
					defaultText = strconv.FormatInt(value.(int64), 10)
				}
				properties[property.key] = "Integer (DefaultValue(" + defaultText + "), PERSISTENT)"
			}
		}
		nodes[spec.name] = properties
	}
	relationships := make(map[string]interface{})
	directions := make(map[string]interface{})
	for _, spec := range relSpecs {
		properties := make(map[string]interface{})
		for _, property := range spec.properties {
			aggregation := property.aggregation
			if aggregation == "DEFAULT" {
				aggregation = "NONE"
			}
			properties[property.key] = "Float (DefaultValue(" + gdsDefaultText(property.defaultValue, "NaN") + "), " + relationshipPersistence + ", Aggregation." + aggregation + ")"
		}
		relationships[spec.name] = properties
		direction := "DIRECTED"
		if spec.orientation == "UNDIRECTED" {
			direction = "UNDIRECTED"
		}
		directions[spec.name] = direction
	}
	return map[string]interface{}{"graphProperties": map[string]interface{}{}, "nodes": nodes, "relationships": relationships, "directions": directions}
}

// gdsDefaultText is a Float default as GDS prints it (1.0, 0.5), or none.
func gdsDefaultText(value interface{}, none string) string {
	exact, _, present, _ := gdsConvertPropertyValue(value, "Float", false)
	if !present {
		return none
	}
	number := exact.(float64)
	if number == math.Trunc(number) {
		return gdsNumberText(number, 'f', 1)
	}
	return gdsNumberText(number, 'g', -1)
}

// gdsNumberText is the shared formatter for floating-point GDS values and
// errors. Java spells infinite values Infinity and -Infinity.
func gdsNumberText(value float64, format byte, precision int) string {
	if math.IsInf(value, 1) {
		return "Infinity"
	}
	if math.IsInf(value, -1) {
		return "-Infinity"
	}
	return strconv.FormatFloat(value, format, precision, 64)
}

// javaDoubleText formats a Double in GDS configuration errors (2.0, 0.5).
func javaDoubleText(value float64) string {
	if value == math.Trunc(value) && math.Abs(value) < 1e15 {
		return gdsNumberText(value, 'f', 1)
	}
	return gdsNumberText(value, 'g', -1)
}

// gdsGraphInfo is a projection's row in gds.graph.list and gds.graph.drop:
// the columns GDS gives, without degreeDistribution. Its memory use isn't
// measured: memoryUsage is "" and sizeInBytes -1, as GDS reports an
// unmeasured graph.
func gdsGraphInfo(projection *GraphProjection) map[string]interface{} {
	density := 0.0
	if n := float64(projection.NodeCount); n > 1 {
		density = float64(projection.RelationshipCount) / (n * (n - 1))
	}
	schema := projection.schema
	if schema == nil {
		schema = map[string]interface{}{"graphProperties": map[string]interface{}{}, "nodes": map[string]interface{}{}, "relationships": map[string]interface{}{}, "directions": map[string]interface{}{}}
	}
	withOrientation := make(map[string]interface{})
	directions, _ := schema["directions"].(map[string]interface{})
	for name, properties := range schema["relationships"].(map[string]interface{}) {
		withOrientation[name] = map[string]interface{}{"properties": properties, "direction": directions[name]}
	}
	return map[string]interface{}{
		"graphName": projection.Name, "database": projection.Database, "databaseLocation": "local",
		"memoryUsage": "", "sizeInBytes": int64(-1),
		"nodeCount": int64(projection.NodeCount), "relationshipCount": int64(projection.RelationshipCount),
		"configuration": projection.Configuration, "density": density,
		"creationTime": projection.CreatedAt, "modificationTime": projection.CreatedAt,
		"schema":                map[string]interface{}{"graphProperties": schema["graphProperties"], "nodes": schema["nodes"], "relationships": schema["relationships"]},
		"schemaWithOrientation": map[string]interface{}{"graphProperties": schema["graphProperties"], "nodes": schema["nodes"], "relationships": withOrientation},
	}
}

// gdsDropColumns are gds.graph.drop's columns; gds.graph.list's add
// degreeDistribution first.
var gdsDropColumns = []string{
	"graphName", "database", "databaseLocation", "memoryUsage", "sizeInBytes", "nodeCount", "relationshipCount",
	"configuration", "density", "creationTime", "modificationTime", "schema", "schemaWithOrientation",
}

// gdsDegreeDistribution is the projection's out-degree distribution as GDS
// reports it: min, max, mean and percentiles.
func gdsDegreeDistribution(projection *GraphProjection) map[string]interface{} {
	degrees := make([]int64, len(projection.NodeIDs))
	// With no labels, types or property requested the shared selector cannot
	// fail. Count every projected direction, including parallel edges and loops.
	_, _ = selectGDSSubgraph(projection, nil, nil, "", gdsSubgraphOptions{
		omitAdjacency: true,
		visitRelationship: func(relationship *ProjectedRelationship) {
			if index, ok := projection.nodeIndex[relationship.Source]; ok {
				degrees[index]++
			}
		},
	})
	sort.Slice(degrees, func(i, j int) bool { return degrees[i] < degrees[j] })
	distribution := map[string]interface{}{"min": int64(0), "max": int64(0), "mean": 0.0}
	percentile := func(p float64) int64 {
		if len(degrees) == 0 {
			return 0
		}
		return degrees[int(math.Ceil(p*float64(len(degrees))))-1]
	}
	for name, p := range map[string]float64{"p50": 0.5, "p75": 0.75, "p90": 0.9, "p95": 0.95, "p99": 0.99, "p999": 0.999} {
		distribution[name] = percentile(p)
	}
	if len(degrees) > 0 {
		sum := int64(0)
		for _, degree := range degrees {
			sum += degree
		}
		distribution["min"], distribution["max"] = degrees[0], degrees[len(degrees)-1]
		distribution["mean"] = float64(sum) / float64(len(degrees))
	}
	return distribution
}

// gdsDefaultValue is a projected property's default value as GDS reports
// it: null when none was given.
func gdsDefaultValue(property gdsPropertySpec) interface{} {
	return property.defaultValue
}

func nodeHasLabel(node *storage.Node, label string) bool {
	for _, have := range node.Labels {
		if have == label {
			return true
		}
	}
	return false
}

func appendUnique(values []string, value string) []string {
	for _, have := range values {
		if have == value {
			return values
		}
	}
	return append(values, value)
}

// callGdsGraphList implements CALL gds.graph.list([graphName]) as GDS does
// (#1161): this database's projections, or the one named, with GDS's
// columns.
func (e *StorageExecutor) callGdsGraphList(args []interface{}) (*ExecuteResult, error) {
	name, err := optionalProcedureString("gds.graph.list", args, 0, "graphName", "")
	if err != nil {
		return nil, err
	}
	columns := append([]string{"degreeDistribution"}, gdsDropColumns...)
	rows := make([][]any, 0)
	for _, projection := range databaseProjections(e.databaseName()) {
		if name != "" && projection.Name != name {
			continue
		}
		info := gdsGraphInfo(projection)
		info["degreeDistribution"] = gdsDegreeDistribution(projection)
		row := make([]any, len(columns))
		for i, column := range columns {
			row[i] = info[column]
		}
		rows = append(rows, row)
	}
	return &ExecuteResult{Columns: columns, Rows: rows}, nil
}

// callGdsGraphDrop implements CALL gds.graph.drop(graphName, failIfMissing
// = true, dbName = ”, username = ”) as GDS does (#1161): a name or a list
// of names, of this database or dbName's. A missing graph fails unless
// failIfMissing is false. NornicDB's catalog isn't per user: username names
// no other catalog.
func (e *StorageExecutor) callGdsGraphDrop(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "gds.graph.drop"
	var names []string
	switch value := procedureArgument(args, 0).(type) {
	case nil:
		return nil, procedureArgumentNullError(procedure, "graphName")
	case string:
		names = []string{value}
	default:
		items, ok := cypherListValue(value)
		if !ok {
			return nil, procedureArgumentTypeError(procedure, "graphName", "STRING or LIST<STRING>", value)
		}
		for _, item := range items {
			name, ok := item.(string)
			if !ok {
				return nil, procedureArgumentTypeError(procedure, "graphName", "STRING or LIST<STRING>", value)
			}
			names = append(names, name)
		}
	}
	failIfMissing := true
	if value := procedureArgument(args, 1); value != nil {
		flag, ok := value.(bool)
		if !ok {
			return nil, procedureArgumentTypeError(procedure, "failIfMissing", "BOOLEAN", value)
		}
		failIfMissing = flag
	}
	database, err := optionalProcedureString(procedure, args, 2, "dbName", "")
	if err != nil {
		return nil, err
	}
	if database == "" {
		database = e.databaseName()
	}
	if _, err := optionalProcedureString(procedure, args, 3, "username", ""); err != nil {
		return nil, err
	}
	// Dropping from another database's catalog needs write there.
	if err := authorizeDatabase(ctx, database, "write"); err != nil {
		return nil, err
	}

	projectionsMu.Lock()
	defer projectionsMu.Unlock()
	if failIfMissing {
		for _, name := range names {
			if _, exists := graphProjections[gdsCatalogKey(database, name)]; !exists {
				return nil, localizedError(localization.CypherGraphProceduresGraphNotFound(name, database), nil)
			}
		}
	}
	rows := make([][]any, 0, len(names))
	for _, name := range names {
		key := gdsCatalogKey(database, name)
		projection, exists := graphProjections[key]
		if !exists {
			continue
		}
		delete(graphProjections, key)
		info := gdsGraphInfo(projection)
		row := make([]any, len(gdsDropColumns))
		for i, column := range gdsDropColumns {
			row[i] = info[column]
		}
		rows = append(rows, row)
	}
	return &ExecuteResult{Columns: gdsDropColumns, Rows: rows}, nil
}

// callGdsGraphExists implements CALL gds.graph.exists(graphName) (#1161).
func (e *StorageExecutor) callGdsGraphExists(args []interface{}) (*ExecuteResult, error) {
	name, err := requiredProcedureString("gds.graph.exists", args, 0, "graphName")
	if err != nil {
		return nil, err
	}
	_, missing := e.graphProjection(name)
	return &ExecuteResult{Columns: []string{"graphName", "exists"}, Rows: [][]any{{name, missing == nil}}}, nil
}

// gdsConfig is a GDS configuration map, read as GDS reads it: keys match
// whatever their case, and a key the procedure doesn't take, a value of the
// wrong type and a missing mandatory value fail with GDS's messages.
type gdsConfig struct {
	values map[string]interface{} // by lower-case key
}

// newGDSConfig reads entries, refusing a key not in allowed.
func newGDSConfig(entries map[string]interface{}, allowed ...string) (*gdsConfig, error) {
	return readGDSConfig(entries, false, allowed...)
}

// readGDSConfig owns key normalization and validation. The aggregation form
// alone ignores unknown configuration keys, as GDS does.
func readGDSConfig(entries map[string]interface{}, ignoreUnknown bool, allowed ...string) (*gdsConfig, error) {
	known := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		known[strings.ToLower(key)] = true
	}
	keys := make([]string, 0, len(entries))
	for key := range entries {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	config := &gdsConfig{values: make(map[string]interface{}, len(entries))}
	for _, key := range keys {
		lower := strings.ToLower(key)
		if !known[lower] {
			if ignoreUnknown {
				continue
			}
			return nil, localizedError(localization.CypherGraphProceduresConfigUnexpectedKey(key), nil)
		}
		config.values[lower] = entries[key]
	}
	return config, nil
}

// has reports whether the map has key, null or not.
func (c *gdsConfig) has(key string) bool {
	_, present := c.values[strings.ToLower(key)]
	return present
}

// value is key's value, nil when absent or null.
func (c *gdsConfig) value(key string) interface{} {
	return c.values[strings.ToLower(key)]
}

func (c *gdsConfig) typeError(key, expected string) error {
	return localizedError(localization.CypherGraphProceduresConfigValueType(key, expected, gdsJavaType(c.value(key))), nil)
}

// gdsConfigIntegerValue is the common integer configuration reader. Native
// GDS requires integer values; APOC retains integral-number coercion.
func gdsConfigIntegerValue(value interface{}, coerce bool) (int64, bool) {
	if !isIntegerProcedureValue(value) {
		if !coerce {
			return 0, false
		}
		number, ok := toFloat64(value)
		if !ok {
			return 0, false
		}
		value = number
	}
	exact, _, ok, err := gdsConvertPropertyValue(value, "Integer", false)
	if ok && err == nil {
		return exact.(int64), true
	}
	return 0, false
}

// gdsScalarNumber supplies numeric configuration values through the projected
// value converter. Configuration readers keep their own type-error messages.
func gdsScalarNumber(value interface{}) (float64, bool) {
	exact, _, ok, err := gdsConvertPropertyValue(value, "Float", false)
	if ok && err == nil {
		return exact.(float64), true
	}
	return 0, false
}

// integer is key's INTEGER value, at least min: fallback when absent, or an
// error when mandatory.
func (c *gdsConfig) integer(key, expected string, min, max, fallback int64, mandatory bool) (int64, error) {
	return c.integerWithErrors(key, min, max, fallback, mandatory,
		func() error { return c.typeError(key, expected) },
		func(number int64) error {
			return localizedError(localization.CypherGraphProceduresConfigValueOutOfRange(key, strconv.FormatInt(number, 10), fmt.Sprintf("[%d, %d]", min, max)), nil)
		})
}

// integerWithErrors owns integer and range validation. GDS's aggregation
// concurrency has its own messages and no native int32 upper bound.
func (c *gdsConfig) integerWithErrors(key string, min, max, fallback int64, mandatory bool, typeError func() error, rangeError func(int64) error) (int64, error) {
	value := c.value(key)
	if value == nil {
		if mandatory {
			return 0, localizedError(localization.CypherGraphProceduresConfigValueMissing(key), nil)
		}
		return fallback, nil
	}
	number, integer := gdsConfigIntegerValue(value, false)
	if !integer {
		return 0, typeError()
	}
	if number < min || number > max {
		return 0, rangeError(number)
	}
	return number, nil
}

// number is key's numeric value, fallback when absent.
func (c *gdsConfig) number(key, expected string, fallback float64) (float64, error) {
	value := c.value(key)
	if value == nil {
		return fallback, nil
	}
	number, ok := gdsScalarNumber(value)
	if !ok {
		return 0, c.typeError(key, expected)
	}
	return number, nil
}

// text is key's STRING value, and whether it was given.
func (c *gdsConfig) text(key string) (string, bool, error) {
	value := c.value(key)
	if value == nil {
		return "", false, nil
	}
	text, ok := value.(string)
	if !ok {
		return "", false, c.typeError(key, "String")
	}
	return text, true, nil
}

// boolean is key's BOOLEAN value, and whether it was given.
func (c *gdsConfig) boolean(key string) (bool, bool, error) {
	value := c.value(key)
	if value == nil {
		return false, false, nil
	}
	flag, ok := value.(bool)
	if !ok {
		return false, false, c.typeError(key, "Boolean")
	}
	return flag, true, nil
}

// jobID is the jobId value, a STRING; a generated one when absent.
func (c *gdsConfig) jobID() (string, error) {
	value := c.value("jobId")
	if value == nil {
		return uuid.New().String(), nil
	}
	text, ok := value.(string)
	if !ok {
		return "", localizedError(localization.CypherGraphProceduresJobIDType(gdsJavaType(value)), nil)
	}
	return text, nil
}

// names is key's label or type names: a STRING or a list of them; nil
// when absent or '*'.
func (c *gdsConfig) names(key string) ([]string, error) {
	return c.stringNames(key, true, true)
}

// stringNames is the one STRING/list-of-STRING reader. Its callers choose
// whether a scalar string is allowed and whether '*' means every name.
func (c *gdsConfig) stringNames(key string, allowString, wildcard bool) ([]string, error) {
	value := c.value(key)
	if value == nil {
		return nil, nil
	}
	if text, ok := value.(string); ok && allowString {
		value = []interface{}{text}
	}
	items, ok := cypherListValue(value)
	if !ok {
		return nil, c.typeError(key, "List")
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		name, ok := item.(string)
		if !ok {
			return nil, c.typeError(key, "List")
		}
		if wildcard && name == "*" {
			return nil, nil
		}
		names = append(names, name)
	}
	return names, nil
}

// gdsJavaType is the Java type name GDS's messages give value.
func gdsJavaType(value interface{}) string {
	switch value.(type) {
	case nil:
		return "null"
	case string:
		return "String"
	case bool:
		return "Boolean"
	case float64, float32:
		return "Double"
	case map[string]interface{}:
		return "HashMap"
	case *storage.Node:
		return "Node"
	}
	if isIntegerProcedureValue(value) {
		return "Long"
	}
	if _, ok := cypherListValue(value); ok {
		return "List"
	}
	return cypherTypeSystemName(value)
}

// gdsJavaClass is the Java class name GDS's projection messages give value.
func gdsJavaClass(value interface{}) string {
	switch name := gdsJavaType(value); name {
	case "String", "Boolean", "Double", "Long":
		return "java.lang." + name
	case "List":
		return "java.util.List"
	case "HashMap":
		return "java.util.Map"
	default:
		return name
	}
}

// gdsNameList is names as GDS's messages list them: ['a', 'b'].
func gdsNameList(names []string) string {
	quoted := make([]string, len(names))
	for i, name := range names {
		quoted[i] = "'" + name + "'"
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// gdsJavaValueText is a projected value as GDS's conversion messages print
// it: 1, 1.0, LongArray[1, 2], DoubleArray[1.0, 2.0].
func gdsJavaValueText(value interface{}) string {
	double := func(number float64) string {
		return gdsDefaultText(number, "")
	}
	switch typed := value.(type) {
	case int64:
		return strconv.FormatInt(typed, 10)
	case float64:
		return double(typed)
	case []int64:
		parts := make([]string, len(typed))
		for i, number := range typed {
			parts[i] = strconv.FormatInt(number, 10)
		}
		return "LongArray[" + strings.Join(parts, ", ") + "]"
	}
	floats, _ := value.([]float64)
	parts := make([]string, len(floats))
	for i, number := range floats {
		parts[i] = double(number)
	}
	return "DoubleArray[" + strings.Join(parts, ", ") + "]"
}

// gdsJavaTypeTarget is a GDS type as its conversion messages name it.
func gdsJavaTypeTarget(kind string) string {
	switch kind {
	case "Integer":
		return "a Long"
	case "Float":
		return "a Double"
	case "List of Integer":
		return "a Long Array"
	}
	return "a Double Array"
}

// gdsPropertyTypeKey keys a projected property's type by its label and key:
// schema entries are per label; conversion builders are per projected key.
func gdsPropertyTypeKey(label, key string) string {
	return label + "\x00" + key
}

// gdsNodeID is the common node representation reader for GDS procedures,
// filters and utility functions. Whether an ID is projected, stored or a label
// is the caller's selection contract; this reader preserves the ID exactly.
func gdsNodeID(value interface{}) (storage.NodeID, bool) {
	switch node := value.(type) {
	case *storage.Node:
		if node != nil {
			return node.ID, true
		}
	case string:
		return storage.NodeID(node), true
	default:
		if isIntegerProcedureValue(value) {
			return storage.NodeID(fmt.Sprint(value)), true
		}
	}
	return "", false
}

// gdsInteger is a value of any Go integer type as an int64 (the shared
// converters skip the narrow and unsigned ones).
func gdsInteger(value interface{}) int64 {
	switch number := value.(type) {
	case int8:
		return int64(number)
	case int16:
		return int64(number)
	case int32:
		return int64(number)
	case uint:
		return int64(number)
	case uint8:
		return int64(number)
	case uint16:
		return int64(number)
	case uint32:
		return int64(number)
	case uint64:
		return int64(number)
	}
	return toInt64(value)
}

// gdsTypeProjected reports whether a relationship projection takes the type.
func gdsTypeProjected(relSpecs []gdsElementSpec, relType string) bool {
	for _, spec := range relSpecs {
		if spec.element == "*" || spec.element == relType {
			return true
		}
	}
	return false
}

func gdsDefaultNumber(value interface{}) float64 { number, _ := toFloat64(value); return number }
