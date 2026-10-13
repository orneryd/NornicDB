package cypher

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// GraphProjection is a gds.graph.project graph (#1190): its nodes, in node
// ID order, and its relationships, oriented as projected. An UNDIRECTED
// relationship is held in both directions, as GDS holds and counts it.
type GraphProjection struct {
	Name                     string
	NodeIDs                  []string
	NodeLabels               map[string][]string           // node -> the projected labels it carries
	Labels                   []string                      // the projected labels ("__ALL__" for '*')
	NodeProperties           map[string]map[string]float64 // node -> projected property -> value
	NodePropertyKeys         map[string][]string           // projected label -> its property keys
	Relationships            []ProjectedRelationship
	Types                    []string            // the projected relationship types ("__ALL__" for '*')
	RelationshipPropertyKeys map[string][]string // projected type -> its property keys
	NodeProjection           map[string]interface{}
	RelationshipProjection   map[string]interface{}
	NodeCount                int
	RelationshipCount        int
	CreatedAt                time.Time
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

// Global graph projection store (thread-safe)
var (
	graphProjections = make(map[string]*GraphProjection)
	projectionsMu    sync.RWMutex
)

// gdsPropertySpec is one projected property: its key in the graph, the
// stored property it reads, its default value and, for a relationship
// property, its aggregation.
type gdsPropertySpec struct {
	key, property string
	defaultValue  *float64
	aggregation   string
}

// gdsElementSpec is one node or relationship projection entry.
type gdsElementSpec struct {
	name        string // the projected label or type
	element     string // the stored label or type; "*" for every one
	orientation string // relationships: NATURAL, REVERSE or UNDIRECTED
	aggregation string // relationships: DEFAULT, NONE, SINGLE, SUM, MIN, MAX or COUNT
	properties  []gdsPropertySpec
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
	projectionsMu.RLock()
	_, exists := graphProjections[graphName]
	projectionsMu.RUnlock()
	if exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphAlreadyExists(graphName), nil)
	}

	projection, err := e.buildGraphProjection(graphName, nodeSpecs, relSpecs)
	if err != nil {
		return nil, err
	}

	projectionsMu.Lock()
	if _, exists := graphProjections[graphName]; exists {
		projectionsMu.Unlock()
		return nil, localizedError(localization.CypherGraphProceduresGraphAlreadyExists(graphName), nil)
	}
	graphProjections[graphName] = projection
	projectionsMu.Unlock()

	return &ExecuteResult{
		Columns: []string{"nodeProjection", "relationshipProjection", "graphName", "nodeCount", "relationshipCount", "projectMillis"},
		Rows: [][]any{{
			projection.NodeProjection, projection.RelationshipProjection, graphName,
			int64(projection.NodeCount), int64(projection.RelationshipCount), time.Since(started).Milliseconds(),
		}},
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
		if _, _, err := config.boolean("indexInverse"); err != nil {
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
					number, err := config.number("defaultValue", "Double", 0)
					if err != nil {
						return nil, err
					}
					spec.defaultValue = &number
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
func (e *StorageExecutor) buildGraphProjection(name string, nodeSpecs, relSpecs []gdsElementSpec) (*GraphProjection, error) {
	projection := &GraphProjection{
		Name:                     name,
		NodeLabels:               make(map[string][]string),
		NodeProperties:           make(map[string]map[string]float64),
		NodePropertyKeys:         make(map[string][]string),
		RelationshipPropertyKeys: make(map[string][]string),
		NodeProjection:           make(map[string]interface{}),
		RelationshipProjection:   make(map[string]interface{}),
		CreatedAt:                time.Now(),
	}
	ctx := context.Background()

	labelsSeen := make(map[string]bool)
	nodePropertySeen := make(map[string]bool)
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
				value, ok := toFloat64(node.Properties[property.property])
				if ok {
					nodePropertySeen[property.property] = true
				} else if property.defaultValue != nil {
					value, ok = *property.defaultValue, true
				}
				if ok {
					if projection.NodeProperties[string(node.ID)] == nil {
						projection.NodeProperties[string(node.ID)] = make(map[string]float64)
					}
					projection.NodeProperties[string(node.ID)][property.key] = value
				}
			}
		}
		if labels != nil {
			projection.NodeIDs = append(projection.NodeIDs, string(node.ID))
			projection.NodeLabels[string(node.ID)] = labels
		}
		return nil
	})
	if err != nil {
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
	sort.Strings(projection.NodeIDs)
	projection.nodeIndex = make(map[string]int, len(projection.NodeIDs))
	for i, id := range projection.NodeIDs {
		projection.nodeIndex[id] = i
	}

	typesSeen := make(map[string]bool)
	relPropertySeen := make(map[string]bool)
	type parallelKey struct {
		spec           int
		source, target string
	}
	merged := make(map[parallelKey]int) // aggregated relationships -> their index
	counts := make(map[int]float64)     // aggregated relationships -> how many were merged
	err = storage.StreamEdgesWithFallback(ctx, e.storage, streamChunkSize, func(edge *storage.Edge) error {
		typesSeen[edge.Type] = true
		start, end := string(edge.StartNode), string(edge.EndNode)
		if _, ok := projection.nodeIndex[start]; !ok {
			return nil
		}
		if _, ok := projection.nodeIndex[end]; !ok {
			return nil
		}
		for specIndex, spec := range relSpecs {
			if spec.element != "*" && edge.Type != spec.element {
				continue
			}
			properties := make(map[string]float64, len(spec.properties))
			for _, property := range spec.properties {
				if value, ok := toFloat64(edge.Properties[property.property]); ok {
					relPropertySeen[property.property] = true
					properties[property.key] = value
				} else if property.defaultValue != nil {
					properties[property.key] = *property.defaultValue
				}
			}
			directions := [][2]string{{start, end}}
			switch spec.orientation {
			case "REVERSE":
				directions = [][2]string{{end, start}}
			case "UNDIRECTED":
				directions = [][2]string{{start, end}, {end, start}}
			}
			for _, direction := range directions {
				relationship := ProjectedRelationship{ID: string(edge.ID), Source: direction[0], Target: direction[1], Type: spec.name, Properties: properties}
				if spec.aggregation == "DEFAULT" || spec.aggregation == "NONE" {
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
				kept := projection.Relationships[index].Properties
				for _, property := range spec.properties {
					value, ok := properties[property.key]
					previous, had := kept[property.key]
					switch {
					case property.aggregation == "COUNT":
						kept[property.key] = counts[index]
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
		}
		return nil
	})
	if err != nil {
		return nil, localizedError(localization.CypherGraphProceduresStreamEdgesFailed(err), err)
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
			if !relPropertySeen[property.property] && property.defaultValue == nil && property.aggregation != "COUNT" {
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
			"type": spec.element, "orientation": spec.orientation, "aggregation": spec.aggregation, "properties": properties, "indexInverse": false,
		}
	}
	projection.NodeCount = len(projection.NodeIDs)
	projection.RelationshipCount = len(projection.Relationships)
	return projection, nil
}

// gdsDefaultValue is a projected property's default value as GDS reports
// it: null when none was given.
func gdsDefaultValue(property gdsPropertySpec) interface{} {
	if property.defaultValue == nil {
		return nil
	}
	return *property.defaultValue
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

// callGdsGraphList implements CALL gds.graph.list()
func (e *StorageExecutor) callGdsGraphList() (*ExecuteResult, error) {
	projectionsMu.RLock()
	defer projectionsMu.RUnlock()

	names := make([]string, 0, len(graphProjections))
	for name := range graphProjections {
		names = append(names, name)
	}
	sort.Strings(names)
	rows := make([][]any, 0, len(names))
	for _, name := range names {
		proj := graphProjections[name]
		rows = append(rows, []any{name, int64(proj.NodeCount), int64(proj.RelationshipCount), proj.CreatedAt.Format(time.RFC3339)})
	}

	return &ExecuteResult{
		Columns: []string{"graphName", "nodeCount", "relationshipCount", "createdAt"},
		Rows:    rows,
	}, nil
}

// callGdsGraphDrop implements CALL gds.graph.drop(graphName)
func (e *StorageExecutor) callGdsGraphDrop(args []interface{}) (*ExecuteResult, error) {
	graphName, err := requiredProcedureString("gds.graph.drop", args, 0, "graphName")
	if err != nil {
		return nil, err
	}

	projectionsMu.Lock()
	proj, exists := graphProjections[graphName]
	if exists {
		delete(graphProjections, graphName)
	}
	projectionsMu.Unlock()

	if !exists {
		return nil, localizedError(localization.CypherGraphProceduresGraphDoesNotExist(graphName), nil)
	}

	return &ExecuteResult{
		Columns: []string{"graphName", "nodeCount", "relationshipCount"},
		Rows: [][]any{
			{graphName, int64(proj.NodeCount), int64(proj.RelationshipCount)},
		},
	}, nil
}

// gdsConfig is a GDS configuration map, read as GDS reads it: keys match
// whatever their case, and a key the procedure doesn't take, a value of the
// wrong type and a missing mandatory value fail with GDS's messages.
type gdsConfig struct {
	values map[string]interface{} // by lower-case key
}

// newGDSConfig reads entries, refusing a key not in allowed.
func newGDSConfig(entries map[string]interface{}, allowed ...string) (*gdsConfig, error) {
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
			return nil, localizedError(localization.CypherGraphProceduresConfigUnexpectedKey(key), nil)
		}
		config.values[lower] = entries[key]
	}
	return config, nil
}

// value is key's value, nil when absent or null.
func (c *gdsConfig) value(key string) interface{} {
	return c.values[strings.ToLower(key)]
}

func (c *gdsConfig) typeError(key, expected string) error {
	return localizedError(localization.CypherGraphProceduresConfigValueType(key, expected, gdsJavaType(c.value(key))), nil)
}

// integer is key's INTEGER value, at least min: fallback when absent, or an
// error when mandatory.
func (c *gdsConfig) integer(key, expected string, min, max, fallback int64, mandatory bool) (int64, error) {
	value := c.value(key)
	if value == nil {
		if mandatory {
			return 0, localizedError(localization.CypherGraphProceduresConfigValueMissing(key), nil)
		}
		return fallback, nil
	}
	if !isIntegerProcedureValue(value) {
		return 0, c.typeError(key, expected)
	}
	number := toInt64(value)
	if number < min || number > max {
		return 0, localizedError(localization.CypherGraphProceduresConfigValueOutOfRange(key, strconv.FormatInt(number, 10), fmt.Sprintf("[%d, %d]", min, max)), nil)
	}
	return number, nil
}

// number is key's numeric value, fallback when absent.
func (c *gdsConfig) number(key, expected string, fallback float64) (float64, error) {
	value := c.value(key)
	if value == nil {
		return fallback, nil
	}
	number, ok := toFloat64(value)
	if _, isText := value.(string); !ok || isText {
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

// names is key's label or type names: a STRING or a list of them; nil
// when absent or '*'.
func (c *gdsConfig) names(key string) ([]string, error) {
	value := c.value(key)
	if value == nil {
		return nil, nil
	}
	if text, ok := value.(string); ok {
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
		if name == "*" {
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
		return "Map"
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
	case "Map":
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
