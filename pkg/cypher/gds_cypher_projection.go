package cypher

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// gdsProjectionRow is one row of gds.graph.project's Cypher aggregation
// projection: its evaluated arguments.
type gdsProjectionRow struct {
	graphName, source, target, data, config interface{}
	arguments                               int
}

// statementTextKey carries the user statement's text, which GDS's Cypher
// aggregation projection reports as its query. Only a statement that names
// gds.graph.project carries it, so other statements allocate nothing.
type statementTextKey struct{}

func withStatementText(ctx context.Context, statement string) context.Context {
	if indexASCIIFold(statement, "gds.graph.project") < 0 {
		return ctx
	}
	return context.WithValue(ctx, statementTextKey{}, statement)
}

func statementTextFromContext(ctx context.Context) string {
	text, _ := ctx.Value(statementTextKey{}).(string)
	return text
}

// addGDSProjectionRow evaluates one row's gds.graph.project(graphName,
// sourceNode, targetNode = null, dataConfig = null, configuration = null)
// arguments into the aggregate's state (#1194).
func (e *StorageExecutor) addGDSProjectionRow(ctx context.Context, state *pipelineAggregateState, scope *rowEvaluationScope) bool {
	arguments := state.argumentTexts(e)
	if len(arguments) < 2 || len(arguments) > 6 {
		recordExpressionFailure(ctx, &cypherfn.ParameterCountError{Function: gdsProjectAggregate, TooMany: len(arguments) > 6})
		return false
	}
	if len(arguments) == 6 {
		recordExpressionFailure(ctx, localizedError(localization.CypherGraphProceduresConfigParametersMerged(), nil))
		return false
	}
	var values [5]interface{}
	for i, argument := range arguments {
		value, ok := scope.evaluate(ctx, e, argument)
		if !ok {
			return false
		}
		values[i] = value
	}
	if state.gdsRows == nil {
		state.gdsStarted = time.Now()
	}
	state.gdsRows = append(state.gdsRows, gdsProjectionRow{
		graphName: values[0], source: values[1], target: values[2], data: values[3], config: values[4], arguments: len(arguments),
	})
	return true
}

// gdsCypherProjection is GDS's Cypher aggregation projection (#1194): the
// rows' source and target nodes, a relationship between each pair (its
// type and numeric properties from the row's dataConfig), a source with a
// null target alone, stored as the graph named by the first row. It
// returns GDS's result map; no rows give null.
func (e *StorageExecutor) gdsCypherProjection(ctx context.Context, rows []gdsProjectionRow, started time.Time) (interface{}, error) {
	if len(rows) == 0 {
		return nil, nil
	}
	name, ok := rows[0].graphName.(string)
	if !ok {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueType("graphName", "String", gdsJavaType(rows[0].graphName)), nil)
	}
	configuration, undirected, inverse, err := gdsAggregationConfiguration(rows[0].config)
	if err != nil {
		return nil, err
	}
	projection := newGraphProjection(name, e.databaseName())
	seen := make(map[string]bool)
	relationshipKeys := make(map[string][]string) // each type's property keys, its first relationship's
	addNode := func(value, labels, properties interface{}, labelsKey, variable string) (string, error) {
		id, integer, err := gdsProjectionNode(value, projection)
		if err != nil {
			return "", err
		}
		nodeLabels, err := gdsAggregationLabels(labels, labelsKey, variable)
		if err != nil {
			return "", err
		}
		// A node's labels and properties are its first occurrence's, as in
		// GDS: a later row naming it adds only its relationship.
		if seen[id] {
			return id, nil
		}
		seen[id] = true
		projection.NodeIDs = append(projection.NodeIDs, id)
		if integer {
			if projection.integerNodeIDs == nil {
				projection.integerNodeIDs = make(map[string]bool)
			}
			projection.integerNodeIDs[id] = true
		}
		if len(nodeLabels) == 0 {
			nodeLabels = []string{"__ALL__"}
		}
		for _, label := range nodeLabels {
			projection.NodeLabels[id] = appendUnique(projection.NodeLabels[id], label)
		}
		if properties == nil {
			return id, nil
		}
		values, ok := properties.(map[string]interface{})
		if !ok {
			return "", localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(properties)), nil)
		}
		keys := make([]string, 0, len(values))
		for key := range values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			for _, label := range projection.NodeLabels[id] {
				projection.NodePropertyKeys[label] = appendUnique(projection.NodePropertyKeys[label], key)
			}
			// A null value is no value, as in GDS. A number or a list of
			// numbers projects; the property's type is its first value's.
			value := values[key]
			if value == nil {
				continue
			}
			exact, kind, ok, err := gdsConvertPropertyValue(value, projection.NodePropertyTypes[key], false)
			if err != nil {
				return "", err
			}
			if !ok {
				return "", localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(value)), nil)
			}
			projection.NodePropertyTypes[key] = kind
			projection.storeNodeProperty(id, key, exact)
		}
		return id, nil
	}

	for index, row := range rows {
		var data *gdsConfig
		if row.data != nil {
			entries, ok := row.data.(map[string]interface{})
			if !ok {
				return nil, localizedError(localization.CypherGraphProceduresConfigValueType("dataConfig", "Map", gdsJavaType(row.data)), nil)
			}
			if data, err = newGDSConfig(entries, "sourceNodeLabels", "targetNodeLabels", "sourceNodeProperties", "targetNodeProperties", "relationshipType", "relationshipProperties"); err != nil {
				return nil, err
			}
			for _, pair := range [][2]string{{"sourceNodeLabels", "targetNodeLabels"}, {"sourceNodeProperties", "targetNodeProperties"}} {
				for _, order := range [][2]string{{pair[0], pair[1]}, {pair[1], pair[0]}} {
					if data.has(order[0]) && !data.has(order[1]) {
						return nil, localizedError(localization.CypherGraphProceduresConfigKeyMissingPair(order[1], order[0]), nil)
					}
				}
			}
		} else {
			data, _ = newGDSConfig(nil)
		}
		source, err := addNode(row.source, data.value("sourceNodeLabels"), data.value("sourceNodeProperties"), "sourceNodeLabels", "s")
		if err != nil {
			return nil, err
		}
		if row.target == nil {
			continue
		}
		target, err := addNode(row.target, data.value("targetNodeLabels"), data.value("targetNodeProperties"), "targetNodeLabels", "t")
		if err != nil {
			return nil, err
		}
		relType := "__ALL__"
		if value := data.value("relationshipType"); value != nil {
			text, ok := value.(string)
			if !ok {
				return nil, localizedError(localization.CypherGraphProceduresRelationshipTypeNotString(gdsNeo4jValueType(value)), nil)
			}
			if text == "" {
				return nil, localizedError(localization.CypherGraphProceduresRelationshipTypeEmpty(), nil)
			}
			relType = text
		}
		// A type's property keys are its first relationship's, as in GDS:
		// a later relationship's other keys are dropped, a missing or null
		// value is no value.
		keys, typed := relationshipKeys[relType]
		properties := make(map[string]float64)
		if value := data.value("relationshipProperties"); value != nil {
			entries, ok := value.(map[string]interface{})
			if !ok {
				return nil, localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(value)), nil)
			}
			for key, entry := range entries {
				if !typed {
					keys = append(keys, key)
				} else if !containsString(keys, key) {
					continue
				}
				if entry == nil {
					continue
				}
				exact, _, ok, err := gdsConvertPropertyValue(entry, "Relationship", false)
				if err != nil {
					return nil, err
				}
				if !ok {
					return nil, localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(entry)), nil)
				}
				properties[key] = exact.(float64)
			}
		}
		if !typed {
			sort.Strings(keys)
			relationshipKeys[relType] = keys
			projection.RelationshipPropertyKeys[relType] = append([]string(nil), keys...)
		}
		projection.Types = appendUnique(projection.Types, relType)
		id := "row-" + strconv.Itoa(index)
		projection.Relationships = append(projection.Relationships, ProjectedRelationship{ID: id, Source: source, Target: target, Type: relType, Properties: properties})
	}

	// Undirected types: each relationship of them both ways, as GDS holds
	// them.
	if _, err := projection.resolveRelationshipTypes(inverse, false, "inverse"); err != nil {
		return nil, err
	}
	undirected, err = projection.resolveRelationshipTypes(undirected, false, "undirected")
	if err != nil {
		return nil, err
	}
	if undirected != nil {
		forward := len(projection.Relationships)
		for _, relationship := range projection.Relationships[:forward] {
			if containsString(undirected, relationship.Type) {
				for _, direction := range gdsRelationshipDirections(relationship.Source, relationship.Target, "UNDIRECTED")[1:] {
					reverse := relationship
					reverse.Source, reverse.Target = direction[0], direction[1]
					projection.Relationships = append(projection.Relationships, reverse)
				}
			}
		}
	}

	for _, keys := range projection.NodePropertyKeys {
		for _, key := range keys {
			if projection.NodePropertyTypes[key] == "" {
				return nil, localizedError(localization.CypherGraphProceduresPropertyTypeUnknown(), nil)
			}
		}
	}
	projection.indexNodes(false)
	for _, id := range projection.NodeIDs {
		for _, label := range projection.NodeLabels[id] {
			projection.Labels = appendUnique(projection.Labels, label)
		}
	}
	projection.finishCounts()
	projection.schema = gdsAggregationSchema(projection, undirected)

	query := statementTextFromContext(ctx)
	configuration["query"] = query
	projection.Configuration = configuration
	if err := registerGraphProjection(projection, true); err != nil {
		return nil, err
	}

	return map[string]interface{}{
		"graphName": name, "nodeCount": int64(projection.NodeCount), "relationshipCount": int64(projection.RelationshipCount),
		"projectMillis": time.Since(started).Milliseconds(), "configuration": configuration, "query": query,
	}, nil
}

// gdsProjectionNode reads a projected node: a node, or a non-negative INTEGER
// id, which needn't be a stored node's, as in GDS. integer reports an INTEGER
// id, which the algorithms' rows return as an INTEGER.
func gdsProjectionNode(value interface{}, scopes ...*GraphProjection) (id string, integer bool, err error) {
	// This import accepts nodes and numeric IDs; string IDs remain a utility extension.
	if isIntegerProcedureValue(value) {
		number := gdsInteger(value)
		if number < 0 {
			return "", false, localizedError(localization.CypherGraphProceduresNodeIDNegative(strconv.FormatInt(number, 10)), nil)
		}
		return strconv.FormatInt(number, 10), true, nil
	}
	if node, ok := value.(*storage.Node); ok && node != nil {
		if len(scopes) > 0 && scopes[0] != nil {
 id, _ := scopes[0].nodeReference(node)
 return id, false, nil
 }
 id, _ := graphEntityDisplayID(node)
 return string(id), false, nil
	}
	typeName := gdsJavaType(value)
	if value == nil {
		typeName = "NO_VALUE"
	}
	return "", false, localizedError(localization.CypherGraphProceduresNodeNotNodeOrInteger(typeName), nil)
}

// gdsAggregationLabels reads a dataConfig's node labels: a STRING, a list
// of them, or false or null for none. true, GDS's deprecated form, is
// refused with its message.
func gdsAggregationLabels(value interface{}, key, variable string) ([]string, error) {
	switch labels := value.(type) {
	case nil:
		return nil, nil
	case bool:
		if labels {
			return nil, localizedError(localization.CypherGraphProceduresLabelsTrueDeprecated(key, variable), nil)
		}
		return nil, nil
	}
	c := &gdsConfig{values: map[string]interface{}{strings.ToLower(key): value}}
	return c.stringNames(key, true, false)
}

// gdsAggregationConfiguration reads the aggregation's configuration map
// (its 5th argument): undirectedRelationshipTypes, inverseIndexedRelationshipTypes,
// readConcurrency, jobId and logProgress, other keys ignored as GDS ignores
// them. It returns the configuration as GDS reports it, the undirected
// types (nil for none) and the inverse-indexed types.
func gdsAggregationConfiguration(value interface{}) (map[string]interface{}, []string, []string, error) {
	configuration := map[string]interface{}{
		"inverseIndexedRelationshipTypes": []interface{}{}, "undirectedRelationshipTypes": []interface{}{},
		"readConcurrency": int64(4), "logProgress": true,
	}
	entries := map[string]interface{}{}
	if value != nil {
		var ok bool
		if entries, ok = value.(map[string]interface{}); !ok {
			return nil, nil, nil, localizedError(localization.CypherGraphProceduresConfigValueType("configuration", "Map", gdsJavaType(value)), nil)
		}
	}
	c, _ := readGDSConfig(entries, true, "undirectedRelationshipTypes", "inverseIndexedRelationshipTypes", "readConcurrency", "jobId", "logProgress")
	var undirected, inverse []string
	for _, key := range []string{"undirectedRelationshipTypes", "inverseIndexedRelationshipTypes"} {
		names, err := c.stringNames(key, false, false)
		if err != nil {
			return nil, nil, nil, err
		}
		if names == nil {
			continue
		}
		configuration[key] = namesOrAll(names)
		// Empty aggregation lists stay empty; unlike algorithm selection,
		// an omitted orientation list does not mean every type.
		if len(names) == 0 {
			configuration[key] = []interface{}{}
		}
		if key == "undirectedRelationshipTypes" {
			undirected = names
		} else {
			inverse = names
		}
	}
	concurrency, err := c.integerWithErrors("readConcurrency", 1, 1<<63-1, 4, false,
		func() error {
			return localizedError(localization.CypherGraphProceduresUnsupportedConcurrency(gdsJavaType(c.value("readConcurrency"))), nil)
		},
		func(number int64) error {
			return localizedError(localization.CypherGraphProceduresConcurrencyBelowOne(strconv.FormatInt(number, 10)), nil)
		})
	if err != nil {
		return nil, nil, nil, err
	}
	configuration["readConcurrency"] = concurrency
	jobID, err := c.jobID()
	if err != nil {
		return nil, nil, nil, err
	}
	configuration["jobId"] = jobID
	if logProgress, given, err := c.boolean("logProgress"); err != nil {
		return nil, nil, nil, err
	} else if given {
		configuration["logProgress"] = logProgress
	}
	return configuration, undirected, inverse, nil
}

// gdsAggregationSchema is an aggregation projection's schema as GDS reports
// it: node properties PERSISTENT, relationship properties TRANSIENT, and no
// relationship types when there are no relationships.
func gdsAggregationSchema(projection *GraphProjection, undirected []string) map[string]interface{} {
	var nodes, relationships []gdsElementSpec
	schemaTypes := make(map[string]string)
	for _, label := range projection.Labels {
		spec := gdsElementSpec{name: label}
		for _, key := range projection.NodePropertyKeys[label] {
			spec.properties = append(spec.properties, gdsPropertySpec{key: key})
			schemaTypes[gdsPropertyTypeKey(label, key)] = projection.NodePropertyTypes[key]
		}
		nodes = append(nodes, spec)
	}
	for _, relType := range projection.Types {
		spec := gdsElementSpec{name: relType, orientation: "NATURAL"}
		if containsString(undirected, relType) {
			spec.orientation = "UNDIRECTED"
		}
		for _, key := range projection.RelationshipPropertyKeys[relType] {
			spec.properties = append(spec.properties, gdsPropertySpec{key: key, aggregation: "NONE"})
		}
		relationships = append(relationships, spec)
	}
	return gdsProjectionSchema(nodes, relationships, schemaTypes, "TRANSIENT")
}

// gdsNeo4jValueType is the Neo4j value type GDS's relationshipType message
// names.
func gdsNeo4jValueType(value interface{}) string {
	switch gdsJavaType(value) {
	case "Long", "Double":
		return "NUMBER"
	case "Boolean":
		return "BOOLEAN"
	case "List":
		return "LIST"
	case "Map", "HashMap":
		return "MAP"
	case "Node":
		return "NODE"
	}
	return strings.ToUpper(gdsJavaType(value))
}
