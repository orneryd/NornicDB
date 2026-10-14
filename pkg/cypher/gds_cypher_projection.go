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
// aggregation projection reports as its query.
type statementTextKey struct{}

func withStatementText(ctx context.Context, statement string) context.Context {
	return context.WithValue(ctx, statementTextKey{}, statement)
}

func statementTextFromContext(ctx context.Context) string {
	text, _ := ctx.Value(statementTextKey{}).(string)
	return text
}

// addGDSProjectionRow evaluates one row's gds.graph.project(graphName,
// sourceNode, targetNode = null, dataConfig = null, configuration = null)
// arguments into the aggregate's state (#1194).
func (e *StorageExecutor) addGDSProjectionRow(ctx context.Context, state *pipelineAggregateState, row pipelineRow) bool {
	arguments := e.splitFunctionArgs(state.expression)
	if len(arguments) < 2 || len(arguments) > 6 {
		recordExpressionFailure(ctx, &cypherfn.ParameterCountError{Function: gdsProjectAggregate, TooMany: len(arguments) > 6})
		return false
	}
	if len(arguments) == 6 {
		recordExpressionFailure(ctx, localizedError(localization.CypherGraphProceduresConfigParametersMerged(), nil))
		return false
	}
	values := make([]interface{}, 5)
	for i, argument := range arguments {
		value, ok := e.evaluateRowExpressionWithContext(ctx, strings.TrimSpace(argument), row)
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
	configuration, undirected, err := gdsAggregationConfiguration(rows[0].config)
	if err != nil {
		return nil, err
	}
	projection := &GraphProjection{
		Name:                     name,
		NodeLabels:               make(map[string][]string),
		NodeProperties:           make(map[string]map[string]float64),
		NodePropertyKeys:         make(map[string][]string),
		RelationshipPropertyKeys: make(map[string][]string),
		CreatedAt:                time.Now(),
		Database:                 e.databaseName(),
	}
	nodePropertyFloat := make(map[string]bool)
	seen := make(map[string]bool)
	addNode := func(value, labels, properties interface{}, labelsKey, variable string) (string, error) {
		id, err := gdsProjectionNode(value)
		if err != nil {
			return "", err
		}
		nodeLabels, err := gdsAggregationLabels(labels, labelsKey, variable)
		if err != nil {
			return "", err
		}
		if !seen[id] {
			seen[id] = true
			projection.NodeIDs = append(projection.NodeIDs, id)
		}
		if len(nodeLabels) == 0 && len(projection.NodeLabels[id]) == 0 {
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
		for key, value := range values {
			number, ok := toFloat64(value)
			if _, isText := value.(string); !ok || isText {
				return "", localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(value)), nil)
			}
			if !isIntegerProcedureValue(value) {
				nodePropertyFloat[key] = true
			}
			if projection.NodeProperties[id] == nil {
				projection.NodeProperties[id] = make(map[string]float64)
			}
			projection.NodeProperties[id][key] = number
			for _, label := range projection.NodeLabels[id] {
				projection.NodePropertyKeys[label] = appendUnique(projection.NodePropertyKeys[label], key)
			}
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
			relType = text
		}
		properties := make(map[string]float64)
		if value := data.value("relationshipProperties"); value != nil {
			entries, ok := value.(map[string]interface{})
			if !ok {
				return nil, localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(value)), nil)
			}
			for key, entry := range entries {
				number, ok := toFloat64(entry)
				if _, isText := entry.(string); !ok || isText {
					return nil, localizedError(localization.CypherGraphProceduresUnsupportedGDSValue(gdsJavaType(entry)), nil)
				}
				properties[key] = number
				projection.RelationshipPropertyKeys[relType] = appendUnique(projection.RelationshipPropertyKeys[relType], key)
			}
		}
		projection.Types = appendUnique(projection.Types, relType)
		id := "row-" + strconv.Itoa(index)
		projection.Relationships = append(projection.Relationships, ProjectedRelationship{ID: id, Source: source, Target: target, Type: relType, Properties: properties})
	}

	// Undirected types: each relationship of them both ways, as GDS holds
	// them.
	if undirected != nil {
		if len(undirected) == 1 && undirected[0] == "*" {
			undirected = append([]string(nil), projection.Types...)
		} else if missing := missingNames(undirected, projection.Types); len(missing) > 0 {
			projected := projection.Types
			if len(projected) == 0 {
				projected = []string{"__ALL__"}
			}
			return nil, localizedError(localization.CypherGraphProceduresUndirectedTypesNotProjected("["+strings.Join(missing, ", ")+"]", gdsNameList(projected)), nil)
		}
		forward := len(projection.Relationships)
		for _, relationship := range projection.Relationships[:forward] {
			if containsString(undirected, relationship.Type) {
				projection.Relationships = append(projection.Relationships, ProjectedRelationship{
					ID: relationship.ID, Source: relationship.Target, Target: relationship.Source, Type: relationship.Type, Properties: relationship.Properties,
				})
			}
		}
	}

	sort.Strings(projection.NodeIDs)
	projection.nodeIndex = make(map[string]int, len(projection.NodeIDs))
	for i, id := range projection.NodeIDs {
		projection.nodeIndex[id] = i
		for _, label := range projection.NodeLabels[id] {
			projection.Labels = appendUnique(projection.Labels, label)
		}
	}
	sort.Strings(projection.Labels)
	sort.Strings(projection.Types)
	projection.NodeCount = len(projection.NodeIDs)
	projection.RelationshipCount = len(projection.Relationships)
	projection.schema = gdsAggregationSchema(projection, undirected, nodePropertyFloat)

	query := statementTextFromContext(ctx)
	configuration["query"] = query
	projection.Configuration = configuration
	key := gdsCatalogKey(projection.Database, name)
	projectionsMu.Lock()
	if _, exists := graphProjections[key]; exists {
		projectionsMu.Unlock()
		return nil, localizedError(localization.CypherGraphProceduresGraphExistsAggregation(name), nil)
	}
	graphProjections[key] = projection
	projectionsMu.Unlock()
	return map[string]interface{}{
		"graphName": name, "nodeCount": int64(projection.NodeCount), "relationshipCount": int64(projection.RelationshipCount),
		"projectMillis": time.Since(started).Milliseconds(), "configuration": configuration, "query": query,
	}, nil
}

// gdsProjectionNode reads a projected node: a node, or an INTEGER id, which
// needn't be a stored node's, as in GDS.
func gdsProjectionNode(value interface{}) (string, error) {
	switch node := value.(type) {
	case *storage.Node:
		if node != nil {
			return string(node.ID), nil
		}
	default:
		if isIntegerProcedureValue(value) {
			return strconv.FormatInt(toInt64(value), 10), nil
		}
	}
	typeName := gdsJavaType(value)
	if value == nil {
		typeName = "NO_VALUE"
	}
	return "", localizedError(localization.CypherGraphProceduresNodeNotNodeOrInteger(typeName), nil)
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
	case string:
		return []string{labels}, nil
	}
	items, ok := cypherListValue(value)
	if !ok {
		return nil, localizedError(localization.CypherGraphProceduresConfigValueType(key, "List", gdsJavaType(value)), nil)
	}
	names := make([]string, 0, len(items))
	for _, item := range items {
		name, ok := item.(string)
		if !ok {
			return nil, localizedError(localization.CypherGraphProceduresConfigValueType(key, "List", gdsJavaType(value)), nil)
		}
		names = append(names, name)
	}
	return names, nil
}

// gdsAggregationConfiguration reads the aggregation's configuration map
// (its 5th argument): undirectedRelationshipTypes, inverseIndexedRelationshipTypes,
// readConcurrency, jobId and logProgress, other keys ignored as GDS ignores
// them. It returns the configuration as GDS reports it and the undirected
// types (nil for none).
func gdsAggregationConfiguration(value interface{}) (map[string]interface{}, []string, error) {
	configuration := map[string]interface{}{
		"inverseIndexedRelationshipTypes": []interface{}{}, "undirectedRelationshipTypes": []interface{}{},
		"readConcurrency": int64(4), "logProgress": true,
	}
	entries := map[string]interface{}{}
	if value != nil {
		var ok bool
		if entries, ok = value.(map[string]interface{}); !ok {
			return nil, nil, localizedError(localization.CypherGraphProceduresConfigValueType("configuration", "Map", gdsJavaType(value)), nil)
		}
	}
	known := map[string]bool{}
	for key := range entries {
		for _, name := range []string{"undirectedRelationshipTypes", "inverseIndexedRelationshipTypes", "readConcurrency", "jobId", "logProgress"} {
			if strings.EqualFold(key, name) {
				known[key] = true
			}
		}
	}
	filtered := map[string]interface{}{}
	for key, entry := range entries {
		if known[key] {
			filtered[key] = entry
		}
	}
	c, _ := newGDSConfig(filtered, "undirectedRelationshipTypes", "inverseIndexedRelationshipTypes", "readConcurrency", "jobId", "logProgress")
	var undirected []string
	for _, key := range []string{"undirectedRelationshipTypes", "inverseIndexedRelationshipTypes"} {
		listed := c.value(key)
		if listed == nil {
			continue
		}
		items, ok := cypherListValue(listed)
		if !ok {
			return nil, nil, c.typeError(key, "List")
		}
		names := make([]interface{}, 0, len(items))
		for _, item := range items {
			name, ok := item.(string)
			if !ok {
				return nil, nil, c.typeError(key, "List")
			}
			names = append(names, name)
			if key == "undirectedRelationshipTypes" {
				undirected = append(undirected, name)
			}
		}
		configuration[key] = names
	}
	if concurrency := c.value("readConcurrency"); concurrency != nil {
		if !isIntegerProcedureValue(concurrency) {
			return nil, nil, localizedError(localization.CypherGraphProceduresUnsupportedConcurrency(gdsJavaType(concurrency)), nil)
		}
		configuration["readConcurrency"] = toInt64(concurrency)
	}
	jobID, err := c.jobID()
	if err != nil {
		return nil, nil, err
	}
	configuration["jobId"] = jobID
	if logProgress, given, err := c.boolean("logProgress"); err != nil {
		return nil, nil, err
	} else if given {
		configuration["logProgress"] = logProgress
	}
	return configuration, undirected, nil
}

// gdsAggregationSchema is an aggregation projection's schema as GDS reports
// it: node properties PERSISTENT, relationship properties TRANSIENT.
func gdsAggregationSchema(projection *GraphProjection, undirected []string, nodePropertyFloat map[string]bool) map[string]interface{} {
	nodes := make(map[string]interface{})
	for _, label := range projection.Labels {
		properties := make(map[string]interface{})
		for _, key := range projection.NodePropertyKeys[label] {
			if nodePropertyFloat[key] {
				properties[key] = "Float (DefaultValue(NaN), PERSISTENT)"
			} else {
				properties[key] = "Integer (DefaultValue(-9223372036854775808), PERSISTENT)"
			}
		}
		nodes[label] = properties
	}
	relationships := make(map[string]interface{})
	directions := make(map[string]interface{})
	types := projection.Types
	if len(types) == 0 {
		types = []string{"__ALL__"}
	}
	for _, relType := range types {
		properties := make(map[string]interface{})
		for _, key := range projection.RelationshipPropertyKeys[relType] {
			properties[key] = "Float (DefaultValue(NaN), TRANSIENT, Aggregation.NONE)"
		}
		relationships[relType] = properties
		direction := "DIRECTED"
		if containsString(undirected, relType) {
			direction = "UNDIRECTED"
		}
		directions[relType] = direction
	}
	return map[string]interface{}{"graphProperties": map[string]interface{}{}, "nodes": nodes, "relationships": relationships, "directions": directions}
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
	case "Map":
		return "MAP"
	case "Node":
		return "NODE"
	}
	return strings.ToUpper(gdsJavaType(value))
}
