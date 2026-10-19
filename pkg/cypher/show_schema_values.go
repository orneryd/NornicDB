package cypher

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// Values SHOW INDEXES and SHOW CONSTRAINTS report as Neo4j 5 does (#530):
// the index provider of each index type, the options map, and a
// createStatement that recreates the index or constraint.

// showIndexProvider is Neo4j's index provider for an index type.
func showIndexProvider(indexType string) string {
	switch indexType {
	case "RANGE":
		return "range-1.0"
	case "TEXT":
		return "text-2.0"
	case "POINT":
		return "point-1.0"
	case "FULLTEXT":
		return "fulltext-1.0"
	case "VECTOR":
		return "vector-2.0"
	case "LOOKUP":
		return "token-lookup-1.0"
	}
	return ""
}

// showIndexOptions is the options column: the index's settings and its
// provider.
func showIndexOptions(provider string, config map[string]interface{}) map[string]interface{} {
	if config == nil {
		config = map[string]interface{}{}
	}
	return map[string]interface{}{"indexConfig": config, "indexProvider": provider}
}

// quoteSchemaName backtick-quotes a name for a generated statement, doubling
// any backtick in it.
func quoteSchemaName(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// schemaPattern is the pattern of an index or constraint statement:
// (n:`A`|`B`) for nodes, ()-[r:`T`]-() for relationships. It returns the
// pattern and its variable.
func schemaPattern(entityType string, labelsOrTypes []string) (string, string) {
	names := make([]string, len(labelsOrTypes))
	for i, name := range labelsOrTypes {
		names[i] = quoteSchemaName(name)
	}
	if entityType == string(storage.ConstraintEntityRelationship) {
		return "()-[r:" + strings.Join(names, "|") + "]-()", "r"
	}
	return "(n:" + strings.Join(names, "|") + ")", "n"
}

// schemaProperties lists properties of variable as n.`a`, n.`b`.
func schemaProperties(variable string, properties []string) string {
	items := make([]string, len(properties))
	for i, property := range properties {
		items[i] = variable + "." + quoteSchemaName(property)
	}
	return strings.Join(items, ", ")
}

// showIndexCreateStatement is the statement that recreates a standalone
// index, in Neo4j's form.
func showIndexCreateStatement(indexType, name, entityType string, labelsOrTypes, properties []string, config map[string]interface{}) string {
	pattern, variable := schemaPattern(entityType, labelsOrTypes)
	statement := "CREATE " + indexType + " INDEX " + quoteSchemaName(name) + " FOR " + pattern + " ON "
	if indexType == "FULLTEXT" {
		statement += "EACH [" + schemaProperties(variable, properties) + "]"
	} else {
		statement += "(" + schemaProperties(variable, properties) + ")"
	}
	if len(config) > 0 {
		statement += " OPTIONS {indexConfig: " + schemaConfigLiteral(config) + "}"
	}
	return statement
}

// schemaConfigLiteral writes an index configuration map as Neo4j prints it
// in a createStatement: {`key`: value,`key2`: value2}, keys in order.
func schemaConfigLiteral(config map[string]interface{}) string {
	keys := make([]string, 0, len(config))
	for key := range config {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	items := make([]string, len(keys))
	for i, key := range keys {
		items[i] = quoteSchemaName(key) + ": " + schemaConfigValue(config[key])
	}
	return "{" + strings.Join(items, ",") + "}"
}

func schemaConfigValue(value interface{}) string {
	switch v := value.(type) {
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(v, `\`, `\\`), "'", `\'`) + "'"
	case bool:
		return strconv.FormatBool(v)
	case int:
		return strconv.Itoa(v)
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return fmt.Sprintf("%v", value)
}

// showConstraintType is the type SHOW CONSTRAINTS reports: Neo4j 5's name,
// which tells node and relationship constraints apart. NornicDB's own
// constraint types keep their names.
func showConstraintType(constraintType storage.ConstraintType, entityType storage.ConstraintEntityType) string {
	relationship := entityType == storage.ConstraintEntityRelationship
	switch constraintType {
	case storage.ConstraintUnique:
		if relationship {
			return "RELATIONSHIP_UNIQUENESS"
		}
		return "UNIQUENESS"
	case storage.ConstraintExists:
		if relationship {
			return "RELATIONSHIP_PROPERTY_EXISTENCE"
		}
		return "NODE_PROPERTY_EXISTENCE"
	case storage.ConstraintPropertyType:
		if relationship {
			return "RELATIONSHIP_PROPERTY_TYPE"
		}
		return "NODE_PROPERTY_TYPE"
	}
	return string(constraintType)
}

// showConstraintCreateStatement is the statement that recreates a
// constraint: Neo4j's form for Neo4j's constraint types, and the documented
// NornicDB form (docs/neo4j-migration/cypher-compatibility.md) for
// temporal no-overlap, domain, cardinality and endpoint policy constraints.
func showConstraintCreateStatement(c storage.Constraint) interface{} {
	head := "CREATE CONSTRAINT " + quoteSchemaName(c.Name) + " FOR "
	entityType := c.EffectiveEntityType()
	pattern, variable := schemaPattern(string(entityType), []string{c.Label})
	properties := "(" + schemaProperties(variable, c.Properties) + ")"
	switch c.Type {
	case storage.ConstraintUnique:
		return head + pattern + " REQUIRE " + properties + " IS UNIQUE"
	case storage.ConstraintNodeKey:
		return head + pattern + " REQUIRE " + properties + " IS NODE KEY"
	case storage.ConstraintRelationshipKey:
		return head + pattern + " REQUIRE " + properties + " IS RELATIONSHIP KEY"
	case storage.ConstraintExists:
		return head + pattern + " REQUIRE " + properties + " IS NOT NULL"
	case storage.ConstraintTemporal:
		return head + pattern + " REQUIRE " + properties + " IS TEMPORAL NO OVERLAP"
	case storage.ConstraintDomain:
		values := make([]string, len(c.AllowedValues))
		for i, value := range c.AllowedValues {
			values[i] = schemaConfigValue(value)
		}
		return head + pattern + " REQUIRE " + schemaProperties(variable, c.Properties) + " IN [" + strings.Join(values, ", ") + "]"
	case storage.ConstraintCardinality:
		relationship := "()-[r:" + quoteSchemaName(c.Label) + "]->()"
		if strings.EqualFold(c.Direction, "INCOMING") {
			relationship = "()<-[r:" + quoteSchemaName(c.Label) + "]-()"
		}
		return head + relationship + " REQUIRE MAX COUNT " + strconv.Itoa(c.MaxCount)
	case storage.ConstraintPolicy:
		return head + "(:" + quoteSchemaName(c.SourceLabel) + ")-[r:" + quoteSchemaName(c.Label) + "]->(:" + quoteSchemaName(c.TargetLabel) + ") REQUIRE " + strings.ToUpper(c.PolicyMode)
	}
	return nil
}

// showPropertyTypeCreateStatement is the statement that recreates a
// property type constraint.
func showPropertyTypeCreateStatement(name string, entityType storage.ConstraintEntityType, label, property, propertyType string) string {
	pattern, variable := schemaPattern(string(entityType), []string{label})
	return "CREATE CONSTRAINT " + quoteSchemaName(name) + " FOR " + pattern + " REQUIRE (" + schemaProperties(variable, []string{property}) + ") IS :: " + propertyType
}

// showConstraintOptions is the options column of a constraint: the owned
// index's settings for an index-backed constraint, else null.
func showConstraintOptions(constraintType storage.ConstraintType) interface{} {
	switch constraintType {
	case storage.ConstraintUnique, storage.ConstraintNodeKey, storage.ConstraintRelationshipKey:
		return showIndexOptions(showIndexProvider("RANGE"), nil)
	}
	return nil
}
