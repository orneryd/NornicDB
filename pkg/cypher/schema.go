// Schema command parsing and execution for Cypher.
//
// This file implements Neo4j schema management commands:
//   - CREATE CONSTRAINT
//   - CREATE INDEX
//   - CREATE RANGE INDEX
//   - CREATE FULLTEXT INDEX
//   - CREATE VECTOR INDEX
package cypher

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

// executeSchemaCommand handles CREATE CONSTRAINT and CREATE INDEX commands.
func (e *StorageExecutor) executeSchemaCommand(ctx context.Context, cypher string) (*ExecuteResult, error) {
	upper := strings.ToUpper(cypher)

	// Order matters: check more specific patterns first
	if strings.Contains(upper, "CREATE CONSTRAINT") {
		return e.executeCreateConstraint(ctx, cypher)
	} else if strings.Contains(upper, "CREATE FULLTEXT INDEX") {
		return e.executeCreateFulltextIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE VECTOR INDEX") {
		return e.executeCreateVectorIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE RANGE INDEX") {
		return e.executeCreateRangeIndex(ctx, cypher)
	} else if strings.Contains(upper, "CREATE INDEX") {
		return e.executeCreateIndex(ctx, cypher)
	}

	return nil, fmt.Errorf("unknown schema command: %s", cypher)
}

// executeCreateConstraint handles CREATE CONSTRAINT commands.
//
// Supported syntax (Neo4j 5.x):
//
//	CREATE CONSTRAINT constraint_name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
//
// Supported syntax (Neo4j 4.x):
//
//	CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
func (e *StorageExecutor) executeCreateConstraint(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern 1 (Neo4j 5.x): CREATE CONSTRAINT name IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintNamedForRequire.FindStringSubmatch(cypher); matches != nil {
		constraintName := matches[1]
		label := matches[3]
		property := matches[5]

		// Add constraint to schema
		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 2 (Neo4j 5.x without name): CREATE CONSTRAINT IF NOT EXISTS FOR (n:Label) REQUIRE n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintUnnamedForRequire.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Pattern 3 (Neo4j 4.x): CREATE CONSTRAINT IF NOT EXISTS ON (n:Label) ASSERT n.property IS UNIQUE
	// Uses pre-compiled pattern from regex_patterns.go
	if matches := constraintOnAssert.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		property := matches[4]
		constraintName := fmt.Sprintf("constraint_%s_%s", strings.ToLower(label), strings.ToLower(property))

		if err := e.storage.GetSchema().AddUniqueConstraint(constraintName, label, property); err != nil {
			return nil, err
		}
		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE CONSTRAINT syntax")
}

// executeCreateIndex handles CREATE INDEX commands.
//
// Supported syntax:
//
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//	CREATE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2)
//	CREATE INDEX IF NOT EXISTS FOR (n:Label) ON (n.prop1, n.prop2, n.prop3)
//
// Supports both single-property and composite (multi-property) indexes.
func (e *StorageExecutor) executeCreateIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property[, n.property2, ...])
	// Uses pre-compiled patterns from regex_patterns.go
	if matches := indexNamedFor.FindStringSubmatch(cypher); matches != nil {
		indexName := matches[1]
		label := matches[3]
		propertiesStr := matches[4] // e.g., "n.prop1, n.prop2"

		// Parse properties (single or multiple)
		properties := e.parseIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Add index to schema (supports composite indexes)
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Try without index name
	if matches := indexUnnamedFor.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		propertiesStr := matches[3] // e.g., "n.prop1, n.prop2"

		// Parse properties
		properties := e.parseIndexProperties(propertiesStr)
		if len(properties) == 0 {
			return nil, fmt.Errorf("no properties specified for index")
		}

		// Generate index name based on label and properties
		propsJoined := strings.Join(properties, "_")
		indexName := fmt.Sprintf("index_%s_%s", strings.ToLower(label), strings.ToLower(propsJoined))

		// Add index
		if err := e.storage.GetSchema().AddPropertyIndex(indexName, label, properties); err != nil {
			return nil, err
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE INDEX syntax")
}

// executeCreateRangeIndex handles CREATE RANGE INDEX commands.
//
// Supported syntax:
//
//	CREATE RANGE INDEX index_name IF NOT EXISTS FOR (n:Label) ON (n.property)
//
// Range indexes optimize queries with range predicates (>, <, >=, <=, BETWEEN).
func (e *StorageExecutor) executeCreateRangeIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE RANGE INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Reuse the standard index pattern - same structure
	if matches := indexNamedFor.FindStringSubmatch(cypher); matches != nil {
		indexName := matches[1]
		label := matches[3]
		propertiesStr := matches[5]

		// Range index only supports single property
		properties := e.parseIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	// Unnamed range index
	if matches := indexUnnamedFor.FindStringSubmatch(cypher); matches != nil {
		label := matches[2]
		propertiesStr := matches[4]

		properties := e.parseIndexProperties(propertiesStr)
		if len(properties) != 1 {
			return nil, fmt.Errorf("RANGE INDEX only supports single property, got %d", len(properties))
		}

		indexName := fmt.Sprintf("range_idx_%s_%s", strings.ToLower(label), properties[0])
		err := e.storage.GetSchema().AddRangeIndex(indexName, label, properties[0])
		if err != nil {
			return nil, fmt.Errorf("failed to create range index: %w", err)
		}

		return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
	}

	return nil, fmt.Errorf("invalid CREATE RANGE INDEX syntax")
}

// parseIndexProperties parses property list from index ON clause.
//
// Handles both single and composite property syntax:
//   - "n.property"           -> ["property"]
//   - "n.prop1, n.prop2"     -> ["prop1", "prop2"]
//   - "n.a, n.b, n.c"        -> ["a", "b", "c"]
func (e *StorageExecutor) parseIndexProperties(propertiesStr string) []string {
	// Split by comma
	parts := strings.Split(propertiesStr, ",")
	var properties []string

	for _, part := range parts {
		part = strings.TrimSpace(part)
		// Extract property name after dot (e.g., "n.prop" -> "prop")
		if dotIdx := strings.LastIndex(part, "."); dotIdx >= 0 && dotIdx < len(part)-1 {
			propName := strings.TrimSpace(part[dotIdx+1:])
			if propName != "" {
				properties = append(properties, propName)
			}
		}
	}

	return properties
}

// executeCreateFulltextIndex handles CREATE FULLTEXT INDEX commands.
//
// Supported syntax:
//
//	CREATE FULLTEXT INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON EACH [n.prop1, n.prop2]
func (e *StorageExecutor) executeCreateFulltextIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE FULLTEXT INDEX name IF NOT EXISTS FOR (n:Label) ON EACH [n.prop1, n.prop2]
	// Uses pre-compiled pattern from regex_patterns.go
	matches := fulltextIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE FULLTEXT INDEX syntax: %s", cypher)
	}

	indexName := matches[1]
	label := matches[3]
	propertiesStr := matches[4]

	// Parse properties: "n.prop1, n.prop2" -> ["prop1", "prop2"]
	properties := []string{}
	for _, prop := range strings.Split(propertiesStr, ",") {
		prop = strings.TrimSpace(prop)
		// Extract property name from "n.property"
		if parts := strings.Split(prop, "."); len(parts) == 2 {
			properties = append(properties, parts[1])
		}
	}

	if len(properties) == 0 {
		return nil, fmt.Errorf("no properties found in fulltext index definition")
	}

	// Add fulltext index
	schema := e.storage.GetSchema()
	if schema == nil {
		return nil, fmt.Errorf("schema manager not available")
	}

	if err := schema.AddFulltextIndex(indexName, []string{label}, properties); err != nil {
		return nil, fmt.Errorf("failed to add fulltext index: %w", err)
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}

// executeCreateVectorIndex handles CREATE VECTOR INDEX commands.
//
// Supported syntax:
//
//	CREATE VECTOR INDEX index_name IF NOT EXISTS
//	FOR (n:Label) ON (n.property)
//	OPTIONS {indexConfig: {`vector.dimensions`: 1024, `vector.similarity_function`: 'cosine'}}
func (e *StorageExecutor) executeCreateVectorIndex(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Pattern: CREATE VECTOR INDEX name IF NOT EXISTS FOR (n:Label) ON (n.property)
	// Uses pre-compiled patterns from regex_patterns.go
	matches := vectorIndexPattern.FindStringSubmatch(cypher)

	if matches == nil {
		return nil, fmt.Errorf("invalid CREATE VECTOR INDEX syntax")
	}

	indexName := matches[1]
	label := matches[3]
	property := matches[5]

	// Parse OPTIONS if present
	dimensions := 1024         // Default
	similarityFunc := "cosine" // Default

	if strings.Contains(cypher, "OPTIONS") {
		// Extract dimensions using pre-compiled pattern
		if dimMatches := vectorDimensionsPattern.FindStringSubmatch(cypher); dimMatches != nil {
			if dim, err := strconv.Atoi(dimMatches[1]); err == nil {
				dimensions = dim
			}
		}

		// Extract similarity function using pre-compiled pattern
		if simMatches := vectorSimilarityPattern.FindStringSubmatch(cypher); simMatches != nil {
			similarityFunc = simMatches[1]
		}
	}

	// Add vector index
	if err := e.storage.GetSchema().AddVectorIndex(indexName, label, property, dimensions, similarityFunc); err != nil {
		return nil, err
	}

	return &ExecuteResult{Columns: []string{}, Rows: [][]interface{}{}}, nil
}
