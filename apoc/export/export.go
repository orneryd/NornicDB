// Package export provides APOC export functions.
//
// This package implements all apoc.export.* functions for exporting
// graph data to various formats (JSON, CSV, Cypher, GraphML).
// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Existing export producers delegate numeric text to its shared owner.
package apocexport

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	apocconvert "github.com/orneryd/nornicdb/apoc/convert"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/pool"
)

// Node represents a graph node.
type Node struct {
	ID         int64
	Labels     []string
	Properties map[string]interface{}
}

// Relationship represents a graph relationship.
type Relationship struct {
	ID         int64
	Type       string
	StartNode  int64
	EndNode    int64
	Properties map[string]interface{}
}

// Json exports data to JSON format.
//
// Example:
//
//	apoc.export.json(nodes, relationships) => JSON string
func Json(nodes []*Node, rels []*Relationship) (string, error) {
	data := map[string]interface{}{
		"nodes":         nodes,
		"relationships": rels,
	}

	bytes, err := valueconvert.PropertyJSON.MarshalIndent(data, "", "  ")
	if err != nil {
		return "", err
	}

	return string(bytes), nil
}

// JsonAll exports all graph data to JSON.
//
// Example:
//
//	apoc.export.json.all('/path/to/export.json') => exported
func JsonAll(nodes []*Node, rels []*Relationship, filePath string) error {
	jsonStr, err := Json(nodes, rels)
	if err != nil {
		return err
	}

	// Write to file if path provided
	if filePath != "" {
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(filePath, []byte(jsonStr), 0644); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
	}

	return nil
}

// JsonData exports specific data to JSON.
//
// Example:
//
//	apoc.export.json.data(nodes, rels, '/path/to/export.json') => exported
func JsonData(nodes []*Node, rels []*Relationship, filePath string) error {
	return JsonAll(nodes, rels, filePath)
}

// Csv exports data to CSV format.
//
// Example:
//
//	apoc.export.csv(nodes, '/path/to/export.csv') => exported
func Csv(nodes []*Node, filePath string) error {
	// Build CSV data
	var builder strings.Builder
	writer := csv.NewWriter(&builder)

	// Write header
	if len(nodes) > 0 {
		headers := []string{"id", "labels"}
		for key := range nodes[0].Properties {
			headers = append(headers, key)
		}
		writer.Write(headers)

		// Write rows
		for _, node := range nodes {
			row := []string{
				fmt.Sprintf("%d", node.ID),
				strings.Join(node.Labels, ";"),
			}
			for key := range nodes[0].Properties {
				if val, ok := node.Properties[key]; ok {
					row = append(row, apocconvert.ToString(val))
				} else {
					row = append(row, "")
				}
			}
			writer.Write(row)
		}
	}

	writer.Flush()

	// Write to file if path provided
	if filePath != "" {
		if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(filePath, []byte(builder.String()), 0644); err != nil {
			return fmt.Errorf("failed to write file: %w", err)
		}
	}

	return nil
}

// CsvAll exports all graph data to CSV.
//
// Example:
//
//	apoc.export.csv.all('/path/to/export.csv') => exported
func CsvAll(nodes []*Node, rels []*Relationship, filePath string) error {
	return Csv(nodes, filePath)
}

// CsvData exports specific data to CSV.
//
// Example:
//
//	apoc.export.csv.data(nodes, '/path/to/export.csv') => exported
func CsvData(nodes []*Node, filePath string) error {
	return Csv(nodes, filePath)
}

// Cypher exports data as Cypher statements.
//
// Example:
//
//	apoc.export.cypher(nodes, rels) => Cypher statements
func Cypher(nodes []*Node, rels []*Relationship) string {
	builder := pool.GetStringBuilder()
	defer pool.PutStringBuilder(builder)

	// Export nodes
	for _, node := range nodes {
		builder.WriteString(fmt.Sprintf("CREATE (n%d", node.ID))

		// Labels
		for _, label := range node.Labels {
			builder.WriteString(fmt.Sprintf(":%s", label))
		}

		// Properties
		if len(node.Properties) > 0 {
			builder.WriteString(" {")
			first := true
			for key, val := range node.Properties {
				if !first {
					builder.WriteString(", ")
				}
				builder.WriteString(key)
				builder.WriteString(": ")
				appendValue(builder, val)
				first = false
			}
			builder.WriteString("}")
		}

		builder.WriteString(");\n")
	}

	// Export relationships
	for _, rel := range rels {
		builder.WriteString(fmt.Sprintf("MATCH (a), (b) WHERE id(a) = %d AND id(b) = %d\n", rel.StartNode, rel.EndNode))
		builder.WriteString(fmt.Sprintf("CREATE (a)-[r:%s", rel.Type))

		// Properties
		if len(rel.Properties) > 0 {
			builder.WriteString(" {")
			first := true
			for key, val := range rel.Properties {
				if !first {
					builder.WriteString(", ")
				}
				builder.WriteString(key)
				builder.WriteString(": ")
				appendValue(builder, val)
				first = false
			}
			builder.WriteString("}")
		}

		builder.WriteString("]->(b);\n")
	}

	return builder.String()
}

// CypherAll exports all graph data as Cypher statements.
//
// Example:
//
//	apoc.export.cypher.all('/path/to/export.cypher') => exported
func CypherAll(nodes []*Node, rels []*Relationship, filePath string) error {
	cypherStr := Cypher(nodes, rels)

	// In production, would write to file
	_ = cypherStr
	_ = filePath

	return nil
}

// CypherData exports specific data as Cypher statements.
//
// Example:
//
//	apoc.export.cypher.data(nodes, rels, '/path/to/export.cypher') => exported
func CypherData(nodes []*Node, rels []*Relationship, filePath string) error {
	return CypherAll(nodes, rels, filePath)
}

// GraphML exports data to GraphML format.
//
// Example:
//
//	apoc.export.graphml(nodes, rels) => GraphML string
func GraphML(nodes []*Node, rels []*Relationship) string {
	var builder strings.Builder

	builder.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	builder.WriteString("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n")
	builder.WriteString("  <graph id=\"G\" edgedefault=\"directed\">\n")

	// Export nodes
	for _, node := range nodes {
		builder.WriteString(fmt.Sprintf("    <node id=\"n%d\">\n", node.ID))
		for key, val := range node.Properties {
			builder.WriteString(fmt.Sprintf("      <data key=\"%s\">%s</data>\n", key, apocconvert.ToString(val)))
		}
		builder.WriteString("    </node>\n")
	}

	// Export relationships
	for _, rel := range rels {
		builder.WriteString(fmt.Sprintf("    <edge source=\"n%d\" target=\"n%d\">\n", rel.StartNode, rel.EndNode))
		builder.WriteString(fmt.Sprintf("      <data key=\"type\">%s</data>\n", rel.Type))
		for key, val := range rel.Properties {
			builder.WriteString(fmt.Sprintf("      <data key=\"%s\">%s</data>\n", key, apocconvert.ToString(val)))
		}
		builder.WriteString("    </edge>\n")
	}

	builder.WriteString("  </graph>\n")
	builder.WriteString("</graphml>\n")

	return builder.String()
}

// GraphMLAll exports all graph data to GraphML.
//
// Example:
//
//	apoc.export.graphml.all('/path/to/export.graphml') => exported
func GraphMLAll(nodes []*Node, rels []*Relationship, filePath string) error {
	graphMLStr := GraphML(nodes, rels)

	// In production, would write to file
	_ = graphMLStr
	_ = filePath

	return nil
}

// GraphMLData exports specific data to GraphML.
//
// Example:
//
//	apoc.export.graphml.data(nodes, rels, '/path/to/export.graphml') => exported
func GraphMLData(nodes []*Node, rels []*Relationship, filePath string) error {
	return GraphMLAll(nodes, rels, filePath)
}

// appendValue owns Cypher literal structure. Every numeric leaf uses AppendNumericText;
// a list or map must remain a container when the exported source is executed.
func appendValue(out *pool.PooledStringBuilder, val interface{}) {
	var number [40]byte
	if text, numeric := valueconvert.AppendNumericText(number[:0], val); numeric {
		out.Write(text)
		return
	}
	if list, ok := valueconvert.ReadList(val); ok {
		out.WriteByte('[')
		for i := 0; i < list.Len(); i++ {
			if i > 0 {
				out.WriteString(", ")
			}
			appendValue(out, list.At(i))
		}
		out.WriteByte(']')
		return
	}
	switch v := val.(type) {
	case nil:
		out.WriteString("null")
	case string:
		out.WriteByte('\'')
		out.WriteString(strings.ReplaceAll(v, "'", "\\'"))
		out.WriteByte('\'')
	case bool:
		fmt.Fprint(out, v)
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out.WriteByte('{')
		for i, key := range keys {
			if i > 0 {
				out.WriteString(", ")
			}
			out.WriteString(cyphertext.PrintIdentifier(key, nil, cyphertext.RawSource))
			out.WriteString(": ")
			appendValue(out, v[key])
		}
		out.WriteByte('}')
	default:
		fmt.Fprintf(out, "'%v'", v)
	}
}

// ToFile writes export data to a file.
//
// Example:
//
//	apoc.export.toFile(data, '/path/to/file.txt') => written
func ToFile(data string, filePath string) error {
	// In production, would write to file
	_ = data
	_ = filePath
	return nil
}

// ToString returns export data as a string.
//
// Example:
//
//	apoc.export.toString(nodes, rels, 'json') => string
func ToString(nodes []*Node, rels []*Relationship, format string) (string, error) {
	switch format {
	case "json":
		return Json(nodes, rels)
	case "cypher":
		return Cypher(nodes, rels), nil
	case "graphml":
		return GraphML(nodes, rels), nil
	default:
		return "", fmt.Errorf("unsupported format: %s", format)
	}
}
