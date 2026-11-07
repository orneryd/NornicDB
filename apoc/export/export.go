// Package export provides APOC export functions.
//
// This package implements all apoc.export.* functions for exporting
// graph data to various formats (JSON, CSV, Cypher, GraphML).
// Author: L6 / #1665 lane (Codex gpt-6.1-sol). Existing export producers delegate numeric text to its shared owner.
package apocexport

import (
	"encoding/csv"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

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

// writeExportFile is the one owner of an export destination: it creates the
// parent directory of filePath and writes data there, reporting why the
// destination could not be written instead of dropping the result. An empty
// filePath names no destination, so there is nothing to write and no error.
func writeExportFile(filePath string, data []byte) error {
	if filePath == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return nil
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

	return writeExportFile(filePath, []byte(jsonStr))
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

	return writeExportFile(filePath, []byte(builder.String()))
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
	return writeExportFile(filePath, []byte(Cypher(nodes, rels)))
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
//
// GraphML rejects malformed UTF-8 and characters forbidden by XML 1.0.
// On error it returns no partial document.
func GraphML(nodes []*Node, rels []*Relationship) (string, error) {
	var builder strings.Builder

	builder.WriteString("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
	builder.WriteString("<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\">\n")
	builder.WriteString("  <graph id=\"G\" edgedefault=\"directed\">\n")

	// Export nodes
	for _, node := range nodes {
		builder.WriteString(fmt.Sprintf("    <node id=\"n%d\">\n", node.ID))
		for key, val := range node.Properties {
			if err := writeGraphMLData(&builder, key, apocconvert.ToString(val)); err != nil {
				return "", err
			}
		}
		builder.WriteString("    </node>\n")
	}

	// Export relationships
	for _, rel := range rels {
		builder.WriteString(fmt.Sprintf("    <edge source=\"n%d\" target=\"n%d\">\n", rel.StartNode, rel.EndNode))
		if err := writeGraphMLData(&builder, "type", rel.Type); err != nil {
			return "", err
		}
		for key, val := range rel.Properties {
			if err := writeGraphMLData(&builder, key, apocconvert.ToString(val)); err != nil {
				return "", err
			}
		}
		builder.WriteString("    </edge>\n")
	}

	builder.WriteString("  </graph>\n")
	builder.WriteString("</graphml>\n")

	return builder.String(), nil
}

// GraphMLAll exports all graph data to GraphML.
//
// Example:
//
//	apoc.export.graphml.all('/path/to/export.graphml') => exported
func GraphMLAll(nodes []*Node, rels []*Relationship, filePath string) error {
	document, err := GraphML(nodes, rels)
	if err != nil {
		return err
	}
	return writeExportFile(filePath, []byte(document))
}

// GraphMLData exports specific data to GraphML.
//
// Example:
//
//	apoc.export.graphml.data(nodes, rels, '/path/to/export.graphml') => exported
func GraphMLData(nodes []*Node, rels []*Relationship, filePath string) error {
	return GraphMLAll(nodes, rels, filePath)
}

// writeGraphMLData writes one property as a GraphML data element. The key is an
// attribute value and the property is element text, so both are escaped and a
// key or value carrying markup cannot break the document.
func writeGraphMLData(builder *strings.Builder, key, value string) error {
	if err := validateXMLText(key); err != nil {
		return err
	}
	if err := validateXMLText(value); err != nil {
		return err
	}
	builder.WriteString("      <data key=\"")
	if err := xml.EscapeText(builder, []byte(key)); err != nil {
		return err
	}
	builder.WriteString("\">")
	if err := xml.EscapeText(builder, []byte(value)); err != nil {
		return err
	}
	builder.WriteString("</data>\n")
	return nil
}

// validateXMLText enforces the XML 1.0 Char production before EscapeText,
// whose replacement behavior would otherwise silently lose invalid input.
func validateXMLText(text string) error {
	for offset := 0; offset < len(text); {
		r, width := utf8.DecodeRuneInString(text[offset:])
		if (r == utf8.RuneError && width == 1) ||
			!(r == '\t' || r == '\n' || r == '\r' ||
				r >= 0x20 && r <= 0xd7ff || r >= 0xe000 && r <= 0xfffd || r >= 0x10000 && r <= 0x10ffff) {
			return fmt.Errorf("GraphML: unrepresentable XML 1.0 character at byte %d", offset)
		}
		offset += width
	}
	return nil
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
	return writeExportFile(filePath, []byte(data))
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
		return GraphML(nodes, rels)
	default:
		return "", fmt.Errorf("unsupported format: %s", format)
	}
}
