package adminimport

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

type Neo4jCSVExportOptions struct {
	OutputDir       string
	Delimiter       rune
	ArrayDelimiter  rune
	VectorDelimiter rune
	Quote           rune
}

const Neo4jCSVSchemaFileName = "schema.cypher"
const Neo4jCSVNornicSchemaFileName = "schema.nornic.json"

type inferredColumn struct {
	Header     string
	Property   string
	Kind       string
	ScalarType string
	VectorDims int
	EmbedKey   string
}

func DiscoverNeo4jCSVSources(dir string, opts Options) (nodeSources []string, relSources []string, err error) {
	entries := make([]string, 0)
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			return nil
		}
		if isCSVLikePath(path) {
			entries = append(entries, path)
		}
		return nil
	})
	if err != nil {
		return nil, nil, &Error{ExitCode: ExitCSV, Message: "failed to scan Neo4j CSV directory", Err: err}
	}
	sort.Strings(entries)
	for _, path := range entries {
		kind, classifyErr := classifyNeo4jCSVFile(path, opts)
		if classifyErr != nil {
			return nil, nil, classifyErr
		}
		switch kind {
		case "node":
			nodeSources = append(nodeSources, path)
		case "relationship":
			relSources = append(relSources, path)
		}
	}
	if len(nodeSources) == 0 && len(relSources) == 0 {
		return nil, nil, unsupported("no Neo4j-compatible CSV files found in source directory")
	}
	if len(nodeSources) == 0 {
		return nil, nil, unsupported("source directory does not contain any node CSV files")
	}
	return nodeSources, relSources, nil
}

func ExportNeo4jCSV(engine storage.ExportableEngine, opts Neo4jCSVExportOptions) error {
	if opts.OutputDir == "" {
		return unsupported("output directory is required")
	}
	if opts.Delimiter == 0 {
		opts.Delimiter = ','
	}
	if opts.ArrayDelimiter == 0 {
		opts.ArrayDelimiter = ';'
	}
	if opts.VectorDelimiter == 0 {
		opts.VectorDelimiter = ';'
	}
	if opts.Quote == 0 {
		opts.Quote = '"'
	}
	if opts.Quote != '"' {
		return unsupported("custom quote characters are not supported for Neo4j CSV export")
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return err
	}

	nodes, err := engine.AllNodes()
	if err != nil {
		return err
	}
	edges, err := engine.AllEdges()
	if err != nil {
		return err
	}

	if err := exportNodesCSV(filepath.Join(opts.OutputDir, "nodes.csv"), nodes, opts); err != nil {
		return err
	}
	if len(edges) > 0 {
		if err := exportRelationshipsCSV(filepath.Join(opts.OutputDir, "relationships.csv"), edges, opts); err != nil {
			return err
		}
	}
	if err := exportNornicSchemaDefinition(filepath.Join(opts.OutputDir, Neo4jCSVNornicSchemaFileName), engine.GetSchema()); err != nil {
		return err
	}
	if err := exportSchemaCypher(filepath.Join(opts.OutputDir, Neo4jCSVSchemaFileName), engine.GetSchema()); err != nil {
		return err
	}
	return nil
}

func DefaultNeo4jCSVSchemaPath(dir string) string {
	return filepath.Join(dir, Neo4jCSVSchemaFileName)
}

func DefaultNeo4jCSVNornicSchemaPath(dir string) string {
	return filepath.Join(dir, Neo4jCSVNornicSchemaFileName)
}

func classifyNeo4jCSVFile(path string, opts Options) (string, error) {
	source, err := openCSVSource([]string{path}, opts.withDefaults())
	if err != nil {
		return "", err
	}
	defer source.Close()
	header, err := source.ReadHeader()
	if err != nil {
		return "", &Error{ExitCode: ExitCSV, Message: "failed to read CSV header", Err: err}
	}
	cols, err := parseHeader(header, false)
	if err == nil {
		hasStart := len(filterColumns(cols, kindStartID)) > 0
		hasEnd := len(filterColumns(cols, kindEndID)) > 0
		if hasStart && hasEnd {
			return "relationship", nil
		}
	}
	if _, err := parseHeader(header, true); err == nil {
		return "node", nil
	}
	return "", unsupported("unsupported CSV header in Neo4j source directory: " + path)
}

func exportNodesCSV(path string, nodes []*storage.Node, opts Neo4jCSVExportOptions) error {
	columns := inferNodeColumns(nodes)
	header := []string{":ID", ":LABEL"}
	for _, col := range columns {
		header = append(header, col.Header)
	}
	rows := make([][]string, 0, len(nodes))
	for _, node := range nodes {
		row := []string{string(node.ID), strings.Join(node.Labels, string(opts.ArrayDelimiter))}
		for _, col := range columns {
			row = append(row, formatNodeColumnValue(node, col, opts))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, header, rows, opts.Delimiter)
}

func exportRelationshipsCSV(path string, edges []*storage.Edge, opts Neo4jCSVExportOptions) error {
	columns := inferEdgeColumns(edges)
	header := []string{":ID", ":START_ID", ":END_ID", ":TYPE"}
	for _, col := range columns {
		header = append(header, col.Header)
	}
	rows := make([][]string, 0, len(edges))
	for _, edge := range edges {
		row := []string{string(edge.ID), string(edge.StartNode), string(edge.EndNode), edge.Type}
		for _, col := range columns {
			row = append(row, formatPropertyValue(edge.Properties[col.Property], col, opts))
		}
		rows = append(rows, row)
	}
	return writeCSV(path, header, rows, opts.Delimiter)
}

func inferNodeColumns(nodes []*storage.Node) []inferredColumn {
	props := make(map[string]inferredColumn)
	for _, node := range nodes {
		for key, value := range node.Properties {
			if key == "id" {
				continue
			}
			props[key] = mergeColumn(props[key], key, value)
		}
		for key, vector := range node.NamedEmbeddings {
			props[":EMBEDDING("+key+")"] = inferredColumn{
				Header:     ":EMBEDDING(" + key + ")",
				Kind:       "embedding",
				EmbedKey:   key,
				VectorDims: len(vector),
			}
		}
	}
	return sortColumns(props)
}

func inferEdgeColumns(edges []*storage.Edge) []inferredColumn {
	props := make(map[string]inferredColumn)
	for _, edge := range edges {
		for key, value := range edge.Properties {
			props[key] = mergeColumn(props[key], key, value)
		}
	}
	return sortColumns(props)
}

func mergeColumn(existing inferredColumn, key string, value any) inferredColumn {
	if existing.Header == "" {
		existing.Property = key
	}
	kind, scalarType, dims := inferValueShape(value)
	if existing.Kind == "" {
		existing.Kind = kind
		existing.ScalarType = scalarType
		existing.VectorDims = dims
	} else if existing.Kind != kind || existing.ScalarType != scalarType {
		existing.Kind = "property"
		existing.ScalarType = "string"
		existing.VectorDims = 0
	} else if dims > existing.VectorDims {
		existing.VectorDims = dims
	}
	switch existing.Kind {
	case "vector":
		existing.Header = fmt.Sprintf("%s:vector{coordinateType:float,dimensions:%d}", key, max(1, existing.VectorDims))
	case "array":
		existing.Header = fmt.Sprintf("%s:%s[]", key, defaultString(existing.ScalarType, "string"))
	default:
		existing.Header = fmt.Sprintf("%s:%s", key, defaultString(existing.ScalarType, "string"))
	}
	return existing
}

func inferValueShape(value any) (kind string, scalarType string, dims int) {
	switch v := value.(type) {
	case []float32:
		return "vector", "vector", len(v)
	case []float64:
		return "vector", "vector", len(v)
	case []any:
		if len(v) == 0 {
			return "array", "string", 0
		}
		if allFloatLike(v) {
			return "vector", "vector", len(v)
		}
		_, scalarType, _ = inferValueShape(v[0])
		if scalarType == "vector" {
			scalarType = "string"
		}
		return "array", scalarType, 0
	case []string:
		return "array", "string", 0
	case []bool:
		return "array", "boolean", 0
	case []int:
		return "array", "long", 0
	case []int64:
		return "array", "long", 0
	case string:
		return "property", "string", 0
	case bool:
		return "property", "boolean", 0
	case int, int8, int16, int32, int64:
		return "property", "long", 0
	case uint, uint8, uint16, uint32, uint64:
		return "property", "long", 0
	case float32, float64:
		return "property", "double", 0
	case time.Time:
		return "property", "datetime", 0
	default:
		rv := reflect.ValueOf(value)
		if rv.IsValid() && rv.Kind() == reflect.Slice {
			if rv.Len() == 0 {
				return "array", "string", 0
			}
			if allFloatLikeReflect(rv) {
				return "vector", "vector", rv.Len()
			}
			first := rv.Index(0).Interface()
			_, inferredScalar, _ := inferValueShape(first)
			if inferredScalar == "vector" {
				inferredScalar = "string"
			}
			return "array", inferredScalar, 0
		}
		return "property", "string", 0
	}
}

func sortColumns(cols map[string]inferredColumn) []inferredColumn {
	keys := make([]string, 0, len(cols))
	for key := range cols {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]inferredColumn, 0, len(keys))
	for _, key := range keys {
		out = append(out, cols[key])
	}
	return out
}

func formatNodeColumnValue(node *storage.Node, col inferredColumn, opts Neo4jCSVExportOptions) string {
	if col.Kind == "embedding" {
		return formatVector(node.NamedEmbeddings[col.EmbedKey], opts.VectorDelimiter)
	}
	return formatPropertyValue(node.Properties[col.Property], col, opts)
}

func formatPropertyValue(value any, col inferredColumn, opts Neo4jCSVExportOptions) string {
	if value == nil {
		return ""
	}
	switch col.Kind {
	case "vector":
		switch v := value.(type) {
		case []float32:
			return formatVector(v, opts.VectorDelimiter)
		case []float64:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, strconv.FormatFloat(item, 'f', -1, 64))
			}
			return strings.Join(parts, string(opts.VectorDelimiter))
		case []any:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, "double"))
			}
			return strings.Join(parts, string(opts.VectorDelimiter))
		default:
			return fmt.Sprint(value)
		}
	case "array":
		switch v := value.(type) {
		case []any:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, col.ScalarType))
			}
			return strings.Join(parts, string(opts.ArrayDelimiter))
		case []string:
			return strings.Join(v, string(opts.ArrayDelimiter))
		case []bool:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, col.ScalarType))
			}
			return strings.Join(parts, string(opts.ArrayDelimiter))
		case []int:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, col.ScalarType))
			}
			return strings.Join(parts, string(opts.ArrayDelimiter))
		case []int64:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, col.ScalarType))
			}
			return strings.Join(parts, string(opts.ArrayDelimiter))
		default:
			rv := reflect.ValueOf(value)
			if rv.IsValid() && rv.Kind() == reflect.Slice {
				parts := make([]string, 0, rv.Len())
				for i := 0; i < rv.Len(); i++ {
					parts = append(parts, formatScalar(rv.Index(i).Interface(), col.ScalarType))
				}
				return strings.Join(parts, string(opts.ArrayDelimiter))
			}
			return fmt.Sprint(value)
		}
	default:
		return formatScalar(value, col.ScalarType)
	}
}

func formatScalar(value any, scalarType string) string {
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 64)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case time.Time:
		if scalarType == "datetime" {
			return v.UTC().Format(time.RFC3339Nano)
		}
		return v.String()
	default:
		return fmt.Sprint(value)
	}
}

func formatVector(vector []float32, delim rune) string {
	parts := make([]string, 0, len(vector))
	for _, item := range vector {
		parts = append(parts, strconv.FormatFloat(float64(item), 'f', -1, 64))
	}
	return strings.Join(parts, string(delim))
}

func allFloatLike(values []any) bool {
	for _, value := range values {
		switch value.(type) {
		case float32, float64:
		default:
			return false
		}
	}
	return len(values) > 0
}

func allFloatLikeReflect(values reflect.Value) bool {
	if !values.IsValid() || values.Kind() != reflect.Slice || values.Len() == 0 {
		return false
	}
	for i := 0; i < values.Len(); i++ {
		switch values.Index(i).Kind() {
		case reflect.Float32, reflect.Float64:
		default:
			return false
		}
	}
	return true
}

func writeCSV(path string, header []string, rows [][]string, delimiter rune) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.Comma = delimiter
	if err := writer.Write(header); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func exportNornicSchemaDefinition(path string, schema *storage.SchemaManager) error {
	if schema == nil {
		return nil
	}
	def := schema.ExportDefinition()
	if !hasSchemaDefinitionContent(def) {
		return nil
	}
	data, err := json.MarshalIndent(def, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(path, data, 0o600)
}

func hasSchemaDefinitionContent(def *storage.SchemaDefinition) bool {
	if def == nil {
		return false
	}
	return len(def.Constraints) > 0 ||
		len(def.ConstraintContracts) > 0 ||
		len(def.PropertyTypeConstraints) > 0 ||
		len(def.PropertyIndexes) > 0 ||
		len(def.CompositeIndexes) > 0 ||
		len(def.FulltextIndexes) > 0 ||
		len(def.VectorIndexes) > 0 ||
		len(def.RangeIndexes) > 0 ||
		len(def.DecayProfileBundles) > 0 ||
		len(def.DecayProfileBindings) > 0 ||
		len(def.PromotionProfiles) > 0 ||
		len(def.PromotionPolicies) > 0
}

func exportSchemaCypher(path string, schema *storage.SchemaManager) error {
	if schema == nil {
		return nil
	}
	statements := make([]string, 0)
	for _, constraint := range schema.GetAllConstraints() {
		if stmt, ok := renderConstraintStatement(constraint); ok {
			statements = append(statements, stmt)
		}
	}
	for _, typeConstraint := range schema.GetAllPropertyTypeConstraints() {
		if stmt, ok := renderPropertyTypeConstraintStatement(typeConstraint); ok {
			statements = append(statements, stmt)
		}
	}
	for _, index := range schema.GetIndexes() {
		if stmt, ok := renderIndexStatement(index); ok {
			statements = append(statements, stmt)
		}
	}
	if len(statements) == 0 {
		return nil
	}
	sort.Strings(statements)
	contents := strings.Join(statements, ";\n") + ";\n"
	return os.WriteFile(path, []byte(contents), 0o600)
}

func renderConstraintStatement(c storage.Constraint) (string, bool) {
	entity := c.EffectiveEntityType()
	props := cypherPropertyRefs(entityVariable(entity), c.Properties)
	var pattern string
	var requirement string
	switch c.Type {
	case storage.ConstraintUnique:
		if len(props) != 1 {
			return "", false
		}
		pattern = cypherPattern(entity, c.Label)
		requirement = props[0] + " IS UNIQUE"
	case storage.ConstraintNodeKey:
		pattern = cypherPattern(entity, c.Label)
		requirement = "(" + strings.Join(props, ", ") + ") IS NODE KEY"
	case storage.ConstraintExists:
		if len(props) != 1 {
			return "", false
		}
		pattern = cypherPattern(entity, c.Label)
		requirement = props[0] + " IS NOT NULL"
	case storage.ConstraintRelationshipKey:
		pattern = cypherPattern(entity, c.Label)
		requirement = "(" + strings.Join(props, ", ") + ") IS RELATIONSHIP KEY"
	case storage.ConstraintTemporal:
		pattern = cypherPattern(entity, c.Label)
		requirement = "(" + strings.Join(props, ", ") + ") IS TEMPORAL NO OVERLAP"
	case storage.ConstraintDomain:
		if len(props) != 1 {
			return "", false
		}
		pattern = cypherPattern(entity, c.Label)
		requirement = props[0] + " IN [" + strings.Join(formatConstraintValues(c.AllowedValues), ", ") + "]"
	case storage.ConstraintCardinality:
		if entity != storage.ConstraintEntityRelationship {
			return "", false
		}
		pattern = cypherRelationshipPattern(c.Label, c.Direction, "", "")
		requirement = fmt.Sprintf("MAX COUNT %d", c.MaxCount)
	case storage.ConstraintPolicy:
		if entity != storage.ConstraintEntityRelationship {
			return "", false
		}
		pattern = cypherRelationshipPattern(c.Label, "OUTGOING", c.SourceLabel, c.TargetLabel)
		requirement = c.PolicyMode
	default:
		return "", false
	}
	return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR %s REQUIRE %s", quoteCypherIdent(c.Name), pattern, requirement), true
}

func renderPropertyTypeConstraintStatement(c storage.PropertyTypeConstraint) (string, bool) {
	entity := c.EffectiveEntityType()
	pattern := cypherPattern(entity, c.Label)
	ref := entityVariable(entity) + "." + quoteCypherIdent(c.Property)
	return fmt.Sprintf("CREATE CONSTRAINT %s IF NOT EXISTS FOR %s REQUIRE %s IS :: %s", quoteCypherIdent(c.Name), pattern, ref, c.ExpectedType), true
}

func renderIndexStatement(index any) (string, bool) {
	m, ok := index.(map[string]interface{})
	if !ok {
		return "", false
	}
	name, _ := m["name"].(string)
	typ, _ := m["type"].(string)
	label, _ := m["label"].(string)
	switch typ {
	case "PROPERTY", "COMPOSITE":
		properties := stringSliceValue(m["properties"])
		if len(properties) == 0 {
			return "", false
		}
		return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR %s ON (%s)", quoteCypherIdent(name), cypherPattern(storage.ConstraintEntityNode, label), strings.Join(cypherPropertyRefs("n", properties), ", ")), true
	case "RANGE":
		if owning, _ := m["owningConstraint"].(string); owning != "" {
			return "", false
		}
		entityType, _ := m["entityType"].(string)
		properties := stringSliceValue(m["properties"])
		if len(properties) == 0 {
			if property, _ := m["property"].(string); property != "" {
				properties = []string{property}
			}
		}
		if len(properties) == 0 {
			return "", false
		}
		entity := storage.ConstraintEntityNode
		if entityType == string(storage.ConstraintEntityRelationship) {
			entity = storage.ConstraintEntityRelationship
		}
		return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR %s ON (%s)", quoteCypherIdent(name), cypherPattern(entity, label), strings.Join(cypherPropertyRefs(entityVariable(entity), properties), ", ")), true
	case "FULLTEXT":
		properties := stringSliceValue(m["properties"])
		labels := stringSliceValue(m["labels"])
		relationshipTypes := stringSliceValue(m["relationshipTypes"])
		if len(properties) == 0 {
			return "", false
		}
		if len(labels) > 0 {
			parts := make([]string, 0, len(labels))
			for _, item := range labels {
				parts = append(parts, quoteCypherIdent(item))
			}
			return fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [%s]", quoteCypherIdent(name), strings.Join(parts, "|"), strings.Join(cypherPropertyRefs("n", properties), ", ")), true
		}
		if len(relationshipTypes) > 0 {
			parts := make([]string, 0, len(relationshipTypes))
			for _, item := range relationshipTypes {
				parts = append(parts, quoteCypherIdent(item))
			}
			return fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR ()-[r:%s]-() ON EACH [%s]", quoteCypherIdent(name), strings.Join(parts, "|"), strings.Join(cypherPropertyRefs("r", properties), ", ")), true
		}
		return "", false
	case "VECTOR":
		property, _ := m["property"].(string)
		dimensions, _ := m["dimensions"].(int)
		if dimensions == 0 {
			if value, ok := m["dimensions"].(float64); ok {
				dimensions = int(value)
			}
		}
		similarity, _ := m["similarityFunc"].(string)
		if property == "" || label == "" || dimensions == 0 {
			return "", false
		}
		entityType, _ := m["entityType"].(string)
		entity := storage.ConstraintEntityNode
		variable := "n"
		if entityType == string(storage.ConstraintEntityRelationship) {
			entity = storage.ConstraintEntityRelationship
			variable = "r"
		}
		options := fmt.Sprintf("OPTIONS {indexConfig: {`vector.dimensions`: %d", dimensions)
		if similarity != "" {
			options += fmt.Sprintf(", `vector.similarity_function`: '%s'", strings.ToLower(similarity))
		}
		options += "}}"
		return fmt.Sprintf("CREATE VECTOR INDEX %s IF NOT EXISTS FOR %s ON (%s) %s", quoteCypherIdent(name), cypherPattern(entity, label), strings.Join(cypherPropertyRefs(variable, []string{property}), ", "), options), true
	default:
		return "", false
	}
}

func cypherPattern(entity storage.ConstraintEntityType, label string) string {
	if entity == storage.ConstraintEntityRelationship {
		return fmt.Sprintf("()-[r:%s]-()", quoteCypherIdent(label))
	}
	return fmt.Sprintf("(n:%s)", quoteCypherIdent(label))
}

func cypherRelationshipPattern(label, direction, sourceLabel, targetLabel string) string {
	left := "()"
	right := "()"
	if sourceLabel != "" {
		left = fmt.Sprintf("(:%s)", quoteCypherIdent(sourceLabel))
	}
	if targetLabel != "" {
		right = fmt.Sprintf("(:%s)", quoteCypherIdent(targetLabel))
	}
	switch strings.ToUpper(direction) {
	case "INCOMING":
		return fmt.Sprintf("%s<-[r:%s]-%s", left, quoteCypherIdent(label), right)
	default:
		return fmt.Sprintf("%s-[r:%s]->%s", left, quoteCypherIdent(label), right)
	}
}

func entityVariable(entity storage.ConstraintEntityType) string {
	if entity == storage.ConstraintEntityRelationship {
		return "r"
	}
	return "n"
}

func cypherPropertyRefs(variable string, properties []string) []string {
	refs := make([]string, 0, len(properties))
	for _, property := range properties {
		refs = append(refs, variable+"."+quoteCypherIdent(property))
	}
	return refs
}

func quoteCypherIdent(value string) string {
	return "`" + strings.ReplaceAll(value, "`", "``") + "`"
}

func formatConstraintValues(values []interface{}) []string {
	formatted := make([]string, 0, len(values))
	for _, value := range values {
		switch v := value.(type) {
		case string:
			formatted = append(formatted, "'"+strings.ReplaceAll(v, "'", "\\'")+"'")
		case bool:
			formatted = append(formatted, formatScalar(v, "boolean"))
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
			formatted = append(formatted, formatScalar(v, "double"))
		default:
			formatted = append(formatted, "'"+strings.ReplaceAll(fmt.Sprint(v), "'", "\\'")+"'")
		}
	}
	return formatted
}

func stringSliceValue(value interface{}) []string {
	slice, ok := value.([]string)
	if ok {
		return slice
	}
	items, ok := value.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(items))
	for _, item := range items {
		if text, ok := item.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

func isCSVLikePath(path string) bool {
	return strings.HasSuffix(path, ".csv") || strings.HasSuffix(path, ".csv.gz") || strings.HasSuffix(path, ".zip")
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
