package adminimport

import (
	"encoding/json"
	"github.com/orneryd/nornicdb/internal/cyphertext"

	"bufio"
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/localization"
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

// csvExportCell is one cell of an exported CSV file: the text the reader must
// decode, or absent when the entity holds no value for the column. The Neo4j CSV
// convention writes an absent property as an unquoted empty cell and a present
// empty string as the quoted empty cell "".
type csvExportCell struct {
	Text   string
	Absent bool
}

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
		return nil, nil, newImportError(ExitCSV, localization.AdminImportScanDirectoryFailed(), err)
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
		return nil, nil, unsupported(localization.AdminImportNoCSVFiles())
	}
	if len(nodeSources) == 0 {
		return nil, nil, unsupported(localization.AdminImportNoNodeCSVFiles())
	}
	return nodeSources, relSources, nil
}

func ExportNeo4jCSV(engine storage.Engine, opts Neo4jCSVExportOptions) error {
	if opts.OutputDir == "" {
		return unsupported(localization.AdminExportOutputDirectoryRequired())
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
		return unsupported(localization.AdminExportCustomQuoteUnsupported())
	}
	if err := os.MkdirAll(opts.OutputDir, 0o755); err != nil {
		return err
	}

	// An export holds every stored entity, those the visibility layer hides
	// from reads included.
	nodes, err := storage.CollectStoredNodes(context.Background(), engine)
	if err != nil {
		return err
	}
	edges, err := storage.CollectStoredEdges(context.Background(), engine)
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
		return "", newImportError(ExitCSV, localization.AdminImportReadHeaderFailed(), err)
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
	return "", unsupported(localization.AdminImportUnsupportedCSVHeader(path))
}

func exportNodesCSV(path string, nodes []*storage.Node, opts Neo4jCSVExportOptions) error {
	return writeEntityCSV(path, []string{":ID", ":LABEL"}, inferNodeColumns(nodes), nodes, opts,
		func(node *storage.Node, lead []csvExportCell) {
			lead[0].Text = string(node.ID)
			lead[1].Text = strings.Join(node.Labels, string(opts.ArrayDelimiter))
		}, nodePropertyCell)
}

func exportRelationshipsCSV(path string, edges []*storage.Edge, opts Neo4jCSVExportOptions) error {
	return writeEntityCSV(path, []string{":ID", ":START_ID", ":END_ID", ":TYPE"}, inferEdgeColumns(edges), edges, opts,
		func(edge *storage.Edge, lead []csvExportCell) {
			lead[0].Text, lead[1].Text = string(edge.ID), string(edge.StartNode)
			lead[2].Text, lead[3].Text = string(edge.EndNode), edge.Type
		}, func(edge *storage.Edge, col inferredColumn, opts Neo4jCSVExportOptions) csvExportCell {
			return propertyCell(edge.Properties[col.Property], col, opts)
		})
}

// writeEntityCSV owns column ordering and row construction for both entity
// kinds. Each row has one allocation; no second property-cell matrix is kept.
func writeEntityCSV[T any](path string, leadHeader []string, columns []inferredColumn, entities []T, opts Neo4jCSVExportOptions, lead func(T, []csvExportCell), property func(T, inferredColumn, Neo4jCSVExportOptions) csvExportCell) error {
	header := make([]string, 0, len(leadHeader)+len(columns))
	header = append(header, leadHeader...)
	for _, col := range columns {
		header = append(header, col.Header)
	}
	rows := make([][]csvExportCell, len(entities))
	for i, entity := range entities {
		row := make([]csvExportCell, len(leadHeader)+len(columns))
		lead(entity, row[:len(leadHeader)])
		for j, col := range columns {
			row[len(leadHeader)+j] = property(entity, col, opts)
		}
		rows[i] = row
	}
	return writeCSV(path, header, rows, opts.Delimiter)
}

func inferNodeColumns(nodes []*storage.Node) []inferredColumn {
	props := make(map[string]inferredColumn)
	for _, node := range nodes {
		for key, value := range node.Properties {
			if key == "id" {
				if s, ok := value.(string); ok && s == string(node.ID) {
					continue
				}
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
		return "array", "double", 0
	case []any:
		if len(v) == 0 {
			return "array", "string", 0
		}
		if floats, wide := floatListShape(v); floats {
			return floatListColumn(wide, len(v))
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
			if floats, wide := floatReflectShape(rv); floats {
				return floatListColumn(wide, rv.Len())
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

// nodePropertyCell is one property cell of a node: a named embedding column
// writes the embedding's coordinates, and every other column writes the stored
// property.
func nodePropertyCell(node *storage.Node, col inferredColumn, opts Neo4jCSVExportOptions) csvExportCell {
	if col.Kind == "embedding" {
		return csvExportCell{Text: formatVector(node.NamedEmbeddings[col.EmbedKey], opts.VectorDelimiter)}
	}
	return propertyCell(node.Properties[col.Property], col, opts)
}

// propertyCell is one property cell of an entity from its stored value: absent
// when the entity holds no value for the column.
func propertyCell(value any, col inferredColumn, opts Neo4jCSVExportOptions) csvExportCell {
	if value == nil {
		return csvExportCell{Absent: true}
	}
	return csvExportCell{Text: formatPropertyValue(value, col, opts)}
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
		case []float64:
			parts := make([]string, 0, len(v))
			for _, item := range v {
				parts = append(parts, formatScalar(item, col.ScalarType))
			}
			return strings.Join(parts, string(opts.ArrayDelimiter))
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

// floatListColumn is the export shape of a list of float coordinates. A list
// whose coordinates need 64 bits exports as double[], which the import parses
// back as float64 coordinates, so no coordinate is narrowed. A list of float32
// coordinates keeps the float coordinate vector type, which the import already
// reads back as float32.
func floatListColumn(wide bool, dims int) (kind string, scalarType string, vectorDims int) {
	if wide {
		return "array", "double", 0
	}
	return "vector", "vector", dims
}

// floatListShape reports whether a list holds only float coordinates, and
// whether any of them needs 64 bits.
func floatListShape(values []any) (floats bool, wide bool) {
	for _, value := range values {
		switch value.(type) {
		case float32:
		case float64:
			wide = true
		default:
			return false, false
		}
	}
	return len(values) > 0, wide
}

// floatReflectShape reports whether a reflected slice holds only float
// coordinates, and whether any of them needs 64 bits.
func floatReflectShape(values reflect.Value) (floats bool, wide bool) {
	if !values.IsValid() || values.Kind() != reflect.Slice || values.Len() == 0 {
		return false, false
	}
	for i := 0; i < values.Len(); i++ {
		switch values.Index(i).Kind() {
		case reflect.Float32:
		case reflect.Float64:
			wide = true
		default:
			return false, false
		}
	}
	return true, wide
}

// writeCSV writes one exported CSV file. encoding/csv owns field quoting,
// escaping, the delimiter and the multi-line grammar. The one cell it cannot
// write is a present empty string: the Neo4j CSV convention writes it as the
// quoted empty cell "" so that a reader keeps it distinct from the bare empty
// cell of an absent property, and encoding/csv never quotes an empty field.
// writeCSV therefore encodes each field on its own - the encoding of a field does
// not depend on the other fields of its record - and writes the quoted empty
// cell itself.
func writeCSV(path string, header []string, rows [][]csvExportCell, delimiter rune) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	out := bufio.NewWriter(file)
	writer := newCSVExportWriter(out, delimiter)
	if err := writer.writeRecord(csvExportHeader(header)); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.writeRecord(row); err != nil {
			return err
		}
	}
	return out.Flush()
}

// csvExportHeader is the header record of an exported file. A header cell names
// a column rather than holding a value, so every cell carries text.
func csvExportHeader(header []string) []csvExportCell {
	record := make([]csvExportCell, len(header))
	for i, name := range header {
		record[i] = csvExportCell{Text: name}
	}
	return record
}

// csvExportWriter writes exported CSV records.
type csvExportWriter struct {
	out       *bufio.Writer
	encode    *csv.Writer
	fields    csvFieldTailWriter
	field     [1]string
	delimiter rune
}

func newCSVExportWriter(out *bufio.Writer, delimiter rune) *csvExportWriter {
	w := &csvExportWriter{out: out, delimiter: delimiter}
	w.fields.out = out
	w.encode = csv.NewWriter(&w.fields)
	w.encode.Comma = delimiter
	return w
}

// csvFieldTailWriter streams one encoded CSV field to the export sink while
// withholding the field's final byte. encoding/csv terminates every encoded
// field with a record newline, and csvExportWriter owns the record terminator,
// so the withheld byte is dropped once encoding/csv has flushed the field.
// Streaming the field this way keeps a large value from being copied: each chunk
// encoding/csv produces goes straight to the sink, and WriteString forwards a
// string chunk without turning it into a byte slice.
type csvFieldTailWriter struct {
	out  io.Writer
	tail byte
	held bool
}

// The adapter is a genuine io.Writer and io.StringWriter, so bufio hands it a
// large field string instead of converting that string to a byte slice.
var (
	_ io.Writer       = (*csvFieldTailWriter)(nil)
	_ io.StringWriter = (*csvFieldTailWriter)(nil)
)

// Write streams one byte chunk of an encoded field, withholding its final byte.
func (w *csvFieldTailWriter) Write(p []byte) (int, error) {
	return writeWithheldTail(w, p, func(body []byte) (int, error) {
		return w.out.Write(body)
	})
}

// WriteString streams one string chunk of an encoded field, withholding its
// final byte.
func (w *csvFieldTailWriter) WriteString(s string) (int, error) {
	return writeWithheldTail(w, s, func(body string) (int, error) {
		return io.WriteString(w.out, body)
	})
}

// withheldTailChunk is one chunk of an encoded field: the byte slice Write
// receives or the string WriteString receives.
type withheldTailChunk interface {
	~[]byte | ~string
}

// writeWithheldTail is the one withheld-tail state machine behind Write and
// WriteString. It forwards chunk's leading bytes and retains its final byte,
// which may be the record newline encoding/csv appends to the field. A later
// chunk releases the retained byte first: more data arrived, so that byte is
// field text. An empty chunk changes nothing, and the byte count a caller is
// told covers the bytes this adapter consumed, retained final byte included.
func writeWithheldTail[T withheldTailChunk](w *csvFieldTailWriter, chunk T, stream func(T) (int, error)) (int, error) {
	size := len(chunk)
	if size == 0 {
		return 0, nil
	}
	if w.held {
		if err := w.releaseTail(); err != nil {
			return 0, err
		}
	}
	if size == 1 {
		w.tail = chunk[0]
		w.held = true
		return 1, nil
	}
	forwarded, err := stream(chunk[:size-1])
	if err == nil && forwarded < size-1 {
		// A sink that reports fewer bytes than it was handed fails the field.
		err = io.ErrShortWrite
	}
	if err != nil {
		return forwarded, err
	}
	w.tail = chunk[size-1]
	w.held = true
	return size, nil
}

// releaseTail forwards the byte withheld from an earlier chunk before the
// current chunk's bytes, so the field reaches the sink in order.
func (w *csvFieldTailWriter) releaseTail() error {
	if bw, ok := w.out.(io.ByteWriter); ok {
		if err := bw.WriteByte(w.tail); err != nil {
			return err
		}
	} else {
		n, err := w.out.Write([]byte{w.tail})
		if err != nil {
			return err
		}
		if n != 1 {
			return io.ErrShortWrite
		}
	}
	w.held = false
	return nil
}

// discardTail drops the withheld final byte of the field encoding/csv flushed:
// it is the field's record newline, which writeRecord writes itself.
func (w *csvFieldTailWriter) discardTail() {
	w.held = false
	w.tail = 0
}

// writeRecord writes one record: an absent cell stays a bare empty cell, a
// present empty string becomes the quoted empty cell, and every other value is
// written as encoding/csv encodes it.
func (w *csvExportWriter) writeRecord(record []csvExportCell) error {
	for i, cell := range record {
		if i > 0 {
			if _, err := w.out.WriteRune(w.delimiter); err != nil {
				return err
			}
		}
		if err := w.writeCell(cell); err != nil {
			return err
		}
	}
	return w.out.WriteByte('\n')
}

func (w *csvExportWriter) writeCell(cell csvExportCell) error {
	if cell.Absent {
		return nil
	}
	if cell.Text == "" {
		_, err := w.out.WriteString(`""`)
		return err
	}
	return w.encodeField(cell.Text)
}

// encodeField streams encoding/csv's encoding of one field into the record. The
// field's own record newline is dropped: writeRecord writes the record
// terminator itself.
func (w *csvExportWriter) encodeField(text string) error {
	w.field[0] = text
	if err := w.encode.Write(w.field[:]); err != nil {
		return err
	}
	w.encode.Flush()
	if err := w.encode.Error(); err != nil {
		return err
	}
	w.fields.discardTail()
	return nil
}

func exportNornicSchemaDefinition(path string, schema *storage.SchemaManager) error {
	if schema == nil {
		return nil
	}
	// Admin export is an offline operation that owns its own lifetime, so the
	// definition snapshot runs under it rather than a caller request.
	def, err := schema.ExportDefinition(context.Background())
	if err != nil {
		return err
	}
	if !hasSchemaDefinitionContent(def) {
		return nil
	}
	// jsontext.Marshal owns exact string identity; encoding/json.Indent reformats
	// those valid bytes without decoding, so escaped lone UTF16 units survive.
	data, err := storage.EncodeSchemaDefinition(def)
	if err != nil {
		return err
	}
	var indented bytes.Buffer
	if err := json.Indent(&indented, data, "", "  "); err != nil {
		return err
	}
	data = indented.Bytes()
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

// constraintStatementRequest names one constraint origin for the canonical
// renderer. general carries a stored general constraint and typeConstraint is
// nil; typeConstraint carries a stored property-type constraint and general is
// the zero value. The origins stay distinct because a general constraint of
// type PROPERTY_TYPE carries no expected type to render.
type constraintStatementRequest struct {
	general        storage.Constraint
	typeConstraint *storage.PropertyTypeConstraint
}

// constraintStatementFormat renders the maintained CREATE CONSTRAINT prefix
// shared by every constraint kind.
const constraintStatementFormat = "CREATE CONSTRAINT %s IF NOT EXISTS FOR %s REQUIRE %s"

// constraintPropertyTypeSuffix completes a property-type requirement after the
// rendered property reference with its IS :: type.
const constraintPropertyTypeSuffix = " IS :: %s"

func exportSchemaCypher(path string, schema *storage.SchemaManager) error {
	if schema == nil {
		return nil
	}
	statements := make([]string, 0)
	for _, constraint := range schema.GetAllConstraints() {
		if stmt, ok := renderConstraintStatement(constraintStatementRequest{general: constraint}); ok {
			statements = append(statements, stmt)
		}
	}
	for _, typeConstraint := range schema.GetAllPropertyTypeConstraints() {
		if stmt, ok := renderConstraintStatement(constraintStatementRequest{typeConstraint: &typeConstraint}); ok {
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

// renderConstraintStatement renders the CREATE CONSTRAINT statement for one
// constraint origin. A property-type request always renders, exactly as the
// property-type export always has; a general request keeps every kind's
// validation and admission rule and rejects PROPERTY_TYPE, which has no
// expected type in the general constraint payload.
func renderConstraintStatement(request constraintStatementRequest) (string, bool) {
	var name, pattern, requirement string
	format := constraintStatementFormat
	var args [4]any
	count := 3
	if typeConstraint := request.typeConstraint; typeConstraint != nil {
		entity := typeConstraint.EffectiveEntityType()
		name = typeConstraint.Name
		pattern = cypherPattern(entity, typeConstraint.Label)
		requirement = entityVariable(entity) + "." + cyphertext.PrintIdentifier(typeConstraint.Property, nil, cyphertext.RawSource)
		format = constraintStatementFormat + constraintPropertyTypeSuffix
		args[3] = typeConstraint.ExpectedType
		count = 4
	} else {
		c := request.general
		entity := c.EffectiveEntityType()
		props := cypherPropertyRefs(entityVariable(entity), c.Properties)
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
			keys, startProp, endProp, _ := storage.TemporalKeyAndWindow(c.Properties)
			pattern = cypherPattern(entity, c.Label)
			format = constraintStatementFormat
			requirement = "temporal.epoch(" + strings.Join(cypherPropertyRefs(entityVariable(entity), []string{startProp, endProp}), ", ") + ")"
			if len(keys) > 0 {
				requirement += " GROUP BY " + strings.Join(cypherPropertyRefs(entityVariable(entity), keys), ", ")
			}
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

		name = c.Name
	}
	args[0] = cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource)
	args[1] = pattern
	args[2] = requirement
	return fmt.Sprintf(format, args[:count]...), true
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
		return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR %s ON (%s)", cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource), cypherPattern(storage.ConstraintEntityNode, label), strings.Join(cypherPropertyRefs("n", properties), ", ")), true
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
		return fmt.Sprintf("CREATE INDEX %s IF NOT EXISTS FOR %s ON (%s)", cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource), cypherPattern(entity, label), strings.Join(cypherPropertyRefs(entityVariable(entity), properties), ", ")), true
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
				parts = append(parts, cyphertext.PrintIdentifier(item, nil, cyphertext.RawSource))
			}
			return fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR (n:%s) ON EACH [%s]", cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource), strings.Join(parts, "|"), strings.Join(cypherPropertyRefs("n", properties), ", ")), true
		}
		if len(relationshipTypes) > 0 {
			parts := make([]string, 0, len(relationshipTypes))
			for _, item := range relationshipTypes {
				parts = append(parts, cyphertext.PrintIdentifier(item, nil, cyphertext.RawSource))
			}
			return fmt.Sprintf("CREATE FULLTEXT INDEX %s IF NOT EXISTS FOR ()-[r:%s]-() ON EACH [%s]", cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource), strings.Join(parts, "|"), strings.Join(cypherPropertyRefs("r", properties), ", ")), true
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
		return fmt.Sprintf("CREATE VECTOR INDEX %s IF NOT EXISTS FOR %s ON (%s) %s", cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource), cypherPattern(entity, label), strings.Join(cypherPropertyRefs(variable, []string{property}), ", "), options), true
	default:
		return "", false
	}
}

func cypherPattern(entity storage.ConstraintEntityType, label string) string {
	if entity == storage.ConstraintEntityRelationship {
		return fmt.Sprintf("()-[r:%s]-()", cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource))
	}
	return fmt.Sprintf("(n:%s)", cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource))
}

func cypherRelationshipPattern(label, direction, sourceLabel, targetLabel string) string {
	left := "()"
	right := "()"
	if sourceLabel != "" {
		left = fmt.Sprintf("(:%s)", cyphertext.PrintIdentifier(sourceLabel, nil, cyphertext.RawSource))
	}
	if targetLabel != "" {
		right = fmt.Sprintf("(:%s)", cyphertext.PrintIdentifier(targetLabel, nil, cyphertext.RawSource))
	}
	switch strings.ToUpper(direction) {
	case "INCOMING":
		return fmt.Sprintf("%s<-[r:%s]-%s", left, cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource), right)
	default:
		return fmt.Sprintf("%s-[r:%s]->%s", left, cyphertext.PrintIdentifier(label, nil, cyphertext.RawSource), right)
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
		refs = append(refs, variable+"."+cyphertext.PrintIdentifier(property, nil, cyphertext.RawSource))
	}
	return refs
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
