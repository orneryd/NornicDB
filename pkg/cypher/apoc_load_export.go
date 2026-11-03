// Package cypher implements APOC data import/export procedures.
package cypher

// Author: declared-list-owners lane (Claude), 2026-10-30: result headers come from procedure_result_columns.go.

import (
	"context"
	"encoding/csv"
	"fmt"
	"slices"
	"strconv"
	"strings"

	apocload "github.com/orneryd/nornicdb/apoc/load"
	"github.com/orneryd/nornicdb/pkg/apocio"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	securitypkg "github.com/orneryd/nornicdb/pkg/security"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// =============================================================================
// apoc.load.json - Load JSON data
// =============================================================================

// jsonLoadProcedure binds the public procedure name to the canonical
// DocumentItems row loader. Both registered names expose the decoded array as
// rows, or one row for any other JSON value; their argument errors retain the
// invoked name. This row shape is separate from the scalar Json document shape
// apoc.import.json and Go callers use.
func jsonLoadProcedure(procedure string) ProcedureHandler {
	loadItems := apocload.JSONItems(apocload.DocumentItems)
	return func(ctx context.Context, _ *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		source, err := requiredProcedureString(procedure, args, 0, "urlOrKeyOrBinary")
		if err != nil {
			return nil, err
		}
		items, err := loadItems(ctx, source)
		if err != nil {
			return nil, err
		}
		rows := make([][]interface{}, len(items))
		for i, item := range items {
			if i%pipelineMaterializedCancelCheck == 0 {
				if err := ctx.Err(); err != nil {
					return nil, err
				}
			}
			rows[i] = []interface{}{item}
		}
		return &ExecuteResult{Columns: columnNames(valueMapColumns), Rows: rows}, nil
	}
}

// =============================================================================
// apoc.load.csv - Load CSV data
// =============================================================================

// callApocLoadCsv loads CSV data from a URL or file path.
// Syntax: CALL apoc.load.csv(urlOrFile, {sep: ',', header: true}) YIELD lineNo, list, map
func (e *StorageExecutor) callApocLoadCsv(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	const procedure = "apoc.load.csv"
	urlOrFile, err := requiredProcedureString(procedure, args, 0, "urlOrBinary")
	if err != nil {
		return nil, err
	}
	// Options from the config map: header (default true) and sep (its
	// first character, default ',').
	config, err := optionalProcedureMap(procedure, args, 1, "config")
	if err != nil {
		return nil, err
	}
	hasHeader := true
	if header, ok := config["header"].(bool); ok {
		hasHeader = header
	}
	separator := ','
	if sep, ok := config["sep"].(string); ok && sep != "" {
		separator = []rune(sep)[0]
	}

	capability, err := apocio.CapabilityFrom(ctx)
	if err != nil {
		return nil, err
	}
	source, err := capability.Open(ctx, urlOrFile, apocio.Request{})
	if err != nil {
		if apocio.IsHTTPSource(urlOrFile) {
			return nil, fmt.Errorf("failed to fetch CSV: %w", err)
		}
		return nil, fmt.Errorf("failed to open CSV: %w", err)
	}
	defer source.Close()

	reader := csv.NewReader(source)

	reader.Comma = separator
	reader.LazyQuotes = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return &ExecuteResult{
			Columns: columnNames(apocLoadCsvColumns),
			Rows:    [][]interface{}{},
		}, nil
	}

	// Build result
	rows := [][]interface{}{}
	var headers []string

	startRow := 0
	if hasHeader && len(records) > 0 {
		headers = records[0]
		startRow = 1
	}

	for i := startRow; i < len(records); i++ {
		record := records[i]
		lineNo := i + 1

		// Build map if we have headers
		recordMap := make(map[string]interface{})
		if hasHeader && headers != nil {
			for j, header := range headers {
				if j < len(record) {
					recordMap[header] = record[j]
				}
			}
		}

		// Convert record to interface slice
		list := make([]interface{}, len(record))
		for j, v := range record {
			list[j] = v
		}

		rows = append(rows, []interface{}{lineNo, list, recordMap})
	}

	return &ExecuteResult{
		Columns: columnNames(apocLoadCsvColumns),
		Rows:    rows,
	}, nil
}

// =============================================================================
// apoc.export.* - one export lifecycle
// =============================================================================
//
// Author: PR1615 export-convergence lane, 2026-10-31: the four apoc.export.*
// procedures own one argument/source/destination lifecycle and keep only their
// format encoder and their public result row.

// exportScope is where one export reads its content: the whole graph, or the
// rows of the caller's query.
type exportScope int

const (
	exportGraphScope exportScope = iota
	exportQueryScope
)

// exportSource is one export's content: the graph read once, or the result of
// the caller's query.
type exportSource struct {
	nodes  []*storage.Node
	edges  []*storage.Edge
	result *ExecuteResult
}

// exportPayload is one encoded export: the content that goes to the export's
// destination, and the single result row the procedure returns for it. Each
// procedure owns its row because the public apoc.export.* columns differ.
type exportPayload struct {
	content []byte
	text    string // CSV retains its builder string; convert only when a file is written.
	row     []interface{}
}

// exportEncoder encodes one export's source in one format.
type exportEncoder func(e *StorageExecutor, ctx context.Context, filePath string, source exportSource) (exportPayload, error)

// exportDeclaration declares one public apoc.export.* procedure: the name its
// argument errors and result header use, where it reads its content, and how it
// encodes that content. The lifecycle every export shares -- its arguments, the
// caller's export destination, its source, its destination write and its result
// -- lives in exportProcedure; a declaration adds its encoder only.
type exportDeclaration struct {
	name   string
	scope  exportScope
	encode exportEncoder
}

// apoc.export.json.all and apoc.export.csv.all export the whole graph;
// apoc.export.json.query and apoc.export.csv.query export one query result.
var (
	exportJSONGraphDeclaration = exportDeclaration{name: "apoc.export.json.all", scope: exportGraphScope, encode: (*StorageExecutor).encodeJSONGraph}
	exportJSONQueryDeclaration = exportDeclaration{name: "apoc.export.json.query", scope: exportQueryScope, encode: (*StorageExecutor).encodeJSONQuery}
	exportCSVGraphDeclaration  = exportDeclaration{name: "apoc.export.csv.all", scope: exportGraphScope, encode: (*StorageExecutor).encodeCSVGraph}
	exportCSVQueryDeclaration  = exportDeclaration{name: "apoc.export.csv.query", scope: exportQueryScope, encode: (*StorageExecutor).encodeCSVQuery}
)

// exportProcedure binds one declaration directly to the canonical procedure
// lifecycle: validation, content, configured destination and result publication.
// There is no per-procedure forwarding method or second dispatch boundary.
func exportProcedure(declaration exportDeclaration) ProcedureHandler {
	return func(ctx context.Context, e *StorageExecutor, _ string, args []interface{}) (*ExecuteResult, error) {
		query := ""
		fileIndex := 0
		if declaration.scope == exportQueryScope {
			var err error
			query, err = requiredProcedureString(declaration.name, args, 0, "query")
			if err != nil {
				return nil, err
			}
			fileIndex = 1
		}
		// No file (or '') returns the export instead of writing it.
		filePath, err := optionalProcedureString(declaration.name, args, fileIndex, "file", "")
		if err != nil {
			return nil, err
		}

		// The destination resolves through the caller's APOC I/O capability, and
		// only when the caller named a file: a streamed export performs no file I/O
		// and needs no export path.
		resolvedPath := ""
		if filePath != "" {
			capability, err := apocio.CapabilityFrom(ctx)
			if err != nil {
				return nil, err
			}
			resolvedPath, err = capability.ExportPath(filePath)
			if err != nil {
				return nil, err
			}
		}

		source, err := e.readExportSource(ctx, declaration.scope, query)
		if err != nil {
			return nil, err
		}

		payload, err := declaration.encode(e, ctx, filePath, source)
		if err != nil {
			return nil, err
		}

		if err := e.writeExport(ctx, declaration.scope, resolvedPath, payload); err != nil {
			return nil, err
		}

		return &ExecuteResult{
			Columns: legacyProcedureColumns(declaration.name),
			Rows:    [][]interface{}{payload.row},
		}, nil
	}
}

// readExportSource reads one export's content once, on the caller's context:
// the result of the caller's query, or the whole graph. A relationship read that
// fails fails the export instead of exporting the nodes alone.
func (e *StorageExecutor) readExportSource(ctx context.Context, scope exportScope, query string) (exportSource, error) {
	if scope == exportQueryScope {
		result, err := e.executeUserStatement(ctx, query, nil)
		if err != nil {
			return exportSource{}, fmt.Errorf("failed to execute query: %w", err)
		}
		return exportSource{result: result}, nil
	}
	if err := ctx.Err(); err != nil {
		return exportSource{}, err
	}
	nodes, err := e.storage.AllNodes(ctx)
	if err != nil {
		return exportSource{}, fmt.Errorf("failed to read nodes: %w", err)
	}
	edges, err := e.storage.AllEdges(ctx)
	if err != nil {
		return exportSource{}, fmt.Errorf("failed to read relationships: %w", err)
	}
	return exportSource{nodes: nodes, edges: edges}, nil
}

// writeExport writes one encoded export to its resolved path. The whole-graph
// procedures create the file's parent directory; the query procedures write
// only into a directory the caller already has. A caller that has stopped
// performs no write and never receives a successful export.
func (e *StorageExecutor) writeExport(ctx context.Context, scope exportScope, path string, payload exportPayload) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if path == "" {
		return nil
	}
	if scope == exportGraphScope {
		if err := securitypkg.EnsureRootedParent(path, 0o755); err != nil {
			return fmt.Errorf("failed to create directory: %w", err)
		}
	}
	content := payload.content
	if content == nil {
		content = []byte(payload.text)
	}
	if err := securitypkg.WriteRootedFile(path, content, 0o644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}
	return ctx.Err()
}

// exportChunks observes the caller between pages of
// pipelineMaterializedCancelCheck items and hands each page to visit, so the work
// between two observations of an export is bounded by that stride however large
// its population is. The observation after the last page reports a caller that
// stopped during it.
func exportChunks(ctx context.Context, length int, visit func(start, end int) error) error {
	for start := 0; start < length; start += pipelineMaterializedCancelCheck {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + pipelineMaterializedCancelCheck
		if end > length {
			end = length
		}
		if err := visit(start, end); err != nil {
			return err
		}
	}
	return ctx.Err()
}

// =============================================================================
// apoc.export.json - Export to JSON
// =============================================================================

// encodeJSONGraph encodes the whole graph as the apoc.export.json.all document:
// its nodes, its relationships, their property count and the document body.
// Syntax: CALL apoc.export.json.all(file, config) YIELD file, nodes, relationships
func (e *StorageExecutor) encodeJSONGraph(ctx context.Context, filePath string, source exportSource) (exportPayload, error) {
	properties := 0
	nodes := make([]map[string]interface{}, 0, len(source.nodes))
	if err := exportChunks(ctx, len(source.nodes), func(start, end int) error {
		page := source.nodes[start:end]
		nodes = e.appendNodesToExportFormat(nodes, page)
		properties += e.countProperties(page, nil)
		return nil
	}); err != nil {
		return exportPayload{}, err
	}
	relationships := make([]map[string]interface{}, 0, len(source.edges))
	if err := exportChunks(ctx, len(source.edges), func(start, end int) error {
		page := source.edges[start:end]
		relationships = e.appendEdgesToExportFormat(relationships, page)
		properties += e.countProperties(nil, page)
		return nil
	}); err != nil {
		return exportPayload{}, err
	}

	content, err := valueconvert.PropertyJSON.MarshalIndent(map[string]interface{}{
		"nodes":         nodes,
		"relationships": relationships,
	}, "", "  ")
	if err != nil {
		return exportPayload{}, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	return exportPayload{
		content: content,
		row:     []interface{}{filePath, len(source.nodes), len(source.edges), properties, string(content)},
	}, nil
}

// encodeJSONQuery encodes one query result as the apoc.export.json.query
// document: its columns and its data.
// Syntax: CALL apoc.export.json.query(query, file, config)
func (e *StorageExecutor) encodeJSONQuery(ctx context.Context, filePath string, source exportSource) (exportPayload, error) {
	if err := ctx.Err(); err != nil {
		return exportPayload{}, err
	}
	content, err := valueconvert.PropertyJSON.MarshalIndent(map[string]interface{}{
		"columns": source.result.Columns,
		"data":    source.result.Rows,
	}, "", "  ")
	if err != nil {
		return exportPayload{}, fmt.Errorf("failed to marshal JSON: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return exportPayload{}, err
	}
	return exportPayload{
		content: content,
		row:     []interface{}{filePath, len(source.result.Rows), string(content)},
	}, nil
}

// =============================================================================
// apoc.export.csv - Export to CSV
// =============================================================================

// encodeCSVGraph encodes the whole graph as the apoc.export.csv.all sections:
// one node section and one relationship section.
// Syntax: CALL apoc.export.csv.all(file, config) YIELD file, nodes, relationships
func (e *StorageExecutor) encodeCSVGraph(ctx context.Context, filePath string, source exportSource) (exportPayload, error) {
	var csvContent strings.Builder

	csvContent.WriteString("# Nodes\n")
	csvContent.WriteString("_id,_labels,properties\n")
	if err := exportChunks(ctx, len(source.nodes), func(start, end int) error {
		for _, node := range source.nodes[start:end] {
			properties, err := valueconvert.PropertyJSON.Marshal(node.Properties)
			if err != nil {
				return fmt.Errorf("failed to marshal properties of node %s: %w", node.ID, err)
			}
			writeCSVField(&csvContent, string(node.ID), false)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, strings.Join(node.Labels, ";"), true)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, string(properties), true)
			csvContent.WriteByte('\n')
		}
		return nil
	}); err != nil {
		return exportPayload{}, err
	}

	csvContent.WriteString("\n# Relationships\n")
	csvContent.WriteString("_id,_type,_start,_end,properties\n")
	if err := exportChunks(ctx, len(source.edges), func(start, end int) error {
		for _, edge := range source.edges[start:end] {
			properties, err := valueconvert.PropertyJSON.Marshal(edge.Properties)
			if err != nil {
				return fmt.Errorf("failed to marshal properties of relationship %s: %w", edge.ID, err)
			}
			writeCSVField(&csvContent, string(edge.ID), false)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, edge.Type, false)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, string(edge.StartNode), false)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, string(edge.EndNode), false)
			csvContent.WriteByte(',')
			writeCSVField(&csvContent, string(properties), true)
			csvContent.WriteByte('\n')
		}
		return nil
	}); err != nil {
		return exportPayload{}, err
	}

	return exportPayload{
		text: csvContent.String(),
		row:  []interface{}{filePath, len(source.nodes), len(source.edges), len(source.nodes) + len(source.edges)},
	}, nil
}

// encodeCSVQuery encodes one query result as the apoc.export.csv.query rows: one
// header line from its columns, then one line per row.
// Syntax: CALL apoc.export.csv.query(query, file, config)
func (e *StorageExecutor) encodeCSVQuery(ctx context.Context, filePath string, source exportSource) (exportPayload, error) {
	var csvContent strings.Builder

	for i, column := range source.result.Columns {
		if i > 0 {
			csvContent.WriteByte(',')
		}
		writeCSVField(&csvContent, column, false)
	}
	csvContent.WriteByte('\n')

	if err := exportChunks(ctx, len(source.result.Rows), func(start, end int) error {
		for _, row := range source.result.Rows[start:end] {
			for i, val := range row {
				if i > 0 {
					csvContent.WriteByte(',')
				}
				switch v := val.(type) {
				case string:
					writeCSVField(&csvContent, v, true)
				case nil:
					// A null value keeps its empty cell.
				default:
					jsonVal, err := valueconvert.PropertyJSON.Marshal(v)
					if err != nil {
						return fmt.Errorf("failed to marshal column %s: %w", exportColumnName(source.result.Columns, i), err)
					}
					writeCSVField(&csvContent, string(jsonVal), true)
				}
			}
			csvContent.WriteByte('\n')
		}
		return nil
	}); err != nil {
		return exportPayload{}, err
	}

	content := csvContent.String()
	return exportPayload{
		text: content,
		row:  []interface{}{filePath, len(source.result.Rows), content},
	}, nil
}

// writeCSVField writes one CSV field into builder. The field is quoted when it
// is always quoted by the export (its labels, properties and query values keep
// the quoting the export has always published) or when its own bytes would
// otherwise move the field boundary: a comma, a quote, or a line break. A
// quoted field doubles its quotes. Nothing here copies the field first.
func writeCSVField(builder *strings.Builder, field string, alwaysQuoted bool) {
	if !alwaysQuoted && !strings.ContainsAny(field, csvFieldSpecials) {
		builder.WriteString(field)
		return
	}
	builder.WriteByte('"')
	for {
		quote := strings.IndexByte(field, '"')
		if quote < 0 {
			builder.WriteString(field)
			break
		}
		builder.WriteString(field[:quote])
		builder.WriteString(`""`)
		field = field[quote+1:]
	}
	builder.WriteByte('"')
}

// csvFieldSpecials are the bytes a CSV field cannot hold unquoted without a
// reader taking them as a field boundary or a quote.
const csvFieldSpecials = ",\"\r\n"

// exportColumnName names one column of a query result for an encoding error; a
// row may be wider than the columns the result declared.
func exportColumnName(columns []string, index int) string {
	if index < len(columns) {
		return fmt.Sprintf("%q", columns[index])
	}
	return fmt.Sprintf("#%d", index)
}

// =============================================================================
// Helper functions
// =============================================================================

func (e *StorageExecutor) appendNodesToExportFormat(result []map[string]interface{}, nodes []*storage.Node) []map[string]interface{} {
	start := len(result)
	result = slices.Grow(result, len(nodes))
	result = result[:start+len(nodes)]
	for i, node := range nodes {
		result[start+i] = map[string]interface{}{
			"id":         string(node.ID),
			"labels":     node.Labels,
			"properties": node.Properties,
		}
	}
	return result
}

func (e *StorageExecutor) appendEdgesToExportFormat(result []map[string]interface{}, edges []*storage.Edge) []map[string]interface{} {
	start := len(result)
	result = slices.Grow(result, len(edges))
	result = result[:start+len(edges)]
	for i, edge := range edges {
		result[start+i] = map[string]interface{}{
			"id":         string(edge.ID),
			"type":       edge.Type,
			"startNode":  string(edge.StartNode),
			"endNode":    string(edge.EndNode),
			"properties": edge.Properties,
		}
	}
	return result
}

func (e *StorageExecutor) countProperties(nodes []*storage.Node, edges []*storage.Edge) int {
	count := 0
	for _, node := range nodes {
		count += len(node.Properties)
	}
	for _, edge := range edges {
		count += len(edge.Properties)
	}
	return count
}

// =============================================================================
// apoc.load.jsonArray - Load JSON array with path
// =============================================================================

// =============================================================================
// apoc.import.json - Import JSON directly into graph
// =============================================================================

// callApocImportJson imports JSON graph data directly.
// Syntax: CALL apoc.import.json(urlOrFile) YIELD nodes, relationships
func (e *StorageExecutor) callApocImportJson(ctx context.Context, args []interface{}) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	// The storage owner qualifies local IDs through transaction/admission
	// wrappers too. Keep the import's supplied IDs local when it does so;
	// pre-prefixing here would make that namespace part of the imported ID.
	prefixIDs := storage.QualifiedEntityID(store, "") == ""
	dbName := GetUseDatabaseFromContext(ctx)
	if dbName == "" {
		dbName = "nornic"
	}
	ensureNodeID := func(id string) storage.NodeID {
		if id == "" {
			return ""
		}
		if !prefixIDs {
			return storage.NodeID(id)
		}
		return storage.NodeID(storage.EnsureDatabasePrefix(dbName, id))
	}
	ensureEdgeID := func(id string) storage.EdgeID {
		if id == "" {
			return ""
		}
		if !prefixIDs {
			return storage.EdgeID(id)
		}
		return storage.EdgeID(storage.EnsureDatabasePrefix(dbName, id))
	}
	urlOrFile, err := requiredProcedureString("apoc.import.json", args, 0, "url")
	if err != nil {
		return nil, err
	}

	if urlOrFile == "" {
		return nil, fmt.Errorf("apoc.import.json requires a URL or file path")
	}

	data, err := apocload.PropertyDocument(ctx, urlOrFile)
	if err != nil {
		return nil, err
	}

	// Import the data
	nodesImported := 0
	relsImported := 0

	if dataMap, ok := data.(map[string]interface{}); ok {
		// Import nodes
		if nodes, ok := dataMap["nodes"].([]interface{}); ok {
			for _, nodeData := range nodes {
				if nodeMap, ok := nodeData.(map[string]interface{}); ok {
					node := &storage.Node{
						Properties: make(map[string]interface{}),
					}

					if id, ok := nodeMap["id"].(string); ok {
						node.ID = ensureNodeID(id)
					}
					if labels, ok := nodeMap["labels"].([]interface{}); ok {
						for _, l := range labels {
							if ls, ok := l.(string); ok {
								node.Labels = append(node.Labels, ls)
							}
						}
					}
					if props, ok := nodeMap["properties"].(map[string]interface{}); ok {
						node.Properties = props
					}

					if node.ID != "" {
						if _, err := store.CreateNode(ctx, node); err == nil {
							nodesImported++
						}
					}
				}
			}
		}

		// Import relationships
		if rels, ok := dataMap["relationships"].([]interface{}); ok {
			for _, relData := range rels {
				if relMap, ok := relData.(map[string]interface{}); ok {
					edge := &storage.Edge{
						Properties: make(map[string]interface{}),
					}

					if id, ok := relMap["id"].(string); ok {
						edge.ID = ensureEdgeID(id)
					}
					if t, ok := relMap["type"].(string); ok {
						edge.Type = t
					}
					if start, ok := relMap["startNode"].(string); ok {
						edge.StartNode = ensureNodeID(start)
					}
					if end, ok := relMap["endNode"].(string); ok {
						edge.EndNode = ensureNodeID(end)
					}
					if props, ok := relMap["properties"].(map[string]interface{}); ok {
						edge.Properties = props
					}

					if edge.ID != "" && edge.StartNode != "" && edge.EndNode != "" {
						if err := store.CreateEdge(ctx, edge); err == nil {
							relsImported++
						}
					}
				}
			}
		}
	}

	return &ExecuteResult{
		Columns: legacyProcedureColumns("apoc.import.json"),
		Rows: [][]interface{}{{
			urlOrFile,
			nodesImported,
			relsImported,
		}},
	}, nil
}

// Make strconv available for any numeric conversions
var _ = strconv.Itoa
