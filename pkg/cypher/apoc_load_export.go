// Package cypher implements APOC data import/export procedures.
package cypher

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
)

var (
	errAPOCLocalImportFileAccessDisabled = errors.New("local APOC import file access is disabled")
	errAPOCLocalExportFileAccessDisabled = errors.New("local APOC export file access is disabled")
	errAPOCRemoteURLAccessDisabled       = errors.New("remote APOC URL access is disabled")
	errAPOCFileAuthorityNotAllowed       = errors.New("file URL may not contain an authority section (i.e. it should be 'file:///')")
	errAPOCFileQueryNotAllowed           = errors.New("file URL may not contain a query component")
	errAPOCFileFragmentNotAllowed        = errors.New("file URL may not contain a fragment component")
)

var apocRemoteHTTPClient = &http.Client{
	Timeout: 10 * time.Second,
	CheckRedirect: func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	},
	Transport: &http.Transport{
		Proxy: nil,
	},
}

func isHTTPSource(path string) bool {
	return strings.HasPrefix(path, "http://") || strings.HasPrefix(path, "https://")
}

func (e *StorageExecutor) ensureLocalAPOCImportFileAccessAllowed() error {
	if e != nil && e.allowLocalAPOCImportFileAccess {
		return nil
	}
	return errAPOCLocalImportFileAccessDisabled

}

func (e *StorageExecutor) ensureLocalAPOCExportFileAccessAllowed() error {
	if e != nil && e.allowLocalAPOCExportFileAccess {
		return nil
	}
	return errAPOCLocalExportFileAccessDisabled

}

func (e *StorageExecutor) ensureRemoteAPOCURLAccessAllowed() error {
	if e != nil && e.allowRemoteAPOCURLAccess {
		return nil
	}
	return errAPOCRemoteURLAccessDisabled
}

func (e *StorageExecutor) resolveAPOCLocalImportFilePath(source string) (string, error) {
	return e.resolveAPOCLocalFilePath(source, e.ensureLocalAPOCImportFileAccessAllowed)
}

func (e *StorageExecutor) resolveAPOCLocalExportFilePath(source string) (string, error) {
	return e.resolveAPOCLocalFilePath(source, e.ensureLocalAPOCExportFileAccessAllowed)
}

func (e *StorageExecutor) resolveAPOCLocalFilePath(source string, ensureAllowed func() error) (string, error) {
	if err := ensureAllowed(); err != nil {
		return "", err
	}

	parsed, err := url.Parse(source)
	if err == nil && parsed.Scheme != "" {
		switch strings.ToLower(parsed.Scheme) {
		case "file":
			if err := validateAPOCFileURL(parsed); err != nil {
				return "", err
			}
			localPath := parsed.Path
			if localPath == "" {
				localPath = parsed.Opaque
			}
			return e.rebaseAPOCImportPath(localPath)
		case "http", "https":
			return "", fmt.Errorf("expected local APOC file source, got remote URL")
		default:
			return "", fmt.Errorf("unsupported APOC file URL scheme %q", parsed.Scheme)
		}
	}

	return e.rebaseAPOCImportPath(source)
}

func validateAPOCFileURL(uri *url.URL) error {
	if uri.Host != "" {
		return errAPOCFileAuthorityNotAllowed
	}
	if uri.RawQuery != "" {
		return errAPOCFileQueryNotAllowed
	}
	if uri.Fragment != "" {
		return errAPOCFileFragmentNotAllowed
	}
	return nil
}

func (e *StorageExecutor) rebaseAPOCImportPath(localPath string) (string, error) {
	root := strings.TrimSpace(e.apocLocalFileAccessRoot)
	if root == "" {
		return filepath.Clean(localPath), nil
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("resolve APOC import root: %w", err)
	}

	relativePath := normalizeAPOCImportRelativePath(localPath)
	target := absRoot
	if relativePath != "" {
		target = filepath.Join(absRoot, filepath.FromSlash(relativePath))
	}
	target, err = filepath.Abs(target)
	if err != nil {
		return "", fmt.Errorf("resolve APOC local file path: %w", err)
	}
	if target != absRoot && !strings.HasPrefix(target, absRoot+string(os.PathSeparator)) {
		return "", fmt.Errorf("file URL points outside configured import directory")
	}
	return target, nil
}

func normalizeAPOCImportRelativePath(localPath string) string {
	slashPath := strings.ReplaceAll(localPath, "\\", "/")
	cleaned := pathpkg.Clean("/" + strings.TrimLeft(slashPath, "/"))
	return strings.TrimPrefix(cleaned, "/")
}

func validateAPOCRemoteURL(rawURL string) (*url.URL, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid APOC remote URL: %w", err)
	}
	if !strings.EqualFold(parsed.Scheme, "http") && !strings.EqualFold(parsed.Scheme, "https") {
		return nil, fmt.Errorf("unsupported APOC remote URL scheme %q", parsed.Scheme)
	}
	if parsed.Host == "" {
		return nil, fmt.Errorf("APOC remote URL host is required")
	}
	if parsed.User != nil {
		return nil, fmt.Errorf("APOC remote URLs may not contain userinfo")
	}
	if parsed.Fragment != "" {
		return nil, fmt.Errorf("APOC remote URLs may not contain fragments")
	}
	hostname := parsed.Hostname()
	if hostname == "" {
		return nil, fmt.Errorf("APOC remote URL host is required")
	}
	ipList, err := net.DefaultResolver.LookupIPAddr(context.Background(), hostname)
	if err != nil {
		return nil, fmt.Errorf("resolve APOC remote host: %w", err)
	}
	for _, ipAddr := range ipList {
		if ip := ipAddr.IP; ip != nil {
			if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
				return nil, fmt.Errorf("APOC remote URL host resolves to a disallowed address")
			}
		}
	}
	return parsed, nil
}

// =============================================================================
// apoc.load.json - Load JSON data
// =============================================================================

// callApocLoadJson loads JSON data from a URL or file path.
// Syntax: CALL apoc.load.json(urlOrFile) YIELD value
func (e *StorageExecutor) callApocLoadJson(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Parse the URL/file argument
	urlOrFile := e.extractApocLoadArg(cypher, "JSON")
	if urlOrFile == "" {
		return nil, fmt.Errorf("apoc.load.json requires a URL or file path")
	}

	var data interface{}
	var err error

	// Check if it's a URL or file path
	if isHTTPSource(urlOrFile) {
		data, err = e.loadJsonFromURL(urlOrFile)
	} else {
		data, err = e.loadJsonFromFile(urlOrFile)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load JSON: %w", err)
	}

	// Return the data
	rows := [][]interface{}{}

	switch v := data.(type) {
	case []interface{}:
		// Array of items - return each as a row
		for _, item := range v {
			rows = append(rows, []interface{}{item})
		}
	case map[string]interface{}:
		// Single object
		rows = append(rows, []interface{}{v})
	default:
		rows = append(rows, []interface{}{data})
	}

	return &ExecuteResult{
		Columns: []string{"value"},
		Rows:    rows,
	}, nil
}

func (e *StorageExecutor) loadJsonFromURL(url string) (interface{}, error) {
	if err := e.ensureRemoteAPOCURLAccessAllowed(); err != nil {
		return nil, err
	}
	parsed, err := validateAPOCRemoteURL(url)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, parsed.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := apocRemoteHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP error: %s", resp.Status)
	}

	var data interface{}
	if err := json.NewDecoder(resp.Body).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

func (e *StorageExecutor) loadJsonFromFile(path string) (interface{}, error) {
	resolvedPath, err := e.resolveAPOCLocalImportFilePath(path)
	if err != nil {
		return nil, err
	}
	file, err := os.Open(resolvedPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data interface{}
	if err := json.NewDecoder(file).Decode(&data); err != nil {
		return nil, err
	}

	return data, nil
}

// =============================================================================
// apoc.load.csv - Load CSV data
// =============================================================================

// callApocLoadCsv loads CSV data from a URL or file path.
// Syntax: CALL apoc.load.csv(urlOrFile, {sep: ',', header: true}) YIELD lineNo, list, map
func (e *StorageExecutor) callApocLoadCsv(ctx context.Context, cypher string) (*ExecuteResult, error) {
	urlOrFile := e.extractApocLoadArg(cypher, "CSV")
	if urlOrFile == "" {
		return nil, fmt.Errorf("apoc.load.csv requires a URL or file path")
	}

	// Parse options
	hasHeader := true
	separator := ','

	// Check for header option
	if strings.Contains(strings.ToUpper(cypher), "HEADER:") {
		if strings.Contains(strings.ToUpper(cypher), "HEADER: FALSE") ||
			strings.Contains(strings.ToUpper(cypher), "HEADER:FALSE") {
			hasHeader = false
		}
	}

	// Check for separator option
	if idx := strings.Index(cypher, "sep:"); idx > 0 {
		remainder := cypher[idx+4:]
		remainder = strings.TrimSpace(remainder)
		if len(remainder) > 0 {
			sepChar := remainder[0]
			if sepChar == '\'' || sepChar == '"' {
				if len(remainder) > 2 {
					separator = rune(remainder[1])
				}
			}
		}
	}

	var reader *csv.Reader
	var closer io.Closer

	// Load from URL or file
	if isHTTPSource(urlOrFile) {
		if err := e.ensureRemoteAPOCURLAccessAllowed(); err != nil {
			return nil, err
		}
		parsed, err := validateAPOCRemoteURL(urlOrFile)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsed.String(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := apocRemoteHTTPClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch CSV: %w", err)
		}
		closer = resp.Body
		reader = csv.NewReader(resp.Body)
	} else {
		resolvedPath, err := e.resolveAPOCLocalImportFilePath(urlOrFile)
		if err != nil {
			return nil, err
		}
		file, err := os.Open(resolvedPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open CSV: %w", err)
		}
		closer = file
		reader = csv.NewReader(file)
	}
	defer closer.Close()

	reader.Comma = separator
	reader.LazyQuotes = true

	// Read all records
	records, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV: %w", err)
	}

	if len(records) == 0 {
		return &ExecuteResult{
			Columns: []string{"lineNo", "list", "map"},
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
		Columns: []string{"lineNo", "list", "map"},
		Rows:    rows,
	}, nil
}

// =============================================================================
// apoc.export.json - Export to JSON
// =============================================================================

// callApocExportJson exports graph data to JSON.
// Syntax: CALL apoc.export.json.all(file, config) YIELD file, nodes, relationships
func (e *StorageExecutor) callApocExportJsonAll(ctx context.Context, cypher string) (*ExecuteResult, error) {
	filePath := e.extractApocExportArg(cypher, "JSON")
	resolvedPath := ""
	if filePath != "" {
		var err error
		resolvedPath, err = e.resolveAPOCLocalExportFilePath(filePath)
		if err != nil {
			return nil, err
		}
	}

	nodes := e.storage.GetAllNodes()
	edges, _ := e.storage.AllEdges()

	export := map[string]interface{}{
		"nodes":         e.nodesToExportFormat(nodes),
		"relationships": e.edgesToExportFormat(edges),
	}

	jsonData, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Write to file if path provided
	if resolvedPath != "" {
		if err := os.MkdirAll(filepath.Dir(resolvedPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(resolvedPath, jsonData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
	}

	return &ExecuteResult{
		Columns: []string{"file", "nodes", "relationships", "properties", "data"},
		Rows: [][]interface{}{{
			filePath,
			len(nodes),
			len(edges),
			e.countProperties(nodes, edges),
			string(jsonData),
		}},
	}, nil
}

// callApocExportJsonQuery exports query results to JSON.
// Syntax: CALL apoc.export.json.query(query, file, config)
func (e *StorageExecutor) callApocExportJsonQuery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Extract the query to execute
	query := e.extractApocExportQuery(cypher)
	filePath := e.extractApocExportArg(cypher, "JSON")
	resolvedPath := ""
	if filePath != "" {
		var err error
		resolvedPath, err = e.resolveAPOCLocalExportFilePath(filePath)
		if err != nil {
			return nil, err
		}
	}

	if query == "" {
		return nil, fmt.Errorf("apoc.export.json.query requires a query")
	}

	// Execute the query
	result, err := e.executeInternal(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Convert to JSON
	export := map[string]interface{}{
		"columns": result.Columns,
		"data":    result.Rows,
	}

	jsonData, err := json.MarshalIndent(export, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal JSON: %w", err)
	}

	// Write to file if path provided
	if resolvedPath != "" {
		if err := os.WriteFile(resolvedPath, jsonData, 0644); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
	}

	return &ExecuteResult{
		Columns: []string{"file", "rows", "data"},
		Rows: [][]interface{}{{
			filePath,
			len(result.Rows),
			string(jsonData),
		}},
	}, nil
}

// =============================================================================
// apoc.export.csv - Export to CSV
// =============================================================================

// callApocExportCsvAll exports all data to CSV.
// Syntax: CALL apoc.export.csv.all(file, config) YIELD file, nodes, relationships
func (e *StorageExecutor) callApocExportCsvAll(ctx context.Context, cypher string) (*ExecuteResult, error) {
	filePath := e.extractApocExportArg(cypher, "CSV")
	resolvedPath := ""
	if filePath != "" {
		var err error
		resolvedPath, err = e.resolveAPOCLocalExportFilePath(filePath)
		if err != nil {
			return nil, err
		}
	}

	nodes := e.storage.GetAllNodes()
	edges, _ := e.storage.AllEdges()

	// Build CSV content
	var csvContent strings.Builder

	// Nodes section
	csvContent.WriteString("# Nodes\n")
	csvContent.WriteString("_id,_labels,properties\n")
	for _, node := range nodes {
		props, _ := json.Marshal(node.Properties)
		csvContent.WriteString(fmt.Sprintf("%s,\"%s\",\"%s\"\n",
			node.ID,
			strings.Join(node.Labels, ";"),
			strings.ReplaceAll(string(props), "\"", "\"\"")))
	}

	csvContent.WriteString("\n# Relationships\n")
	csvContent.WriteString("_id,_type,_start,_end,properties\n")
	for _, edge := range edges {
		props, _ := json.Marshal(edge.Properties)
		csvContent.WriteString(fmt.Sprintf("%s,%s,%s,%s,\"%s\"\n",
			edge.ID,
			edge.Type,
			edge.StartNode,
			edge.EndNode,
			strings.ReplaceAll(string(props), "\"", "\"\"")))
	}

	content := csvContent.String()

	// Write to file if path provided
	if resolvedPath != "" {
		if err := os.MkdirAll(filepath.Dir(resolvedPath), 0755); err != nil {
			return nil, fmt.Errorf("failed to create directory: %w", err)
		}
		if err := os.WriteFile(resolvedPath, []byte(content), 0644); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
	}

	return &ExecuteResult{
		Columns: []string{"file", "nodes", "relationships", "rows"},
		Rows: [][]interface{}{{
			filePath,
			len(nodes),
			len(edges),
			len(nodes) + len(edges),
		}},
	}, nil
}

// callApocExportCsvQuery exports query results to CSV.
// Syntax: CALL apoc.export.csv.query(query, file, config)
func (e *StorageExecutor) callApocExportCsvQuery(ctx context.Context, cypher string) (*ExecuteResult, error) {
	query := e.extractApocExportQuery(cypher)
	filePath := e.extractApocExportArg(cypher, "CSV")
	resolvedPath := ""
	if filePath != "" {
		var err error
		resolvedPath, err = e.resolveAPOCLocalExportFilePath(filePath)
		if err != nil {
			return nil, err
		}
	}

	if query == "" {
		return nil, fmt.Errorf("apoc.export.csv.query requires a query")
	}

	// Execute the query
	result, err := e.executeInternal(ctx, query, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}

	// Build CSV
	var csvContent strings.Builder

	// Header
	csvContent.WriteString(strings.Join(result.Columns, ","))
	csvContent.WriteByte('\n')

	// Data rows
	for _, row := range result.Rows {
		values := make([]string, len(row))
		for i, val := range row {
			switch v := val.(type) {
			case string:
				values[i] = fmt.Sprintf("\"%s\"", strings.ReplaceAll(v, "\"", "\"\""))
			case nil:
				values[i] = ""
			default:
				jsonVal, _ := json.Marshal(v)
				values[i] = fmt.Sprintf("\"%s\"", strings.ReplaceAll(string(jsonVal), "\"", "\"\""))
			}
		}
		csvContent.WriteString(strings.Join(values, ","))
		csvContent.WriteByte('\n')
	}

	content := csvContent.String()

	// Write to file if path provided
	if resolvedPath != "" {
		if err := os.WriteFile(resolvedPath, []byte(content), 0644); err != nil {
			return nil, fmt.Errorf("failed to write file: %w", err)
		}
	}

	return &ExecuteResult{
		Columns: []string{"file", "rows", "data"},
		Rows: [][]interface{}{{
			filePath,
			len(result.Rows),
			content,
		}},
	}, nil
}

// =============================================================================
// Helper functions
// =============================================================================

func (e *StorageExecutor) extractApocLoadArg(cypher, loadType string) string {
	upper := strings.ToUpper(cypher)
	marker := "APOC.LOAD." + loadType
	idx := strings.Index(upper, marker)
	if idx < 0 {
		return ""
	}

	remainder := cypher[idx+len(marker):]
	openParen := strings.Index(remainder, "(")
	if openParen < 0 {
		return ""
	}

	// Find the first argument (URL or file path)
	afterParen := remainder[openParen+1:]
	// Handle quoted strings
	afterParen = strings.TrimSpace(afterParen)

	var endIdx int
	if len(afterParen) > 0 && (afterParen[0] == '\'' || afterParen[0] == '"') {
		quote := afterParen[0]
		closeQuote := strings.Index(afterParen[1:], string(quote))
		if closeQuote > 0 {
			return afterParen[1 : closeQuote+1]
		}
	}

	// Unquoted - find comma or close paren
	endIdx = strings.IndexAny(afterParen, ",)")
	if endIdx > 0 {
		return strings.TrimSpace(afterParen[:endIdx])
	}

	return ""
}

func (e *StorageExecutor) extractApocExportArg(cypher, exportType string) string {
	// Similar to load but for export procedures
	upper := strings.ToUpper(cypher)
	markers := []string{"APOC.EXPORT." + exportType + ".ALL", "APOC.EXPORT." + exportType + ".QUERY"}

	for _, marker := range markers {
		idx := strings.Index(upper, marker)
		if idx >= 0 {
			remainder := cypher[idx+len(marker):]
			openParen := strings.Index(remainder, "(")
			if openParen >= 0 {
				afterParen := strings.TrimSpace(remainder[openParen+1:])

				// For query export, second arg is file
				if strings.HasSuffix(marker, ".QUERY") {
					// Skip first arg (query)
					commaIdx := strings.Index(afterParen, ",")
					if commaIdx > 0 {
						afterParen = strings.TrimSpace(afterParen[commaIdx+1:])
					}
				}

				// Extract file path
				if len(afterParen) > 0 && (afterParen[0] == '\'' || afterParen[0] == '"') {
					quote := afterParen[0]
					closeQuote := strings.Index(afterParen[1:], string(quote))
					if closeQuote > 0 {
						return afterParen[1 : closeQuote+1]
					}
				}
			}
		}
	}

	return ""
}

func (e *StorageExecutor) extractApocExportQuery(cypher string) string {
	upper := strings.ToUpper(cypher)
	idx := strings.Index(upper, ".QUERY")
	if idx < 0 {
		return ""
	}

	remainder := cypher[idx+6:] // After ".QUERY"
	openParen := strings.Index(remainder, "(")
	if openParen < 0 {
		return ""
	}

	afterParen := strings.TrimSpace(remainder[openParen+1:])

	// First argument is the query
	if len(afterParen) > 0 && (afterParen[0] == '\'' || afterParen[0] == '"') {
		quote := afterParen[0]
		closeQuote := strings.Index(afterParen[1:], string(quote))
		if closeQuote > 0 {
			return afterParen[1 : closeQuote+1]
		}
	}

	return ""
}

func (e *StorageExecutor) nodesToExportFormat(nodes []*storage.Node) []map[string]interface{} {
	result := make([]map[string]interface{}, len(nodes))
	for i, node := range nodes {
		result[i] = map[string]interface{}{
			"id":         string(node.ID),
			"labels":     node.Labels,
			"properties": node.Properties,
		}
	}
	return result
}

func (e *StorageExecutor) edgesToExportFormat(edges []*storage.Edge) []map[string]interface{} {
	result := make([]map[string]interface{}, len(edges))
	for i, edge := range edges {
		result[i] = map[string]interface{}{
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

// callApocLoadJsonArray loads a JSON array from a specific path.
// Syntax: CALL apoc.load.jsonArray(urlOrFile, path) YIELD value
func (e *StorageExecutor) callApocLoadJsonArray(ctx context.Context, cypher string) (*ExecuteResult, error) {
	urlOrFile := e.extractApocLoadArg(cypher, "JSONARRAY")
	if urlOrFile == "" {
		return nil, fmt.Errorf("apoc.load.jsonArray requires a URL or file path")
	}

	var data interface{}
	var err error

	if isHTTPSource(urlOrFile) {
		data, err = e.loadJsonFromURL(urlOrFile)
	} else {
		data, err = e.loadJsonFromFile(urlOrFile)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load JSON: %w", err)
	}

	// Extract array
	rows := [][]interface{}{}
	switch v := data.(type) {
	case []interface{}:
		for _, item := range v {
			rows = append(rows, []interface{}{item})
		}
	default:
		rows = append(rows, []interface{}{data})
	}

	return &ExecuteResult{
		Columns: []string{"value"},
		Rows:    rows,
	}, nil
}

// =============================================================================
// apoc.load.csvParams - Load CSV with params
// =============================================================================

// callApocLoadCsvParams loads CSV with configurable parameters.
func (e *StorageExecutor) callApocLoadCsvParams(ctx context.Context, cypher string) (*ExecuteResult, error) {
	// Delegates to callApocLoadCsv with param parsing
	return e.callApocLoadCsv(ctx, cypher)
}

// =============================================================================
// apoc.import.json - Import JSON directly into graph
// =============================================================================

// callApocImportJson imports JSON graph data directly.
// Syntax: CALL apoc.import.json(urlOrFile) YIELD nodes, relationships
func (e *StorageExecutor) callApocImportJson(ctx context.Context, cypher string) (*ExecuteResult, error) {
	store := e.getStorage(ctx)
	prefixIDs := true
	dbName := GetUseDatabaseFromContext(ctx)
	if dbName == "" {
		if ns, ok := store.(*storage.NamespacedEngine); ok {
			dbName = ns.Namespace()
			prefixIDs = false
		} else {
			dbName = "nornic"
		}
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
	urlOrFile := e.extractApocLoadArg(cypher, "JSON")
	if urlOrFile == "" {
		// Try IMPORT marker
		upper := strings.ToUpper(cypher)
		idx := strings.Index(upper, "APOC.IMPORT.JSON")
		if idx >= 0 {
			remainder := cypher[idx+16:]
			openParen := strings.Index(remainder, "(")
			if openParen >= 0 {
				afterParen := strings.TrimSpace(remainder[openParen+1:])
				if len(afterParen) > 0 && (afterParen[0] == '\'' || afterParen[0] == '"') {
					quote := afterParen[0]
					closeQuote := strings.Index(afterParen[1:], string(quote))
					if closeQuote > 0 {
						urlOrFile = afterParen[1 : closeQuote+1]
					}
				}
			}
		}
	}

	if urlOrFile == "" {
		return nil, fmt.Errorf("apoc.import.json requires a URL or file path")
	}

	var data interface{}
	var err error

	if isHTTPSource(urlOrFile) {
		data, err = e.loadJsonFromURL(urlOrFile)
	} else {
		data, err = e.loadJsonFromFile(urlOrFile)
	}

	if err != nil {
		return nil, fmt.Errorf("failed to load JSON: %w", err)
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
						if _, err := store.CreateNode(node); err == nil {
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
						if err := store.CreateEdge(edge); err == nil {
							relsImported++
						}
					}
				}
			}
		}
	}

	return &ExecuteResult{
		Columns: []string{"source", "nodes", "relationships"},
		Rows: [][]interface{}{{
			urlOrFile,
			nodesImported,
			relsImported,
		}},
	}, nil
}

// Make strconv available for any numeric conversions
var _ = strconv.Itoa
