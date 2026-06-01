package adminimport

import (
	"archive/zip"
	"bufio"
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	ExitOK                  = 0
	ExitCSV                 = 2
	ExitBadRelationship     = 3
	ExitDuplicateID         = 4
	ExitConstraintViolation = 5
	ExitUnsupported         = 6
)

type Options struct {
	DatabaseName         string
	NodeSources          []string
	RelSources           []string
	DataDir              string
	Delimiter            rune
	ArrayDelimiter       rune
	VectorDelimiter      rune
	Quote                rune
	IDType               string
	NormalizeTypes       bool
	IgnoreExtraColumns   bool
	IgnoreEmptyStrings   bool
	BadTolerance         int
	SkipBadRelationships bool
	SkipDuplicateNodes   bool
	ReportFile           string
	SchemaFile           string
	BuildIndexes         bool
	ChunkSize            int
	Now                  time.Time
	Verbose              bool
}

type Report struct {
	DatabaseName          string        `json:"databaseName"`
	NodesImported         int64         `json:"nodesImported"`
	RelationshipsImported int64         `json:"relationshipsImported"`
	BadRelationships      int64         `json:"badRelationships"`
	DuplicateNodesSkipped int64         `json:"duplicateNodesSkipped"`
	IndexesBuilt          bool          `json:"indexesBuilt"`
	StartedAt             time.Time     `json:"startedAt"`
	CompletedAt           time.Time     `json:"completedAt"`
	Duration              time.Duration `json:"durationNanos"`
	Status                string        `json:"status"`
	Errors                []string      `json:"errors,omitempty"`
}

type Error struct {
	ExitCode int
	Message  string
	Err      error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Err == nil {
		return e.Message
	}
	return e.Message + ": " + e.Err.Error()
}

func (e *Error) Unwrap() error { return e.Err }

func ImportFull(ctx context.Context, engine storage.Engine, opts Options) (Report, error) {
	opts = opts.withDefaults()
	report := Report{DatabaseName: opts.DatabaseName, StartedAt: time.Now(), Status: "failed"}
	if opts.DatabaseName == "" {
		return report, unsupported("database name is required")
	}
	if len(opts.NodeSources) == 0 {
		return report, unsupported("at least one --nodes source is required")
	}
	if engine == nil {
		return report, unsupported("storage engine is required")
	}
	if opts.IDType == "actual" {
		return report, unsupported("--id-type=actual is not supported")
	}

	target := storage.NewNamespacedEngine(engine, opts.DatabaseName)
	if err := ensureEmpty(target); err != nil {
		return report, err
	}

	state := &importState{idMap: make(map[string]storage.NodeID)}
	for _, sourceSpec := range opts.NodeSources {
		if err := importNodeSource(ctx, target, opts, sourceSpec, state, &report); err != nil {
			report.Errors = append(report.Errors, err.Error())
			_ = writeReport(opts.ReportFile, report)
			return report, err
		}
	}
	for _, sourceSpec := range opts.RelSources {
		if err := importRelationshipSource(ctx, target, opts, sourceSpec, state, &report); err != nil {
			report.Errors = append(report.Errors, err.Error())
			_ = writeReport(opts.ReportFile, report)
			return report, err
		}
	}
	if opts.SchemaFile != "" {
		if err := applySchema(ctx, target, opts.SchemaFile); err != nil {
			report.Errors = append(report.Errors, err.Error())
			_ = writeReport(opts.ReportFile, report)
			return report, &Error{ExitCode: ExitConstraintViolation, Message: "schema application failed", Err: err}
		}
	}
	if opts.BuildIndexes {
		svc := search.NewService(target)
		if opts.DataDir != "" {
			base := filepath.Join(opts.DataDir, "search", opts.DatabaseName)
			svc.SetPersistenceEnabled(true)
			svc.SetFulltextIndexPath(filepath.Join(base, "bm25"))
			svc.SetVectorIndexPath(filepath.Join(base, "vectors"))
			svc.SetHNSWIndexPath(filepath.Join(base, "hnsw"))
		}
		if err := svc.BuildIndexes(ctx); err != nil {
			report.Errors = append(report.Errors, err.Error())
			_ = writeReport(opts.ReportFile, report)
			return report, err
		}
		report.IndexesBuilt = true
	}
	report.Status = "success"
	report.CompletedAt = time.Now()
	report.Duration = report.CompletedAt.Sub(report.StartedAt)
	return report, writeReport(opts.ReportFile, report)
}

func (o Options) withDefaults() Options {
	if o.Delimiter == 0 {
		o.Delimiter = ','
	}
	if o.ArrayDelimiter == 0 {
		o.ArrayDelimiter = ';'
	}
	if o.VectorDelimiter == 0 {
		o.VectorDelimiter = ';'
	}
	if o.Quote == 0 {
		o.Quote = '"'
	}
	if o.IDType == "" {
		o.IDType = "string"
	}
	if o.ChunkSize <= 0 {
		o.ChunkSize = 1000
	}
	if o.Now.IsZero() {
		o.Now = time.Now()
	}
	return o
}

type importState struct {
	idMap            map[string]storage.NodeID
	anonymousNodeSeq int
	edgeSeq          int
}

type sourceSpec struct {
	prefix []string
	files  []string
}

type headerKind int

const (
	kindProperty headerKind = iota
	kindID
	kindLabel
	kindIgnore
	kindStartID
	kindEndID
	kindType
	kindNamedEmbedding
)

type columnSpec struct {
	Name       string
	Kind       headerKind
	Type       string
	IDSpace    string
	VectorDims int
	EmbedKey   string
	Options    map[string]string
}

func importNodeSource(ctx context.Context, engine storage.Engine, opts Options, raw string, state *importState, report *Report) error {
	spec, err := parseSourceSpec(raw, true)
	if err != nil {
		return err
	}
	source, err := openCSVSource(spec.files, opts)
	if err != nil {
		return err
	}
	defer source.Close()

	header, err := source.ReadHeader()
	if err != nil {
		return csvErr(spec.files[0], 1, 0, err)
	}
	cols, err := parseHeader(header, true)
	if err != nil {
		return err
	}
	idCols := filterColumns(cols, kindID)
	namedEmbeddingDims := make(map[string]int)
	nodes := make([]*storage.Node, 0, opts.ChunkSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		record, filePath, rowNum, err := source.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return csvErr(filePath, rowNum, 0, err)
		}
		anonymousSeq := -1
		if len(idCols) == 0 {
			anonymousSeq = state.anonymousNodeSeq
			state.anonymousNodeSeq++
		}
		node, lookupKey, err := nodeFromRecord(record, filePath, cols, idCols, spec.prefix, opts, rowNum, anonymousSeq, namedEmbeddingDims)
		if err != nil {
			return err
		}
		if lookupKey != "" {
			if _, exists := state.idMap[lookupKey]; exists {
				if opts.SkipDuplicateNodes {
					report.DuplicateNodesSkipped++
					continue
				}
				return &Error{ExitCode: ExitDuplicateID, Message: fmt.Sprintf("duplicate node ID at row %d", rowNum)}
			}
			state.idMap[lookupKey] = node.ID
		}
		nodes = append(nodes, node)
		if len(nodes) >= opts.ChunkSize {
			if err := engine.BulkCreateNodes(nodes); err != nil {
				return err
			}
			report.NodesImported += int64(len(nodes))
			nodes = nodes[:0]
		}
	}
	if len(nodes) > 0 {
		if err := engine.BulkCreateNodes(nodes); err != nil {
			return err
		}
		report.NodesImported += int64(len(nodes))
	}
	return nil
}

func importRelationshipSource(ctx context.Context, engine storage.Engine, opts Options, raw string, state *importState, report *Report) error {
	spec, err := parseSourceSpec(raw, false)
	if err != nil {
		return err
	}
	source, err := openCSVSource(spec.files, opts)
	if err != nil {
		return err
	}
	defer source.Close()

	header, err := source.ReadHeader()
	if err != nil {
		return csvErr(spec.files[0], 1, 0, err)
	}
	cols, err := parseHeader(header, false)
	if err != nil {
		return err
	}
	startCols := filterColumns(cols, kindStartID)
	endCols := filterColumns(cols, kindEndID)
	typeCols := filterColumns(cols, kindType)
	if len(startCols) == 0 || len(endCols) == 0 {
		return unsupported("relationship source requires :START_ID and :END_ID columns")
	}
	edges := make([]*storage.Edge, 0, opts.ChunkSize)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		record, filePath, rowNum, err := source.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return csvErr(filePath, rowNum, 0, err)
		}
		edgeSeq := state.edgeSeq
		state.edgeSeq++
		edge, skip, err := edgeFromRecord(record, filePath, cols, startCols, endCols, typeCols, spec.prefix, opts, state, rowNum, edgeSeq, report)
		if err != nil {
			return err
		}
		if skip {
			continue
		}
		edges = append(edges, edge)
		if len(edges) >= opts.ChunkSize {
			if err := engine.BulkCreateEdges(edges); err != nil {
				return err
			}
			report.RelationshipsImported += int64(len(edges))
			edges = edges[:0]
		}
	}
	if len(edges) > 0 {
		if err := engine.BulkCreateEdges(edges); err != nil {
			return err
		}
		report.RelationshipsImported += int64(len(edges))
	}
	return nil
}

func nodeFromRecord(record []string, filePath string, cols []columnSpec, idCols []int, prefixLabels []string, opts Options, line int, anonymousSeq int, namedEmbeddingDims map[string]int) (*storage.Node, string, error) {
	props := make(map[string]any)
	named := make(map[string][]float32)
	labels := append([]string{}, prefixLabels...)
	var idValues []string
	var idSpaces []string
	for i, col := range cols {
		if i >= len(record) {
			continue
		}
		value := record[i]
		switch col.Kind {
		case kindID:
			if value == "" {
				return nil, "", csvErr(filePath, line, i+1, fmt.Errorf("missing ID value"))
			}
			canonical, err := canonicalID(value, opts.IDType)
			if err != nil {
				return nil, "", csvErr(filePath, line, i+1, err)
			}
			idValues = append(idValues, canonical)
			idSpaces = append(idSpaces, col.IDSpace)
			if col.Name != "" {
				props[col.Name] = canonical
			}
		case kindLabel:
			if value == "" {
				continue
			}
			labels = append(labels, splitList(value, opts.ArrayDelimiter)...)
		case kindProperty:
			if value == "" {
				if opts.IgnoreEmptyStrings || !preserveEmptyStringProperty(col) {
					continue
				}
				props[col.Name] = ""
				continue
			}
			parsed, err := parsePropertyValue(value, col, opts)
			if err != nil {
				return nil, "", csvErr(filePath, line, i+1, err)
			}
			props[col.Name] = parsed
		case kindNamedEmbedding:
			if value == "" {
				continue
			}
			dims := col.VectorDims
			if dims == 0 {
				dims = namedEmbeddingDims[col.EmbedKey]
			}
			vec, err := parseVector(value, opts.VectorDelimiter, dims)
			if err != nil {
				return nil, "", csvErr(filePath, line, i+1, err)
			}
			if dims == 0 {
				namedEmbeddingDims[col.EmbedKey] = len(vec)
			}
			named[col.EmbedKey] = vec
		case kindIgnore:
		}
	}
	if len(record) > len(cols) && !opts.IgnoreExtraColumns {
		return nil, "", csvErr(filePath, line, len(cols)+1, fmt.Errorf("extra column without header"))
	}
	var id storage.NodeID
	var lookupKey string
	if len(idCols) > 0 {
		id = storage.NodeID(strings.Join(idValues, "|"))
		lookupKey = importIDKey(idSpaces, idValues)
	} else {
		id = storage.NodeID(fmt.Sprintf("_anon_%d", anonymousSeq))
	}
	node := &storage.Node{ID: id, Labels: uniqueNonEmpty(labels), Properties: props, CreatedAt: opts.Now, UpdatedAt: opts.Now}
	if len(named) > 0 {
		node.NamedEmbeddings = named
	}
	return node, lookupKey, nil
}

func edgeFromRecord(record []string, filePath string, cols []columnSpec, startCols, endCols, typeCols []int, prefixType []string, opts Options, state *importState, line int, edgeSeq int, report *Report) (*storage.Edge, bool, error) {
	props := make(map[string]any)
	startValues, startSpaces, startCol, err := idValuesFromRecord(record, cols, startCols, opts)
	if err != nil {
		return nil, false, csvErr(filePath, line, startCol+1, err)
	}
	endValues, endSpaces, endCol, err := idValuesFromRecord(record, cols, endCols, opts)
	if err != nil {
		return nil, false, csvErr(filePath, line, endCol+1, err)
	}
	startID, ok := state.idMap[importIDKey(startSpaces, startValues)]
	if !ok {
		return nil, opts.SkipBadRelationships, badRelationship(opts, report, line, "missing start node")
	}
	endID, ok := state.idMap[importIDKey(endSpaces, endValues)]
	if !ok {
		return nil, opts.SkipBadRelationships, badRelationship(opts, report, line, "missing end node")
	}
	edgeType := ""
	if len(prefixType) > 0 {
		edgeType = prefixType[0]
	}
	for _, idx := range typeCols {
		if idx < len(record) && record[idx] != "" {
			edgeType = record[idx]
		}
	}
	if edgeType == "" {
		return nil, false, unsupported("relationship source requires :TYPE column or --relationships=TYPE= prefix")
	}
	for i, col := range cols {
		if i >= len(record) {
			continue
		}
		if col.Kind != kindProperty {
			continue
		}
		value := record[i]
		if value == "" {
			if opts.IgnoreEmptyStrings || !preserveEmptyStringProperty(col) {
				continue
			}
			props[col.Name] = ""
			continue
		}
		parsed, err := parsePropertyValue(value, col, opts)
		if err != nil {
			return nil, false, csvErr(filePath, line, i+1, err)
		}
		props[col.Name] = parsed
	}
	return &storage.Edge{ID: storage.EdgeID(fmt.Sprintf("rel_%d", edgeSeq)), StartNode: startID, EndNode: endID, Type: edgeType, Properties: props, CreatedAt: opts.Now, UpdatedAt: opts.Now, Confidence: 1.0}, false, nil
}

func badRelationship(opts Options, report *Report, line int, msg string) error {
	report.BadRelationships++
	if opts.SkipBadRelationships {
		return nil
	}
	return &Error{ExitCode: ExitBadRelationship, Message: fmt.Sprintf("bad relationship at row %d: %s", line, msg)}
}

func idValuesFromRecord(record []string, cols []columnSpec, indexes []int, opts Options) ([]string, []string, int, error) {
	values := make([]string, 0, len(indexes))
	spaces := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		if idx >= len(record) {
			return nil, nil, idx, fmt.Errorf("missing ID column")
		}
		if record[idx] == "" {
			return nil, nil, idx, fmt.Errorf("missing ID value")
		}
		value, err := canonicalID(record[idx], opts.IDType)
		if err != nil {
			return nil, nil, idx, err
		}
		values = append(values, value)
		spaces = append(spaces, cols[idx].IDSpace)
	}
	return values, spaces, 0, nil
}

func parseHeader(header []string, node bool) ([]columnSpec, error) {
	cols := make([]columnSpec, len(header))
	for i, raw := range header {
		col, err := parseColumnSpec(strings.TrimSpace(raw), node)
		if err != nil {
			return nil, err
		}
		cols[i] = col
	}
	return cols, nil
}

func parseColumnSpec(raw string, node bool) (columnSpec, error) {
	if raw == "" {
		return columnSpec{Kind: kindIgnore}, nil
	}
	if strings.HasPrefix(raw, ":") {
		keyword, arg, opts := parseKeyword(raw[1:])
		switch strings.ToUpper(keyword) {
		case "ID":
			if !node {
				return columnSpec{}, unsupported(":ID is only valid in node headers")
			}
			return columnSpec{Kind: kindID, IDSpace: arg}, nil
		case "LABEL":
			return columnSpec{Kind: kindLabel}, nil
		case "IGNORE":
			return columnSpec{Kind: kindIgnore}, nil
		case "START_ID":
			return columnSpec{Kind: kindStartID, IDSpace: arg}, nil
		case "END_ID":
			return columnSpec{Kind: kindEndID, IDSpace: arg}, nil
		case "TYPE":
			return columnSpec{Kind: kindType}, nil
		case "EMBEDDING", "NAMED_EMBEDDING":
			dims := 0
			if d, ok := opts["dimensions"]; ok {
				parsed, err := strconv.Atoi(d)
				if err != nil {
					return columnSpec{}, unsupported("invalid vector dimensions in header: " + raw)
				}
				dims = parsed
			}
			return columnSpec{Kind: kindNamedEmbedding, EmbedKey: defaultString(arg, "default"), Options: opts, VectorDims: dims}, nil
		default:
			return columnSpec{}, unsupported("unsupported header token: " + raw)
		}
	}
	name, typ := splitNameType(raw)
	keyword, arg, opts := parseKeyword(typ)
	if strings.EqualFold(keyword, "ID") {
		return columnSpec{Name: name, Kind: kindID, IDSpace: arg}, nil
	}
	dims := 0
	if strings.EqualFold(keyword, "vector") {
		if d, ok := opts["dimensions"]; ok {
			parsed, err := strconv.Atoi(d)
			if err != nil {
				return columnSpec{}, unsupported("invalid vector dimensions in header: " + raw)
			}
			dims = parsed
		}
	}
	return columnSpec{Name: name, Kind: kindProperty, Type: strings.ToLower(keyword), IDSpace: arg, Options: opts, VectorDims: dims}, nil
}

func splitNameType(raw string) (string, string) {
	idx := strings.IndexRune(raw, ':')
	if idx < 0 {
		return raw, "string"
	}
	return raw[:idx], raw[idx+1:]
}

func parseKeyword(raw string) (string, string, map[string]string) {
	options := make(map[string]string)
	base := raw
	if brace := strings.IndexRune(base, '{'); brace >= 0 {
		end := strings.LastIndex(base, "}")
		if end > brace {
			for _, part := range strings.Split(base[brace+1:end], ",") {
				key, val, ok := strings.Cut(strings.TrimSpace(part), ":")
				if ok {
					options[strings.TrimSpace(key)] = strings.TrimSpace(val)
				}
			}
			base = base[:brace]
		}
	}
	arg := ""
	if open := strings.IndexRune(base, '('); open >= 0 {
		if close := strings.LastIndex(base, ")"); close > open {
			arg = base[open+1 : close]
			base = base[:open]
		}
	}
	return base, arg, options
}

func parsePropertyValue(value string, col columnSpec, opts Options) (any, error) {
	if strings.HasSuffix(col.Type, "[]") {
		base := strings.TrimSuffix(col.Type, "[]")
		parts := splitList(value, opts.ArrayDelimiter)
		out := make([]any, 0, len(parts))
		for _, part := range parts {
			parsed, err := parseScalar(part, base, opts)
			if err != nil {
				return nil, err
			}
			out = append(out, parsed)
		}
		return out, nil
	}
	if col.Type == "vector" {
		return parseVector(value, opts.VectorDelimiter, col.VectorDims)
	}
	return parseScalar(value, col.Type, opts)
}

func parseScalar(value, typ string, opts Options) (any, error) {
	switch strings.ToLower(defaultString(typ, "string")) {
	case "string", "char", "point", "date", "localtime", "time", "localdatetime", "datetime", "duration":
		return value, nil
	case "byte", "short", "int", "long":
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return nil, err
		}
		if opts.NormalizeTypes {
			return v, nil
		}
		return v, nil
	case "float", "double":
		v, err := strconv.ParseFloat(value, 64)
		if err != nil {
			return nil, err
		}
		return v, nil
	case "boolean":
		v, err := strconv.ParseBool(strings.TrimSpace(value))
		if err != nil {
			return nil, err
		}
		return v, nil
	default:
		return nil, unsupported("unsupported property type: " + typ)
	}
}

func parseVector(value string, delim rune, dims int) ([]float32, error) {
	parts := splitList(value, delim)
	if dims > 0 && len(parts) != dims {
		return nil, fmt.Errorf("vector dimensions mismatch: expected %d, got %d", dims, len(parts))
	}
	out := make([]float32, 0, len(parts))
	for _, part := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(part), 32)
		if err != nil {
			return nil, err
		}
		out = append(out, float32(v))
	}
	return out, nil
}

func parseSourceSpec(raw string, node bool) (sourceSpec, error) {
	var prefix []string
	filesPart := raw
	if before, after, ok := strings.Cut(raw, "="); ok {
		prefix = strings.Split(before, ":")
		filesPart = after
	}
	files := splitList(filesPart, ',')
	if len(files) == 0 || files[0] == "" {
		return sourceSpec{}, unsupported("empty import source")
	}
	for i := range files {
		files[i] = strings.TrimSpace(files[i])
	}
	if !node && len(prefix) > 1 {
		return sourceSpec{}, unsupported("relationship source accepts at most one type prefix")
	}
	return sourceSpec{prefix: uniqueNonEmpty(prefix), files: files}, nil
}

type csvSourceReader struct {
	files       []string
	opts        Options
	currentFile int
	reader      *csv.Reader
	closer      io.Closer
	rowInFile   int
	headerRead  bool
	currentPath string
}

func openCSVSource(files []string, opts Options) (*csvSourceReader, error) {
	if opts.Quote != '"' {
		return nil, unsupported("custom --quote is not supported by the Go CSV reader yet")
	}
	for _, path := range files {
		if _, err := os.Stat(path); err != nil {
			return nil, &Error{ExitCode: ExitCSV, Message: "failed to open CSV file", Err: err}
		}
	}
	return &csvSourceReader{files: files, opts: opts}, nil
}

func (s *csvSourceReader) Close() error {
	if s.closer != nil {
		return s.closer.Close()
	}
	return nil
}

func (s *csvSourceReader) ReadHeader() ([]string, error) {
	if s.headerRead {
		return nil, io.EOF
	}
	if err := s.openCurrent(); err != nil {
		return nil, err
	}
	header, err := s.reader.Read()
	if err != nil {
		return nil, err
	}
	s.headerRead = true
	s.rowInFile = 1
	return header, nil
}

func (s *csvSourceReader) Read() ([]string, string, int, error) {
	if !s.headerRead {
		return nil, s.currentPath, 0, io.EOF
	}
	for {
		if err := s.openCurrent(); err != nil {
			return nil, s.currentPath, s.rowInFile + 1, err
		}
		record, err := s.reader.Read()
		if errors.Is(err, io.EOF) {
			if err := s.advanceFile(); err != nil {
				return nil, s.currentPath, s.rowInFile + 1, err
			}
			continue
		}
		if err != nil {
			return nil, s.currentPath, s.rowInFile + 1, err
		}
		s.rowInFile++
		return record, s.currentPath, s.rowInFile, nil
	}
}

func (s *csvSourceReader) openCurrent() error {
	if s.reader != nil {
		return nil
	}
	if s.currentFile >= len(s.files) {
		return io.EOF
	}
	path := s.files[s.currentFile]
	reader, closer, err := openOne(path)
	if err != nil {
		return err
	}
	r := csv.NewReader(reader)
	r.Comma = s.opts.Delimiter
	r.LazyQuotes = false
	r.FieldsPerRecord = -1
	s.reader = r
	s.closer = closer
	s.currentPath = path
	s.rowInFile = 0
	return nil
}

func (s *csvSourceReader) advanceFile() error {
	if s.closer != nil {
		_ = s.closer.Close()
		s.closer = nil
	}
	s.reader = nil
	s.currentFile++
	if s.currentFile >= len(s.files) {
		return io.EOF
	}
	s.rowInFile = 0
	return nil
}

func openOne(path string) (io.Reader, io.Closer, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, &Error{ExitCode: ExitCSV, Message: "failed to open CSV file", Err: err}
	}
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			_ = f.Close()
			return nil, nil, &Error{ExitCode: ExitCSV, Message: "failed to open gzip CSV file", Err: err}
		}
		return gz, closers{gz, f}, nil
	}
	if strings.HasSuffix(path, ".zip") {
		zr, err := zip.OpenReader(path)
		if err != nil {
			_ = f.Close()
			return nil, nil, &Error{ExitCode: ExitCSV, Message: "failed to open zip CSV file", Err: err}
		}
		_ = f.Close()
		if len(zr.File) != 1 {
			_ = zr.Close()
			return nil, nil, unsupported("zip CSV sources must contain exactly one file")
		}
		rc, err := zr.File[0].Open()
		if err != nil {
			_ = zr.Close()
			return nil, nil, &Error{ExitCode: ExitCSV, Message: "failed to open zipped CSV member", Err: err}
		}
		return rc, closers{rc, zr}, nil
	}
	return f, f, nil
}

type closers []io.Closer

func (c closers) Close() error {
	var firstErr error
	for _, closer := range c {
		if err := closer.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func applySchema(ctx context.Context, engine storage.Engine, path string) error {
	if strings.EqualFold(filepath.Ext(path), ".json") {
		return applySchemaDefinition(engine, path)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	exec := cypher.NewStorageExecutor(engine)
	for _, stmt := range splitCypherStatements(string(contents)) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := exec.Execute(ctx, stmt, nil); err != nil {
			return err
		}
	}
	return nil
}

func applySchemaDefinition(engine storage.Engine, path string) error {
	contents, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var def storage.SchemaDefinition
	if err := json.Unmarshal(contents, &def); err != nil {
		return err
	}
	schema := engine.GetSchema()
	if err := schema.ReplaceFromDefinition(&def); err != nil {
		return err
	}
	if err := rebuildSchemaDerivedState(engine, schema); err != nil {
		return err
	}
	for _, constraint := range schema.GetAllConstraints() {
		if err := storage.ValidateConstraintOnCreationForEngine(engine, constraint); err != nil {
			return err
		}
	}
	for _, typeConstraint := range schema.GetAllPropertyTypeConstraints() {
		if err := storage.ValidatePropertyTypeConstraintOnCreationForEngine(engine, typeConstraint); err != nil {
			return err
		}
	}
	for _, contract := range schema.GetAllConstraintContracts() {
		if err := storage.ValidateConstraintContractOnCreationForEngine(engine, contract); err != nil {
			return err
		}
	}
	return nil
}

func rebuildSchemaDerivedState(engine storage.Engine, schema *storage.SchemaManager) error {
	if err := storage.RefreshUniqueConstraintValuesForEngine(engine, schema); err != nil {
		return err
	}
	nodes, err := engine.AllNodes()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		for _, label := range node.Labels {
			for property, value := range node.Properties {
				if _, ok := schema.GetPropertyIndex(label, property); ok {
					if err := schema.PropertyIndexInsert(label, property, node.ID, value); err != nil {
						return err
					}
				}
			}
			for _, idx := range schema.GetCompositeIndexesForLabel(label) {
				if idx == nil {
					continue
				}
				if err := idx.IndexNode(node.ID, node.Properties); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func splitCypherStatements(s string) []string {
	var out []string
	scanner := bufio.NewScanner(strings.NewReader(s))
	var b strings.Builder
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(line, "//") || strings.HasPrefix(line, "#") {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	for _, stmt := range strings.Split(b.String(), ";") {
		out = append(out, strings.TrimSpace(stmt))
	}
	return out
}

func ensureEmpty(engine storage.Engine) error {
	nodes, err := engine.NodeCount()
	if err != nil {
		return err
	}
	edges, err := engine.EdgeCount()
	if err != nil {
		return err
	}
	if nodes != 0 || edges != 0 {
		return unsupported("database import full requires an empty target database")
	}
	return nil
}

func writeReport(path string, report Report) error {
	if path == "" {
		return nil
	}
	data, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

func filterColumns(cols []columnSpec, kind headerKind) []int {
	var out []int
	for i, col := range cols {
		if col.Kind == kind {
			out = append(out, i)
		}
	}
	return out
}

func canonicalID(value, idType string) (string, error) {
	if strings.EqualFold(idType, "integer") {
		v, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(v, 10), nil
	}
	return value, nil
}

func importIDKey(spaces, values []string) string {
	return strings.Join(spaces, "\x1f") + "\x1e" + strings.Join(values, "\x1f")
}

func splitList(value string, delim rune) []string {
	parts := strings.Split(value, string(delim))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func preserveEmptyStringProperty(col columnSpec) bool {
	if strings.HasSuffix(col.Type, "[]") || strings.EqualFold(col.Type, "vector") {
		return false
	}
	switch strings.ToLower(defaultString(col.Type, "string")) {
	case "string", "char":
		return true
	default:
		return false
	}
}

func uniqueNonEmpty(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func defaultString(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

func csvErr(file string, row, col int, err error) error {
	where := fmt.Sprintf("row %d", row)
	if col > 0 {
		where += fmt.Sprintf(", column %d", col)
	}
	if file != "" {
		where = file + ": " + where
	}
	return &Error{ExitCode: ExitCSV, Message: "CSV parse error at " + where, Err: err}
}

func unsupported(message string) error {
	return &Error{ExitCode: ExitUnsupported, Message: message}
}
