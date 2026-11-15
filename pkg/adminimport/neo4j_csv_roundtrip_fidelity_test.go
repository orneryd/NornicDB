package adminimport

import (
	"compress/gzip"
	"context"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// importNeo4jCSVPackage loads sources into a fresh database the way the admin
// tool's import command does and returns the loaded namespace.
func importNeo4jCSVPackage(t *testing.T, opts Options, nodeSources, relSources []string) storage.Engine {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { _ = base.Close() })
	opts.DatabaseName = "roundtrip"
	opts.NodeSources = nodeSources
	opts.RelSources = relSources
	opts.BuildIndexes = false
	opts.Now = fixedImportTime
	_, err := ImportFull(context.Background(), base, opts)
	require.NoError(t, err)
	return storage.NewNamespacedEngine(base, "roundtrip")
}

// roundTripNeo4jCSV exports an engine with the admin CSV export and imports that
// export into a fresh database, as the admin tool's export and import commands
// do. It returns the imported namespace and the export directory.
func roundTripNeo4jCSV(t *testing.T, source storage.Engine) (storage.Engine, string) {
	t.Helper()
	outDir := filepath.Join(t.TempDir(), "neo4j-csv")
	require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: outDir}))
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, Options{})
	require.NoError(t, err)
	return importNeo4jCSVPackage(t, Options{}, nodeSources, relSources), outDir
}

func readExportedFile(t *testing.T, dir, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(dir, name))
	require.NoError(t, err)
	return string(data)
}

func writeGzipFile(t *testing.T, path, content string) {
	t.Helper()
	file, err := os.Create(path)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
}

// TestNeo4jCSVExportImportDistinguishesAbsentAndEmptyStringProperties is the
// Neo4j CSV convention round trip: a property the entity does not hold is an
// unquoted empty cell, a property that holds the empty string is the quoted
// empty cell "", and any other text - a literal backslash included - is written
// as plain CSV text. The import must reproduce each property set exactly.
func TestNeo4jCSVExportImportDistinguishesAbsentAndEmptyStringProperties(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	source := storage.NewNamespacedEngine(base, "source")

	nodes := []*storage.Node{
		{ID: "n1", Labels: []string{"Person"}, Properties: map[string]any{"uid": "n1", "name": "Alice", "nickname": "", "note": `\N`}},
		{ID: "n2", Labels: []string{"Person"}, Properties: map[string]any{"uid": "n2"}},
		{ID: "n3", Labels: []string{"Person"}, Properties: map[string]any{"uid": "n3", "name": "", "nickname": `\`, "note": `""`}},
		{ID: "n4", Labels: []string{"Person"}, Properties: map[string]any{"uid": "n4", "name": "Bob", "nickname": ""}},
	}
	for _, node := range nodes {
		_, err := source.CreateNode(ctx, node)
		require.NoError(t, err)
	}
	edges := []*storage.Edge{
		{ID: "e1", StartNode: "n1", EndNode: "n2", Type: "KNOWS", Properties: map[string]any{"uid": "e1", "note": ""}},
		{ID: "e2", StartNode: "n2", EndNode: "n3", Type: "KNOWS", Properties: map[string]any{"uid": "e2"}},
		{ID: "e3", StartNode: "n3", EndNode: "n4", Type: "KNOWS", Properties: map[string]any{"uid": "e3", "note": `\N`}},
	}
	for _, edge := range edges {
		require.NoError(t, source.CreateEdge(ctx, edge))
	}

	dest, outDir := roundTripNeo4jCSV(t, source)

	for _, want := range nodes {
		got, err := dest.GetNode(ctx, want.ID)
		require.NoError(t, err, want.ID)
		require.Equal(t, want.Properties, got.Properties, "node %s properties", want.ID)
	}
	gotEdges, err := storage.CollectStoredEdges(ctx, dest)
	require.NoError(t, err)
	require.Len(t, gotEdges, len(edges))
	byID := make(map[storage.EdgeID]map[string]any, len(gotEdges))
	for _, edge := range gotEdges {
		byID[edge.ID] = edge.Properties
	}
	for _, want := range edges {
		require.Equal(t, want.Properties, byID[want.ID], "relationship %s properties", want.ID)
	}

	// The exported cells carry the convention: an absent property keeps a bare
	// empty cell, a present empty string is the quoted empty cell, and a real
	// value is written exactly - the literal \N needs no escaping.
	nodesCSV := readExportedFile(t, outDir, "nodes.csv")
	require.Contains(t, nodesCSV, ":ID,:LABEL,name:string,nickname:string,note:string,uid:string\n")
	require.Contains(t, nodesCSV, "\nn1,Person,Alice,\"\",\\N,n1\n")
	require.Contains(t, nodesCSV, "\nn2,Person,,,,n2\n")
	require.Contains(t, nodesCSV, "\nn3,Person,\"\",\\,\"\"\"\"\"\",n3\n")
	require.Contains(t, nodesCSV, "\nn4,Person,Bob,\"\",,n4\n")
	require.NotContains(t, nodesCSV, "nullToken")

	relsCSV := readExportedFile(t, outDir, "relationships.csv")
	require.Contains(t, relsCSV, ":ID,:START_ID,:END_ID,:TYPE,note:string,uid:string\n")
	require.Contains(t, relsCSV, "\ne1,n1,n2,KNOWS,\"\",e1\n")
	require.Contains(t, relsCSV, "\ne2,n2,n3,KNOWS,,e2\n")
	require.Contains(t, relsCSV, "\ne3,n3,n4,KNOWS,\\N,e3\n")
}

// TestNeo4jCSVExportImportKeepsFloat64ListCoordinates is the coordinate
// precision round trip: a list of float64 values exports as double[] so every
// coordinate keeps its value, while a list of float32 values keeps the float
// coordinate vector type and a named embedding keeps its float32 coordinates.
func TestNeo4jCSVExportImportKeepsFloat64ListCoordinates(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	source := storage.NewNamespacedEngine(base, "source")

	vector := []float64{0.3767786022034998, -0.28792035946100825, 1.0 / 3.0}
	// A float64 list whose values are exactly representable at float32 width
	// must still come back as float64 coordinates.
	narrow := []float64{float64(float32(0.4)), float64(float32(0.5))}
	embedding := []float32{0.4, 0.5, 0.6}
	_, err := source.CreateNode(ctx, &storage.Node{
		ID:     "v1",
		Labels: []string{"Coord"},
		Properties: map[string]any{
			"uid":      "v1",
			"vec":      vector,
			"vec32":    embedding,
			"narrowed": narrow,
		},
		NamedEmbeddings: map[string][]float32{"default": embedding},
	})
	require.NoError(t, err)

	// Every coordinate of the float64 list becomes another value at float32
	// width, so narrowing them on import would change the property.
	for _, coordinate := range vector {
		require.NotEqual(t, coordinate, float64(float32(coordinate)))
	}

	dest, outDir := roundTripNeo4jCSV(t, source)

	got, err := dest.GetNode(ctx, "v1")
	require.NoError(t, err)
	require.Equal(t, vector, got.Properties["vec"])
	require.Equal(t, embedding, got.Properties["vec32"])
	require.Equal(t, narrow, got.Properties["narrowed"])
	require.Equal(t, embedding, got.NamedEmbeddings["default"])

	nodesCSV := readExportedFile(t, outDir, "nodes.csv")
	header := nodesCSV[:strings.IndexByte(nodesCSV, '\n')]
	require.Contains(t, header, "vec:double[]")
	require.Contains(t, header, "narrowed:double[]")
	require.Contains(t, header, "vec32:vector{coordinateType:float,dimensions:3}")
	require.Contains(t, header, ":EMBEDDING(default)")
	for _, coordinate := range vector {
		require.Contains(t, nodesCSV, strconv.FormatFloat(coordinate, 'f', -1, 64))
	}
}

// TestNeo4jCSVImportDistinguishesCellsInCRLFAndGzipSources reads the convention
// from compressed and multi-file sources with CRLF line endings.
func TestNeo4jCSVImportDistinguishesCellsInCRLFAndGzipSources(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	first := filepath.Join(dir, "nodes-a.csv")
	require.NoError(t, os.WriteFile(first, []byte(":ID,name:string,nickname:string\r\nn1,Alice,\"\"\r\nn2,,\r\n"), 0o600))
	second := filepath.Join(dir, "nodes-b.csv.gz")
	writeGzipFile(t, second, "n3,\"\",Bob\r\n")

	// One source names both files, so the compressed file continues the first
	// file's columns like any other multi-file source.
	dest := importNeo4jCSVPackage(t, Options{}, []string{first + "," + second}, nil)

	n1, err := dest.GetNode(ctx, "n1")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "Alice", "nickname": ""}, n1.Properties)
	n2, err := dest.GetNode(ctx, "n2")
	require.NoError(t, err)
	require.Empty(t, n2.Properties)
	n3, err := dest.GetNode(ctx, "n3")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "", "nickname": "Bob"}, n3.Properties)
}

// TestNeo4jCSVExportImportPreservesQuotedMultilineAndDelimiterValues keeps the
// CSV quoting grammar the export already used: delimiters, quotes and multi-line
// values are quoted and escaped by the CSV writer and read back unchanged.
func TestNeo4jCSVExportImportPreservesQuotedMultilineAndDelimiterValues(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	source := storage.NewNamespacedEngine(base, "source")

	properties := map[string]any{
		"uid":    "q1",
		"note":   "line one\nline two, with comma and \"quotes\"",
		"padded": " padded",
		"quoted": `"`,
	}
	_, err := source.CreateNode(ctx, &storage.Node{ID: "q1", Labels: []string{"Quote"}, Properties: properties})
	require.NoError(t, err)

	dest, outDir := roundTripNeo4jCSV(t, source)

	got, err := dest.GetNode(ctx, "q1")
	require.NoError(t, err)
	require.Equal(t, properties, got.Properties)

	// The writer quotes the value and doubles its quotes, and a leading space
	// keeps its quoting too.
	nodesCSV := readExportedFile(t, outDir, "nodes.csv")
	require.Contains(t, nodesCSV, "\"line one\nline two, with comma and \"\"quotes\"\"\"")
	require.Contains(t, nodesCSV, "\" padded\"")
	require.Contains(t, nodesCSV, "\"\"\"\"")
}

// TestNeo4jCSVImportIgnoreEmptyStringsSkipsQuotedEmptyCells proves the present
// empty string is an empty string value: the ignore-empty-strings control drops
// it, while an absent property is dropped either way.
func TestNeo4jCSVImportIgnoreEmptyStringsSkipsQuotedEmptyCells(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(path, []byte(":ID,name:string,nickname:string\nn1,,\nn2,\"\",Bob\n"), 0o600))

	t.Run("ignored", func(t *testing.T) {
		dest := importNeo4jCSVPackage(t, Options{IgnoreEmptyStrings: true}, []string{path}, nil)
		preserved, err := dest.GetNode(ctx, "n2")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"nickname": "Bob"}, preserved.Properties)
		absent, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Empty(t, absent.Properties)
	})

	t.Run("preserved", func(t *testing.T) {
		dest := importNeo4jCSVPackage(t, Options{}, []string{path}, nil)
		preserved, err := dest.GetNode(ctx, "n2")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"name": "", "nickname": "Bob"}, preserved.Properties)
		absent, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Empty(t, absent.Properties)
	})
}

// quotedFields reports the quoted flag of every field, which is the part of a
// parsed record a text comparison cannot see.
func quotedFields(record csvRecord) []bool {
	quoted := make([]bool, len(record))
	for i := range record {
		quoted[i] = record[i].Quoted
	}
	return quoted
}

// TestCSVSourceReaderReportsQuotedFields pins the parser contract the import
// relies on: every field carries the text encoding/csv decoded plus whether the
// source quoted it, across CRLF line endings, a skipped blank line, and a
// multi-line value whose next field starts on a later line.
func TestCSVSourceReaderReportsQuotedFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.csv")
	content := strings.Join([]string{
		":ID,a:string,b:string,c:string",
		`n1,"",,x`,
		"",
		`n2,a,"b`,
		`c",""`,
		"",
	}, "\r\n")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))

	source, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer source.Close()

	header, err := source.ReadHeader()
	require.NoError(t, err)
	require.Equal(t, []string{":ID", "a:string", "b:string", "c:string"}, header)

	first, _, row, err := source.Read()
	require.NoError(t, err)
	require.Equal(t, 2, row)
	require.Equal(t, []string{"n1", "", "", "x"}, first.fieldTexts())
	require.Equal(t, []bool{false, true, false, false}, quotedFields(first))

	second, _, row, err := source.Read()
	require.NoError(t, err)
	require.Equal(t, 3, row)
	// encoding/csv normalizes the CRLF inside a quoted value to LF.
	require.Equal(t, []string{"n2", "a", "b\nc", ""}, second.fieldTexts())
	require.Equal(t, []bool{false, false, true, true}, quotedFields(second))

	_, _, _, err = source.Read()
	require.ErrorIs(t, err, io.EOF)
}

// TestCSVSourceReaderReportsEmptyFieldAtEndOfFile covers a file whose last field
// is an empty cell with no trailing newline: the position the parser reports is
// the end of the input, so the recorder has no byte to inspect.
func TestCSVSourceReaderReportsEmptyFieldAtEndOfFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nodes.csv")
	require.NoError(t, os.WriteFile(path, []byte(":ID,a:string,b:string\nn1,x,"), 0o600))

	source, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer source.Close()

	_, err = source.ReadHeader()
	require.NoError(t, err)
	record, _, row, err := source.Read()
	require.NoError(t, err)
	require.Equal(t, 2, row)
	require.Equal(t, []string{"n1", "x", ""}, record.fieldTexts())
	require.Equal(t, []bool{false, false, false}, quotedFields(record))
}

func TestCSVSourceReaderReleasesLargeRecordBuffer(t *testing.T) {
	path := filepath.Join(t.TempDir(), "large.csv")
	large := strings.Repeat("x", 8*csvRawReadAhead)
	require.NoError(t, os.WriteFile(path, []byte(":ID,value:string\nn1,"+large+"\nn2,\"\"\n"), 0o600))
	source, err := openCSVSource([]string{path}, Options{}.withDefaults())
	require.NoError(t, err)
	defer source.Close()
	_, err = source.ReadHeader()
	require.NoError(t, err)
	first, _, _, err := source.Read()
	require.NoError(t, err)
	require.Equal(t, large, first[1].Text)
	require.Greater(t, cap(source.raw.buf), 2*csvRawReadAhead)
	second, _, _, err := source.Read()
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, quotedFields(second))
	require.LessOrEqual(t, cap(source.raw.buf), 2*csvRawReadAhead, "later small records must not retain a previous large raw input buffer")
}
