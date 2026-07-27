package cypher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestApocLoadExportHelpers_ExtractLoadArg(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, "data.json", e.extractApocLoadArg("CALL apoc.load.json('data.json') YIELD value", "JSON"))
	assert.Equal(t, "https://example.com/a.csv", e.extractApocLoadArg("CALL apoc.load.csv(https://example.com/a.csv) YIELD map", "CSV"))
	assert.Equal(t, "", e.extractApocLoadArg("CALL somethingElse()", "JSON"))
}

func TestApocLoadExportHelpers_ExtractExportArgAndQuery(t *testing.T) {
	e := &StorageExecutor{}
	assert.Equal(t, "out.json", e.extractApocExportArg("CALL apoc.export.json.all('out.json', {})", "JSON"))
	assert.Equal(t, "out.csv", e.extractApocExportArg("CALL apoc.export.csv.query('MATCH (n) RETURN n', 'out.csv', {})", "CSV"))
	assert.Equal(t, "MATCH (n) RETURN n", e.extractApocExportQuery("CALL apoc.export.csv.query('MATCH (n) RETURN n', 'out.csv', {})"))
	assert.Equal(t, "", e.extractApocExportQuery("CALL apoc.export.csv.all('x.csv',{})"))
}

func TestApocLoadExportHelpers_ExportFormatting(t *testing.T) {
	e := &StorageExecutor{}
	nodes := []*storage.Node{{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}}
	edges := []*storage.Edge{{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"since": int64(2020)}}}

	nf := e.nodesToExportFormat(nodes)
	ef := e.edgesToExportFormat(edges)
	assert.Len(t, nf, 1)
	assert.Len(t, ef, 1)
	assert.Equal(t, "n1", nf[0]["id"])
	assert.Equal(t, "KNOWS", ef[0]["type"])
	assert.EqualValues(t, int64(2020), ef[0]["properties"].(map[string]interface{})["since"])
}

func TestApocLoadExportHelpers_CountProperties(t *testing.T) {
	e := &StorageExecutor{}
	nodes := []*storage.Node{{Properties: map[string]interface{}{"a": 1, "b": 2}}, {Properties: map[string]interface{}{}}}
	edges := []*storage.Edge{{Properties: map[string]interface{}{"c": 3}}}
	assert.Equal(t, 3, e.countProperties(nodes, edges))
	assert.Equal(t, 0, e.countProperties(nil, nil))
}

func TestApocLoadExportHelpers_CallApocLoadCsvParams_Delegates(t *testing.T) {
	e := &StorageExecutor{}
	// invalid invocation should return same validation error path as callApocLoadCsv
	_, err := e.callApocLoadCsvParams(context.TODO(), "CALL apoc.load.csvParams()")
	assert.Error(t, err)
}

func TestApocLoadExportHelpers_CallApocLoadJsonArray_FromFile(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	e := NewStorageExecutor(eng)
	e.SetAllowLocalAPOCFileAccess(true)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "arr.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`[1,{"a":2},3]`), 0o644))

	q := "CALL apoc.load.jsonArray('" + jsonPath + "') YIELD value RETURN value"
	res, err := e.callApocLoadJsonArray(context.Background(), q)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"value"}, res.Columns)
	assert.Len(t, res.Rows, 3)
}

func TestApocLoadExportHelpers_CallApocLoadJsonArray_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	e := NewStorageExecutor(eng)
	e.SetAllowLocalAPOCFileAccess(true)
	ctx := context.Background()

	_, err := e.callApocLoadJsonArray(ctx, "CALL apoc.load.jsonArray()")
	require.Error(t, err)

	dir := t.TempDir()
	objPath := filepath.Join(dir, "obj.json")
	require.NoError(t, os.WriteFile(objPath, []byte(`{"k":"v"}`), 0o644))

	// Non-array payload falls into default single-row branch.
	res, err := e.callApocLoadJsonArray(ctx, "CALL apoc.load.jsonArray('"+objPath+"') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	obj, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "v", obj["k"])

	// URL source branch.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`[{"x":1},{"x":2}]`))
	}))
	defer srv.Close()

	res, err = e.callApocLoadJsonArray(ctx, "CALL apoc.load.jsonArray('"+srv.URL+"') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	// Load failure branch.
	_, err = e.callApocLoadJsonArray(ctx, "CALL apoc.load.jsonArray('"+filepath.Join(dir, "missing.json")+"') YIELD value")
	require.Error(t, err)
}

func TestApocLoadExportHelpers_CallApocImportJson_FromFile(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()
	e := NewStorageExecutor(eng)
	e.SetAllowLocalAPOCFileAccess(true)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "graph.json")
	graphJSON := `{
		"nodes": [
			{"id":"n1","labels":["Person"],"properties":{"name":"alice"}},
			{"id":"n2","labels":["Person"],"properties":{"name":"bob"}}
		],
		"relationships": [
			{"id":"e1","type":"KNOWS","startNode":"n1","endNode":"n2","properties":{"since":2020}}
		]
	}`
	require.NoError(t, os.WriteFile(jsonPath, []byte(graphJSON), 0o644))

	q := "CALL apoc.import.json('" + jsonPath + "') YIELD source, nodes, relationships"
	res, err := e.callApocImportJson(context.Background(), q)
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Equal(t, []string{"source", "nodes", "relationships"}, res.Columns)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, jsonPath, res.Rows[0][0])
	assert.EqualValues(t, 2, res.Rows[0][1])
	assert.EqualValues(t, 1, res.Rows[0][2])
}

func TestApocLoadExportHelpers_LoadJsonFromURL_AndQueryExports(t *testing.T) {
	eng := newTestMemoryEngine(t)
	defer eng.Close()
	e := NewStorageExecutor(eng)
	e.SetAllowLocalAPOCFileAccess(true)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"x":1,"y":"z"}`))
		case "/bad":
			http.Error(w, "boom", http.StatusBadGateway)
		default:
			_, _ = w.Write([]byte(`not-json`))
		}
	}))
	defer srv.Close()

	data, err := e.loadJsonFromURL(srv.URL + "/ok")
	require.NoError(t, err)
	require.NotNil(t, data)
	_, err = e.loadJsonFromURL(srv.URL + "/bad")
	require.Error(t, err)
	_, err = e.loadJsonFromURL(srv.URL + "/malformed")
	require.Error(t, err)

	// File loader branches: missing file and malformed JSON.
	_, err = e.loadJsonFromFile(filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)
	badPath := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(badPath, []byte(`{"x":`), 0o644))
	_, err = e.loadJsonFromFile(badPath)
	require.Error(t, err)

	tmpDir := t.TempDir()
	jsonOut := filepath.Join(tmpDir, "q.json")
	csvOut := filepath.Join(tmpDir, "q.csv")

	jsonRes, err := e.callApocExportJsonQuery(context.Background(), "CALL apoc.export.json.query('RETURN 1 AS name', '"+jsonOut+"', {})")
	require.NoError(t, err)
	require.Len(t, jsonRes.Rows, 1)
	assert.Equal(t, jsonOut, jsonRes.Rows[0][0])
	assert.EqualValues(t, 1, jsonRes.Rows[0][1])
	_, err = os.Stat(jsonOut)
	require.NoError(t, err)

	csvRes, err := e.callApocExportCsvQuery(context.Background(), "CALL apoc.export.csv.query('RETURN 1 AS name', '"+csvOut+"', {})")
	require.NoError(t, err)
	require.Len(t, csvRes.Rows, 1)
	assert.Equal(t, csvOut, csvRes.Rows[0][0])
	assert.EqualValues(t, 1, csvRes.Rows[0][1])
	_, err = os.Stat(csvOut)
	require.NoError(t, err)

	_, err = e.callApocExportJsonQuery(context.Background(), "CALL apoc.export.json.query('', '"+jsonOut+"', {})")
	require.Error(t, err)
	_, err = e.callApocExportCsvQuery(context.Background(), "CALL apoc.export.csv.query('', '"+csvOut+"', {})")
	require.Error(t, err)

	// Query execution failure branch.
	_, err = e.callApocExportJsonQuery(context.Background(), "CALL apoc.export.json.query('THIS IS NOT CYPHER', '"+jsonOut+"', {})")
	require.Error(t, err)

	// File write failure branch (json.query does not auto-create dirs).
	badExportPath := filepath.Join(tmpDir, "missing", "nested", "q.json")
	_, err = e.callApocExportJsonQuery(context.Background(), "CALL apoc.export.json.query('RETURN 1 AS x', '"+badExportPath+"', {})")
	require.Error(t, err)
}

func TestApocLoadExportHelpers_CallApocLoadCsv_OptionsAndSources(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	ctx := context.Background()

	dir := t.TempDir()
	withHeader := filepath.Join(dir, "with_header.csv")
	noHeader := filepath.Join(dir, "no_header.csv")
	empty := filepath.Join(dir, "empty.csv")

	require.NoError(t, os.WriteFile(withHeader, []byte("name,age\nalice,30\nbob,31\n"), 0o644))
	require.NoError(t, os.WriteFile(noHeader, []byte("x;1\ny;2\n"), 0o644))
	require.NoError(t, os.WriteFile(empty, []byte(""), 0o644))

	res, err := exec.callApocLoadCsv(ctx, "CALL apoc.load.csv('"+withHeader+"') YIELD lineNo, list, map")
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "alice", res.Rows[0][2].(map[string]interface{})["name"])
	require.Equal(t, "31", res.Rows[1][2].(map[string]interface{})["age"])

	res, err = exec.callApocLoadCsv(ctx, "CALL apoc.load.csv('"+noHeader+"', {header:false, sep:';'}) YIELD lineNo, list, map")
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "x", res.Rows[0][1].([]interface{})[0])
	require.Empty(t, res.Rows[0][2].(map[string]interface{}))

	res, err = exec.callApocLoadCsv(ctx, "CALL apoc.load.csv('"+empty+"') YIELD lineNo, list, map")
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("c1,c2\nv1,v2\n"))
	}))
	defer srv.Close()

	res, err = exec.callApocLoadCsv(ctx, "CALL apoc.load.csv('"+srv.URL+"') YIELD lineNo, list, map")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "v1", res.Rows[0][2].(map[string]interface{})["c1"])
}

func TestApocLoadExportHelpers_CallApocLoadJson_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	ctx := context.Background()

	_, err := exec.callApocLoadJson(ctx, "CALL apoc.load.json()")
	require.Error(t, err)

	dir := t.TempDir()
	arrayPath := filepath.Join(dir, "arr.json")
	objectPath := filepath.Join(dir, "obj.json")
	scalarPath := filepath.Join(dir, "scalar.json")

	require.NoError(t, os.WriteFile(arrayPath, []byte(`[1,2,3]`), 0o644))
	require.NoError(t, os.WriteFile(objectPath, []byte(`{"a":1}`), 0o644))
	require.NoError(t, os.WriteFile(scalarPath, []byte(`42`), 0o644))

	res, err := exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+arrayPath+"') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)

	res, err = exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+objectPath+"') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	_, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)

	res, err = exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+scalarPath+"') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 42.0, res.Rows[0][0])
}

func TestApocLoadExportHelpers_LocalFileLoadsDeniedByDefault(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "payload.json")
	csvPath := filepath.Join(dir, "payload.csv")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"x":1}`), 0o644))
	require.NoError(t, os.WriteFile(csvPath, []byte("a,b\n1,2\n"), 0o644))

	_, err := exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+jsonPath+"') YIELD value")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)

	_, err = exec.callApocLoadCsv(ctx, "CALL apoc.load.csv('"+csvPath+"') YIELD lineNo, list, map")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)

	_, err = exec.callApocLoadJsonArray(ctx, "CALL apoc.load.jsonArray('"+jsonPath+"') YIELD value")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)

	_, err = exec.callApocImportJson(ctx, "CALL apoc.import.json('"+jsonPath+"') YIELD source, nodes, relationships")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)

	_, err = exec.callApocExportJsonAll(ctx, "CALL apoc.export.json.all('"+jsonPath+"', {})")
	require.ErrorIs(t, err, errAPOCLocalExportFileAccessDisabled)

	_, err = exec.callApocExportCsvQuery(ctx, "CALL apoc.export.csv.query('RETURN 1 AS x', '"+csvPath+"', {})")
	require.ErrorIs(t, err, errAPOCLocalExportFileAccessDisabled)
}

func TestApocLoadExportHelpers_ResolveAPOCLocalFilePath_MirrorsNeo4jRules(t *testing.T) {
	e := &StorageExecutor{}

	_, err := e.resolveAPOCLocalImportFilePath("file:///tmp/payload.json")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)

	e.SetAllowLocalAPOCFileAccess(true)
	importRoot := t.TempDir()
	e.SetAPOCLocalFileAccessRoot(importRoot)

	resolved, err := e.resolveAPOCLocalImportFilePath("file:///../safe/payload.json")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(importRoot, "safe", "payload.json"), resolved)

	resolved, err = e.resolveAPOCLocalImportFilePath("/tmp/../safe/payload.json")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(importRoot, "safe", "payload.json"), resolved)

	_, err = e.resolveAPOCLocalImportFilePath("file://localhost/safe/payload.json")
	require.ErrorIs(t, err, errAPOCFileAuthorityNotAllowed)

	_, err = e.resolveAPOCLocalImportFilePath("file:///safe/payload.json?x=1")
	require.ErrorIs(t, err, errAPOCFileQueryNotAllowed)

	_, err = e.resolveAPOCLocalImportFilePath("file:///safe/payload.json#frag")
	require.ErrorIs(t, err, errAPOCFileFragmentNotAllowed)
}

func TestApocLoadExportHelpers_ImportExportFileAccessCanBeSplit(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "payload.json")
	csvPath := filepath.Join(dir, "payload.csv")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"x":1}`), 0o644))

	exec.SetAllowLocalAPOCImportFileAccess(true)
	_, err := exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+jsonPath+"') YIELD value")
	require.NoError(t, err)
	_, err = exec.callApocExportCsvQuery(ctx, "CALL apoc.export.csv.query('RETURN 1 AS x', '"+csvPath+"', {})")
	require.ErrorIs(t, err, errAPOCLocalExportFileAccessDisabled)

	exec.SetAllowLocalAPOCImportFileAccess(false)
	exec.SetAllowLocalAPOCExportFileAccess(true)
	_, err = exec.callApocLoadJson(ctx, "CALL apoc.load.json('"+jsonPath+"') YIELD value")
	require.ErrorIs(t, err, errAPOCLocalImportFileAccessDisabled)
	_, err = exec.callApocExportCsvQuery(ctx, "CALL apoc.export.csv.query('RETURN 1 AS x', '"+csvPath+"', {})")
	require.NoError(t, err)
}

func TestApocLoadExportHelpers_FileURLReadsUseConfiguredImportRoot(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	importRoot := t.TempDir()
	exec.SetAPOCLocalFileAccessRoot(importRoot)
	ctx := context.Background()

	safeDir := filepath.Join(importRoot, "safe")
	require.NoError(t, os.MkdirAll(safeDir, 0o755))
	jsonPath := filepath.Join(safeDir, "payload.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"name":"neo"}`), 0o644))

	res, err := exec.callApocLoadJson(ctx, "CALL apoc.load.json('file:///../safe/payload.json') YIELD value")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	valueMap, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "neo", valueMap["name"])
}

func TestApocLoadExportHelpers_FileURLExportsUseConfiguredImportRoot(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	importRoot := t.TempDir()
	exec.SetAPOCLocalFileAccessRoot(importRoot)
	ctx := context.Background()

	outDir := filepath.Join(importRoot, "safe")
	require.NoError(t, os.MkdirAll(outDir, 0o755))

	_, err := exec.callApocExportCsvQuery(ctx, "CALL apoc.export.csv.query('RETURN 42 AS marker', 'file:///../safe/out.csv', {})")
	require.NoError(t, err)

	content, err := os.ReadFile(filepath.Join(outDir, "out.csv"))
	require.NoError(t, err)
	assert.Contains(t, string(content), "42")
}

func TestApocLoadExportHelpers_CallApocExportJsonAll_NoFile(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)

	res, err := exec.callApocExportJsonAll(ctx, "CALL apoc.export.json.all('', {})")
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	assert.Equal(t, "", res.Rows[0][0])
	assert.EqualValues(t, 1, res.Rows[0][1])
	assert.Contains(t, res.Rows[0][4].(string), "\"nodes\"")
}
func TestApocLoadExportHelpers_CallApocExportJsonAll_WriteError(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	ctx := context.Background()

	_, err := eng.CreateNode(&storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)

	// /dev/null is a file on Unix-like systems, so creating /dev/null/subdir should fail.
	_, err = exec.callApocExportJsonAll(ctx, "CALL apoc.export.json.all('/dev/null/subdir/out.json', {})")
	require.Error(t, err)
}
