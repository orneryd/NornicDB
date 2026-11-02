package cypher

import (
	"context"
	"encoding/csv"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	apocload "github.com/orneryd/nornicdb/apoc/load"
	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(req *http.Request) (*http.Response, error)

func (fn roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

// apocIOTestContext is the caller context a standalone Go caller supplies: the
// executor's APOC I/O capability, bound explicitly. Resolving it at call time
// keeps each test's configuration order meaningful.
func apocIOTestContext(e *StorageExecutor) context.Context {
	return apocio.WithCapability(context.Background(), e.apocIO.Capability())
}

func allowPublicAPOCTestHost(exec *StorageExecutor) {
	exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
	})
}

func TestApocLoadExportHelpers_ExportFormatting(t *testing.T) {
	e := &StorageExecutor{}
	nodes := []*storage.Node{{ID: "n1", Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "alice"}}}
	edges := []*storage.Edge{{ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2", Properties: map[string]interface{}{"since": int64(2020)}}}

	nf := e.appendNodesToExportFormat(nil, nodes)
	ef := e.appendEdgesToExportFormat(nil, edges)
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

func TestApocLoadExportHelpers_CallApocLoadJsonArray_FromFile(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	e := NewStorageExecutor(eng)
	e.SetAllowLocalAPOCFileAccess(true)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "arr.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`[1,{"a":2},3]`), 0o644))

	res, err := jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(e), e, "", []interface{}{jsonPath})
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
	e.SetAllowRemoteAPOCURLAccess(true)
	e.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	allowPublicAPOCTestHost(e)

	_, err := jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(e), e, "", nil)
	require.Error(t, err)

	dir := t.TempDir()
	objPath := filepath.Join(dir, "obj.json")
	require.NoError(t, os.WriteFile(objPath, []byte(`{"k":"v"}`), 0o644))

	// Non-array payload falls into default single-row branch.
	res, err := jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(e), e, "", []interface{}{objPath})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	obj, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "v", obj["k"])

	// URL source branch.
	e.apocIO = e.apocIO.WithHTTPClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader(`[{"x":1},{"x":2}]`)),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	})
	res, err = jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(e), e, "", []interface{}{"https://example.com/array.json"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)

	// Load failure branch.
	_, err = jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(e), e, "", []interface{}{filepath.Join(dir, "missing.json")})
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

	res, err := e.callApocImportJson(apocIOTestContext(e), []interface{}{jsonPath})
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
	e.SetAllowRemoteAPOCURLAccess(true)
	e.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	allowPublicAPOCTestHost(e)
	e.apocIO = e.apocIO.WithHTTPClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			switch req.URL.Path {
			case "/ok":
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader(`{"x":1,"y":"z"}`)),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			case "/bad":
				return &http.Response{
					StatusCode: http.StatusBadGateway,
					Status:     "502 Bad Gateway",
					Body:       io.NopCloser(strings.NewReader("boom")),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			default:
				return &http.Response{
					StatusCode: http.StatusOK,
					Status:     "200 OK",
					Body:       io.NopCloser(strings.NewReader("not-json")),
					Header:     make(http.Header),
					Request:    req,
				}, nil
			}
		}),
	})
	data, err := apocload.Json(apocIOTestContext(e), "https://example.com/ok")
	require.NoError(t, err)
	require.NotNil(t, data)
	_, err = apocload.Json(apocIOTestContext(e), "https://example.com/bad")
	require.Error(t, err)
	_, err = apocload.Json(apocIOTestContext(e), "https://example.com/malformed")
	require.Error(t, err)

	// File loader branches: missing file and malformed JSON.
	_, err = apocload.Json(apocIOTestContext(e), filepath.Join(t.TempDir(), "missing.json"))
	require.Error(t, err)
	badPath := filepath.Join(t.TempDir(), "bad.json")
	require.NoError(t, os.WriteFile(badPath, []byte(`{"x":`), 0o644))
	_, err = apocload.Json(apocIOTestContext(e), badPath)
	require.Error(t, err)

	tmpDir := t.TempDir()
	jsonOut := filepath.Join(tmpDir, "q.json")
	csvOut := filepath.Join(tmpDir, "q.csv")

	jsonRes, err := exportProcedure(exportJSONQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"RETURN 1 AS name", jsonOut, map[string]interface{}{}})
	require.NoError(t, err)
	require.Len(t, jsonRes.Rows, 1)
	assert.Equal(t, jsonOut, jsonRes.Rows[0][0])
	assert.EqualValues(t, 1, jsonRes.Rows[0][1])
	_, err = os.Stat(jsonOut)
	require.NoError(t, err)

	csvRes, err := exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"RETURN 1 AS name", csvOut, map[string]interface{}{}})
	require.NoError(t, err)
	require.Len(t, csvRes.Rows, 1)
	assert.Equal(t, csvOut, csvRes.Rows[0][0])
	assert.EqualValues(t, 1, csvRes.Rows[0][1])
	_, err = os.Stat(csvOut)
	require.NoError(t, err)

	_, err = exportProcedure(exportJSONQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"", jsonOut, map[string]interface{}{}})
	require.Error(t, err)
	_, err = exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"", csvOut, map[string]interface{}{}})
	require.Error(t, err)

	// Query execution failure branch.
	_, err = exportProcedure(exportJSONQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"THIS IS NOT CYPHER", jsonOut, map[string]interface{}{}})
	require.Error(t, err)

	// File write failure branch (json.query does not auto-create dirs).
	badExportPath := filepath.Join(tmpDir, "missing", "nested", "q.json")
	_, err = exportProcedure(exportJSONQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"RETURN 1 AS x", badExportPath, map[string]interface{}{}})
	require.Error(t, err)
}

func TestApocLoadExportHelpers_CallApocLoadCsv_OptionsAndSources(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)
	exec.SetAllowRemoteAPOCURLAccess(true)
	exec.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	allowPublicAPOCTestHost(exec)

	dir := t.TempDir()
	withHeader := filepath.Join(dir, "with_header.csv")
	noHeader := filepath.Join(dir, "no_header.csv")
	empty := filepath.Join(dir, "empty.csv")

	require.NoError(t, os.WriteFile(withHeader, []byte("name,age\nalice,30\nbob,31\n"), 0o644))
	require.NoError(t, os.WriteFile(noHeader, []byte("x;1\ny;2\n"), 0o644))
	require.NoError(t, os.WriteFile(empty, []byte(""), 0o644))

	res, err := exec.callApocLoadCsv(apocIOTestContext(exec), []interface{}{withHeader})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "alice", res.Rows[0][2].(map[string]interface{})["name"])
	require.Equal(t, "31", res.Rows[1][2].(map[string]interface{})["age"])

	res, err = exec.callApocLoadCsv(apocIOTestContext(exec), []interface{}{noHeader, map[string]interface{}{"header": false, "sep": ";"}})
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
	require.Equal(t, "x", res.Rows[0][1].([]interface{})[0])
	require.Empty(t, res.Rows[0][2].(map[string]interface{}))

	res, err = exec.callApocLoadCsv(apocIOTestContext(exec), []interface{}{empty})
	require.NoError(t, err)
	require.Empty(t, res.Rows)

	exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{
		Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusOK,
				Status:     "200 OK",
				Body:       io.NopCloser(strings.NewReader("c1,c2\nv1,v2\n")),
				Header:     make(http.Header),
				Request:    req,
			}, nil
		}),
	})
	res, err = exec.callApocLoadCsv(apocIOTestContext(exec), []interface{}{"https://example.com/data.csv"})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.Equal(t, "v1", res.Rows[0][2].(map[string]interface{})["c1"])
}

func TestApocLoadExportHelpers_CallApocLoadJson_Branches(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)
	exec.SetAllowLocalAPOCFileAccess(true)

	_, err := jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", nil)
	require.Error(t, err)

	dir := t.TempDir()
	arrayPath := filepath.Join(dir, "arr.json")
	objectPath := filepath.Join(dir, "obj.json")
	scalarPath := filepath.Join(dir, "scalar.json")

	require.NoError(t, os.WriteFile(arrayPath, []byte(`[1,2,3]`), 0o644))
	require.NoError(t, os.WriteFile(objectPath, []byte(`{"a":1}`), 0o644))
	require.NoError(t, os.WriteFile(scalarPath, []byte(`42`), 0o644))

	res, err := jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{arrayPath})
	require.NoError(t, err)
	require.Len(t, res.Rows, 3)

	res, err = jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{objectPath})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	_, ok := res.Rows[0][0].(map[string]interface{})
	require.True(t, ok)

	res, err = jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{scalarPath})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	require.EqualValues(t, 42.0, res.Rows[0][0])
}

// TestApocLoadExportHelpers_NativeProceduresRequireCapability proves the native
// APOC I/O procedures read the statement's one typed capability: a context that
// carries none performs no I/O even when the executor's policy allows it. A
// missing capability has no unrestricted fallback.
func TestApocLoadExportHelpers_NativeProceduresRequireCapability(t *testing.T) {
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "payload.json")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"k":"v"}`), 0o644))

	e := NewStorageExecutor(newTestMemoryEngine(t))
	e.SetAllowLocalAPOCFileAccess(true)
	e.SetAllowRemoteAPOCURLAccess(true)
	e.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	allowPublicAPOCTestHost(e)

	_, err := jsonLoadProcedure("apoc.load.json")(context.Background(), e, "", []interface{}{jsonPath})
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)

	csvOut := filepath.Join(dir, "out.csv")
	_, err = exportProcedure(exportCSVQueryDeclaration)(context.Background(), e, "", []interface{}{"RETURN 1 AS x", csvOut, map[string]interface{}{}})
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)
	_, statErr := os.Stat(csvOut)
	require.True(t, os.IsNotExist(statErr), "a context without a capability must not write the export file")

	// The same calls through the caller-bound capability succeed.
	res, err := jsonLoadProcedure("apoc.load.json")(apocIOTestContext(e), e, "", []interface{}{jsonPath})
	require.NoError(t, err)
	require.Len(t, res.Rows, 1)
	_, err = exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(e), e, "", []interface{}{"RETURN 1 AS x", csvOut, map[string]interface{}{}})
	require.NoError(t, err)
}

func TestApocLoadExportHelpers_RemoteURLAdmission(t *testing.T) {
	for _, policy := range []struct {
		name      string
		enabled   bool
		allowlist []string
		want      error
	}{
		{name: "disabled", want: apocio.ErrRemoteURLAccessDisabled},
		{name: "enabled without allowlist", enabled: true, want: apocio.ErrRemoteURLHostNotAllowed},
		{name: "different allowlisted host", enabled: true, allowlist: []string{"api.example.com"}, want: apocio.ErrRemoteURLHostNotAllowed},
	} {
		t.Run(policy.name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			exec.SetAllowRemoteAPOCURLAccess(policy.enabled)
			exec.SetAPOCRemoteURLAllowlist(policy.allowlist)
			var resolutions, requests atomic.Int64
			exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
				resolutions.Add(1)
				return nil, errors.New("unexpected DNS after policy rejection")
			}).WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				requests.Add(1)
				return nil, errors.New("unexpected HTTP after policy rejection")
			})})
			for _, operation := range []string{"Go JSON", "apoc.load.json", "apoc.load.jsonArray", "apoc.load.csv", "apoc.import.json"} {
				t.Run(operation, func(t *testing.T) {
					const source = "https://example.com/data"
					var err error
					if operation == "Go JSON" {
						_, err = apocload.Json(apocIOTestContext(exec), source)
					} else {
						result, callErr := exec.Execute(context.Background(), "CALL "+operation+"($source)", map[string]interface{}{"source": source})
						err = callErr
						require.Nil(t, result)
					}
					require.ErrorIs(t, err, policy.want)
					require.Zero(t, resolutions.Load())
					require.Zero(t, requests.Load())
				})
			}
		})
	}
}

func TestApocLoadExportHelpers_LocalFileLoadsDeniedByDefault(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "payload.json")
	csvPath := filepath.Join(dir, "payload.csv")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"x":1}`), 0o644))
	require.NoError(t, os.WriteFile(csvPath, []byte("a,b\n1,2\n"), 0o644))

	_, err := jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{jsonPath})
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)

	_, err = exec.callApocLoadCsv(apocIOTestContext(exec), []interface{}{csvPath})
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)

	_, err = jsonLoadProcedure("apoc.load.jsonArray")(apocIOTestContext(exec), exec, "", []interface{}{jsonPath})
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)

	_, err = exec.callApocImportJson(apocIOTestContext(exec), []interface{}{jsonPath})
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)

	_, err = exportProcedure(exportJSONGraphDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{jsonPath, map[string]interface{}{}})
	require.ErrorIs(t, err, apocio.ErrLocalExportFileAccessDisabled)

	_, err = exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{"RETURN 1 AS x", csvPath, map[string]interface{}{}})
	require.ErrorIs(t, err, apocio.ErrLocalExportFileAccessDisabled)
}

func TestApocLoadExportHelpers_ResolveAPOCLocalFilePath_MirrorsNeo4jRules(t *testing.T) {
	e := &StorageExecutor{}

	_, err := e.apocIO.Capability().ImportPath("file:///tmp/payload.json")
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)

	e.SetAllowLocalAPOCFileAccess(true)
	importRoot := t.TempDir()
	e.SetAPOCLocalFileAccessRoot(importRoot)

	resolved, err := e.apocIO.Capability().ImportPath("file:///../safe/payload.json")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(importRoot, "safe", "payload.json"), resolved)

	resolved, err = e.apocIO.Capability().ImportPath("/tmp/../safe/payload.json")
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(importRoot, "safe", "payload.json"), resolved)

	_, err = e.apocIO.Capability().ImportPath("file://localhost/safe/payload.json")
	require.ErrorIs(t, err, apocio.ErrFileAuthorityNotAllowed)

	_, err = e.apocIO.Capability().ImportPath("file:///safe/payload.json?x=1")
	require.ErrorIs(t, err, apocio.ErrFileQueryNotAllowed)

	_, err = e.apocIO.Capability().ImportPath("file:///safe/payload.json#frag")
	require.ErrorIs(t, err, apocio.ErrFileFragmentNotAllowed)
}

func TestApocLoadExportHelpers_ImportExportFileAccessCanBeSplit(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "payload.json")
	csvPath := filepath.Join(dir, "payload.csv")
	require.NoError(t, os.WriteFile(jsonPath, []byte(`{"x":1}`), 0o644))

	exec.SetAllowLocalAPOCImportFileAccess(true)
	_, err := jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{jsonPath})
	require.NoError(t, err)
	_, err = exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{"RETURN 1 AS x", csvPath, map[string]interface{}{}})
	require.ErrorIs(t, err, apocio.ErrLocalExportFileAccessDisabled)

	exec.SetAllowLocalAPOCImportFileAccess(false)
	exec.SetAllowLocalAPOCExportFileAccess(true)
	_, err = jsonLoadProcedure("apoc.load.json")(apocIOTestContext(exec), exec, "", []interface{}{jsonPath})
	require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)
	_, err = exportProcedure(exportCSVQueryDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{"RETURN 1 AS x", csvPath, map[string]interface{}{}})
	require.NoError(t, err)
}

func TestApocLoadExportHelpers_FileURLUsesConfiguredImportRoot(t *testing.T) {
	for _, operation := range []string{"read JSON", "write CSV"} {
		t.Run(operation, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
			exec.SetAllowLocalAPOCFileAccess(true)
			root := t.TempDir()
			exec.SetAPOCLocalFileAccessRoot(root)
			safe := filepath.Join(root, "safe")
			require.NoError(t, os.MkdirAll(safe, 0o755))
			if operation == "read JSON" {
				require.NoError(t, os.WriteFile(filepath.Join(safe, "payload.json"), []byte(`{"name":"neo"}`), 0o644))
				result, err := exec.Execute(context.Background(), "CALL apoc.load.json($source)", map[string]interface{}{"source": "file:///../safe/payload.json"})
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				require.Equal(t, map[string]interface{}{"name": "neo"}, result.Rows[0][0])
				return
			}
			result, err := exec.Execute(context.Background(), "CALL apoc.export.csv.query('RETURN 42 AS marker',$file,{})", map[string]interface{}{"file": "file:///../safe/out.csv"})
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)
			content, err := os.ReadFile(filepath.Join(safe, "out.csv"))
			require.NoError(t, err)
			records, err := csv.NewReader(strings.NewReader(string(content))).ReadAll()
			require.NoError(t, err)
			require.Equal(t, [][]string{{"marker"}, {"42"}}, records)
		})
	}
}

func TestApocLoadExportHelpers_CallApocExportJsonAll_NoFile(t *testing.T) {
	base := newTestMemoryEngine(t)
	eng := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(eng)

	_, err := eng.CreateNode(t.Context(), &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)

	res, err := exportProcedure(exportJSONGraphDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{"", map[string]interface{}{}})
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

	_, err := eng.CreateNode(t.Context(), &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person"},
		Properties: map[string]interface{}{"name": "alice"},
	})
	require.NoError(t, err)

	parent := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(parent, []byte("file"), 0o600))
	_, err = exportProcedure(exportJSONGraphDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{filepath.Join(parent, "subdir", "out.json"), map[string]interface{}{}})
	require.Error(t, err)
}

// TestApocLoadExportHelpers_NativeJSONNamesShareDocumentItems proves both
// native CALL names publish the canonical DocumentItems row shape from the one
// factory: an object is one row for both names. A malformed document keeps the
// scalar loader's error prefix. Strict arrays belong to the plugin's
// JSONItems(RequireArray) binding, not to the native apoc.load.jsonArray name.
func TestApocLoadExportHelpers_NativeJSONNamesShareDocumentItems(t *testing.T) {
	exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "test"))
	exec.SetAllowLocalAPOCFileAccess(true)

	dir := t.TempDir()
	objectPath := filepath.Join(dir, "object.json")
	require.NoError(t, os.WriteFile(objectPath, []byte(`{"k":"v"}`), 0o644))
	malformedPath := filepath.Join(dir, "malformed.json")
	require.NoError(t, os.WriteFile(malformedPath, []byte(`{"k":`), 0o644))

	for _, name := range []string{"apoc.load.json", "apoc.load.jsonArray"} {
		res, err := jsonLoadProcedure(name)(apocIOTestContext(exec), exec, "", []interface{}{objectPath})
		require.NoError(t, err, name)
		require.Len(t, res.Rows, 1, name)
		require.Equal(t, map[string]interface{}{"k": "v"}, res.Rows[0][0], name)

		_, err = jsonLoadProcedure(name)(apocIOTestContext(exec), exec, "", []interface{}{malformedPath})
		require.Error(t, err, name)
		require.True(t, strings.HasPrefix(err.Error(), "failed to load JSON: "),
			"%s keeps the canonical prefix: %v", name, err)
	}
}
