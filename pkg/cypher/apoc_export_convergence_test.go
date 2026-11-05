package cypher

// PR1615 export convergence.
//
// The four apoc.export.* procedures own one argument/source/destination
// lifecycle (exportDeclaration + StorageExecutor.export) and keep only their
// format encoder and their public result row. These regressions cover that
// lifecycle:
//
//   - a real caller cancellation, fired before the call and from the export's own
//     graph read, with live controls on the same statements: the caller's error
//     comes back, no row is published, and neither the file nor (for a
//     whole-graph export) its parent directory is written. The seeded graph's
//     values cannot be encoded, so an export that started encoding despite the
//     stopped caller would fail with an encoding error instead of the caller's
//     cancellation;
//   - the shared observation stride of the encoding loops and the destination
//     writer, on the owners themselves: no page starts and no write happens after
//     the caller stopped;
//   - a source read or a value encoding that fails: the export fails instead of
//     returning a partial export as success;
//   - the preserved destination contract (the whole-graph procedures create the
//     file's parent directory, the query procedures do not), the preserved CSV
//     quoting, and the preserved public columns, rows and content.
//
// Existing direct regressions call the canonical export owner; public CALL
// regressions exercise declarations bound directly by the registry.

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// pr1615ExportPopulation exceeds one observation stride of the export encoding
// loops, so a canceled case can prove no page was encoded after the caller
// stopped.
const pr1615ExportPopulation = 2500

// pr1615NamespacedStore is the graph an export reads: a namespace wrapper, so the
// export sees the caller-facing IDs of its nodes.
func pr1615NamespacedStore(t *testing.T) storage.Engine {
	t.Helper()
	return storage.NewNamespacedEngine(newTestMemoryEngine(t), "test")
}

// pr1615ExportExecutor is one executor whose local export file access is allowed
// so its exports reach the capability's export path, with a temporary directory
// for their files.
func pr1615ExportExecutor(t *testing.T) (*StorageExecutor, string) {
	t.Helper()
	exec := NewStorageExecutor(pr1615NamespacedStore(t))
	exec.SetAllowLocalAPOCFileAccess(true)
	return exec, t.TempDir()
}

// pr1615ExportCall is one export invocation and where its file goes.
type pr1615ExportCall struct {
	name string
	// createsParents reports that the procedure creates the destination
	// directory of its file.
	createsParents bool
	call           func(exec *StorageExecutor, ctx context.Context, file string) (*ExecuteResult, error)
}

// pr1615Exports are the four public export procedures.
func pr1615Exports() []pr1615ExportCall {
	return []pr1615ExportCall{
		{
			name:           "apoc.export.json.all",
			createsParents: true,
			call: func(exec *StorageExecutor, ctx context.Context, file string) (*ExecuteResult, error) {
				return exportProcedure(exportJSONGraphDeclaration)(ctx, exec, "", []interface{}{file, map[string]interface{}{}})
			},
		},
		{
			name:           "apoc.export.csv.all",
			createsParents: true,
			call: func(exec *StorageExecutor, ctx context.Context, file string) (*ExecuteResult, error) {
				return exportProcedure(exportCSVGraphDeclaration)(ctx, exec, "", []interface{}{file, map[string]interface{}{}})
			},
		},
		{
			name: "apoc.export.json.query",
			call: func(exec *StorageExecutor, ctx context.Context, file string) (*ExecuteResult, error) {
				return exportProcedure(exportJSONQueryDeclaration)(ctx, exec, "", []interface{}{"MATCH (n:Export) RETURN n.value AS value", file, map[string]interface{}{}})
			},
		},
		{
			name: "apoc.export.csv.query",
			call: func(exec *StorageExecutor, ctx context.Context, file string) (*ExecuteResult, error) {
				return exportProcedure(exportCSVQueryDeclaration)(ctx, exec, "", []interface{}{"MATCH (n:Export) RETURN n.value AS value", file, map[string]interface{}{}})
			},
		},
	}
}

// pr1615ExportPath is where one export call writes its file: a directory the
// caller does not have yet for the whole-graph procedures, which create it, and
// the existing temporary directory for the query procedures, which do not.
func pr1615ExportPath(dir, name string, createsParents bool) string {
	if createsParents {
		return filepath.Join(dir, "made", name)
	}
	return filepath.Join(dir, name)
}

// pr1615SeedUnencodableGraph stores the nodes of one export whose value the JSON
// encoder cannot represent. A stopped caller's export that still started encoding
// fails on that value, so an encoding error instead of the caller's cancellation
// is the evidence that the stopped caller was ignored.
func pr1615SeedUnencodableGraph(t *testing.T, store storage.Engine) {
	t.Helper()
	for index := 0; index < pr1615ExportPopulation; index++ {
		_, err := store.CreateNode(t.Context(), &storage.Node{
			ID:         storage.NodeID(fmt.Sprintf("pr1615-node-%05d", index)),
			Labels:     []string{"Export"},
			Properties: map[string]interface{}{"value": math.NaN()},
		})
		require.NoError(t, err)
	}
}

// pr1615CancelingEngine is a storage engine whose node read fires the caller's
// CancelFunc and then returns the graph, so the cancellation arrives from inside
// the export's own source read.
type pr1615CancelingEngine struct {
	storage.Engine
	cancel context.CancelFunc
}

func (e *pr1615CancelingEngine) AllNodes(ctx context.Context) ([]*storage.Node, error) {
	nodes, err := e.Engine.AllNodes(ctx)
	e.cancel()
	return nodes, err
}

// pr1615RequireNoFile proves an export wrote nothing at its destination.
func pr1615RequireNoFile(t *testing.T, path string, createsParents bool) {
	t.Helper()
	_, statErr := os.Stat(path)
	require.True(t, os.IsNotExist(statErr), "a stopped caller's export must not write its file")
	if createsParents {
		_, dirErr := os.Stat(filepath.Dir(path))
		require.True(t, os.IsNotExist(dirErr), "a stopped caller's export must not create the destination directory")
	}
}

// TestAPOCExportCallerCancellationStopsExport proves a stopped caller never
// receives a successful export: for every procedure the caller's error comes
// back, no row is published and no file is written, whichever stage the
// cancellation arrives at.
func TestAPOCExportCallerCancellationStopsExport(t *testing.T) {
	for _, export := range pr1615Exports() {
		export := export
		t.Run(export.name+"/canceled-before-call", func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			pr1615SeedUnencodableGraph(t, exec.storage)
			ctx, cancel := context.WithCancel(apocIOTestContext(exec))
			cancel()

			out := pr1615ExportPath(dir, "export.out", export.createsParents)
			result, err := export.call(exec, ctx, out)

			require.Nil(t, result, "a stopped caller's export must not publish a result")
			require.ErrorIs(t, err, context.Canceled)
			pr1615RequireNoFile(t, out, export.createsParents)
		})
		t.Run(export.name+"/streamed/canceled", func(t *testing.T) {
			exec, _ := pr1615ExportExecutor(t)
			pr1615SeedUnencodableGraph(t, exec.storage)
			ctx, cancel := context.WithCancel(apocIOTestContext(exec))
			cancel()

			result, err := export.call(exec, ctx, "")

			require.Nil(t, result, "a stopped caller's streamed export must not publish a result")
			require.ErrorIs(t, err, context.Canceled)
		})
		t.Run(export.name+"/live", func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			_, seedErr := exec.storage.CreateNode(t.Context(), &storage.Node{
				ID:         "pr1615-node-live",
				Labels:     []string{"Export"},
				Properties: map[string]interface{}{"value": "pr1615"},
			})
			require.NoError(t, seedErr)
			ctx, cancel := context.WithCancel(apocIOTestContext(exec))
			defer cancel()

			out := pr1615ExportPath(dir, "export.out", export.createsParents)
			result, err := export.call(exec, ctx, out)

			require.NoError(t, err)
			require.NotNil(t, result)
			content, readErr := os.ReadFile(out)
			require.NoError(t, readErr, "a live caller's export writes its file")
			require.Contains(t, string(content), "pr1615")
		})
	}
}

// TestAPOCExportGraphReadCancellationStopsEncoding proves the whole-graph export
// encoding observes a cancellation that its own graph read fired: the export
// fails with the caller's cancellation and not with the encoding failure of the
// values it would have encoded.
func TestAPOCExportGraphReadCancellationStopsEncoding(t *testing.T) {
	for _, export := range pr1615Exports() {
		export := export
		if !export.createsParents {
			continue
		}
		t.Run(export.name, func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			pr1615SeedUnencodableGraph(t, exec.storage)
			ctx, cancel := context.WithCancel(apocIOTestContext(exec))
			defer cancel()
			canceling := NewStorageExecutor(&pr1615CancelingEngine{Engine: exec.storage, cancel: cancel})

			out := pr1615ExportPath(dir, "export.out", export.createsParents)
			result, err := export.call(canceling, ctx, out)

			require.Nil(t, result, "a stopped caller's export must not publish a result")
			require.ErrorIs(t, err, context.Canceled)
			require.NotContains(t, err.Error(), "failed to marshal",
				"no page of the export may be encoded after the caller stopped")
			pr1615RequireNoFile(t, out, export.createsParents)
		})
	}
}

// TestAPOCExportObservationStride proves the shared observation owner of the
// export encoding loops: no page starts after the caller stopped, and a caller
// that stops during the last page is reported instead of finishing successfully.
func TestAPOCExportObservationStride(t *testing.T) {
	const total = 3*pipelineMaterializedCancelCheck + 7

	t.Run("stopped-between-pages", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		visited := 0
		err := exportChunks(ctx, total, func(start, end int) error {
			visited += end - start
			if visited == 2*pipelineMaterializedCancelCheck {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, 2*pipelineMaterializedCancelCheck, visited,
			"no page starts after the caller stopped")
	})

	t.Run("stopped-during-last-page", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		visited := 0
		err := exportChunks(ctx, total, func(start, end int) error {
			visited += end - start
			if end == total {
				cancel()
			}
			return nil
		})
		require.ErrorIs(t, err, context.Canceled)
		require.EqualValues(t, total, visited)
	})

	t.Run("live", func(t *testing.T) {
		visited := 0
		require.NoError(t, exportChunks(context.Background(), total, func(start, end int) error {
			visited += end - start
			return nil
		}))
		require.EqualValues(t, total, visited)
	})
}

// TestAPOCExportWriteObservesCaller proves the destination writer of every export
// writes nothing for a stopped caller and reports that caller's error, and that
// the whole-graph writer alone creates the destination directory.
func TestAPOCExportWriteObservesCaller(t *testing.T) {
	t.Run("canceled", func(t *testing.T) {
		for _, scope := range []exportScope{exportGraphScope, exportQueryScope} {
			exec, dir := pr1615ExportExecutor(t)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			out := filepath.Join(dir, "made", "export.out")

			err := exec.writeExport(ctx, scope, out, exportPayload{content: []byte("pr1615")})

			require.ErrorIs(t, err, context.Canceled)
			_, statErr := os.Stat(out)
			require.True(t, os.IsNotExist(statErr), "a stopped caller's write must not create the file")
			_, dirErr := os.Stat(filepath.Dir(out))
			require.True(t, os.IsNotExist(dirErr), "a stopped caller's write must not create the directory")
		}
	})

	t.Run("live", func(t *testing.T) {
		exec, dir := pr1615ExportExecutor(t)
		out := filepath.Join(dir, "made", "export.out")

		require.NoError(t, exec.writeExport(context.Background(), exportGraphScope, out, exportPayload{content: []byte("pr1615")}))
		content, err := os.ReadFile(out)
		require.NoError(t, err)
		require.Equal(t, "pr1615", string(content))
	})
}

// pr1615FailingEdgesEngine is a storage engine whose relationship read fails
// while its other reads and its writes work.
type pr1615FailingEdgesEngine struct {
	storage.Engine
	err error
}

func (e *pr1615FailingEdgesEngine) AllEdges(ctx context.Context) ([]*storage.Edge, error) {
	return nil, e.err
}

// TestAPOCExportReportsIncompleteSource proves an export that cannot read its
// whole source fails instead of returning the nodes it could read as a
// successful export.
func TestAPOCExportReportsIncompleteSource(t *testing.T) {
	base := pr1615NamespacedStore(t)
	exec := NewStorageExecutor(&pr1615FailingEdgesEngine{Engine: base, err: errors.New("pr1615 relationship read failure")})

	result, err := exportProcedure(exportCSVGraphDeclaration)(context.Background(), exec, "", []interface{}{"", map[string]interface{}{}})

	require.Nil(t, result, "an export with an unreadable source publishes no result")
	require.ErrorContains(t, err, "failed to read relationships")
	require.ErrorContains(t, err, "pr1615 relationship read failure")
}

// TestAPOCExportReportsUnencodableValue proves an export that cannot encode one
// of its values fails instead of exporting an empty value in its place. A NaN
// number is a stored value the JSON encoder cannot represent.
func TestAPOCExportReportsUnencodableValue(t *testing.T) {
	cases := []struct {
		name string
		call func(exec *StorageExecutor) (*ExecuteResult, error)
		want string
	}{
		{
			name: "apoc.export.csv.all",
			call: func(exec *StorageExecutor) (*ExecuteResult, error) {
				return exportProcedure(exportCSVGraphDeclaration)(context.Background(), exec, "", []interface{}{"", map[string]interface{}{}})
			},
			want: "failed to marshal properties of node pr1615-unencodable",
		},
		{
			name: "apoc.export.csv.query",
			call: func(exec *StorageExecutor) (*ExecuteResult, error) {
				return exportProcedure(exportCSVQueryDeclaration)(context.Background(), exec, "", []interface{}{"MATCH (n:Unencodable) RETURN n.value AS value", "", map[string]interface{}{}})
			},
			want: `failed to marshal column "value"`,
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			exec := NewStorageExecutor(pr1615NamespacedStore(t))
			_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
				ID:         "pr1615-unencodable",
				Labels:     []string{"Unencodable"},
				Properties: map[string]interface{}{"value": math.NaN()},
			})
			require.NoError(t, err)

			result, err := tc.call(exec)

			require.Nil(t, result, "an export with an unencodable value publishes no result")
			require.ErrorContains(t, err, tc.want)
			require.ErrorContains(t, err, "NaN")
		})
	}
}

// TestAPOCExportDestinationContract preserves the destination split of the four
// procedures: the whole-graph exports create the file's parent directory, the
// query exports write only into a directory the caller already has.
func TestAPOCExportDestinationContract(t *testing.T) {
	for _, export := range pr1615Exports() {
		export := export
		t.Run(export.name, func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			out := pr1615ExportPath(dir, "export.out", export.createsParents)

			result, err := export.call(exec, apocIOTestContext(exec), out)

			require.NoError(t, err)
			require.NotNil(t, result)
			require.Equal(t, out, result.Rows[0][0])
			content, readErr := os.ReadFile(out)
			require.NoError(t, readErr)
			require.NotEmpty(t, content)
		})
		t.Run(export.name+"/missing-parent", func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			out := filepath.Join(dir, "made", "export.out")

			result, err := export.call(exec, apocIOTestContext(exec), out)

			if export.createsParents {
				require.NoError(t, err)
				_, statErr := os.Stat(out)
				require.NoError(t, statErr)
				return
			}
			require.Nil(t, result)
			require.ErrorContains(t, err, "failed to write file")
			_, statErr := os.Stat(out)
			require.True(t, os.IsNotExist(statErr))
			_, dirErr := os.Stat(filepath.Dir(out))
			require.True(t, os.IsNotExist(dirErr), "a query export must not create the destination directory")
		})
	}
}

// TestAPOCExportResultContract pins each procedure's public columns, row values
// and encoded content, so the shared lifecycle keeps every documented format.
func TestAPOCExportResultContract(t *testing.T) {
	newExecutor := func(t *testing.T) *StorageExecutor {
		t.Helper()
		exec := NewStorageExecutor(pr1615NamespacedStore(t))
		_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
			ID:         "n1",
			Labels:     []string{"Person"},
			Properties: map[string]interface{}{"name": "alice", "since": int64(2020)},
		})
		require.NoError(t, err)
		_, err = exec.storage.CreateNode(t.Context(), &storage.Node{ID: "n2", Labels: []string{"Person"}})
		require.NoError(t, err)
		require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{
			ID:         "e1",
			Type:       "KNOWS",
			StartNode:  "n1",
			EndNode:    "n2",
			Properties: map[string]interface{}{"since": int64(2020)},
		}))
		return exec
	}

	t.Run("apoc.export.json.all", func(t *testing.T) {
		result, err := exportProcedure(exportJSONGraphDeclaration)(context.Background(), newExecutor(t), "", []interface{}{"", map[string]interface{}{}})
		require.NoError(t, err)
		require.Equal(t, []string{"file", "nodes", "relationships", "properties", "data"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "", result.Rows[0][0])
		require.EqualValues(t, 2, result.Rows[0][1])
		require.EqualValues(t, 1, result.Rows[0][2])
		require.EqualValues(t, 3, result.Rows[0][3])

		var document map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(result.Rows[0][4].(string)), &document))
		byID := map[string]map[string]interface{}{}
		for _, encoded := range document["nodes"].([]interface{}) {
			node := encoded.(map[string]interface{})
			byID[node["id"].(string)] = node
		}
		require.Len(t, byID, 2)
		require.Equal(t, []interface{}{"Person"}, byID["n1"]["labels"])
		require.Equal(t, map[string]interface{}{"name": "alice", "since": float64(2020)}, byID["n1"]["properties"])
		require.Equal(t, []interface{}{"Person"}, byID["n2"]["labels"])
		require.Empty(t, byID["n2"]["properties"])
		require.Equal(t, []interface{}{
			map[string]interface{}{
				"id":         "e1",
				"type":       "KNOWS",
				"startNode":  "n1",
				"endNode":    "n2",
				"properties": map[string]interface{}{"since": float64(2020)},
			},
		}, document["relationships"])
	})

	t.Run("apoc.export.csv.all", func(t *testing.T) {
		result, err := exportProcedure(exportCSVGraphDeclaration)(context.Background(), newExecutor(t), "", []interface{}{"", map[string]interface{}{}})
		require.NoError(t, err)
		require.Equal(t, []string{"file", "nodes", "relationships", "rows"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "", result.Rows[0][0])
		require.EqualValues(t, 2, result.Rows[0][1])
		require.EqualValues(t, 1, result.Rows[0][2])
		require.EqualValues(t, 3, result.Rows[0][3])
	})

	t.Run("apoc.export.json.query", func(t *testing.T) {
		result, err := exportProcedure(exportJSONQueryDeclaration)(context.Background(), newExecutor(t), "", []interface{}{"RETURN 1 AS name", "", map[string]interface{}{}})
		require.NoError(t, err)
		require.Equal(t, []string{"file", "rows", "data"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "", result.Rows[0][0])
		require.EqualValues(t, 1, result.Rows[0][1])

		var document map[string]interface{}
		require.NoError(t, json.Unmarshal([]byte(result.Rows[0][2].(string)), &document))
		require.Equal(t, []interface{}{"name"}, document["columns"])
		require.Equal(t, []interface{}{[]interface{}{float64(1)}}, document["data"])
	})

	t.Run("apoc.export.csv.query", func(t *testing.T) {
		result, err := exportProcedure(exportCSVQueryDeclaration)(context.Background(), newExecutor(t), "", []interface{}{"RETURN 1 AS name", "", map[string]interface{}{}})
		require.NoError(t, err)
		require.Equal(t, []string{"file", "rows", "data"}, result.Columns)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "", result.Rows[0][0])
		require.EqualValues(t, 1, result.Rows[0][1])
		require.Equal(t, "name\n\"1\"\n", result.Rows[0][2])
	})
}

// TestAPOCExportCSVQuoting preserves the whole-graph CSV encoding of one record:
// labels joined with ';', property JSON quoted with doubled quotes, the
// relationship columns in their declared order, and both section headers.
func TestAPOCExportCSVQuoting(t *testing.T) {
	exec, dir := pr1615ExportExecutor(t)
	_, err := exec.storage.CreateNode(t.Context(), &storage.Node{
		ID:         "n1",
		Labels:     []string{"Person", "Employee"},
		Properties: map[string]interface{}{"name": `a "quoted" name`},
	})
	require.NoError(t, err)
	_, err = exec.storage.CreateNode(t.Context(), &storage.Node{ID: "n2", Labels: []string{"Person"}})
	require.NoError(t, err)
	require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{
		ID: "e1", Type: "KNOWS", StartNode: "n1", EndNode: "n2",
		Properties: map[string]interface{}{"since": int64(2020)},
	}))

	out := filepath.Join(dir, "export.csv")
	result, err := exportProcedure(exportCSVGraphDeclaration)(apocIOTestContext(exec), exec, "", []interface{}{out, map[string]interface{}{}})
	require.NoError(t, err)
	require.EqualValues(t, 2, result.Rows[0][1])
	raw, err := os.ReadFile(out)
	require.NoError(t, err)
	content := string(raw)

	properties, err := json.Marshal(map[string]interface{}{"name": `a "quoted" name`})
	require.NoError(t, err)
	require.Contains(t, content, "# Nodes\n_id,_labels,properties\n")
	require.Contains(t, content, fmt.Sprintf("n1,\"Person;Employee\",\"%s\"\n", strings.ReplaceAll(string(properties), `"`, `""`)))
	require.Contains(t, content, "\n# Relationships\n_id,_type,_start,_end,properties\n")
	require.Contains(t, content, `e1,KNOWS,n1,n2,"{""since"":2020}"`+"\n")
}

// TestAPOCExportArgumentErrors pins each procedure's argument errors: the shared
// lifecycle keeps the invoked procedure name and the declared argument name.
func TestAPOCExportArgumentErrors(t *testing.T) {
	cases := []struct {
		name, procedure, argument string
		call                      func() error
	}{
		{
			name: "apoc.export.json.all/file", procedure: "apoc.export.json.all", argument: "file",
			call: func() error {
				_, err := exportProcedure(exportJSONGraphDeclaration)(context.Background(), (&StorageExecutor{}), "", []interface{}{int64(1)})
				return err
			},
		},
		{
			name: "apoc.export.csv.all/file", procedure: "apoc.export.csv.all", argument: "file",
			call: func() error {
				_, err := exportProcedure(exportCSVGraphDeclaration)(context.Background(), (&StorageExecutor{}), "", []interface{}{int64(1)})
				return err
			},
		},
		{
			name: "apoc.export.json.query/query", procedure: "apoc.export.json.query", argument: "query",
			call: func() error {
				_, err := exportProcedure(exportJSONQueryDeclaration)(context.Background(), (&StorageExecutor{}), "", nil)
				return err
			},
		},
		{
			name: "apoc.export.json.query/file", procedure: "apoc.export.json.query", argument: "file",
			call: func() error {
				_, err := exportProcedure(exportJSONQueryDeclaration)(context.Background(), (&StorageExecutor{}), "", []interface{}{"RETURN 1", int64(1)})
				return err
			},
		},
		{
			name: "apoc.export.csv.query/query", procedure: "apoc.export.csv.query", argument: "query",
			call: func() error {
				_, err := exportProcedure(exportCSVQueryDeclaration)(context.Background(), (&StorageExecutor{}), "", nil)
				return err
			},
		},
		{
			name: "apoc.export.csv.query/file", procedure: "apoc.export.csv.query", argument: "file",
			call: func() error {
				_, err := exportProcedure(exportCSVQueryDeclaration)(context.Background(), (&StorageExecutor{}), "", []interface{}{"RETURN 1", int64(1)})
				return err
			},
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			require.Error(t, err)
			require.ErrorContains(t, err, tc.procedure)
			require.ErrorContains(t, err, tc.argument)
		})
	}
}

// Identifiers and aliases are data too: comma/quote characters must not
// change the CSV's field boundaries or make the exported records unreadable.
func TestAPOCExportCSVQuotesIdentifiersAndHeaders(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		t.Run("graph identifiers", func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			first, second := storage.NodeID("node,one"), storage.NodeID("node\"two")
			_, err := exec.storage.CreateNode(t.Context(), &storage.Node{ID: first, Labels: []string{"A\"B"}, Properties: map[string]interface{}{"name": "one"}})
			require.NoError(t, err)
			_, err = exec.storage.CreateNode(t.Context(), &storage.Node{ID: second, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "two"}})
			require.NoError(t, err)
			require.NoError(t, exec.storage.CreateEdge(t.Context(), &storage.Edge{ID: "edge,\"one", StartNode: first, EndNode: second, Type: "LINK,\"quoted", Properties: map[string]interface{}{"weight": int64(1)}}))
			path := filepath.Join(dir, "graph.csv")
			_, err = exec.Execute(context.Background(), "CALL apoc.export.csv.all($file,{})", map[string]interface{}{"file": path})
			require.NoError(t, err)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			reader := csv.NewReader(strings.NewReader(string(raw)))
			reader.Comment = '#'
			reader.FieldsPerRecord = -1
			records, err := reader.ReadAll()
			require.NoError(t, err)
			require.Len(t, records, 5)
			require.Equal(t, []string{"_id", "_labels", "properties"}, records[0])
			require.ElementsMatch(t, [][]string{{string(first), "A\"B", `{"name":"one"}`}, {string(second), "Person", `{"name":"two"}`}}, records[1:3])
			require.Equal(t, []string{"_id", "_type", "_start", "_end", "properties"}, records[3])
			require.Equal(t, []string{"edge,\"one", "LINK,\"quoted", string(first), string(second), `{"weight":1}`}, records[4])
		})
		t.Run("query header", func(t *testing.T) {
			exec, dir := pr1615ExportExecutor(t)
			path := filepath.Join(dir, "query.csv")
			_, err := exec.Execute(context.Background(), "CALL apoc.export.csv.query('RETURN 42 AS `a,b`',$file,{})", map[string]interface{}{"file": path})
			require.NoError(t, err)
			raw, err := os.ReadFile(path)
			require.NoError(t, err)
			records, err := csv.NewReader(strings.NewReader(string(raw))).ReadAll()
			require.NoError(t, err)
			require.Equal(t, [][]string{{"a,b"}, {"42"}}, records)
		})
	})
}
