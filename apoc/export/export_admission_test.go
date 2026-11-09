package apocexport_test

// The apoc.export Go file entry points are the bundled plugin's real I/O
// handlers. These checks pin the admission contract they now share with the
// apoc.load Go handlers: the caller's context is the whole admission, a missing
// or denied capability writes nothing, and the destination is the one pkg/apocio
// resolves.

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/apoc"
	apocexport "github.com/orneryd/nornicdb/apoc/export"
	"github.com/orneryd/nornicdb/apoc/registry"
	apocstorage "github.com/orneryd/nornicdb/apoc/storage"
	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// exportCaller is the statement context a query executor supplies for a caller
// whose APOC I/O policy allows export file writes.
func exportCaller(t *testing.T) context.Context {
	t.Helper()
	return apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{AllowExportFileAccess: true}).Capability())
}

// exportFileHelpers is the whole Go file-export population: every entry point
// that names a destination for its caller's graph or text.
func exportFileHelpers(nodes []*apocexport.Node, rels []*apocexport.Relationship) map[string]func(context.Context, string) error {
	return map[string]func(context.Context, string) error{
		"JsonAll":    func(ctx context.Context, path string) error { return apocexport.JsonAll(ctx, nodes, rels, path) },
		"Csv":        func(ctx context.Context, path string) error { return apocexport.Csv(ctx, nodes, path) },
		"CypherAll":  func(ctx context.Context, path string) error { return apocexport.CypherAll(ctx, nodes, rels, path) },
		"GraphMLAll": func(ctx context.Context, path string) error { return apocexport.GraphMLAll(ctx, nodes, rels, path) },
		"ToFile":     func(ctx context.Context, path string) error { return apocexport.ToFile(ctx, "raw content", path) },
	}
}

// TestExportFileHelpersRefuseUnadmittedCallers proves the denial side of every
// file helper: a context with no capability, a bound but denied capability, and
// a zero capability bound over an allowed caller each fail with the owner's
// error and leave no file behind.
func TestExportFileHelpersRefuseUnadmittedCallers(t *testing.T) {
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}
	callers := []struct {
		name string
		ctx  context.Context
		want error
	}{
		{"missing capability", context.Background(), apocio.ErrCapabilityMissing},
		{"denied capability", apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{}).Capability()), apocio.ErrLocalExportFileAccessDisabled},
		{"shadowed capability", apocio.WithCapability(exportCaller(t), apocio.Capability{}), apocio.ErrCapabilityMissing},
	}
	for name, write := range exportFileHelpers(nodes, nil) {
		for _, caller := range callers {
			t.Run(name+"/"+caller.name, func(t *testing.T) {
				path := filepath.Join(t.TempDir(), "nested", "export.out")
				require.ErrorIs(t, write(caller.ctx, path), caller.want)
				_, err := os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist, "a refused export writes nothing")
			})
		}
	}
}

// TestExportFileHelpersWriteTheCanonicalDocument proves the allowed side: each
// file helper writes exactly the bytes its in-memory owner produces, including
// a parent directory the caller has not created yet.
func TestExportFileHelpersWriteTheCanonicalDocument(t *testing.T) {
	nodes := []*apocexport.Node{{ID: 1, Labels: []string{"Person"}, Properties: map[string]interface{}{"name": "a&b"}}}
	rels := []*apocexport.Relationship{{ID: 2, StartNode: 1, EndNode: 1, Type: "KNOWS", Properties: map[string]interface{}{"since": float64(2020)}}}
	jsonDocument, err := apocexport.Json(nodes, rels)
	require.NoError(t, err)
	graphMLDocument, err := apocexport.GraphML(nodes, rels)
	require.NoError(t, err)
	const csvDocument = "id,labels,name\n1,Person,a&b\n"

	for _, tc := range []struct {
		name string
		want string
	}{
		{"JsonAll", jsonDocument},
		{"Csv", csvDocument},
		{"CypherAll", apocexport.Cypher(nodes, rels)},
		{"GraphMLAll", graphMLDocument},
		{"ToFile", "raw content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "export.out")
			require.NoError(t, exportFileHelpers(nodes, rels)[tc.name](exportCaller(t), path))
			written, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(written))
		})
	}
}

// TestExportEmptyDestinationWritesNothing pins the established no-file behavior:
// a call that names no destination performs no I/O, so it needs no capability
// and reports no error.
func TestExportEmptyDestinationWritesNothing(t *testing.T) {
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}
	for name, write := range exportFileHelpers(nodes, nil) {
		t.Run(name, func(t *testing.T) {
			require.NoError(t, write(context.Background(), ""))
			require.NoError(t, write(exportCaller(t), ""))
		})
	}
}

// TestExportCanceledCallerWritesNothing proves the caller's lifetime is observed
// before any filesystem work: a caller that already stopped gets that error and
// no destination exists afterwards.
func TestExportCanceledCallerWritesNothing(t *testing.T) {
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}
	for name, write := range exportFileHelpers(nodes, nil) {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(exportCaller(t))
			cancel()
			path := filepath.Join(t.TempDir(), "nested", "export.out")
			require.ErrorIs(t, write(ctx, path), context.Canceled)
			_, err := os.Stat(path)
			require.ErrorIs(t, err, os.ErrNotExist)
		})
	}
}

// TestExportDestinationErrorsAreReturned proves the shared writer's failures
// reach the caller: a directory is not a writable export file, and a file cannot
// become the destination's parent directory.
func TestExportDestinationErrorsAreReturned(t *testing.T) {
	ctx := exportCaller(t)
	require.Error(t, apocexport.ToFile(ctx, "content", t.TempDir()), "a directory is not a writable export file")

	parent := filepath.Join(t.TempDir(), "not-a-directory")
	require.NoError(t, os.WriteFile(parent, []byte("existing"), 0o600))
	require.Error(t, apocexport.GraphMLAll(ctx, []*apocexport.Node{{ID: 1}}, nil, filepath.Join(parent, "graph.graphml")))
	written, err := os.ReadFile(parent)
	require.NoError(t, err)
	require.Equal(t, "existing", string(written))
}

// TestExportDestinationIsResolvedByTheExistingRootOwner proves the export
// helpers write where pkg/apocio resolves, not where the caller's raw path
// points: with an export root configured, a relative destination lands beneath
// it and an absolute destination named elsewhere is rebased beneath it instead.
func TestExportDestinationIsResolvedByTheExistingRootOwner(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	ctx := apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{
		AllowExportFileAccess: true,
		FileAccessRoot:        root,
	}).Capability())
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "value"}}}

	require.NoError(t, apocexport.JsonAll(ctx, nodes, nil, filepath.Join("nested", "graph.json")))
	written, err := os.ReadFile(filepath.Join(root, "nested", "graph.json"))
	require.NoError(t, err)
	require.NotEmpty(t, written)

	absolute := filepath.Join(outside, "graph.json")
	require.NoError(t, apocexport.JsonAll(ctx, nodes, nil, absolute))
	_, err = os.Stat(absolute)
	require.ErrorIs(t, err, os.ErrNotExist, "nothing is written outside the configured export root")
	files := rootedFiles(t, root)
	require.Len(t, files, 2, "the rebased destination is one more document beneath the export root")
	require.Contains(t, files, "nested/graph.json")
}

// rootedFiles lists the files beneath root as slash-separated relative paths.
func rootedFiles(t *testing.T, root string) []string {
	t.Helper()
	var files []string
	require.NoError(t, filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		files = append(files, filepath.ToSlash(relative))
		return nil
	}))
	return files
}

// TestRegisteredExportAliasesRequireAnAdmittedCallerContext exercises every
// bundled file name against the canonical context-first writer it registers.
func TestRegisteredExportAliasesRequireAnAdmittedCallerContext(t *testing.T) {
	require.NoError(t, apoc.Initialize(apocstorage.NewInMemoryStorage(), nil))
	nodes := []*apocexport.Node{{ID: 1, Properties: map[string]interface{}{"name": "a&b"}}}
	rels := []*apocexport.Relationship(nil)
	jsonText, err := apocexport.Json(nodes, rels)
	require.NoError(t, err)
	graphText, err := apocexport.GraphML(nodes, rels)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		prefix []interface{}
		want   string
	}{
		{"jsonAll", []interface{}{nodes, rels}, jsonText}, {"jsonData", []interface{}{nodes, rels}, jsonText},
		{"csv", []interface{}{nodes}, "id,labels,name\n1,,a&b\n"},
		{"csvAll", []interface{}{nodes}, "id,labels,name\n1,,a&b\n"},
		{"csvData", []interface{}{nodes}, "id,labels,name\n1,,a&b\n"},
		{"cypherAll", []interface{}{nodes, rels}, apocexport.Cypher(nodes, rels)},
		{"cypherData", []interface{}{nodes, rels}, apocexport.Cypher(nodes, rels)},
		{"graphMLAll", []interface{}{nodes, rels}, graphText}, {"graphMLData", []interface{}{nodes, rels}, graphText},
		{"toFile", []interface{}{"raw content"}, "raw content"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := "apoc.export." + tc.name
			path := filepath.Join(t.TempDir(), "out.data")
			args := append(tc.prefix, path)
			_, err := apoc.Call(name, args)
			require.ErrorIs(t, err, registry.ErrUnsupportedSignature)
			_, err = os.Stat(path)
			require.ErrorIs(t, err, os.ErrNotExist)
			canceled, cancel := context.WithCancel(exportCaller(t))
			cancel()
			for _, caller := range []struct {
				ctx  context.Context
				want error
			}{
				{t.Context(), apocio.ErrCapabilityMissing},
				{apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{}).Capability()), apocio.ErrLocalExportFileAccessDisabled},
				{canceled, context.Canceled},
			} {
				_, err = callRegisteredExport(t, name, caller.ctx, args...)
				require.ErrorIs(t, err, caller.want)
				_, err = os.Stat(path)
				require.ErrorIs(t, err, os.ErrNotExist)
			}
			_, err = callRegisteredExport(t, name, exportCaller(t), args...)
			require.NoError(t, err)
			written, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(written))
		})
	}
}
