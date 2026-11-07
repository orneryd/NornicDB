package apocexport

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

// exportCaller is the statement context a query executor supplies for a caller
// whose APOC I/O policy allows export file writes.
func exportCaller(t *testing.T) context.Context {
	t.Helper()
	return apocio.WithCapability(t.Context(), apocio.NewPolicy(apocio.Config{AllowExportFileAccess: true}).Capability())
}

// GraphMLAll must deliver the canonical GraphML document to the destination it
// was given: the named file, including a parent directory that does not exist
// yet, holds exactly the bytes the GraphML string API produces, and its graphMLData
// alias writes the same document instead of accepting the path and dropping it.
func TestGraphMLFileExportWritesCanonicalDocument(t *testing.T) {
	nodes := []*Node{{ID: 1, Properties: map[string]interface{}{"name": "a&b"}}}
	rels := []*Relationship(nil)
	want, err := GraphML(nodes, rels)
	require.NoError(t, err)
	for _, tc := range []struct {
		name   string
		export func(context.Context, []*Node, []*Relationship, string) error
	}{
		{"graphMLAll", GraphMLAll},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "graph.graphml")

			require.NoError(t, tc.export(exportCaller(t), nodes, rels, path))

			written, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, want, string(written))
			require.Contains(t, string(written), `<data key="name">a&amp;b</data>`)
		})
	}
}

// A call that names no destination has nothing to write and nothing to report,
// including for a caller with no export capability bound.
func TestGraphMLFileExportWithoutDestinationWritesNothing(t *testing.T) {
	require.NoError(t, GraphMLAll(exportCaller(t), []*Node{{ID: 1}}, nil, ""))
	require.NoError(t, GraphMLAll(context.Background(), []*Node{{ID: 1}}, nil, ""))
}
