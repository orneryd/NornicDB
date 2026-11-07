package apocexport

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
		export func([]*Node, []*Relationship, string) error
	}{
		{"graphMLAll", GraphMLAll},
		{"graphMLData", GraphMLData},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "nested", "graph.graphml")

			require.NoError(t, tc.export(nodes, rels, path))

			written, err := os.ReadFile(path)
			require.NoError(t, err)
			require.Equal(t, want, string(written))
			require.Contains(t, string(written), `<data key="name">a&amp;b</data>`)
		})
	}
}

// A call that names no destination has nothing to write and nothing to report.
func TestGraphMLFileExportWithoutDestinationWritesNothing(t *testing.T) {
	require.NoError(t, GraphMLAll([]*Node{{ID: 1}}, nil, ""))
}
