package adminimport

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"
)

func TestImportRejectsTruncatedNodeIdentifierColumn(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes.csv")
	require.NoError(t, os.WriteFile(path, []byte("name:string,id:ID\nAlice\n"), 0o600))
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	_, err := ImportFull(t.Context(), base, Options{DatabaseName: "target", NodeSources: []string{path}, Now: fixedImportTime})
	require.ErrorContains(t, err, "missing ID column")
	require.ErrorContains(t, err, "column 2")
	nodes, readErr := storage.NewNamespacedEngine(base, "target").AllNodes(t.Context())
	require.NoError(t, readErr)
	require.Empty(t, nodes, "a truncated ID must not create an anonymous or empty-ID node")
}
