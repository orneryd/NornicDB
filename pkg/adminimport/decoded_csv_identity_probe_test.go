package adminimport

import (
	"context"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedCSVNameRoundTrip(t *testing.T) {
	for _, tc := range []struct{ name, label, property, embedding string }{
		{"ordinary", "Ordinary", "property", "embedding"},
		{"space_label", " ", "property", "embedding"},
		{"surrounding_label", " Label ", "property", "embedding"},
		{"colon_property", "Ordinary", "a:b", "embedding"},
		{"space_property", "Ordinary", " ", "embedding"},
		{"brace_embedding", "Ordinary", "property", "embedding{part}"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fixtureowner.OwnChecked(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			source := storage.NewNamespacedEngine(base, "source")
			_, err := source.CreateNode(&storage.Node{ID: "subject", Labels: []string{tc.label}, Properties: map[string]any{tc.property: int64(7)}, NamedEmbeddings: map[string][]float32{tc.embedding: {1, 0}}})
			require.NoError(t, err)
			original, err := source.GetNode("subject")
			require.NoError(t, err)
			require.Equal(t, []string{tc.label}, original.Labels)
			require.Contains(t, original.Properties, tc.property)
			require.Contains(t, original.NamedEmbeddings, tc.embedding)
			dir := t.TempDir()
			require.NoError(t, ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: dir}))
			nodes, relationships, err := DiscoverNeo4jCSVSources(dir, Options{})
			require.NoError(t, err)
			destination := fixtureowner.OwnChecked(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			_, err = ImportFull(context.Background(), destination, Options{DatabaseName: "destination", NodeSources: nodes, RelSources: relationships, Now: fixedImportTime})
			require.NoError(t, err)
			copied, err := storage.NewNamespacedEngine(destination, "destination").GetNode("subject")
			require.NoError(t, err)
			require.Equal(t, original.Labels, copied.Labels)
			require.Contains(t, copied.Properties, tc.property)
			require.Equal(t, int64(7), copied.Properties[tc.property])
			require.Equal(t, original.NamedEmbeddings, copied.NamedEmbeddings)
		})
	}
}
