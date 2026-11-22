package adminimport

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"strings"
	"testing"
)

func TestN15220NOR280PublicCSVLabelRoundTrip(t *testing.T) {
	for _, subject := range []struct {
		name          string
		labels        []string
		delimiter     rune
		ordinaryFirst bool
	}{
		{name: "ordinary", labels: []string{"Alpha", "Beta"}},
		{name: "default delimiter", labels: []string{"A;B"}},
		{name: "custom delimiter", labels: []string{"A|B"}, delimiter: '|'},
		{name: "alternate delimiter", labels: []string{"A;B"}, delimiter: '|'},
		{name: "mixed labels", labels: []string{"A;B", "Other"}},
		{name: "comma", labels: []string{"A,B"}},
		{name: "quote", labels: []string{"A\"B"}},
		{name: "newline", labels: []string{"A\nB"}},
		{name: "unicode", labels: []string{"Χαρά", "图"}},
		{name: "ambiguous after ordinary", labels: []string{"A;B"}, ordinaryFirst: true},
	} {
		t.Run(subject.name, func(t *testing.T) {
			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			source := storage.NewNamespacedEngine(base, "source")
			if subject.ordinaryFirst {
				_, err := source.CreateNode(t.Context(), &storage.Node{ID: "01-ordinary", Labels: []string{"Ordinary"}})
				require.NoError(t, err)
			}
			_, err := source.CreateNode(t.Context(), &storage.Node{ID: "02-subject", Labels: subject.labels})
			require.NoError(t, err)
			written, err := source.GetNode(t.Context(), "02-subject")
			require.NoError(t, err)
			out := filepath.Join(t.TempDir(), "csv")
			delimiter := subject.delimiter
			if delimiter == 0 {
				delimiter = ';'
			}
			ambiguous := false
			for _, label := range written.Labels {
				ambiguous = ambiguous || strings.ContainsRune(label, delimiter)
			}
			err = ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: out, ArrayDelimiter: subject.delimiter})
			if err != nil && ambiguous {
				require.Contains(t, strings.ToLower(err.Error()), "delimiter", "ambiguous label error must explain the delimiter boundary")
				require.Contains(t, err.Error(), string(delimiter))
				return
			}
			require.NoError(t, err)
			options := Options{ArrayDelimiter: subject.delimiter}
			nodes, edges, err := DiscoverNeo4jCSVSources(out, options)
			require.NoError(t, err)
			destination := importNeo4jCSVPackage(t, options, nodes, edges)
			got, err := destination.GetNode(t.Context(), "02-subject")
			require.NoError(t, err)
			require.Equal(t, written.Labels, got.Labels, "successful export/import must preserve exact label membership and order")
			if subject.ordinaryFirst {
				ordinary, err := destination.GetNode(t.Context(), "01-ordinary")
				require.NoError(t, err)
				require.Equal(t, []string{"Ordinary"}, ordinary.Labels)
			}
		})
	}
}
