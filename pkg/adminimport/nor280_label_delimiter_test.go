package adminimport

import (
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNOR280PublicCSVLabelRoundTrip(t *testing.T) {
	for _, subject := range []struct {
		name            string
		labels          []string
		delimiter       rune
		ordinaryFirst   bool
		protectExisting bool
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
		{name: "leading delimiter", labels: []string{";A"}},
		{name: "trailing delimiter", labels: []string{"A;"}},
		{name: "repeated delimiter", labels: []string{"A;;B"}},
		{name: "delimiter only", labels: []string{";"}},
		{name: "Unicode delimiter", labels: []string{"AλB"}, delimiter: 'λ'},
		{name: "Unicode alternate delimiter", labels: []string{"A;B"}, delimiter: 'λ'},
		{name: "empty label", labels: []string{""}},
		{name: "leading space", labels: []string{" Alpha"}},
		{name: "trailing newline", labels: []string{"Alpha\n"}},
		{name: "Unicode surrounding whitespace", labels: []string{"Alpha\u2003"}},
		{name: "internal space", labels: []string{"Alpha Beta"}},
		{name: "preserve existing output", labels: []string{"A;B"}, ordinaryFirst: true, protectExisting: true},
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
			existing := make(map[string]string)
			if subject.protectExisting {
				require.NoError(t, os.MkdirAll(out, 0o755))
				for _, name := range []string{"nodes.csv", "relationships.csv", Neo4jCSVSchemaFileName, Neo4jCSVNornicSchemaFileName} {
					existing[name] = "preserved " + name
					require.NoError(t, os.WriteFile(filepath.Join(out, name), []byte(existing[name]), 0o600))
				}
			}
			delimiter := subject.delimiter
			if delimiter == 0 {
				delimiter = ';'
			}
			ambiguous := false
			normalized := false
			for _, label := range written.Labels {
				ambiguous = ambiguous || strings.ContainsRune(label, delimiter)
				normalized = normalized || label == "" || strings.TrimSpace(label) != label
			}
			err = ExportNeo4jCSV(source, Neo4jCSVExportOptions{OutputDir: out, ArrayDelimiter: subject.delimiter})
			if (ambiguous || normalized) && err != nil {
				var exportErr *Error
				require.ErrorAs(t, err, &exportErr)
				require.Equal(t, ExitUnsupported, exportErr.ExitCode)
				require.Contains(t, strings.ToLower(err.Error()), "label")
				if ambiguous {
					require.Contains(t, strings.ToLower(err.Error()), "delimiter")
					require.Contains(t, err.Error(), string(delimiter))
					require.Contains(t, err.Error(), "--array-delimiter")
				} else {
					require.Contains(t, strings.ToLower(err.Error()), "whitespace")
				}
				entries, readErr := os.ReadDir(out)
				require.NoError(t, readErr)
				require.Len(t, entries, len(existing), "unsupported labels must not publish or replace package files")
				for name, want := range existing {
					got, readErr := os.ReadFile(filepath.Join(out, name))
					require.NoError(t, readErr)
					require.Equal(t, want, string(got))
				}
				return
			}
			if subject.protectExisting && (ambiguous || normalized) {
				require.Error(t, err, "unrepresentable labels must be rejected before replacing existing package files")
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
