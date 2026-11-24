package adminimport

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// preserveOpenPackage writes a package file set into outDir so a rejected
// export can be proven to leave an existing package untouched.
func preserveOpenPackage(t *testing.T, outDir string) map[string]string {
	t.Helper()
	require.NoError(t, os.MkdirAll(outDir, 0o755))
	existing := make(map[string]string)
	for _, name := range []string{"nodes.csv", "relationships.csv", Neo4jCSVSchemaFileName, Neo4jCSVNornicSchemaFileName} {
		existing[name] = "preserved " + name
		require.NoError(t, os.WriteFile(filepath.Join(outDir, name), []byte(existing[name]), 0o600))
	}
	return existing
}

// requireUnsupportedExport proves export rejected the package with the default
// array delimiter before replacing any file.
func requireUnsupportedExport(t *testing.T, source storage.Engine, outDir, property string) {
	t.Helper()
	requireUnsupportedExportWithOptions(t, source, outDir, property, Neo4jCSVExportOptions{})
}

// requireUnsupportedExportWithOptions proves export rejected the package before
// replacing any file, naming the ambiguous property and the configured array
// delimiter.
func requireUnsupportedExportWithOptions(t *testing.T, source storage.Engine, outDir, property string, opts Neo4jCSVExportOptions) {
	t.Helper()
	existing := preserveOpenPackage(t, outDir)
	opts.OutputDir = outDir
	delimiter := opts.ArrayDelimiter
	if delimiter == 0 {
		delimiter = ';'
	}
	err := ExportNeo4jCSV(source, opts)
	require.Error(t, err)
	var exportErr *Error
	require.ErrorAs(t, err, &exportErr)
	require.Equal(t, ExitUnsupported, exportErr.ExitCode)
	require.Contains(t, err.Error(), property)
	require.Contains(t, err.Error(), strconv.QuoteRune(delimiter))
	require.Contains(t, err.Error(), "--array-delimiter")
	entries, readErr := os.ReadDir(outDir)
	require.NoError(t, readErr)
	require.Len(t, entries, len(existing), "a rejected export must not publish or replace package files")
	for name, want := range existing {
		got, readErr := os.ReadFile(filepath.Join(outDir, name))
		require.NoError(t, readErr)
		require.Equal(t, want, string(got), name)
	}
}

func newRoundTripSource(t *testing.T) storage.Engine {
	t.Helper()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	return storage.NewNamespacedEngine(base, "source")
}

// roundTripNeo4jCSVWithOptions is the same public export/import round trip as
// roundTripNeo4jCSV with a configured delimiter for both directions.
func roundTripNeo4jCSVWithOptions(t *testing.T, source storage.Engine, exportOpts Neo4jCSVExportOptions, importOpts Options) (storage.Engine, string) {
	t.Helper()
	outDir := filepath.Join(t.TempDir(), "neo4j-csv")
	exportOpts.OutputDir = outDir
	require.NoError(t, ExportNeo4jCSV(source, exportOpts))
	nodeSources, relSources, err := DiscoverNeo4jCSVSources(outDir, importOpts)
	require.NoError(t, err)
	return importNeo4jCSVPackage(t, importOpts, nodeSources, relSources), outDir
}

// TestN152_268CSVListValuesRoundTripOrRejectAmbiguity covers the Neo4j CSV
// array contract: a list the delimiter encoding can decode back to the same
// elements round trips exactly, an empty list keeps a present empty cell, and
// a list the encoding would alter is rejected before any package file is
// replaced.
func TestN152_268CSVListValuesRoundTripOrRejectAmbiguity(t *testing.T) {
	ctx := context.Background()

	t.Run("empty and plain string lists round trip", func(t *testing.T) {
		source := newRoundTripSource(t)
		_, err := source.CreateNode(ctx, &storage.Node{
			ID:     "l1",
			Labels: []string{"Tags"},
			Properties: map[string]any{
				"uid":   "l1",
				"empty": []any{},
				"plain": []string{"alpha", "beta"},
			},
		})
		require.NoError(t, err)
		want, err := source.GetNode(ctx, "l1")
		require.NoError(t, err)

		dest, outDir := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "l1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties, "an empty list and a plain string list must round trip exactly")
		require.Equal(t, []any{}, got.Properties["empty"])

		nodesCSV := readExportedFile(t, outDir, "nodes.csv")
		require.Contains(t, nodesCSV, "empty:string[]")
		require.Contains(t, nodesCSV, "plain:string[]")
		// The empty list is a present value, so its cell is the quoted empty
		// cell; the plain list is the joined text.
		require.Contains(t, nodesCSV, "\nl1,Tags,\"\",alpha;beta,l1\n")
	})

	t.Run("relationship empty list round trips", func(t *testing.T) {
		source := newRoundTripSource(t)
		_, err := source.CreateNode(ctx, &storage.Node{ID: "l1", Labels: []string{"Tags"}})
		require.NoError(t, err)
		_, err = source.CreateNode(ctx, &storage.Node{ID: "l2", Labels: []string{"Tags"}})
		require.NoError(t, err)
		require.NoError(t, source.CreateEdge(ctx, &storage.Edge{
			ID:         "e1",
			StartNode:  "l1",
			EndNode:    "l2",
			Type:       "TAGGED",
			Properties: map[string]any{"tags": []any{}},
		}))
		wantEdges, err := storage.CollectStoredEdges(ctx, source)
		require.NoError(t, err)
		require.Len(t, wantEdges, 1)

		dest, _ := roundTripNeo4jCSV(t, source)

		gotEdges, err := storage.CollectStoredEdges(ctx, dest)
		require.NoError(t, err)
		require.Len(t, gotEdges, 1)
		require.Equal(t, wantEdges[0].Properties, gotEdges[0].Properties)
	})

	t.Run("node list elements the delimiter encoding cannot preserve are rejected", func(t *testing.T) {
		for _, subject := range []struct {
			name  string
			value []any
		}{
			{name: "array delimiter", value: []any{"a;b"}},
			{name: "empty element", value: []any{"alpha", ""}},
			{name: "surrounding whitespace", value: []any{" padded"}},
			{name: "CRLF line break", value: []any{"a\r\nb"}},
		} {
			t.Run(subject.name, func(t *testing.T) {
				source := newRoundTripSource(t)
				_, err := source.CreateNode(ctx, &storage.Node{
					ID:         "l1",
					Labels:     []string{"Tags"},
					Properties: map[string]any{"tags": subject.value},
				})
				require.NoError(t, err)
				requireUnsupportedExport(t, source, filepath.Join(t.TempDir(), "csv"), "tags")
			})
		}
	})

	t.Run("custom array delimiter round trips and decides the conflict", func(t *testing.T) {
		source := newRoundTripSource(t)
		_, err := source.CreateNode(ctx, &storage.Node{
			ID:         "l1",
			Properties: map[string]any{"tags": []string{"a;b", "c"}},
		})
		require.NoError(t, err)
		want, err := source.GetNode(ctx, "l1")
		require.NoError(t, err)

		dest, _ := roundTripNeo4jCSVWithOptions(t, source, Neo4jCSVExportOptions{ArrayDelimiter: '|'}, Options{ArrayDelimiter: '|'})
		got, err := dest.GetNode(ctx, "l1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties, "a value without the configured delimiter must round trip")

		conflict := newRoundTripSource(t)
		_, err = conflict.CreateNode(ctx, &storage.Node{ID: "l2", Properties: map[string]any{"tags": []string{"a|b"}}})
		require.NoError(t, err)
		requireUnsupportedExportWithOptions(t, conflict, filepath.Join(t.TempDir(), "csv"), "tags", Neo4jCSVExportOptions{ArrayDelimiter: '|'})
	})

	t.Run("relationship list elements the delimiter encoding cannot preserve are rejected", func(t *testing.T) {
		source := newRoundTripSource(t)
		_, err := source.CreateNode(ctx, &storage.Node{ID: "l1", Labels: []string{"Tags"}})
		require.NoError(t, err)
		_, err = source.CreateNode(ctx, &storage.Node{ID: "l2", Labels: []string{"Tags"}})
		require.NoError(t, err)
		require.NoError(t, source.CreateEdge(ctx, &storage.Edge{
			ID:         "e1",
			StartNode:  "l1",
			EndNode:    "l2",
			Type:       "TAGGED",
			Properties: map[string]any{"tags": []string{"a;b"}},
		}))
		requireUnsupportedExport(t, source, filepath.Join(t.TempDir(), "csv"), "tags")
	})
}

// TestN152_267CSVNamedIDPropertyRoundTrip covers the node identifier columns:
// a named id column is a stored property, not only the node identity, so
// export and re-import must keep it. A differently named identifier column and
// an ordinary id property are the controls.
func TestN152_267CSVNamedIDPropertyRoundTrip(t *testing.T) {
	ctx := context.Background()

	importPath := func(t *testing.T, content string) string {
		t.Helper()
		path := filepath.Join(t.TempDir(), "nodes.csv")
		require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
		return path
	}

	t.Run("id:ID keeps its id property", func(t *testing.T) {
		source := importNeo4jCSVPackage(t, Options{}, []string{importPath(t, "id:ID,name:string\nn1,Alice\nn2,Bob\n")}, nil)
		want, err := source.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"id": "n1", "name": "Alice"}, want.Properties)

		dest, outDir := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties, "the id property written by a named id column must survive export and re-import")
		nodesCSV := readExportedFile(t, outDir, "nodes.csv")
		require.Contains(t, nodesCSV, "id:string")
		require.Contains(t, nodesCSV, "\nn1,\"\",n1,Alice\n")
	})

	t.Run("plain id property equal to the node id is kept", func(t *testing.T) {
		source := importNeo4jCSVPackage(t, Options{}, []string{importPath(t, ":ID,id:string,name:string\nn1,n1,Alice\n")}, nil)
		want, err := source.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"id": "n1", "name": "Alice"}, want.Properties)

		dest, _ := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties)
	})

	t.Run("uid:ID control keeps its uid property", func(t *testing.T) {
		source := importNeo4jCSVPackage(t, Options{}, []string{importPath(t, "uid:ID,name:string\nn1,Alice\n")}, nil)
		want, err := source.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"uid": "n1", "name": "Alice"}, want.Properties)

		dest, _ := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties)
	})

	t.Run("plain id property distinct from the node id control", func(t *testing.T) {
		source := importNeo4jCSVPackage(t, Options{}, []string{importPath(t, ":ID,id:string,name:string\nn1,legacy,Alice\n")}, nil)
		want, err := source.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"id": "legacy", "name": "Alice"}, want.Properties)

		dest, _ := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "n1")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties)
	})

	t.Run("anonymous :ID control does not gain an id property", func(t *testing.T) {
		source := importNeo4jCSVPackage(t, Options{}, []string{importPath(t, ":ID,name:string\nn9,Nine\n")}, nil)
		want, err := source.GetNode(ctx, "n9")
		require.NoError(t, err)
		require.Equal(t, map[string]any{"name": "Nine"}, want.Properties)

		dest, _ := roundTripNeo4jCSV(t, source)

		got, err := dest.GetNode(ctx, "n9")
		require.NoError(t, err)
		require.Equal(t, want.Properties, got.Properties)
	})
}

// TestN152_266CSVDateTimeRoundTrip covers the datetime column type: a value the
// Cypher datetime constructor stores keeps its datetime type, instant, offset
// and named zone through export and re-import. A plain string that only looks
// like a datetime is the control and stays a string.
func TestN152_266CSVDateTimeRoundTrip(t *testing.T) {
	ctx := context.Background()
	source := newRoundTripSource(t)
	exec := cypher.NewStorageExecutor(source)

	subjects := []struct {
		uid  string
		text string
		want string
	}{
		{uid: "offset", text: "2024-01-15T10:30:00.123456789+02:00", want: "2024-01-15T10:30:00.123456789+02:00"},
		{uid: "utc", text: "2024-01-15T09:30:00Z", want: "2024-01-15T09:30Z"},
		{uid: "named", text: "2024-07-01T12:00:00[Europe/Stockholm]", want: "2024-07-01T12:00+02:00[Europe/Stockholm]"},
	}
	for _, subject := range subjects {
		result, err := exec.Execute(ctx, "RETURN datetime($text) AS at", map[string]any{"text": subject.text})
		require.NoError(t, err)
		value, ok := result.Rows[0][0].(cypher.CypherDateTime)
		require.True(t, ok, "%s: datetime() must build a datetime value, got %T", subject.uid, result.Rows[0][0])
		_, err = source.CreateNode(ctx, &storage.Node{
			ID:         storage.NodeID(subject.uid),
			Labels:     []string{"Event"},
			Properties: map[string]any{"uid": subject.uid, "at": value},
		})
		require.NoError(t, err)
	}
	// Control: a string property whose text is an ISO datetime is not a
	// datetime column and must come back as the same string.
	const note = "2024-01-15T10:30:00+02:00"
	_, err := source.CreateNode(ctx, &storage.Node{
		ID:         "plain",
		Labels:     []string{"Event"},
		Properties: map[string]any{"uid": "plain", "note": note},
	})
	require.NoError(t, err)

	want := make(map[string]cypher.CypherDateTime)
	for _, subject := range subjects {
		node, err := source.GetNode(ctx, storage.NodeID(subject.uid))
		require.NoError(t, err)
		value, ok := node.Properties["at"].(cypher.CypherDateTime)
		require.True(t, ok, "%s: the stored property must be a datetime, got %T", subject.uid, node.Properties["at"])
		want[subject.uid] = value
	}

	dest, outDir := roundTripNeo4jCSV(t, source)

	nodesCSV := readExportedFile(t, outDir, "nodes.csv")
	require.Contains(t, nodesCSV, "at:datetime")
	require.NotContains(t, nodesCSV, "at:string")
	require.Contains(t, nodesCSV, "note:string")
	require.Contains(t, nodesCSV, "[Europe/Stockholm]", "a named-zone datetime must keep its zone text")
	for _, subject := range subjects {
		require.Contains(t, nodesCSV, subject.want, subject.uid)
	}

	for uid, expected := range want {
		got, err := dest.GetNode(ctx, storage.NodeID(uid))
		require.NoError(t, err)
		value, ok := got.Properties["at"].(cypher.CypherDateTime)
		require.True(t, ok, "%s: the datetime type must survive export and re-import, got %T", uid, got.Properties["at"])
		require.Equal(t, expected.ZoneID, value.ZoneID)
		require.Equal(t, expected.Time.UnixNano(), value.Time.UnixNano(), "%s: the instant must survive", uid)
		require.Equal(t, expected.String(), value.String(), "%s: offset and zone must survive", uid)
	}

	plain, err := dest.GetNode(ctx, "plain")
	require.NoError(t, err)
	require.Equal(t, note, plain.Properties["note"], "a plain string must stay a string")
	require.IsType(t, "", plain.Properties["note"])
}

// TestImportDatetimeColumnRejectsUnparseableText proves a datetime column that
// carries text the datetime parser rejects fails visibly instead of storing a
// string that silently lost the column's declared type.
func TestImportDatetimeColumnRejectsUnparseableText(t *testing.T) {
	for _, subject := range []struct {
		name  string
		value string
		want  string
	}{
		{name: "not a datetime", value: "not-a-datetime", want: "invalid datetime"},
		{name: "offset disagrees with the named zone", value: "2024-07-01T12:00:00+05:00[Europe/Stockholm]", want: "offset"},
	} {
		t.Run(subject.name, func(t *testing.T) {
			dir := t.TempDir()
			nodesPath := filepath.Join(dir, "nodes.csv")
			require.NoError(t, os.WriteFile(nodesPath, []byte(":ID,at:datetime\nn1,"+subject.value+"\n"), 0o600))

			base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
			_, err := ImportFull(context.Background(), base, Options{
				DatabaseName: "datetime",
				NodeSources:  []string{nodesPath},
				Now:          fixedImportTime,
			})
			require.Error(t, err)
			var importErr *Error
			require.ErrorAs(t, err, &importErr)
			require.Equal(t, ExitCSV, importErr.ExitCode)
			require.Contains(t, err.Error(), subject.want)
			nodes, readErr := storage.NewNamespacedEngine(base, "datetime").AllNodes(context.Background())
			require.NoError(t, readErr)
			require.Empty(t, nodes)
		})
	}
}
