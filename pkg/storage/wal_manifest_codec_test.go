package storage

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The manifest's published form is its loaded form: one encoding carries the
// version and the field names, so a written manifest decodes into the same
// segments, order and retirement watermark without the load-side tail merge.
func TestWALManifestCodecRoundTripsPublishedForm(t *testing.T) {
	dir := t.TempDir()
	manifest := &WALManifest{
		Segments: []WALSegment{
			{FirstSeq: 20, LastSeq: 30, Path: "seg-20-30.wal"},
			{FirstSeq: 1, LastSeq: 19, Path: "seg-1-19.wal"},
		},
		RetiredThrough: 9,
	}
	_, err := accessWALManifest(dir, walPublishManifest, manifest)
	require.NoError(t, err)

	published, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)
	decoded := &WALManifest{}
	require.NoError(t, json.Unmarshal(published, decoded))
	require.Equal(t, walManifestVersion, decoded.Version)
	require.Equal(t, uint64(9), decoded.RetiredThrough)
	require.Equal(t, manifest.Segments, decoded.Segments, "the published encoding lost manifest state")

	loaded, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	// Load order is the sequence order the readers rely on, so the published
	// segments are compared as the sorted set they decode to.
	sorted := *decoded
	sorted.Segments = []WALSegment{decoded.Segments[1], decoded.Segments[0]}
	require.Equal(t, &sorted, loaded, "loading a published manifest changed it")
}

func TestWALManifestCodecSuppliesVersionForOlderDocuments(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(walSegmentsDir(dir), 0755))
	require.NoError(t, os.WriteFile(walManifestPath(dir), []byte(`{"segments":[{"first_seq":1,"last_seq":1,"path":"seg-1-1.wal"}]}`), 0600))
	decoded, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Equal(t, walManifestVersion, decoded.Version, "a document without a version must load at the current version")
	require.Len(t, decoded.Segments, 1)

	_, err = accessWALManifest(dir, walPublishManifest, &WALManifest{})
	require.NoError(t, err)
	encoded, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"version": 1`)
	require.Contains(t, string(encoded), `"segments": null`)

	require.Equal(t, "{\n  \"version\": 1,\n  \"segments\": null\n}\n", string(encoded), "publication preserves release wire bytes")
	_, err = accessWALManifest(dir, walPublishManifest, nil)
	require.ErrorContains(t, err, "manifest is nil")
}

// Publishing replaces the manifest atomically: the staged document never
// survives a failed publish, and an interrupted publish leaves the previous
// manifest readable.
func TestWALManifestPublishIsAtomic(t *testing.T) {
	dir := t.TempDir()
	_, err := accessWALManifest(dir, walPublishManifest, &WALManifest{RetiredThrough: 4})
	require.NoError(t, err)
	before, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)

	require.NoError(t, os.MkdirAll(walManifestPath(dir)+".tmp", 0o755))
	_, err = accessWALManifest(dir, walPublishManifest, &WALManifest{Version: 1})
	require.Error(t, err)
	require.NoError(t, os.Remove(walManifestPath(dir)+".tmp"))

	after, err := os.ReadFile(walManifestPath(dir))
	require.NoError(t, err)
	require.Equal(t, before, after, "a failed publish disturbed the published manifest")
	manifest, err := accessWALManifest(dir, walReadManifest, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(4), manifest.RetiredThrough)
}
