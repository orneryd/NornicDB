package search

import (
	"bytes"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Preserve the review probe: resolving a path must not hide traversal from
// safepath validation and allow a save to replace a sibling's vector file.
func TestRelationshipPersistenceRejectsTraversal(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	require.NoError(t, os.Mkdir("safe", 0o700))
	require.NoError(t, os.Mkdir("outside", 0o700))
	sep := string(filepath.Separator)
	relative := "safe" + sep + ".." + sep + "outside" + sep + "bm25"
	for _, tc := range []struct{ name, path string }{
		{"relative", relative},
		{"absolute", root + sep + relative},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := filepath.Join(root, "outside", "bm25") + relationshipVectorsFileSuffix
			const sentinel = "existing sibling index must remain untouched"
			require.NoError(t, os.WriteFile(target, []byte(sentinel), 0o600))
			svc := NewService(relationshipTestEngine(t), 3, nil)
			t.Cleanup(func() { _ = svc.Close() })
			svc.SetPersistenceEnabled(true)
			svc.SetFulltextIndexPath(tc.path)
			embed := &countingEmbed{}
			svc.EnableRelationshipSearch(embed.embed)
			err := svc.saveRelationshipVectors(svc.relationshipSearch())
			assert.Error(t, err, "non-canonical traversal must reach safepath validation")
			data, err := os.ReadFile(target)
			require.NoError(t, err)
			assert.Equal(t, sentinel, string(data), "rejected path must not overwrite sibling vectors")

			// The shared path owner must also reject reads. Seed a valid file
			// so a nil result cannot be explained by a failed gob decode.
			stored := relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: map[string]*relationshipEntry{
				"sibling": {Source: 1, Vectors: [][]float32{{1, 0, 0}}},
			}}
			var encoded bytes.Buffer
			require.NoError(t, gob.NewEncoder(&encoded).Encode(stored))
			require.NoError(t, os.WriteFile(target, encoded.Bytes(), 0o600))
			svc.SetFulltextIndexPath(filepath.Join(root, "outside", "bm25"))
			require.Equal(t, stored.Entries, svc.loadRelationshipVectors())
			svc.SetFulltextIndexPath(tc.path)
			assert.Nil(t, svc.loadRelationshipVectors(), "rejected path must not reuse sibling vectors")
		})
	}
}
