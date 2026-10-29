package search

import (
	"bytes"
	"context"
	"encoding/gob"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/embeddingutil"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

// Converted historical snapshots must reach the real search worker on restart;
// re-embedding the same relationships is not an equivalent migration.
func TestRelationshipSnapshotUpgradeReusesVectorsOnRestart(t *testing.T) {
	engine := relationshipTestEngine(t)
	path := filepath.Join(t.TempDir(), "bm25")
	seed := NewService(engine, 3, nil)
	t.Cleanup(func() { _ = seed.Close() })
	seed.EnableRelationshipSearch(nil)
	want := relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: make(map[string]*relationshipEntry)}
	for index, id := range []string{"r1", "r2"} {
		edge, err := engine.GetEdge(storage.EdgeID(id))
		require.NoError(t, err)
		want.Entries[id] = &relationshipEntry{
			Source:  seed.relationshipSearch().source(edge),
			Vectors: [][]float32{{float32(index), 1, 0}},
			Spans:   []embeddingutil.ChunkSpan{{Chunk: 0, Property: "reason", Start: index, End: index + 5}},
		}
	}
	var old bytes.Buffer
	require.NoError(t, gob.NewEncoder(&old).Encode(want))
	file := path + relationshipVectorsFileSuffix
	require.NoError(t, os.WriteFile(file, old.Bytes(), 0o600))
	require.NoError(t, UpgradeRelationshipSnapshot(file, true, nil))
	converted, err := os.ReadFile(file)
	require.NoError(t, err)
	var got relationshipVectorsFile
	require.NoError(t, msgpack.Unmarshal(converted, &got))
	require.Equal(t, want, got)

	embed := &countingEmbed{}
	restarted := startRelationshipService(t, engine, embed.embed, func(s *Service) {
		s.SetPersistenceEnabled(true)
		s.SetFulltextIndexPath(path)
	})
	select {
	case <-restarted.relationshipSearch().ready:
	case <-time.After(5 * time.Second):
		t.Fatal("relationship search did not become ready")
	}
	require.Equal(t, len(want.Entries), restarted.RelationshipEmbeddingCount())
	rs := restarted.relationshipSearch()
	rs.mu.RLock()
	actual := make(map[string]*relationshipEntry, len(rs.entries))
	for id, entry := range rs.entries {
		actual[id] = entry
	}
	rs.mu.RUnlock()
	for id, expected := range want.Entries {
		require.Equal(t, expected, actual[id])
	}
	require.NoError(t, restarted.Close())
	require.Zero(t, embed.calls.Load(), "restart must not regenerate migrated vectors")
}

// A direct Service consumer cannot discard or overwrite an unreadable or
// historical file if its caller did not complete the one-time upgrade first.
func TestRelationshipSnapshotWithoutUpgradeDoesNotOverwrite(t *testing.T) {
	var old bytes.Buffer
	require.NoError(t, gob.NewEncoder(&old).Encode(relationshipVectorsFile{Format: relationshipVectorsFormat, Entries: map[string]*relationshipEntry{"r1": {Source: 1, Vectors: [][]float32{{1, 0, 0}}}}}))
	for _, tc := range []struct {
		name       string
		data       []byte
		upgradeErr bool
	}{{"historical", old.Bytes(), true}, {"corrupt", []byte("invalid snapshot"), false}} {
		t.Run(tc.name, func(t *testing.T) {
			engine := relationshipTestEngine(t)
			path := filepath.Join(t.TempDir(), "bm25")
			file := path + relationshipVectorsFileSuffix
			require.NoError(t, os.WriteFile(file, tc.data, 0o600))
			embed := &countingEmbed{}
			svc := startRelationshipService(t, engine, embed.embed, func(s *Service) {
				s.SetPersistenceEnabled(true)
				s.SetFulltextIndexPath(path)
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := svc.SearchRelationships(ctx, "", nil, nil)
			require.Error(t, err)
			if tc.upgradeErr {
				require.ErrorIs(t, err, ErrRelationshipSnapshotUpgradeRequired)
			}
			closeErr := svc.Close()
			require.Error(t, closeErr)
			if tc.upgradeErr {
				require.ErrorIs(t, closeErr, ErrRelationshipSnapshotUpgradeRequired)
			}
			require.Zero(t, embed.calls.Load())
			again, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, tc.data, again)
		})
	}
}
