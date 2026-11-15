package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/stretchr/testify/require"
)

func TestProjectedReadsPreserveOrdinaryStorageFormat(t *testing.T) {
	projectedReadsFlag(t, false)
	dir := t.TempDir()
	open := func() *BadgerEngine {
		b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(dir))
		require.NoError(t, err)
		t.Cleanup(func() { _ = b.Close() })
		return b
	}
	b := open()
	_, err := b.CreateNode(t.Context(), &Node{ID: "test:existing", Labels: []string{"Doc"}, Properties: map[string]any{"key": "existing", "body": strings.Repeat("ordinary-format documentation ", 2048), "numbers": []int64{1, 2, 3, 4}}})
	require.NoError(t, err)
	require.NoError(t, b.Close())
	config.SetProjectedReadsEnabled(true)
	b = open()
	var before []byte
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:existing"))
		if err != nil {
			return err
		}
		before, err = item.ValueCopy(nil)
		return err
	}))
	first, err := b.GetNodeProjected(t.Context(), "test:existing", []string{"key", "absent"})
	require.NoError(t, err)
	require.Equal(t, map[string]any{"key": "existing"}, first.Properties)
	wider, err := b.GetNodeProjected(t.Context(), "test:existing", []string{"body", "numbers"})
	require.NoError(t, err)
	require.Equal(t, []int64{1, 2, 3, 4}, wider.Properties["numbers"])
	require.NoError(t, b.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(nodeKey("test:existing"))
		if err != nil {
			return err
		}
		after, err := item.ValueCopy(nil)
		require.Equal(t, before, after, "projected reads must not rewrite the primary record")
		return err
	}))
	_, err = b.CreateNode(t.Context(), &Node{ID: "test:new", Properties: map[string]any{"body": strings.Repeat("still ordinary ", 4096)}})
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(t.Context(), &Edge{ID: "test:edge", StartNode: "test:existing", EndNode: "test:new", Type: "LINK", Properties: map[string]any{"text": strings.Repeat("ordinary edge ", 512)}}))
	require.NoError(t, b.Close())
	file, err := os.Open(filepath.Join(dir, "MANIFEST"))
	require.NoError(t, err)
	manifest, _, err := badger.ReplayManifestFile(file, 0, badger.DefaultOptions(dir))
	require.NoError(t, err)
	require.NoError(t, file.Close())
	require.NotEmpty(t, manifest.Tables)
	for _, table := range manifest.Tables {
		require.Contains(t, []options.CompressionType{options.None, options.Snappy, options.ZSTD}, table.Compression)
	}
	config.SetProjectedReadsEnabled(false)
	b = open()
	node, err := b.GetNode(t.Context(), "test:new")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("still ordinary ", 4096), node.Properties["body"])
	edge, err := b.GetEdge(t.Context(), "test:edge")
	require.NoError(t, err)
	require.Equal(t, NodeID("test:existing"), edge.StartNode)
}
