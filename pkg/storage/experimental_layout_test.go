package storage

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dgraph-io/badger/v4"
	"github.com/dgraph-io/badger/v4/options"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

func TestPhysicalLayoutUsesStoreOptions(t *testing.T) {
	ctx := t.Context()
	for _, layout := range []struct {
		name         string
		experimental bool
	}{{"standard", false}, {"experimental", true}} {
		t.Run(layout.name, func(t *testing.T) {
			dir := t.TempDir()
			opts := BadgerOptions{DataDir: dir, ExperimentalLayout: layout.experimental}
			open := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)
			engine, err := open(NewBadgerEngineWithOptions(opts))
			require.NoError(t, err)
			t.Cleanup(func() { _ = engine.Close() })
			node := &Node{ID: "test:doc", Labels: []string{"Doc"}, Properties: map[string]any{
				"text": strings.Repeat("compressible text ", 256),
				"rank": int64(42),
			}}
			id, err := engine.CreateNode(ctx, node)
			require.NoError(t, err)
			require.NoError(t, engine.CreateEdge(ctx, &Edge{ID: "test:edge", StartNode: id, EndNode: id, Type: "LINKS", Properties: node.Properties}))
			compressed := false
			require.NoError(t, engine.db.View(func(txn *badger.Txn) error {
				item, err := txn.Get(nodeKey(id))
				if err != nil {
					return err
				}
				return item.Value(func(raw []byte) error {
					envelope, err := SplitNodeEnvelope(raw)
					if err != nil {
						return err
					}
					_, err = walkPhysicalPropertyValues(envelope.properties, nil, func(_ uint64, start, end int) {
						compressed = compressed || isExperimentalProperty(envelope.properties[start:end])
					})
					return err
				})
			}))
			require.Equal(t, layout.experimental, compressed)
			require.NoError(t, engine.Close())

			file, err := os.Open(filepath.Join(dir, "MANIFEST"))
			require.NoError(t, err)
			manifest, _, err := badger.ReplayManifestFile(file, 0, badger.DefaultOptions(dir))
			require.NoError(t, file.Close())
			require.NoError(t, err)
			require.NotEmpty(t, manifest.Tables)
			for _, table := range manifest.Tables {
				require.Equal(t, layout.experimental, table.Compression == options.NornicColumnar)
			}

			reopened, err := open(NewBadgerEngineWithOptions(opts))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, reopened.Close()) })
			actual, err := reopened.GetNode(ctx, id)
			require.NoError(t, err)
			require.Equal(t, node.Properties, actual.Properties)
			projected, err := reopened.GetNodeProjected(ctx, id, []string{"rank"})
			require.NoError(t, err)
			require.Equal(t, map[string]any{"rank": int64(42)}, projected.Properties)
			edge, err := reopened.GetEdge(ctx, "test:edge")
			require.NoError(t, err)
			require.Equal(t, node.Properties, edge.Properties)
			matched := 0
			require.NoError(t, reopened.StreamNodesWithOptions(ctx, StreamNodesOptions{
				Label: "Doc", Projection: []string{"rank"},
				PropertyStringEquals: map[string]string{"text": node.Properties["text"].(string)},
			}, func(row *Node) error {
				matched++
				require.Equal(t, projected.Properties, row.Properties)
				return nil
			}))
			require.Equal(t, 1, matched)
		})
	}
}
