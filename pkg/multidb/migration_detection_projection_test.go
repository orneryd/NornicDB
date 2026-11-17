package multidb

import (
	"context"
	"errors"
	"strings"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type migrationDetectionReader struct {
	*storage.BadgerEngine
	t     *testing.T
	scans int
}

func (s *migrationDetectionReader) AllNodes(context.Context) ([]*storage.Node, error) {
	return nil, errors.New("namespace detection must not materialize full node images")
}

func (s *migrationDetectionReader) StreamNodesWithOptions(ctx context.Context, opts storage.StreamNodesOptions, visit func(*storage.Node) error) error {
	if opts.Prefix != "" {
		return s.BadgerEngine.StreamNodesWithOptions(ctx, opts, visit)
	}
	s.scans++
	require.NotNil(s.t, opts.Projection, "an empty projection requests identity and metadata only")
	require.Empty(s.t, opts.Projection)
	require.False(s.t, opts.WithEmbeddings)
	return s.BadgerEngine.StreamNodesWithOptions(ctx, opts, func(node *storage.Node) error {
		require.Empty(s.t, node.Properties, "namespace detection needs no property payload")
		require.Empty(s.t, node.ChunkEmbeddings)
		require.Empty(s.t, node.NamedEmbeddings)
		return visit(node)
	})
}

func TestDatabaseManagerDetectsNamespaceWithoutHydratingPayloads(t *testing.T) {
	inner, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	require.NoError(t, err)
	for _, id := range []storage.NodeID{"nornic:a", "nornic:b"} {
		_, err = inner.CreateNode(t.Context(), &storage.Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": strings.Repeat("payload ", 1024)}})
		require.NoError(t, err)
	}
	reader := &migrationDetectionReader{BadgerEngine: inner, t: t}
	manager, err := NewDatabaseManager(t.Context(), reader, nil)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, manager.Close()) })
	require.Positive(t, reader.scans)
	store, err := manager.GetDefaultStorage()
	require.NoError(t, err)
	node, err := store.GetNode(t.Context(), "a")
	require.NoError(t, err)
	require.Equal(t, strings.Repeat("payload ", 1024), node.Properties["text"], "detection must preserve stored data")
}

type migrationDetectionFailure struct {
	storage.Engine
	nodeErr error
	edgeErr error
}

func (s *migrationDetectionFailure) StreamNodesWithOptions(context.Context, storage.StreamNodesOptions, func(*storage.Node) error) error {
	return s.nodeErr
}
func (s *migrationDetectionFailure) AllEdges(context.Context) ([]*storage.Edge, error) {
	return nil, s.edgeErr
}

func TestNamespaceDetectionPropagatesReaderFailure(t *testing.T) {
	sentinel := errors.New("namespace reader failed")
	for _, nodeFailure := range []bool{true, false} {
		fault := &migrationDetectionFailure{Engine: newLegacyTestEngine(t).MemoryEngine}
		if nodeFailure {
			fault.nodeErr = sentinel
		} else {
			fault.edgeErr = sentinel
		}
		found, err := (&DatabaseManager{}).detectUnprefixedData(t.Context(), fault)
		require.ErrorIs(t, err, sentinel)
		require.False(t, found)
	}
}
