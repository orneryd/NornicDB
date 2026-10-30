package replication

import (
	"context"
	"encoding/json"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"github.com/vmihailenco/msgpack/v5"
)

func wireFixture(t *testing.T, buffered bool, dataDirs ...string) (*storage.BadgerEngine, *StorageAdapter) {
	t.Helper()
	dir := t.TempDir()
	if len(dataDirs) > 0 {
		dir = dataDirs[0]
	}
	b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir:             dir,
		WriteBehind:         buffered,
		WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	a, err := NewStorageAdapterWithWAL(b, t.TempDir())
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, a)
	return b, a
}

func wireCommand(t *testing.T, id string, ops ...storage.Operation) *Command {
	t.Helper()
	data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "wire", LogicalID: id, Operations: ops})
	require.NoError(t, err)
	return &Command{Type: CmdCommitTransaction, Data: data, Timestamp: time.Now()}
}

func wireNode(id, value string) *storage.Node {
	return &storage.Node{ID: storage.NodeID("wire:" + id), Labels: []string{"Wire"}, Properties: map[string]any{"value": value}, CreatedAt: time.Unix(1700000000, 4).UTC(), UpdatedAt: time.Unix(1700000000, 8).UTC()}
}

func wireEdge(id, start, end, value string) *storage.Edge {
	return &storage.Edge{ID: storage.EdgeID("wire:" + id), StartNode: storage.NodeID("wire:" + start), EndNode: storage.NodeID("wire:" + end), Type: "LINK", Properties: map[string]any{"value": value}, CreatedAt: time.Unix(1700000000, 4).UTC()}
}

func TestPrototypeTransactionWireAtomicAndReceipt(t *testing.T) {
	b, a := wireFixture(t, false)
	for _, id := range []string{"a", "b", "gone"} {
		_, err := b.CreateNode(wireNode(id, "old"))
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(wireEdge("old", "a", "b", "old")))
	var events atomic.Int64
	b.OnNodeCreated(func(*storage.Node) { events.Add(1) })
	b.OnNodeUpdated(func(*storage.Node) { events.Add(1) })
	b.OnNodeDeleted(func(storage.NodeID) { events.Add(1) })
	b.OnEdgeCreated(func(*storage.Edge) { events.Add(1) })
	b.OnEdgeUpdated(func(*storage.Edge) { events.Add(1) })
	b.OnEdgeDeleted(func(storage.EdgeID) { events.Add(1) })
	ops := []storage.Operation{
		{Type: storage.OpUpdateNode, Node: wireNode("a", "updated")}, {Type: storage.OpCreateNode, Node: wireNode("new", "created")},
		{Type: storage.OpUpdateEdge, Edge: wireEdge("old", "a", "b", "updated")}, {Type: storage.OpDeleteEdge, EdgeID: storage.EdgeID("wire:old")},
		{Type: storage.OpCreateEdge, Edge: wireEdge("new-edge", "a", "new", "created")}, {Type: storage.OpUpdateEdge, Edge: wireEdge("new-edge", "a", "new", "final")},
		{Type: storage.OpDeleteNode, NodeID: storage.NodeID("wire:gone")}, {Type: storage.OpCreateNode, Node: wireNode("gone", "recreated")},
	}
	invalid := append(append([]storage.Operation(nil), ops...), storage.Operation{Type: storage.OpCreateEdge, Edge: wireEdge("invalid", "a", "missing", "invalid")})
	require.Error(t, a.ApplyCommand(context.Background(), wireCommand(t, "atomic", invalid...)))
	old, err := b.GetStoredNode("wire:a")
	require.NoError(t, err)
	require.Equal(t, "old", old.Properties["value"])
	_, err = b.GetStoredNode("wire:new")
	require.ErrorIs(t, err, storage.ErrNotFound)
	edge, err := b.GetStoredEdge("wire:old")
	require.NoError(t, err)
	require.Equal(t, "old", edge.Properties["value"])
	gone, err := b.GetStoredNode("wire:gone")
	require.NoError(t, err)
	require.Equal(t, "old", gone.Properties["value"])
	require.Zero(t, events.Load())
	// The failed transaction did not commit its receipt: the corrected same-ID
	// command applies, including delete/recreate, through one native transaction.
	command := wireCommand(t, "atomic", ops...)
	require.NoError(t, a.ApplyCommand(context.Background(), command))
	got, err := b.GetStoredNode("wire:a")
	require.NoError(t, err)
	require.Equal(t, "updated", got.Properties["value"])
	got, err = b.GetStoredNode("wire:new")
	require.NoError(t, err)
	require.Equal(t, "created", got.Properties["value"])
	got, err = b.GetStoredNode("wire:gone")
	require.NoError(t, err)
	require.Equal(t, "recreated", got.Properties["value"])
	_, err = b.GetStoredEdge("wire:old")
	require.ErrorIs(t, err, storage.ErrNotFound)
	edge, err = b.GetStoredEdge("wire:new-edge")
	require.NoError(t, err)
	require.Equal(t, "final", edge.Properties["value"])
	beforeEvents := events.Load()
	require.Positive(t, beforeEvents)
	beforeVersion, ok := b.GraphMutationVersionInNamespace("wire")
	require.True(t, ok)
	nodes, err := b.NodeCount()
	require.NoError(t, err)
	edges, err := b.EdgeCount()
	require.NoError(t, err)
	require.NoError(t, a.ApplyCommand(context.Background(), command))
	require.Equal(t, beforeEvents, events.Load())
	afterVersion, _ := b.GraphMutationVersionInNamespace("wire")
	require.Equal(t, beforeVersion, afterVersion)
	afterNodes, err := b.NodeCount()
	require.NoError(t, err)
	afterEdges, err := b.EdgeCount()
	require.NoError(t, err)
	require.Equal(t, nodes, afterNodes)
	require.Equal(t, edges, afterEdges)
}

func TestPrototypeTransactionWireRejectsVersionAndNamespace(t *testing.T) {
	for _, kind := range []string{"version", "namespace", "endpoint"} {
		t.Run(kind, func(t *testing.T) {
			b, a := wireFixture(t, false)
			ops := []storage.Operation{{Type: storage.OpCreateNode, Node: wireNode("a", "value")}}
			var command *Command
			switch kind {
			case "version":
				command = wireCommand(t, "invalid", ops...)
				var envelope map[string]any
				require.NoError(t, msgpack.Unmarshal(command.Data, &envelope))
				envelope["Version"] = storage.EntityTransactionVersion + 1
				data, err := msgpack.Marshal(envelope)
				require.NoError(t, err)
				command.Data = data
			case "namespace":
				data, err := storage.EncodeEntityTransaction(storage.EntityTransactionRequest{Namespace: "different", LogicalID: "invalid", Operations: ops})
				require.NoError(t, err)
				command = &Command{Type: CmdCommitTransaction, Data: data}
			case "endpoint":
				invalid := append(append([]storage.Operation(nil), ops...), storage.Operation{Type: storage.OpCreateEdge, Edge: &storage.Edge{ID: "wire:r", StartNode: "wire:a", EndNode: "different:b", Type: "LINK"}})
				command = wireCommand(t, "invalid", invalid...)
			}
			require.Error(t, a.ApplyCommand(context.Background(), command))
			nodes, err := b.NodeCount()
			require.NoError(t, err)
			require.Zero(t, nodes)
			// Corrected metadata using the same logical ID remains admissible.
			require.NoError(t, a.ApplyCommand(context.Background(), wireCommand(t, "invalid", ops...)))
		})
	}
}

func wireDictionaryIdentity(t *testing.T, b *storage.BadgerEngine, key string) string {
	t.Helper()
	dir := t.TempDir()
	w, err := storage.NewWAL(dir, storage.DefaultWALConfig())
	require.NoError(t, err)
	fixtureowner.OwnChecked(t, w)
	b.SetBufferedCommitWAL(w)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("wire"))
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&storage.Node{ID: storage.NodeID("wire:dictionary-" + key), Properties: map[string]any{key: "sentinel"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	require.NoError(t, b.FlushWriteBehind())
	require.NoError(t, w.Sync())
	entries, err := storage.ReadWALEntriesFromDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.PropertyEpoch == nil {
			continue
		}
		data, err := json.Marshal(entry.PropertyEpoch)
		require.NoError(t, err)
		var identity struct {
			DictionaryID string `json:"dictionary_id"`
		}
		require.NoError(t, json.Unmarshal(data, &identity))
		if identity.DictionaryID != "" {
			return identity.DictionaryID
		}
	}
	t.Fatal("no property dictionary identity exported by real WAL")
	return ""
}
func TestPrototypeTransactionWireUsesDestinationDictionaries(t *testing.T) {
	source, _ := wireFixture(t, true)
	destination, peer := wireFixture(t, true)
	sourceID := wireDictionaryIdentity(t, source, "alpha")
	destinationID := wireDictionaryIdentity(t, destination, "beta")
	require.NotEqual(t, sourceID, destinationID)
	location, locationErr := time.LoadLocation("Europe/London")
	require.NoError(t, locationErr)
	stamp := time.Date(2024, 3, 5, 10, 20, 30, 123456789, location)
	node := wireNode("typed", "typed")
	node.Properties = map[string]any{"alpha": int32(9), "beta": []byte{0, 1, 255}, "float": float32(1.25), "list": []int64{1, 2}, "when": stamp, "nested": map[string]any{"count": int64(7)}}
	node.ChunkEmbeddings = [][]float32{{1, 0, 0}, {0, 1, 0}}
	node.NamedEmbeddings = map[string][]float32{"named": {0, 0, 1}}
	_, err := source.CreateNode(node)
	require.NoError(t, err)
	authoritative, err := source.GetStoredNode(node.ID)
	require.NoError(t, err)
	_, err = source.CreateNode(wireNode("dictionary-beta", "source-endpoint"))
	require.NoError(t, err)
	edge := wireEdge("typed-edge", "typed", "dictionary-beta", "typed")
	edge.Properties = map[string]any{"alpha": int64(4), "when": stamp}
	require.NoError(t, source.CreateEdge(edge))
	authoritativeEdge, err := source.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.NoError(t, peer.ApplyCommand(context.Background(), wireCommand(t, "typed", storage.Operation{Type: storage.OpCreateNode, Node: authoritative}, storage.Operation{Type: storage.OpCreateEdge, Edge: authoritativeEdge})))
	got, err := destination.GetStoredNode(node.ID)
	require.NoError(t, err)
	require.Equal(t, authoritative.Properties, got.Properties)
	require.Equal(t, authoritative.ChunkEmbeddings, got.ChunkEmbeddings)
	require.Equal(t, authoritative.NamedEmbeddings, got.NamedEmbeddings)
	require.True(t, authoritative.CreatedAt.Equal(got.CreatedAt))
	require.True(t, authoritative.UpdatedAt.Equal(got.UpdatedAt))
	gotEdge, err := destination.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.Equal(t, authoritativeEdge.Properties, gotEdge.Properties)
	sentinel, err := destination.GetStoredNode("wire:dictionary-beta")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"beta": "sentinel"}, sentinel.Properties)
}

// This counterexample is deliberately outside a final Submit route: peer
// restaging alone cannot certify an originating leader transaction's outcome.
func TestPrototypeTransactionReceiptSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	open := func() (*storage.BadgerEngine, *StorageAdapter) {
		b, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(dir))
		require.NoError(t, err)
		a, err := NewStorageAdapterWithWAL(b, t.TempDir())
		require.NoError(t, err)
		fixtureowner.OwnChecked(t, a)
		return b, a
	}
	b, a := open()
	command := wireCommand(t, "durable-receipt", storage.Operation{Type: storage.OpCreateNode, Node: wireNode("durable", "value")})
	require.NoError(t, a.ApplyCommand(context.Background(), command))
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
	b, a = open()
	var events atomic.Int64
	b.OnNodeCreated(func(*storage.Node) { events.Add(1) })
	require.NoError(t, a.ApplyCommand(context.Background(), command))
	require.Zero(t, events.Load())
	n, err := b.GetStoredNode("wire:durable")
	require.NoError(t, err)
	require.Equal(t, "value", n.Properties["value"])
	count, err := b.NodeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func TestPrototypeTransactionCommandPreservesExistingEnum(t *testing.T) {
	require.EqualValues(t, 18, CmdBulkDeleteEdges)
	require.EqualValues(t, 19, CmdCommitTransaction)
}

func TestPrototypeTransactionWireAllStoredSemanticFields(t *testing.T) {
	source, _ := wireFixture(t, false)
	destinationDir := t.TempDir()
	destination, peer := wireFixture(t, false, destinationDir)
	for _, b := range []*storage.BadgerEngine{source, destination} {
		_, err := b.CreateNode(wireNode("target", "endpoint"))
		require.NoError(t, err)
	}
	node := wireNode("metadata", "value")
	{
		metadata := node.RuntimeMetadata()
		metadata.Database = "transient-provenance"
		node.SetRuntimeMetadata(metadata)
	}
	{
		metadata := node.RuntimeMetadata()
		metadata.VisibilitySuppressed = true
		node.SetRuntimeMetadata(metadata)
	}
	node.EmbedMeta = map[string]any{"embedding_model": "synthetic", "chunk_count": int(1), "has_embedding": true}
	node.ChunkEmbeddings = [][]float32{{1, 2, 3}}
	node.NamedEmbeddings = map[string][]float32{"named": {3, 2, 1}}
	_, err := source.CreateNode(node)
	require.NoError(t, err)
	storedNode, err := source.GetStoredNode(node.ID)
	require.NoError(t, err)
	edge := wireEdge("metadata-edge", "metadata", "target", "edge")
	edge.Database = "transient-provenance"
	edge.UpdatedAt = time.Unix(1700000002, 99).UTC()
	edge.Confidence = 0.75
	edge.AutoGenerated = true
	edge.VisibilitySuppressed = true
	require.NoError(t, source.CreateEdge(edge))
	storedEdge, err := source.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.NoError(t, peer.ApplyCommand(context.Background(), wireCommand(t, "metadata", storage.Operation{Type: storage.OpCreateNode, Node: storedNode}, storage.Operation{Type: storage.OpCreateEdge, Edge: storedEdge})))
	nodePeer, err := destination.GetStoredNode(node.ID)
	require.NoError(t, err)
	edgePeer, err := destination.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	assertStoredNodeParity(t, storedNode, nodePeer)
	assertStoredEdgeParity(t, storedEdge, edgePeer)
	next := storage.CopyNode(storedNode)
	next.EmbedMeta["embedding_model"] = "replacement"
	{
		metadata := next.RuntimeMetadata()
		metadata.VisibilitySuppressed = false
		next.SetRuntimeMetadata(metadata)
	}
	require.NoError(t, source.UpdateNode(next))
	nextStored, err := source.GetStoredNode(node.ID)
	require.NoError(t, err)
	nextEdge := storage.CopyEdge(storedEdge)
	nextEdge.UpdatedAt = time.Unix(1700000004, 321).UTC()
	nextEdge.Confidence = 0.5
	nextEdge.AutoGenerated = false
	nextEdge.VisibilitySuppressed = false
	require.NoError(t, source.UpdateEdge(nextEdge))
	nextStoredEdge, err := source.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	require.NoError(t, peer.ApplyCommand(context.Background(), wireCommand(t, "metadata-update", storage.Operation{Type: storage.OpUpdateNode, Node: nextStored}, storage.Operation{Type: storage.OpUpdateEdge, Edge: nextStoredEdge})))
	nodePeer, err = destination.GetStoredNode(node.ID)
	require.NoError(t, err)
	edgePeer, err = destination.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	assertStoredNodeParity(t, nextStored, nodePeer)
	assertStoredEdgeParity(t, nextStoredEdge, edgePeer)
	require.NoError(t, peer.Close())
	require.NoError(t, destination.Close())
	reopened, err := fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(destinationDir))
	require.NoError(t, err)
	persistedNode, err := reopened.GetStoredNode(node.ID)
	require.NoError(t, err)
	persistedEdge, err := reopened.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	assertStoredNodeParity(t, nextStored, persistedNode)
	assertStoredEdgeParity(t, nextStoredEdge, persistedEdge)
}
func assertStoredNodeParity(t *testing.T, want, got *storage.Node) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.Labels, got.Labels)
	require.Equal(t, want.Properties, got.Properties)
	require.Equal(t, want.EmbedMeta, got.EmbedMeta)
	require.Equal(t, want.ChunkEmbeddings, got.ChunkEmbeddings)
	require.Equal(t, want.NamedEmbeddings, got.NamedEmbeddings)
	require.Equal(t, want.RuntimeMetadata().VisibilitySuppressed, got.RuntimeMetadata().VisibilitySuppressed)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt))
	require.True(t, want.UpdatedAt.Equal(got.UpdatedAt))
	require.Empty(t, got.RuntimeMetadata().Database)
}
func assertStoredEdgeParity(t *testing.T, want, got *storage.Edge) {
	t.Helper()
	require.Equal(t, want.ID, got.ID)
	require.Equal(t, want.StartNode, got.StartNode)
	require.Equal(t, want.EndNode, got.EndNode)
	require.Equal(t, want.Type, got.Type)
	require.Equal(t, want.Properties, got.Properties)
	require.Equal(t, want.Confidence, got.Confidence)
	require.Equal(t, want.AutoGenerated, got.AutoGenerated)
	require.Equal(t, want.VisibilitySuppressed, got.VisibilitySuppressed)
	require.True(t, want.CreatedAt.Equal(got.CreatedAt))
	require.True(t, want.UpdatedAt.Equal(got.UpdatedAt))
	require.Empty(t, got.Database)
}

func TestPrototypeTransactionSemanticFieldInventory(t *testing.T) {
	fields := func(value any, exclude map[string]bool) []string {
		typ := reflect.TypeOf(value)
		var names []string
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if field.IsExported() && !exclude[field.Name] {
				names = append(names, field.Name)
			}
		}
		return names
	}
	nodeFields := fields(storage.Node{}, nil)
	nodeFields = append(nodeFields, fields(storage.NodeRuntimeMetadata{}, map[string]bool{"Database": true, "EmbeddingsStoredSeparately": true})...)
	require.ElementsMatch(t, []string{"ID", "Labels", "Properties", "CreatedAt", "UpdatedAt", "NamedEmbeddings", "ChunkEmbeddings", "EmbedMeta", "VisibilitySuppressed"}, nodeFields)
	require.ElementsMatch(t, []string{"ID", "StartNode", "EndNode", "Type", "Properties", "CreatedAt", "UpdatedAt", "Confidence", "AutoGenerated", "VisibilitySuppressed"}, fields(storage.Edge{}, map[string]bool{"Database": true}))
}

func TestPrototypeTransactionCodecAllStoredFields(t *testing.T) {
	b, _ := wireFixture(t, false)
	node := wireNode("codec", "value")
	node.EmbedMeta = map[string]any{"chunk_count": int8(1), "embedding_model": "synthetic"}
	{
		metadata := node.RuntimeMetadata()
		metadata.VisibilitySuppressed = true
		node.SetRuntimeMetadata(metadata)
	}
	node.ChunkEmbeddings = [][]float32{{1, 2}}
	node.NamedEmbeddings = map[string][]float32{"named": {2, 1}}
	_, err := b.CreateNode(node)
	require.NoError(t, err)
	stored, err := b.GetStoredNode(node.ID)
	require.NoError(t, err)
	edge := wireEdge("codec-edge", "codec", "codec", "value")
	edge.UpdatedAt = time.Unix(1700000004, 123).UTC()
	edge.VisibilitySuppressed = true
	edge.Confidence = 0.8
	edge.AutoGenerated = true
	require.NoError(t, b.CreateEdge(edge))
	storedEdge, err := b.GetStoredEdge(edge.ID)
	require.NoError(t, err)
	for _, mode := range []bool{false, true} {
		nodeKind, edgeKind := storage.OpCreateNode, storage.OpCreateEdge
		if mode {
			nodeKind, edgeKind = storage.OpUpdateNode, storage.OpUpdateEdge
		}
		command := wireCommand(t, "codec", storage.Operation{Type: nodeKind, Node: stored}, storage.Operation{Type: edgeKind, Edge: storedEdge})
		_, ops, err := storage.DecodeEntityTransaction(command.Data)
		require.NoError(t, err)
		require.Len(t, ops, 2)
		assertStoredNodeParity(t, stored, ops[0].Node)
		assertStoredEdgeParity(t, storedEdge, ops[1].Edge)
	}
}

func TestPrototypeTransactionWireLargeVectorUpdateParity(t *testing.T) {
	source, _ := wireFixture(t, false)
	destinationDir := t.TempDir()
	destination, peer := wireFixture(t, false, destinationDir)
	node := wireNode("large-vector", "value")
	node.ChunkEmbeddings = [][]float32{{1}}
	_, err := source.CreateNode(node)
	require.NoError(t, err)
	small, err := source.GetStoredNode(node.ID)
	require.NoError(t, err)
	require.NoError(t, peer.ApplyCommand(context.Background(), wireCommand(t, "small-vector", storage.Operation{Type: storage.OpCreateNode, Node: small})))
	large := storage.CopyNode(small)
	large.ChunkEmbeddings = [][]float32{make([]float32, 16000)}
	for i := range large.ChunkEmbeddings[0] {
		large.ChunkEmbeddings[0][i] = 1.25
	}
	large.EmbedMeta = map[string]any{"embedding_model": "larger"}
	require.NoError(t, source.UpdateNode(large))
	authoritative, err := source.GetStoredNode(node.ID)
	require.NoError(t, err)
	require.Len(t, authoritative.ChunkEmbeddings, 1)
	require.Len(t, authoritative.ChunkEmbeddings[0], 16000)
	require.NoError(t, peer.ApplyCommand(context.Background(), wireCommand(t, "large-vector", storage.Operation{Type: storage.OpUpdateNode, Node: authoritative})))
	require.NoError(t, peer.Close())
	require.NoError(t, destination.Close())
	destination, err = fixtureowner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(destinationDir))
	require.NoError(t, err)
	got, err := destination.GetStoredNode(node.ID)
	require.NoError(t, err)
	require.Len(t, got.ChunkEmbeddings, 1, "destination native staging must persist its own separate chunk layout")
	require.Equal(t, authoritative.ChunkEmbeddings, got.ChunkEmbeddings)
}
