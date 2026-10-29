package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/vmihailenco/msgpack/v5"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

func storedFactNode(t *testing.T, b Engine, id NodeID, labels ...string) {
	t.Helper()
	_, err := b.CreateNode(&Node{ID: id, Labels: labels, Properties: map[string]any{"key": "chosen", "excluded": strings.Repeat("property", 1024)}, EmbedMeta: map[string]any{"excluded": strings.Repeat("metadata", 1024)}, ChunkEmbeddings: [][]float32{{1, 2, 3}}})
	require.NoError(t, err)
}

func TestStoredReadNativeModes(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	storedFactNode(t, b, "d7:n", "L")
	storedFactNode(t, b, "d7:peer", "Peer")
	require.NoError(t, b.CreateEdge(&Edge{ID: "d7:edge", StartNode: "d7:n", EndNode: "d7:peer", Type: "R", Properties: map[string]any{"key": "edge-value", "excluded": strings.Repeat("d7:edge", 1024)}}))
	request := StoredReadRequest{Nodes: []NodeID{"d7:n"}, Edges: []EdgeID{"d7:edge"}, Physical: true}
	facts, err := ReadStoredMetadata(context.Background(), b, request)
	require.NoError(t, err)
	require.NotEmpty(t, facts.Views)
	require.NotEmpty(t, facts.Nodes)
	require.Equal(t, []string{"L"}, facts.Nodes[0].Labels)
	require.True(t, facts.Nodes[0].Exists)
	require.NotZero(t, facts.Nodes[0].Reference.NumericID)
	require.NotZero(t, facts.Nodes[0].Reference.Generation)
	require.Equal(t, NodeID("d7:peer"), facts.Edges[0].EndNode)
	require.NotEmpty(t, facts.PhysicalEdges)
	request.Mode = StoredSelectedProperties
	request.NodeProperties = []string{"key"}
	request.EdgeProperties = []string{"key"}
	selected, err := ReadStored(context.Background(), b, request)
	require.NoError(t, err)
	projection, ok := selected.(StoredProjection)
	require.True(t, ok)
	require.Equal(t, map[string]any{"key": "chosen"}, projection.Nodes[0].Properties)
	require.Equal(t, map[string]any{"key": "edge-value"}, projection.Edges[0].Properties)
	request.Mode = StoredFullMaterialization
	full, err := ReadStored(context.Background(), b, request)
	require.NoError(t, err)
	body, ok := full.(StoredFullRead)
	require.True(t, ok)
	require.Contains(t, body.Nodes[0].Properties, "excluded")
	require.Contains(t, body.Nodes[0].EmbedMeta, "excluded")
}

func TestStoredReadCapturedAndPending(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	storedFactNode(t, b, "d7:n", "Old")
	snapshot, err := b.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	defer snapshot.Close()
	node, err := b.GetNode("d7:n")
	require.NoError(t, err)
	node.Labels = []string{"New"}
	require.NoError(t, b.UpdateNode(node))
	facts, err := ReadStoredMetadata(context.Background(), snapshot, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.NoError(t, err)
	require.Equal(t, []string{"Old"}, facts.Nodes[0].Labels)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	defer tx.Rollback()
	_, err = tx.CreateNode(&Node{ID: "d7:pending", Labels: []string{"Pending"}})
	require.NoError(t, err)
	facts, err = ReadStoredMetadata(context.Background(), tx, StoredReadRequest{Nodes: []NodeID{"d7:pending"}})
	require.NoError(t, err)
	require.True(t, facts.Nodes[0].Pending)
	require.True(t, facts.Nodes[0].Exists)
	require.NoError(t, tx.Rollback())
	_, err = ReadStoredMetadata(context.Background(), tx, StoredReadRequest{Nodes: []NodeID{"d7:pending"}})
	require.ErrorIs(t, err, ErrTransactionClosed)
}

type storedReadVisibleTrap struct {
	Engine
	calls int
}

func (t *storedReadVisibleTrap) GetNode(NodeID) (*Node, error) {
	t.calls++
	panic("visible read cannot supply stored facts")
}
func (t *storedReadVisibleTrap) AllNodes() ([]*Node, error) {
	t.calls++
	panic("full materialization cannot supply metadata")
}

func TestStoredReadMandatoryRefusal(t *testing.T) {
	trap := &storedReadVisibleTrap{}
	_, err := ReadStoredMetadata(context.Background(), trap, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Zero(t, trap.calls)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = ReadStoredMetadata(ctx, trap, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, trap.calls)
}

func TestStoredReadPayloadExclusion(t *testing.T) {
	b, openErr := NewBadgerEngine(t.TempDir())
	require.NoError(t, openErr)
	defer b.Close()
	storedFactNode(t, b, "d7:n", "L")
	var baseline StoredReadWork
	for _, size := range []int{1, 1 << 18} {
		node, err := b.GetNode("d7:n")
		require.NoError(t, err)
		node.Properties["excluded"] = strings.Repeat("x", size)
		node.EmbedMeta["excluded"] = strings.Repeat("y", size)
		node.ChunkEmbeddings = [][]float32{make([]float32, size)}
		require.NoError(t, b.UpdateNode(node))
		var work StoredReadWork
		facts, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"d7:n"}, Work: &work})
		require.NoError(t, err)
		require.True(t, facts.Nodes[0].Exists)
		require.Equal(t, uint64(0), work.PropertyValues)
		require.Equal(t, uint64(0), work.PropertyBytes)
		require.Equal(t, uint64(0), work.VectorValues)
		require.Equal(t, uint64(1), work.Captures)
		if size == 1 {
			baseline = work
		} else {
			require.Equal(t, baseline, work, "metadata decoder work cannot scale with excluded payload")
		}
	}
}

func TestStoredReadNamespaceAndPresence(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	storedFactNode(t, b, "d7:n", "Own")
	storedFactNode(t, b, "foreign:n", "Foreign")
	scoped := NewNamespacedEngine(b, "d7")
	for _, owner := range []any{scoped, &TracedEngine{Engine: scoped}} {
		facts, err := ReadStoredMetadata(context.Background(), owner, StoredReadRequest{Nodes: []NodeID{"n"}, Physical: true})
		require.NoError(t, err)
		require.Len(t, facts.Nodes, 1)
		require.Equal(t, NodeID("n"), facts.Nodes[0].ID)
		require.Equal(t, "d7", facts.Nodes[0].View.Namespace)
		require.Equal(t, []string{"Own"}, facts.Nodes[0].Labels)
		require.Equal(t, facts.Nodes[0].View, facts.PhysicalNodes[0].View)
		present, err := StoredNodePresent(context.Background(), owner, "n")
		require.NoError(t, err)
		require.True(t, present)
	}
	trap := &storedReadVisibleTrap{}
	_, err := StoredNodePresent(context.Background(), trap, "n")
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Zero(t, trap.calls)
}

func TestStoredReadAdjacencyRecheck(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	for _, id := range []NodeID{"d7:n", "d7:old", "d7:new"} {
		storedFactNode(t, b, id, string(id))
	}
	edge := &Edge{ID: "d7:e", StartNode: "d7:n", EndNode: "d7:old", Type: "R"}
	require.NoError(t, b.CreateEdge(edge))
	request := StoredReadRequest{Adjacency: []StoredAdjacencyRequest{{Node: "d7:n", Direction: Outgoing}}, Physical: true}
	before, err := ReadStoredMetadata(context.Background(), b, request)
	require.NoError(t, err)
	require.Len(t, before.Edges, 1)
	require.Equal(t, NodeID("d7:old"), before.Edges[0].EndNode)
	edge.EndNode = "d7:new"
	require.NoError(t, b.UpdateEdge(edge))
	after, err := ReadStoredMetadata(context.Background(), b, request)
	require.NoError(t, err)
	require.Len(t, after.Edges, 1)
	require.Equal(t, NodeID("d7:new"), after.Edges[0].EndNode)
	for _, f := range after.Nodes {
		require.Equal(t, after.Views[0].Store, f.View.Store)
		require.Equal(t, after.Views[0].Timestamp, f.View.Timestamp)
	}
	// A stale independent endpoint read fails the same fact oracle.
	stale := before.Edges[0]
	require.NotEqual(t, after.Edges[0].EndNode, stale.EndNode)
}

func TestStoredReadSnapshotReferenceAndLifetime(t *testing.T) {
	b := NewMemoryEngine()
	defer b.Close()
	storedFactNode(t, b, "d7:n", "Old")
	snapshot, err := b.OpenReadSnapshot(context.Background())
	require.NoError(t, err)
	old, err := ReadStoredMetadata(context.Background(), snapshot, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.NoError(t, err)
	_, _, dropErr := b.DeleteByPrefix("d7:")
	require.NoError(t, dropErr)
	storedFactNode(t, b, "d7:n", "New")
	current, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.NoError(t, err)
	_, err = ReadStoredMetadata(context.Background(), snapshot, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	// The underlying DropPrefix destroys every physical body version. This
	// unsupported retained view must refuse, never return false stored absence.
	require.ErrorIs(t, err, ErrNotImplemented)
	var retained StoredReference
	require.NoError(t, snapshot.(*badgerReadSnapshot).view.withView(func(txn *badger.Txn) error { var e error; retained, e = b.storedNodeReference(txn, "d7:n"); return e }))
	require.Equal(t, old.Nodes[0].Reference, retained)
	require.NotEqual(t, current.Nodes[0].Reference, retained)
	// A substituted current/live dictionary entry fails this identity oracle.
	mutant := current.Nodes[0].Reference
	require.NotEqual(t, old.Nodes[0].Reference, mutant)
	snapshot.Close()
	_, err = ReadStoredMetadata(context.Background(), snapshot, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.ErrorIs(t, err, ErrStorageClosed)
}

// The actual ACK issuer supplies the expected head; metadata does not derive
// its expected value from the buffer selector it is testing.
func TestStoredReadBufferedHeadsAndPhysical(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	_, err := b.CreateNode(&Node{ID: "test:n", Labels: []string{"Old"}})
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "test:peer", Labels: []string{"Peer"}})
	require.NoError(t, err)
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:n", EndNode: "test:peer", Type: "R"}))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	node, err := tx.GetNode("test:n")
	require.NoError(t, err)
	node.Labels = []string{"ACK"}
	node.Properties = map[string]any{"ignored": strings.Repeat("x", 16384)}
	require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	edge, err := tx.GetEdge("test:r")
	require.NoError(t, err)
	edge.Type = "ACK_R"
	require.NoError(t, tx.UpdateEdge(edge))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	var issued MVCCVersion
	b.writeBehind.mu.RLock()
	for _, commit := range b.writeBehind.active.Commits() {
		if commit.txID == tx.ID {
			issued = commit.version
		}
	}
	b.writeBehind.mu.RUnlock()
	require.False(t, issued.IsZero(), "expected version comes from the ACK issuer, not the fact selector")
	var work StoredReadWork
	facts, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"test:n"}, Edges: []EdgeID{"test:r"}, Physical: true, Work: &work})
	require.NoError(t, err)
	require.Equal(t, issued, facts.Nodes[0].Head.Version)
	require.Equal(t, []string{"ACK"}, facts.Nodes[0].Labels)
	require.True(t, facts.Nodes[0].Buffered)
	require.Equal(t, []string{"Old"}, facts.PhysicalNodes[0].Labels)
	require.False(t, facts.PhysicalNodes[0].Buffered)
	require.Equal(t, issued, facts.Edges[0].Head.Version)
	require.Equal(t, "ACK_R", facts.Edges[0].Type)
	require.Equal(t, "R", facts.PhysicalEdges[0].Type)
	require.Equal(t, uint64(1), work.Captures)
	require.Zero(t, work.PropertyValues)
	require.Zero(t, work.PropertyBytes)
	require.Zero(t, work.VectorValues)
}

func storedMetadataWorkOutcome(work StoredReadWork) error {
	if work.PropertyValues != 0 || work.PropertyBytes != 0 || work.VectorValues != 0 || work.FullMaterializations != 0 {
		return fmt.Errorf("excluded payload materialized: %+v", work)
	}
	if work.Captures != 1 {
		return fmt.Errorf("capture count %d", work.Captures)
	}
	return nil
}
func TestStoredReadFullThenStripControl(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(fmt.Sprint(buffered), func(t *testing.T) {
			var b *BadgerEngine
			if buffered {
				b = newWriteBehindTestEngine(t, time.Hour)
				b.writeBehind.Close()
			} else {
				var err error
				b, err = NewBadgerEngine(t.TempDir())
				require.NoError(t, err)
				t.Cleanup(func() { require.NoError(t, b.Close()) })
			}
			storedFactNode(t, b, "d7:n", "L")
			if buffered {
				tx, err := b.BeginTransaction()
				require.NoError(t, err)
				require.NoError(t, tx.SetImplicit(true))
				node, err := tx.GetNode("d7:n")
				require.NoError(t, err)
				node.Properties["excluded"] = strings.Repeat("y", 2048)
				require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
				require.NoError(t, tx.Commit())
				require.True(t, tx.AcknowledgedBuffered())
			}
			var metadataWork StoredReadWork
			expected, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"d7:n"}, Work: &metadataWork})
			require.NoError(t, err)
			require.NoError(t, storedMetadataWorkOutcome(metadataWork))
			var mutantWork StoredReadWork
			full, err := ReadStored(context.Background(), b, StoredReadRequest{Mode: StoredFullMaterialization, Nodes: []NodeID{"d7:n"}, Work: &mutantWork})
			require.NoError(t, err)
			stripped := full.StoredFacts()
			require.Equal(t, expected.Nodes, stripped.Nodes) // values alone would pass
			require.Error(t, storedMetadataWorkOutcome(mutantWork), "same oracle rejects full hydration followed by stripping")
		})
	}
}
func TestStoredReadBatchTopologyWork(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	ids := make([]NodeID, 256)
	for i := range ids {
		ids[i] = NodeID(fmt.Sprintf("n%d", i))
		_, err := b.CreateNode(&Node{ID: NodeID("d7:" + string(ids[i])), Labels: []string{"L"}})
		require.NoError(t, err)
	}
	owner := NewTracedEngine(NewWALEngine(NewNamespacedEngine(b, "d7"), nil))
	_, err := ReadStoredMetadata(context.Background(), owner, StoredReadRequest{Nodes: ids[:1]})
	require.NoError(t, err)
	var baseline [2]uint64
	for _, count := range []int{1, 256} {
		structural := &CapabilityWork{}
		release := ObserveCapabilityWork(structural)
		var work StoredReadWork
		facts, err := ReadStoredMetadata(context.Background(), owner, StoredReadRequest{Nodes: ids[:count], Work: &work})
		release()
		require.NoError(t, err)
		require.Len(t, facts.Nodes, count)
		require.NoError(t, storedMetadataWorkOutcome(work))
		actual := [2]uint64{structural.ProviderVisits.Load(), structural.TypeChecks.Load()}
		if count == 1 {
			baseline = actual
		} else {
			require.Equal(t, baseline, actual, "structural binding work must not grow per requested fact")
		}
		t.Logf("%d facts provider/type work %v", count, actual)
	}
}

type storedFactReadBoundary struct {
	calls   int
	failure error
}

func (b *storedFactReadBoundary) WithStoredEntityRead(_ StoredEntityReadKind, _ string, read func() error) error {
	b.calls++
	if b.failure != nil {
		return b.failure
	}
	return read()
}
func TestStoredReadUsesCanonicalFaultBoundary(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	storedFactNode(t, b, "d7:n", "L")
	expected := errors.New("stored read refused")
	boundary := &storedFactReadBoundary{failure: expected}
	b.updateStoredEntityOwners(func(registry storedEntityObserverRegistry) {
		registry["d7"] = storedEntityObserverEntry{boundary: boundary}
	})
	_, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.ErrorIs(t, err, expected)
	require.Equal(t, 1, boundary.calls)
	_, err = b.GetStoredNode("d7:n")
	require.ErrorIs(t, err, expected)
	require.Equal(t, 2, boundary.calls)
	boundary.failure = nil
	facts, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{"d7:n"}})
	require.NoError(t, err)
	require.True(t, facts.Nodes[0].Exists)
	require.Equal(t, 3, boundary.calls, "one original read continuation per fact")
}

func TestStoredReadRequiredStringsDoNotRetainExcludedPayload(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	_, err := b.CreateNode(&Node{ID: "d7:p", Labels: []string{"Peer"}})
	require.NoError(t, err)
	nodePayload := "d7:nL" + strings.Repeat("x", 16384)
	edgePayload := "d7:eRd7:nd7:p" + strings.Repeat("y", 16384)
	nodeID := NodeID(nodePayload[:4])
	edgeID := EdgeID(edgePayload[:4])
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: nodeID, Labels: []string{nodePayload[4:5]}, Properties: map[string]any{"excluded": nodePayload}})
	require.NoError(t, err)
	require.NoError(t, tx.CreateEdge(&Edge{ID: edgeID, StartNode: nodeID, EndNode: "d7:p", Type: edgePayload[4:5], Properties: map[string]any{"excluded": edgePayload}}))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	// Keep identical values while planting large backing strings in the ACK
	// overlay. This changes no index, visibility, identity or graph semantics.
	func() {
		b.writeBehind.mu.Lock()
		defer b.writeBehind.mu.Unlock()
		nodeState := b.writeBehind.entityStateLocked(bufferedNodeEntity, string(nodeID))
		edgeState := b.writeBehind.entityStateLocked(bufferedEdgeEntity, string(edgeID))
		require.True(t, nodeState.found)
		require.True(t, edgeState.found)
		nodeState.node.Labels = []string{nodePayload[4:5]}
		edgeState.edge.Type = edgePayload[4:5]
		edgeState.edge.StartNode = NodeID(edgePayload[5:9])
		edgeState.edge.EndNode = NodeID(edgePayload[9:13])
	}()
	facts, err := ReadStoredMetadata(context.Background(), b, StoredReadRequest{Nodes: []NodeID{nodeID}, Edges: []EdgeID{edgeID}})
	require.NoError(t, err)
	require.True(t, facts.Nodes[0].Exists)
	require.True(t, facts.Edges[0].Exists)
	require.False(t, unsafe.StringData(string(facts.Nodes[0].ID)) == unsafe.StringData(nodePayload), "input ID backing must not retain the excluded property")
	require.False(t, unsafe.StringData(facts.Nodes[0].Labels[0]) == unsafe.StringData(nodePayload[4:]), "cloning only the label slice retains the excluded property")
	require.False(t, unsafe.StringData(facts.Edges[0].Type) == unsafe.StringData(edgePayload[4:]), "edge type must own its bounded bytes")
	require.False(t, unsafe.StringData(string(facts.Edges[0].StartNode)) == unsafe.StringData(edgePayload[5:]), "endpoint must own its bounded bytes")
}

func TestStoredReadSelectedAndFullPhysicalPredecessors(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	_, err := b.CreateNode(&Node{ID: "d7:n", Labels: []string{"Old"}, Properties: map[string]any{"key": "old", "excluded": "untouched"}})
	require.NoError(t, err)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	t.Cleanup(func() { _ = tx.Rollback() })
	node, err := tx.GetNode("d7:n")
	require.NoError(t, err)
	node.Labels = []string{"New"}
	node.Properties["key"] = "new"
	require.NoError(t, tx.UpdateGraphNode(node, ClientNodeEdits))
	selected, err := ReadStored(context.Background(), tx, StoredReadRequest{Mode: StoredSelectedProperties, Nodes: []NodeID{"d7:n"}, NodeProperties: []string{"key"}, Physical: true})
	require.NoError(t, err)
	projection := selected.(StoredProjection)
	require.Equal(t, map[string]any{"key": "new"}, projection.Nodes[0].Properties)
	require.Equal(t, map[string]any{"key": "old"}, projection.PhysicalNodes[0].Properties)
	require.Equal(t, []string{"Old"}, projection.PhysicalNodes[0].Fact.Labels)
	require.Equal(t, projection.Nodes[0].Fact.View, projection.PhysicalNodes[0].Fact.View)
	full, err := ReadStored(context.Background(), tx, StoredReadRequest{Mode: StoredFullMaterialization, Nodes: []NodeID{"d7:n"}, Physical: true})
	require.NoError(t, err)
	bodies := full.(StoredFullRead)
	require.Equal(t, "new", bodies.Nodes[0].Properties["key"])
	require.Equal(t, "old", bodies.PhysicalNodes[0].Properties["key"])
	// Equivalent physical/logical images still own independent mutable payloads.
	equal, err := ReadStored(context.Background(), b, StoredReadRequest{Mode: StoredSelectedProperties, Nodes: []NodeID{"d7:n"}, NodeProperties: []string{"key"}, Physical: true})
	require.NoError(t, err)
	copies := equal.(StoredProjection)
	copies.Nodes[0].Properties["key"] = "edited"
	require.Equal(t, "old", copies.PhysicalNodes[0].Properties["key"])
}
func TestStoredReadMetadataRefusesBeforeExcludedField(t *testing.T) {
	var payload bytes.Buffer
	encoder := msgpack.NewEncoder(&payload)
	require.NoError(t, encoder.EncodeMapLen(3))
	require.NoError(t, encoder.EncodeString("EmbedMeta"))
	require.NoError(t, encoder.EncodeMapLen(1<<20))
	// This excluded map is intentionally truncated. A metadata request must
	// refuse the noncanonical field order before inspecting its payload at all.
	metadata := append([]byte(serializationMagic), serializationVersion, serializerIDMsgpack)
	metadata = append(metadata, payload.Bytes()...)
	body := []byte{nodeFormatTokenizedV1}
	body = binary.AppendUvarint(body, 0)
	body = append(body, metadata...)
	var work StoredReadWork
	_, err := decodeNodeFactMetadata(context.Background(), body, &work)
	require.ErrorIs(t, err, ErrNotImplemented)
	require.Zero(t, work.PropertyValues)
	require.Zero(t, work.PropertyBytes)
	require.Zero(t, work.VectorValues)
}

// The metadata variant cannot acquire a payload carrier even if a future body
// or property field happens to be nil in every current fixture.
func storedMetadataShapeProblem(typ reflect.Type) error {
	// MVCC's existing timestamp is an atomic scalar. Its standard-library
	// location pointer is not an entity/property/embedding carrier.
	if typ == reflect.TypeFor[time.Time]() {
		return nil
	}
	switch typ.Kind() {
	case reflect.Struct:
		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			if err := storedMetadataShapeProblem(field.Type); err != nil {
				return fmt.Errorf("%s.%s: %w", typ, field.Name, err)
			}
		}
		return nil
	case reflect.Slice:
		if typ.Elem().Kind() != reflect.String && typ.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("metadata carries payload slice %s", typ)
		}
		return storedMetadataShapeProblem(typ.Elem())
	case reflect.Bool, reflect.String, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return nil
	default:
		return fmt.Errorf("metadata carries %s", typ)
	}
}

func TestStoredReadMetadataPayloadShape(t *testing.T) {
	require.NoError(t, storedMetadataShapeProblem(reflect.TypeFor[StoredMetadata]()))
	for name, typ := range map[string]reflect.Type{
		"node": reflect.TypeFor[struct {
			Facts StoredMetadata
			Body  *Node
		}](),
		"edge": reflect.TypeFor[struct {
			Facts StoredMetadata
			Body  *Edge
		}](),
		"properties": reflect.TypeFor[struct {
			Facts      StoredMetadata
			Properties map[string]any
		}](),
		"opaque": reflect.TypeFor[struct {
			Facts   StoredMetadata
			Payload any
		}](),
		"vectors": reflect.TypeFor[struct {
			Facts   StoredMetadata
			Vectors []float32
		}](),
		"bytes": reflect.TypeFor[struct {
			Facts   StoredMetadata
			Payload []byte
		}](),
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, storedMetadataShapeProblem(typ), "the same shape oracle must reject a nil payload carrier")
		})
	}
}

// Observe entry into the actual shared preparation owner during the public
// operation; equal facts from a private raw-buffer selection do not qualify.
type storedReadOwnerContext struct {
	context.Context
	visits int
}

func (c *storedReadOwnerContext) Err() error {
	var pcs [2]uintptr
	if n := runtime.Callers(2, pcs[:]); n > 0 {
		frame, _ := runtime.CallersFrames(pcs[:n]).Next()
		if strings.HasSuffix(frame.Function, ".readBufferedEntitiesWithContext") {
			c.visits++
		}
	}
	return c.Context.Err()
}

func TestStoredReadBufferedAdjacencyUsesReadOwner(t *testing.T) {
	b := newWriteBehindTestEngine(t, time.Hour)
	b.writeBehind.Close()
	for _, id := range []NodeID{"test:a", "test:b"} {
		_, err := b.CreateNode(&Node{ID: id, Labels: []string{"L"}})
		require.NoError(t, err)
	}
	require.NoError(t, b.CreateEdge(&Edge{ID: "test:r", StartNode: "test:a", EndNode: "test:b", Type: "R"}))
	tx, err := b.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	require.NoError(t, tx.SetNamespace("test"))
	edge, err := tx.GetEdge("test:r")
	require.NoError(t, err)
	edge.Type = "ACK_R"
	edge.Properties = map[string]any{"excluded": strings.Repeat("payload", 4096)}
	require.NoError(t, tx.UpdateEdge(edge))
	require.NoError(t, tx.Commit())
	require.True(t, tx.AcknowledgedBuffered())
	ctx := &storedReadOwnerContext{Context: context.Background()}
	var work StoredReadWork
	facts, err := ReadStoredMetadata(ctx, b, StoredReadRequest{Adjacency: []StoredAdjacencyRequest{{Node: "test:a", Direction: Outgoing}}, Work: &work})
	require.NoError(t, err)
	require.Len(t, facts.Edges, 1)
	require.Equal(t, "ACK_R", facts.Edges[0].Type)
	require.True(t, facts.Edges[0].Buffered)
	require.Zero(t, work.PropertyValues)
	require.Zero(t, work.VectorValues)
	require.Equal(t, 1, ctx.visits, "the required captured overlay owner must execute once for this adjacency request")
}
