package storage

import (
	"context"
	"errors"
	"fmt"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestStoredReadCompositeQualifiedAdmission(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for _, ns := range []string{"tenant", "foreign"} {
		for _, local := range []string{"a", "b"} {
			_, err := b.CreateNode(&Node{ID: NodeID(ns + ":" + local), Labels: []string{ns}})
			require.NoError(t, err)
		}
		require.NoError(t, b.CreateEdge(&Edge{ID: EdgeID(ns + ":e"), StartNode: NodeID(ns + ":a"), EndNode: NodeID(ns + ":b"), Type: "R"}))
	}
	denied := errors.New("unavailable constituent admission")
	var opened []string
	rejectTenant := false
	c := NewCompositeEngine(map[string]Engine{"a-denied": NewNamespacedEngine(b, "foreign"), "z-selected": NewNamespacedEngine(b, "tenant")}, map[string]string{"a-denied": "foreign", "z-selected": "tenant"}, map[string]string{"a-denied": "read", "z-selected": "read"}).WithConstituentAccess(func(database string, engine Engine, write bool) (Engine, error) {
		opened = append(opened, database)
		if database == "foreign" || rejectTenant {
			return nil, denied
		}
		return engine, nil
	})
	nodeID := NodeID(SourceNodeElementID("tenant", "a"))
	edgeID := EdgeID(SourceRelationshipElementID("tenant", "e"))
	ordinary, err := c.GetNode(nodeID)
	require.NoError(t, err)
	require.Equal(t, "tenant", ordinary.Database)
	for _, mode := range []StoredReadMode{StoredMetadataOnly, StoredSelectedProperties, StoredFullMaterialization} {
		for name, request := range map[string]StoredReadRequest{
			"node": {Nodes: []NodeID{nodeID}}, "edge": {Edges: []EdgeID{edgeID}}, "adjacency": {Adjacency: []StoredAdjacencyRequest{{Node: nodeID, Direction: Outgoing}}},
		} {
			t.Run(fmt.Sprintf("%d/%s", mode, name), func(t *testing.T) {
				opened = nil
				request.Mode = mode
				request.Physical = true
				result, err := ReadStored(context.Background(), c, request)
				require.NoError(t, err)
				require.NotEmpty(t, result.StoredFacts().Views)
				require.Equal(t, []string{"tenant"}, opened, "only the requested source is admitted; metadata discovery never opens a sibling")
				for _, view := range result.StoredFacts().Views {
					require.Equal(t, "z-selected", view.Constituent)
				}
			})
		}
	}
	opened = nil
	result, err := ReadStoredMetadata(context.Background(), c, StoredReadRequest{Nodes: []NodeID{"a"}})
	require.ErrorIs(t, err, denied)
	require.Empty(t, result.Nodes, "an unqualified request still requires the unavailable constituent")
	rejectTenant = true
	opened = nil
	_, err = ReadStoredMetadata(context.Background(), c, StoredReadRequest{Nodes: []NodeID{nodeID}})
	require.ErrorIs(t, err, denied)
	require.Equal(t, []string{"tenant"}, opened, "binding topology never caches live participant admission")
}

type foundationMetadataCounter struct {
	Engine
	namespaceCalls int
}

func (e *foundationMetadataCounter) Namespace() string { e.namespaceCalls++; return "tenant" }

func TestFoundationCompositeMetadataBindingWork(t *testing.T) {
	b := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, b.Close()) })
	for i := 0; i < 256; i++ {
		_, err := b.CreateNode(&Node{ID: NodeID(fmt.Sprintf("tenant:n%d", i)), Labels: []string{"L"}})
		require.NoError(t, err)
	}
	source := &foundationMetadataCounter{Engine: NewNamespacedEngine(b, "tenant")}
	modes := map[string]string{"a": "read"}
	allowed := true
	c := NewCompositeEngine(map[string]Engine{"a": source}, map[string]string{"a": "tenant"}, modes).WithConstituentAccess(func(_ string, engine Engine, _ bool) (Engine, error) {
		if !allowed {
			return nil, ErrNotFound
		}
		return engine, nil
	})
	_, err := c.GetNode(NodeID(SourceNodeElementID("tenant", "n0")))
	require.NoError(t, err)
	var baseline [2]uint64
	for _, count := range []int{1, 256} {
		source.namespaceCalls = 0
		work := &CapabilityWork{}
		release := ObserveCapabilityWork(work)
		for i := 0; i < count; i++ {
			node, err := c.GetNode(NodeID(SourceNodeElementID("tenant", NodeID(fmt.Sprintf("n%d", i)))))
			require.NoError(t, err)
			require.Equal(t, "tenant", node.Database)
		}
		release()
		require.Zero(t, source.namespaceCalls, "view metadata providers are bound before row reads")
		actual := [2]uint64{work.ProviderVisits.Load(), work.TypeChecks.Load()}
		if count == 1 {
			baseline = actual
		} else {
			require.Equal(t, baseline, actual, "structural view work must not grow with public point-read rows")
		}
	}
	modes["a"] = "write"
	_, err = c.GetNode(NodeID(SourceNodeElementID("tenant", "n0")))
	require.NoError(t, err, "caller configuration is fixed at handoff")
	allowed = false
	_, err = c.GetNode(NodeID(SourceNodeElementID("tenant", "n0")))
	require.ErrorIs(t, err, ErrNotFound, "bound participant admission must remain live")
	allowed = true
	_, err = c.GetNode(NodeID(SourceNodeElementID("tenant", "n0")))
	require.NoError(t, err)

}

// Core reads use the issued Engine. Optional data capability delegation must not
// erase that engine's interceptors when a namespace qualifies a core request.
type foundationCoreReadInterceptor struct {
	Engine
	nodes, edges int
}

func (e *foundationCoreReadInterceptor) ReadCapabilitySource() (Engine, bool) { return e.Engine, true }
func (e *foundationCoreReadInterceptor) GetNode(id NodeID) (*Node, error) {
	e.nodes++
	return e.Engine.GetNode(id)
}
func (e *foundationCoreReadInterceptor) GetEdge(id EdgeID) (*Edge, error) {
	e.edges++
	return e.Engine.GetEdge(id)
}

func TestFoundationNamespaceCoreReadKeepsIssuedEngine(t *testing.T) {
	base := NewMemoryEngine()
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	_, err := base.CreateNode(&Node{ID: "tenant:n"})
	require.NoError(t, err)
	require.NoError(t, base.CreateEdge(&Edge{ID: "tenant:e", StartNode: "tenant:n", EndNode: "tenant:n", Type: "R"}))
	issued := &foundationCoreReadInterceptor{Engine: base}
	view := NewNamespacedEngine(issued, "tenant")
	node, err := view.GetNode("n")
	require.NoError(t, err)
	require.Equal(t, NodeID("n"), node.ID)
	edge, err := view.GetEdge("e")
	require.NoError(t, err)
	require.Equal(t, EdgeID("e"), edge.ID)
	require.Equal(t, 1, issued.nodes)
	require.Equal(t, 1, issued.edges)
}
