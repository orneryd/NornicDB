package cypher_test

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/replication"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type differentialCoreEngine struct{ storage.Engine }

func (e *differentialCoreEngine) GetInnerEngine() storage.Engine { return e.Engine }

// Capability masks preserve the identity of a composite view. Hiding optional
// reads must not change its explicit-targeting policy.
type differentialCompositeCore struct{ storage.Engine }

func (e *differentialCompositeCore) GetInnerEngine() storage.Engine { return e.Engine }
func (*differentialCompositeCore) IsComposite() bool                { return true }
func (*differentialCompositeCore) CompositeName() string            { return "comp" }

type differentialCompositeUnavailable struct{ *differentialUnavailableEngine }

func (*differentialCompositeUnavailable) IsComposite() bool     { return true }
func (*differentialCompositeUnavailable) CompositeName() string { return "comp" }

// The method set deliberately overstates support. The availability contract
// must stop these methods from being selected, before a callback sees a row.
type differentialUnavailableEngine struct {
	storage.Engine
	calls int
}

func (e *differentialUnavailableEngine) GetInnerEngine() storage.Engine { return e.Engine }

func (e *differentialUnavailableEngine) ReadCapabilityAvailable(request reflect.Type) bool {
	return request == reflect.TypeFor[storage.Engine]()
}

func (e *differentialUnavailableEngine) poison() error { e.calls++; return storage.ErrNotImplemented }
func (e *differentialUnavailableEngine) GetNodeProjected(storage.NodeID, []string) (*storage.Node, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetQueryNode(storage.NodeID) (*storage.Node, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetNodeWithoutEmbeddings(storage.NodeID) (*storage.Node, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) BatchGetNodesWithoutEmbeddings([]storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) StreamNodesByLabelProjected(string, []string, func(*storage.Node) error) error {
	return e.poison()
}
func (e *differentialUnavailableEngine) StreamNodes(context.Context, func(*storage.Node) error) error {
	return e.poison()
}
func (e *differentialUnavailableEngine) StreamEdges(context.Context, func(*storage.Edge) error) error {
	return e.poison()
}
func (e *differentialUnavailableEngine) StreamNodeChunks(context.Context, int, func([]*storage.Node) error) error {
	return e.poison()
}
func (e *differentialUnavailableEngine) GetAdjacentEdges(storage.NodeID) ([]*storage.Edge, []*storage.Edge, error) {
	return nil, nil, e.poison()
}
func (e *differentialUnavailableEngine) RecordMaterializedAccess(string) { _ = e.poison() }
func (e *differentialUnavailableEngine) GetNodesByLabelVisibleAt(string, storage.MVCCVersion) ([]*storage.Node, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetOutgoingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetIncomingEdgesVisibleAt(storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetEdgesByTypeVisibleAt(string, storage.MVCCVersion) ([]*storage.Edge, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) GetEdgesBetweenVisibleAt(storage.NodeID, storage.NodeID, storage.MVCCVersion) ([]*storage.Edge, error) {
	return nil, e.poison()
}
func (e *differentialUnavailableEngine) LookupNodesByEquality(string, map[string]interface{}) ([]*storage.Node, bool, error) {
	return nil, false, e.poison()
}

// Generate every ordering of these production wrappers. Size tracking owns a
// database-scoped engine; replication owns physical storage, below namespace
// translation. T and W can be on either side of all three. Repeated T/W and
// transaction/composite boundaries are exercised by the other family tests.
func differentialProductionOrders() []string {
	return differentialOrders("TWSNR", "SNR")
}

func differentialOrders(letters, viewOrder string, additionalOrders ...string) []string {
	var orders []string
	var visit func(string, string)
	visit = func(prefix, remaining string) {
		if remaining == "" {
			for _, chain := range append([]string{viewOrder}, additionalOrders...) {
				for i := 1; i < len(chain); i++ {
					if strings.IndexByte(prefix, chain[i-1]) >= strings.IndexByte(prefix, chain[i]) {
						return
					}
				}
			}
			orders = append(orders, prefix)
			return
		}
		for i := range remaining {
			visit(prefix+remaining[i:i+1], remaining[:i]+remaining[i+1:])
		}
	}
	visit("", letters)
	sort.Strings(orders)
	return orders
}

func differentialWrap(t *testing.T, base, managed storage.Engine, order, state string, gap int) (storage.Engine, *differentialUnavailableEngine) {
	t.Helper()
	engine := base
	var unavailable *differentialUnavailableEngine
	for i := len(order); i >= 0; i-- {
		if gap == i && state != "present" {
			composite, isComposite := storage.ViewMetadataForEngine[interface{ IsComposite() bool }](engine)
			isComposite = isComposite && composite.IsComposite()
			if state == "absent" {
				if isComposite {
					engine = &differentialCompositeCore{engine}
				} else {
					engine = &differentialCoreEngine{engine}
				}
			} else {
				unavailable = &differentialUnavailableEngine{Engine: engine}
				if isComposite {
					engine = &differentialCompositeUnavailable{unavailable}
				} else {
					engine = unavailable
				}
			}
		}
		if i == 0 {
			break
		}
		switch order[i-1] {
		case 'T':
			engine = storage.NewTracedEngine(engine)
		case 'W':
			engine = storage.NewWALEngine(engine, nil)
		case 'R':
			engine = replication.NewReplicatedEngine(engine, nil, time.Second)
		case 'N':
			engine = storage.NewNamespacedEngine(engine, "tenant")
		case 'S':
			field := reflect.ValueOf(managed).Elem().FieldByName("Engine")
			require.True(t, field.CanSet())
			field.Set(reflect.ValueOf(engine))
			// This fixture replaces a dependency after the manager issued its
			// original view. Publish that replacement through the same explicit
			// rebind owner production writers use; row reads never rediscover it.
			storage.RebindReadView(managed)
			engine = managed
		case 'C':
			composite := storage.NewCompositeEngine(map[string]storage.Engine{"tenant": engine}, map[string]string{"tenant": "tenant"}, map[string]string{"tenant": "read_write"})
			composite.SetCompositeName("comp")
			engine = composite
		}
	}
	return engine, unavailable
}

func differentialNodeValues(nodes []*storage.Node) []int64 {
	var values []int64
	for _, n := range nodes {
		if n != nil {
			values = append(values, n.Properties["v"].(int64))
		}
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	return values
}

// These public queries force projected label/node reads, batched hydration,
// full/label streams, adjacency, temporal lookup, and entity materialization.
// The oracle exposes only Engine methods on the same namespace and data.
func TestEngineDecoratorsDifferentialAvailability(t *testing.T) {
	queries := []string{
		"MATCH (n:R2Absent) RETURN n.v",
		"MATCH (n:R2Absent) WHERE n.v=1 RETURN n.v",
		"MATCH (n) WHERE n.v=1 RETURN n.v",
		"MATCH (n:R2Absent)-[:LINK]->(m) RETURN m.v",
		"MATCH (n:R2Absent)-[:LINK*1..2]->(m) RETURN m.v",
		"MATCH (n:R2Absent) RETURN count(n)",
		"CALL db.temporal.asOf('R2Absent','v',1,'from','to','2024-01-15T00:00:00Z') YIELD node RETURN node.v",
		"MATCH (n:R2Absent) RETURN n",
	}
	orders := differentialProductionOrders()
	require.Len(t, orders, 20)
	comparisons := 0
	for _, order := range orders {
		for _, state := range []string{"present", "absent", "unavailable"} {
			for gap := 0; gap <= len(order); gap++ {
				if state == "present" && gap != 0 {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/gap%d", order, state, gap), func(t *testing.T) {
					base := storage.NewMemoryEngineWithMVCCHistory()
					manager, err := multidb.NewDatabaseManager(base, multidb.NewConfigFromDefaultDatabase("tenant"))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					managed, err := manager.GetStorage("tenant")
					require.NoError(t, err)
					tenant := storage.NewNamespacedEngine(base, "tenant")
					for _, seed := range []struct {
						id    storage.NodeID
						label string
						v     int64
					}{{"n", "R2Absent", 1}, {"m", "Target", 2}} {
						_, err = tenant.CreateNode(&storage.Node{ID: seed.id, Labels: []string{seed.label}, Properties: map[string]interface{}{"v": seed.v, "from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "to": time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)}})
						require.NoError(t, err)
					}
					require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "e", StartNode: "n", EndNode: "m", Type: "LINK"}))
					_, err = storage.NewNamespacedEngine(base, "other").CreateNode(&storage.Node{ID: "n", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(99)}})
					require.NoError(t, err)
					_, err = tenant.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(66)}})
					require.NoError(t, err)
					require.NoError(t, tenant.SetManualSuppression(storage.StoredEntityNodeKind, "hidden", true))
					_, err = tenant.CreateNode(&storage.Node{ID: "history", Labels: []string{"History"}, Properties: map[string]interface{}{"v": int64(7), "from": time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC), "to": time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)}})
					require.NoError(t, err)
					head, err := tenant.GetNodeCurrentHead("history")
					require.NoError(t, err)
					require.NoError(t, tenant.DeleteNode("history"))
					engine, unavailable := differentialWrap(t, base, managed, order, state, gap)
					oracle := cypher.NewStorageExecutor(&differentialCoreEngine{tenant})
					executor := cypher.NewStorageExecutor(engine)
					for _, query := range queries {
						expected, err := oracle.Execute(context.Background(), query, nil)
						require.NoError(t, err, "oracle: %s", query)
						actual, err := executor.Execute(context.Background(), query, nil)
						require.NoError(t, err, "%s", query)
						if query == "MATCH (n:R2Absent) RETURN n" {
							require.Len(t, actual.Rows, 1)
							n, ok := actual.Rows[0][0].(*storage.Node)
							require.True(t, ok)
							require.Equal(t, storage.NodeID("n"), n.ID)
							require.Equal(t, int64(1), n.Properties["v"])
						} else {
							require.Equal(t, expected.Rows, actual.Rows, "%s", query)
						}
						comparisons++
					}
					// Direct batched optional consumers use the same core fallback.
					var nodes map[storage.NodeID]*storage.Node
					if reader, ok := capabilityForTest[storage.BatchNodeWithoutEmbeddingsReader](engine); ok {
						nodes, err = reader.BatchGetNodesWithoutEmbeddings([]storage.NodeID{"n", "m"})
					} else {
						nodes, err = engine.BatchGetNodes([]storage.NodeID{"n", "m"})
					}
					require.NoError(t, err)
					require.Len(t, nodes, 2)
					require.Equal(t, int64(1), nodes["n"].Properties["v"])
					require.Equal(t, int64(2), nodes["m"].Properties["v"])
					query := fmt.Sprintf("CALL db.temporal.asOf('History','v',7,'from','to','2024-01-15T00:00:00Z','%s',%d) YIELD node RETURN node.v", head.Version.CommitTimestamp.Format(time.RFC3339Nano), head.Version.CommitSequence)
					history, historyErr := executor.Execute(context.Background(), query, nil)
					if state == "present" {
						expected, oracleErr := cypher.NewStorageExecutor(tenant).Execute(context.Background(), query, nil)
						require.NoError(t, oracleErr)
						require.NoError(t, historyErr)
						require.Equal(t, expected.Rows, history.Rows)
						require.Equal(t, [][]interface{}{{int64(7)}}, history.Rows)
					} else {
						// Engine alone has no historical operation. Compare its
						// refusal, never substitute today's graph for a snapshot.
						_, oracleErr := oracle.Execute(context.Background(), query, nil)
						require.Error(t, oracleErr)
						require.Error(t, historyErr)
						require.ErrorIs(t, historyErr, storage.ErrNotImplemented)
					}
					// The transaction view must own equality lookup and its
					// fallback, including updates/new nodes still uncommitted.
					for _, reader := range []*cypher.StorageExecutor{oracle, executor} {
						for _, statement := range []string{"BEGIN", "MATCH (n:R2Absent) SET n.v=3", "MERGE (n:R2Absent {v:3}) ON MATCH SET n.touched=true", "MERGE (n:Pending {v:4}) ON CREATE SET n.v=5", "MERGE (n:Pending {v:5}) ON MATCH SET n.touched=true"} {
							_, txErr := reader.Execute(context.Background(), statement, nil)
							require.NoError(t, txErr, statement)
						}
						pending, txErr := reader.Execute(context.Background(), "MATCH (n) WHERE n.v IN [3,5] RETURN n.v ORDER BY n.v", nil)
						require.NoError(t, txErr)
						require.Equal(t, [][]interface{}{{int64(3)}, {int64(5)}}, pending.Rows)
						_, txErr = reader.Execute(context.Background(), "ROLLBACK", nil)
						require.NoError(t, txErr)
						comparisons++
					}
					if unavailable != nil {
						require.Zero(t, unavailable.calls, "an unavailable optional operation was invoked")
					}
					comparisons += 2
				})
			}
		}
	}
	t.Logf("production orders=%d; differential comparisons=%d", len(orders), comparisons)
}

func TestEngineDecoratorsCompositeDifferentialAvailability(t *testing.T) {
	// DatabaseManager builds C(S(N(physical))). Also exercise an outer size
	// decorator over a composite. Both S and C stay above namespace translation;
	// neither may hide the other's read boundary or availability decision.
	orders := differentialOrders("TWSCNR", "SNR", "CNR")
	require.Len(t, orders, 60)
	comparisons := 0
	for _, order := range orders {
		for _, state := range []string{"present", "absent", "unavailable"} {
			for gap := 0; gap <= len(order); gap++ {
				if state == "present" && gap != 0 {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/gap%d", order, state, gap), func(t *testing.T) {
					base := storage.NewMemoryEngineWithMVCCHistory()
					manager, err := multidb.NewDatabaseManager(base, multidb.NewConfigFromDefaultDatabase("tenant"))
					require.NoError(t, err)
					t.Cleanup(func() { require.NoError(t, manager.Close()) })
					managed, err := manager.GetStorage("tenant")
					require.NoError(t, err)
					tenant := storage.NewNamespacedEngine(base, "tenant")
					for _, seed := range []struct {
						id    storage.NodeID
						label string
						v     int64
					}{{"n", "R2Absent", 1}, {"m", "Target", 2}} {
						_, err = tenant.CreateNode(&storage.Node{ID: seed.id, Labels: []string{seed.label}, Properties: map[string]interface{}{"v": seed.v}})
						require.NoError(t, err)
					}
					require.NoError(t, tenant.CreateEdge(&storage.Edge{ID: "e", StartNode: "n", EndNode: "m", Type: "LINK"}))
					_, err = storage.NewNamespacedEngine(base, "other").CreateNode(&storage.Node{ID: "n", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(99)}})
					require.NoError(t, err)
					_, err = tenant.CreateNode(&storage.Node{ID: "hidden", Labels: []string{"R2Absent"}, Properties: map[string]interface{}{"v": int64(66)}})
					require.NoError(t, err)
					require.NoError(t, tenant.SetManualSuppression(storage.StoredEntityNodeKind, "hidden", true))
					engine, unavailable := differentialWrap(t, base, managed, order, state, gap)
					// The root is refused before graph reads or writes, as on a
					// plain composite; availability cannot erase its identity.
					executor := cypher.NewStorageExecutor(engine)
					for _, query := range []string{"MATCH (n:R2Absent) RETURN n.v", "MERGE (n:R2Absent {v:1}) RETURN n.v"} {
						_, err = executor.Execute(context.Background(), query, nil)
						require.Error(t, err)
						code, _ := nerrors.Neo4jStatus(err)
						require.Equal(t, "Neo.ClientError.Statement.NotAllowed", code)
						comparisons++
					}
					// Read the same composite view through its storage surface.
					// Its constituent data is exactly the plain tenant oracle.
					var nodes []*storage.Node
					if projected, ok := capabilityForTest[storage.ProjectedLabelNodeReader](engine); ok {
						err = projected.StreamNodesByLabelProjected("R2Absent", []string{"v"}, func(n *storage.Node) error { nodes = append(nodes, n); return nil })
					} else {
						nodes, err = engine.GetNodesByLabel("R2Absent")
					}
					require.NoError(t, err)
					expected, err := tenant.GetNodesByLabel("R2Absent")
					require.NoError(t, err)
					require.Equal(t, differentialNodeValues(expected), differentialNodeValues(nodes))
					comparisons++
					var node *storage.Node
					if projected, ok := capabilityForTest[storage.ProjectedNodeReader](engine); ok {
						node, err = projected.GetNodeProjected("n", []string{"v"})
					} else {
						node, err = engine.GetNode("n")
					}
					require.NoError(t, err)
					require.Equal(t, int64(1), node.Properties["v"])
					comparisons++
					var outgoing []*storage.Edge
					if adjacent, ok := capabilityForTest[storage.AdjacentEdgesEngine](engine); ok {
						outgoing, _, err = adjacent.GetAdjacentEdges("n")
					} else {
						outgoing, err = engine.GetOutgoingEdges("n")
					}
					require.NoError(t, err)
					require.Len(t, outgoing, 1)
					require.Equal(t, storage.NodeID("m"), outgoing[0].EndNode)
					comparisons++
					var batch map[storage.NodeID]*storage.Node
					if reader, ok := capabilityForTest[storage.BatchNodeWithoutEmbeddingsReader](engine); ok {
						batch, err = reader.BatchGetNodesWithoutEmbeddings([]storage.NodeID{"n", "m"})
					} else {
						batch, err = engine.BatchGetNodes([]storage.NodeID{"n", "m"})
					}
					require.NoError(t, err)
					require.Len(t, batch, 2)
					comparisons++
					var streamed []*storage.Node
					visit := func(n *storage.Node) error { streamed = append(streamed, n); return nil }
					if reader, ok := capabilityForTest[storage.StreamingEngine](engine); ok {
						err = reader.StreamNodes(context.Background(), visit)
					} else {
						err = engine.StreamNodesWithOptions(context.Background(), storage.StreamNodesOptions{ApplyDecayFilter: true}, visit)
					}
					require.NoError(t, err)
					require.Equal(t, []int64{1, 2}, differentialNodeValues(streamed))
					comparisons++
					_, historical := capabilityForTest[storage.MVCCIndexedVisibilityEngine](engine)
					require.Equal(t, state == "present", historical)
					comparisons++
					if unavailable != nil {
						require.Zero(t, unavailable.calls)
					}
				})
			}
		}
	}
	t.Logf("composite production orders=%d; differential comparisons=%d", len(orders), comparisons)
}

func (e *differentialCoreEngine) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return e.Engine, true
}

func (e *differentialCompositeCore) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return e.Engine, true
}

func (e *differentialUnavailableEngine) CapabilityOwnerSource(policy storage.OwnerPolicy) (storage.Engine, bool) {
	return e.Engine, true
}

func (e *differentialCoreEngine) Namespace() string        { return storage.DatabaseOf(e.Engine) }
func (e *differentialUnavailableEngine) Namespace() string { return storage.DatabaseOf(e.Engine) }
