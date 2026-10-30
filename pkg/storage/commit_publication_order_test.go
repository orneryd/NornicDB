package storage

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

type publicationOrderWorker struct {
	done chan struct{}
	err  error
}

func (w *publicationOrderWorker) start(write func() error) {
	w.done = make(chan struct{})
	go func() { defer close(w.done); w.err = write() }()
}
func (w *publicationOrderWorker) join(t *testing.T) {
	t.Helper()
	if w.done == nil {
		return
	}
	select {
	case <-w.done:
	case <-time.After(15 * time.Second):
		t.Fatal("publication writer did not finish")
	}
}

// Each older write has committed before the newer write begins. Pausing at
// the existing commit-tail seam lets a newer writer overtake the older tail;
// its final public body, index and notifications must still describe the
// physical commit order. The seam is now after ordered derived publication.
func TestCommitPublicationPublicOrder(t *testing.T) {
	modes := []struct {
		name                       string
		buffered, olderTx, newerTx bool
	}{
		{name: "sync-direct-direct"}, {name: "sync-tx-direct", olderTx: true},
		{name: "sync-direct-tx", newerTx: true}, {name: "sync-tx-tx", olderTx: true, newerTx: true},
		{name: "buffered-explicit-direct", buffered: true, olderTx: true},
		{name: "buffered-explicit-explicit", buffered: true, olderTx: true, newerTx: true},
	}
	cases := []struct{ name, older, newer, final string }{
		{"update-update", "update", "update", "C"},
		{"delete-recreate-same-value", "delete", "create", "A"},
		{"delete-recreate-different-value", "delete", "create", "C"},
	}
	for _, entity := range []string{"node", "edge"} {
		for _, mode := range modes {
			for _, c := range cases {
				t.Run(entity+"/"+mode.name+"/"+c.name, func(t *testing.T) {
					b, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: mode.buffered, WriteBehindInterval: time.Hour}))
					if b != nil {
						fixtureowner.Own(t, b)
					}
					require.NoError(t, err)
					sm := b.GetSchemaForNamespace("db")
					node := func(value string) *Node {
						return &Node{ID: "db:n", Labels: []string{"L"}, Properties: map[string]any{"v": value}}
					}
					edge := func(value string) *Edge {
						return &Edge{ID: "db:e", StartNode: "db:a", EndNode: "db:b", Type: "LINK", Properties: map[string]any{"v": value}}
					}
					if entity == "node" {
						require.NoError(t, sm.AddPropertyIndex("index_v", "L", []string{"v"}))
						sm.MarkPropertyIndexesFilled()
						_, err = b.CreateNode(node("A"))
						require.NoError(t, err)
					} else {
						// Unlabelled endpoints have no positional label counter dependency;
						// this isolates the entity publication contract from counter admission.
						for _, id := range []NodeID{"db:a", "db:b"} {
							_, err = b.CreateNode(&Node{ID: id})
							require.NoError(t, err)
						}
						require.NoError(t, b.CreateEdge(edge("A")))
					}
					require.NoError(t, b.FlushWriteBehind())
					if entity == "node" {
						_, err = b.GetNode("db:n")
					} else {
						_, err = b.GetEdge("db:e")
					}
					require.NoError(t, err)
					var eventsMu sync.Mutex
					var events []string
					record := func(kind string, value any) {
						eventsMu.Lock()
						defer eventsMu.Unlock()
						events = append(events, fmt.Sprint(kind, ":", value))
					}
					if entity == "node" {
						b.OnNodeUpdated(func(n *Node) { record("update", n.Properties["v"]) })
						b.OnNodeCreated(func(n *Node) { record("create", n.Properties["v"]) })
						b.OnNodeDeleted(func(NodeID) { record("delete", "") })
					} else {
						b.OnEdgeUpdated(func(e *Edge) { record("update", e.Properties["v"]) })
						b.OnEdgeCreated(func(e *Edge) { record("create", e.Properties["v"]) })
						b.OnEdgeDeleted(func(EdgeID) { record("delete", "") })
					}
					write := func(kind, value string, transactional bool) error {
						if !transactional {
							if entity == "node" {
								switch kind {
								case "update":
									return b.UpdateNode(NodeUpdateRequest{After: node(value)})
								case "delete":
									return b.DeleteNode("db:n")
								default:
									_, e := b.CreateNode(node(value))
									return e
								}
							}
							switch kind {
							case "update":
								return b.UpdateEdge(EdgeUpdateRequest{After: edge(value)})
							case "delete":
								return b.DeleteEdge("db:e")
							default:
								return b.CreateEdge(edge(value))
							}
						}
						tx, e := b.BeginTransaction()
						if e != nil {
							return e
						}
						defer tx.Rollback()
						if entity == "node" {
							switch kind {
							case "update":
								e = tx.UpdateGraphNode(NodeUpdateRequest{After: node(value), Policy: ClientNodeEdits})
							case "delete":
								e = tx.DeleteGraphNode("db:n", CascadeEdges)
							default:
								_, e = tx.CreateNode(node(value))
							}
						} else {
							switch kind {
							case "update":
								e = tx.UpdateEdge(EdgeUpdateRequest{After: edge(value)})
							case "delete":
								e = tx.DeleteEdge("db:e")
							default:
								e = tx.CreateEdge(edge(value))
							}
						}
						if e != nil {
							return e
						}
						return tx.Commit()
					}
					entered, release := make(chan struct{}), make(chan struct{})
					var releaseOnce sync.Once
					var hookCalls atomic.Int32
					var older, newer publicationOrderWorker
					restore := setCommitTailHook(func() {
						if hookCalls.Add(1) == 1 {
							close(entered)
							<-release
						}
					})
					// Register cleanup before starting either worker, including every fatal
					// assertion below. Releasing and joining precedes hook restoration/Close.
					t.Cleanup(func() { defer restore(); releaseOnce.Do(func() { close(release) }); older.join(t); newer.join(t) })
					older.start(func() error { return write(c.older, "B", mode.olderTx) })
					select {
					case <-entered:
					case <-older.done:
						t.Fatalf("older writer finished before commit-tail rendezvous: %v", older.err)
					case <-time.After(15 * time.Second):
						t.Fatal("older writer did not reach commit-tail rendezvous")
					}
					newer.start(func() error { return write(c.newer, c.final, mode.newerTx) })
					newer.join(t)
					releaseOnce.Do(func() { close(release) })
					older.join(t)
					require.NoError(t, older.err)
					require.NoError(t, newer.err)
					require.NoError(t, b.FlushWriteBehind())
					if entity == "node" {
						var physical *Node
						require.NoError(t, b.withView(func(txn *badger.Txn) error {
							item, e := txn.Get(nodeKey("db:n"))
							if e != nil {
								return e
							}
							return item.Value(func(data []byte) error { physical, e = b.decodeNodeWithEmbeddings(txn, data, "db:n"); return e })
						}))
						require.Equal(t, c.final, physical.Properties["v"], "physical committed body")
						got, e := b.GetNode("db:n")
						require.NoError(t, e)
						require.Equal(t, c.final, got.Properties["v"], "public cached body")
						require.Equal(t, []NodeID{"db:n"}, sm.PropertyIndexLookup("L", "v", c.final), "latest value, including same-value recreation, remains indexed")
						for _, old := range []string{"A", "B"} {
							if old != c.final {
								require.Empty(t, sm.PropertyIndexLookup("L", "v", old), "superseded posting %s", old)
							}
						}
						nodes, e := b.GetNodesByLabel("L")
						require.NoError(t, e)
						require.Len(t, nodes, 1)
						require.Equal(t, c.final, nodes[0].Properties["v"])
					} else {
						var physical *Edge
						require.NoError(t, b.withView(func(txn *badger.Txn) error {
							item, e := txn.Get(edgeKey("db:e"))
							if e != nil {
								return e
							}
							return item.Value(func(data []byte) error { physical, e = b.decodeEdgeBodyByID(data, "db:e"); return e })
						}))
						require.Equal(t, c.final, physical.Properties["v"], "physical committed relationship")
						got, e := b.GetEdge("db:e")
						require.NoError(t, e)
						require.Equal(t, c.final, got.Properties["v"], "public relationship cache")
						for _, read := range []func() ([]*Edge, error){func() ([]*Edge, error) { return b.GetEdgesByType("LINK") }, func() ([]*Edge, error) { return b.GetOutgoingEdges("db:a") }, func() ([]*Edge, error) { return b.GetIncomingEdges("db:b") }, func() ([]*Edge, error) { return b.GetEdgesBetween("db:a", "db:b") }} {
							edges, e := read()
							require.NoError(t, e)
							require.Len(t, edges, 1)
							require.Equal(t, EdgeID("db:e"), edges[0].ID)
							require.Equal(t, c.final, edges[0].Properties["v"])
						}
					}
					expected := []string{"update:B", "update:" + c.final}
					if c.older == "delete" {
						expected = []string{"delete:", "create:" + c.final}
					}
					eventsMu.Lock()
					observed := append([]string(nil), events...)
					eventsMu.Unlock()
					require.Equal(t, expected, observed, "callbacks follow physical commit order")
				})
			}
		}
	}
}
