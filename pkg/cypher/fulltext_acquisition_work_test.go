// Author: Codex implementation lane, 2026-10-27 (NOR-148 S01-S27 work contract).
package cypher

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/search"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This carrier uses release04 APIs. The observer decorates actual physical
// readers and deliberately declines promoted snapshot certification: it changes
// returned rows by adding a formatting meter. No product observer/hook is added.
type fulltextWorkProbe struct {
	*storage.NamespacedEngine
	nodes, edges, stops, formatted atomic.Int64
	readErr                        error
}

type fulltextWorkValue struct{ formatted *atomic.Int64 }

func (v fulltextWorkValue) String() string                          { v.formatted.Add(1); return "format meter" }
func (p *fulltextWorkProbe) OwnsSnapshotReader(storage.Engine) bool { return false }
func (p *fulltextWorkProbe) AllNodes() ([]*storage.Node, error) {
	rows, err := p.NamespacedEngine.AllNodes()
	p.nodes.Add(int64(len(rows)))
	if p.readErr != nil {
		return nil, p.readErr
	}
	for _, row := range rows {
		row.Properties["work_meter"] = fulltextWorkValue{&p.formatted}
	}
	return rows, err
}
func (p *fulltextWorkProbe) AllEdges() ([]*storage.Edge, error) {
	rows, err := p.NamespacedEngine.AllEdges()
	p.edges.Add(int64(len(rows)))
	if p.readErr != nil {
		return nil, p.readErr
	}
	for _, row := range rows {
		row.Properties["work_meter"] = fulltextWorkValue{&p.formatted}
	}
	return rows, err
}
func (p *fulltextWorkProbe) StreamEdges(ctx context.Context, visit func(*storage.Edge) error) error {
	return p.NamespacedEngine.StreamEdges(ctx, func(edge *storage.Edge) error {
		p.edges.Add(1)
		edge.Properties["work_meter"] = fulltextWorkValue{&p.formatted}
		err := visit(edge)
		if err == storage.ErrIterationStopped {
			p.stops.Add(1)
		}
		return err
	})
}
func (p *fulltextWorkProbe) reset() {
	p.nodes.Store(0)
	p.edges.Store(0)
	p.stops.Store(0)
	p.formatted.Store(0)
}

func fulltextWorkFixture(tb testing.TB, count int, buffered bool) (*storage.BadgerEngine, *storage.NamespacedEngine) {
	tb.Helper()
	base, err := storage.NewBadgerEngineWithOptions(storage.BadgerOptions{InMemory: true, WriteBehind: buffered, WriteBehindInterval: time.Hour})
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, base.Close()) })
	store := storage.NewNamespacedEngine(base, "acquisition")
	nodes := make([]*storage.Node, count)
	for i := range nodes {
		label, text := "Source", strings.Repeat("hotel routine services ", 16)
		if i == 0 {
			label = "Other"
		}
		if i == 1 {
			text = ""
		}
		nodes[i] = &storage.Node{ID: storage.NodeID(fmt.Sprintf("n%05d", i)), Labels: []string{label}, Properties: map[string]any{"id": fmt.Sprintf("n%05d", i), "text": text}}
	}
	require.NoError(tb, store.BulkCreateNodes(nodes))
	for i := 0; i < count; i++ {
		kind, text := "LINK", nodes[i].Properties["text"]
		if i == 0 {
			kind = "OTHER"
		}
		require.NoError(tb, store.CreateEdge(&storage.Edge{ID: storage.EdgeID(fmt.Sprintf("e%05d", i)), StartNode: "n00000", EndNode: "n00001", Type: kind, Properties: map[string]any{"id": fmt.Sprintf("e%05d", i), "text": text}}))
	}
	require.NoError(tb, base.FlushWriteBehind())
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
	for _, ddl := range []string{
		"CREATE FULLTEXT INDEX work_nodes FOR (n:Source) ON EACH [n.text]",
		"CREATE FULLTEXT INDEX work_edges FOR ()-[r:LINK]-() ON EACH [r.text]",
	} {
		_, err := exec.Execute(context.Background(), ddl, nil)
		require.NoError(tb, err)
	}
	require.NoError(tb, base.FlushWriteBehind())
	return base, store
}

func fulltextWorkQuery(relationships bool) string {
	if relationships {
		return "CALL db.index.fulltext.queryRelationships('work_edges',$q,$opts) YIELD relationship,score RETURN relationship.id,score"
	}
	return "CALL db.index.fulltext.queryNodes('work_nodes',$q,$opts) YIELD node,score RETURN node.id,score"
}

func TestFulltextAcquisitionWork(t *testing.T) {
	const count = 32
	for _, buffered := range []bool{false, true} {
		for _, shape := range []string{"native_namespace", "wal_forward", "traced_fallback", "transient"} {
			t.Run(fmt.Sprintf("%s/buffered=%t", shape, buffered), func(t *testing.T) {
				_, store := fulltextWorkFixture(t, count, buffered)
				probe := &fulltextWorkProbe{NamespacedEngine: store}
				var engine storage.Engine = probe
				if shape == "traced_fallback" {
					engine = storage.NewTracedEngine(probe)
				}
				if shape == "wal_forward" {
					wal, err := storage.NewWAL(t.TempDir(), nil)
					require.NoError(t, err)
					wrapped := storage.NewWALEngine(probe, wal)
					t.Cleanup(func() { require.NoError(t, wrapped.Close()) })
					engine = wrapped
				}
				exec := NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0})
				if shape == "transient" {
					exec.SetSearchServiceResolver("acquisition", func(string) *search.Service { return nil })
				}
				for _, relationships := range []bool{false, true} {
					for _, q := range []string{"*", "*:*", "text:*"} {
						for _, skip := range []int64{0, 1} {
							for call := 0; call < 3; call++ {
								probe.reset()
								got, err := exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": q, "opts": map[string]any{"limit": int64(1), "skip": skip}})
								formatted := probe.formatted.Load()
								t.Logf("WORK relationships=%t mode=%q skip=%d call=%d nodes=%d edges=%d stops=%d formats=%d", relationships, q, skip, call, probe.nodes.Load(), probe.edges.Load(), probe.stops.Load(), formatted)
								require.NoError(t, err)
								require.Len(t, got.Rows, 1)
								prefix := "n"
								if relationships {
									prefix = "e"
								}
								require.Equal(t, fmt.Sprintf("%s%05d", prefix, 2+skip), got.Rows[0][0])
								assert.Zero(t, formatted, "unranked acquisition built scoring fields")
								if relationships {
									want := int64(3) + skip
									if shape == "traced_fallback" {
										want = count
									}
									assert.Equal(t, want, probe.edges.Load(), "reader crossed the release prefix on repeated call %d", call)
									if shape != "traced_fallback" {
										assert.Equal(t, int64(1), probe.stops.Load(), "stop did not reach native reader")
									}
								} else {
									assert.Equal(t, int64(count), probe.nodes.Load(), "node materialization was incorrectly truncated")
								}
							}
						}
					}
					probe.reset()
					complete, err := exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": "text:*", "opts": map[string]any{}})
					require.NoError(t, err)
					require.Len(t, complete.Rows, count-2, "ordered prefix was admitted as complete")
					assert.Zero(t, probe.formatted.Load(), "complete unranked traversal built scoring fields")
					assert.Equal(t, int64(count), probe.nodes.Load()+probe.edges.Load())
					// A prefix must not be admitted as a complete corpus. Also retain
					// the complete-statistics demand and original formatting phase.
					probe.reset()
					got, err := exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": "hotel", "opts": map[string]any{}})
					require.NoError(t, err)
					require.Len(t, got.Rows, count-2)
					require.Equal(t, int64(count-1), probe.formatted.Load())
					for _, q := range []string{"(unbalanced", "   "} {
						probe.reset()
						_, err := exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": q, "opts": map[string]any{}})
						if q == "(unbalanced" {
							require.Error(t, err)
						} else {
							require.NoError(t, err)
						}
						assert.Zero(t, probe.formatted.Load(), "construction ran before parser gate")
						if relationships {
							require.Equal(t, int64(count), probe.edges.Load())
						} else {
							require.Zero(t, probe.nodes.Load())
						}
					}
					probe.readErr = errors.New("work acquisition failed")
					probe.reset()
					_, err = exec.Execute(context.Background(), fulltextWorkQuery(relationships), map[string]any{"q": "hotel", "opts": map[string]any{}})
					require.ErrorContains(t, err, "work acquisition failed")
					assert.Zero(t, probe.formatted.Load(), "construction ran before read completed")
					probe.readErr = nil
				}
			})
		}
	}
}

// Same release-API physical fixture in both binaries, explicitly uploaded by
// --benchmark-fixture. Query result caching is disabled and rows are consumed.
func BenchmarkNOR148FulltextRelationshipPrefix(b *testing.B) {
	for _, buffered := range []bool{false, true} {
		b.Run(fmt.Sprintf("buffered=%t", buffered), func(b *testing.B) {
			_, store := fulltextWorkFixture(b, 2000, buffered)
			exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 0})
			for _, q := range []string{"*", "text:*"} {
				name := "wildcard"
				if q != "*" {
					name = "presence"
				}
				b.Run(name, func(b *testing.B) {
					params := map[string]any{"q": q, "opts": map[string]any{"limit": int64(1)}}
					result, err := exec.Execute(context.Background(), fulltextWorkQuery(true), params)
					require.NoError(b, err)
					require.Len(b, result.Rows, 1)
					b.ReportAllocs()
					b.ResetTimer()
					for call := 0; call < b.N; call++ {
						result, err = exec.Execute(context.Background(), fulltextWorkQuery(true), params)
						if err != nil || len(result.Rows) != 1 {
							b.Fatalf("prefix result: %v", err)
						}
					}
				})
			}
		})
	}
}
