package storage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The hidden-ID policy belongs to the parent transaction owner. Namespace
// counters and retirement must preserve its no-op disposition across wrappers,
// buffered publication, reopen and same-name recreation.
func TestNamespaceHiddenCreateCountsAndRetirement(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		for _, route := range []string{"direct", "bulk", "transaction"} {
			t.Run(route+map[bool]string{false: "/sync", true: "/buffered"}[buffered], func(t *testing.T) {
				dir := t.TempDir()
				open := func() *BadgerEngine {
					engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: dir, WriteBehind: buffered, WriteBehindInterval: time.Hour})
					require.NoError(t, err)
					return engine
				}
				engine := open()
				defer func() { require.NoError(t, engine.Close()) }()
				seed := func(namespace string) {
					_, err := engine.CreateNode(&Node{ID: NodeID(namespace + ":s"), Labels: []string{"Start"}})
					require.NoError(t, err)
					_, err = engine.CreateNode(&Node{ID: NodeID(namespace + ":e"), Labels: []string{"End"}})
					require.NoError(t, err)
					require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(namespace + ":r"), StartNode: NodeID(namespace + ":s"), EndNode: NodeID(namespace + ":e"), Type: "LINK", Properties: map[string]any{"value": int64(1)}}))
				}
				seed("hidden_a")
				seed("hidden_b")
				require.NoError(t, engine.FlushWriteBehind())
				require.NoError(t, engine.SetEdgeManuallySuppressed("hidden_a:r", true))
				create := func(edge *Edge) error {
					switch route {
					case "direct":
						return engine.CreateEdge(edge)
					case "bulk":
						return engine.BulkCreateEdges([]*Edge{edge})
					default:
						tx, err := engine.BeginTransaction()
						require.NoError(t, err)
						defer tx.Rollback()
						if err := tx.SetNamespace("hidden_a"); err != nil {
							return err
						}
						if err := tx.CreateEdge(edge); err != nil {
							return err
						}
						return tx.Commit()
					}
				}
				check := func(a int64) {
					for namespace, want := range map[string]int64{"hidden_a": a, "hidden_b": 1} {
						view := NewNamespacedEngine(NewTracedEngine(NewWALEngine(engine, nil)), namespace)
						count, err := view.EdgeCountByType("LINK")
						require.NoError(t, err)
						require.Equal(t, want, count, namespace+" type")
						count, err = view.EdgeCountByStartLabel("Start", "LINK")
						require.NoError(t, err)
						require.Equal(t, want, count, namespace+" start")
						count, err = view.EdgeCountByEndLabel("End", "LINK")
						require.NoError(t, err)
						require.Equal(t, want, count, namespace+" end")
					}
				}
				check(1)
				retry := &Edge{ID: "hidden_a:r", StartNode: "hidden_a:s", EndNode: "hidden_a:e", Type: "LINK", Properties: map[string]any{"value": int64(1)}}
				require.NoError(t, create(retry))
				retry.Properties["value"] = int64(2)
				require.ErrorIs(t, create(retry), ErrAlreadyExists)
				check(1)
				require.NoError(t, engine.FlushWriteBehind())
				require.NoError(t, engine.Close())
				engine = open()
				check(1)
				_, err := engine.GetEdge("hidden_a:r")
				require.ErrorIs(t, err, ErrNotFound)
				_, _, err = DeletePrefixContext(context.Background(), engine, "hidden_a:")
				require.NoError(t, err)
				check(0)
				seed("hidden_a")
				check(1)
				_, err = engine.GetEdge("hidden_a:r")
				require.NoError(t, err, "retired suppression cannot hide the new lifetime")
			})
		}
	}
}
