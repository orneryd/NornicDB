package storage

// Canonical label-scan convergence (#1682, R17): a StreamNodesWithOptions scan
// that carries a property filter takes the same scoped label owner as an
// unfiltered one, so it decodes only the matching label's bodies in scope and
// settles the filter in that owner's scan decoder before materializing a
// candidate. The rows, the errors and the write-behind overlay stay the ones
// the generic filtered scan returns.

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// filteredLabelFixture creates Common/Record nodes in the "test" database plus
// the two decoy populations the label index must exclude: bodies of another
// label in the same database, and a matching-label node in another database.
// The physical node cache is cleared so no scan answers from it.
func filteredLabelFixture(t *testing.T, count, decoys int) (*BadgerEngine, []NodeID) {
	t.Helper()
	engine := newTestEngine(t)
	created := make([]NodeID, 0, count)
	for i := 0; i < count; i++ {
		id := NodeID(fmt.Sprintf("test:r%04d", i))
		_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Common", "Record"}, Properties: map[string]any{
			"keep": "yes",
			"i":    int64(i),
			"big":  strings.Repeat("b", 256),
		}})
		require.NoError(t, err)
		created = append(created, id)
	}
	for i := 0; i < decoys; i++ {
		_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("test:x%02d", i)), Labels: []string{"Decoy"}, Properties: map[string]any{
			"keep": "yes",
			"big":  strings.Repeat("d", 256),
		}})
		require.NoError(t, err)
	}
	_, err := engine.CreateNode(t.Context(), &Node{ID: "elsewhere:r0", Labels: []string{"Common", "Record"}, Properties: map[string]any{
		"keep": "yes",
	}})
	require.NoError(t, err)
	engine.nodeCacheMu.Lock()
	clear(engine.nodeCache)
	engine.nodeCacheMu.Unlock()
	return engine, created
}

// scanStreamNodes runs one StreamNodesWithOptions scan over the engine's
// current view and returns the rows it visited.
func scanStreamNodes(t *testing.T, engine *BadgerEngine, opts StreamNodesOptions) []*Node {
	t.Helper()
	var nodes []*Node
	require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
		nodes = append(nodes, node)
		return nil
	}))
	return nodes
}

// nodeRow is the caller-visible row of a scanned node: a node that carries the
// engine's internal provenance handle and one that does not are the same row.
// Exported runtime metadata is compared by value as part of that contract.
type nodeRow struct {
	RuntimeMetadata NodeRuntimeMetadata
	ID              NodeID
	Labels          []string
	Properties      map[string]interface{}
	CreatedAt       time.Time
	UpdatedAt       time.Time
	NamedEmbeddings map[string][]float32
	ChunkEmbeddings [][]float32
	EmbedMeta       map[string]interface{}
}

func rowOf(node *Node) nodeRow {
	return nodeRow{
		RuntimeMetadata: node.RuntimeMetadata(),
		ID:              node.ID,
		Labels:          node.Labels,
		Properties:      node.Properties,
		CreatedAt:       node.CreatedAt,
		UpdatedAt:       node.UpdatedAt,
		NamedEmbeddings: node.NamedEmbeddings,
		ChunkEmbeddings: node.ChunkEmbeddings,
		EmbedMeta:       node.EmbedMeta,
	}
}

// A filtered whole-node label scan decodes the matching label's bodies in scope
// and nothing else: the other label's bodies and the other database's body
// never reach the property filter, and a kept row is the whole node the
// unfiltered label scan returns.
func TestStreamNodesWithOptionsFilteredLabelScanReadsOnlyMatchingLabelBodies(t *testing.T) {
	const count = labelScanPointLookups + 20
	engine, created := filteredLabelFixture(t, count, 2*count+2)

	whole := scanStreamNodes(t, engine, StreamNodesOptions{Label: "Common", Prefix: "test:", QueryNodes: true})
	require.Len(t, whole, count, "the unfiltered label scan is this scan's population")
	for i, node := range whole {
		require.Equal(t, created[i], node.ID)
	}

	filterCalls := 0
	keeps := func(properties map[string]interface{}) bool {
		filterCalls++
		return properties["keep"] == "yes"
	}
	got := scanStreamNodes(t, engine, StreamNodesOptions{
		Label:                    "Common",
		Prefix:                   "test:",
		QueryNodes:               true,
		PropertyFilter:           keeps,
		PropertyFilterProjection: []string{"keep"},
	})
	require.Len(t, got, count)
	require.Equal(t, count, filterCalls,
		"only the matching label's bodies in scope reach the property filter")
	for i := range got {
		require.Equal(t, rowOf(whole[i]), rowOf(got[i]),
			"a kept row is the whole node the unfiltered scan returns")
	}
	require.Equal(t, "yes", got[0].Properties["keep"])
	require.Equal(t, int64(0), got[0].Properties["i"])
	require.Equal(t, strings.Repeat("b", 256), got[0].Properties["big"])
	require.Len(t, got[0].Labels, 2)

	rejected := 0
	none := scanStreamNodes(t, engine, StreamNodesOptions{
		Label:      "Common",
		Prefix:     "test:",
		QueryNodes: true,
		PropertyFilter: func(map[string]interface{}) bool {
			rejected++
			return false
		},
		PropertyFilterProjection: []string{"keep"},
	})
	require.Empty(t, none, "a rejected candidate is never materialized")
	require.Equal(t, count, rejected,
		"every matching-label candidate is filtered once, and no other body is read")

	filterCalls = 0
	everywhere := scanStreamNodes(t, engine, StreamNodesOptions{
		Label:                    "Common",
		QueryNodes:               true,
		PropertyFilter:           keeps,
		PropertyFilterProjection: []string{"keep"},
	})
	require.Len(t, everywhere, count+1, "a scan without a prefix reaches every database's matching label")
	require.Equal(t, count+1, filterCalls)
	require.Equal(t, NodeID("elsewhere:r0"), everywhere[count].ID)
}

// The filtered owner keeps the rows it inherits from the label index: a node
// with no stored properties, a node matched by a label that is not its first,
// and a node whose filter property is absent are all complete rows.
func TestStreamNodesWithOptionsFilteredLabelScanKeepsEmptyAndMultilabelRows(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprintf("dense_%t", dense), func(t *testing.T) {
			engine := newTestEngine(t)
			empty := &Node{ID: "test:e0", Labels: []string{"Record", "Empty"}}
			second := &Node{ID: "test:e1", Labels: []string{"Empty", "Record"}, Properties: map[string]any{"keep": "yes"}}
			bare := &Node{ID: "test:e2", Labels: []string{"Record"}, Properties: map[string]any{}}
			dropped := &Node{ID: "test:e3", Labels: []string{"Record"}, Properties: map[string]any{"keep": "no", "big": "x"}}
			decoy := &Node{ID: "test:e4", Labels: []string{"Other"}, Properties: map[string]any{"keep": "yes"}}
			for _, node := range []*Node{empty, second, bare, dropped, decoy} {
				_, err := engine.CreateNode(t.Context(), node)
				require.NoError(t, err)
			}

			// Both physical strategies must preserve the same complete rows.
			// Only the sparse strategy promises candidate-only filter work.
			decoys := 8
			if dense {
				decoys = 0
			}
			for i := 0; i < decoys; i++ {
				_, err := engine.CreateNode(t.Context(), &Node{ID: NodeID(fmt.Sprintf("test:x%d", i)), Labels: []string{"Other"}, Properties: map[string]any{"keep": "yes"}})
				require.NoError(t, err)
			}

			whole := scanStreamNodes(t, engine, StreamNodesOptions{Label: "Record", Prefix: "test:", QueryNodes: true})
			require.Len(t, whole, 4)

			calls := 0
			got := scanStreamNodes(t, engine, StreamNodesOptions{
				Label:      "Record",
				Prefix:     "test:",
				QueryNodes: true,
				PropertyFilter: func(properties map[string]interface{}) bool {
					calls++
					return properties["keep"] != "no"
				},
				PropertyFilterProjection: []string{"keep"},
			})
			require.Len(t, got, 3)
			if !dense {
				require.Equal(t, 4, calls, "the sparse index walk excludes other labels before property filtering")
			}
			require.Equal(t, rowOf(whole[0]), rowOf(got[0]), "a node with no stored properties is a complete row")
			require.Empty(t, got[0].Properties)
			require.Len(t, got[0].Labels, 2)
			require.Equal(t, rowOf(whole[1]), rowOf(got[1]), "a node matched by its second label is a complete row")
			require.Equal(t, "yes", got[1].Properties["keep"])
			require.Equal(t, rowOf(whole[2]), rowOf(got[2]))
			require.Empty(t, got[2].Properties)
		})
	}

}

// A candidate body the scan decoder cannot decode fails the filtered label scan
// with the generic scan's wrapping, whether the scan's filter keeps or rejects,
// in the early and late candidate positions.
func TestStreamNodesWithOptionsFilteredLabelScanReportsBodyErrors(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprintf("dense_%t", dense), func(t *testing.T) {
			for _, phase := range []struct {
				name  string
				index func(count int) int
			}{
				{"early", func(int) int { return 0 }},
				{"late", func(count int) int { return count - 1 }},
			} {
				for _, filter := range []struct {
					name string
					keep func(map[string]interface{}) bool
				}{
					{"kept", func(map[string]interface{}) bool { return true }},
					{"rejected", func(map[string]interface{}) bool { return false }},
				} {
					t.Run(phase.name+"/"+filter.name, func(t *testing.T) {
						const count = labelScanPointLookups + 10
						decoys := 2*count + 2
						if dense {
							decoys = 0
						}
						engine, created := filteredLabelFixture(t, count, decoys)
						corrupt := created[phase.index(count)]
						require.NoError(t, engine.withUpdate(context.Background(), updateRequest{}, func(txn *badger.Txn) error {
							return txn.Set(nodeKey(corrupt), []byte{0xff})
						}))
						err := engine.StreamNodesWithOptions(context.Background(), StreamNodesOptions{
							Label:                    "Common",
							Prefix:                   "test:",
							QueryNodes:               true,
							PropertyFilter:           filter.keep,
							PropertyFilterProjection: []string{"keep"},
						}, func(*Node) error { return nil })
						require.Error(t, err, "a record the scan cannot decode fails the scan")
						require.Contains(t, err.Error(), fmt.Sprintf("scanning node %q:", corrupt),
							"a filtered decode failure keeps the generic scan's error wrapping")
					})
				}
			}
		})
	}

}

// The filtered owner keeps the stream's stop, cancellation and visitor-error
// contract: ErrIterationStopped ends the scan, a canceled context ends it even
// between rows, and a visitor error is returned to the caller.
func TestStreamNodesWithOptionsFilteredLabelScanStopsAndCancels(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprintf("dense_%t", dense), func(t *testing.T) {
			const count = labelScanPointLookups + 20
			decoys := 2*count + 2
			if dense {
				decoys = 0
			}
			engine, _ := filteredLabelFixture(t, count, decoys)
			opts := StreamNodesOptions{
				Label:                    "Common",
				Prefix:                   "test:",
				QueryNodes:               true,
				PropertyFilter:           func(map[string]interface{}) bool { return true },
				PropertyFilterProjection: []string{"keep"},
			}

			for _, stopAt := range []int{5, count - 1} {
				visited := 0
				require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(*Node) error {
					visited++
					if visited == stopAt {
						return ErrIterationStopped
					}
					return nil
				}), "ErrIterationStopped ends a filtered label scan")
				require.Equal(t, stopAt, visited)
			}

			boom := errors.New("visitor failed")
			visited := 0
			require.ErrorIs(t, engine.StreamNodesWithOptions(context.Background(), opts, func(*Node) error {
				visited++
				if visited == 3 {
					return boom
				}
				return nil
			}), boom)
			require.Equal(t, 3, visited)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			visited = 0
			err := engine.StreamNodesWithOptions(ctx, opts, func(*Node) error {
				visited++
				if visited == 3 {
					cancel()
				}
				return nil
			})
			require.ErrorIs(t, err, context.Canceled)
			require.Equal(t, 3, visited, "the canceled scan stops at the next row")

			canceled, cancelNow := context.WithCancel(context.Background())
			cancelNow()
			require.ErrorIs(t, engine.StreamNodesWithOptions(canceled, opts, func(*Node) error {
				t.Fatal("a canceled scan visits no row")
				return nil
			}), context.Canceled)
		})
	}

}

// The filtered label scan reports the write-behind overlay exactly as the
// unfiltered one does: a pending create and a pending rename are visited, a
// pending removal hides the committed row, and the flushed scan returns the
// same rows. Pending rows are not filtered by the scan's option, so the caller
// keeps doing its residual check.
func TestStreamNodesWithOptionsFilteredLabelScanSeesWriteBehindOverlay(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	keeps := func(properties map[string]interface{}) bool { return properties["keep"] == "yes" }
	opts := StreamNodesOptions{
		Label:                    "Record",
		Prefix:                   "test:",
		QueryNodes:               true,
		PropertyFilter:           keeps,
		PropertyFilterProjection: []string{"keep"},
	}
	rows := func() map[NodeID]map[string]interface{} {
		found := map[NodeID]map[string]interface{}{}
		require.NoError(t, engine.StreamNodesWithOptions(context.Background(), opts, func(node *Node) error {
			if !keeps(node.Properties) {
				return nil // the caller's residual check on a row the option may not filter
			}
			require.NotContains(t, found, node.ID)
			found[node.ID] = node.Properties
			return nil
		}))
		return found
	}

	bufferedCreate(t, engine, &Node{ID: "test:keep", Labels: []string{"Record"}, Properties: map[string]any{"keep": "yes", "v": int64(1)}})
	bufferedCreate(t, engine, &Node{ID: "test:renamed", Labels: []string{"Record"}, Properties: map[string]any{"keep": "no", "v": int64(2)}})
	bufferedCreate(t, engine, &Node{ID: "test:removed", Labels: []string{"Record"}, Properties: map[string]any{"keep": "yes", "v": int64(3)}})
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	require.Zero(t, engine.writeBehind.PendingOps())

	bufferedWrite(t, engine, func(tx *BadgerTransaction) error {
		if _, err := tx.CreateNode(context.Background(), &Node{ID: "test:created", Labels: []string{"Record"}, Properties: map[string]any{"keep": "yes", "v": int64(4)}}); err != nil {
			return err
		}
		if err := tx.UpdateGraphNode(context.Background(), NodeUpdateRequest{
			After:  &Node{ID: "test:renamed", Labels: []string{"Record"}, Properties: map[string]any{"keep": "yes", "v": int64(2)}},
			Policy: ClientNodeEdits,
		}); err != nil {
			return err
		}
		return tx.DeleteGraphNode(context.Background(), "test:removed", CascadeEdges)
	})
	require.Greater(t, engine.writeBehind.PendingOps(), 0, "the fixture keeps every write buffered until it flushes")

	pending := rows()
	require.Len(t, pending, 3)
	require.Equal(t, "yes", pending["test:keep"]["keep"])
	require.Equal(t, int64(2), pending["test:renamed"]["v"], "the pending rename is visited")
	require.Equal(t, int64(4), pending["test:created"]["v"], "the pending create is visited")
	require.NotContains(t, pending, NodeID("test:removed"), "the pending removal hides the committed row")

	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	require.Zero(t, engine.writeBehind.PendingOps())
	require.Equal(t, pending, rows(), "the flushed scan returns the rows the overlay reported")
}

// CALL-containing reads retain full-vector hydration because a called
// procedure may write. Their filter still uses the same label scan owner.
func TestStreamNodesWithOptionsFilteredLabelScanHydrationModes(t *testing.T) {
	for _, dense := range []bool{false, true} {
		t.Run(fmt.Sprintf("dense_%t", dense), func(t *testing.T) {
			decoys := 4
			if dense {
				decoys = 0
			}
			engine, _ := filteredLabelFixture(t, 1, decoys)
			id := NodeID("test:embedded")
			_, err := engine.CreateNode(t.Context(), &Node{ID: id, Labels: []string{"Common"}, Properties: map[string]any{"keep": "yes"}, ChunkEmbeddings: [][]float32{{1, 2}}, NamedEmbeddings: map[string][]float32{"title": {3, 4}}})
			require.NoError(t, err)
			for _, mode := range []struct {
				name           string
				query, vectors bool
			}{
				{"query_metadata", true, false}, {"full_vectors", false, true}, {"vectors_override_metadata", true, true},
			} {
				t.Run(mode.name, func(t *testing.T) {
					calls := 0
					var got *Node
					err := engine.StreamNodesWithOptions(t.Context(), StreamNodesOptions{
						Label: "Common", Prefix: "test:", QueryNodes: mode.query, WithEmbeddings: mode.vectors,
						PropertyFilter:           func(properties map[string]interface{}) bool { calls++; return properties["keep"] == "yes" },
						PropertyFilterProjection: []string{"keep"},
					}, func(node *Node) error {
						if node.ID == id {
							got = node
						}
						return nil
					})
					require.NoError(t, err)
					require.Equal(t, 2, calls, "only the selected label and namespace are decoded in every hydration mode")
					require.NotNil(t, got)
					var want *Node
					if mode.vectors {
						want, err = engine.GetNode(t.Context(), id)
					} else {
						want, err = engine.GetQueryNode(t.Context(), id)
					}
					require.NoError(t, err)
					require.Equal(t, rowOf(want), rowOf(got), "hydration matches the canonical point read")
				})
			}
		})
	}

}

// A dense filtered label uses the shared sequential scanner, and an early
// stop must also stop before the acknowledged write-behind overlay is visited.
func TestStreamNodesWithOptionsDenseFilteredLabelStopsBeforeOverlay(t *testing.T) {
	// The manual drain owner keeps the acknowledged create pending in the
	// overlay for the whole scan; the explicit flush is a non-cancellable waiter
	// (the fixture stopped the worker), so it still lands the committed rows,
	// and no background flush can retire the pending one before the early stop
	// reaches it.
	engine := newWriteBehindTestEngine(t, time.Hour, writeBehindTestOptions{manualDrain: true})
	for _, name := range []string{"a", "b", "c", "other"} {
		label := "Common"
		if name == "other" {
			label = "Other"
		}
		bufferedCreate(t, engine, &Node{ID: NodeID("test:" + name), Labels: []string{label}, Properties: map[string]any{"keep": "yes"}})
	}
	require.NoError(t, engine.FlushWriteBehind(context.Background()))
	bufferedCreate(t, engine, &Node{ID: "test:pending", Labels: []string{"Common"}, Properties: map[string]any{"keep": "yes"}})
	require.Greater(t, engine.writeBehind.PendingOps(), 0)
	opts := StreamNodesOptions{Label: "Common", Prefix: "test:", QueryNodes: true, PropertyFilter: func(properties map[string]interface{}) bool { return properties["keep"] == "yes" }, PropertyFilterProjection: []string{"keep"}}
	rows := scanStreamNodes(t, engine, opts)
	require.Len(t, rows, 4)
	for _, node := range rows {
		require.True(t, hasLabel(node.Labels, "Common"))
		require.Equal(t, "yes", node.Properties["keep"])
	}
	visited := 0
	require.NoError(t, engine.StreamNodesWithOptions(t.Context(), opts, func(*Node) error { visited++; return ErrIterationStopped }))
	require.Equal(t, 1, visited, "the physical stop must prevent overlay visitation")
}

// The density decision is a cost choice, not a visibility choice. In
// particular, a deindexed row is hidden even when its scorer is absent.
func TestStreamNodesWithOptionsFilteredLabelVisibilityMatchesIndexOwner(t *testing.T) {
	for _, decoys := range []int{0, 12} {
		t.Run(fmt.Sprintf("decoys_%d", decoys), func(t *testing.T) {
			engine, created := filteredLabelFixture(t, 4, decoys)
			engine.SetDecayEnabled(true)
			tombstoneLabelIndexEntry(t, engine, "Common", created[1])
			for _, reveal := range []bool{false, true} {
				engine.SetRevealAll(reveal)
				want := streamLabelNodes(t, engine, "test:", "Common", nil)
				wantIDs := make([]NodeID, len(want))
				for i, node := range want {
					wantIDs[i] = node.ID
				}
				got := scanStreamNodes(t, engine, StreamNodesOptions{
					Label: "Common", Prefix: "test:", QueryNodes: true, ApplyDecayFilter: true,
					PropertyFilter:           func(properties map[string]interface{}) bool { return properties["keep"] == "yes" },
					PropertyFilterProjection: []string{"keep"},
				})
				gotIDs := make([]NodeID, len(got))
				for i, node := range got {
					gotIDs[i] = node.ID
				}
				require.Equal(t, wantIDs, gotIDs, "density must preserve canonical label visibility; reveal=%t", reveal)
				if !reveal {
					require.NotContains(t, gotIDs, created[1])
				}
			}
		})
	}
}
