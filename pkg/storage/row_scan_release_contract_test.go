package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// The committed-row scans must return what release returns. The oracle in
// testdata was recorded on release with this same file
// (NOR148_ROW_SCAN_ORACLE=record), from fixtures with overlay-shadowed and
// undecodable rows.

const rowScanOracleFile = "testdata/row_scan_release_oracle.json"

type rowScanOutcome struct {
	Rows []string `json:"rows"`
	Err  string   `json:"err"`
}

func newRowScanFixture(t *testing.T, corrupt bool) *BadgerEngine {
	t.Helper()
	engine, err := NewBadgerEngineWithOptions(BadgerOptions{DataDir: t.TempDir(), WriteBehind: true, WriteBehindInterval: time.Hour})
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	for i := 1; i <= 4; i++ {
		id := NodeID(fmt.Sprintf("test:n%d", i))
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"Doc"}, Properties: map[string]any{"text": "committed"}})
		require.NoError(t, err)
	}
	for i := 1; i <= 3; i++ {
		require.NoError(t, engine.CreateEdge(&Edge{ID: EdgeID(fmt.Sprintf("test:e%d", i)), StartNode: "test:n1", EndNode: "test:n2", Type: "REL", Properties: map[string]any{"note": "committed"}}))
	}
	require.NoError(t, engine.FlushWriteBehind())
	if corrupt {
		require.NoError(t, engine.db.Update(func(txn *badger.Txn) error {
			if err := txn.Set(append([]byte{prefixNode}, "test:nbad"...), []byte{0xff}); err != nil {
				return err
			}
			return txn.Set(append([]byte{prefixEdge}, "test:ebad"...), []byte{0xff})
		}))
	}
	// Buffered writes: one replaces a committed row, one is new.
	require.NoError(t, engine.UpdateNode(&Node{ID: "test:n2", Labels: []string{"Doc"}, Properties: map[string]any{"text": "buffered"}}))
	_, err = engine.CreateNode(&Node{ID: "test:n9", Labels: []string{"Doc"}, Properties: map[string]any{"text": "buffered"}})
	require.NoError(t, err)
	require.NoError(t, engine.UpdateEdge(&Edge{ID: "test:e2", StartNode: "test:n1", EndNode: "test:n2", Type: "REL", Properties: map[string]any{"note": "buffered"}}))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "test:e9", StartNode: "test:n1", EndNode: "test:n3", Type: "REL", Properties: map[string]any{"note": "buffered"}}))
	return engine
}

func nodeRow(node *Node) string {
	text, _ := node.Properties["text"].(string)
	return string(node.ID) + "=" + text
}

func edgeRow(edge *Edge) string {
	note, _ := edge.Properties["note"].(string)
	return string(edge.ID) + "=" + note
}

func rowScanResult(rows []string, err error) rowScanOutcome {
	sort.Strings(rows)
	out := rowScanOutcome{Rows: rows}
	if out.Rows == nil {
		out.Rows = []string{}
	}
	if err != nil {
		out.Err = err.Error()
	}
	return out
}

// rowScanOutcomes runs every release-visible committed-row scan.
func rowScanOutcomes(t *testing.T, engine *BadgerEngine) map[string]rowScanOutcome {
	t.Helper()
	ctx := context.Background()
	out := map[string]rowScanOutcome{}
	nodes, err := engine.AllNodes()
	var rows []string
	for _, n := range nodes {
		rows = append(rows, nodeRow(n))
	}
	out["AllNodes"] = rowScanResult(rows, err)
	edges, err := engine.AllEdges()
	rows = nil
	for _, e := range edges {
		rows = append(rows, edgeRow(e))
	}
	out["AllEdges"] = rowScanResult(rows, err)
	rows = nil
	err = engine.StreamEdges(ctx, func(e *Edge) error { rows = append(rows, edgeRow(e)); return nil })
	out["StreamEdges"] = rowScanResult(rows, err)
	rows = nil
	err = engine.StreamNodes(ctx, func(n *Node) error { rows = append(rows, nodeRow(n)); return nil })
	out["StreamNodes"] = rowScanResult(rows, err)
	snapshot, err := engine.OpenReadSnapshot(ctx)
	require.NoError(t, err)
	defer snapshot.Close()
	rows = nil
	err = snapshot.IterateNodes(func(n *Node) bool { rows = append(rows, nodeRow(n)); return true })
	out["Snapshot.IterateNodes"] = rowScanResult(rows, err)
	rows = nil
	err = snapshot.StreamEdges(ctx, func(e *Edge) error { rows = append(rows, edgeRow(e)); return nil })
	out["Snapshot.StreamEdges"] = rowScanResult(rows, err)
	return out
}

func TestRowScanReleaseContract(t *testing.T) {
	got := map[string]map[string]rowScanOutcome{}
	for _, fixture := range []string{"clean", "corrupt"} {
		got[fixture] = rowScanOutcomes(t, newRowScanFixture(t, fixture == "corrupt"))
	}
	if os.Getenv("NOR148_ROW_SCAN_ORACLE") == "record" {
		data, err := json.MarshalIndent(got, "", " ")
		require.NoError(t, err)
		t.Logf("NOR148_ROW_SCAN_ORACLE_BEGIN\n%s\nNOR148_ROW_SCAN_ORACLE_END", data)
		return
	}
	data, err := os.ReadFile(rowScanOracleFile)
	require.NoError(t, err)
	var want map[string]map[string]rowScanOutcome
	require.NoError(t, json.Unmarshal(data, &want))
	require.Equal(t, want, got)
}

// The visible snapshot iterators read AllNodes/AllEdges at the captured
// point: same rows, and an undecodable row fails them as it fails AllNodes and
// AllEdges.
func TestRowScanVisibleSnapshotMatchesAllReads(t *testing.T) {
	type visibleSnapshot interface {
		IterateVisibleNodes(context.Context, func(*Node) bool) error
		IterateAllEdges(context.Context, func(*Edge) error) error
	}
	for _, fixture := range []string{"clean", "corrupt"} {
		t.Run(fixture, func(t *testing.T) {
			engine := newRowScanFixture(t, fixture == "corrupt")
			ctx := context.Background()
			snapshot, err := engine.OpenReadSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Close()
			visible, ok := snapshot.(visibleSnapshot)
			if !ok {
				t.Skip("release snapshot has no visible iterators")
			}
			nodes, nodesErr := engine.AllNodes()
			var want, rows []string
			for _, n := range nodes {
				want = append(want, nodeRow(n))
			}
			err = visible.IterateVisibleNodes(ctx, func(n *Node) bool { rows = append(rows, nodeRow(n)); return true })
			require.Equal(t, nodesErr != nil, err != nil, "IterateVisibleNodes error: %v, AllNodes error: %v", err, nodesErr)
			if err == nil {
				require.Equal(t, rowScanResult(want, nil).Rows, rowScanResult(rows, nil).Rows)
			}
			edges, edgesErr := engine.AllEdges()
			want, rows = nil, nil
			for _, e := range edges {
				want = append(want, edgeRow(e))
			}
			err = visible.IterateAllEdges(ctx, func(e *Edge) error { rows = append(rows, edgeRow(e)); return nil })
			require.Equal(t, edgesErr != nil, err != nil, "IterateAllEdges error: %v, AllEdges error: %v", err, edgesErr)
			if err == nil {
				require.Equal(t, rowScanResult(want, nil).Rows, rowScanResult(rows, nil).Rows)
			}
		})
	}
}
