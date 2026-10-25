package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// Generated proof for the label-index reader contract across drops,
// recreations and rebuilds. Every public reader is run once undisturbed (the
// oracle), then again paused at each lifecycle seam on its path while a
// transition runs, and the two answers are compared:
//
//   - A reader admitted while the index is ready answers ID-level questions
//     from its snapshot's index exactly: every ID once, in the same order.
//   - Body-level answers, and every answer of a reader admitted while the
//     index is building, may lose a dropped node: DROP removes primary bodies
//     physically. Nothing else may differ. No row is duplicated, every
//     surviving row is present, and no row carries the recreated node's
//     properties or an older version's.

const (
	matrixLabel  = "Target"
	matrixOldGen = "old"
	matrixNewGen = "new"
)

// The dropped namespace holds a:1, which is recreated, and a:2, which is not.
var matrixFixture = []string{"keep:first", "a:1", "keep:second", "a:2", "keep:last"}

type matrixRow struct {
	id  string
	gen any
}

func newMatrixEngine(t *testing.T, opts BadgerOptions) *BadgerEngine {
	t.Helper()
	opts.InMemory = true
	opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	b, err := NewBadgerEngineWithOptions(opts)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close() })
	return b
}

func createMatrixNodes(t *testing.T, b *BadgerEngine, labels []string, gen string) {
	t.Helper()
	for _, id := range matrixFixture {
		_, err := b.CreateNode(&Node{ID: NodeID(id), Labels: labels, Properties: map[string]any{"gen": gen}})
		require.NoError(t, err)
	}
}

// markMatrixIndexBuilding publishes a building marker over the complete index,
// as an interrupted rebuild leaves it, and returns its generation.
func markMatrixIndexBuilding(t *testing.T, b *BadgerEngine) uint64 {
	t.Helper()
	generation := b.labelIndex.markerGeneration.Load() + 1
	_, err := b.writeLabelIndexBuildingMarker(generation)
	require.NoError(t, err)
	return generation
}

func matrixDropRecreate(t *testing.T, b *BadgerEngine) {
	t.Helper()
	_, _, err := b.DeleteByPrefix("a:")
	require.NoError(t, err)
	_, err = b.CreateNode(&Node{ID: "a:1", Labels: []string{matrixLabel}, Properties: map[string]any{"gen": matrixNewGen}})
	require.NoError(t, err)
}

func matrixRebuild(t *testing.T, b *BadgerEngine) {
	t.Helper()
	// A rebuild waits only for readers older than the previous full
	// rebuild's marker (the one at open). The paused reader began after it,
	// so the rebuild runs to completion while that reader holds its snapshot.
	_, err := b.rebuildLabelIndex(context.Background())
	require.NoError(t, err)
}

type matrixTransition struct {
	name string
	run  func(t *testing.T, b *BadgerEngine, buildingGen uint64)
}

func matrixTransitions(building bool) []matrixTransition {
	dropRecreate := matrixTransition{"drop-recreate", func(t *testing.T, b *BadgerEngine, _ uint64) {
		matrixDropRecreate(t, b)
	}}
	twice := matrixTransition{"drop-recreate-twice", func(t *testing.T, b *BadgerEngine, _ uint64) {
		matrixDropRecreate(t, b)
		matrixDropRecreate(t, b)
	}}
	if building {
		return []matrixTransition{dropRecreate, twice,
			{"publish-ready-drop-recreate", func(t *testing.T, b *BadgerEngine, generation uint64) {
				require.NoError(t, b.completeLabelIndexGeneration(generation))
				matrixDropRecreate(t, b)
			}},
		}
	}
	return []matrixTransition{dropRecreate, twice,
		{"drop-recreate-rebuild", func(t *testing.T, b *BadgerEngine, _ uint64) {
			matrixDropRecreate(t, b)
			matrixRebuild(t, b)
		}},
		{"rebuild-drop-recreate", func(t *testing.T, b *BadgerEngine, _ uint64) {
			matrixRebuild(t, b)
			matrixDropRecreate(t, b)
		}},
	}
}

func nodeRows(nodes []*Node) []matrixRow {
	rows := make([]matrixRow, 0, len(nodes))
	for _, n := range nodes {
		if n == nil {
			continue
		}
		rows = append(rows, matrixRow{id: string(n.ID), gen: n.Properties["gen"]})
	}
	return rows
}

func sortedRows(rows []matrixRow) []matrixRow {
	out := append([]matrixRow(nil), rows...)
	sort.Slice(out, func(i, j int) bool { return out[i].id < out[j].id })
	return out
}

func matrixDropped(id string) bool { return len(id) > 2 && id[:2] == "a:" }

// requireMatrixRows compares a disturbed answer with the oracle. exact demands
// the oracle itself; otherwise dropped rows may be missing. ordered keeps the
// oracle's order, and limit (when > 0) is the number of rows an early-stopped
// read asked for.
func requireMatrixRows(t *testing.T, oracle, got []matrixRow, exact, ordered bool, limit int) {
	t.Helper()
	if exact {
		if !ordered {
			oracle, got = sortedRows(oracle), sortedRows(got)
		}
		if limit > 0 && limit < len(oracle) {
			oracle = oracle[:limit]
		}
		require.Equal(t, oracle, got, "a ready reader answers from its snapshot exactly")
		return
	}
	requireRowsMayMiss(t, oracle, got, matrixDropped, ordered, limit)
}

// requireRowsMayMiss is the inexact comparison: only rows for which mayMiss
// holds may be absent.
func requireRowsMayMiss(t *testing.T, oracle, got []matrixRow, mayMiss func(string) bool, ordered bool, limit int) {
	t.Helper()
	if !ordered {
		oracle, got = sortedRows(oracle), sortedRows(got)
	}
	matrixDropped := mayMiss
	seen := make(map[string]bool, len(got))
	pos := 0
	for _, row := range got {
		require.False(t, seen[row.id], "row %s returned twice in %v", row.id, got)
		seen[row.id] = true
		require.NotEqual(t, matrixNewGen, row.gen, "row %s carries the recreated node's properties", row.id)
		for pos < len(oracle) && oracle[pos].id != row.id {
			require.True(t, matrixDropped(oracle[pos].id), "surviving row %s is missing from %v", oracle[pos].id, got)
			pos++
		}
		require.Less(t, pos, len(oracle), "row %s is not in the reader's snapshot %v", row.id, oracle)
		require.Equal(t, oracle[pos], row, "row %s differs from the reader's snapshot", row.id)
		pos++
	}
	if limit > 0 && len(got) >= limit {
		require.Len(t, got, limit, "an early stop returns exactly the rows asked for")
		return
	}
	for ; pos < len(oracle); pos++ {
		require.True(t, matrixDropped(oracle[pos].id), "surviving row %s is missing from %v", oracle[pos].id, got)
	}
}

// matrixRead is one public reader. limit > 0 stops the read after that many
// rows through its own early-stop mechanism.
type matrixRead struct {
	name    string
	idLevel bool
	ordered bool
	limits  []int
	read    func(b *BadgerEngine, limit int) ([]matrixRow, error)
	// first marks a single-node read; order is the same reader's full
	// stream, against which its one row is judged.
	first bool
	order func(b *BadgerEngine) ([]matrixRow, error)
}

func stopAfter(limit int, n int) bool { return limit > 0 && n >= limit }

func matrixEngineReads() []matrixRead {
	forEach := func(scope string) func(*BadgerEngine, int) ([]matrixRow, error) {
		return func(b *BadgerEngine, limit int) ([]matrixRow, error) {
			var rows []matrixRow
			err := b.ForEachNodeIDByLabelInScope(scope, matrixLabel, func(id NodeID) bool {
				rows = append(rows, matrixRow{id: string(id)})
				return !stopAfter(limit, len(rows))
			})
			return rows, err
		}
	}
	stream := func(stream func(*BadgerEngine, func(*Node) error) error) func(*BadgerEngine, int) ([]matrixRow, error) {
		return func(b *BadgerEngine, limit int) ([]matrixRow, error) {
			var nodes []*Node
			err := stream(b, func(n *Node) error {
				nodes = append(nodes, n)
				if stopAfter(limit, len(nodes)) {
					return ErrIterationStopped
				}
				return nil
			})
			if errors.Is(err, ErrIterationStopped) {
				err = nil
			}
			return nodeRows(nodes), err
		}
	}
	list := func(get func(*BadgerEngine) ([]*Node, error)) func(*BadgerEngine, int) ([]matrixRow, error) {
		return func(b *BadgerEngine, _ int) ([]matrixRow, error) {
			nodes, err := get(b)
			return nodeRows(nodes), err
		}
	}
	first := func(get func(*BadgerEngine) (*Node, error)) func(*BadgerEngine, int) ([]matrixRow, error) {
		return func(b *BadgerEngine, _ int) ([]matrixRow, error) {
			node, err := get(b)
			return nodeRows([]*Node{node}), err
		}
	}
	gen := []string{"gen"}
	return []matrixRead{
		{name: "ForEachNodeIDByLabel", idLevel: true, ordered: true, limits: []int{0, 1, 2, 4}, read: forEach("")},
		{name: "ForEachNodeIDByLabelInScope/a", idLevel: true, ordered: true, limits: []int{0, 1}, read: forEach("a:")},
		{name: "ForEachNodeIDByLabelInScope/keep", idLevel: true, ordered: true, limits: []int{0}, read: forEach("keep:")},
		{name: "HasLabelBatch", idLevel: true, read: func(b *BadgerEngine, _ int) ([]matrixRow, error) {
			ids := []NodeID{"a:3"}
			for _, id := range matrixFixture {
				ids = append(ids, NodeID(id))
			}
			got, err := b.HasLabelBatch(ids, matrixLabel)
			var rows []matrixRow
			for id, ok := range got {
				if ok {
					rows = append(rows, matrixRow{id: string(id)})
				}
			}
			return rows, err
		}},
		{name: "GetNodesByLabel", read: list(func(b *BadgerEngine) ([]*Node, error) { return b.GetNodesByLabel(matrixLabel) })},
		{name: "GetNodesByLabelInScope/a", read: list(func(b *BadgerEngine) ([]*Node, error) { return b.GetNodesByLabelInScope("a:", matrixLabel) })},
		{name: "GetNodesByLabelInScope/keep", read: list(func(b *BadgerEngine) ([]*Node, error) { return b.GetNodesByLabelInScope("keep:", matrixLabel) })},
		{name: "GetStoredNodesByLabel", read: list(func(b *BadgerEngine) ([]*Node, error) { return b.GetStoredNodesByLabel(matrixLabel) })},
		{name: "StreamNodesByLabelProjected", ordered: true, limits: []int{0, 1, 2}, read: stream(func(b *BadgerEngine, visit func(*Node) error) error {
			return b.StreamNodesByLabelProjected(matrixLabel, gen, visit)
		})},
		{name: "StreamNodesByLabelProjectedInScope/a", ordered: true, limits: []int{0, 1}, read: stream(func(b *BadgerEngine, visit func(*Node) error) error {
			return b.StreamNodesByLabelProjectedInScope("a:", matrixLabel, gen, visit)
		})},
		{name: "StreamStoredNodesByLabelProjected", ordered: true, limits: []int{0, 2}, read: stream(func(b *BadgerEngine, visit func(*Node) error) error {
			return b.StreamStoredNodesByLabelProjected(matrixLabel, gen, visit)
		})},
		{name: "GetFirstNodeByLabel", first: true,
			read: first(func(b *BadgerEngine) (*Node, error) { return b.GetFirstNodeByLabel(matrixLabel) }),
			order: func(b *BadgerEngine) ([]matrixRow, error) {
				return list(func(b *BadgerEngine) ([]*Node, error) { return b.GetNodesByLabel(matrixLabel) })(b, 0)
			}},
		{name: "GetFirstNodeByLabelInScope/keep", first: true,
			read: first(func(b *BadgerEngine) (*Node, error) { return b.GetFirstNodeByLabelInScope("keep:", matrixLabel) }),
			order: func(b *BadgerEngine) ([]matrixRow, error) {
				return list(func(b *BadgerEngine) ([]*Node, error) { return b.GetNodesByLabelInScope("keep:", matrixLabel) })(b, 0)
			}},
	}
}

// requireFirstRow checks a single-row read: the row is the first of the
// oracle, or, when the oracle's leading rows were dropped, the first
// surviving one.
func requireFirstRow(t *testing.T, oracle, got []matrixRow) {
	t.Helper()
	require.Len(t, got, 1, "a first-node read returns one node")
	for _, row := range oracle {
		if row.id == got[0].id {
			require.Equal(t, row, got[0])
			return
		}
		require.True(t, matrixDropped(row.id), "first read skipped surviving row %s for %v", row.id, got)
	}
	t.Fatalf("first read returned %v, not in its snapshot %v", got, oracle)
}

type matrixSeam struct {
	name string
	slot func(b *BadgerEngine) *atomic.Pointer[func()]
}

var matrixSeams = []matrixSeam{
	{"after-admission", func(b *BadgerEngine) *atomic.Pointer[func()] { return &b.labelIndex.afterAdmission }},
	{"before-reverse-lookup", func(b *BadgerEngine) *atomic.Pointer[func()] { return &b.labelIndex.beforeReverseLookup }},
	{"before-forward-lookup", func(b *BadgerEngine) *atomic.Pointer[func()] { return &b.labelIndex.beforeForwardLookup }},
}

// runPausedAtSeam runs read with seam armed. If the read reaches the seam, it
// is held there while transition runs. It reports whether the seam was hit.
func runPausedAtSeam(t *testing.T, b *BadgerEngine, seam matrixSeam, read func() ([]matrixRow, error), transition func()) ([]matrixRow, bool) {
	t.Helper()
	paused, release := pauseAtLabelHook(t, seam.slot(b))
	var rows []matrixRow
	var readErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		rows, readErr = read()
	}()
	hit := false
	select {
	case <-paused:
		hit = true
		transition()
		release()
		waitDropRetention(t, done, "paused read")
	case <-done:
		release()
	case <-time.After(time.Minute):
		t.Fatal("timed out waiting for the read or its seam")
	}
	require.NoError(t, readErr)
	return rows, hit
}

// Every public label read, paused at every lifecycle seam on its path while
// the index is ready or building, against drops, recreations and rebuilds.
func TestLabelScanEntryPointsAcrossDropAndRecreate(t *testing.T) {
	for _, building := range []bool{false, true} {
		mode := map[bool]string{false: "ready", true: "building"}[building]
		for _, read := range matrixEngineReads() {
			limits := read.limits
			if len(limits) == 0 {
				limits = []int{0}
			}
			for _, limit := range limits {
				hits := 0
				for _, transition := range matrixTransitions(building) {
					for _, seam := range matrixSeams {
						name := fmt.Sprintf("%s/%s/limit-%d/%s/%s", mode, read.name, limit, transition.name, seam.name)
						t.Run(name, func(t *testing.T) {
							b := newMatrixEngine(t, BadgerOptions{})
							createMatrixNodes(t, b, []string{matrixLabel}, matrixOldGen)
							var buildingGen uint64
							if building {
								buildingGen = markMatrixIndexBuilding(t, b)
							}
							oracleRead := func() ([]matrixRow, error) { return read.read(b, 0) }
							if read.first {
								oracleRead = func() ([]matrixRow, error) { return read.order(b) }
							}
							oracle, err := oracleRead()
							require.NoError(t, err)
							got, hit := runPausedAtSeam(t, b, seam, func() ([]matrixRow, error) { return read.read(b, limit) },
								func() { transition.run(t, b, buildingGen) })
							if hit {
								hits++
							}
							switch {
							case read.first && !hit:
								require.Equal(t, oracle[:1], got)
							case !hit:
								requireMatrixRows(t, oracle, got, true, read.ordered, limit)
							case read.first:
								requireFirstRow(t, oracle, got)
							default:
								requireMatrixRows(t, oracle, got, read.idLevel && !building, read.ordered, limit)
							}
						})
					}
				}
				if hits == 0 {
					t.Errorf("%s %s limit %d reached no lifecycle seam", mode, read.name, limit)
				}
			}
		}
	}
}

// The warm first-ID cache of an unscoped ID stream is probed with its own
// forward lookup. Paused there while that very ID is dropped and recreated,
// the stream still answers its snapshot exactly once, for full and
// early-stopped reads.
func TestForEachNodeIDByLabelWarmFirstCacheAcrossDropAndRecreate(t *testing.T) {
	read := matrixEngineReads()[0]
	for _, limit := range []int{0, 1, 2, 3} {
		for _, transition := range matrixTransitions(false) {
			t.Run(fmt.Sprintf("limit-%d/%s", limit, transition.name), func(t *testing.T) {
				b := newMatrixEngine(t, BadgerOptions{})
				// a:1 is numbered first, so it is the index's first entry and
				// the warmed cache's first ID.
				for _, id := range []string{"a:1", "keep:first", "a:2", "keep:last"} {
					_, err := b.CreateNode(&Node{ID: NodeID(id), Labels: []string{matrixLabel}, Properties: map[string]any{"gen": matrixOldGen}})
					require.NoError(t, err)
				}
				oracle, err := read.read(b, 0)
				require.NoError(t, err)
				cached, ok := b.labelCacheGetFirst(matrixLabel)
				require.True(t, ok)
				require.Equal(t, NodeID("a:1"), cached, "the oracle read warmed the cache with the dropped ID")
				got, hit := runPausedAtSeam(t, b, matrixSeams[2], func() ([]matrixRow, error) { return read.read(b, limit) },
					func() { transition.run(t, b, 0) })
				require.True(t, hit, "the cached first ID is probed through the forward lookup")
				requireMatrixRows(t, oracle, got, true, true, limit)
			})
		}
	}
}

// Point-in-time label reads walk the MVCC heads of their snapshot. Paused
// after the snapshot is captured and before any history is resolved, they
// still return every node's requested version from the retained history of
// the number in their snapshot, with its earlier labels and properties, and
// never the recreated node.
//
// Retained history of a live node is a chain of undo records ending at its
// current body, which DROP removes physically, so a dropped live node (a:1)
// may be missing. a:2 was deleted before the drop: its chain ends at the full
// body archived by the delete, so its earlier version stays available and
// must be returned. A transaction's label read at its BEGIN version holds
// the same contract.
func TestLabelVisibleAtReadsAcrossDropAndRecreate(t *testing.T) {
	reads := []struct {
		name    string
		label   string
		scope   string
		current bool
		tx      bool
	}{
		{name: "GetNodesByLabelVisibleAt/earlier-label", label: "Earlier"},
		{name: "GetNodesByLabelVisibleAt/earlier", label: matrixLabel},
		{name: "GetNodesByLabelVisibleAtInScope/a/earlier", label: matrixLabel, scope: "a:"},
		{name: "GetNodesByLabelVisibleAtInScope/keep/earlier", label: matrixLabel, scope: "keep:"},
		// At v2 a dropped live node's body is gone and v2 was never archived:
		// a:1 may be missing, but must not read as v1 or as the new node.
		{name: "GetNodesByLabelVisibleAt/current", label: matrixLabel, current: true},
		{name: "GetNodesByLabelVisibleAtInScope/a/current", label: matrixLabel, scope: "a:", current: true},
		{name: "Transaction.GetNodesByLabel/current", label: matrixLabel, current: true, tx: true},
	}
	for _, read := range reads {
		for _, transition := range matrixTransitions(false) {
			t.Run(read.name+"/"+transition.name, func(t *testing.T) {
				b := newMatrixEngine(t, BadgerOptions{EngineOptions: EngineOptions{RetentionPolicy: RetentionPolicy{MaxVersionsPerKey: 100}}})
				createMatrixNodes(t, b, []string{matrixLabel, "Earlier"}, "v1")
				v1 := matrixHighWater(t, b)
				for _, id := range matrixFixture {
					require.NoError(t, b.UpdateNode(&Node{ID: NodeID(id), Labels: []string{matrixLabel}, Properties: map[string]any{"gen": "v2"}}))
				}
				version := v1
				if read.current {
					version = matrixHighWater(t, b)
				}
				// Deleted after v2: visible at v1 and v2 from its archived full
				// body, absent for a transaction that begins now.
				require.NoError(t, b.DeleteNode("a:2"))
				get := func() ([]matrixRow, error) {
					nodes, err := b.GetNodesByLabelVisibleAtInScope(read.scope, read.label, version)
					return nodeRows(nodes), err
				}
				var tx *BadgerTransaction
				begin := func() {
					var err error
					tx, err = b.BeginTransaction()
					require.NoError(t, err)
					t.Cleanup(func() { _ = tx.Rollback() })
				}
				if read.tx {
					begin()
					get = func() ([]matrixRow, error) {
						nodes, err := tx.GetNodesByLabel(read.label)
						return nodeRows(nodes), err
					}
				}
				oracle, err := get()
				require.NoError(t, err)
				var want []string
				for _, id := range matrixFixture {
					if strings.HasPrefix(id, read.scope) && !(read.tx && id == "a:2") {
						want = append(want, id)
					}
				}
				oracleIDs := make([]string, 0, len(oracle))
				for _, row := range sortedRows(oracle) {
					oracleIDs = append(oracleIDs, row.id)
				}
				sort.Strings(want)
				require.Equal(t, want, oracleIDs, "the oracle sees every node at the requested version")
				var got []matrixRow
				if read.tx {
					// A new transaction: the oracle's answer is cached in tx.
					begin()
					transition.run(t, b, 0)
					got, err = get()
					require.NoError(t, err)
				} else {
					var hit bool
					got, hit = runPausedAtSeam(t, b, matrixSeams[1], get, func() { transition.run(t, b, 0) })
					require.True(t, hit, "the head walk resolves numbers through the lifecycle owner")
				}
				// The oracle holds each row's requested version, so a row read
				// as another version, or as the recreated node, fails.
				requireRowsMayMiss(t, oracle, got, func(id string) bool { return id == "a:1" }, false, 0)
			})
		}
	}
}

func matrixHighWater(t *testing.T, b *BadgerEngine) MVCCVersion {
	t.Helper()
	var high MVCCVersion
	for _, id := range matrixFixture {
		head, err := b.GetNodeCurrentHead(NodeID(id))
		require.NoError(t, err)
		if high.Compare(head.Version) < 0 {
			high = head.Version
		}
	}
	return high
}

// Explicit transactions read labels from the snapshot they began with. The
// drop, recreation or rebuild runs after BEGIN, and for an early-stopped
// stream between the stop and its resume, so the resume cursor sits on the
// dropped ID or before it.
func TestLabelTransactionReadsAcrossDropAndRecreate(t *testing.T) {
	type txRead struct {
		name      string
		namespace string
		stopAt    int
		read      func(tx *BadgerTransaction, limit int) ([]matrixRow, error)
	}
	getNodes := func(tx *BadgerTransaction, _ int) ([]matrixRow, error) {
		nodes, err := tx.GetNodesByLabel(matrixLabel)
		return nodeRows(nodes), err
	}
	stream := func(tx *BadgerTransaction, limit int) ([]matrixRow, error) {
		var nodes []*Node
		err := tx.StreamNodesByLabelProjected(matrixLabel, []string{"gen"}, func(n *Node) error {
			nodes = append(nodes, n)
			if stopAfter(limit, len(nodes)) {
				return ErrIterationStopped
			}
			return nil
		})
		if errors.Is(err, ErrIterationStopped) {
			err = nil
		}
		return nodeRows(nodes), err
	}
	reads := []txRead{
		{name: "GetNodesByLabel", read: getNodes},
		{name: "GetNodesByLabel/pinned-a", namespace: "a", read: getNodes},
		{name: "GetNodesByLabel/pinned-keep", namespace: "keep", read: getNodes},
		{name: "StreamNodesByLabelProjected", read: stream},
		{name: "StreamNodesByLabelProjected/pinned-a", namespace: "a", read: stream},
		// Fixture order keep:first, a:1, ...: a stop after one row leaves
		// the cursor before a:1, after two rows on it.
		{name: "StreamNodesByLabelProjected/resume-before-dropped", stopAt: 1, read: stream},
		{name: "StreamNodesByLabelProjected/resume-at-dropped", stopAt: 2, read: stream},
		{name: "StreamNodesByLabelProjected/pinned-a/resume-at-dropped", namespace: "a", stopAt: 1, read: stream},
	}
	begin := func(t *testing.T, b *BadgerEngine, namespace string) *BadgerTransaction {
		t.Helper()
		tx, err := b.BeginTransaction()
		require.NoError(t, err)
		t.Cleanup(func() { _ = tx.Rollback() })
		if namespace != "" {
			require.NoError(t, tx.SetNamespace(namespace))
		}
		return tx
	}
	for _, building := range []bool{false, true} {
		mode := map[bool]string{false: "ready", true: "building"}[building]
		for _, read := range reads {
			for _, transition := range matrixTransitions(building) {
				t.Run(mode+"/"+read.name+"/"+transition.name, func(t *testing.T) {
					b := newMatrixEngine(t, BadgerOptions{})
					createMatrixNodes(t, b, []string{matrixLabel}, matrixOldGen)
					var buildingGen uint64
					if building {
						buildingGen = markMatrixIndexBuilding(t, b)
					}
					oracle, err := read.read(begin(t, b, read.namespace), 0)
					require.NoError(t, err)
					require.NotEmpty(t, oracle)

					tx := begin(t, b, read.namespace)
					var prefix []matrixRow
					if read.stopAt > 0 {
						prefix, err = read.read(tx, read.stopAt)
						require.NoError(t, err)
						require.Equal(t, oracle[:read.stopAt], prefix, "the early stop returns the snapshot's first rows")
					}
					transition.run(t, b, buildingGen)
					got, err := read.read(tx, 0)
					require.NoError(t, err)
					if read.stopAt > 0 {
						require.Equal(t, prefix, got[:read.stopAt], "the resumed read replays its prefix")
					}
					requireMatrixRows(t, oracle, got, false, read.stopAt > 0, 0)
					for _, row := range prefix {
						require.Contains(t, got, row, "a row emitted before the drop stays in the resumed read")
					}
				})
			}
		}
	}
}

// The count fallback runs while the index is building. Paused at its
// admission while the generation it started under is published ready, it
// still counts its snapshot's bodies with the buffered creates, deletes and
// relabels overlaid, globally and per namespace.
func TestLabelCountFallbackAcrossReadyPublication(t *testing.T) {
	counts := []struct {
		name  string
		count func(b *BadgerEngine) (int64, error)
	}{
		{"NodeCountByLabel", func(b *BadgerEngine) (int64, error) { return b.NodeCountByLabel(matrixLabel) }},
		{"NodeCountByLabelInNamespace", func(b *BadgerEngine) (int64, error) { return b.NodeCountByLabelInNamespace("test", matrixLabel) }},
	}
	for _, count := range counts {
		t.Run(count.name, func(t *testing.T) {
			b := newMatrixEngine(t, BadgerOptions{WriteBehind: true, WriteBehindInterval: time.Hour, WriteBehindMaxOps: 1000})
			for _, id := range []NodeID{"test:relabelled", "test:deleted", "test:kept", "other:kept"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{matrixLabel}})
				require.NoError(t, err)
			}
			require.NoError(t, b.FlushWriteBehind())
			generation := markMatrixIndexBuilding(t, b)
			require.NoError(t, b.UpdateNode(&Node{ID: "test:relabelled", Labels: []string{"Other"}}))
			require.NoError(t, b.DeleteNode("test:deleted"))
			_, err := b.CreateNode(&Node{ID: "test:created", Labels: []string{matrixLabel}})
			require.NoError(t, err)
			// Oracle: test:kept and test:created, plus other:kept globally.
			want := int64(2)
			if count.name == "NodeCountByLabel" {
				want = 3
			}
			undisturbed, err := count.count(b)
			require.NoError(t, err)
			require.Equal(t, want, undisturbed)

			// The outer readiness check has chosen the fallback; the
			// generation is published ready before the reader is admitted.
			paused, release := pauseAtLabelHook(t, &b.labelIndex.beforeAdmission)
			var got int64
			var countErr error
			done := make(chan struct{})
			go func() {
				defer close(done)
				got, countErr = count.count(b)
			}()
			waitDropRetention(t, paused, "count admission")
			require.NoError(t, b.completeLabelIndexGeneration(generation))
			release()
			waitDropRetention(t, done, "count")
			require.NoError(t, countErr)
			require.Equal(t, want, got, "the count answers its snapshot with the buffered writes")
			require.NoError(t, b.FlushWriteBehind())
			after, err := count.count(b)
			require.NoError(t, err)
			require.Equal(t, want, after, "the flushed, ready count agrees")
		})
	}
}

// A malformed persisted count is repaired from the label index under the
// count lock. A full rebuild crossing the repair's internal scan, which
// cannot be crossed by DROP, leaves the scan on its own generation, and the
// repaired count is persisted.
func TestLabelCountRepairAcrossRebuild(t *testing.T) {
	for _, seam := range []string{"before-admission", "after-admission"} {
		t.Run(seam, func(t *testing.T) {
			b := newMatrixEngine(t, BadgerOptions{})
			for _, id := range []NodeID{"test:1", "test:2", "test:3", "other:1"} {
				_, err := b.CreateNode(&Node{ID: id, Labels: []string{matrixLabel}})
				require.NoError(t, err)
			}
			require.NoError(t, b.db.Update(func(txn *badger.Txn) error {
				return txn.Set(labelCountKey("test", matrixLabel), []byte{1})
			}))
			slot := &b.labelIndex.afterAdmission
			if seam == "before-admission" {
				slot = &b.labelIndex.beforeAdmission
			}
			paused, release := pauseAtLabelHook(t, slot)
			var got int64
			var countErr error
			done := make(chan struct{})
			go func() {
				defer close(done)
				got, countErr = b.NodeCountByLabelInNamespace("test", matrixLabel)
			}()
			waitDropRetention(t, paused, "repair scan")
			matrixRebuild(t, b)
			release()
			waitDropRetention(t, done, "repair")
			require.NoError(t, countErr)
			require.EqualValues(t, 3, got)
			persisted, err := b.readNamedCountInNamespace(countDeltaKindLabel, "test", matrixLabel)
			require.NoError(t, err)
			require.EqualValues(t, 3, persisted, "the repaired count is persisted")
		})
	}
}

// Close stops the reclaimers for good. A drop that retires entries after
// that, while an older transaction is open, keeps the entries resolvable but
// starts no reclaimer, so Close cannot return with one still waiting for the
// transaction.
func TestLabelIndexRetiredReclaimersStopAtClose(t *testing.T) {
	b := newMatrixEngine(t, BadgerOptions{})
	createMatrixNodes(t, b, []string{matrixLabel}, matrixOldGen)
	num, ok := b.idDict.lookupNodeNumID("a:1")
	require.True(t, ok)
	tx, err := b.BeginTransaction()
	require.NoError(t, err)

	b.labelIndex.retired.stop()
	_, _, err = b.DeleteByPrefix("a:")
	require.NoError(t, err)
	id, ok := b.labelIndexNodeID(num)
	require.True(t, ok, "entries retired after stop stay resolvable")
	require.Equal(t, NodeID("a:1"), id)
	b.labelIndex.retired.mu.RLock()
	rearmed := b.labelIndex.retired.ctx != nil
	b.labelIndex.retired.mu.RUnlock()
	require.False(t, rearmed, "a drop after stop must not start a reclaimer")
	require.NoError(t, tx.Rollback())
	require.NoError(t, b.Close())
}

// Additions racing stop either register before it and are cancelled and
// joined by it, or arrive after it and register nothing.
func TestRetiredLabelNodeIDsAddRacingStop(t *testing.T) {
	for i := 0; i < 200; i++ {
		var r retiredLabelNodeIDs
		started := make(chan struct{})
		added := make(chan struct{})
		go func() {
			defer close(added)
			close(started)
			batch, ctx := r.add(map[uint64]NodeID{uint64(i): "a:1"})
			if ctx != nil {
				go func() {
					defer r.waiters.Done()
					<-ctx.Done()
					r.release(batch)
				}()
			}
		}()
		<-started
		r.stop()
		<-added
		r.mu.RLock()
		require.Nil(t, r.ctx, "no context survives stop")
		r.mu.RUnlock()
		r.stop()
	}
}

// The dictionary check's building fallback numbers labelled bodies through
// its own snapshot, not the live dictionary, which may have changed since.
func TestDictionaryCheckFallbackUsesSnapshotNumbers(t *testing.T) {
	b := newMatrixEngine(t, BadgerOptions{})
	createMatrixNodes(t, b, []string{matrixLabel}, matrixOldGen)
	num, ok := b.idDict.lookupNodeNumID("keep:first")
	require.True(t, ok)
	markMatrixIndexBuilding(t, b)
	require.NoError(t, b.withView(func(txn *badger.Txn) error {
		b.idDict.nodePairRemoveByID("keep:first")
		labelled, err := b.labelNumIDsForDictionaryCheck(txn)
		require.NoError(t, err)
		require.True(t, labelled[num], "the snapshot's number counts as labelled")
		require.Len(t, labelled, len(matrixFixture))
		return nil
	}))
}
