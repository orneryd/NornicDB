package storage

// PR1667 / NOR71 current-release applicability probes.
//
// PR1667 (old branch, pkg/storage/badger_label_index_retention_test.go at
// ae6d84331ad8f3f3415f88fc4ec9df9cca950714) added a label-reader retirement
// foundation: a DROP DATABASE kept the dropped nodes' numeric label-index
// entries resolvable (generation lifecycle, drop markers, retired numeric
// entries) so that a label stream or snapshot admitted before the drop still
// visited every node of the generation it started with, instead of omitting
// them or resolving them through the dictionary the drop rebuilt.
//
// This file reproduces only what those regressions established, on the current
// release's own contracts:
//
//   - the public engine label-id stream, ForEachNodeIDByLabelInScope, held open
//     by pausing inside its own visit callback (every caller owns that
//     callback; no hook is needed or added),
//   - the public DROP DATABASE contract, BadgerEngine.DeleteByPrefix, run to
//     completion while the stream is still admitted,
//   - the managed handle's Backup stream, held open by a writer that parks it,
//     and the public BadgerEngine.Restore of that stream.
//
// None of PR1667's state is ported: no generations, drop markers, retired
// numeric entries, drop/retire hooks, nodeRef/NodeEnvelope, or marker format.
// The original regressions held the *drop* at its building marker; that marker
// does not exist on the current release, so the controls here run the drop to
// completion with the stream still paused instead of pausing the drop. The
// population is the one the original regressions used, so the assertions stay
// comparable.
//
// On the current release the label-id stream resolves each snapshot entry's
// numeric ID through the live dictionary (nodeIndexReader -> idDict), which a
// DROP DATABASE empties rather than retires: a dropped row is skipped before
// any node body or property is decoded, so no property is ever resolved
// through the rebuilt property-key dictionary. The omission is the earlier
// divergence these probes record.
//
// Each test states the retired guarantee directly, so a failure records the
// exact divergence on the current release rather than a weakened expectation.
// A paused stream, drop and backup all release and join through t.Cleanup, so a
// failed assertion cannot leave a goroutine or a held read behind.

import (
	"bytes"
	"context"
	"github.com/orneryd/nornicdb/internal/testlatch"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/stretchr/testify/require"
)

const pr1667RetirementLabel = "Target"

// pr1667NewEngine owns one on-disk engine through the repository fixture owner.
func pr1667NewEngine(t *testing.T) *BadgerEngine {
	t.Helper()
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(t.TempDir()))
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	return engine
}

// pr1667CreateTargetNodes creates the population under label Target with one
// snapshot property per node, and returns the IDs sorted for comparison.
func pr1667CreateTargetNodes(t *testing.T, b *BadgerEngine, ids ...string) []string {
	t.Helper()
	for _, id := range ids {
		pr1667CreateTargetNode(t, b, id, id+"-snapshot")
	}
	want := append([]string(nil), ids...)
	sort.Strings(want)
	return want
}

func pr1667CreateTargetNode(t *testing.T, b *BadgerEngine, id, name string) {
	t.Helper()
	_, err := b.CreateNode(t.Context(), &Node{
		ID:         NodeID(id),
		Labels:     []string{pr1667RetirementLabel},
		Properties: map[string]any{"name": name},
	})
	require.NoError(t, err)
}

// pr1667TargetIDs is a fresh snapshot read of the label after the drops.
func pr1667TargetIDs(t *testing.T, b *BadgerEngine, scope string) []string {
	t.Helper()
	nodes, err := b.GetNodesByLabelInScope(context.Background(), scope, pr1667RetirementLabel)
	require.NoError(t, err)
	ids := make([]string, 0, len(nodes))
	for _, node := range nodes {
		ids = append(ids, string(node.ID))
	}
	sort.Strings(ids)
	return ids
}

// pr1667Wait bounds every wait on a parked stream: an exceeded bound is a
// reported failure, never a sleep the test tolerates.
func pr1667Wait(t *testing.T, c <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-c:
	case <-time.After(time.Minute):
		t.Fatalf("timed out waiting for %s", name)
	}
}

// pr1667PausedLabelStream is one public label-id stream held open inside its
// own visit callback after its first after IDs.
type pr1667PausedLabelStream struct {
	paused chan struct{}
	resume *testlatch.Latch
	done   chan struct{}
	got    []NodeID
	err    error
}

func pr1667StartPausedLabelStream(t *testing.T, b *BadgerEngine, after int) *pr1667PausedLabelStream {
	t.Helper()
	s := &pr1667PausedLabelStream{
		paused: make(chan struct{}),
		resume: testlatch.New(),
		done:   make(chan struct{}),
	}
	go func() {
		defer close(s.done)
		s.err = b.ForEachNodeIDByLabelInScope(context.Background(), "", pr1667RetirementLabel, func(id NodeID) bool {
			s.got = append(s.got, id)
			if len(s.got) == after {
				close(s.paused)
				s.resume.Wait()
			}
			return true
		})
	}()
	t.Cleanup(func() {
		s.resume.Release()
		<-s.done
	})
	pr1667Wait(t, s.paused, "the admitted label stream to reach its pause")
	return s
}

// finish releases the stream, joins it, and returns the IDs it visited.
func (s *pr1667PausedLabelStream) finish(t *testing.T, when string) []string {
	t.Helper()
	s.resume.Release()
	pr1667Wait(t, s.done, "the admitted label stream to finish ("+when+")")
	require.NoError(t, s.err, "label stream across %s", when)
	ids := make([]string, 0, len(s.got))
	for _, id := range s.got {
		ids = append(ids, string(id))
	}
	sort.Strings(ids)
	return ids
}

// pr1667DropPrefix runs the public DROP DATABASE for prefix while the label
// stream is admitted. A drop that does not finish inside the bound is reported
// as a failure instead of hanging the package.
func pr1667DropPrefix(t *testing.T, b *BadgerEngine, prefix string) {
	t.Helper()
	type dropResult struct {
		nodes int64
		err   error
	}
	done := make(chan dropResult, 1)
	go func() {
		nodes, _, err := b.DeleteByPrefix(prefix)
		done <- dropResult{nodes: nodes, err: err}
	}()
	select {
	case result := <-done:
		require.NoError(t, result.err, "DROP DATABASE %s", prefix)
		require.Greater(t, result.nodes, int64(0), "DROP DATABASE %s must delete the prefix's nodes", prefix)
	case <-time.After(time.Minute):
		t.Fatalf("DROP DATABASE %s did not finish while a label stream was admitted", prefix)
	}
}

// pr1667PausedBackup parks the managed handle's Backup stream in its writer, so
// the snapshot it reads stays in flight while drops run.
type pr1667PausedBackup struct {
	once    sync.Once
	started chan struct{}
	resume  *testlatch.Latch
	done    chan struct{}
	buf     bytes.Buffer
	err     error
}

func (w *pr1667PausedBackup) Write(p []byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	w.resume.Wait()
	return w.buf.Write(p)
}

func pr1667StartPausedBackup(t *testing.T, b *BadgerEngine) *pr1667PausedBackup {
	t.Helper()
	w := &pr1667PausedBackup{
		started: make(chan struct{}),
		resume:  testlatch.New(),
		done:    make(chan struct{}),
	}
	go func() {
		defer close(w.done)
		_, w.err = b.db.Backup(w, 0)
	}()
	t.Cleanup(func() {
		w.resume.Release()
		<-w.done
	})
	pr1667Wait(t, w.started, "the backup stream to start")
	return w
}

// finish releases the backup stream, joins it and returns the restored label
// IDs of a fresh engine loaded with what it wrote.
func (w *pr1667PausedBackup) finish(t *testing.T, when string) []string {
	t.Helper()
	w.resume.Release()
	pr1667Wait(t, w.done, "the backup stream to finish ("+when+")")
	require.NoError(t, w.err, "backup stream across %s", when)
	path := filepath.Join(t.TempDir(), "backup.bin")
	require.NoError(t, os.WriteFile(path, w.buf.Bytes(), 0o600))
	restored := pr1667NewEngine(t)
	require.NoError(t, restored.Restore(path))
	return pr1667TargetIDs(t, restored, "")
}

// An admitted label stream must keep every ID of the snapshot it started with
// while a first DROP DATABASE completes and a second drops and re-creates its
// namespace. Today the stream resolves each snapshot entry through the live
// numeric dictionary, so the dropped nodes are omitted.
func TestPR1667CurrentReleaseLabelStreamAcrossTwoDrops(t *testing.T) {
	b := pr1667NewEngine(t)
	want := pr1667CreateTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "keep:last")
	stream := pr1667StartPausedLabelStream(t, b, 2)

	pr1667DropPrefix(t, b, "a:")
	pr1667DropPrefix(t, b, "b:")
	pr1667CreateTargetNode(t, b, "b:1", "recreated")
	recreated, err := b.GetNode(t.Context(), "b:1")
	require.NoError(t, err)
	require.Equal(t, "recreated", recreated.Properties["name"],
		"the re-create writes new state; the paused stream must not resolve it")

	require.Equal(t, []string{"b:1", "keep:first", "keep:last", "keep:second"}, pr1667TargetIDs(t, b, ""),
		"precondition: both drops took effect and the re-create is admitted")
	require.Equal(t, []string{"keep:first", "keep:last", "keep:second"}, pr1667TargetIDs(t, b, "keep"),
		"the untouched namespace keeps every node")

	require.Equal(t, want, stream.finish(t, "two drops"),
		"an admitted label stream must keep its original generation's IDs")
}

// The same holds across two completed drops and a third over a third
// namespace.
func TestPR1667CurrentReleaseLabelStreamAcrossThirdDrop(t *testing.T) {
	b := pr1667NewEngine(t)
	want := pr1667CreateTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "c:1", "c:2", "keep:last")
	stream := pr1667StartPausedLabelStream(t, b, 2)

	for _, prefix := range []string{"a:", "b:", "c:"} {
		pr1667DropPrefix(t, b, prefix)
	}

	require.Equal(t, []string{"keep:first", "keep:last", "keep:second"}, pr1667TargetIDs(t, b, ""),
		"precondition: all three drops took effect")
	require.Equal(t, want, stream.finish(t, "three drops"),
		"an admitted label stream must keep its original generation's IDs")
}

// A backup opened between two drops keeps its own snapshot, neither blocks the
// drops, and the older label stream stays complete.
func TestPR1667CurrentReleaseLabelStreamAndBackupInterleavedWithDrops(t *testing.T) {
	b := pr1667NewEngine(t)
	want := pr1667CreateTargetNodes(t, b, "keep:first", "keep:second", "a:1", "a:2", "b:1", "keep:last")
	stream := pr1667StartPausedLabelStream(t, b, 2)

	pr1667DropPrefix(t, b, "a:")
	backup := pr1667StartPausedBackup(t, b)
	pr1667DropPrefix(t, b, "b:")

	require.Equal(t, []string{"keep:first", "keep:last", "keep:second"}, pr1667TargetIDs(t, b, ""),
		"precondition: both drops took effect while the backup was in flight")
	require.Equal(t, []string{"b:1", "keep:first", "keep:last", "keep:second"}, backup.finish(t, "one drop"),
		"the backup keeps the snapshot it started with, after the first drop")
	require.Equal(t, want, stream.finish(t, "two drops with a backup in flight"),
		"an admitted label stream must keep its original generation's IDs")
}

func TestPR1667CurrentReleaseSnapshotKeepsPropertiesAcrossDrop(t *testing.T) {
	b := pr1667NewEngine(t)
	pr1667CreateTargetNode(t, b, "a:old", "original")
	tx, err := b.BeginTransaction(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, tx.SetNamespace("a"))
	pr1667DropPrefix(t, b, "a:")
	_, err = b.CreateNode(t.Context(), &Node{ID: "a:old", Labels: []string{pr1667RetirementLabel}, Properties: map[string]any{"different": int64(99)}})
	require.NoError(t, err)
	var got []*Node
	require.NoError(t, tx.StreamNodesByLabelProjected(context.Background(), pr1667RetirementLabel, nil, func(n *Node) error { got = append(got, n); return nil }))
	point, err := tx.GetNode(t.Context(), "a:old")
	require.NoError(t, err)
	require.Equal(t, map[string]any{"name": "original"}, point.Properties)
	require.Len(t, got, 1)
	require.Equal(t, map[string]any{"name": "original"}, got[0].Properties, "old snapshot must not decode its token through the recreated namespace dictionary")
}
