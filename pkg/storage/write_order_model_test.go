package storage

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/pkg/storage/writeordertest"
)

// engineDriver adapts a BadgerEngine to the write-order model harness. The
// write-behind buffer's background flusher keeps running, as in production,
// with an hour's interval: it applies a generation only when a step wakes
// it (a flush, Close, or a write ordered behind it), so a generation is
// applied, and its listeners run, where a step drains it.
type engineDriver struct {
	t           testing.TB
	dir         string
	writeBehind bool
	e           *BadgerEngine
	txs         map[int]*BadgerTransaction
}

func newEngineDriver(t testing.TB, writeBehind bool) *engineDriver {
	return &engineDriver{t: t, dir: t.TempDir(), writeBehind: writeBehind, txs: map[int]*BadgerTransaction{}}
}

func modelNodeID(name string) NodeID { return NodeID("test:" + name) }
func modelEdgeID(name string) EdgeID { return EdgeID("test:" + name) }
func modelName(id string) string     { return id[len("test:"):] }

func modelValue(props map[string]any) int64 {
	v, _ := props["v"].(int64)
	return v
}

func outcomeOf(err error) (writeordertest.Outcome, error) {
	if err != nil {
		return writeordertest.Rejected, err
	}
	return writeordertest.Committed, nil
}

func (d *engineDriver) Open(events func(writeordertest.Event)) error {
	e, err := runtimeFixtureOwner.OwnResult[*BadgerEngine](d.t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{DataDir: d.dir, WriteBehind: d.writeBehind, WriteBehindInterval: time.Hour}))
	if err != nil {
		return err
	}
	if e.writeBehind != nil {
		e.writeBehind.SetDelayBounds(time.Hour, time.Hour)
	}
	d.e = e
	e.OnNodeCreated(func(n *Node) {
		events(writeordertest.NodeEvent(modelName(string(n.ID)), modelValue(n.Properties), false))
	})
	e.OnNodeUpdated(func(n *Node) {
		events(writeordertest.NodeEvent(modelName(string(n.ID)), modelValue(n.Properties), false))
	})
	e.OnNodeDeleted(func(id NodeID) { events(writeordertest.NodeEvent(modelName(string(id)), 0, true)) })
	edgeEvent := func(ed *Edge) {
		events(writeordertest.EdgeEvent(modelName(string(ed.ID)), modelValue(ed.Properties), false,
			modelName(string(ed.StartNode)), modelName(string(ed.EndNode))))
	}
	e.OnEdgeCreated(edgeEvent)
	e.OnEdgeUpdated(edgeEvent)
	e.OnEdgeDeleted(func(id EdgeID) { events(writeordertest.EdgeEvent(modelName(string(id)), 0, true, "", "")) })
	return nil
}

func (d *engineDriver) Close() error {
	if d.e == nil {
		return nil
	}
	err := d.e.Close()
	d.e = nil
	return err
}

func (d *engineDriver) Supports(writeordertest.StepKind) bool { return true }

func stageOp(tx *BadgerTransaction, op writeordertest.Op, v int64) error {
	props := map[string]any{"v": v}
	switch op.Kind {
	case writeordertest.CreateNode:
		_, err := tx.CreateNode(&Node{ID: modelNodeID(op.Node), Properties: props})
		return err
	case writeordertest.UpdateNode:
		return tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: modelNodeID(op.Node), Properties: props}, Policy: ClientNodeEdits})
	case writeordertest.DeleteNode:
		return tx.DeleteGraphNode(modelNodeID(op.Node), CascadeEdges)
	case writeordertest.CreateEdge:
		return tx.CreateEdge(&Edge{ID: modelEdgeID(op.Edge), StartNode: modelNodeID(op.Start), EndNode: modelNodeID(op.End), Type: "LINK", Properties: props})
	case writeordertest.UpdateEdge:
		return tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: modelEdgeID(op.Edge), StartNode: modelNodeID(op.Start), EndNode: modelNodeID(op.End), Type: "LINK", Properties: props}})
	case writeordertest.DeleteEdge:
		return tx.DeleteEdge(modelEdgeID(op.Edge))
	}
	return fmt.Errorf("unknown op %s", op.Kind)
}

// Write performs op as a client does: a write the store refuses with a
// retryable conflict (Badger's, raised when the background flusher's replay
// commits keys the write read, such as a derived count) is retried. The
// store reports such a write as having taken no effect.
func (d *engineDriver) Write(op writeordertest.Op, v int64, implicit bool) (writeordertest.Outcome, error) {
	for attempt := 0; ; attempt++ {
		outcome, err := d.writeOnce(op, v, implicit)
		if attempt < 8 && (errors.Is(err, badger.ErrConflict) || errors.Is(err, ErrConflict)) {
			continue
		}
		return outcome, err
	}
}

func (d *engineDriver) writeOnce(op writeordertest.Op, v int64, implicit bool) (writeordertest.Outcome, error) {
	if implicit {
		tx, err := d.e.BeginTransaction()
		if err != nil {
			return outcomeOf(err)
		}
		if err := tx.SetImplicit(true); err != nil {
			_ = tx.Rollback()
			return outcomeOf(err)
		}
		if err := stageOp(tx, op, v); err != nil {
			_ = tx.Rollback()
			return outcomeOf(err)
		}
		return outcomeOf(tx.Commit())
	}
	props := map[string]any{"v": v}
	var err error
	switch op.Kind {
	case writeordertest.CreateNode:
		_, err = d.e.CreateNode(&Node{ID: modelNodeID(op.Node), Properties: props})
	case writeordertest.UpdateNode:
		err = d.e.UpdateNode(NodeUpdateRequest{After: &Node{ID: modelNodeID(op.Node), Properties: props}})
	case writeordertest.DeleteNode:
		err = d.e.DeleteNode(modelNodeID(op.Node))
	case writeordertest.CreateEdge:
		err = d.e.CreateEdge(&Edge{ID: modelEdgeID(op.Edge), StartNode: modelNodeID(op.Start), EndNode: modelNodeID(op.End), Type: "LINK", Properties: props})
	case writeordertest.UpdateEdge:
		err = d.e.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: modelEdgeID(op.Edge), StartNode: modelNodeID(op.Start), EndNode: modelNodeID(op.End), Type: "LINK", Properties: props}})
	case writeordertest.DeleteEdge:
		err = d.e.DeleteEdge(modelEdgeID(op.Edge))
	}
	return outcomeOf(err)
}

func (d *engineDriver) Begin(slot int, implicit bool) error {
	tx, err := d.e.BeginTransaction()
	if err != nil {
		return err
	}
	if implicit {
		if err := tx.SetImplicit(true); err != nil {
			_ = tx.Rollback()
			return err
		}
	}
	d.txs[slot] = tx
	return nil
}

func (d *engineDriver) Stage(slot int, op writeordertest.Op, v int64) (writeordertest.Outcome, error) {
	return outcomeOf(stageOp(d.txs[slot], op, v))
}

func (d *engineDriver) Commit(slot int) (writeordertest.Outcome, error) {
	tx := d.txs[slot]
	delete(d.txs, slot)
	return outcomeOf(tx.Commit())
}

func (d *engineDriver) Rollback(slot int) {
	if tx := d.txs[slot]; tx != nil {
		_ = tx.Rollback()
		delete(d.txs, slot)
	}
}

func (d *engineDriver) Flush() error {
	if err := d.e.FlushWriteBehind(); err != nil {
		return err
	}
	// FlushWriteBehind joins buffered generations. A direct write can queue
	// behind a concurrently running notification drainer, even with no buffered
	// generation left. The model's quiescent observation must join that work
	// too, before comparing the last delivered image with the stored image.
	n := d.e.queueCommitOperations(nil, nil, notificationAwaited)
	if n != nil {
		d.e.completeCommitNotification(n)
		d.e.awaitCommitNotification(n)
	}
	return nil
}

func (d *engineDriver) DeletePrefix() (writeordertest.Outcome, error) {
	_, _, err := d.e.DeleteByPrefix("test:")
	return outcomeOf(err)
}

func (d *engineDriver) backupPath(slot int) string {
	return filepath.Join(d.dir+"-backups", strconv.Itoa(slot))
}

func (d *engineDriver) Backup(slot int) error {
	if err := os.MkdirAll(d.dir+"-backups", 0o755); err != nil {
		return err
	}
	d.t.Cleanup(func() { _ = os.RemoveAll(d.dir + "-backups") })
	return d.e.Backup(d.backupPath(slot))
}

func (d *engineDriver) Restore(slot int) (writeordertest.Outcome, error) {
	return outcomeOf(d.e.Restore(d.backupPath(slot)))
}

func (d *engineDriver) Suppress(id string, edge, flag bool) (writeordertest.Outcome, error) {
	if edge {
		return outcomeOf(d.e.SetManualSuppression(StoredEntityEdgeKind, string(modelEdgeID(id)), flag))
	}
	return outcomeOf(d.e.SetManualSuppression(StoredEntityNodeKind, string(modelNodeID(id)), flag))
}

func (d *engineDriver) Sidecar(node string, v int64) (writeordertest.Outcome, error) {
	return outcomeOf(d.e.UpdateNodeEmbeddingSidecar(&Node{ID: modelNodeID(node), Properties: map[string]any{"v": v},
		ChunkEmbeddings: [][]float32{{1, 2, 3}}, EmbedMeta: map[string]any{"has_embedding": true, "chunk_count": 1}}))
}

func (d *engineDriver) Observe(nodes, edges []string) (writeordertest.Observed, error) {
	d.e.invalidateCachesAfterRestore()
	obs := writeordertest.Observed{State: writeordertest.NewState()}
	for _, name := range nodes {
		n, err := d.e.GetStoredNode(modelNodeID(name))
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return obs, err
		default:
			obs.State.Nodes[name] = modelValue(n.Properties)
		}
	}
	for _, name := range edges {
		e, err := d.e.GetStoredEdge(modelEdgeID(name))
		switch {
		case errors.Is(err, ErrNotFound):
		case err != nil:
			return obs, err
		default:
			obs.State.Edges[name] = writeordertest.EdgeBody{Start: modelName(string(e.StartNode)), End: modelName(string(e.EndNode)), V: modelValue(e.Properties)}
		}
	}
	obs.HiddenNodes, obs.HiddenEdges = map[string]bool{}, map[string]bool{}
	for name := range obs.State.Nodes {
		obs.HiddenNodes[name] = d.e.manuallySuppressed(manualSuppressionNode, string(modelNodeID(name)))
	}
	for name := range obs.State.Edges {
		obs.HiddenEdges[name] = d.e.manuallySuppressed(manualSuppressionEdge, string(modelEdgeID(name)))
	}
	var err error
	if obs.NodeCount, err = d.e.NodeCount(); err != nil {
		return obs, err
	}
	obs.EdgeCount, err = d.e.EdgeCount()
	return obs, err
}

// modelSeeds is the generated programs to run: NORNICDB_WRITE_ORDER_MODEL_SEED
// runs those listed (comma-separated), NORNICDB_WRITE_ORDER_MODEL_PROGRAMS
// sets how many (default 150).
func modelSeeds() []int64 {
	if list := os.Getenv("NORNICDB_WRITE_ORDER_MODEL_SEED"); list != "" {
		var seeds []int64
		for _, field := range strings.Split(list, ",") {
			if s, err := strconv.ParseInt(strings.TrimSpace(field), 10, 64); err == nil {
				seeds = append(seeds, s)
			}
		}
		return seeds
	}
	n := 150
	if v, err := strconv.Atoi(os.Getenv("NORNICDB_WRITE_ORDER_MODEL_PROGRAMS")); err == nil && v > 0 {
		n = v
	}
	seeds := make([]int64, n)
	for i := range seeds {
		seeds[i] = int64(i + 1)
	}
	return seeds
}

// TestWriteOrderModel runs generated programs of direct writes, implicit and
// explicit transactions held open across other steps, flushes, reopens,
// prefix deletes, backup and restore, manual suppression, embedding sidecars
// and listeners that write back, in both write-behind modes, against the
// reference model. A failure names its class, seed and steps.
func TestWriteOrderModel(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		writeBehind := writeBehind
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			if os.Getenv("NORNICDB_WRITE_ORDER_MODEL_MINIMIZE") != "" {
				newDriver := func() writeordertest.Driver { return newEngineDriver(t, writeBehind) }
				for _, seed := range modelSeeds() {
					steps := writeordertest.Generate(seed, 60)
					if fail, _ := writeordertest.Run(newDriver(), steps); fail != "" {
						steps, fail = writeordertest.Minimize(newDriver, steps, fail)
						_, trace := writeordertest.Run(newDriver(), steps)
						t.Errorf("seed=%d minimized to %d steps: %s\nsteps:\n  %s", seed, len(steps), fail, strings.Join(trace, "\n  "))
					}
				}
				return
			}
			writeordertest.RunPrograms(t, fmt.Sprintf("write-behind=%v", writeBehind),
				func() writeordertest.Driver { return newEngineDriver(t, writeBehind) }, modelSeeds(), 60)
		})
	}
}

// TestWriteOrderSchedules runs the storage-owned named cases in both
// write-behind modes.
func TestWriteOrderSchedules(t *testing.T) {
	for _, writeBehind := range []bool{false, true} {
		writeBehind := writeBehind
		t.Run(fmt.Sprintf("writeBehind=%v", writeBehind), func(t *testing.T) {
			writeordertest.RunSchedules(t, writeordertest.StorageSchedules(),
				func() writeordertest.Driver { return newEngineDriver(t, writeBehind) })
		})
	}
}
