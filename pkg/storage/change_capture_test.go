package storage

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The retention job keeps events for the engine's retention (2 days by
// default) and always keeps the last transaction's; dropping a database
// removes its events; Close stops the job.
func TestChangeEventRetentionAndDrop(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	if engine != nil {
		fixtureowner.Own(t, engine)
	}
	require.NoError(t, err)
	require.Equal(t, ChangeCaptureOff, engine.ChangeCapture("cdc"))
	require.Equal(t, defaultChangeRetention, engine.changeRetention())
	engine.SetChangeCapture("cdc", ChangeCaptureFull)
	engine.SetChangeCapture("cdc", ChangeCaptureDiff) // a second database or mode starts no second job
	require.Equal(t, ChangeCaptureDiff, engine.ChangeCapture("cdc"))
	engine.SetChangeRetention(time.Hour)
	require.Equal(t, time.Hour, engine.changeRetention())

	for _, id := range []NodeID{"cdc:a", "cdc:b"} {
		_, err := engine.CreateNode(&Node{ID: id, Labels: []string{"L"}, Properties: map[string]any{"v": int64(1)}})
		require.NoError(t, err)
	}
	earliest, last, err := engine.ChangeBounds("cdc")
	require.NoError(t, err)
	require.Equal(t, [2]uint64{1, 2}, [2]uint64{earliest, last})

	// Nothing is old enough now; two hours on, all but the last transaction.
	engine.changes.job.RunOnce(time.Now())
	events, err := engine.ChangeEvents("cdc", 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 2)
	engine.changes.job.RunOnce(time.Now().Add(2 * time.Hour))
	events, err = engine.ChangeEvents("cdc", 0, 0, 0)
	require.NoError(t, err)
	require.Len(t, events, 1)
	require.Equal(t, uint64(2), events[0].CommitSeq)
	limited, err := engine.ChangeEvents("cdc", 0, 0, 1)
	require.NoError(t, err)
	require.Len(t, limited, 1)

	_, _, err = engine.DeleteByPrefix("cdc:")
	require.NoError(t, err)
	_, last, err = engine.ChangeBounds("cdc")
	require.NoError(t, err)
	require.Zero(t, last)

	require.NoError(t, engine.Close())
	require.Nil(t, engine.changes.job)
	engine.SetChangeCapture("cdc", ChangeCaptureOff)
	require.Equal(t, ChangeCaptureOff, engine.ChangeCapture("cdc"))
	require.Equal(t, ChangeCaptureOff, ParseChangeCaptureMode("off"))
	require.Equal(t, "OFF", ChangeCaptureOff.String())
}

// A capturing database's direct writes run as transactions: the bulk and
// relationship writes too, each one transaction.
func TestChangeCaptureDirectBulkWrites(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	engine.SetChangeCapture("cdc", ChangeCaptureFull)
	require.NoError(t, engine.BulkCreateNodes([]*Node{{ID: "cdc:a", Labels: []string{"A"}}, {ID: "cdc:b", Labels: []string{"B"}}}))
	require.NoError(t, engine.BulkCreateEdges([]*Edge{{ID: "cdc:r", Type: "R", StartNode: "cdc:a", EndNode: "cdc:b"}}))
	require.NoError(t, engine.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: "cdc:r", Type: "R", StartNode: "cdc:a", EndNode: "cdc:b", Properties: map[string]any{"w": int64(2)}}}))
	require.NoError(t, engine.DeleteEdge("cdc:r"))
	require.NoError(t, engine.CreateEdge(&Edge{ID: "cdc:s", Type: "S", StartNode: "cdc:a", EndNode: "cdc:b"}))
	require.NoError(t, engine.BulkDeleteEdges([]EdgeID{"cdc:s"}))
	require.NoError(t, engine.BulkDeleteNodes([]NodeID{"cdc:a", "cdc:b"}))
	events, err := engine.ChangeEvents("cdc", 0, 0, 0)
	require.NoError(t, err)
	var shape []string
	for _, event := range events {
		kind := "r"
		if event.Node {
			kind = "n"
		}
		shape = append(shape, kind+string(event.Operation))
	}
	require.Equal(t, []string{"nc", "nc", "rc", "ru", "rd", "rc", "rd", "nd", "nd"}, shape)
	require.Equal(t, events[0].CommitSeq, events[1].CommitSeq, "one bulk write, one transaction")
	require.Equal(t, []string{"A"}, events[2].StartLabels)
	require.Equal(t, []string{"B"}, events[2].EndLabels)
}

// changeShape is each event as kind and operation ("nc", "ru", ...).
func changeShape(events []ChangeEvent) []string {
	shape := make([]string, len(events))
	for i, event := range events {
		kind := "r"
		if event.Node {
			kind = "n"
		}
		shape[i] = kind + string(event.Operation)
	}
	return shape
}

// Transactions in a capturing database: an update's FULL and DIFF states,
// a label change, an update that changes nothing and a node created and
// deleted in one transaction (no events), a connected node's delete (its
// relationship first), endpoint labels read from stored nodes, metadata,
// a rollback (nothing), and reading after a position.
func TestChangeCaptureTransactions(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	require.Equal(t, ChangeCaptureDiff, ParseChangeCaptureMode(" diff "))
	require.Equal(t, ChangeCaptureFull, ParseChangeCaptureMode("FULL"))
	require.Equal(t, "DIFF", ChangeCaptureDiff.String())
	require.Equal(t, "FULL", ChangeCaptureFull.String())

	run := func(write func(tx *BadgerTransaction) error) {
		t.Helper()
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		require.NoError(t, write(tx))
		require.NoError(t, tx.Commit())
	}
	for _, mode := range []ChangeCaptureMode{ChangeCaptureFull, ChangeCaptureDiff} {
		ns := strings.ToLower(mode.String())
		id := func(local string) NodeID { return NodeID(ns + ":" + local) }
		engine.SetChangeCapture(ns, mode)
		// Endpoints stored before the relationship's transaction.
		for _, local := range []string{"a", "b"} {
			_, err := engine.CreateNode(&Node{ID: id(local), Labels: []string{"L" + local}, Properties: map[string]any{"v": int64(1), "k": "x"}})
			require.NoError(t, err)
		}
		run(func(tx *BadgerTransaction) error {
			require.NoError(t, tx.SetMetadata(map[string]interface{}{"app": "pd"}))
			return tx.CreateEdge(&Edge{ID: EdgeID(ns + ":r"), Type: "R", StartNode: id("a"), EndNode: id("b"), Properties: map[string]any{"w": int64(1)}})
		})
		run(func(tx *BadgerTransaction) error {
			if err := tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id("a"), Labels: []string{"La", "New"}, Properties: map[string]any{"v": int64(2), "k": "x"}}, Policy: ClientNodeEdits}); err != nil {
				return err
			}
			return tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: EdgeID(ns + ":r"), Type: "R", StartNode: id("a"), EndNode: id("b"), Properties: map[string]any{"w": int64(2)}}})
		})
		run(func(tx *BadgerTransaction) error { // no change, and created then deleted
			if err := tx.UpdateGraphNode(NodeUpdateRequest{After: &Node{ID: id("b"), Labels: []string{"Lb"}, Properties: map[string]any{"v": int64(1), "k": "x"}}, Policy: ClientNodeEdits}); err != nil {
				return err
			}
			if err := tx.UpdateEdge(EdgeUpdateRequest{After: &Edge{ID: EdgeID(ns + ":r"), Type: "R", StartNode: id("a"), EndNode: id("b"), Properties: map[string]any{"w": int64(2)}}}); err != nil {
				return err
			}
			if _, err := tx.CreateNode(&Node{ID: id("tmp")}); err != nil {
				return err
			}
			return tx.DeleteGraphNode(id("tmp"), CascadeEdges)
		})
		tx, err := engine.BeginTransaction()
		require.NoError(t, err)
		_, err = tx.CreateNode(&Node{ID: id("gone")})
		require.NoError(t, err)
		require.NoError(t, tx.Rollback())
		run(func(tx *BadgerTransaction) error { return tx.DeleteGraphNode(id("a"), CascadeEdges) }) // cascades, as DETACH DELETE

		events, err := engine.ChangeEvents(ns, 0, 0, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"nc", "nc", "rc", "nu", "ru", "rd", "nd"}, changeShape(events), mode.String())
		require.Equal(t, map[string]any{"app": "pd"}, events[2].TxMetadata)
		require.Equal(t, []string{"La"}, events[2].StartLabels, "a stored endpoint's labels")
		require.Equal(t, []string{"Lb"}, events[2].EndLabels)
		update, edgeUpdate := events[3], events[4]
		if mode == ChangeCaptureFull {
			require.Equal(t, &ChangeState{Properties: map[string]any{"v": int64(1), "k": "x"}, Labels: []string{"La"}}, update.Before)
			require.Equal(t, &ChangeState{Properties: map[string]any{"v": int64(2), "k": "x"}, Labels: []string{"La", "New"}}, update.After)
		} else {
			require.Equal(t, &ChangeState{Properties: map[string]any{"v": int64(1)}}, update.Before)
			require.Equal(t, &ChangeState{Properties: map[string]any{"v": int64(2)}, Labels: []string{"New"}}, update.After)
		}
		require.Equal(t, &ChangeState{Properties: map[string]any{"w": int64(1)}}, edgeUpdate.Before)
		require.Equal(t, &ChangeState{Properties: map[string]any{"w": int64(2)}}, edgeUpdate.After)
		require.Equal(t, events[5].CommitSeq, events[6].CommitSeq, "the relationship's delete comes with its node's")
		require.Equal(t, &ChangeState{Properties: map[string]any{"w": int64(2)}}, events[5].Before)

		after, err := engine.ChangeEvents(ns, events[4].CommitSeq, events[4].Seq, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"rd", "nd"}, changeShape(after))
	}
}

// The retention job starts and stops, and ChangeCaptureOf finds the Badger
// engine under a namespace wrapper, and nothing for no engine.
func TestChangeRetentionJobAndCaptureOf(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineInMemory())
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	job := newChangeRetentionJob(engine, 0)
	require.Equal(t, time.Minute, job.interval)
	job = newChangeRetentionJob(engine, time.Millisecond)
	job.Start(context.Background())
	job.Start(context.Background()) // already running
	time.Sleep(5 * time.Millisecond)
	job.Stop()
	job.Stop()

	capture, ok := ChangeCaptureOf(NewNamespacedEngine(engine, "cdc"))
	require.True(t, ok)
	require.Equal(t, engine, capture)
	_, ok = ChangeCaptureOf(nil)
	require.False(t, ok)
}

// With write-behind on, a capturing database's commits still go through
// Badger, each a transaction with its events: implicit transactions, direct
// creates, updates and deletes alike; an upsert of a missing node creates
// it; a failing direct write rolls back and records nothing.
func TestChangeCaptureWithWriteBehindAndFailedWrites(t *testing.T) {
	engine, err := fixtureowner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngineWithOptions(BadgerOptions{InMemory: true, WriteBehind: true}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	require.True(t, engine.writeBehindEnabled())
	engine.SetChangeCapture("cdc", ChangeCaptureFull)
	async := engine

	require.NoError(t, engine.UpdateNode(NodeUpdateRequest{After: &Node{ID: "cdc:a", Labels: []string{"A"}}})) // upsert: a create
	for _, id := range []NodeID{"cdc:b", "cdc:c", "cdc:d"} {
		_, err := async.CreateNode(&Node{ID: id, Labels: []string{"N"}})
		require.NoError(t, err)
	}
	require.NoError(t, async.CreateEdge(&Edge{ID: "cdc:r", Type: "R", StartNode: "cdc:a", EndNode: "cdc:b"}))
	require.NoError(t, async.CreateEdge(&Edge{ID: "cdc:s", Type: "S", StartNode: "cdc:a", EndNode: "cdc:c"}))
	require.NoError(t, async.DeleteEdge("cdc:r"))
	require.NoError(t, async.BulkDeleteEdges([]EdgeID{"cdc:s"}))
	require.NoError(t, async.DeleteNode("cdc:b"))
	require.NoError(t, async.BulkDeleteNodes([]NodeID{"cdc:c", "cdc:d"}))

	// Failing direct writes: a duplicate create, a bulk create holding one,
	// a relationship to a missing node, a delete of a missing node.
	_, err = engine.CreateNode(&Node{ID: "cdc:a"})
	require.Error(t, err)
	require.Error(t, engine.BulkCreateNodes([]*Node{{ID: "cdc:e"}, {ID: "cdc:a"}}))
	require.Error(t, engine.BulkCreateEdges([]*Edge{{ID: "cdc:t", Type: "T", StartNode: "cdc:a", EndNode: "cdc:missing"}}))
	require.Error(t, engine.DeleteNode("cdc:missing"))

	events, err := engine.ChangeEvents("cdc", 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, []string{"nc", "nc", "nc", "nc", "rc", "rc", "rd", "rd", "nd", "nd", "nd"}, changeShape(events))
	_, err = engine.GetNode("cdc:e")
	require.ErrorIs(t, err, ErrNotFound, "the failed bulk create rolled back")

	// An implicit transaction in the capturing database: events at once.
	tx, err := engine.BeginTransaction()
	require.NoError(t, err)
	require.NoError(t, tx.SetImplicit(true))
	_, err = tx.CreateNode(&Node{ID: "cdc:implicit", Labels: []string{"I"}})
	require.NoError(t, err)
	require.NoError(t, tx.Commit())
	events, err = engine.ChangeEvents("cdc", 0, 0, 0)
	require.NoError(t, err)
	require.Equal(t, "implicit", events[len(events)-1].ID)
}
