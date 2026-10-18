package storage

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// A replayed commit's listeners run after the stage published it, not
// inside the stage: a listener that commits a write of the same node (a key
// the replay's publication holds while it runs) completes, and so does the
// flush that replayed it.
func TestWriteBehindReplayListenerThatCommitsDoesNotDeadlock(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	committed := make(chan error, 1)
	engine.OnNodeCreated(func(node *Node) {
		if node.ID != "test:first" {
			return
		}
		updated := CopyNode(node)
		updated.Properties = map[string]any{"touched": true}
		committed <- engine.UpdateNode(updated)
	})
	bufferedCreate(t, engine, &Node{ID: "test:first", Labels: []string{"L"}})

	flushed := make(chan error, 1)
	go func() { flushed <- engine.FlushWriteBehind() }()
	select {
	case err := <-committed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("a listener's commit during replay waits on the replay's publication")
	}
	select {
	case err := <-flushed:
		require.NoError(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("the replaying flush never returned")
	}
	got, err := engine.GetNode("test:first")
	require.NoError(t, err)
	require.Equal(t, true, got.Properties["touched"])
}

// A replay dispatches its commits' listeners in commit order, each after
// the stage published it: the callback finds its node in the property index.
func TestWriteBehindReplayListenersRunInCommitOrderAfterPublication(t *testing.T) {
	engine := newWriteBehindTestEngine(t, time.Hour)
	sm := engine.GetSchemaForNamespace("test")
	require.NoError(t, sm.AddPropertyIndex("replay_l_v", "L", []string{"v"}))
	sm.MarkPropertyIndexesFilled()
	const commits = 20
	type seen struct {
		id      NodeID
		indexed bool
	}
	order := make(chan seen, commits)
	engine.OnNodeCreated(func(node *Node) {
		value := node.Properties["v"]
		indexed := false
		for _, id := range sm.PropertyIndexLookup("L", "v", value) {
			indexed = indexed || id == node.ID
		}
		order <- seen{node.ID, indexed}
	})
	for i := 0; i < commits; i++ {
		bufferedCreate(t, engine, &Node{ID: NodeID(fmt.Sprintf("test:n%d", i)), Labels: []string{"L"}, Properties: map[string]any{"v": int64(i)}})
	}
	require.NoError(t, engine.FlushWriteBehind())
	for i := 0; i < commits; i++ {
		select {
		case got := <-order:
			require.Equal(t, NodeID(fmt.Sprintf("test:n%d", i)), got.id, "listeners in commit order")
			require.True(t, got.indexed, "%s published before its listener ran", got.id)
		case <-time.After(10 * time.Second):
			t.Fatalf("listener %d never ran", i)
		}
	}
}
