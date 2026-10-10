package storage

import (
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func bufferedTestNode(id string, props map[string]any) *Node {
	return &Node{ID: NodeID(id), Labels: []string{"L"}, Properties: props}
}

func bufferedTestEdge(id, from, to, typ string) *Edge {
	return &Edge{ID: EdgeID(id), StartNode: NodeID(from), EndNode: NodeID(to), Type: typ}
}

// blockingApply gates the replay so tests can hold a generation in the
// draining state while asserting writer/read behavior.
type blockingApply struct {
	mu      sync.Mutex
	started chan *CommitBuffer
	release chan struct{}
	err     error
	applied []*CommitBuffer
}

func newBlockingApply() *blockingApply {
	return &blockingApply{started: make(chan *CommitBuffer, 16), release: make(chan struct{})}
}

func (b *blockingApply) fn(buf *CommitBuffer) error {
	b.started <- buf
	<-b.release
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.err != nil {
		return b.err
	}
	b.applied = append(b.applied, buf)
	return nil
}

func (b *blockingApply) appliedCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.applied)
}

func TestWriteBehindBuffer_ReadYourOwnWritesBeforeApply(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	node := bufferedTestNode("n1", map[string]any{"v": int64(1)})
	buf.AppendNode(node)

	got, found, deleted := buf.GetNode("n1")
	require.True(t, found, "acknowledged write must be visible before apply")
	require.False(t, deleted)
	require.Equal(t, int64(1), got.Properties["v"])
	require.Equal(t, 1, buf.PendingOps())
}

func TestWriteBehindBuffer_NeverBlocksWritesDuringDrain(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buf.Close()

	buf.AppendNode(bufferedTestNode("n1", map[string]any{"v": int64(1)}))
	buf.AppendDelete("n2")
	buf.FlushAsync() // rotate so n1 enters draining; apply blocks in apply.fn

	<-apply.started // the flusher is now mid-apply

	// Writers must not wait for the blocked flush: append many ops with a
	// tight deadline.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			buf.AppendNode(bufferedTestNode("n1", map[string]any{"v": int64(i)}))
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("writers blocked behind a draining generation")
	}

	close(apply.release)
	require.NoError(t, buf.Flush())
}

// FlushAsync rotates without waiting; test-only helper on the buffer.
func (w *WriteBehindBuffer) FlushAsync() {
	w.rotate()
	w.signal()
}

func TestWriteBehindBuffer_AdaptiveTargetTracksDrainRate(t *testing.T) {
	const interval = 100 * time.Millisecond
	apply := func(buf *CommitBuffer) error {
		time.Sleep(10 * time.Millisecond) // deterministic-ish per-drain cost
		return nil
	}
	buf := NewWriteBehindBuffer(interval, 1_000_000, apply)
	defer buf.Close()

	// Before any drain the target is the configured cap.
	require.Equal(t, 1_000_000, buf.TargetSize())
	require.Zero(t, buf.DrainOpsPerSecond())

	// One drained generation of 1000 ops at ~10ms → ~100K ops/s.
	for i := 0; i < 1000; i++ {
		buf.AppendNode(bufferedTestNode("n"+strconv.Itoa(i), nil))
	}
	buf.FlushAsync()
	require.Eventually(t, func() bool { return buf.PendingOps() == 0 }, 5*time.Second, time.Millisecond)

	// rate ≈ 100K ops/s and the adaptive delay ≈ the ~10ms measured drain
	// latency, so target ≈ rate × delay ≈ 1K, clamped to
	// [minAdaptiveOps, maxOps].
	rate := buf.DrainOpsPerSecond()
	require.Positive(t, rate)
	require.Less(t, buf.CurrentInterval(), interval, "delay must track measured drain latency")
	target := buf.TargetSize()
	require.GreaterOrEqual(t, target, minAdaptiveOps)
	require.LessOrEqual(t, target, 1_000_000)
	require.InDelta(t, rate*buf.CurrentInterval().Seconds(), target, 5000,
		"target must track measured rate × adaptive delay")
}

func TestWriteBehindBuffer_AdaptiveTargetClampedByMaxOps(t *testing.T) {
	apply := func(buf *CommitBuffer) error { return nil }
	buf := NewWriteBehindBuffer(10*time.Millisecond, 500, apply)
	defer buf.Close()

	for i := 0; i < 500; i++ {
		buf.AppendNode(bufferedTestNode("n"+strconv.Itoa(i), nil))
	}
	require.NoError(t, buf.Flush())
	// Instant applies measure an enormous rate, but the target can never
	// exceed the configured cap.
	require.LessOrEqual(t, buf.TargetSize(), 500)
}

func TestWriteBehindBuffer_DelayPinnedWarning(t *testing.T) {
	apply := func(buf *CommitBuffer) error {
		time.Sleep(10 * time.Millisecond)
		return nil
	}
	buf := NewWriteBehindBuffer(2*time.Millisecond, 0, apply)
	defer buf.Close()

	var warns []string
	buf.SetWarn(func(msg string) { warns = append(warns, msg) })
	// Pin the adaptive delay far below the measured drain latency.
	buf.SetDelayBounds(time.Millisecond, 5*time.Millisecond)

	for i := 0; i < 200; i++ {
		buf.AppendNode(bufferedTestNode("n"+strconv.Itoa(i), nil))
	}
	require.NoError(t, buf.Flush())
	require.Equal(t, 5*time.Millisecond, buf.CurrentInterval(), "delay clamps at its bound")
	require.NotEmpty(t, warns, "a pinned delay must be reported")
	require.Contains(t, warns[len(warns)-1], "cannot keep up")
}

func TestWriteBehindBuffer_BackgroundApplyPreservesOrder(t *testing.T) {
	var mu sync.Mutex
	var order []string
	apply := func(buf *CommitBuffer) error {
		mu.Lock()
		defer mu.Unlock()
		for _, m := range buf.Mutations() {
			order = append(order, m.kind.String()+":"+m.id)
		}
		return nil
	}
	buf := NewWriteBehindBuffer(time.Hour, 0, apply)
	defer buf.Close()

	buf.AppendNode(bufferedTestNode("a", nil))
	buf.AppendEdge(bufferedTestEdge("e1", "a", "b", "R"))
	buf.AppendDelete("a")

	require.NoError(t, buf.Flush())
	require.Equal(t, []string{"node:a", "edge:e1", "delete:a"}, order)
}

func TestWriteBehindBuffer_CrossGenerationShadowing(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	buf.AppendNode(bufferedTestNode("n1", map[string]any{"v": int64(1)}))
	buf.FlushAsync()
	<-apply.started // generation 1 is draining (unapplied)

	buf.AppendNode(bufferedTestNode("n1", map[string]any{"v": int64(2)}))
	got, found, deleted := buf.GetNode("n1")
	require.True(t, found)
	require.False(t, deleted)
	require.Equal(t, int64(2), got.Properties["v"], "newest generation must shadow older unapplied ones")
}

func TestWriteBehindBuffer_DeleteHidesWrite(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	buf.AppendNode(bufferedTestNode("n1", nil))
	buf.AppendDelete("n1")
	_, found, deleted := buf.GetNode("n1")
	require.True(t, found)
	require.True(t, deleted, "a buffered delete must hide the buffered write")
}

func TestWriteBehindBuffer_SizeThresholdRotates(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 2, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	buf.AppendNode(bufferedTestNode("n1", nil))
	buf.AppendNode(bufferedTestNode("n2", nil))
	select {
	case <-apply.started:
	case <-time.After(2 * time.Second):
		t.Fatal("size threshold did not trigger a drain")
	}
}

func TestWriteBehindBuffer_ApplyErrorRetries(t *testing.T) {
	var calls atomic.Int64
	apply := func(buf *CommitBuffer) error {
		if calls.Add(1) == 1 {
			return errors.New("apply boom")
		}
		return nil
	}
	buf := NewWriteBehindBuffer(time.Hour, 0, apply)
	defer buf.Close()

	buf.AppendNode(bufferedTestNode("n1", nil))
	require.NoError(t, buf.Flush())
	require.Equal(t, int64(2), calls.Load(), "failed apply must be retried")
	require.Equal(t, 0, buf.PendingOps())
}

func TestWriteBehindBuffer_AppliedGenerationFallsThrough(t *testing.T) {
	applied := make(chan struct{}, 1)
	apply := func(buf *CommitBuffer) error {
		applied <- struct{}{}
		return nil
	}
	buf := NewWriteBehindBuffer(time.Hour, 0, apply)
	defer buf.Close()

	buf.AppendNode(bufferedTestNode("n1", nil))
	require.NoError(t, buf.Flush())
	_, found, _ := buf.GetNode("n1")
	require.False(t, found, "applied generations must fall through to the engine")
	require.Equal(t, 0, buf.PendingOps())
}

func TestWriteBehindBuffer_LabelAndTypeOverlays(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Hour, 0, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	buf.AppendNode(bufferedTestNode("n1", nil))
	buf.AppendNode(&Node{ID: "n2", Labels: []string{"Other"}})
	buf.AppendEdge(bufferedTestEdge("e1", "n1", "n2", "R"))

	labels := buf.LabelNodes("L")
	require.Len(t, labels, 1)
	require.Equal(t, NodeID("n1"), labels[0].ID)

	edges := buf.TypeEdges("R")
	require.Len(t, edges, 1)
	require.Equal(t, EdgeID("e1"), edges[0].ID)

	buf.AppendDelete("n1")
	require.Empty(t, buf.LabelNodes("L"), "deleted buffered nodes drop out of label overlay")
}

func TestWriteBehindBuffer_ConcurrentAppendsAndReads(t *testing.T) {
	apply := newBlockingApply()
	buf := NewWriteBehindBuffer(time.Millisecond, 50, apply.fn)
	defer buf.Close()
	defer func() {
		close(apply.release)
		require.NoError(t, buf.Flush())
	}()

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				buf.AppendNode(bufferedTestNode("n1", map[string]any{"w": int64(w), "i": int64(i)}))
			}
		}(w)
	}
	for r := 0; r < 2; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				_, _, _ = buf.GetNode("n1")
				_ = buf.PendingOps()
			}
		}()
	}
	wg.Wait()
}

func (k bufferedOpKind) String() string {
	switch k {
	case bufferedOpNode:
		return "node"
	case bufferedOpEdge:
		return "edge"
	case bufferedOpDelete:
		return "delete"
	}
	return "unknown"
}
