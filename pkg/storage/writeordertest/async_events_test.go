package writeordertest

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// asyncDriver is an in-memory store that delivers notifications on a
// dispatcher goroutine of its own, in commit order, as a write-behind worker
// does: notifications and their listeners' writes run while the harness takes
// later steps. Flush (and Observe, and Close) wait until nothing is queued or
// being delivered.
type asyncDriver struct {
	mu      sync.Mutex
	state   State
	events  func(Event)
	queue   [][]Event
	busy    bool       // the dispatcher is delivering a batch
	changed *sync.Cond // on mu: the queue grew, or a batch was delivered
}

func (d *asyncDriver) Open(events func(Event)) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state.Nodes == nil {
		d.state = NewState()
	}
	d.events = events
	if d.changed == nil {
		d.changed = sync.NewCond(&d.mu)
		go d.dispatch()
	}
	return nil
}

// dispatch delivers the queued batches, oldest first, one at a time.
func (d *asyncDriver) dispatch() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for {
		for len(d.queue) == 0 {
			d.changed.Wait()
		}
		batch, events := d.queue[0], d.events
		d.queue = d.queue[1:]
		d.busy = true
		d.mu.Unlock()
		time.Sleep(time.Millisecond)
		for _, ev := range batch {
			events(ev)
		}
		d.mu.Lock()
		d.busy = false
		d.changed.Broadcast()
	}
}

// idle waits until nothing is queued or being delivered.
func (d *asyncDriver) idle() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for len(d.queue) > 0 || d.busy {
		d.changed.Wait()
	}
}

func (d *asyncDriver) Close() error {
	d.idle()
	return nil
}

func (d *asyncDriver) Supports(k StepKind) bool {
	return k == Direct || k == Flush || k == Arm || k == Reopen
}

func (d *asyncDriver) Write(op Op, v int64, implicit bool) (Outcome, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !op.valid(d.state) && op.Kind != UpdateNode {
		return Rejected, errors.New("invalid")
	}
	keys := op.apply(d.state, v)
	evs := make([]Event, 0, len(keys))
	for _, k := range keys {
		id := k[2:]
		if k[0] == 'n' {
			nv, ok := d.state.Nodes[id]
			evs = append(evs, NodeEvent(id, nv, !ok))
		} else {
			e, ok := d.state.Edges[id]
			evs = append(evs, EdgeEvent(id, e.V, !ok, e.Start, e.End))
		}
	}
	d.queue = append(d.queue, evs)
	d.changed.Broadcast()
	return Committed, nil
}

func (d *asyncDriver) Flush() error {
	d.idle()
	return nil
}

func (d *asyncDriver) Observe(nodes, edges []string) (Observed, error) {
	d.idle()
	d.mu.Lock()
	defer d.mu.Unlock()
	return Observed{State: d.state.clone(), NodeCount: -1, EdgeCount: -1}, nil
}

func (d *asyncDriver) Begin(int, bool) error { return errors.New("unsupported") }
func (d *asyncDriver) Stage(int, Op, int64) (Outcome, error) {
	return Rejected, errors.New("unsupported")
}
func (d *asyncDriver) Commit(int) (Outcome, error)    { return Rejected, errors.New("unsupported") }
func (d *asyncDriver) Rollback(int)                   {}
func (d *asyncDriver) DeletePrefix() (Outcome, error) { return Rejected, errors.New("unsupported") }
func (d *asyncDriver) Backup(int) error               { return errors.New("unsupported") }
func (d *asyncDriver) Restore(int) (Outcome, error)   { return Rejected, errors.New("unsupported") }
func (d *asyncDriver) Suppress(string, bool, bool) (Outcome, error) {
	return Rejected, errors.New("unsupported")
}
func (d *asyncDriver) Sidecar(string, int64) (Outcome, error) {
	return Rejected, errors.New("unsupported")
}

// TestAsyncNotificationsAndListenerWrites: notifications delivered on a
// store's own goroutines, and the listener writes they trigger, run while
// the harness arms more listeners and writes again. Under -race the harness
// must not race with them, and the model must still match the store.
func TestAsyncNotificationsAndListenerWrites(t *testing.T) {
	var steps []Step
	for round := 0; round < 20; round++ {
		n := NodeName(round % Nodes)
		steps = append(steps,
			Step{Kind: Arm, Op: Op{Node: n}},
			Step{Kind: Direct, Op: Op{Kind: CreateNode, Node: n}},
			Step{Kind: Arm, Op: Op{Node: NodeName((round + 1) % Nodes)}},
			Step{Kind: Direct, Op: Op{Kind: UpdateNode, Node: n}},
			Step{Kind: Flush},
			Step{Kind: Direct, Op: Op{Kind: DeleteNode, Node: n}},
			Step{Kind: Flush},
		)
	}
	steps = append(steps, Step{Kind: Reopen})
	if fail, trace := Run(&asyncDriver{}, steps); fail != "" {
		t.Fatalf("%s\n%v", fail, trace)
	}
}
