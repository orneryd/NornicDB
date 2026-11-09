package writeordertest

import (
	"sync"
	"testing"
)

// The claim boundary must account for work before event releases the harness
// lock: settlement may run immediately after that boundary, before any Write.
func TestListenerClaimReservesAllPendingWrites(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		t.Run(map[bool]string{false: "live", true: "deleted"}[deleted], func(t *testing.T) {
			r := &run{events: map[string][]int64{}, arms: []*arm{{key: nodeKey("n0")}, {key: nodeKey("n0"), implicit: true}, {key: nodeKey("n1")}, {key: nodeKey("n0"), fired: true}}}
			r.listened = sync.NewCond(&r.cb)
			claimed := r.claimEvent(NodeEvent("n0", 7, deleted))
			want := 2
			eventVersion := int64(7)
			if deleted {
				want = 0
				eventVersion = -1
			}
			if len(claimed) != want || r.listening != want || r.listenerWrites != want {
				t.Fatalf("claim/reservation mismatch: claimed=%d outstanding=%d writes=%d want=%d", len(claimed), r.listening, r.listenerWrites, want)
			}
			if r.arms[2].fired {
				t.Fatal("nonmatching arm claimed")
			}
			if r.arms[0].fired == deleted || r.arms[1].fired == deleted {
				t.Fatal("matching arm claim policy changed")
			}
			if got := r.events[nodeKey("n0")]; len(got) != 1 || got[0] != eventVersion {
				t.Fatalf("notification record=%v", got)
			}
			if repeated := r.claimEvent(NodeEvent("n0", 8, deleted)); len(repeated) != 0 || r.listening != want || r.listenerWrites != want {
				t.Fatal("already claimed work was reserved twice")
			}
		})
	}
}

type listenerReservationDriver struct {
	Driver
	writes int
}

func (d *listenerReservationDriver) Write(Op, int64, bool) (Outcome, error) {
	d.writes++
	return Committed, nil
}

func TestListenerReservationCompletesBeforeSettlement(t *testing.T) {
	d := &listenerReservationDriver{}
	r := &run{d: d, events: map[string][]int64{}, arms: []*arm{{key: edgeKey("e0")}, {key: edgeKey("e0"), implicit: true}}}
	r.listened = sync.NewCond(&r.cb)
	r.event(EdgeEvent("e0", 7, false, "n0", "n1"))
	pending := r.takePending()
	if r.listening != 0 || r.listenerWrites != 2 || d.writes != 2 || len(pending) != 2 {
		t.Fatalf("incomplete listener work: outstanding=%d total=%d writes=%d pending=%v", r.listening, r.listenerWrites, d.writes, pending)
	}
	for _, p := range pending {
		if p.op.Kind != UpdateEdge || p.op.Edge != "e0" || p.op.Start != "n0" || p.op.End != "n1" || p.outcome != Committed {
			t.Fatalf("listener write changed: %+v", p)
		}
	}
	if len(r.takePending()) != 0 {
		t.Fatal("settlement consumed a listener twice")
	}
}
