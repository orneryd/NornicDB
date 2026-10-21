package memory

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestPolicyDisabledDoesNotSample(t *testing.T) {
	called := false
	policy := NewPolicy(0, 0, time.Second, func() (Sample, error) {
		called = true
		return Sample{}, errors.New("unexpected sample")
	}, time.Now)
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	lease, err := policy.Reserve(1 << 30)
	if err != nil || lease == nil || called {
		t.Fatalf("disabled reserve: lease=%v err=%v sampled=%v", lease, err, called)
	}
	lease.Release()
}

func TestPolicyRejectsStaleReadingsAndRecovers(t *testing.T) {
	now := time.Unix(100, 0)
	sample := Sample{Used: 100, Ceiling: 1000}
	policy := NewPolicy(800, 100, time.Second, func() (Sample, error) { return sample, nil }, func() time.Time { return now })
	if _, err := policy.Reserve(1); !errors.Is(err, ErrStaleSample) {
		t.Fatalf("before first sample: %v", err)
	}
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	lease, err := policy.Reserve(500)
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Second)
	if _, err := policy.Reserve(1); !errors.Is(err, ErrStaleSample) {
		t.Fatalf("stale sample: %v", err)
	}
	lease.Release()
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	if lease, err := policy.Reserve(500); err != nil {
		t.Fatal(err)
	} else {
		lease.Release()
	}
}

func TestPolicyConcurrentReservationsCannotOverspend(t *testing.T) {
	policy := NewPolicy(1000, 100, time.Second, func() (Sample, error) {
		return Sample{Used: 100, Ceiling: 1000}, nil
	}, time.Now)
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	var accepted atomic.Int64
	var leasesMu sync.Mutex
	var leases []*Lease
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lease, err := policy.Reserve(100)
			if err != nil {
				if !errors.Is(err, ErrLimitExceeded) {
					t.Errorf("unexpected reserve error: %v", err)
				}
				return
			}
			accepted.Add(1)
			leasesMu.Lock()
			leases = append(leases, lease)
			leasesMu.Unlock()
		}()
	}
	wg.Wait()
	if got := accepted.Load(); got != 8 {
		t.Fatalf("accepted %d reservations, want 8", got)
	}
	for _, lease := range leases {
		lease.Release()
		lease.Release() // duplicate terminal cleanup must not underflow
	}
	if got := policy.Reserved(); got != 0 {
		t.Fatalf("reserved bytes after release = %d", got)
	}
}

func TestPolicyAccountsForUnattributedGrowthDuringActiveWork(t *testing.T) {
	sample := Sample{Used: 100, Ceiling: 1000}
	policy := NewPolicy(1000, 100, time.Second, func() (Sample, error) { return sample, nil }, time.Now)
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	active, err := policy.Reserve(500)
	if err != nil {
		t.Fatal(err)
	}
	defer active.Release()
	// A background allocation raised the physical reading while the first
	// forecast remains live. Admission must conservatively cover both.
	sample.Used = 400
	if err := policy.Refresh(); err != nil {
		t.Fatal(err)
	}
	if _, err := policy.Reserve(100); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("unattributed growth should stop admission, got %v", err)
	}
}

func TestPolicyRunRefreshesOnEveryTick(t *testing.T) {
	var value atomic.Int64
	value.Store(100)
	observed := make(chan int64, 2)
	policy := NewPolicy(1000, 100, time.Second, func() (Sample, error) {
		used := value.Load()
		observed <- used
		return Sample{Used: used, Ceiling: 1000}, nil
	}, time.Now)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ticks := make(chan time.Time)
	done := make(chan struct{})
	go func() {
		policy.Run(ctx, ticks)
		close(done)
	}()
	if used := <-observed; used != 100 {
		t.Fatalf("initial sample = %d", used)
	}
	value.Store(950)
	ticks <- time.Now()
	if used := <-observed; used != 950 {
		t.Fatalf("tick sample = %d", used)
	}
	// The next unbuffered tick is accepted only after the first refresh
	// published its reading.
	ticks <- time.Now()
	if _, err := policy.Reserve(1); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("pressure after periodic refresh = %v", err)
	}
	cancel()
	<-done
}
