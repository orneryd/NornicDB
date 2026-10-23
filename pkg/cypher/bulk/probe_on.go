//go:build cancellationprobe

package bulk

import "sync"

// The probe build counts, for each checkpoint, the owner's checks after its
// statement was canceled: the work a pass does after the cancellation. The
// owner observes the statement once per Stride checks, so no
// pass may make more than that many. It can also cancel a statement at a
// chosen check, in the middle of a pass.
var probe struct {
	mu sync.Mutex
	// since is each canceled checkpoint's index at its first check after the
	// cancellation (a reused address has a lower index: a new checkpoint).
	since map[*Checkpoint]int
	most  int
	// left checks remain before fire runs.
	left int
	fire func()
}

func probeCheck(c *Checkpoint) {
	probe.mu.Lock()
	var fire func()
	if probe.fire != nil {
		if probe.left--; probe.left <= 0 {
			fire, probe.fire = probe.fire, nil
		}
	}
	probe.mu.Unlock()
	if fire != nil {
		fire()
	}
	if c.ctx == nil || c.ctx.Err() == nil {
		return
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if probe.since == nil {
		probe.since = map[*Checkpoint]int{}
	}
	since, seen := probe.since[c]
	if !seen || c.index < since {
		since = c.index
		probe.since[c] = since
	}
	if checks := c.index - since + 1; checks > probe.most {
		probe.most = checks
	}
}

// ProbeArm runs fire at the after-th check of the owner from now
// (in the goroutine that makes it), once; a later arm replaces it.
func ProbeArm(after int, fire func()) {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	probe.left, probe.fire = after, fire
}

// ProbeMost returns the most checks one pass made after its
// statement was canceled since the last call, disarms the probe and resets
// the count.
func ProbeMost() int {
	probe.mu.Lock()
	defer probe.mu.Unlock()
	most := probe.most
	probe.most, probe.since, probe.fire = 0, nil, nil
	return most
}

// ProbeInterval is the owner's observation interval.
const ProbeInterval = Stride
