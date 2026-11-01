// Author: #1636 lane (Claude). Planted enrollment-census controls in the
// reviewer's shapes. Never product code; overlaid only for the planted run.
package zzenrollplant

import (
	"context"
	"sync"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
)

// E1: the owner kept beside a private wait group (replicator shape).
type plantedReplicator struct {
	work   lifecycle.WorkLifetime
	wg     sync.WaitGroup
	closed bool
}

// E1: the owner beside a private stop channel reached through an interface.
type plantedTransport struct {
	work   *lifecycle.WorkLifetime
	stopCh chan struct{}
	join   interface {
		Add(int)
		Done()
		Wait()
	}
}

// E3: a stop that returns early on its own flag and never joins the owner.
func (r *plantedReplicator) Shutdown() error {
	if r.closed {
		return nil
	}
	r.closed = true
	r.wg.Wait()
	return nil
}

// E3: a Close that disposes without the owner's protocol.
func (t *plantedTransport) Close() error { close(t.stopCh); return nil }

// E2: optional enrollment, success with no owner (model beginUse shape).
func plantedBeginUse(owner *lifecycle.WorkLifetime) (func(), error) {
	if owner == nil {
		return func() {}, nil
	}
	_, release, _ := owner.Acquire(context.Background())
	return release, nil
}

type plantedManager struct{ owned *lifecycle.WorkLifetime }

// E2: optional enrollment reported as accepted by an owner-holding type.
func (m *plantedManager) retain() (func(), bool) { return func() {}, true }

// E2: owner identity reset while its epoch may still run work (Heimdall shape).
func (m *plantedManager) reset() { m.owned = nil }

// E6: a failed-start cleanup outcome discarded (StartRuntime shape).
func plantedFailedStart(r *plantedReplicator) { _ = r.Shutdown() }

// plantedRuntime is a module interface an owner-holding type implements.
type plantedRuntime interface{ Shutdown() error }

// E6: the same discard through the interface (StartRuntime's Replicator shape).
func plantedStartRuntime(r plantedRuntime) { _ = r.Shutdown() }

var _ = []any{plantedReplicator{}, plantedTransport{}, plantedBeginUse, (&plantedManager{}).retain, (&plantedManager{}).reset, plantedFailedStart, plantedStartRuntime}
