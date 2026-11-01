// Author: #1636 lane (Claude). Planted E5 controls: owner code that runs
// foreign code while holding its own mutex (Outcomes.Record shape).
package lock

import (
	"errors"
	"sync"
)

type plantedOutcomes struct {
	mu  sync.Mutex
	err error
}

// E5: a foreign static call (errors.Is reaches error methods) under the lock.
func (o *plantedOutcomes) record(err error) {
	o.mu.Lock()
	if !errors.Is(o.err, err) {
		o.err = err
	}
	o.mu.Unlock()
}

// E5: an interface method under the lock.
func (o *plantedOutcomes) describe() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err.Error()
}

var _ = []any{(&plantedOutcomes{}).record, (&plantedOutcomes{}).describe}
