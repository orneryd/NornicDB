package lifecycle

import (
	"fmt"
	"sync"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
)

// Outcomes retains the completion outcomes of an owner's work, graceful and
// forced, with attribution. The zero value is ready to use. WorkLifetime embeds
// it; owners that only aggregate sibling results use it directly.
type Outcomes struct {
	mu  sync.Mutex
	err error
}

// Record keeps err, prefixed with operation when given. Attribution is formatted
// before taking the publication lock: an extension's Error method may itself
// call back into its host.
func (o *Outcomes) Record(operation string, err error) {
	if err == nil {
		return
	}
	if operation != "" {
		err = fmt.Errorf("%s: %w", operation, err)
	}
	o.mu.Lock()
	o.err = compose.Outcomes(o.err, err)
	o.mu.Unlock()
}

// Err returns the outcomes recorded so far.
func (o *Outcomes) Err() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.err
}
