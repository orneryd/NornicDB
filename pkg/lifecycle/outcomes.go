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
	mu   sync.Mutex
	errs []error // in recording order; composed outside mu
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
	o.errs = append(o.errs, err)
	o.mu.Unlock()
}

// Err returns the outcomes recorded so far. Composition calls the errors' own
// Is/Unwrap methods, which may be extension code, so it runs outside mu.
func (o *Outcomes) Err() error {
	o.mu.Lock()
	errs := append([]error(nil), o.errs...)
	o.mu.Unlock()
	return compose.Outcomes(nil, errs...)
}
