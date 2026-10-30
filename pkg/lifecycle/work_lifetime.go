package lifecycle

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
)

// WorkLifetime is the admission and actual-completion owner shared by HTTP
// services. A caller deadline never discharges accepted work. The zero value is
// ready to use; close admission, signal cancellation, then join Quiescent.
type WorkLifetime struct {
	once      sync.Once
	mu        sync.Mutex
	closing   bool
	active    int
	quiescent chan struct{}
	ctx       context.Context
	cancel    context.CancelFunc
	outcomes  Outcomes
}
type workKey struct{}
type workTicket struct {
	owner    *WorkLifetime
	active   bool
	request  *workTicket
	complete []func()
}

// NewWorkLifetime creates the same admission/completion owner with one parent-
// bound cancellation source. The zero value keeps its process-root behavior.
func NewWorkLifetime(parent context.Context) *WorkLifetime {
	requireContext("NewWorkLifetime", parent)
	w := &WorkLifetime{}
	w.initContext(parent)
	return w
}
func (w *WorkLifetime) init() { w.initContext(context.Background()) }
func (w *WorkLifetime) initContext(parent context.Context) {
	w.once.Do(func() { w.quiescent = make(chan struct{}); w.ctx, w.cancel = context.WithCancel(parent) })
}

// Context is canceled by Cancel after admission is closed.
func (w *WorkLifetime) Context() context.Context { w.init(); return w.ctx }

// Cancel signals accepted work; it does not declare it completed.
func (w *WorkLifetime) Cancel() { w.init(); w.cancel() }

// Quiescent closes only after admission is closed and actual users return.
func (w *WorkLifetime) Quiescent() <-chan struct{} { w.init(); return w.quiescent }

// RecordError retains an accepted lifetime's completion outcome.
func (w *WorkLifetime) RecordError(operation string, err error) { w.outcomes.Record(operation, err) }

// Err returns the outcomes collected so far; WaitClosed returns the final set.
func (w *WorkLifetime) Err() error { return w.outcomes.Err() }

// Close seals admission, signals stop and joins actual completion. An active
// worker must return its ticket before its caller joins this same lifetime.
func (w *WorkLifetime) Close() error { return w.CloseContext(context.Background()) }

// CloseContext bounds only the caller's wait. Accepted work retains its ticket
// and reports its outcome until actual completion, also after this returns.
func (w *WorkLifetime) CloseContext(ctx context.Context) error {
	w.CloseAdmission()
	w.Cancel()
	return w.WaitClosed(ctx)
}

// WaitClosed joins a lifetime whose admission has already been closed.
func (w *WorkLifetime) WaitClosed(ctx context.Context) error {
	requireContext("WorkLifetime.WaitClosed", ctx)
	select {
	case <-w.Quiescent():
		return w.Err()
	case <-ctx.Done():
		return compose.Outcomes(w.Err(), ctx.Err())
	}
}

// requireContext rejects a missing context. The owner never substitutes one:
// a nil context reaching it is a construction defect in its caller.
func requireContext(operation string, ctx context.Context) {
	if ctx == nil {
		panic("lifecycle: " + operation + " requires a non-nil context")
	}
}

// CloseAdmission seals roots. An active parent can still enroll its real child.
// It reports whether this call closed admission, so one caller owns the stop.
func (w *WorkLifetime) CloseAdmission() bool {
	w.init()
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closing {
		return false
	}
	w.closing = true
	if w.active == 0 {
		close(w.quiescent)
	}
	return true
}

// Closing reports whether admission has been closed.
func (w *WorkLifetime) Closing() bool {
	w.init()
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.closing
}

// Acquire enrolls before launch and returns its actual-completion release.
func (w *WorkLifetime) Acquire(ctx context.Context) (context.Context, func(), bool) {
	return w.acquire(ctx, false)
}
func (w *WorkLifetime) acquire(ctx context.Context, request bool) (context.Context, func(), bool) {
	w.init()
	requireContext("WorkLifetime.Acquire", ctx)
	parent, _ := ctx.Value(workKey{}).(*workTicket)
	w.mu.Lock()
	if w.closing && (parent == nil || parent.owner != w || !parent.active) {
		w.mu.Unlock()
		return ctx, func() {}, false
	}
	ticket := &workTicket{owner: w, active: true}
	if request {
		ticket.request = ticket
	} else if parent != nil && parent.owner == w {
		ticket.request = parent.request
	}
	w.active++
	w.mu.Unlock()
	return context.WithValue(ctx, workKey{}, ticket), func() {
		w.mu.Lock()
		if !ticket.active {
			w.mu.Unlock()
			return
		}
		ticket.active = false
		callbacks := ticket.complete
		ticket.complete = nil
		w.mu.Unlock()
		for _, complete := range callbacks {
			complete()
		}
		w.mu.Lock()
		w.active--
		if w.closing && w.active == 0 {
			close(w.quiescent)
		}
		w.mu.Unlock()
	}, true
}

// Handler owns the real request through outer buffered response publication.
func (w *WorkLifetime) Handler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		ctx, release, ok := w.acquire(r.Context(), true)
		if !ok {
			out.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer release()
		next.ServeHTTP(out, r.WithContext(ctx))
	})
}

// RetainHandler enrolls TimeoutHandler's child before its goroutine can run,
// preserving the worker's own cancellation/deadline when it eventually starts.
// A request this lifetime has not admitted is enrolled as its own request.
func (w *WorkLifetime) RetainHandler(next http.Handler, r *http.Request) http.Handler {
	parent, _ := r.Context().Value(workKey{}).(*workTicket)
	if parent == nil || parent.owner != w {
		return w.Handler(next)
	}
	ctx, release, ok := w.Acquire(r.Context())
	ticket := ctx.Value(workKey{})
	return http.HandlerFunc(func(out http.ResponseWriter, r *http.Request) {
		if !ok {
			out.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		defer release()
		next.ServeHTTP(out, r.WithContext(context.WithValue(r.Context(), workKey{}, ticket)))
	})
}

// OnRequestComplete transfers a resource share to the outer response publisher
// of a request this lifetime admitted; an already-finished request runs
// complete immediately. The callback always runs outside host state locks.
func (w *WorkLifetime) OnRequestComplete(ctx context.Context, complete func()) error {
	ticket, _ := ctx.Value(workKey{}).(*workTicket)
	if ticket == nil || ticket.owner != w || ticket.request == nil {
		return errors.New("request is not admitted by this lifetime")
	}
	root := ticket.request
	w.mu.Lock()
	if root.active {
		root.complete = append(root.complete, complete)
		w.mu.Unlock()
		return nil
	}
	w.mu.Unlock()
	complete()
	return nil
}
