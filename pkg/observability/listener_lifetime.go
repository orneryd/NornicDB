package observability

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/orneryd/nornicdb/pkg/errors/compose"
	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/util"
)

// listenerLifetime extends the component's existing Start/Shutdown owner. The
// component Start frame joins actual retirement; Shutdown bounds only its caller.
// Both command HTTP services share this protocol and WorkLifetime admission.
type listenerLifetime struct {
	initOnce   sync.Once
	mu         sync.Mutex
	started    bool
	work       lifecycle.WorkLifetime
	server     *http.Server
	listener   *retirementListener
	beforeStop func(context.Context) error
	stopped    lifecycle.OnceCompletion // the retirement driver, run once
	forced     lifecycle.OnceCompletion // the caller deadline that forced the transport
}

func (c *listenerLifetime) init(server *http.Server, listener net.Listener, beforeStop func(context.Context) error) {
	c.initOnce.Do(func() {
		c.server, c.beforeStop = server, beforeStop
		c.listener = &retirementListener{Listener: listener, owner: c}
		handler := server.Handler
		if handler == nil {
			handler = http.DefaultServeMux
		}
		server.Handler = c.work.Handler(handler)
	})
}

func (c *listenerLifetime) record(operation string, err error) {
	if err != nil && !util.ErrorIsOnly(err, net.ErrClosed) && !util.ErrorIsOnly(err, http.ErrServerClosed) {
		c.work.RecordError(operation, err)
	}
}

func (c *listenerLifetime) run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	serveCtx, release, accepted := c.work.Acquire(c.work.Context())
	if !accepted {
		return fmt.Errorf("observability listener is stopped")
	}
	c.mu.Lock()
	if c.started || c.work.Closing() {
		c.mu.Unlock()
		release()
		return fmt.Errorf("observability listener already started or stopped")
	}
	c.started = true
	// Children inherit the admitted Serve ticket. Metrics can still be scraped
	// while provider flush runs, after root admission is sealed (OQ4).
	c.server.BaseContext = func(net.Listener) context.Context { return serveCtx }
	c.mu.Unlock()
	served := make(chan struct{})
	go func() {
		defer release()
		c.record("observability HTTP serve", c.server.Serve(c.listener))
		close(served)
	}()
	select {
	case <-ctx.Done():
	case <-served:
	}
	// Preserve the component's graceful-drain budget, then keep its actual
	// lifetime joined even when that budget expired. The supervisor cannot
	// release command-owned resources while this Start frame is still active.
	budget, cancel := context.WithTimeout(context.WithoutCancel(ctx), lifecycle.ShutdownTimeout)
	waitErr := c.shutdown(budget)
	budgetErr := budget.Err()
	cancel()
	<-c.stopped.Done()
	if budgetErr != nil {
		return compose.Outcomes(waitErr, c.work.Err())
	}
	return c.work.Err()
}

func (c *listenerLifetime) beginStop(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	if !c.work.CloseAdmission() {
		return
	}
	c.work.Cancel()
	cleanup := context.WithoutCancel(ctx)
	c.stopped.Go(func() error {
		// Invocation precedes listener retirement (OQ4). A caller deadline
		// can force the transport while this retained foreign call finishes;
		// its real return is still collected before completion is published.
		providerDone := make(chan struct{})
		if c.beforeStop != nil {
			go func() {
				c.record("observability provider shutdown", c.beforeStop(cleanup))
				close(providerDone)
			}()
		} else {
			close(providerDone)
		}
		select {
		case <-providerDone:
		case <-c.forced.Done():
		}
		c.record("observability HTTP shutdown", util.ShutdownHTTPServer(retirementBudget{Context: cleanup, owner: c}, c.server))
		// Shutdown may race Serve registration or precede Start entirely.
		// The same listener owner closes the constructed resource exactly once.
		c.record("observability listener close", c.listener.Close())
		<-providerDone
		<-c.work.Quiescent()
		return nil
	})
}

func (c *listenerLifetime) shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.beginStop(ctx)
	select {
	case <-c.stopped.Done():
		return c.work.Err()
	case <-ctx.Done():
		cause := ctx.Err()
		c.forced.Go(func() error { return cause })
		return compose.Outcomes(c.work.Err(), cause)
	}
}

func (c *listenerLifetime) wait(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-c.stopped.Done():
		return c.work.Err()
	case <-ctx.Done():
		return compose.Outcomes(c.work.Err(), ctx.Err())
	}
}

// retirementBudget carries the owner's graceful/forced phase signal, not a
// second work lifetime or cancellation source. Values stay live for cleanup.
type retirementBudget struct {
	context.Context
	owner *listenerLifetime
}

func (b retirementBudget) Done() <-chan struct{} { return b.owner.forced.Done() }
func (b retirementBudget) Err() error            { return b.owner.forced.Err() }

type retirementListener struct {
	net.Listener
	completion lifecycle.OnceCompletion
	owner      *listenerLifetime
}

func (l *retirementListener) Close() error {
	return l.completion.Do(func() error {
		err := l.Listener.Close()
		l.owner.record("observability native listener close", err)
		return err
	})
}
