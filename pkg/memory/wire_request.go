package memory

import (
	"context"
	"net/http"
	"sync"
)

type wireRequestKey struct{}

// Context passes the same owner to delegated consumers without transferring
// cleanup. Only the enclosing Run releases the request's reservation.
func (w *WireRequest) Context(ctx context.Context) context.Context {
	if w == nil {
		return ctx
	}
	return context.WithValue(ctx, wireRequestKey{}, w)
}

// WireRequestFromContext borrows an enclosing request's reservation owner.
func WireRequestFromContext(ctx context.Context) *WireRequest {
	w, _ := ctx.Value(wireRequestKey{}).(*WireRequest)
	return w
}

// WireRequest owns a pre-decode reservation across reads, setup and cleanup.
// Run defers cleanup on return or panic; retained workers finish their credit.
// Move explicitly transfers
// ownership to a queue; Reset ends one message in a longer-lived transport.
type WireRequest struct {
	mu           sync.Mutex
	policy       *Policy
	lease        *Lease
	closed       bool
	holds        int
	closePending bool
}

// NewWireRequest starts an empty, lazily reserved request. Disabled admission
// uses a nil owner so the existing fast path requires no allocation.
func NewWireRequest(policy *Policy) *WireRequest {
	if policy == nil || policy.limit <= 0 {
		return nil
	}
	return &WireRequest{policy: policy}
}

// Run keeps the reservation through the complete consumer callback.
func (w *WireRequest) Run(consume func() error) error {
	defer w.Close()
	return consume()
}

// WireSize adds actual wire components without wrapping into a small forecast.
func WireSize(parts ...int64) int64 {
	var total int64
	for _, part := range parts {
		if part < 0 || part > (1<<63-1)-total {
			return 1<<63 - 1
		}
		total += part
	}
	return total
}

// Grow reserves the full known wire size before it is decoded or accumulated.
func (w *WireRequest) Grow(parts ...int64) error {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return ErrLimitExceeded
	}
	size := WireSize(parts...)
	if w.lease != nil {
		return w.lease.GrowWireRequest(size, size)
	}
	lease, err := w.policy.ReserveWireRequest(size, size)
	if err == nil {
		w.lease = lease
	}
	return err
}

// WireLimit is the current capacity for all wire components in this request.
func (w *WireRequest) WireLimit() (int64, error) {
	if w == nil {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, ErrLimitExceeded
	}
	return w.lease.WireLimit()
}

// Move transfers the reservation once, leaving the caller's deferred Close safe.
func (w *WireRequest) Move() *WireRequest {
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return nil
	}
	next := &WireRequest{policy: w.policy, lease: w.lease}
	w.lease = nil
	w.closed = true
	return next
}

// Reset releases a completed frame before waiting for another one.
func (w *WireRequest) Reset() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lease.Release()
	w.lease = nil
}

// Close ends the outer scope; any retained decoder worker must also finish
// before the reservation is returned. Cleanup is safe to repeat concurrently.
func (w *WireRequest) Close() {
	if w == nil {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	w.closePending = true
	w.closeIfIdle()
}

// RetainHandler pins this owner before an asynchronous HTTP worker is started.
// Its credit remains available for body reads after a timeout response returns,
// and is released only after both the outer request and worker have ended.
func (w *WireRequest) RetainHandler(next http.Handler) http.Handler {
	if w == nil {
		return next
	}
	w.mu.Lock()
	w.holds++
	w.mu.Unlock()
	return http.HandlerFunc(func(writer http.ResponseWriter, r *http.Request) {
		defer func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.holds--
			w.closeIfIdle()
		}()
		next.ServeHTTP(writer, r)
	})
}

// Caller holds mu. Close is requested by the outer Run, never by a borrower.
func (w *WireRequest) closeIfIdle() {
	if w.closePending && w.holds == 0 {
		w.closed = true
		w.lease.Release()
		w.lease = nil
	}
}

type httpMetadataKey struct{}

// HTTPRequestWireBytes counts the request line, host, headers and query string.
// A body-size cap is independent of these bytes and must not clamp them.
func HTTPRequestWireBytes(r *http.Request) int64 {
	if size, ok := r.Context().Value(httpMetadataKey{}).(int64); ok {
		return size
	}
	uri := r.RequestURI
	if uri == "" && r.URL != nil {
		uri = r.URL.RequestURI()
	}
	size := WireSize(int64(len(r.Method)), int64(len(uri)), int64(len(r.Proto)), 4)
	if r.Host != "" {
		size = WireSize(size, int64(len(r.Host)), 8)
	}
	return WireSize(size, HTTPHeaderWireBytes(r.Header), 2)
}

// HTTPHeaderWireBytes counts decoded header fields. Request trailers may only
// be inspected after Body.Read returns EOF, when net/http has published them.
func HTTPHeaderWireBytes(header http.Header) int64 {
	var size int64
	for key, values := range header {
		for _, value := range values {
			size = WireSize(size, int64(len(key)), int64(len(value)), 4)
		}
	}
	return size
}
