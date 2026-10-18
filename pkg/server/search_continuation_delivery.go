package server

import (
	"context"
	"net/http"
	"sync"
	"time"
)

type searchDeliveryKey struct{}

// TimeoutHandler buffers the response before writing to the connection. Keep
// delivery failure outside that buffer so an undelivered qid is discarded too.
type searchContinuationDelivery struct {
	mu sync.Mutex
	failed bool
	discard func()
}

func (d *searchContinuationDelivery) register(discard func()) {
	d.mu.Lock()
	failed := d.failed
	if !failed { d.discard = discard }
	d.mu.Unlock()
	if failed { discard() }
}

func (d *searchContinuationDelivery) fail() {
	d.mu.Lock()
	d.failed = true
	discard := d.discard
	d.discard = nil
	d.mu.Unlock()
	if discard != nil { discard() }
}

type searchDeliveryWriter struct {
	http.ResponseWriter
	delivery *searchContinuationDelivery
}

func (w searchDeliveryWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if err != nil { w.delivery.fail() }
	return n, err
}

func (w searchDeliveryWriter) WriteHeader(status int) {
	if status != http.StatusOK { w.delivery.fail() }
	w.ResponseWriter.WriteHeader(status)
}

func serveSearchWithTimeout(w http.ResponseWriter, r *http.Request, next http.Handler, timeout time.Duration) {
	delivery := &searchContinuationDelivery{}
	stop := context.AfterFunc(r.Context(), delivery.fail)
	defer stop()
	r = r.WithContext(context.WithValue(r.Context(), searchDeliveryKey{}, delivery))
	http.TimeoutHandler(next, timeout, "request timeout: search busy").ServeHTTP(searchDeliveryWriter{w, delivery}, r)
}
