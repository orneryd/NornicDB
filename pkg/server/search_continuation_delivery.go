package server

import (
	"errors"
	"io"
	"net/http"

	"github.com/orneryd/nornicdb/pkg/lifecycle"
	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type searchDeliveryWriter struct {
	http.ResponseWriter
	delivery  *resultstream.Delivery
	wrotePage bool
}

func (w *searchDeliveryWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *searchDeliveryWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		w.delivery.Fail()
	} else if w.delivery.ObserveWritten(data) {
		w.wrotePage = true
	}
	return n, err
}
func (w *searchDeliveryWriter) WriteHeader(status int) {
	if status >= http.StatusBadRequest {
		w.delivery.Fail()
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *searchDeliveryWriter) FlushError() error {
	err := http.NewResponseController(w.ResponseWriter).Flush()
	if err != nil && !errors.Is(err, http.ErrNotSupported) {
		w.delivery.Fail()
	}
	return err
}
func (w *searchDeliveryWriter) Flush() { _ = w.FlushError() }

// All HTTP producers share the final response boundary, outside any timeout
// buffers. This includes Cypher transactions, GraphQL and tool adapters as well
// as the direct search route.
func continuationDeliveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		delivery := &resultstream.Delivery{}
		// A request cancellation fails the delivery; every exit settles that
		// callback before the final Fail.
		stopFail := lifecycle.AfterFuncJoined(r.Context(), delivery.Fail)
		defer func() {
			stopFail()
			delivery.Fail()
		}()
		r = r.WithContext(resultstream.WithDelivery(r.Context(), delivery))
		writer := &searchDeliveryWriter{ResponseWriter: w, delivery: delivery}
		next.ServeHTTP(writer, r)
		if writer.wrotePage && delivery.Pending() {
			_ = writer.FlushError()
		}
		stopFail()
		if r.Context().Err() == nil {
			delivery.DeliveredWritten()
		}
	})
}
