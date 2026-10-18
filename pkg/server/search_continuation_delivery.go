package server

import (
	"context"
	"net/http"
	"time"

	"github.com/orneryd/nornicdb/pkg/resultstream"
)

type searchDeliveryWriter struct {
	http.ResponseWriter
	delivery *resultstream.Delivery
}

func (w searchDeliveryWriter) Write(data []byte) (int, error) {
	n, err := w.ResponseWriter.Write(data)
	if err != nil {
		w.delivery.Fail()
	}
	return n, err
}

func (w searchDeliveryWriter) WriteHeader(status int) {
	if status != http.StatusOK {
		w.delivery.Fail()
	}
	w.ResponseWriter.WriteHeader(status)
}

func serveSearchWithTimeout(w http.ResponseWriter, r *http.Request, next http.Handler, timeout time.Duration) {
	delivery := &resultstream.Delivery{}
	stop := context.AfterFunc(r.Context(), delivery.Fail)
	defer stop()
	r = r.WithContext(resultstream.WithDelivery(r.Context(), delivery))
	http.TimeoutHandler(next, timeout, "request timeout: search busy").ServeHTTP(searchDeliveryWriter{w, delivery}, r)
	stop()
	delivery.Delivered()
}
