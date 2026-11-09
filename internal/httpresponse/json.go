// Package httpresponse owns common HTTP JSON response framing for API servers.
package httpresponse

import (
	"github.com/orneryd/nornicdb/internal/jsontext"
	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"net/http"
)

// WriteJSON writes the supplied status and standard JSON body. As at the
// existing HTTP boundaries, headers are committed before encoding and an
// encoding/write failure does not attempt a second response.
func WriteJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = jsontext.NewEncoder(writer).Encode(value)
}

// WritePropertyJSON binds the property-number domain before the shared HTTP
// response owner writes headers and standard JSON. It adds no response framing
// or error policy of its own.
func WritePropertyJSON(writer http.ResponseWriter, status int, value any) {
	WriteJSON(writer, status, valueconvert.PropertyJSON.Prepare(value))
}
