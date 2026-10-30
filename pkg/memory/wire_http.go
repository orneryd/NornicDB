package memory

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"time"
)

// HTTPWireOptions preserves a caller's existing body cap independently of
// admission. Without LimitBody, streamed bytes are bounded only by the policy.
type HTTPWireOptions struct {
	MaxBodyBytes int64
	LimitBody    bool
	// MetadataOnly runs before middleware; body guards belong to the actual
	// decoder worker, which can outlive a timeout response.
	MetadataOnly bool
}

type httpReadControllerKey struct{}

// HandleHTTPRequest owns admission until the consumer actually returns, even
// after cancellation or upgrade. Delegated consumers borrow the context owner
// instead of creating another reservation. reject writes the caller's existing
// transient memory response, including failures discovered during body reads.
func HandleHTTPRequest(policy *Policy, w http.ResponseWriter, r *http.Request, options HTTPWireOptions, reject func(http.ResponseWriter, *http.Request, error), next http.Handler) {
	if policy == nil {
		next.ServeHTTP(w, r)
		return
	}
	if wire := WireRequestFromContext(r.Context()); wire != nil {
		serveAdmittedHTTPRequest(w, r, wire, options, reject, next)
		return
	}
	wire := NewWireRequest(policy)
	_ = wire.Run(func() error {
		hasBody := r.Body != nil && r.Body != http.NoBody
		metadata := HTTPRequestWireBytes(r)
		var bodySize int64
		if hasBody {
			bodySize = max(0, r.ContentLength)
			if r.ContentLength < 0 && options.LimitBody {
				bodySize = max(0, options.MaxBodyBytes)
			}
		}
		if err := wire.Grow(metadata, bodySize); err != nil {
			reject(w, r, err)
			return err
		}
		ctx := context.WithValue(wire.Context(r.Context()), httpMetadataKey{}, metadata)
		// Preserve read-deadline control before TimeoutHandler substitutes its
		// buffered writer. Only the actual body worker may use it to drain.
		ctx = context.WithValue(ctx, httpReadControllerKey{}, http.NewResponseController(w))
		r = r.WithContext(ctx)
		serveAdmittedHTTPRequest(w, r, wire, options, reject, next)
		return nil
	})
}

func serveAdmittedHTTPRequest(w http.ResponseWriter, r *http.Request, wire *WireRequest, options HTTPWireOptions, reject func(http.ResponseWriter, *http.Request, error), next http.Handler) {
	if options.MetadataOnly || r.Body == nil || r.Body == http.NoBody {
		next.ServeHTTP(w, r)
		return
	}
	guard := &wireResponseWriter{ResponseWriter: w, request: r, reject: reject}
	originalBody := r.Body
	body := originalBody
	if options.LimitBody {
		body = http.MaxBytesReader(w, body, options.MaxBodyBytes)
	}
	guardedBody := &wireBody{ReadCloser: body, request: r, wire: wire, metadata: HTTPRequestWireBytes(r), reject: func(err error) { guard.err = err }}
	r.Body = guardedBody
	next.ServeHTTP(guard, r)
	guard.finish()
	// The decoder has stopped at the existing cap. Preserve the error reply
	// on HTTP/1 instead of closing with unread TCP bytes (which can send RST).
	// Only drain an already-started oversized body, never a pre-read rejection.
	if guardedBody.overLimit && r.ProtoMajor == 1 {
		controller, _ := r.Context().Value(httpReadControllerKey{}).(*http.ResponseController)
		if controller == nil {
			controller = http.NewResponseController(w)
		}
		if controller.SetReadDeadline(time.Now().Add(time.Second)) == nil {
			// Flush only through the current writer; a timeout writer must keep
			// owning its buffered status rather than flushing an implicit 200.
			_ = http.NewResponseController(w).Flush()
			_, _ = io.Copy(io.Discard, originalBody)
		}
	}
}

// Bytes are charged before reaching a decoder. The initial metadata snapshot
// cannot shrink if a downstream handler consumes or rewrites request headers.
type wireBody struct {
	io.ReadCloser
	request   *http.Request
	wire      *WireRequest
	metadata  int64
	read      int64
	trailers  int64
	reject    func(error)
	overLimit bool
}

func (b *wireBody) Read(buffer []byte) (int, error) {
	n, err := b.ReadCloser.Read(buffer)
	var limitError *http.MaxBytesError
	if errors.As(err, &limitError) {
		b.overLimit = true
	}
	b.read = WireSize(b.read, int64(n))
	if err == io.EOF {
		b.trailers = HTTPHeaderWireBytes(b.request.Trailer)
	}
	if admission := b.wire.Grow(b.metadata, b.read, b.trailers); admission != nil {
		b.reject(admission)
		return 0, admission
	}
	return n, err
}

// Translate a reader rejection even if the decoder normally reports bad JSON.
type wireResponseWriter struct {
	http.ResponseWriter
	request *http.Request
	reject  func(http.ResponseWriter, *http.Request, error)
	err     error
	sent    bool
}

func (w *wireResponseWriter) finish() {
	if w.err != nil && !w.sent {
		w.sent = true
		w.reject(w.ResponseWriter, w.request, w.err)
	}
}
func (w *wireResponseWriter) WriteHeader(status int) {
	if w.err != nil {
		w.finish()
		return
	}
	w.ResponseWriter.WriteHeader(status)
}
func (w *wireResponseWriter) Write(data []byte) (int, error) {
	if w.err != nil {
		w.finish()
		return len(data), nil
	}
	return w.ResponseWriter.Write(data)
}
func (w *wireResponseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *wireResponseWriter) Flush() {
	w.finish()
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}
func (w *wireResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	return http.NewResponseController(w.ResponseWriter).Hijack()
}
