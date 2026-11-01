package load

// apoc.load.binary and apoc.load.stream register one canonical byte loader,
// Binary, while Go callers keep the reader API Stream. These checks pin
// Binary's contract at the Go boundary those registrations rely on: a source
// with zero or one config map, the caller's lifetime for both the open and the
// body read, a closed body on every path, denial before any access, and
// rejection of a second config map before any I/O. config is the optional
// argument both Cypher names accept and, as before, has no other effect.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/require"
)

const streamResultPayload = "stream-payload"

// streamResultBody is a source body that records its Close and can fail its
// read. With hold set, its first read stays in flight until the request
// context ends, as a real transport body does.
type streamResultBody struct {
	payload string
	readErr error
	ctx     context.Context
	hold    chan struct{}
	entered chan struct{}
	once    sync.Once
	read    bool
	mu      sync.Mutex
	closes  int
}

func (b *streamResultBody) Read(buffer []byte) (int, error) {
	if b.hold != nil {
		b.once.Do(func() { close(b.entered) })
		<-b.ctx.Done()
		return 0, b.ctx.Err()
	}
	if b.readErr != nil {
		return 0, b.readErr
	}
	if b.read {
		return 0, io.EOF
	}
	b.read = true
	return copy(buffer, b.payload), nil
}

func (b *streamResultBody) Close() error {
	b.mu.Lock()
	b.closes++
	b.mu.Unlock()
	return nil
}

func (b *streamResultBody) closeCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.closes
}

// streamResultTransport answers with one recorded body per request and keeps
// the request so a case can check what the loader actually sent.
type streamResultTransport struct {
	payload string
	readErr error
	hold    chan struct{}
	entered chan struct{}
	mu      sync.Mutex
	bodies  []*streamResultBody
	reqs    []*http.Request
}

func (f *streamResultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	body := &streamResultBody{
		payload: f.payload,
		readErr: f.readErr,
		ctx:     req.Context(),
		hold:    f.hold,
		entered: f.entered,
	}
	f.mu.Lock()
	f.bodies = append(f.bodies, body)
	f.reqs = append(f.reqs, req)
	f.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       body,
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func (f *streamResultTransport) issued() []*streamResultBody {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*streamResultBody(nil), f.bodies...)
}

func (f *streamResultTransport) requests() []*http.Request {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*http.Request(nil), f.reqs...)
}

// streamResultCapability binds an explicitly allowed remote APOC policy over
// the recording transport.
func streamResultCapability(transport *streamResultTransport) apocio.Capability {
	return apocio.NewPolicy(apocio.Config{
		AllowRemoteURLAccess: true,
		RemoteURLAllowlist:   []string{"example.com"},
		HostResolver:         publicResolver,
		HTTPClient:           &http.Client{Transport: transport},
	}).Capability()
}

// streamResultFileCapability binds an import root holding the payload.
func streamResultFileCapability(t *testing.T) apocio.Capability {
	t.Helper()
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "payload.txt"), []byte(streamResultPayload), 0o600))
	return apocio.NewPolicy(apocio.Config{AllowImportFileAccess: true, FileAccessRoot: root}).Capability()
}

// streamResultFileContext returns a caller context that may read the payload.
func streamResultFileContext(t *testing.T) context.Context {
	t.Helper()
	return callerContext(streamResultFileCapability(t))
}

// TestBinaryReturnsReadBytesInsideCallerLifetime is the registrations'
// contract: the source is read and closed while the caller is live, and the
// bytes survive that context's end.
func TestBinaryReturnsReadBytesInsideCallerLifetime(t *testing.T) {
	transport := &streamResultTransport{payload: streamResultPayload}
	ctx, cancel := context.WithCancel(callerContext(streamResultCapability(transport)))

	data, err := Binary(ctx, "https://example.com/stream", map[string]interface{}{"format": "json"})
	require.NoError(t, err)
	require.Equal(t, streamResultPayload, string(data))

	bodies := transport.issued()
	require.Len(t, bodies, 1)
	require.Equal(t, 1, bodies[0].closeCount(), "the loaded body is closed before Binary returns")

	cancel()
	require.Equal(t, streamResultPayload, string(data), "the returned bytes are not bound to the caller's context")

	// A canceled caller cannot load another source.
	_, err = Binary(ctx, "https://example.com/stream")
	require.ErrorIs(t, err, context.Canceled)
	require.Len(t, transport.issued(), 1, "a canceled caller opens no source")
}

// TestBinaryAcceptsZeroOrOneConfig keeps apoc.load.stream's existing optional
// config argument: zero or one map is accepted and has no other effect.
func TestBinaryAcceptsZeroOrOneConfig(t *testing.T) {
	t.Run("go-call-without-config", func(t *testing.T) {
		transport := &streamResultTransport{payload: streamResultPayload}
		data, err := Binary(callerContext(streamResultCapability(transport)), "https://example.com/stream")
		require.NoError(t, err)
		require.Equal(t, streamResultPayload, string(data))
	})

	t.Run("config-is-ignored", func(t *testing.T) {
		transport := &streamResultTransport{payload: streamResultPayload}
		ctx := callerContext(streamResultCapability(transport))
		configs := []map[string]interface{}{
			nil,
			{},
			{"format": "json", "headers": map[string]string{"X-Test": "1"}},
		}
		for _, config := range configs {
			data, err := Binary(ctx, "https://example.com/stream", config)
			require.NoError(t, err, "config %v", config)
			require.Equal(t, streamResultPayload, string(data))
		}
		for _, req := range transport.requests() {
			require.Empty(t, req.Header.Get("X-Test"), "config has no effect on the request")
		}
	})

	t.Run("import-file-source", func(t *testing.T) {
		ctx := streamResultFileContext(t)
		for _, call := range []struct {
			name string
			load func() ([]byte, error)
		}{
			{name: "one-argument", load: func() ([]byte, error) { return Binary(ctx, "payload.txt") }},
			{name: "two-argument", load: func() ([]byte, error) {
				return Binary(ctx, "payload.txt", map[string]interface{}{"format": "json"})
			}},
		} {
			data, err := call.load()
			require.NoError(t, err, call.name)
			require.Equal(t, streamResultPayload, string(data), call.name)
		}
	})
}

// TestBinaryRejectsMoreThanOneConfigBeforeAccess keeps the argument boundary:
// at most one config map, and a second one fails before any I/O.
func TestBinaryRejectsMoreThanOneConfigBeforeAccess(t *testing.T) {
	transport := &streamResultTransport{payload: streamResultPayload}
	ctx := callerContext(streamResultCapability(transport))

	data, err := Binary(ctx, "https://example.com/stream",
		map[string]interface{}{"format": "json"}, map[string]interface{}{"format": "csv"})
	require.ErrorContains(t, err, "at most one config map, got 2")
	require.Empty(t, data, "a rejected call publishes no payload")
	require.Empty(t, transport.issued(), "a rejected call opens no source")
}

// TestBinaryClosesBodyOnReadFailure proves a failed read still releases the
// body and publishes no partial payload.
func TestBinaryClosesBodyOnReadFailure(t *testing.T) {
	readErr := errors.New("source read failed")
	transport := &streamResultTransport{payload: streamResultPayload, readErr: readErr}

	data, err := Binary(callerContext(streamResultCapability(transport)), "https://example.com/file")
	require.ErrorIs(t, err, readErr)
	require.Empty(t, data, "a failed read publishes no payload")

	bodies := transport.issued()
	require.Len(t, bodies, 1)
	require.Equal(t, 1, bodies[0].closeCount(), "a failed read still closes the body")
}

// TestBinaryDeniedBeforeAccess keeps the denied default: no capability and a
// zero capability both fail before any source is opened.
func TestBinaryDeniedBeforeAccess(t *testing.T) {
	transport := &streamResultTransport{payload: streamResultPayload}

	_, err := Binary(context.Background(), "https://example.com/file")
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)

	_, err = Binary(apocio.WithCapability(callerContext(streamResultCapability(transport)), apocio.Capability{}), "https://example.com/file")
	require.ErrorIs(t, err, apocio.ErrCapabilityMissing)
	require.Empty(t, transport.issued(), "a denied load must not open a source")
}

// TestBinaryReadObservesCallerLifetime covers a caller cancel func and a caller
// deadline that end the load while its source body is being read: the read
// fails with that context's error and the opened body is closed.
func TestBinaryReadObservesCallerLifetime(t *testing.T) {
	cases := []struct {
		name   string
		caller func(context.Context) (context.Context, context.CancelFunc)
		expire func(context.CancelFunc)
		want   error
	}{
		{
			name:   "cancel-during-read",
			caller: func(parent context.Context) (context.Context, context.CancelFunc) { return context.WithCancel(parent) },
			expire: func(cancel context.CancelFunc) { cancel() },
			want:   context.Canceled,
		},
		{
			name: "deadline-during-read",
			caller: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(parent, time.Minute)
			},
			// The fake clock advances once every goroutine is blocked, so the
			// caller's deadline expires while the body read is in flight.
			expire: func(context.CancelFunc) { time.Sleep(2 * time.Minute) },
			want:   context.DeadlineExceeded,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				transport := &streamResultTransport{
					payload: streamResultPayload,
					hold:    make(chan struct{}),
					entered: make(chan struct{}),
				}
				ctx, cancel := testCase.caller(callerContext(streamResultCapability(transport)))
				defer cancel()

				type outcome struct {
					data []byte
					err  error
				}
				done := make(chan outcome, 1)
				go func() {
					data, err := Binary(ctx, "https://example.com/stream")
					done <- outcome{data: data, err: err}
				}()

				select {
				case <-transport.entered:
				case <-time.After(time.Second):
					t.Fatal("the source read never started")
				}
				testCase.expire(cancel)

				select {
				case got := <-done:
					require.ErrorIs(t, got.err, testCase.want)
					require.Empty(t, got.data, "a canceled load publishes no payload")
				case <-time.After(time.Second):
					t.Fatalf("the canceled load did not return within %s", time.Second)
				}

				bodies := transport.issued()
				require.Len(t, bodies, 1)
				require.Equal(t, 1, bodies[0].closeCount(), "the source body is closed when the load fails")
			})
		})
	}
}

// TestStreamReaderIsUsableWhileCallerContextLives keeps the Go reader contract:
// the caller reads and closes the returned reader while its context is live,
// and the reader stops at that context's end.
func TestStreamReaderIsUsableWhileCallerContextLives(t *testing.T) {
	capability := streamResultFileCapability(t)

	t.Run("live-context-reads-and-closes", func(t *testing.T) {
		ctx, cancel := context.WithCancel(callerContext(capability))
		defer cancel()

		reader, err := Stream(ctx, "payload.txt", map[string]interface{}{"format": "json"})
		require.NoError(t, err)
		require.NotNil(t, reader)
		data, err := io.ReadAll(reader)
		require.NoError(t, err)
		require.Equal(t, streamResultPayload, string(data))
		require.NoError(t, reader.Close())
	})

	t.Run("caller-cancel-ends-the-open-reader", func(t *testing.T) {
		ctx, cancel := context.WithCancel(callerContext(capability))
		defer cancel()

		reader, err := Stream(ctx, "payload.txt", nil)
		require.NoError(t, err)
		defer reader.Close()

		cancel()
		_, err = io.ReadAll(reader)
		require.ErrorIs(t, err, context.Canceled, "an open reader reads on the caller's context")

		// The same canceled caller cannot open another source.
		_, err = Stream(ctx, "payload.txt", nil)
		require.ErrorIs(t, err, context.Canceled)
	})
}
