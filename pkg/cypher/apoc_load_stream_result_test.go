package cypher

// apoc.load.binary and apoc.load.stream are bundled scalar APOC plugin exports
// registered against one canonical handler, so both names return the source's
// bytes read and closed inside the live statement context: a Cypher value
// outlives the statement that produced it while a reader does not. These
// regressions drive the actual bundled registrations through public Execute, so
// re-registering a reader handler, or changing either name's accepted
// arguments, fails here even if the Go entry points still work.
//
// The Go byte-loader and reader contracts are covered in apoc/load.

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/orneryd/nornicdb/pkg/apocio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	streamResultPayload   = "stream-payload"
	streamResultDeadline  = time.Minute
	streamResultWaitLimit = time.Second
)

// streamResultNames are the two bundled Cypher names the plugin registers
// against the canonical byte loader.
var streamResultNames = []string{"apoc.load.binary", "apoc.load.stream"}

// streamResultQuery is the public call of one of the names: the source and the
// optional config map both names accept.
func streamResultQuery(name string) string {
	return "RETURN " + name + "($source, $config) AS value"
}

// streamResultTransport answers with one controlled body and records the
// request and whether that body was closed.
type streamResultTransport struct {
	mu       sync.Mutex
	requests int
	closed   int
	// hold, when non-nil, keeps the body's first read blocked until the
	// request context ends, so a test can cancel or expire a live read.
	hold    chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (f *streamResultTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	f.mu.Lock()
	f.requests++
	f.mu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Status:     "200 OK",
		Body:       &streamResultBody{transport: f, ctx: req.Context()},
		Header:     make(http.Header),
		Request:    req,
	}, nil
}

func (f *streamResultTransport) counts() (requests, closed int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests, f.closed
}

// streamResultBody is the loaded source's body: it honors its request context
// as a real transport body does, and records its Close.
type streamResultBody struct {
	transport *streamResultTransport
	ctx       context.Context
	read      bool
}

func (b *streamResultBody) Read(buffer []byte) (int, error) {
	if b.transport.hold != nil {
		b.transport.once.Do(func() { close(b.transport.entered) })
		select {
		case <-b.transport.hold:
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	if b.read {
		return 0, io.EOF
	}
	b.read = true
	return copy(buffer, streamResultPayload), nil
}

func (b *streamResultBody) Close() error {
	b.transport.mu.Lock()
	b.transport.closed++
	b.transport.mu.Unlock()
	return nil
}

// streamResultRemoteExecutor configures an executor for explicitly allowed
// remote APOC loads against the recording transport.
func streamResultRemoteExecutor(t *testing.T, transport *streamResultTransport) *StorageExecutor {
	t.Helper()
	exec := NewStorageExecutor(newTestMemoryEngine(t))
	exec.SetAllowRemoteAPOCURLAccess(true)
	exec.SetAPOCRemoteURLAllowlist([]string{"example.com"})
	exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
	})
	exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: transport})
	return exec
}

func streamResultParams(source string) map[string]interface{} {
	return map[string]interface{}{"source": source, "config": map[string]interface{}{}}
}

// streamResultRemoteBytes runs one bundled name against the recording transport
// and returns the single row's value.
func streamResultRemoteBytes(t *testing.T, transport *streamResultTransport, name, source string) interface{} {
	t.Helper()
	result, err := streamResultRemoteExecutor(t, transport).Execute(context.Background(), streamResultQuery(name), streamResultParams(source))
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	return result.Rows[0][0]
}

// TestBundledAPOCLoadBytesReturnsBytes is the correction: both bundled names
// yield the source's bytes, complete after the statement retires.
func TestBundledAPOCLoadBytesReturnsBytes(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t, streamResultNames...))

	for _, name := range streamResultNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Run("import-file", func(t *testing.T) {
				root := t.TempDir()
				require.NoError(t, os.WriteFile(filepath.Join(root, "payload.txt"), []byte(streamResultPayload), 0o600))
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				exec.SetAllowLocalAPOCImportFileAccess(true)
				exec.SetAPOCLocalFileAccessRoot(root)

				result, err := exec.Execute(context.Background(), streamResultQuery(name), streamResultParams("file:///payload.txt"))
				require.NoError(t, err)
				require.Len(t, result.Rows, 1)
				value := result.Rows[0][0]
				_, isReader := value.(io.ReadCloser)
				require.False(t, isReader, "the bundled export must not return a reader: %T", value)
				data, ok := value.([]byte)
				require.True(t, ok, "the bundled export returns the source's bytes: %T", value)
				require.Equal(t, streamResultPayload, string(data))
			})

			t.Run("allowlisted-remote", func(t *testing.T) {
				transport := &streamResultTransport{}
				value := streamResultRemoteBytes(t, transport, name, "https://example.com/stream")
				data, ok := value.([]byte)
				require.True(t, ok, "the bundled export returns the source's bytes: %T", value)
				require.Equal(t, streamResultPayload, string(data))

				requests, closed := transport.counts()
				require.Equal(t, 1, requests)
				require.Equal(t, 1, closed, "the loaded body is closed before the result is returned")
			})
		})
	}
}

// TestBundledAPOCLoadBytesDeniesBeforeAccess keeps the denied default for both
// names: no file is read and no outbound request is made.
func TestBundledAPOCLoadBytesDeniesBeforeAccess(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t, streamResultNames...))

	for _, name := range streamResultNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Run("remote-disabled", func(t *testing.T) {
				transport := &streamResultTransport{}
				exec := NewStorageExecutor(newTestMemoryEngine(t))
				exec.apocIO = exec.apocIO.WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
					return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
				})
				exec.apocIO = exec.apocIO.WithHTTPClient(&http.Client{Transport: transport})

				result, err := exec.Execute(context.Background(), streamResultQuery(name), streamResultParams("https://example.com/stream"))
				require.ErrorIs(t, err, apocio.ErrRemoteURLAccessDisabled)
				require.Nil(t, result)
				requests, closed := transport.counts()
				require.Zero(t, requests, "a denied APOC load must not make an outbound request")
				require.Zero(t, closed)
			})

			t.Run("file-disabled", func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "payload.txt")
				require.NoError(t, os.WriteFile(file, []byte(streamResultPayload), 0o600))
				exec := NewStorageExecutor(newTestMemoryEngine(t))

				result, err := exec.Execute(context.Background(), streamResultQuery(name), streamResultParams(file))
				require.ErrorIs(t, err, apocio.ErrLocalImportFileAccessDisabled)
				require.Nil(t, result)
			})
		})
	}
}

// TestBundledAPOCLoadBytesSurvivesStatementRetirement reproduces the reported
// failure: Execute runs on a statement context whose cancel func runs when the
// statement retires. The returned bytes stay readable afterwards, where a
// reader handed out as a value did not.
func TestBundledAPOCLoadBytesSurvivesStatementRetirement(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t, streamResultNames...))

	for _, name := range streamResultNames {
		name := name
		t.Run(name, func(t *testing.T) {
			transport := &streamResultTransport{}
			exec := streamResultRemoteExecutor(t, transport)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			result, err := exec.Execute(ctx, streamResultQuery(name), streamResultParams("https://example.com/stream"))
			require.NoError(t, err)
			require.Len(t, result.Rows, 1)

			// Normal statement retirement: the statement's own cancel func runs.
			cancel()
			require.ErrorIs(t, ctx.Err(), context.Canceled)

			data, ok := result.Rows[0][0].([]byte)
			require.True(t, ok, "the result is serializable bytes, not a statement-bound reader: %T", result.Rows[0][0])
			assert.Equal(t, streamResultPayload, string(data))
			requests, closed := transport.counts()
			require.Equal(t, 1, requests)
			require.Equal(t, 1, closed)
		})
	}
}

// TestBundledAPOCLoadBytesCallerCancellation covers a caller cancel func and a
// caller deadline that end the load while its source body is being read: the
// read observes the caller's lifetime, fails with that context's error, and
// still closes the body it opened.
func TestBundledAPOCLoadBytesCallerCancellation(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t, streamResultNames...))

	cases := []struct {
		name   string
		caller func(context.Context) (context.Context, context.CancelFunc)
		// expire ends the caller's lifetime once the read is in flight.
		expire func(context.CancelFunc)
		want   error
	}{
		{
			name: "cancel-during-read",
			caller: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithCancel(parent)
			},
			expire: func(cancel context.CancelFunc) { cancel() },
			want:   context.Canceled,
		},
		{
			name: "deadline-during-read",
			caller: func(parent context.Context) (context.Context, context.CancelFunc) {
				return context.WithTimeout(parent, streamResultDeadline)
			},
			// The fake clock advances once every goroutine is blocked, so the
			// caller's deadline expires while the body read is in flight.
			expire: func(context.CancelFunc) {
				time.Sleep(2 * streamResultDeadline)
			},
			want: context.DeadlineExceeded,
		},
	}

	for _, name := range streamResultNames {
		name := name
		for _, testCase := range cases {
			testCase := testCase
			t.Run(name+"/"+testCase.name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					transport := &streamResultTransport{hold: make(chan struct{}), entered: make(chan struct{})}
					exec := streamResultRemoteExecutor(t, transport)
					ctx, cancel := testCase.caller(context.Background())
					defer cancel()

					outcome := make(chan struct {
						result *ExecuteResult
						err    error
					}, 1)
					go func() {
						result, err := exec.Execute(ctx, streamResultQuery(name), streamResultParams("https://example.com/stream"))
						outcome <- struct {
							result *ExecuteResult
							err    error
						}{result: result, err: err}
					}()

					// The load is now reading the source on the caller's lifetime.
					select {
					case <-transport.entered:
					case <-time.After(streamResultWaitLimit):
						t.Fatalf("the %s read never started", name)
					}
					testCase.expire(cancel)

					select {
					case got := <-outcome:
						require.ErrorIs(t, got.err, testCase.want)
						require.Nil(t, got.result, "a canceled APOC load must not return rows")
					case <-time.After(streamResultWaitLimit):
						t.Fatalf("the canceled %s load did not return within %s", name, streamResultWaitLimit)
					}

					requests, closed := transport.counts()
					require.Equal(t, 1, requests)
					require.Equal(t, 1, closed, "the source body is closed when the load fails")
				})
			})
		}
	}
}

// TestBundledAPOCLoadBytesArgumentShape covers the argument contract both
// names now share: a source with an optional config map, wrong types reported
// as errors, and more than one config map rejected before any I/O.
func TestBundledAPOCLoadBytesArgumentShape(t *testing.T) {
	withPluginFunctions(t, bundledAPOCPluginFunctions(t, streamResultNames...))

	for _, name := range streamResultNames {
		name := name
		t.Run(name, func(t *testing.T) {
			t.Run("source-alone", func(t *testing.T) {
				transport := &streamResultTransport{}
				result, err := streamResultRemoteExecutor(t, transport).Execute(context.Background(),
					"RETURN "+name+"($source) AS value", map[string]interface{}{"source": "https://example.com/stream"})
				require.NoError(t, err)
				data, ok := result.Rows[0][0].([]byte)
				require.True(t, ok, "one argument returns bytes: %T", result.Rows[0][0])
				require.Equal(t, streamResultPayload, string(data))
			})

			t.Run("null-config", func(t *testing.T) {
				transport := &streamResultTransport{}
				value := streamResultRemoteBytes(t, transport, name, "https://example.com/stream")
				data, ok := value.([]byte)
				require.True(t, ok, "a null config is accepted: %T", value)
				require.Equal(t, streamResultPayload, string(data))

				result, err := streamResultRemoteExecutor(t, transport).Execute(context.Background(),
					streamResultQuery(name), map[string]interface{}{"source": "https://example.com/stream", "config": nil})
				require.NoError(t, err)
				data, ok = result.Rows[0][0].([]byte)
				require.True(t, ok, "a null config is accepted: %T", result.Rows[0][0])
				require.Equal(t, streamResultPayload, string(data))
			})

			t.Run("wrong-source-type", func(t *testing.T) {
				_, err := NewStorageExecutor(newTestMemoryEngine(t)).Execute(context.Background(),
					"RETURN "+name+"($source) AS value", map[string]interface{}{"source": int64(7)})
				require.ErrorContains(t, err, "argument 1 cannot be converted to string")
			})

			t.Run("wrong-config-type", func(t *testing.T) {
				transport := &streamResultTransport{}
				_, err := streamResultRemoteExecutor(t, transport).Execute(context.Background(),
					streamResultQuery(name), map[string]interface{}{"source": "https://example.com/stream", "config": "not-a-map"})
				require.ErrorContains(t, err, "argument 2 cannot be converted to map[string]interface {}")
				requests, _ := transport.counts()
				require.Zero(t, requests, "a rejected call must not open a source")
			})

			t.Run("two-config-maps", func(t *testing.T) {
				transport := &streamResultTransport{}
				_, err := streamResultRemoteExecutor(t, transport).Execute(context.Background(),
					"RETURN "+name+"($source, $first, $second) AS value",
					map[string]interface{}{
						"source": "https://example.com/stream",
						"first":  map[string]interface{}{},
						"second": map[string]interface{}{},
					})
				require.ErrorContains(t, err, "at most one config map, got 2")
				requests, _ := transport.counts()
				require.Zero(t, requests, "more than one config map is rejected before any I/O")
			})
		})
	}
}
