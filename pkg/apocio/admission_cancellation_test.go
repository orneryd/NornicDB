package apocio

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCanceledFileAdmissionReturnsCallerError(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "data.json"), []byte(`{"value":1}`), 0o600))
	capability := NewPolicy(Config{AllowImportFileAccess: true, FileAccessRoot: root}).Capability()
	for _, deadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "deadline"}[deadline], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if deadline {
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
			}
			body, err := capability.Open(ctx, "data.json", Request{})
			if body != nil {
				require.NoError(t, body.Close())
			}
			require.ErrorIs(t, err, ctx.Err())
			require.Nil(t, body, "canceled admission must not return an open file")
		})
	}
	body, err := capability.Open(context.Background(), "data.json", Request{})
	require.NoError(t, err)
	require.NoError(t, body.Close())
}

func TestFileStreamStopsReadingAfterCallerCancellation(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(root, "data.bin"), []byte("first-second"), 0o600))
	capability := NewPolicy(Config{AllowImportFileAccess: true, FileAccessRoot: root}).Capability()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	body, err := capability.Open(ctx, "data.bin", Request{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, body.Close()) })
	buffer := make([]byte, 5)
	n, err := body.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, "first", string(buffer[:n]))
	cancel()
	n, err = body.Read(buffer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n, "no further file bytes are delivered after caller cancellation")
}

type cancelDuringRead struct {
	cancel context.CancelFunc
	reads  int
	closes int
	err    error
}

func (r *cancelDuringRead) Read(buffer []byte) (int, error) {
	r.reads++
	copy(buffer, "data")
	r.cancel()
	return 4, r.err
}
func (r *cancelDuringRead) Close() error { r.closes++; return nil }

func TestLocalSourceDiscardsBytesWhenCallerCancelsDuringRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	underlying := &cancelDuringRead{cancel: cancel}
	body := sourceReader{ReadCloser: underlying, ctx: ctx}
	buffer := make([]byte, 8)
	n, err := body.Read(buffer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	n, err = body.Read(buffer)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, n)
	require.Equal(t, 1, underlying.reads, "no later read reaches the source")
	require.NoError(t, body.Close())
	require.Equal(t, 1, underlying.closes)
}

func TestSourceReaderPreservesPrimaryReadFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	failure := errors.New("controlled source read failure")
	underlying := &cancelDuringRead{cancel: cancel, err: failure}
	body := sourceReader{ReadCloser: underlying, ctx: ctx}
	_, err := body.Read(make([]byte, 8))
	require.Same(t, failure, err, "the I/O failure is not replaced by a concurrent cancellation")
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NoError(t, body.Close())
}
