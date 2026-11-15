package testfixture

import "sync/atomic"

// ErrorWriter rejects writes with Err and counts concurrent attempts safely.
// Configure Err before sharing the writer; transport fixtures can embed it.
//
//	writer := &testfixture.ErrorWriter{Err: errWriteFailed}
//	_, _ = writer.Write(nil) // Calls.Load() == 1; the error is errWriteFailed.
type ErrorWriter struct {
	Err   error
	Calls atomic.Int64
}

// Write implements io.Writer, counting the attempt in Calls, accepting no bytes,
// and returning Err unchanged.
func (w *ErrorWriter) Write([]byte) (int, error) {
	w.Calls.Add(1)
	return 0, w.Err
}
