package bolt

import (
	"net"
	"strings"
	"testing"
)

func TestAutoCommitPull_SendsNornicBookmark(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	session := newTestSession(server, &mockExecutor{})

	// Simulate an autocommit write query that produced no records.
	session.lastResult = &QueryResult{
		Columns: []string{},
		Rows:    [][]any{},
	}
	session.lastQueryIsWrite = true
	session.pendingFlush = false

	errCh := make(chan error, 1)
	go func() {
		_, data, err := ReadMessage(client)
		if err != nil {
			errCh <- err
			return
		}
		meta, _, err := decodePackStreamMap(data, 0)
		if err != nil {
			errCh <- err
			return
		}
		bm, _ := meta["bookmark"].(string)
		if !strings.HasPrefix(bm, "nornicdb:bookmark:") {
			errCh <- &testError{"missing/invalid bookmark", bm}
			return
		}
		if bm == "nornicdb:tx:auto" {
			errCh <- &testError{"legacy placeholder bookmark emitted", bm}
			return
		}
		errCh <- nil
	}()

	if err := session.handlePull(nil); err != nil {
		t.Fatalf("handlePull error: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
}

type testError struct {
	msg string
	val string
}

func (e *testError) Error() string {
	if e.val == "" {
		return e.msg
	}
	return e.msg + ": " + e.val
}
