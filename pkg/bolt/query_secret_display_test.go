package bolt

import (
	"context"
	"io"
	"net"
	"strings"
	"testing"
)

// Exercise the RUN log consumer, including a literal crossing its display limit.
func TestBoltQueryDisplayRedactsBeforeTruncation(t *testing.T) {
	const credential = "synthetic-bolt-query-credential-492817361"
	for _, padding := range []int{0, 300} {
		cfg := DefaultConfig()
		cfg.LogQueries, cfg.RequireAuth = true, false
		server, output := newCapturingServer(t, cfg)
		serverConn, clientConn := net.Pipe()
		done := make(chan struct{})
		go func() { _, _ = io.Copy(io.Discard, clientConn); close(done) }()
		var consumed string
		executor := &mockExecutor{executeFunc: func(ctx context.Context, query string, params map[string]any) (*QueryResult, error) {
			consumed = query
			return &QueryResult{Columns: []string{"result"}, Rows: [][]any{{"ok"}}}, nil
		}}
		session := newTestSession(serverConn, executor)
		session.server = server
		query := "RETURN '" + credential + strings.Repeat("p", padding) + "' AS result"
		data := append(encodePackStreamString(query), encodePackStreamMap(map[string]any{})...)
		err := session.handleRun(data)
		_ = serverConn.Close()
		_ = clientConn.Close()
		<-done
		if err != nil {
			t.Fatal("RUN failed; details withheld")
		}
		if consumed != query {
			t.Error("logging changed the executed query")
		}
		if strings.Contains(output.String(), credential) {
			t.Error("Bolt query log disclosed a literal; output withheld")
		}
		if !strings.Contains(output.String(), "RETURN") {
			t.Error("query log lost its non-secret clause")
		}
	}
}
