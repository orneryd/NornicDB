package bolt

import (
	"context"
	"io"
	"net"
	"reflect"
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

func TestBoltQueryParametersUseCredentialContextWithoutChangingExecution(t *testing.T) {
	const secret = "synthetic-bolt-context-secret-541937"
	for _, entry := range []struct{ name, query, clause string }{
		{"URI", "RETURN 'https://source-user:" + secret + "@example.test/path' AS endpoint", "RETURN"},
		{"opaque PASSWORD", "CREATE COMPOSITE DATABASE contextual ALIAS r FOR DATABASE remote AT 'https://example.test' USER 'service' PASSWORD '" + secret + "' ACCESS read", "<REDACTED>"},
	} {
		t.Run(entry.name, func(t *testing.T) {
			query := entry.query
			params := map[string]any{"ordinary": secret, "nested": map[string]any{"password": "synthetic-nested-password-72941", "count": int64(7)}, "safe": "visible"}
			want := map[string]any{"ordinary": secret, "nested": map[string]any{"password": "synthetic-nested-password-72941", "count": int64(7)}, "safe": "visible"}
			cfg := DefaultConfig()
			cfg.LogQueries, cfg.RequireAuth = true, false
			server, output := newCapturingServer(t, cfg)
			serverConn, clientConn := net.Pipe()
			done := make(chan struct{})
			go func() { _, _ = io.Copy(io.Discard, clientConn); close(done) }()
			t.Cleanup(func() { _ = serverConn.Close(); _ = clientConn.Close(); <-done })
			called := false
			executor := &mockExecutor{executeFunc: func(ctx context.Context, got string, values map[string]any) (*QueryResult, error) {
				called = true
				if got != query || !reflect.DeepEqual(values, want) {
					t.Error("diagnostic projection changed execution inputs")
				}
				return &QueryResult{Columns: []string{"endpoint"}, Rows: [][]any{{"ok"}}}, nil
			}}
			session := newTestSession(serverConn, executor)
			session.server = server
			data := append(encodePackStreamString(query), encodePackStreamMap(params)...)
			if err := session.handleRun(data); err != nil {
				t.Fatal("RUN failed; details withheld")
			}
			if !called {
				t.Fatal("RUN did not execute")
			}
			if !reflect.DeepEqual(params, want) {
				t.Error("logging changed caller parameters")
			}
			for _, marker := range []string{secret, "synthetic-nested-password-72941"} {
				if strings.Contains(output.String(), marker) {
					t.Error("Bolt query log disclosed credential; output withheld")
				}
			}
			if !strings.Contains(output.String(), "visible") || !strings.Contains(output.String(), entry.clause) {
				t.Error("projection lost ordinary diagnostic context")
			}
		})
	}
}
