package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type r12LabHarness struct {
	t      *testing.T
	db     *nornicdb.DB
	srv    *Server
	router http.Handler
	authz  string
}

func r12Lab(t *testing.T) *r12LabHarness {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Database.AsyncWritesEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil {
		t.Fatalf("open R12 database: %v", err)
	}
	authenticator, err := auth.NewAuthenticator(auth.AuthConfig{
		SecurityEnabled: true,
		JWTSecret:       []byte("test-secret-key-for-testing-only-32b"),
	}, storage.NewNamespacedEngine(db.GetBaseStorageForManager(), "system"))
	if err != nil {
		_ = db.Close()
		t.Fatalf("R12 authenticator: %v", err)
	}
	if _, err := authenticator.CreateUser(context.Background(), "admin", "password123", []auth.Role{auth.RoleAdmin}); err != nil && !errors.Is(err, auth.ErrUserExists) {
		_ = db.Close()
		t.Fatalf("R12 user: %v", err)
	}
	srv, err := New(db, authenticator, DefaultConfig())
	if err != nil {
		_ = db.Close()
		t.Fatalf("R12 server: %v", err)
	}
	token, _, err := authenticator.Authenticate(context.Background(), "admin", "password123", "127.0.0.1", "R12")
	if err != nil {
		_ = srv.Stop(context.Background())
		_ = db.Close()
		t.Fatalf("R12 token: %v", err)
	}
	lab := &r12LabHarness{t: t, db: db, srv: srv, router: srv.buildRouter(), authz: "Bearer " + token.AccessToken}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = srv.Stop(ctx)
		_ = db.Close()
	})
	return lab
}

func nor287RequireNoInternalDetail(t *testing.T, code int, body []byte) {
	t.Helper()
	if code >= http.StatusOK && code < http.StatusMultipleChoices {
		return
	}
	if strings.Contains(string(body), errNOR287CountReadFailure.Error()) {
		t.Fatalf("HTTP %d must not expose the internal count-read cause: %s", code, body)
	}
}

func (l *r12LabHarness) post(path string, body any) (map[string]any, int, time.Duration) {
	l.t.Helper()
	raw, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(string(raw)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", l.authz)
	rec := httptest.NewRecorder()
	start := time.Now()
	l.router.ServeHTTP(rec, req)
	var out map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return out, rec.Code, time.Since(start)
}

func (l *r12LabHarness) cypher(query string, params map[string]any) {
	l.t.Helper()
	out, code, _ := l.post("/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}})
	if code >= http.StatusBadRequest {
		l.t.Fatalf("R12 query %q: HTTP %d: %v", query, code, out)
	}
	if errors, _ := out["errors"].([]any); len(errors) > 0 {
		l.t.Fatalf("R12 query %q: %v", query, errors)
	}
}

func r12ScaleRun(l *r12LabHarness, name, query string, params map[string]any, dumps []time.Duration) (map[string]any, float64) {
	done := make(chan struct{})
	go func() {
		start := time.Now()
		for _, delay := range dumps {
			wait := time.Until(start.Add(delay))
			if wait < 0 {
				wait = 0
			}
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
				slowbDump(fmt.Sprintf("R12 %s at %s", name, delay))
			case <-done:
				timer.Stop()
				return
			}
		}
	}()
	out, code, elapsed := l.post("/db/nornic/tx/commit", map[string]any{"statements": []any{map[string]any{"statement": query, "parameters": params}}})
	close(done)
	if code >= http.StatusBadRequest {
		fmt.Printf("R12 ERROR name=%s code=%d result=%v\n", name, code, out)
	}
	return out, elapsed.Seconds()
}

func slowbDump(tag string) {
	var b strings.Builder
	if profile := pprof.Lookup("goroutine"); profile != nil {
		_ = profile.WriteTo(&b, 1)
	}
	fmt.Printf("SLOWB DUMP BEGIN %s\n%s\nSLOWB DUMP END %s\n", tag, b.String(), tag)
}
