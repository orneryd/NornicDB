package observability

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/pprof/profile"
)

func TestPprofRuntimeFormats(t *testing.T) {
	mux := http.NewServeMux()
	registerPprofHandlers(mux)
	for _, path := range []string{"heap?gc=1", "allocs", "goroutine", "block", "mutex", "threadcreate", "heap?seconds=0.01", "profile?seconds=1"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRecorder()
			mux.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/debug/pprof/"+path, nil))
			if r.Code != http.StatusOK {
				t.Fatal("profile request failed")
			}
			p, err := profile.ParseData(r.Body.Bytes())
			if err != nil {
				t.Fatal("profiler did not emit a pprof-compatible binary")
			}
			if err := p.CheckValid(); err != nil {
				t.Fatal("profiler emitted an invalid profile")
			}
		})
	}
	for _, path := range []string{"goroutine?debug=1", "symbol", "trace?seconds=0.01"} {
		r := httptest.NewRecorder()
		mux.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/debug/pprof/"+path, nil))
		if r.Code != http.StatusOK || r.Body.Len() == 0 {
			t.Fatal("runtime diagnostic handler failed")
		}
	}
	r := httptest.NewRecorder()
	mux.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/debug/pprof/heap?debug=1&seconds=1", nil))
	if r.Code != http.StatusBadRequest {
		t.Error("text delta request must be rejected")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitPprof(httptest.NewRequest(http.MethodGet, "/", nil).WithContext(ctx), time.Hour) {
		t.Error("cancelled profiling request did not stop")
	}
	r = httptest.NewRecorder()
	mux.ServeHTTP(r, httptest.NewRequest(http.MethodGet, "/debug/pprof/missing", nil))
	if r.Code != http.StatusNotFound {
		t.Error("unknown profile must be absent")
	}
	if strings.Contains(r.Body.String(), "cmdline") {
		t.Error("unknown profile unexpectedly emitted argv")
	}
}
