package observability

import (
	"bytes"
	"fmt"
	"html"
	"io"
	"net/http"
	"os"
	"runtime"
	runtimepprof "runtime/pprof"
	runtimetrace "runtime/trace"
	"strconv"
	"strings"
	"time"

	"github.com/google/pprof/profile"
)

// Keep profiling registration explicit: runtime/pprof and runtime/trace have no
// HTTP init handlers. The profile codec preserves named-profile delta requests.
func registerPprofHandlers(mux *http.ServeMux) {
	mux.HandleFunc("GET /debug/pprof/", servePprofIndex)
	mux.HandleFunc("GET /debug/pprof/cmdline", func(w http.ResponseWriter, r *http.Request) {
		pprofHeaders(w, "text/plain; charset=utf-8", "")
		_, _ = io.WriteString(w, strings.Join(DisplayArgs(os.Args), "\x00"))
	})
	mux.HandleFunc("GET /debug/pprof/profile", serveCPUProfile)
	mux.HandleFunc("GET /debug/pprof/trace", serveRuntimeTrace)
	mux.HandleFunc("GET /debug/pprof/symbol", servePprofSymbols)
	mux.HandleFunc("POST /debug/pprof/symbol", servePprofSymbols)
}

func pprofHeaders(w http.ResponseWriter, contentType, filename string) {
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	if filename != "" {
		w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename=%q`, filename))
	}
}

func pprofError(w http.ResponseWriter, code int, err error) {
	w.Header().Del("Content-Disposition")
	pprofHeaders(w, "text/plain; charset=utf-8", "")
	http.Error(w, err.Error(), code)
}

func servePprofIndex(w http.ResponseWriter, r *http.Request) {
	name := strings.TrimPrefix(r.URL.Path, "/debug/pprof/")
	if name != "" {
		serveRuntimeProfile(w, r, name)
		return
	}
	pprofHeaders(w, "text/html; charset=utf-8", "")
	_, _ = io.WriteString(w, "<html><head><title>Profiles</title></head><body><h1>Profiles</h1><ul>")
	for _, p := range runtimepprof.Profiles() {
		_, _ = fmt.Fprintf(w, `<li><a href="%s?debug=1">%s</a> (%d)</li>`, html.EscapeString(p.Name()), html.EscapeString(p.Name()), p.Count())
	}
	for _, name := range []string{"cmdline", "profile", "symbol", "trace"} {
		_, _ = fmt.Fprintf(w, `<li><a href="%s">%s</a></li>`, name, name)
	}
	_, _ = io.WriteString(w, "</ul></body></html>")
}

func serveRuntimeProfile(w http.ResponseWriter, r *http.Request, name string) {
	p := runtimepprof.Lookup(name)
	if p == nil {
		http.NotFound(w, r)
		return
	}
	debug, _ := strconv.Atoi(r.URL.Query().Get("debug"))
	if seconds := r.URL.Query().Get("seconds"); seconds != "" {
		duration, err := time.ParseDuration(seconds + "s")
		if err != nil || duration <= 0 || debug != 0 {
			pprofError(w, http.StatusBadRequest, fmt.Errorf("seconds must be positive and debug must be zero"))
			return
		}
		serveProfileDelta(w, r, p, duration)
		return
	}
	if name == "heap" && r.URL.Query().Get("gc") == "1" {
		runtime.GC()
	}
	if debug == 0 {
		pprofHeaders(w, "application/octet-stream", name)
	} else {
		pprofHeaders(w, "text/plain; charset=utf-8", "")
	}
	_ = p.WriteTo(w, debug)
}

func readRuntimeProfile(p *runtimepprof.Profile) (*profile.Profile, error) {
	var data bytes.Buffer
	if err := p.WriteTo(&data, 0); err != nil {
		return nil, err
	}
	return profile.ParseData(data.Bytes())
}

func serveProfileDelta(w http.ResponseWriter, r *http.Request, p *runtimepprof.Profile, duration time.Duration) {
	before, err := readRuntimeProfile(p)
	if err != nil {
		pprofError(w, http.StatusInternalServerError, err)
		return
	}
	started := time.Now()
	if !waitPprof(r, duration) {
		return
	}
	after, err := readRuntimeProfile(p)
	if err != nil {
		pprofError(w, http.StatusInternalServerError, err)
		return
	}
	before.Scale(-1)
	delta, err := profile.Merge([]*profile.Profile{before, after})
	if err != nil {
		pprofError(w, http.StatusInternalServerError, err)
		return
	}
	delta.TimeNanos, delta.DurationNanos = time.Now().UnixNano(), int64(time.Since(started))
	pprofHeaders(w, "application/octet-stream", p.Name())
	_ = delta.Write(w)
}

func waitPprof(r *http.Request, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-r.Context().Done():
		return false
	case <-timer.C:
		return true
	}
}

func serveCPUProfile(w http.ResponseWriter, r *http.Request) {
	seconds, _ := strconv.Atoi(r.URL.Query().Get("seconds"))
	if seconds <= 0 {
		seconds = 30
	}
	pprofHeaders(w, "application/octet-stream", "profile")
	if err := runtimepprof.StartCPUProfile(w); err != nil {
		pprofError(w, http.StatusInternalServerError, err)
		return
	}
	defer runtimepprof.StopCPUProfile()
	waitPprof(r, time.Duration(seconds)*time.Second)
}

func serveRuntimeTrace(w http.ResponseWriter, r *http.Request) {
	seconds, _ := strconv.ParseFloat(r.URL.Query().Get("seconds"), 64)
	if seconds <= 0 {
		seconds = 1
	}
	pprofHeaders(w, "application/octet-stream", "trace")
	if err := runtimetrace.Start(w); err != nil {
		pprofError(w, http.StatusInternalServerError, err)
		return
	}
	defer runtimetrace.Stop()
	waitPprof(r, time.Duration(seconds*float64(time.Second)))
}

func servePprofSymbols(w http.ResponseWriter, r *http.Request) {
	pprofHeaders(w, "text/plain; charset=utf-8", "")
	_, _ = io.WriteString(w, "num_symbols: 1\n")
	query := r.URL.RawQuery
	if r.Method == http.MethodPost {
		data, _ := io.ReadAll(r.Body)
		query = string(data)
	}
	for _, address := range strings.Fields(strings.ReplaceAll(query, "+", " ")) {
		pc, err := strconv.ParseUint(address, 0, 64)
		if err != nil {
			continue
		}
		if fn := runtime.FuncForPC(uintptr(pc)); fn != nil {
			_, _ = fmt.Fprintf(w, "%s %s\n", address, fn.Name())
		}
	}
}
