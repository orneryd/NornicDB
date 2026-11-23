//go:build closeout_l2_e20_r2

package bolt

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

// E20 Reproduction 2 (over Bolt): a statement of about 720 string literals
// joined by `+` keeps the server busy for about half a minute. A client
// deadline of 20 ms ends only the client's wait; a 1 ms transaction timeout
// is answered after 32-35 s; without a limit the first request commits after
// 30 s (2dbc67e96).
//
// The server's own duration of every RUN is read from its structured log
// (the "bolt.query.run" event's duration attribute, emitted with
// LogQueries: true), captured by a slog handler passed as Config.Logger. That
// replaces reading the server's text log in the entry.
//
// Thresholds: the entry's defect is 32-35 s for the 1 ms transaction timeout,
// 30.4 s for the request without a limit and 32-35 s of server time for the
// request the client cancelled after 20 ms; the fixed rounds of the entry
// answer in 3-424 ms. A 1 ms transaction timeout answered after more than
// 3 s, a request without a limit taking more than 5 s, or any server RUN
// lasting more than 5 s is the defect.

const (
	l2E20R2TxTimeoutBound = 3 * time.Second
	l2E20R2NoLimitBound   = 5 * time.Second
	l2E20R2ServerBound    = 5 * time.Second
)

type l2E20R2Record struct {
	at       time.Time
	status   string
	duration time.Duration
	errText  string
}

type l2E20R2Log struct {
	mu      sync.Mutex
	records []l2E20R2Record
}

func (l *l2E20R2Log) snapshot() []l2E20R2Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]l2E20R2Record(nil), l.records...)
}

type l2E20R2Handler struct {
	log   *l2E20R2Log
	attrs []slog.Attr
}

func (h l2E20R2Handler) Enabled(context.Context, slog.Level) bool { return true }

func (h l2E20R2Handler) Handle(_ context.Context, r slog.Record) error {
	var event, status, errText string
	var duration time.Duration
	var hasDuration bool
	visit := func(a slog.Attr) bool {
		switch a.Key {
		case "event_id":
			event = a.Value.String()
		case "status":
			status = a.Value.String()
		case "error":
			errText = a.Value.String()
		case "duration":
			if a.Value.Kind() == slog.KindDuration {
				duration, hasDuration = a.Value.Duration(), true
			}
		}
		return true
	}
	for _, a := range h.attrs {
		visit(a)
	}
	r.Attrs(visit)
	if event != "bolt.query.run" || !hasDuration {
		return nil
	}
	h.log.mu.Lock()
	h.log.records = append(h.log.records, l2E20R2Record{at: time.Now(), status: status, duration: duration, errText: errText})
	h.log.mu.Unlock()
	return nil
}

func (h l2E20R2Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := append(append([]slog.Attr(nil), h.attrs...), attrs...)
	return l2E20R2Handler{log: h.log, attrs: next}
}

func (h l2E20R2Handler) WithGroup(string) slog.Handler { return h }

type l2E20R2Lab struct {
	db     *nornicdb.DB
	driver neo4j.DriverWithContext
	server *Server
	done   chan error
	log    *l2E20R2Log
}

func l2E20R2Open(t *testing.T) *l2E20R2Lab {
	t.Helper()
	config := nornicdb.DefaultConfig()
	config.Memory.DecayEnabled = false
	config.Memory.AutoLinksEnabled = false
	config.Memory.EmbeddingEnabled = false
	db, err := nornicdb.Open(t.TempDir(), config)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	manager, err := multidb.NewDatabaseManager(context.Background(), db.GetBaseStorageForManager(), nil)
	if err != nil {
		_ = db.Close()
		t.Fatalf("database manager: %v", err)
	}
	capture := &l2E20R2Log{}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16, LogQueries: true, Logger: slog.New(l2E20R2Handler{log: capture})}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = db.Close()
		t.Fatalf("listen: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	if err != nil {
		_ = server.Close()
		<-done
		_ = db.Close()
		t.Fatalf("driver: %v", err)
	}
	lab := &l2E20R2Lab{db: db, driver: driver, server: server, done: done, log: capture}
	t.Cleanup(func() {
		_ = lab.driver.Close(context.Background())
		_ = lab.server.Close()
		<-lab.done
		if err := lab.db.Close(); err != nil {
			t.Errorf("close db: %v", err)
		}
	})
	return lab
}

type l2E20R2Kind int

const (
	l2E20R2Deadline1ms l2E20R2Kind = iota
	l2E20R2Deadline20ms
	l2E20R2TxTimeout1ms
	l2E20R2NoLimit
)

func (k l2E20R2Kind) String() string {
	return [...]string{"deadline1ms", "deadline20ms", "txTimeout1ms", "noLimit"}[k]
}

type l2E20R2Outcome struct {
	kind    l2E20R2Kind
	elapsed time.Duration
	err     error
	count   int64
}

func (o l2E20R2Outcome) String() string {
	e := "committed"
	if o.err != nil {
		e = o.err.Error()
		if len(e) > 90 {
			e = e[:90]
		}
	}
	return fmt.Sprintf("%s:%.4fs(%s;count=%d)", o.kind, o.elapsed.Seconds(), e, o.count)
}

// request runs statement once as kind on a new session, the client's time
// covering the run and the consumption of its result, closes the session,
// waits 1 s (as the entry) and counts the run's :Probe nodes on a new session.
func (l *l2E20R2Lab) request(t *testing.T, statement, run string, kind l2E20R2Kind) l2E20R2Outcome {
	t.Helper()
	session := l.driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: "nornic"})
	ctx := context.Background()
	cancel := context.CancelFunc(func() {})
	var configurers []func(*neo4j.TransactionConfig)
	switch kind {
	case l2E20R2Deadline1ms:
		ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
	case l2E20R2Deadline20ms:
		ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
	case l2E20R2TxTimeout1ms:
		configurers = append(configurers, neo4j.WithTxTimeout(time.Millisecond))
	}
	started := time.Now()
	result, err := session.Run(ctx, statement, map[string]any{"run": run}, configurers...)
	if err == nil {
		_, err = result.Consume(ctx)
	}
	outcome := l2E20R2Outcome{kind: kind, elapsed: time.Since(started), err: err}
	cancel()
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	_ = session.Close(closeCtx)
	closeCancel()
	time.Sleep(time.Second)
	outcome.count = l.count(t, run)
	return outcome
}

func (l *l2E20R2Lab) count(t *testing.T, run string) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	session := l.driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, "MATCH (n:Probe {run: $run}) RETURN count(n) AS c", map[string]any{"run": run})
	if err != nil {
		t.Fatalf("count %s: %v", run, err)
	}
	record, err := result.Single(ctx)
	if err != nil {
		t.Fatalf("count %s: %v", run, err)
	}
	switch v := record.Values[0].(type) {
	case int64:
		return v
	case int:
		return int64(v)
	}
	t.Fatalf("count %s: unexpected value %T %v", run, record.Values[0], record.Values[0])
	return -1
}

func l2E20R2Statement() string {
	return "CREATE (:Probe {run: $run, k: " + strings.Repeat("'secret' + ", 720) + "'x'})"
}

func l2E20R2MaxServer(records []l2E20R2Record) (time.Duration, string) {
	var longest time.Duration
	var status string
	for _, r := range records {
		if r.duration > longest {
			longest, status = r.duration, r.status
		}
	}
	return longest, status
}

func TestL2_e20_r2_Repro(t *testing.T) {
	lab := l2E20R2Open(t)
	statement := l2E20R2Statement()
	t.Logf("statement: %d bytes", len(statement))
	kinds := []l2E20R2Kind{l2E20R2Deadline1ms, l2E20R2Deadline20ms, l2E20R2TxTimeout1ms, l2E20R2NoLimit}
	var rounds [][]l2E20R2Outcome
	var cancelled20 time.Time
	defect := false
	for round := 1; round <= 5; round++ {
		var outcomes []l2E20R2Outcome
		for _, kind := range kinds {
			o := lab.request(t, statement, fmt.Sprintf("r%d-%s", round, kind), kind)
			if round == 1 && kind == l2E20R2Deadline20ms {
				cancelled20 = time.Now()
			}
			outcomes = append(outcomes, o)
			t.Logf("round %d %s", round, o)
			switch kind {
			case l2E20R2TxTimeout1ms:
				if o.elapsed > l2E20R2TxTimeoutBound {
					defect = true
					t.Errorf("round %d: the 1 ms transaction timeout was answered after %v (bound %v)", round, o.elapsed, l2E20R2TxTimeoutBound)
				}
			case l2E20R2NoLimit:
				if o.err != nil {
					t.Errorf("round %d: the request without a limit failed: %v", round, o.err)
				}
				if o.elapsed > l2E20R2NoLimitBound {
					defect = true
					t.Errorf("round %d: the request without a limit took %v (bound %v)", round, o.elapsed, l2E20R2NoLimitBound)
				}
			}
			if o.err != nil && o.count != 0 {
				// Not the entry's defect (it found 0); reported so a partial
				// write after a failed request is never hidden.
				t.Errorf("round %d %s failed (%v) but %d :Probe node(s) were stored", round, kind, o.err, o.count)
			}
		}
		rounds = append(rounds, outcomes)
	}
	// The server's execution of round 1's cancelled 20 ms request may still
	// be running; give it until 40 s after the client gave up (the entry's
	// longest server time is 35.3 s) before reading the server's durations.
	deadline := cancelled20.Add(40 * time.Second)
	for time.Now().Before(deadline) {
		if longest, _ := l2E20R2MaxServer(lab.log.snapshot()); longest > l2E20R2ServerBound {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	records := lab.log.snapshot()
	longest, status := l2E20R2MaxServer(records)
	var serverLines []string
	for _, r := range records {
		if r.duration > 100*time.Millisecond {
			e := r.errText
			if len(e) > 60 {
				e = e[:60]
			}
			serverLines = append(serverLines, fmt.Sprintf("%s:%.3fs(%s)", r.status, r.duration.Seconds(), e))
		}
	}
	t.Logf("server RUN records: %d; over 100 ms: %s", len(records), strings.Join(serverLines, " "))
	if longest > l2E20R2ServerBound {
		defect = true
		t.Errorf("the server ran a RUN for %v (status %s; bound %v): a cancelled or timed-out request kept executing", longest, status, l2E20R2ServerBound)
	}
	var summary []string
	for i, outcomes := range rounds {
		var parts []string
		for _, o := range outcomes {
			parts = append(parts, o.String())
		}
		summary = append(summary, fmt.Sprintf("round%d=[%s]", i+1, strings.Join(parts, " ")))
	}
	verdict := "OK"
	if defect {
		verdict = "DEFECT"
	}
	t.Logf("L2RESULT E20-R2 %s serverMax=%.3fs(%s) %s", verdict, longest.Seconds(), status, strings.Join(summary, " "))
}

// TestL2_e20_r2_Control: a short statement of the same kind commits quickly
// without a limit and is counted, a 1 ms transaction timeout on it is
// answered quickly, and the log capture sees the server's RUN records (so the
// Repro's server-duration check can see them).
func TestL2_e20_r2_Control(t *testing.T) {
	lab := l2E20R2Open(t)
	short := "CREATE (:Probe {run: $run, k: 'secret' + 'x'})"
	free := lab.request(t, short, "c-free", l2E20R2NoLimit)
	t.Logf("control %s", free)
	if free.err != nil || free.count != 1 || free.elapsed > l2E20R2NoLimitBound {
		t.Errorf("short statement without a limit: %s", free)
	}
	timed := lab.request(t, short, "c-tx", l2E20R2TxTimeout1ms)
	t.Logf("control %s", timed)
	if timed.elapsed > l2E20R2TxTimeoutBound {
		t.Errorf("short statement with a 1 ms transaction timeout: %s", timed)
	}
	if timed.err != nil && timed.count != 0 {
		t.Errorf("short statement failed (%v) but stored %d node(s)", timed.err, timed.count)
	}
	records := lab.log.snapshot()
	t.Logf("control: %d server RUN records captured", len(records))
	if len(records) == 0 {
		t.Errorf("the log capture saw no bolt.query.run record")
	}
}
