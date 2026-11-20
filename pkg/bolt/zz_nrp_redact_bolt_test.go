package bolt

// Part 50.16 (neutral): what a cancelled or timed-out Bolt request does when
// the server's query log is on (the RUN handler calls cypher.RedactLiterals,
// which takes no context, before the statement runs).
// Database: nornicdb.Open on a temporary directory, DefaultConfig with decay,
// automatic links and embeddings off; a database manager over it; a Bolt
// server (MaxConnections 16, no auth) with LogQueries on and a logger that
// records every log record with its time. Client: neo4j-go-driver v5.
// Statement W (write, 7,9xx bytes, under the 8,192-byte grammar-check limit):
//   CREATE (:Probe {run: $run, k: 'secret' + 'secret' + ... + 'x'})
// Cases, each 5 times with a new $run: client context deadline 1 ms; 20 ms;
// Bolt tx_timeout 1 ms; plain. After each request: wait 1 s, then count
// :Probe {run: $run} on a new session. The first request of the process is
// the first "cancel" case (fresh process, nothing parsed before it).

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/nornicdb"
)

type nrpLogRecord struct {
	at  time.Time
	msg string
}

type nrpLogHandler struct {
	mu      sync.Mutex
	records []nrpLogRecord
}

func (h *nrpLogHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *nrpLogHandler) Handle(_ context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool { b.WriteString(" " + a.String()); return true })
	h.mu.Lock()
	h.records = append(h.records, nrpLogRecord{at: time.Now(), msg: b.String()})
	h.mu.Unlock()
	return nil
}
func (h *nrpLogHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *nrpLogHandler) WithGroup(string) slog.Handler      { return h }

// queryEvents returns the log records after index from that mention the run marker.
func (h *nrpLogHandler) since(from int) []nrpLogRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]nrpLogRecord(nil), h.records[from:]...)
}
func (h *nrpLogHandler) count() int { h.mu.Lock(); defer h.mu.Unlock(); return len(h.records) }

func nrpManager(t *testing.T, db *nornicdb.DB) *multidb.DatabaseManager {
	f := reflect.ValueOf(multidb.NewDatabaseManager)
	args := []reflect.Value{}
	if f.Type().NumIn() == 3 {
		args = append(args, reflect.ValueOf(context.Background()))
	}
	args = append(args, reflect.ValueOf(db.GetBaseStorageForManager()), reflect.Zero(f.Type().In(f.Type().NumIn()-1)))
	out := f.Call(args)
	if !out[1].IsNil() {
		t.Fatal(out[1].Interface())
	}
	return out[0].Interface().(*multidb.DatabaseManager)
}

func nrpWriteStatement() string {
	var b strings.Builder
	b.WriteString("CREATE (:Probe {run: $run, k: ")
	for b.Len() < 7950 {
		b.WriteString("'secret' + ")
	}
	b.WriteString("'x'})")
	return b.String()
}

func TestNRPBoltCancelWithQueryLog(t *testing.T) {
	cfg := nornicdb.DefaultConfig()
	cfg.Memory.DecayEnabled = false
	cfg.Memory.AutoLinksEnabled = false
	cfg.Memory.EmbeddingEnabled = false
	db, err := nornicdb.Open(t.TempDir(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	manager := nrpManager(t, db)
	logs := &nrpLogHandler{}
	server := NewWithDatabaseManager(&Config{Port: 0, MaxConnections: 16, LogQueries: true, Logger: slog.New(logs)}, nil, manager)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	defer func() { _ = server.Close(); <-done }()
	driver, err := neo4j.NewDriverWithContext(fmt.Sprintf("bolt://127.0.0.1:%d", listener.Addr().(*net.TCPAddr).Port), neo4j.NoAuth())
	if err != nil {
		t.Fatal(err)
	}
	defer driver.Close(context.Background())
	if err := driver.VerifyConnectivity(context.Background()); err != nil {
		t.Fatal(err)
	}
	w := nrpWriteStatement()
	t.Logf("NRP statement bytes=%d", len(w))

	count := func(run string) int64 {
		ctx := context.Background()
		s := driver.NewSession(ctx, neo4j.SessionConfig{DatabaseName: "nornic"})
		defer s.Close(ctx)
		r, err := s.Run(ctx, "MATCH (p:Probe {run: $run}) RETURN count(p) AS c", map[string]any{"run": run})
		if err != nil {
			t.Fatal(err)
		}
		rec, err := r.Single(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return rec.Values[0].(int64)
	}
	one := func(kind string, i int) {
		run := fmt.Sprintf("%s-%d", kind, i)
		ctx := context.Background()
		var cancel context.CancelFunc = func() {}
		var opts []func(*neo4j.TransactionConfig)
		switch kind {
		case "cancel":
			ctx, cancel = context.WithTimeout(ctx, time.Millisecond)
		case "cancel20":
			ctx, cancel = context.WithTimeout(ctx, 20*time.Millisecond)
		case "txtimeout":
			opts = append(opts, neo4j.WithTxTimeout(time.Millisecond))
		}
		defer cancel()
		from := logs.count()
		s := driver.NewSession(context.Background(), neo4j.SessionConfig{DatabaseName: "nornic"})
		start := time.Now()
		r, err := s.Run(ctx, w, map[string]any{"run": run}, opts...)
		if err == nil {
			_, err = r.Consume(ctx)
		}
		elapsed := time.Since(start)
		_ = s.Close(context.Background())
		time.Sleep(time.Second)
		var events []string
		for _, rec := range logs.since(from) {
			if strings.Contains(rec.msg, "CREATE") || strings.Contains(strings.ToLower(rec.msg), "query") {
				events = append(events, fmt.Sprintf("+%.1fms %.90q", float64(rec.at.Sub(start).Microseconds())/1000, rec.msg))
			}
		}
		msg := "none"
		if err != nil {
			msg = err.Error()
			if len(msg) > 160 {
				msg = msg[:160]
			}
		}
		t.Logf("NRPSUM bolt case=%s i=%d client_ms=%.2f err=%q stored=%d log_events=%d", kind, i, float64(elapsed.Microseconds())/1000, msg, count(run), len(events))
		for _, ev := range events {
			t.Logf("NRP   log %s", ev)
		}
	}
	for i := 1; i <= 5; i++ {
		one("cancel", i)
		one("cancel20", i)
		one("txtimeout", i)
		one("plain", i)
	}
	// The redactor's own cost on this statement in this (now warm) process.
	var max time.Duration
	var sum time.Duration
	for i := 0; i < 5; i++ {
		start := time.Now()
		out := cypher.RedactLiterals(w)
		d := time.Since(start)
		sum += d
		if d > max {
			max = d
		}
		if strings.Contains(out, "secret") {
			t.Logf("NRP redaction kept a literal")
		}
	}
	t.Logf("NRPSUM redact warm mean_ms=%.3f max_ms=%.3f", float64(sum.Microseconds())/5000, float64(max.Microseconds())/1000)
}
