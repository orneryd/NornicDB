package cypher

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func r2DiagnosticCanary(t *testing.T) string {
	t.Helper()
	var value [18]byte
	if _, err := rand.Read(value[:]); err != nil {
		t.Fatal("canary generation failed")
	}
	return hex.EncodeToString(value[:])
}

// r2StatisticsRetrieval executes a statistics retrieval and returns the
// section/data columns and rows as the executor publishes them.
func r2StatisticsRetrieval(t *testing.T, e *StorageExecutor, section string) ([]string, [][]interface{}) {
	t.Helper()
	result, err := e.Execute(context.Background(), "CALL db.stats.retrieve('"+section+"')", nil)
	if err != nil {
		t.Fatalf("statistics retrieval for %s failed; details withheld", section)
	}
	return result.Columns, result.Rows
}

func r2StatisticsQueries(t *testing.T, e *StorageExecutor) map[string]map[string]interface{} {
	t.Helper()
	columns, rows := r2StatisticsRetrieval(t, e, "QUERIES")
	if len(columns) != 2 || columns[0] != "section" || columns[1] != "data" {
		t.Fatalf("QUERIES signature changed: %v", columns)
	}
	queries := make(map[string]map[string]interface{}, len(rows))
	for _, row := range rows {
		data, ok := row[1].(map[string]interface{})
		if !ok {
			t.Fatal("QUERIES row data is not a map")
		}
		queries[data["query"].(string)] = data
	}
	return queries
}

// TestR2QueryStatisticsRawOrdinaryStatements is the accepted Neo4j 5.26
// statistics contract for ordinary traffic: db.stats.retrieve('QUERIES')
// returns the executed statement text with its ordinary numeric, string and
// property literals raw, under both parsers.
func TestR2QueryStatisticsRawOrdinaryStatements(t *testing.T) {
	base := newTestMemoryEngine(t)
	statements := []string{
		"RETURN 530 AS value",
		"RETURN 4271 AS value, 3.5 AS ratio",
		"CREATE (n:R2Raw {name: 'ordinary-name-value', score: 4271, active: true, ratio: 3.5})",
		"MATCH (n:R2Raw) WHERE n.name = 'RETURN530 AS value' RETURN count(n) AS matched",
	}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			restore := func() {}
			if parser == config.ParserTypeANTLR {
				restore = config.WithANTLRParser()
			} else {
				restore = config.WithNornicParser()
			}
			defer restore()
			e := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
			for _, statement := range statements {
				if _, err := e.Execute(context.Background(), statement, nil); err != nil {
					t.Fatalf("ordinary statement %q failed; details withheld", statement)
				}
			}
			queries := r2StatisticsQueries(t, e)
			for _, statement := range statements {
				data, recorded := queries[statement]
				if !recorded {
					t.Errorf("statistics omitted the raw executed statement %q", statement)
					continue
				}
				summary := data["invocationSummary"].(map[string]interface{})
				if summary["invocationCount"] != int64(1) {
					t.Errorf("statement %q recorded %v invocations, want 1", statement, summary["invocationCount"])
				}
				invocations := data["invocations"].([]interface{})
				if len(invocations) != 1 {
					t.Errorf("statement %q carries %d invocations, want 1", statement, len(invocations))
				}
				timing := summary["executionTimeInUs"].(map[string]interface{})
				if timing["min"] != timing["max"] || timing["avg"] != timing["min"] {
					t.Errorf("statement %q invocation timing is inconsistent: %v", statement, timing)
				}
			}
			// The GH530 lookup is the exact-language statement Neo4j records.
			const gh530 = "MATCH (n:R2Raw) WHERE n.name = 'RETURN530 AS value' RETURN count(n) AS matched"
			recorded, found := queries[gh530]
			if !found {
				t.Fatal("the exact WHERE lookup was not recorded verbatim")
			}
			if recorded["query"] != gh530 {
				t.Error("the recorded WHERE lookup text changed")
			}
		})
	}
}

// TestR2QueryStatisticsAdministrationAbsent is the accepted credential
// contract: Neo4j 5.26 records no administration command in any database's
// QUERIES collector, so no credential the command carried reaches
// db.stats.retrieve('QUERIES') from a user database or from the system
// database.
func TestR2QueryStatisticsAdministrationAbsent(t *testing.T) {
	password, role := r2DiagnosticCanary(t), r2DiagnosticCanary(t)
	admin := []string{
		"CREATE USER r2_saved_user SET PASSWORD '" + password + "' CHANGE NOT REQUIRED",
		"ALTER USER r2_saved_user SET PASSWORD '" + password + "'",
		"CREATE ROLE " + role,
		"CREATE ALIAS r2_saved_alias FOR DATABASE nornic",
	}
	base := newTestMemoryEngine(t)
	t.Run("user-database-session", func(t *testing.T) {
		e := NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
		if _, err := e.Execute(context.Background(), "RETURN 530 AS value", nil); err != nil {
			t.Fatal("ordinary statement failed; details withheld")
		}
		for _, statement := range admin {
			administrative := StatementDatabase("nornic", statement) == "system"
			if !administrative {
				t.Fatal("canonical router did not classify the administration statement")
			}
			started := time.Now()
			generation := e.queryStatistics.start(statement, started, administrative)
			if generation != 0 {
				t.Error("administration statement entered the query collector")
			}
			// Admission excludes even a successfully executed command. The
			// public protocol test supplies the real administration handlers.
			e.queryStatistics.record(generation, statement, started, time.Microsecond, true)
		}
		raw, err := json.Marshal(r2StatisticsQueries(t, e))
		if err != nil {
			t.Fatal("statistics serialization failed")
		}
		if strings.Contains(string(raw), password) || strings.Contains(string(raw), role) {
			t.Error("user-database statistics disclosed an administration credential; capture withheld")
		}
		if strings.Contains(string(raw), "CREATE USER") || strings.Contains(string(raw), "ALTER USER") || strings.Contains(string(raw), "CREATE ROLE") || strings.Contains(string(raw), "CREATE ALIAS") {
			t.Error("user-database statistics recorded an administration command")
		}
	})
	// The system database owns none of these collectors: status and
	// retrieval report no statistics there.
	t.Run("system-database", func(t *testing.T) {
		system := NewStorageExecutor(storage.NewNamespacedEngine(base, "system"))
		for _, statement := range append([]string{"RETURN 530 AS value"}, admin...) {
			started := time.Now()
			generation := system.queryStatistics.start(statement, started, StatementDatabase("system", statement) == "system")
			if generation != 0 {
				t.Error("system database admitted a statement to query statistics")
			}
			system.queryStatistics.record(generation, statement, started, time.Microsecond, true)
		}
		columns, rows := r2StatisticsRetrieval(t, system, "QUERIES")
		if len(columns) != 2 || len(rows) != 0 {
			t.Errorf("system QUERIES retrieval returned %d columns and %d rows, want the existing signature and no rows", len(columns), len(rows))
		}
		for _, section := range []string{"ALL", "GRAPH COUNTS"} {
			columns, rows := r2StatisticsRetrieval(t, system, section)
			if len(columns) == 0 || len(rows) != 0 {
				t.Errorf("system %s retrieval returned %d rows after administration commands, want none", section, len(rows))
			}
		}
		status, err := system.Execute(context.Background(), "CALL db.stats.status()", nil)
		if err != nil {
			t.Fatal("system statistics status failed; details withheld")
		}
		if len(status.Columns) != 3 || len(status.Rows) != 0 {
			t.Errorf("system statistics status returned %d columns and %d rows, want the existing signature and no rows", len(status.Columns), len(status.Rows))
		}
	})
}

// TestR2QueryStatisticsInvocationAccounting pins the accounting a raw
// statement's entry keeps: invocation count and timing span every execution,
// retrieval does not reset them, and distinct statements stay distinct.
func TestR2QueryStatisticsInvocationAccounting(t *testing.T) {
	e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
	ctx := context.Background()
	statements := []string{"RETURN 530 AS value", "RETURN 4271 AS other"}
	for i, statement := range statements {
		for invocation := 0; invocation <= i; invocation++ {
			if _, err := e.Execute(ctx, statement, nil); err != nil {
				t.Fatal("ordinary statement failed; details withheld")
			}
		}
	}
	// Retrieval cannot reset or consume the collected entries.
	for attempt := 0; attempt < 2; attempt++ {
		queries := r2StatisticsQueries(t, e)
		if len(queries) != 2 {
			t.Fatalf("retrieval %d saw %d distinct statements, want 2", attempt+1, len(queries))
		}
		for i, statement := range statements {
			data, found := queries[statement]
			if !found {
				t.Fatalf("retrieval %d omitted %q", attempt+1, statement)
			}
			summary := data["invocationSummary"].(map[string]interface{})
			invocations := data["invocations"].([]interface{})
			if summary["invocationCount"] != int64(i+1) || len(invocations) != i+1 {
				t.Errorf("retrieval %d changed invocation accounting for %q: %v / %d", attempt+1, statement, summary["invocationCount"], len(invocations))
			}
			record := e.queryStatistics.queries[statement]
			timing := summary["executionTimeInUs"].(map[string]interface{})
			if timing["min"] != record.minimum || timing["max"] != record.maximum || timing["avg"] != record.total/record.count {
				t.Errorf("retrieval %d changed recorded timing for %q", attempt+1, statement)
			}
			for _, invocation := range invocations {
				entry := invocation.(map[string]interface{})
				if _, ok := entry["elapsedExecutionTimeInUs"].(int64); !ok {
					t.Errorf("invocation of %q lost its execution time", statement)
				}
				if _, ok := entry["startTimestampMillis"].(int64); !ok {
					t.Errorf("invocation of %q lost its start timestamp", statement)
				}
			}
		}
	}
}

// Adapted from the independent r2 real-expression regression. EXPLAIN/PROFILE
// results are deliberately outside the accepted diagnostic-output contract.
func TestR2ErrorOutputs(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		name := "plain-processor"
		if wrapped {
			name = "redacting-processor"
		}
		t.Run(name, func(t *testing.T) {
			canary := r2DiagnosticCanary(t)
			recorder := tracetest.NewSpanRecorder()
			var processor sdktrace.SpanProcessor = recorder
			if wrapped {
				processor = observability.NewRedactingSpanProcessor(processor)
			}
			provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(processor))
			previous := otel.GetTracerProvider()
			otel.SetTracerProvider(provider)
			defer func() { _ = provider.Shutdown(context.Background()); otel.SetTracerProvider(previous) }()
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "nornic"))
			for _, expression := range []string{"date", "duration"} {
				_, err := e.Execute(context.Background(), "RETURN "+expression+"('"+canary+"')", nil)
				var semantic *classifiedCypherError
				if !errors.As(err, &semantic) || semantic.code != "Neo.ClientError.Statement.SyntaxError" || semantic.detail != "InvalidArgument" {
					t.Fatal("expression error lost its type or classification; details withheld")
				}
				if !strings.Contains(err.Error(), canary) {
					t.Error("trace projection changed the caller's original error")
				}
				if !strings.Contains(err.Error(), "Text cannot be parsed to a") {
					t.Error("expression diagnostic lost its useful reason")
				}
			}
			statuses, exceptions := 0, 0
			for _, span := range recorder.Ended() {
				if span.Status().Code == codes.Error {
					statuses++
					if !strings.Contains(span.Status().Description, "Text cannot be parsed to a") {
						t.Error("span error lost its useful reason")
					}
				}
				if strings.Contains(span.Status().Description, canary) {
					t.Error("span status disclosed a literal; capture withheld")
				}
				for _, event := range span.Events() {
					for _, attr := range event.Attributes {
						if strings.Contains(attr.Value.Emit(), canary) {
							t.Error("span event disclosed a literal; capture withheld")
						}
						if string(attr.Key) == "exception.type" {
							exceptions++
							if attr.Value.AsString() != "*cypher.classifiedCypherError" {
								t.Error("span exception type changed")
							}
						}
					}
				}
			}
			if statuses != 2 || exceptions != 2 {
				t.Fatal("error status or exception event missing")
			}
			result, err := e.Execute(context.Background(), "RETURN 'ordinary-value' AS value, 123 AS number", nil)
			if err != nil || len(result.Rows) != 1 || result.Rows[0][0] != "ordinary-value" {
				t.Fatal("legitimate query result changed")
			}
		})
	}
}

type r2SpanValueError string

func (err r2SpanValueError) Error() string { return string(err) }

func TestR2SpanErrorSDKContract(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	defer func() { _ = provider.Shutdown(context.Background()) }()
	tracer := provider.Tracer("test")
	for _, err := range []error{
		errors.New("context deadline exceeded"),
		r2SpanValueError("cannot complete this operation"),
		newSemanticError("Neo.ClientError.Statement.TypeError", "InvalidArgumentType", "expected a number"),
	} {
		_, reference := tracer.Start(context.Background(), "sdk-reference")
		reference.RecordError(err)
		reference.End()
		_, projected := tracer.Start(context.Background(), "projected")
		recordSpanError(projected, err)
		projected.End()
		spans := recorder.Ended()
		before, after := spans[len(spans)-2].Events(), spans[len(spans)-1].Events()
		if len(before) != 1 || len(after) != 1 || before[0].Name != after[0].Name || len(before[0].Attributes) != len(after[0].Attributes) {
			t.Fatal("exception projection changed the SDK event contract")
		}
		for i, attr := range before[0].Attributes {
			if attr != after[0].Attributes[i] {
				t.Error("exception projection changed a public reason or original error type")
			}
		}
	}
}
