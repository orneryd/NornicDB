package differential

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadCorpora(t *testing.T) {
	dir := t.TempDir()
	sweepPath := filepath.Join(dir, "sweep.json.gz")
	file, err := os.Create(sweepPath)
	require.NoError(t, err)
	writer := gzip.NewWriter(file)
	require.NoError(t, json.NewEncoder(writer).Encode(SweepCorpus{Setup: []string{"CREATE ()"}, Cases: []SweepCase{{ID: "a", Query: "RETURN 1 AS v"}}}))
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	sweep, err := LoadSweep(sweepPath)
	require.NoError(t, err)
	require.Equal(t, []string{"CREATE ()"}, sweep.Setup)
	require.Len(t, sweep.Cases, 1)

	_, err = LoadSweep(filepath.Join(dir, "missing.json.gz"))
	require.Error(t, err)
	plain := filepath.Join(dir, "plain.json.gz")
	require.NoError(t, os.WriteFile(plain, []byte("{}"), 0o644))
	_, err = LoadSweep(plain)
	require.ErrorContains(t, err, "read")
	broken := filepath.Join(dir, "broken.json.gz")
	file, err = os.Create(broken)
	require.NoError(t, err)
	writer = gzip.NewWriter(file)
	_, err = writer.Write([]byte("{"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	_, err = LoadSweep(broken)
	require.ErrorContains(t, err, "decode")
	empty := filepath.Join(dir, "empty.json.gz")
	file, err = os.Create(empty)
	require.NoError(t, err)
	writer = gzip.NewWriter(file)
	require.NoError(t, json.NewEncoder(writer).Encode(SweepCorpus{}))
	require.NoError(t, writer.Close())
	require.NoError(t, file.Close())
	_, err = LoadSweep(empty)
	require.ErrorContains(t, err, "no cases")

	issuesPath := filepath.Join(dir, "issues.json")
	require.NoError(t, os.WriteFile(issuesPath, []byte(`[{"issue": 1, "title": "t", "statements": [{"id": "x", "query": "RETURN 1"}]}]`), 0o644))
	issues, err := LoadIssues(issuesPath)
	require.NoError(t, err)
	require.Equal(t, 1, issues[0].Issue)
	_, err = LoadIssues(filepath.Join(dir, "missing.json"))
	require.Error(t, err)
	require.NoError(t, os.WriteFile(issuesPath, []byte(`{`), 0o644))
	_, err = LoadIssues(issuesPath)
	require.ErrorContains(t, err, "decode")
	require.NoError(t, os.WriteFile(issuesPath, []byte(`[]`), 0o644))
	_, err = LoadIssues(issuesPath)
	require.ErrorContains(t, err, "no issues")
}

func rows(values ...[]any) Outcome { return Outcome{Columns: []string{"v"}, Rows: values} }

func TestSame(t *testing.T) {
	failed := Outcome{Code: "Neo.ClientError.Statement.SyntaxError", Message: "a"}
	require.True(t, Same("RETURN x", failed, Outcome{Code: failed.Code, Message: "b"}), "codes agree, texts may not")
	require.False(t, Same("RETURN x", failed, Outcome{Code: "Neo.ClientError.Statement.TypeError"}))
	require.False(t, Same("RETURN x", failed, rows()))
	require.False(t, Same("RETURN x", rows(), Outcome{Columns: []string{"w"}}))
	require.False(t, Same("RETURN x", rows(), Outcome{Columns: []string{"v", "w"}}))
	require.False(t, Same("RETURN x", rows([]any{1}), rows()))
	require.True(t, Same("UNWIND [1, 2] AS x RETURN x", rows([]any{1}, []any{2}), rows([]any{2}, []any{1})), "rows are a multiset")
	require.False(t, Same("UNWIND [1, 2] AS x RETURN x ORDER BY x", rows([]any{1}, []any{2}), rows([]any{2}, []any{1})), "ORDER BY keeps order")
	require.True(t, Same("RETURN collect(x) AS v", rows([]any{[]any{1, 2}}), rows([]any{[]any{2, 1}})), "collect() has no order")
	require.False(t, Same("WITH x ORDER BY x RETURN collect(x) AS v", rows([]any{[]any{1, 2}}), rows([]any{[]any{2, 1}})))
	require.True(t, Same("RETURN collect({a: [2, 1]}) AS v", rows([]any{[]any{map[string]any{"a": []any{1, 2}}}}), rows([]any{[]any{map[string]any{"a": []any{2, 1}}}})))
	require.True(t, Same("MATCH (n) WITH n LIMIT 1 RETURN n.id AS v", rows([]any{1}), rows([]any{2})), "arbitrary rows compare by count")
	require.False(t, Same("MATCH (n) WITH n LIMIT 1 RETURN n.id AS v", rows([]any{1}), rows()))
	require.True(t, SameGraphState(rows([]any{[]any{"A", "B"}}), rows([]any{[]any{"B", "A"}})))
	require.Equal(t, "unencodable", canonical(make(chan int), false))
}

func TestArbitraryAndWrites(t *testing.T) {
	for query, arbitrary := range map[string]bool{
		"MATCH (n) WITH n LIMIT 1 SET n.x = 1":                   true,
		"MATCH (n) RETURN n SKIP 1":                              true,
		"MATCH (n) WITH n ORDER BY n.id LIMIT 1 SET n.x = 1":     false,
		"MATCH (n) RETURN n ORDER BY n.id SKIP 1 LIMIT 1":        false,
		"MATCH (n) WITH n ORDER BY n.id WITH n LIMIT 1 RETURN n": true,
		"MATCH (n) RETURN n":                                     false,
	} {
		require.Equal(t, arbitrary, Arbitrary(query), query)
	}
	require.True(t, Writes("CREATE (n)"))
	require.False(t, Writes("MATCH (n) RETURN n"))
	require.False(t, Writes("MATCH (n) WITH n LIMIT 1 SET n.x = 1"))
}

func TestRatchetCheckAndUpdate(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "known.jsonl")
	ratchet, err := LoadRatchet(path)
	require.NoError(t, err)
	require.Empty(t, ratchet)
	require.NoError(t, os.WriteFile(path, []byte("{\"id\":\"a\",\"route\":\"r\",\"issue\":1}\n\n{\"id\":\"u\",\"route\":\"r\",\"issue\":1,\"unstable\":true}\n{\"id\":\"o\",\"route\":\"other\",\"issue\":2}\n"), 0o644))
	ratchet, err = LoadRatchet(path)
	require.NoError(t, err)
	report := ratchet.Check("r", []Result{
		{ID: "a"},              // known difference
		{ID: "b"},              // new difference
		{ID: "u", Match: true}, // unstable
		{ID: "c", Match: true}, // matches, not listed
	})
	require.Equal(t, 1, report.Known)
	require.Equal(t, 1, report.Unstable)
	require.Len(t, report.Regressed, 1)
	require.Equal(t, "b", report.Regressed[0].ID)
	require.Empty(t, report.Stale)
	report = ratchet.Check("r", []Result{{ID: "a", Match: true}})
	require.Equal(t, []Entry{{ID: "a", Route: "r", Issue: 1}}, report.NowMatches)
	require.Equal(t, []Entry{{ID: "u", Route: "r", Issue: 1, Unstable: true}}, report.Stale, "a listed statement that no longer runs")

	require.NoError(t, ratchet.Update(path, "r", [][]Result{
		{{ID: "a", Issue: 1, Match: true}, {ID: "b", Issue: 3}, {ID: "f", Issue: 4}},
		{{ID: "a", Issue: 1, Match: true}, {ID: "b", Issue: 3}, {ID: "f", Issue: 4, Match: true}},
	}))
	content, err := os.ReadFile(path)
	require.NoError(t, err)
	// The route's entries are rebuilt from the runs: u, which didn't run, goes.
	require.Equal(t, "{\"id\":\"o\",\"route\":\"other\",\"issue\":2}\n"+
		"{\"id\":\"b\",\"route\":\"r\",\"issue\":3}\n"+
		"{\"id\":\"f\",\"route\":\"r\",\"issue\":4,\"unstable\":true}\n", string(content))
	require.NoError(t, Ratchet{}.Update(filepath.Join(dir, "new.jsonl"), "r", [][]Result{{{ID: "z", Issue: 5}, {ID: "y", Issue: 5}}}))
	content, err = os.ReadFile(filepath.Join(dir, "new.jsonl"))
	require.NoError(t, err)
	require.Equal(t, "{\"id\":\"y\",\"route\":\"r\",\"issue\":5}\n{\"id\":\"z\",\"route\":\"r\",\"issue\":5}\n", string(content))

	require.NoError(t, os.WriteFile(path, []byte("not json\n"), 0o644))
	_, err = LoadRatchet(path)
	require.ErrorContains(t, err, "not a ratchet entry")
	_, err = LoadRatchet(dir)
	require.Error(t, err)
	_, err = LoadRatchet(filepath.Join(path, "below-a-file"))
	require.Error(t, err)
	require.Error(t, Ratchet{}.Update(filepath.Join(dir, "missing", "x.jsonl"), "r", nil))
}

// recorder is a testing.TB that records failures instead of failing.
type recorder struct {
	testing.TB
	errors []string
	fatal  string
	logs   []string
}

func (r *recorder) Helper() {}
func (r *recorder) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}
func (r *recorder) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}
func (r *recorder) Fatalf(format string, args ...any) {
	r.fatal = fmt.Sprintf(format, args...)
	panic(r)
}

func assertRecorded(t *testing.T, path, route string, run func() ([]Result, int, error)) (record *recorder) {
	record = &recorder{TB: t}
	defer func() {
		if recovered := recover(); recovered != nil && recovered != record {
			panic(recovered)
		}
	}()
	Assert(record, path, route, run)
	return record
}

func TestAssert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "known.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("{\"id\":\"known\",\"route\":\"r\",\"issue\":1}\n{\"id\":\"fixed\",\"route\":\"r\",\"issue\":1}\n{\"id\":\"gone\",\"route\":\"r\",\"issue\":1}\n{\"id\":\"also-gone\",\"route\":\"r\",\"issue\":1}\n"), 0o644))
	var results []Result
	results = append(results, Result{ID: "known"}, Result{ID: "fixed", Match: true})
	for index := 0; index < maxReported+1; index++ {
		results = append(results, Result{ID: fmt.Sprintf("new%d", index), Query: "RETURN 1", Neo4j: Outcome{Code: "x"}, NornicDB: Outcome{Code: strings.Repeat("y", 700)}})
	}
	record := assertRecorded(t, path, "r", func() ([]Result, int, error) { return results, 2, nil })
	require.Len(t, record.errors, maxReported+1)
	require.Contains(t, record.errors[0], "differs from Neo4j")
	require.Contains(t, record.errors[maxReported], "1 more")
	require.Contains(t, strings.Join(record.logs, "\n"), "regressed=41 now_match=1 stale=2 reset_retries=2")
	require.Contains(t, strings.Join(record.logs, "\n"), "remove from the ratchet (matches Neo4j now)")
	require.Contains(t, strings.Join(record.logs, "\n"), "remove from the ratchet (no longer compared)")

	many := make([]Result, 0, maxReported+2)
	content := ""
	for index := 0; index < maxReported+2; index++ {
		id := fmt.Sprintf("m%d", index)
		content += fmt.Sprintf("{\"id\":%q,\"route\":\"r\",\"issue\":1}\n", id)
		many = append(many, Result{ID: id, Match: true})
	}
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))
	record = assertRecorded(t, path, "r", func() ([]Result, int, error) { return many, 0, nil })
	require.Empty(t, record.errors)
	require.Contains(t, strings.Join(record.logs, "\n"), "2 more (matches Neo4j now)")

	record = assertRecorded(t, path, "r", func() ([]Result, int, error) { return nil, 0, errors.New("server down") })
	require.Contains(t, record.fatal, "server down")
	require.NoError(t, os.WriteFile(path, []byte("bad\n"), 0o644))
	record = assertRecorded(t, path, "r", func() ([]Result, int, error) { return nil, 0, nil })
	require.Contains(t, record.fatal, "load differential ratchet")

	t.Setenv(UpdateRatchetEnv, "1")
	require.NoError(t, os.WriteFile(path, nil, 0o644))
	runs := 0
	record = assertRecorded(t, path, "r", func() ([]Result, int, error) {
		runs++
		return []Result{{ID: "d", Issue: 7}}, 0, nil
	})
	require.Equal(t, UpdateRuns, runs)
	require.Empty(t, record.fatal)
	written, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "{\"id\":\"d\",\"route\":\"r\",\"issue\":7}\n", string(written))
	record = assertRecorded(t, path, "r", func() ([]Result, int, error) { return nil, 0, errors.New("crashed") })
	require.Contains(t, record.fatal, "crashed")
	record = assertRecorded(t, filepath.Join(t.TempDir(), "missing", "known.jsonl"), "r", func() ([]Result, int, error) { return nil, 0, nil })
	require.Contains(t, record.fatal, "update differential ratchet")
	require.Equal(t, "1", mustJSON(1))
	require.Contains(t, mustJSON(make(chan int)), "0x")
}

// fakeServer answers statements from a table and records what ran.
type fakeServer struct {
	mu         sync.Mutex
	answers    map[string]Outcome
	ran        []string
	resetFails int
	// failResetsAfter, when set, fails every reset after that many.
	failResetsAfter int
	resets          int
}

func (server *fakeServer) Execute(ctx context.Context, query string) Outcome {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.ran = append(server.ran, query)
	if answer, ok := server.answers[query]; ok {
		return answer
	}
	return Outcome{Columns: []string{"v"}, Rows: [][]any{{query}}}
}

func (server *fakeServer) Reset(context.Context) error {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.resets++
	if server.failResetsAfter > 0 && server.resets > server.failResetsAfter {
		return errors.New("down")
	}
	if server.resetFails > 0 {
		server.resetFails--
		return errors.New("transient")
	}
	return nil
}

func TestRunSweepAndIssues(t *testing.T) {
	neo4j := &fakeServer{answers: map[string]Outcome{"RETURN 2 AS v": rows([]any{2})}}
	nornic := &fakeServer{answers: map[string]Outcome{"RETURN 2 AS v": rows([]any{3})}, resetFails: 1}
	pair := &Pair{Neo4j: neo4j, NornicDB: nornic}
	options := Options{StatementTimeout: time.Second, Workers: 2, ResetAttempts: 2}
	sweep := SweepCorpus{Setup: []string{"CREATE (:S)"}, Cases: []SweepCase{
		{ID: "one", Query: "RETURN 1 AS v"},
		{ID: "two", Query: "RETURN 2 AS v"},
		{ID: "write", Query: "CREATE (n:W)", Rollback: true},
	}}
	results, err := RunSweep(context.Background(), sweep, pair, options)
	require.NoError(t, err)
	require.Equal(t, 1, pair.ResetRetries)
	byID := map[string]Result{}
	for _, result := range results {
		byID[result.ID] = result
	}
	require.True(t, byID["one"].Match)
	require.False(t, byID["two"].Match)
	require.True(t, byID["write"].Match)
	require.True(t, byID["write:graph"].Match)
	require.Equal(t, SweepIssue, byID["two"].Issue)

	neo4j.answers[GraphStateQueries[0]] = rows([]any{"a"})
	issues := []IssueCase{{Issue: 12, Statements: []IssueStatement{{ID: "c", Query: "CREATE (:I)"}, {ID: "r", Query: "MATCH (n) RETURN n"}}}}
	issueResults, err := RunIssues(context.Background(), issues, pair, options)
	require.NoError(t, err)
	require.Len(t, issueResults, 3)
	require.Equal(t, "c:graph", issueResults[1].ID)
	require.False(t, issueResults[1].Match)
	require.Equal(t, 12, issueResults[1].Issue)

	var logged []string
	all, retries, err := RunCorpora(context.Background(), pair, sweep, issues, func(format string, args ...any) { logged = append(logged, fmt.Sprintf(format, args...)) })
	require.NoError(t, err)
	require.Len(t, all, len(results)+len(issueResults))
	require.Equal(t, 1, retries)
	require.Contains(t, logged[0], "sweep and 3 issue results")
}

func TestRunFailures(t *testing.T) {
	options := Options{StatementTimeout: time.Second, Workers: 1, ResetAttempts: 1}
	broken := &Pair{Neo4j: &fakeServer{}, NornicDB: &fakeServer{resetFails: 5}}
	_, err := RunSweep(context.Background(), SweepCorpus{Cases: []SweepCase{{ID: "a", Query: "RETURN 1"}}}, broken, options)
	require.ErrorContains(t, err, "reset NornicDB")
	_, err = RunIssues(context.Background(), []IssueCase{{Issue: 3}}, broken, options)
	require.ErrorContains(t, err, "issue #3")
	_, _, err = RunCorpora(context.Background(), broken, SweepCorpus{}, nil, t.Logf)
	require.ErrorContains(t, err, "sweep")

	setupFails := &Pair{Neo4j: &fakeServer{}, NornicDB: &fakeServer{answers: map[string]Outcome{"CREATE ()": {Code: "x"}}}}
	_, err = RunSweep(context.Background(), SweepCorpus{Setup: []string{"CREATE ()"}}, setupFails, options)
	require.ErrorContains(t, err, "setup")
	_, err = RunSweep(context.Background(), SweepCorpus{Setup: []string{"CREATE ()"}, Cases: []SweepCase{{ID: "w", Query: "CREATE (n)", Rollback: true}}},
		&Pair{Neo4j: &fakeServer{}, NornicDB: &onceServer{fakeServer: fakeServer{}}}, options)
	require.ErrorContains(t, err, "setup")

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	healthy := &Pair{Neo4j: &fakeServer{}, NornicDB: &fakeServer{}}
	_, err = RunSweep(cancelled, SweepCorpus{Cases: []SweepCase{{ID: "a", Query: "RETURN 1"}}}, healthy, options)
	require.ErrorIs(t, err, context.Canceled)
	_, err = RunIssues(cancelled, []IssueCase{{Issue: 1, Statements: []IssueStatement{{ID: "a", Query: "RETURN 1"}}}}, healthy, options)
	require.ErrorIs(t, err, context.Canceled)
	_, _, err = RunCorpora(context.Background(), healthy, SweepCorpus{}, []IssueCase{{Issue: 2}}, t.Logf)
	require.NoError(t, err)
	failingIssues := &Pair{Neo4j: &fakeServer{}, NornicDB: &fakeServer{failResetsAfter: 1}}
	_, _, err = RunCorpora(context.Background(), failingIssues, SweepCorpus{}, []IssueCase{{Issue: 2}}, t.Logf)
	require.ErrorContains(t, err, "issue reproductions")
}

// onceServer fails every setup statement after the first reset, as a server
// that loses its graph between write cases.
type onceServer struct {
	fakeServer
	setups int
}

func (server *onceServer) Execute(ctx context.Context, query string) Outcome {
	if query == "CREATE ()" {
		server.setups++
		if server.setups > 1 {
			return Outcome{Code: "x"}
		}
	}
	return server.fakeServer.Execute(ctx, query)
}
