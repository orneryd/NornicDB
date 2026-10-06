package differential

import (
	"context"
	"fmt"
	"regexp"
	"sync"
	"time"
)

// Executor runs statements through one route on one server. Implementations
// are safe for concurrent use.
type Executor interface {
	// Execute runs query and returns its outcome; a failure is an Outcome
	// with a Code, never an error of the harness.
	Execute(ctx context.Context, query string) Outcome
	// Reset empties the graph, indexes and constraints included.
	Reset(ctx context.Context) error
}

// Result is one statement's outcome on both servers. A graph-state result
// (Kind "graph") compares the graph after a statement that writes; its ID is
// the statement's with a ":graph" suffix.
type Result struct {
	ID       string  `json:"id"`
	Issue    int     `json:"issue"`
	Kind     string  `json:"kind"`
	Query    string  `json:"query"`
	Neo4j    Outcome `json:"neo4j"`
	NornicDB Outcome `json:"nornicdb"`
	Match    bool    `json:"match"`
}

// Options bound a run: each statement's deadline, the number of statements
// read in parallel, how often a reset is retried after a transient error, and
// how often a read whose rows came back in another order runs again
// (rerunOrder).
type Options struct {
	StatementTimeout time.Duration
	Workers          int
	ResetAttempts    int
	OrderRetries     int
}

// DefaultOptions keep a full run within a CI job's budget: no statement can
// hold the run longer than its timeout.
var DefaultOptions = Options{StatementTimeout: 20 * time.Second, Workers: 4, ResetAttempts: 3, OrderRetries: 2}

// Pair is a route's two servers.
type Pair struct {
	Neo4j    Executor
	NornicDB Executor
	// ResetRetries counts the resets retried after a transient error (#732's
	// race), reported with the run.
	ResetRetries int
	mu           sync.Mutex
}

func (pair *Pair) execute(ctx context.Context, options Options, query string) (Outcome, Outcome) {
	var reference, actual Outcome
	var wait sync.WaitGroup
	wait.Add(2)
	run := func(executor Executor, into *Outcome) {
		defer wait.Done()
		statementCtx, cancel := context.WithTimeout(ctx, options.StatementTimeout)
		defer cancel()
		*into = executor.Execute(statementCtx, query)
	}
	go run(pair.Neo4j, &reference)
	go run(pair.NornicDB, &actual)
	wait.Wait()
	return reference, actual
}

var callClause = regexp.MustCompile(`(?i)\bCALL\b`)

// rerunOrder runs a statement again on NornicDB, up to options.OrderRetries
// times, when its rows are Neo4j's in another order, it sorts on a key it
// doesn't return, and it can't write. Rows that tie under such a key have no
// defined order (in Neo4j either) and nothing shows which rows tie, so a run
// in Neo4j's order shows the rows are the same. It returns the outcome to
// report and whether it agrees with reference.
func (pair *Pair) rerunOrder(ctx context.Context, options Options, query string, reference, actual Outcome) (Outcome, bool) {
	if !OrderOnly(query, reference, actual) || returnShape(query, reference.Columns).sortKeys != nil ||
		writeClause.MatchString(query) || callClause.MatchString(query) {
		return actual, false
	}
	for attempt := 0; attempt < options.OrderRetries; attempt++ {
		statementCtx, cancel := context.WithTimeout(ctx, options.StatementTimeout)
		again := pair.NornicDB.Execute(statementCtx, query)
		cancel()
		if Same(query, reference, again) {
			return again, true
		}
	}
	return actual, false
}

// compare is a statement's Result: its outcomes, run again when only their
// order differs (rerunOrder).
func (pair *Pair) compare(ctx context.Context, options Options, id string, issue int, query string, reference, actual Outcome) Result {
	match := Same(query, reference, actual)
	if !match {
		actual, match = pair.rerunOrder(ctx, options, query, reference, actual)
	}
	return Result{ID: id, Issue: issue, Kind: "statement", Query: query, Neo4j: reference, NornicDB: actual, Match: match}
}

// reset empties both graphs and runs setup on both. A setup statement that
// fails is a harness error: the run stops instead of comparing a partial
// graph.
func (pair *Pair) reset(ctx context.Context, options Options, setup []string) error {
	for name, executor := range map[string]Executor{"Neo4j": pair.Neo4j, "NornicDB": pair.NornicDB} {
		var err error
		for attempt := 0; attempt < max(1, options.ResetAttempts); attempt++ {
			resetCtx, cancel := context.WithTimeout(ctx, options.StatementTimeout)
			err = executor.Reset(resetCtx)
			cancel()
			if err == nil {
				break
			}
			pair.mu.Lock()
			pair.ResetRetries++
			pair.mu.Unlock()
		}
		if err != nil {
			return fmt.Errorf("reset %s: %w", name, err)
		}
	}
	for _, statement := range setup {
		reference, actual := pair.execute(ctx, options, statement)
		if reference.Failed() || actual.Failed() {
			return fmt.Errorf("setup %q: Neo4j %s %s, NornicDB %s %s", statement, reference.Code, reference.Message, actual.Code, actual.Message)
		}
	}
	return nil
}

func (pair *Pair) graphState(ctx context.Context, options Options, id string, issue int, query string) Result {
	result := Result{ID: id + ":graph", Issue: issue, Kind: "graph", Query: query, Match: true}
	for _, stateQuery := range GraphStateQueries {
		reference, actual := pair.execute(ctx, options, stateQuery)
		if !SameGraphState(reference, actual) {
			result.Neo4j, result.NornicDB, result.Match = reference, actual, false
			return result
		}
	}
	return result
}

// RunSweep compares every sweep case on pair. The read-only cases run first,
// options.Workers at a time, on Setup's graph; each case that writes then runs
// alone, with its graph state compared, and the graph is set up again after
// it.
func RunSweep(ctx context.Context, corpus SweepCorpus, pair *Pair, options Options) ([]Result, error) {
	if err := pair.reset(ctx, options, corpus.Setup); err != nil {
		return nil, err
	}
	results := make([]Result, 0, len(corpus.Cases)+len(corpus.Cases)/50)
	var reads []SweepCase
	var writes []SweepCase
	for _, sweepCase := range corpus.Cases {
		if sweepCase.Rollback {
			writes = append(writes, sweepCase)
		} else {
			reads = append(reads, sweepCase)
		}
	}
	readResults := make([]Result, len(reads))
	jobs := make(chan int)
	var wait sync.WaitGroup
	for worker := 0; worker < max(1, options.Workers); worker++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range jobs {
				sweepCase := reads[index]
				reference, actual := pair.execute(ctx, options, sweepCase.Query)
				readResults[index] = Result{ID: sweepCase.ID, Issue: SweepIssue, Kind: "statement", Query: sweepCase.Query,
					Neo4j: reference, NornicDB: actual, Match: Same(sweepCase.Query, reference, actual)}
			}
		}()
	}
	for index := range reads {
		if ctx.Err() != nil {
			break
		}
		jobs <- index
	}
	close(jobs)
	wait.Wait()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	results = append(results, readResults...)
	for _, sweepCase := range writes {
		reference, actual := pair.execute(ctx, options, sweepCase.Query)
		results = append(results,
			Result{ID: sweepCase.ID, Issue: SweepIssue, Kind: "statement", Query: sweepCase.Query,
				Neo4j: reference, NornicDB: actual, Match: Same(sweepCase.Query, reference, actual)},
			pair.graphState(ctx, options, sweepCase.ID, SweepIssue, sweepCase.Query))
		if err := pair.reset(ctx, options, corpus.Setup); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// RunIssues compares every issue's statements on pair, issue by issue, each
// on an empty graph and in the issue's order. After a statement that writes,
// the graph state is compared too.
func RunIssues(ctx context.Context, issues []IssueCase, pair *Pair, options Options) ([]Result, error) {
	var results []Result
	for _, issue := range issues {
		if err := pair.reset(ctx, options, nil); err != nil {
			return nil, fmt.Errorf("issue #%d: %w", issue.Issue, err)
		}
		for _, statement := range issue.Statements {
			reference, actual := pair.execute(ctx, options, statement.Query)
			results = append(results, pair.compare(ctx, options, statement.ID, issue.Issue, statement.Query, reference, actual))
			if Writes(statement.Query) {
				results = append(results, pair.graphState(ctx, options, statement.ID, issue.Issue, statement.Query))
			}
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	return results, nil
}

// RunCorpora runs the sweep and the issue reproductions on pair, as one
// differential.Assert run: their results, and the resets retried so far.
func RunCorpora(ctx context.Context, pair *Pair, sweep SweepCorpus, issues []IssueCase, logf func(string, ...any)) ([]Result, int, error) {
	started := time.Now()
	results, err := RunSweep(ctx, sweep, pair, DefaultOptions)
	if err != nil {
		return nil, pair.ResetRetries, fmt.Errorf("sweep: %w", err)
	}
	issueResults, err := RunIssues(ctx, issues, pair, DefaultOptions)
	if err != nil {
		return nil, pair.ResetRetries, fmt.Errorf("issue reproductions: %w", err)
	}
	logf("%d sweep and %d issue results in %s", len(results), len(issueResults), time.Since(started).Round(time.Second))
	return append(results, issueResults...), pair.ResetRetries, nil
}
