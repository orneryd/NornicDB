package server

import (
	"context"
	"regexp"
	"strings"
	"sync"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

type huntH5Errors struct {
	mu     sync.Mutex
	counts map[string]int
	first  map[string]string
}

func (e *huntH5Errors) add(code, query, message string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.counts == nil {
		e.counts, e.first = map[string]int{}, map[string]string{}
	}
	key := code + " | " + query
	e.counts[key]++
	if _, ok := e.first[key]; !ok {
		e.first[key] = message
	}
}

func huntTransient(err error) bool {
	var status *neo4jdriver.Neo4jError
	if asNeo4jError(err, &status) {
		return strings.HasPrefix(status.Code, "Neo.TransientError.")
	}
	return false
}

func asNeo4jError(err error, target **neo4jdriver.Neo4jError) bool {
	for err != nil {
		if typed, ok := err.(*neo4jdriver.Neo4jError); ok {
			*target = typed
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}

func huntCode(err error) string {
	var status *neo4jdriver.Neo4jError
	if asNeo4jError(err, &status) {
		return status.Code
	}
	if strings.Contains(err.Error(), "deadline") || strings.Contains(err.Error(), "timeout") {
		return "client:timeout"
	}
	return "client:" + huntFirstLine(err.Error())
}

func huntTx(h *huntServer, errs *huntH5Errors, label string, fn func(ctx context.Context, tx neo4jdriver.ExplicitTransaction) error) error {
	var last error
	for attempt := 0; attempt < 20; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
		tx, err := session.BeginTransaction(ctx)
		if err == nil {
			err = fn(ctx, tx)
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(context.Background())
			}
		}
		_ = session.Close(context.Background())
		cancel()
		if err == nil {
			return nil
		}
		last = err
		if !huntTransient(err) {
			errs.add(huntCode(err), label, err.Error())
			return err
		}
		time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
	}
	errs.add("transient retries exhausted", label, last.Error())
	return last
}

func huntRun(ctx context.Context, tx neo4jdriver.ExplicitTransaction, query string, params map[string]any) ([]*neo4jdriver.Record, error) {
	result, err := tx.Run(ctx, query, params)
	if err != nil {
		return nil, err
	}
	return result.Collect(ctx)
}

var huntNondeterministicRE = regexp.MustCompile(`(?i)\b(rand|randomUUID|timestamp|elementId|id|linenumber|file)\s*\(|\b(datetime|date|time|localtime|localdatetime)(\.(realtime|statement|transaction))?\s*\(\s*\)|\bdatetime\.|\bdb\.info\b|\bdbms\.|SHOW\s+(TRANSACTIONS|DATABASES?|SETTINGS|PROCEDURES|FUNCTIONS|INDEXES|CONSTRAINTS|USERS|ROLES)`)

func huntValuesComparable(query string) bool { return !huntNondeterministicRE.MatchString(query) }

func huntAgree(query string, a, b differential.Outcome) (bool, string) {
	switch {
	case a.Failed() && b.Failed():
		if a.Code == b.Code {
			return true, ""
		}
		return false, "error class differs"
	case a.Failed() != b.Failed():
		return false, "one path failed, the other returned rows"
	}
	if !huntValuesComparable(query) {
		if strings.Join(a.Columns, "\x00") == strings.Join(b.Columns, "\x00") && len(a.Rows) == len(b.Rows) {
			return true, ""
		}
		return false, "columns or row count differ (values not comparable)"
	}
	if differential.Same(query, a, b) {
		return true, ""
	}
	return false, "rows differ"
}

