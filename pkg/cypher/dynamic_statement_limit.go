package cypher

import (
	"sync/atomic"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// DefaultDynamicStatementLimit is the most statement text, in bytes, a
// procedure may be given to run (apoc.cypher.run, runMany, doIt, the
// apoc.periodic bodies, apoc.export.*.query): the HTTP request limit's
// default. Such text is data the statement built or read, not the client's
// statement, so the request limits don't bound it; this limit does, and with
// it the work of parsing it, which a cancellation does not interrupt.
const DefaultDynamicStatementLimit int64 = 10 << 20

var dynamicStatementLimit atomic.Int64

func init() { dynamicStatementLimit.Store(DefaultDynamicStatementLimit) }

// SetDynamicStatementLimit sets the limit on statement text a procedure runs
// (the server sets its request limit); 0 or less restores the default.
func SetDynamicStatementLimit(bytes int64) {
	if bytes <= 0 {
		bytes = DefaultDynamicStatementLimit
	}
	dynamicStatementLimit.Store(bytes)
}

// DynamicStatementLimit is the limit on statement text a procedure runs.
func DynamicStatementLimit() int64 { return dynamicStatementLimit.Load() }

// requiredStatementText reads statement text a procedure runs (a STRING
// argument it needs), held to the limit before anything reads it.
func requiredStatementText(procedure string, args []interface{}, index int, argument string) (string, error) {
	text, err := requiredProcedureString(procedure, args, index, argument)
	if err != nil {
		return "", err
	}
	return text, dynamicStatementTextError(procedure, text)
}

// dynamicStatementTextError refuses statement text a procedure was given to
// run that is longer than the limit, before anything reads it.
func dynamicStatementTextError(procedure, text string) error {
	if limit := dynamicStatementLimit.Load(); int64(len(text)) > limit {
		return localizedStatusError("Neo.ClientError.Statement.ArgumentError", "InvalidArgumentValue",
			localization.CypherProceduresStatementTooLong(procedure, int64(len(text)), limit))
	}
	return nil
}
