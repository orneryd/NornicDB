package storage

import "context"

// RemoteStatementRunner runs one statement of a remote engine's operations,
// and returns its columns and rows.
type RemoteStatementRunner func(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error)

// WithStatementRunner is the remote engine whose every operation runs its
// statements through run instead of a session of its own: a composite
// statement's remote participant runs them in the statement's remote
// transaction, so nothing an operation writes is kept unless the statement
// commits. A transaction begun on it joins run's: it ends with whatever run
// belongs to, not with its own Commit or Rollback. The engine is issued to
// owner (StatementOwner).
func (r *RemoteEngine) WithStatementRunner(owner any, run RemoteStatementRunner) *RemoteEngine {
	return &RemoteEngine{transport: runnerTransport{run: run}, schema: r.schema, owner: owner}
}

// NewStatementRemoteEngine is a remote engine of its own schema, issued to
// owner, whose every operation runs its statements through run
// (WithStatementRunner).
func NewStatementRemoteEngine(owner any, run RemoteStatementRunner) *RemoteEngine {
	return &RemoteEngine{transport: runnerTransport{run: run}, schema: NewSchemaManager(), owner: owner}
}

// StatementOwner is the statement the engine was issued to
// (WithStatementRunner), or nil for an engine of its own.
func (r *RemoteEngine) StatementOwner() any { return r.owner }

// runnerTransport is the transport of a remote engine bound to a statement
// runner (RemoteEngine.WithStatementRunner).
type runnerTransport struct {
	run RemoteStatementRunner
}

func (t runnerTransport) query(ctx context.Context, statement string, params map[string]interface{}, mode RemoteAccessMode) ([]string, [][]interface{}, error) {
	return t.run(ctx, statement, params, mode)
}

func (t runnerTransport) queryBatch(ctx context.Context, statements []remoteStatement) error {
	for _, stmt := range statements {
		if _, _, err := t.run(ctx, stmt.Statement, stmt.Parameters, RemoteAccessWrite); err != nil {
			return err
		}
	}
	return nil
}

func (t runnerTransport) close() error { return nil }

// joinedCypherTx is a transaction begun on a remote engine bound to a
// statement runner: its statements run in the runner's transaction, which
// ends with its owner.
type joinedCypherTx struct {
	mode RemoteAccessMode
	run  RemoteStatementRunner
}

func (t joinedCypherTx) QueryCypher(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	return t.run(ctx, statement, params, t.mode)
}

func (joinedCypherTx) Commit(context.Context) error { return nil }

func (joinedCypherTx) Rollback(context.Context) error { return nil }
