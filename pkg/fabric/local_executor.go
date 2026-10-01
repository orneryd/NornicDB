package fabric

import (
	"context"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/storage"
)

// CypherExecutor is the interface for executing Cypher queries against a storage engine.
// It decouples the fabric package from the concrete cypher.StorageExecutor to avoid
// circular imports.
type CypherExecutor interface {
	// ExecuteQuery runs a Cypher query against a storage engine and returns columns + rows.
	ExecuteQuery(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error)
	// ExecuteQueryWithRecord runs a Cypher query with correlated record bindings.
	ExecuteQueryWithRecord(ctx context.Context, dbName string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, [][]interface{}, error)
}

// LocalFragmentExecutor executes fragments against a local storage engine
// via the existing Cypher executor infrastructure.
type LocalFragmentExecutor struct {
	cypherExec CypherExecutor
	getEngine  func(dbName string) (storage.Engine, error)
}

type recordQueryExecutor interface {
	ExecuteRecordQuery(context.Context, string, string, map[string]interface{}, map[string]interface{}) ([]string, [][]interface{}, bool, error)
}

// NewLocalFragmentExecutor creates a local executor.
//
// Parameters:
//   - cypherExec: the Cypher query executor
//   - getEngine: function to resolve a database name to a storage.Engine
func NewLocalFragmentExecutor(cypherExec CypherExecutor, getEngine func(string) (storage.Engine, error)) *LocalFragmentExecutor {
	return &LocalFragmentExecutor{
		cypherExec: cypherExec,
		getEngine:  getEngine,
	}
}

// Execute runs a Cypher query against a local database.
func (l *LocalFragmentExecutor) Execute(ctx context.Context, loc *LocationLocal, query string, params map[string]interface{}) (*ResultStream, error) {
	return l.ExecuteWithRecord(ctx, loc, query, params, nil)
}

// ExecuteRows runs a Cypher query and returns a row iterator.
func (l *LocalFragmentExecutor) ExecuteRows(ctx context.Context, loc *LocationLocal, query string, params map[string]interface{}) ([]string, RowIterator, error) {
	return l.ExecuteWithRecordRows(ctx, loc, query, params, nil)
}

// ExecuteWithRecord runs a Cypher query against a local database with optional correlated bindings.
func (l *LocalFragmentExecutor) ExecuteWithRecord(ctx context.Context, loc *LocationLocal, query string, params map[string]interface{}, recordBindings map[string]interface{}) (*ResultStream, error) {
	if executor, ok := l.cypherExec.(recordQueryExecutor); ok {
		columns, rows, handled, err := executor.ExecuteRecordQuery(ctx, loc.DBName, query, params, recordBindings)
		if handled {
			if err != nil {
				return nil, fmt.Errorf("local execution on '%s' failed: %w", loc.DBName, err)
			}
			return &ResultStream{Columns: columns, Rows: rows}, nil
		}
	}
	engine, err := l.getEngine(loc.DBName)
	if err != nil {
		return nil, fmt.Errorf("failed to get storage for database '%s': %w", loc.DBName, err)
	}

	columns, rows, err := l.cypherExec.ExecuteQueryWithRecord(ctx, loc.DBName, engine, query, params, recordBindings)
	if err != nil {
		return nil, fmt.Errorf("local execution on '%s' failed: %w", loc.DBName, err)
	}

	return &ResultStream{
		Columns: columns,
		Rows:    rows,
	}, nil
}

// ExecuteWithRecordRows runs a Cypher query with correlated bindings and
// returns a row iterator over the result.
func (l *LocalFragmentExecutor) ExecuteWithRecordRows(ctx context.Context, loc *LocationLocal, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, RowIterator, error) {
	res, err := l.ExecuteWithRecord(ctx, loc, query, params, recordBindings)
	if err != nil {
		return nil, nil, err
	}
	if res == nil {
		return nil, NewResultRowIterator(nil), nil
	}
	return res.Columns, NewResultRowIterator(res), nil
}
