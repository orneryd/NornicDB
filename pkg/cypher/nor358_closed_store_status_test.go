package cypher

import (
	"context"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Closed-store status on public Execute (NOR-358; NOR-293 comment e2b952ca).
//
// A statement that reaches a closed store is server-side unavailability, so it
// reports the retryable Neo.TransientError.General.DatabaseUnavailable status
// the sibling transient sentinel storage.ErrStorageRestoring already gets
// (pkg/errors/status.go). That covers every producer that keeps the closed
// store's sentinel: a matching read, an implicit transaction that could not
// start, a DDL statement, and a procedure whose own failure has no status
// (procedureRuntimeError keeps a status the failure already has).
//
// The control retains the owner that stays in charge: a statement mistake is
// still a compile-time SyntaxError of the statement itself.
func TestNOR358ClosedStoreExecuteStatusClassification(t *testing.T) {
	operations := []struct {
		name  string
		query string
		code  string
		// mistake marks a statement that is wrong on any store: it never
		// succeeds before the close, and its own status is the control.
		mistake bool
	}{
		{"label_read", "MATCH (n:Doc) RETURN count(n) AS c", nornicerrors.TransientDatabaseUnavailable, false},
		{"property_read", "MATCH (n:Doc {id:'a'}) RETURN n.id AS id", nornicerrors.TransientDatabaseUnavailable, false},
		{"procedure_control", "CALL db.labels() YIELD label RETURN label", nornicerrors.TransientDatabaseUnavailable, false},
		{"create_control", "CREATE (n:Doc {id:'new'}) RETURN n.id AS id", nornicerrors.TransientDatabaseUnavailable, false},
		{"set_control", "MATCH (n:Doc {id:'a'}) SET n.v=1 RETURN n.v AS v", nornicerrors.TransientDatabaseUnavailable, false},
		{"index_control", "CREATE INDEX nor358_idx IF NOT EXISTS FOR (n:Doc) ON (n.id)", nornicerrors.TransientDatabaseUnavailable, false},
		{"constraint_control", "CREATE CONSTRAINT nor358_uniq IF NOT EXISTS FOR (n:Doc) REQUIRE n.id IS UNIQUE", nornicerrors.TransientDatabaseUnavailable, false},
		{"query_mistake", "MATCH (n:Doc RETURN count(n) AS c", nornicerrors.StatementSyntaxError, true},
	}
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, backend := range []string{"durable", "inmemory"} {
				for _, prefix := range []string{"", "CYPHER 25 "} {
					for _, operation := range operations {
						t.Run(backend+"/"+prefix+operation.name, func(t *testing.T) {
							ctx := context.Background()
							base := nor358BadgerEngine(t, backend)
							exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
							_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a'})", nil)
							require.NoError(t, err)
							if operation.mistake {
								_, err = exec.Execute(ctx, prefix+operation.query, nil)
								require.Error(t, err, "a statement mistake must fail on the open store too")
								code, _ := nornicerrors.Neo4jStatus(err)
								require.Equal(t, operation.code, code)
							} else {
								_, err = exec.Execute(ctx, prefix+operation.query, nil)
								require.NoError(t, err, "the same statement must succeed before close")
							}
							require.NoError(t, base.Close())
							result, err := exec.Execute(ctx, prefix+operation.query, nil)
							require.Error(t, err)
							require.Nil(t, result)
							code, message := nornicerrors.Neo4jStatus(err)
							t.Logf("code=%s cause=%T message=%q", code, err, message)
							require.Equal(t, operation.code, code)
							if !operation.mistake {
								// The closed store stays the reported cause of its own
								// failure, and the statement itself is not blamed.
								require.ErrorIs(t, err, storage.ErrStorageClosed)
								require.Contains(t, message, storage.ErrStorageClosed.Error())
								require.False(t, nornicerrors.IsCompileTimeStatus(code), "a successfully compiled statement did not become invalid syntax when storage closed")
							}
						})
					}
				}
			}
		})
	}
}

// A transaction operation that the closed store stops reports the store's own
// unavailability status: an explicit BEGIN cannot prime its namespace, and an
// explicit COMMIT reports the commit failure's closed cause with that same
// status. A ROLLBACK of work that never reached the closed store still
// completes.
func TestNOR358ClosedStoreTransactionOperationStatus(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, backend := range []string{"durable", "inmemory"} {
				for _, operation := range []struct{ name, command, code string }{
					{"begin", "BEGIN", nornicerrors.TransientDatabaseUnavailable},
					{"commit", "COMMIT", nornicerrors.TransientDatabaseUnavailable},
					{"rollback", "ROLLBACK", ""},
				} {
					t.Run(backend+"/"+operation.name, func(t *testing.T) {
						ctx := context.Background()
						base := nor358BadgerEngine(t, backend)
						exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
						_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a'})", nil)
						require.NoError(t, err)
						if operation.name != "begin" {
							_, err = exec.Execute(ctx, "BEGIN", nil)
							require.NoError(t, err, "the transaction must open before the close")
						}
						require.NoError(t, base.Close())
						result, err := exec.Execute(ctx, operation.command, nil)
						if operation.code == "" {
							require.NoError(t, err, "a rollback needs nothing from the closed store")
							require.NotNil(t, result)
							return
						}
						require.Error(t, err)
						require.Nil(t, result)
						code, message := nornicerrors.Neo4jStatus(err)
						t.Logf("command=%s cause=%T code=%s message=%q", operation.command, err, code, message)
						require.Equal(t, operation.code, code)
						require.ErrorIs(t, err, storage.ErrStorageClosed)
						require.Contains(t, message, storage.ErrStorageClosed.Error())
					})
				}
			}
		})
	}
}

func TestNOR358UncodedClosedStoreClassifierContract(t *testing.T) {
	for name, err := range map[string]error{
		"bare":    storage.ErrStorageClosed,
		"wrapped": fmt.Errorf("reading the index failed: %w", storage.ErrStorageClosed),
	} {
		t.Run(name, func(t *testing.T) {
			require.ErrorIs(t, err, storage.ErrStorageClosed)
			code, message := nornicerrors.Neo4jStatus(err)
			require.Contains(t, message, "storage closed")
			require.Equal(t, nornicerrors.TransientDatabaseUnavailable, code)
			require.True(t, nornicerrors.HasNeo4jStatus(err))
		})
	}
}

func nor358BadgerEngine(t *testing.T, backend string) *storage.BadgerEngine {
	t.Helper()
	var base *storage.BadgerEngine
	var err error
	if backend == "inmemory" {
		base, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
	} else {
		base, err = fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(t.TempDir()))
	}
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	return base
}

// A transaction opened before the store closed must fail its next statement
// with an error, not with the storage driver's closed-handle panic (the engine
// converts that panic into its own closed outcome).
func TestNOR358OpenTransactionDoesNotPanicAfterStoreCloses(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, backend := range []string{"durable", "inmemory"} {
				for _, query := range []string{"MATCH (n:Doc {id:'a'}) RETURN n.id", "CALL db.labels() YIELD label RETURN label", "MATCH (n:Doc {id:'a'}) SET n.v=1 RETURN n.v"} {
					t.Run(backend+"/"+query, func(t *testing.T) {
						base := nor358BadgerEngine(t, backend)
						exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
						_, err := exec.Execute(t.Context(), "CREATE (:Doc {id:'a'})", nil)
						require.NoError(t, err)
						_, err = exec.Execute(t.Context(), query, nil)
						require.NoError(t, err)
						_, err = exec.Execute(t.Context(), "BEGIN", nil)
						require.NoError(t, err)
						t.Cleanup(func() { _, _ = exec.Execute(context.Background(), "ROLLBACK", nil) })
						require.NoError(t, base.Close())
						require.NotPanics(t, func() { _, err = exec.Execute(t.Context(), query, nil) }, "the public executor must return an error when its underlying store has closed")
						require.Error(t, err)
					})
				}
			}
		})
	}
}
