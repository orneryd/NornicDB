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

// An uncoded closed-store read must be unavailable, never a syntax error.
// BEGIN/procedure owners have their own established status contracts, retained
// as controls rather than treating every coded wrapper as another defect.
func TestNOR358ClosedStoreReadStatusAndOwnedProducerControls(t *testing.T) {
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := config.GetParserType()
			config.SetParserType(parser)
			t.Cleanup(func() { config.SetParserType(previous) })
			for _, backend := range []string{"durable", "inmemory"} {
				for _, prefix := range []string{"", "CYPHER 25 "} {
					for _, operation := range []struct{ name, query, code string }{
						{"label_read", "MATCH (n:Doc) RETURN count(n) AS c", nornicerrors.TransientDatabaseUnavailable},
						{"property_read", "MATCH (n:Doc {id:'a'}) RETURN n.id AS id", nornicerrors.TransientDatabaseUnavailable},
						{"procedure_control", "CALL db.labels() YIELD label RETURN label", "Neo.ClientError.Procedure.ProcedureCallFailed"},
						{"create_control", "CREATE (n:Doc {id:'new'}) RETURN n.id AS id", "Neo.DatabaseError.Transaction.TransactionStartFailed"},
						{"set_control", "MATCH (n:Doc {id:'a'}) SET n.v=1 RETURN n.v AS v", "Neo.DatabaseError.Transaction.TransactionStartFailed"},
						{"index_control", "CREATE INDEX nor358_idx IF NOT EXISTS FOR (n:Doc) ON (n.id)", "Neo.DatabaseError.Transaction.TransactionStartFailed"},
						{"constraint_control", "CREATE CONSTRAINT nor358_uniq IF NOT EXISTS FOR (n:Doc) REQUIRE n.id IS UNIQUE", "Neo.DatabaseError.Transaction.TransactionStartFailed"},
					} {
						t.Run(backend+"/"+prefix+operation.name, func(t *testing.T) {
							ctx := context.Background()
							base := nor358BadgerEngine(t, backend)
							exec := NewStorageExecutor(storage.NewNamespacedEngine(base, "test"), QueryCachePolicy{MaxEntries: 0, TTL: 0})
							_, err := exec.Execute(ctx, "CREATE (:Doc {id:'a'})", nil)
							require.NoError(t, err)
							_, err = exec.Execute(ctx, prefix+operation.query, nil)
							require.NoError(t, err, "the same statement must succeed before close")
							require.NoError(t, base.Close())
							result, err := exec.Execute(ctx, prefix+operation.query, nil)
							require.ErrorIs(t, err, storage.ErrStorageClosed)
							require.Nil(t, result)
							code, message := nornicerrors.Neo4jStatus(err)
							t.Logf("code=%s cause=%T message=%q", code, err, message)
							require.Equal(t, operation.code, code)
							require.False(t, nornicerrors.IsCompileTimeStatus(code), "a successfully compiled statement did not become invalid syntax when storage closed")
						})
					}
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
