package cypher

// A CALL statement's error is the caller's own error when the caller's context
// ends after the statement was admitted.
//
// db.index.fulltext.drop and db.index.vector.drop are registered write
// procedures, so a standalone CALL of one opens an implicit statement
// transaction. This file releases the caller's context at that transaction's
// real BEGIN (the observer only forwards the engine's own BeginTransaction and
// CommitTransaction) and asserts the error the statement reports afterwards.
//
// The caller's error, not a procedure lookup failure: the CALL reader parses
// the fragment to find the procedure it names. A context that ended between
// admission and that read makes the parse fail; dropping the parser error left
// an empty name behind, which the CALL owner then looked up and reported as
// Neo.ClientError.Procedure.ProcedureNotFound with an empty procedure name.
// CREATE at the same seam kept its context.Canceled, so the two clause owners
// disagreed about one caller's cancellation. The controls are the same wrapper
// unarmed (the statement runs) and a genuinely unknown procedure with a
// healthy caller (still ProcedureNotFound, with its own name).

import (
	"context"
	"testing"
	"time"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const callCancellationUnknownProcedure = "nope.call_cancellation_control"

// callCancellationBeginOwner is the observed transaction owner between the
// executor and the real engine: it forwards the engine's own Begin and Commit
// and releases the case's caller context once, at the statement's real BEGIN.
type callCancellationBeginOwner struct {
	*storage.NamespacedEngine
	base  *storage.MemoryEngine
	begin func()
	began bool
}

func (o *callCancellationBeginOwner) BeginTransaction(ctx context.Context) (*storage.BadgerTransaction, error) {
	tx, err := o.base.BeginTransaction(ctx)
	if err == nil && o.begin != nil && !o.began {
		o.began = true
		o.begin()
	}
	return tx, err
}

func (o *callCancellationBeginOwner) CommitTransaction(ctx context.Context, tx *storage.BadgerTransaction) error {
	return tx.CommitContext(ctx)
}

type callCancellationVariant struct {
	name    string
	want    error
	request func() (context.Context, func())
}

func callCancellationVariants() []callCancellationVariant {
	return []callCancellationVariant{
		{
			name: "cancellation",
			want: context.Canceled,
			request: func() (context.Context, func()) {
				ctx, cancel := context.WithCancel(context.Background())
				return ctx, cancel
			},
		},
	}
}

type callCancellationSubject struct {
	name    string
	query   string
	dropped string
}

func callCancellationSubjects() []callCancellationSubject {
	return []callCancellationSubject{
		{"fulltext drop", "CALL db.index.fulltext.drop('" + indexDropFulltextName + "')", indexDropFulltextName},
		{"vector drop", "CALL db.index.vector.drop('" + indexDropVectorName + "')", indexDropVectorName},
		{"create control", "CREATE (:CallCancellationProbe)", ""},
	}
}

func callCancellationCreateIndexes(t *testing.T, ctx context.Context, exec *StorageExecutor) {
	t.Helper()
	_, err := exec.Execute(ctx, "CREATE FULLTEXT INDEX "+indexDropFulltextName+" FOR (n:Doc) ON EACH [n.body]", nil)
	require.NoError(t, err)
	_, err = exec.Execute(ctx, "CREATE VECTOR INDEX "+indexDropVectorName+
		" FOR (n:Doc) ON (n.body) OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }", nil)
	require.NoError(t, err)
}

// TestCallCancellationKeepsCallerErrorShape is the public statement contract:
// the caller's cancellation is the statement's error, keeps
// errors.Is, carries no synthetic procedure status, and publishes nothing.
// The healthy and unknown-procedure controls establish that the same statement
// and the same wrapper still run, and that unknown procedures still report
// their own name.
func TestCallCancellationKeepsCallerErrorShape(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNICDB_PARSER", parser)
			for _, variant := range callCancellationVariants() {
				t.Run(variant.name, func(t *testing.T) {
					for _, subject := range callCancellationSubjects() {
						t.Run(subject.name, func(t *testing.T) {
							base := newTestMemoryEngine(t)
							owner := &callCancellationBeginOwner{
								NamespacedEngine: storage.NewNamespacedEngine(base, indexDropNamespace),
								base:             base,
							}
							exec := NewStorageExecutor(owner)
							other := NewStorageExecutor(storage.NewNamespacedEngine(base, indexDropOtherNamespace))
							healthy, healthyCancel := context.WithTimeout(context.Background(), 30*time.Second)
							defer healthyCancel()
							for _, target := range []*StorageExecutor{exec, other} {
								callCancellationCreateIndexes(t, healthy, target)
							}

							request, release := variant.request()
							defer release()
							owner.begin = release
							result, err := exec.Execute(request, subject.query, nil)

							require.True(t, owner.began, "the fixture must release the caller's context at the statement's real BEGIN")
							require.Error(t, err, "a statement whose caller context ended must report it")
							require.ErrorIs(t, err, variant.want)
							require.ErrorIs(t, request.Err(), variant.want)
							require.NotContains(t, err.Error(), "ProcedureNotFound",
								"a CALL must not substitute a procedure lookup failure for the caller's error")
							require.False(t, nornicerrors.HasStatusDiagnostic(err),
								"the caller's error keeps the protocol's ordinary policy instead of a synthetic status")
							require.Nil(t, result, "a statement whose caller context ended publishes no result")
							require.Equal(t, indexDropBothKinds(), readSchemaNames(t, healthy, exec, indexDropNamesQuery),
								"the statement published nothing in its own namespace")
							require.Equal(t, indexDropBothKinds(), readSchemaNames(t, healthy, other, indexDropNamesQuery),
								"the statement published nothing in the other namespace")

							// Healthy control: the same statement through the same
							// unarmed wrapper still runs.
							owner.begin = nil
							retry, err := exec.Execute(healthy, subject.query, nil)
							require.NoError(t, err)
							require.NotNil(t, retry)

							// Unknown-procedure control: with a healthy caller the
							// same owner still reports the name it was given.
							unknown, err := exec.Execute(healthy, "CALL "+callCancellationUnknownProcedure+"()", nil)
							require.Error(t, err)
							require.Nil(t, unknown)
							require.Contains(t, err.Error(), "Neo.ClientError.Procedure.ProcedureNotFound")
							require.Contains(t, err.Error(), callCancellationUnknownProcedure,
								"the lookup failure keeps the procedure name instead of an empty one")

							if subject.dropped == "" {
								count, err := exec.Execute(healthy, "MATCH (n:CallCancellationProbe) RETURN count(n) AS c", nil)
								require.NoError(t, err)
								require.Equal(t, [][]interface{}{{int64(1)}}, count.Rows,
									"the healthy retry creates its node exactly once")
								require.Equal(t, indexDropBothKinds(), readSchemaNames(t, healthy, exec, indexDropNamesQuery))
								require.Equal(t, indexDropBothKinds(), readSchemaNames(t, healthy, other, indexDropNamesQuery))
								return
							}
							require.Equal(t, [][]interface{}{{subject.dropped, true}}, retry.Rows)
							remaining := indexDropBothKinds()
							for index, name := range remaining {
								if name == subject.dropped {
									remaining = append(remaining[:index], remaining[index+1:]...)
									break
								}
							}
							require.Equal(t, remaining, readSchemaNames(t, healthy, exec, indexDropNamesQuery),
								"the healthy retry drops only the kind its own procedure names")
							require.Equal(t, indexDropBothKinds(), readSchemaNames(t, healthy, other, indexDropNamesQuery))
						})
					}
				})
			}
		})
	}
}

// A real expired deadline covers the reader and public request boundaries.
// Mid-transaction cancellation is covered above with a real CancelFunc; this
// test does not claim to release a deadline after BEGIN.
func TestCallExpiredDeadlineKeepsActualDeadlineError(t *testing.T) {
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			t.Setenv("NORNICDB_PARSER", parser)
			ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
			defer cancel()
			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
			select {
			case <-ctx.Done():
			default:
				t.Fatal("actual expired deadline must close Done")
			}
			name, err := extractProcedureName(ctx, "CALL db.index.fulltext.drop('absent')")
			require.Empty(t, name)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			base := newTestMemoryEngine(t)
			owner := &callCancellationBeginOwner{NamespacedEngine: storage.NewNamespacedEngine(base, indexDropNamespace), base: base, begin: func() {}}
			result, err := NewStorageExecutor(owner).Execute(ctx, "CALL db.index.fulltext.drop('absent')", nil)
			require.Nil(t, result)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.NotContains(t, err.Error(), "ProcedureNotFound")
			require.False(t, owner.began, "an already expired request must not open a transaction")
		})
	}
}
