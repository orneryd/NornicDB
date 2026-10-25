package cypher

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const hiddenRelationshipMerge = "MATCH (a:A), (b:B) MERGE (a)-[r:LINK {key:'same'}]->(b)"

// hiddenRelationshipStorageModes are the states a hidden predecessor is met
// in: committed synchronously, flushed from write-behind, or acknowledged and
// rewritten with its own value, so suppression and the retry find its body in
// the write-behind overlay rather than in storage.
var hiddenRelationshipStorageModes = []struct {
	name              string
	buffered, flushed bool
}{
	{"sync", false, true},
	{"write-behind/flushed", true, true},
	{"write-behind/acknowledged", true, false},
}

// openHiddenRelationshipStorage opens a fresh engine, buffered or synchronous.
func openHiddenRelationshipStorage(t *testing.T, buffered bool) (*storage.BadgerEngine, *StorageExecutor) {
	t.Helper()
	base, err := runtimeFixtureOwner.OwnResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineWithOptions(storage.BadgerOptions{
		DataDir: t.TempDir(), WriteBehind: buffered, WriteBehindInterval: time.Hour,
	}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = base.Close() })
	return base, NewStorageExecutor(storage.NewNamespacedEngine(base, "nornic"))
}

// settleHiddenRelationshipPredecessor flushes the predecessor; when flushed is
// false it then rewrites it with value through an acknowledged buffered SET,
// which stays in the write-behind overlay.
func settleHiddenRelationshipPredecessor(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, flushed bool, value string, params map[string]interface{}) {
	t.Helper()
	require.NoError(t, base.FlushWriteBehind())
	if !flushed {
		_, err := exec.Execute(context.Background(), "MATCH ()-[r:LINK]->() SET r.payload = "+value, params)
		require.NoError(t, err)
	}
}

// forEachHiddenRelationshipFixture runs check under both parsers and every
// storage mode against a stored LINK {key:'same', payload:'stored'} that is
// suppressed before check runs. It returns the stored predecessor.
func forEachHiddenRelationshipFixture(t *testing.T, check func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge)) {
	t.Helper()
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, mode := range hiddenRelationshipStorageModes {
			t.Run(fmt.Sprintf("%s/%s", parser, mode.name), func(t *testing.T) {
				config.SetParserType(parser)
				base, exec := openHiddenRelationshipStorage(t, mode.buffered)
				ctx := context.Background()
				_, err := exec.Execute(ctx, "CREATE (:A {id:'a'}), (:B {id:'b'})", nil)
				require.NoError(t, err)
				_, err = exec.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload='stored'", nil)
				require.NoError(t, err)
				settleHiddenRelationshipPredecessor(t, base, exec, mode.flushed, "'stored'", nil)
				edges, err := storage.CollectStoredEdges(ctx, base)
				require.NoError(t, err)
				require.Len(t, edges, 1)
				before := storage.CopyEdge(edges[0])
				require.NoError(t, base.SetEdgeManuallySuppressed(before.ID, true))
				check(t, base, exec, before)
			})
		}
	}
}

// requireHiddenRelationshipUnchanged asserts that the stored predecessor kept
// its body and stayed hidden.
func requireHiddenRelationshipUnchanged(t *testing.T, base *storage.BadgerEngine, before *storage.Edge) {
	t.Helper()
	require.NoError(t, base.FlushWriteBehind())
	after, err := base.GetStoredEdge(before.ID)
	require.NoError(t, err)
	require.Equal(t, before.Properties, after.Properties)
	require.Equal(t, before.StartNode, after.StartNode)
	require.Equal(t, before.EndNode, after.EndNode)
	require.True(t, before.CreatedAt.Equal(after.CreatedAt))
	_, err = base.GetEdge(before.ID)
	require.ErrorIs(t, err, storage.ErrNotFound)
	count, err := base.EdgeCount()
	require.NoError(t, err)
	require.EqualValues(t, 1, count)
}

func requireHiddenIdentityConflict(t *testing.T, err error) {
	t.Helper()
	var conflict *storage.HiddenEdgeIdentityConflictError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "Neo.ClientError.Schema.ConstraintValidationFailed", conflict.BoltErrorCode())
}

func TestRepeatedHiddenRelationshipMergeStaysNoOpAndInvisible(t *testing.T) {
	forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge) {
		ctx := context.Background()
		const retry = hiddenRelationshipMerge + " ON CREATE SET r.payload='stored' ON MATCH SET r.payload='MATCH-FIRED'"
		result, err := exec.Execute(ctx, "UNWIND [1,2] AS i "+retry, nil)
		require.NoError(t, err, "duplicate rows must reuse the hidden no-op disposition")
		require.Zero(t, result.Stats.RelationshipsCreated)
		require.Zero(t, result.Stats.PropertiesSet, "ON MATCH must not run for the hidden candidate")

		_, err = exec.Execute(ctx, "BEGIN", nil)
		require.NoError(t, err)
		for attempt := 0; attempt < 2; attempt++ {
			result, err = exec.Execute(ctx, retry, nil)
			require.NoError(t, err)
			require.Zero(t, result.Stats.RelationshipsCreated)
			require.Zero(t, result.Stats.PropertiesSet, "statement %d ran ON MATCH", attempt+1)
		}
		visible, err := exec.Execute(ctx, "MATCH ()-[r:LINK]->() RETURN count(r)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, visible.Rows, "MATCH after MERGE must not see the hidden candidate")
		_, err = exec.Execute(ctx, "COMMIT", nil)
		require.NoError(t, err)
		requireHiddenRelationshipUnchanged(t, base, before)
	})
}

// The candidate's final image is the MERGE pattern plus every SET reaching
// the MERGE's own binding. A later MATCH cannot see the hidden candidate, so
// it neither completes nor changes that image.
func TestHiddenRelationshipCandidateFinalImageSources(t *testing.T) {
	t.Run("bound SET identical", func(t *testing.T) {
		forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge) {
			result, err := exec.Execute(context.Background(), "UNWIND [1,2] AS i "+hiddenRelationshipMerge+" SET r.payload='stored'", nil)
			require.NoError(t, err)
			require.Zero(t, result.Stats.RelationshipsCreated)
			requireHiddenRelationshipUnchanged(t, base, before)
		})
	})
	t.Run("bound SET different", func(t *testing.T) {
		forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge) {
			_, err := exec.Execute(context.Background(), hiddenRelationshipMerge+" WITH r SET r.payload='different'", nil)
			requireHiddenIdentityConflict(t, err)
			requireHiddenRelationshipUnchanged(t, base, before)
		})
	})
	t.Run("later MATCH cannot change the candidate", func(t *testing.T) {
		forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge) {
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload='stored'", nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH ()-[r:LINK]->() SET r.payload='different'", nil)
			require.NoError(t, err)
			require.Zero(t, result.Stats.PropertiesSet)
			_, err = exec.Execute(ctx, "COMMIT", nil)
			require.NoError(t, err)
			requireHiddenRelationshipUnchanged(t, base, before)
		})
	})
	t.Run("later MATCH cannot complete the candidate", func(t *testing.T) {
		forEachHiddenRelationshipFixture(t, func(t *testing.T, base *storage.BadgerEngine, exec *StorageExecutor, before *storage.Edge) {
			ctx := context.Background()
			_, err := exec.Execute(ctx, "BEGIN", nil)
			require.NoError(t, err)
			_, err = exec.Execute(ctx, hiddenRelationshipMerge, nil)
			require.NoError(t, err)
			result, err := exec.Execute(ctx, "MATCH ()-[r:LINK]->() SET r.payload='stored'", nil)
			require.NoError(t, err)
			require.Zero(t, result.Stats.PropertiesSet)
			_, err = exec.Execute(ctx, "COMMIT", nil)
			requireHiddenIdentityConflict(t, err)
			requireHiddenRelationshipUnchanged(t, base, before)
		})
	})
}

// Outside a transaction there is no hidden-ID candidate to report.
func TestTransactionWrapperEdgeCreateWasNoOpWithoutTransaction(t *testing.T) {
	var none *transactionStorageWrapper
	require.False(t, none.EdgeCreateWasNoOp("r"))
	require.False(t, (&transactionStorageWrapper{namespace: "nornic", separator: ":"}).EdgeCreateWasNoOp("r"))
}

// Final-image identity compares property values as storage holds them. The
// stored body has crossed the property codec (a homogeneous list decodes as a
// typed slice, numbers keep their stored width), while a Cypher or parameter
// value has not; the same value must still be identical, and a different one
// must still conflict.
func TestHiddenRelationshipIdentityUsesStoredValueRepresentation(t *testing.T) {
	cases := []struct {
		name, same, different string
		params                map[string]interface{}
	}{
		{name: "string list", same: "['stored']", different: "['other']"},
		{name: "integer list", same: "[1, 2]", different: "[1, 3]"},
		{name: "float list", same: "[1.5, 2.5]", different: "[1.5]"},
		{name: "boolean list", same: "[true, false]", different: "[false, true]"},
		{name: "integer and float list", same: "[1, 2.5]", different: "[2, 2.5]"},
		{name: "integer is not float", same: "1", different: "1.0"},
		{name: "large integer", same: "9007199254740993", different: "9007199254740992"},
		{name: "date", same: "date('2024-01-02')", different: "date('2024-01-03')"},
		{name: "datetime", same: "datetime('2024-01-02T03:04:05.123456789Z')", different: "datetime('2024-01-02T03:04:05Z')"},
		{name: "date list", same: "[date('2024-01-02'), date('2024-01-03')]", different: "[date('2024-01-02')]"},
		{name: "duration", same: "duration('P1DT2H')", different: "duration('P1D')"},
		{name: "point", same: "point({x: 1.0, y: 2.0})", different: "point({x: 1.0, y: 3.0})"},
		{name: "generic list parameter", same: "$same", different: "$different", params: map[string]interface{}{
			"same": []interface{}{"stored", "x"}, "different": []interface{}{"stored"},
		}},
		{name: "typed list parameter", same: "$same", different: "$different", params: map[string]interface{}{
			"same": []string{"stored", "x"}, "different": []string{"x", "stored"},
		}},
	}
	previousParser := config.GetParserType()
	t.Cleanup(func() { config.SetParserType(previousParser) })
	for _, parser := range []string{config.ParserTypeNornic, config.ParserTypeANTLR} {
		for _, mode := range hiddenRelationshipStorageModes {
			for _, tc := range cases {
				t.Run(fmt.Sprintf("%s/%s/%s", parser, mode.name, tc.name), func(t *testing.T) {
					config.SetParserType(parser)
					base, exec := openHiddenRelationshipStorage(t, mode.buffered)
					ctx := context.Background()
					_, err := exec.Execute(ctx, "CREATE (:A {id:'a'}), (:B {id:'b'})", nil)
					require.NoError(t, err)
					_, err = exec.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload = "+tc.same, tc.params)
					require.NoError(t, err)
					// Flushed, the retry meets the persisted representation; acknowledged,
					// it meets the overlay body, which must be that same representation.
					settleHiddenRelationshipPredecessor(t, base, exec, mode.flushed, tc.same, tc.params)
					edges, err := storage.CollectStoredEdges(ctx, base)
					require.NoError(t, err)
					require.Len(t, edges, 1)
					before := storage.CopyEdge(edges[0])
					require.NoError(t, base.SetEdgeManuallySuppressed(before.ID, true))

					result, err := exec.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload = "+tc.same, tc.params)
					require.NoError(t, err, "an identical value is the permitted no-op")
					require.Zero(t, result.Stats.RelationshipsCreated)
					requireHiddenRelationshipUnchanged(t, base, before)

					_, err = exec.Execute(ctx, hiddenRelationshipMerge+" ON CREATE SET r.payload = "+tc.different, tc.params)
					requireHiddenIdentityConflict(t, err)
					requireHiddenRelationshipUnchanged(t, base, before)
				})
			}
		}
	}
}
