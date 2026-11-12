package search

// The profile ledger of a complete continuation read: the node scan that builds
// the catalog, the schema constraint catalog that build acquires, and the node
// hydration each page performs. The build is synchronous in the caller's
// context, so its records belong to the request that asked for it, and every
// pull reports only the page it hydrates under its own context.

import (
	"context"
	"errors"
	"fmt"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// engineWithoutOptionalReads keeps only storage.Engine and the graph revision
// the complete continuation build requires, so the optional readers this change
// reports on are absent and both the scan and the hydration take their own
// fallbacks.
type engineWithoutOptionalReads struct {
	storage.Engine

	mutations graphMutationVersionProvider
}

func (e *engineWithoutOptionalReads) GraphMutationVersion() (uint64, bool) {
	return e.mutations.GraphMutationVersion()
}

// embeddingFreeBatchDeclineEngine declines the optional embedding-free batch
// read and counts the reads each caller's own fallback takes instead: the point
// reader, or the engine's bulk node read.
type embeddingFreeBatchDeclineEngine struct {
	storage.Engine

	optionalCalls int
	pointReads    int
	bulkReads     int
}

func (e *embeddingFreeBatchDeclineEngine) BatchGetNodesWithoutEmbeddings(context.Context, []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	e.optionalCalls++
	return nil, storage.ErrNotImplemented
}

func (e *embeddingFreeBatchDeclineEngine) GetNodeWithoutEmbeddings(ctx context.Context, id storage.NodeID) (*storage.Node, error) {
	e.pointReads++
	return e.Engine.GetNode(ctx, id)
}

func (e *embeddingFreeBatchDeclineEngine) BatchGetNodes(ctx context.Context, ids []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	e.bulkReads++
	return e.Engine.BatchGetNodes(ctx, ids)
}

// embeddingFreeBatchFailureEngine fails the optional batch read after returning
// a partial map: the failure is one attempt with the zero rows it returned.
type embeddingFreeBatchFailureEngine struct {
	storage.Engine

	err error
}

func (e *embeddingFreeBatchFailureEngine) BatchGetNodesWithoutEmbeddings(context.Context, []storage.NodeID) (map[storage.NodeID]*storage.Node, error) {
	return map[storage.NodeID]*storage.Node{"doc-a": {ID: "doc-a"}}, e.err
}

func continuationTestEngine(t *testing.T) *storage.MemoryEngine {
	t.Helper()
	return fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
}

// TestOptionalBatchNodesWithoutEmbeddingsOwnerIsTheOneInvocationOwner is the
// matrix over the consolidated optional-capability owner: the reader absent, the
// reader answering, the reader declining with ErrNotImplemented and the reader
// failing. Only the two cases that ran a read are reported, the rows are the
// returned map's entries, and a failure is one attempt with zero rows.
func TestOptionalBatchNodesWithoutEmbeddingsOwnerIsTheOneInvocationOwner(t *testing.T) {
	base := continuationTestEngine(t)
	ids := []storage.NodeID{"doc-a", "doc-b"}

	t.Run("absent", func(t *testing.T) {
		engine := &engineWithoutOptionalReads{Engine: base, mutations: base}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, answered, err := optionalBatchNodesWithoutEmbeddings(ctx, engine, ids)

		require.NoError(t, err)
		require.False(t, answered, "an engine without the optional reader performed no batch read")
		require.Nil(t, nodes)
		_, _, recorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.False(t, recorded, "no request was made, so nothing is reported")
		require.Empty(t, observer.RecordedKinds())
	})

	t.Run("implemented", func(t *testing.T) {
		engine := storage.NewNamespacedEngine(base, "nornic")
		for _, id := range ids {
			_, err := engine.CreateNode(t.Context(), &storage.Node{ID: id})
			require.NoError(t, err)
		}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, answered, err := optionalBatchNodesWithoutEmbeddings(ctx, engine, ids)

		require.NoError(t, err)
		require.True(t, answered, "the optional reader answered the request")
		require.Len(t, nodes, 2)
		rows, hits, recorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.True(t, recorded, "the request that ran is reported")
		require.EqualValues(t, 2, rows, "the returned map's entries")
		require.EqualValues(t, 1, hits, "one batch request, whatever it returned")
	})

	t.Run("ErrNotImplemented", func(t *testing.T) {
		engine := &embeddingFreeBatchDeclineEngine{Engine: base}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, answered, err := optionalBatchNodesWithoutEmbeddings(ctx, engine, ids)

		require.NoError(t, err)
		require.False(t, answered, "a declined capability performed no read")
		require.Nil(t, nodes)
		require.Equal(t, 1, engine.optionalCalls)
		_, _, recorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.False(t, recorded, "a declined capability is not this read")
		require.Empty(t, observer.RecordedKinds())
	})

	t.Run("failed batch", func(t *testing.T) {
		batchErr := errors.New("batch read failed")
		engine := &embeddingFreeBatchFailureEngine{Engine: base, err: batchErr}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, answered, err := optionalBatchNodesWithoutEmbeddings(ctx, engine, ids)

		require.ErrorIs(t, err, batchErr)
		require.True(t, answered, "a failed request did run")
		require.Nil(t, nodes, "a failed batch returns no nodes")
		rows, hits, recorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.True(t, recorded, "the failed attempt is reported")
		require.Zero(t, rows, "a failed read returned no rows")
		require.EqualValues(t, 1, hits, "one attempt")
	})
}

// TestEmbeddingFreeBatchFallbacksStayDistinct proves the consolidated owner did
// not flatten the two genuine fallbacks it serves: the metadata caller falls back
// to the point reader, and the continuation page falls back to the engine's bulk
// node read. Neither replaces the other.
func TestEmbeddingFreeBatchFallbacksStayDistinct(t *testing.T) {
	base := continuationTestEngine(t)
	ids := []storage.NodeID{"nornic:doc-a", "nornic:doc-b"}
	for _, id := range ids {
		_, err := base.CreateNode(t.Context(), &storage.Node{ID: id})
		require.NoError(t, err)
	}

	t.Run("metadata point fallback", func(t *testing.T) {
		engine := &embeddingFreeBatchDeclineEngine{Engine: base}
		service := NewService(engine, DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, err := service.batchGetNodesWithoutEmbeddings(ctx, ids)

		require.NoError(t, err)
		require.Len(t, nodes, 2)
		require.Equal(t, 1, engine.optionalCalls)
		require.Equal(t, 2, engine.pointReads, "the metadata caller reads the nodes it needs one by one")
		require.Zero(t, engine.bulkReads, "the metadata caller does not fall back to the bulk read")
		rows, hits, recorded := observer.Reads(queryprofile.KindNodeByIDSeek, "")
		require.True(t, recorded)
		require.EqualValues(t, 2, rows)
		require.EqualValues(t, 2, hits)
		_, _, batchRecorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.False(t, batchRecorded, "the declined batch performed no read")
	})

	t.Run("continuation bulk fallback", func(t *testing.T) {
		engine := &embeddingFreeBatchDeclineEngine{Engine: base}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, err := batchContinuationNodesWithoutEmbeddings(ctx, engine, ids)

		require.NoError(t, err)
		require.Len(t, nodes, 2)
		require.Equal(t, 1, engine.optionalCalls)
		require.Equal(t, 1, engine.bulkReads, "the page falls back to the engine's bulk node read")
		require.Zero(t, engine.pointReads, "the page does not fall back to the point reader")
		_, _, pointRecorded := observer.Reads(queryprofile.KindNodeByIDSeek, "")
		require.False(t, pointRecorded)
		rows, hits, batchRecorded := observer.Reads(queryprofile.KindNodeBatchByIDSeek, "")
		require.True(t, batchRecorded, "the bulk fallback actually performed a read")
		require.EqualValues(t, 2, rows)
		require.EqualValues(t, 1, hits)
	})

	// An empty metadata request preserves its own semantics: the service answers
	// it without running the optional reader at all, so nothing is reported.
	t.Run("empty metadata request", func(t *testing.T) {
		engine := &embeddingFreeBatchDeclineEngine{Engine: base}
		service := NewService(engine, DefaultVectorDimensions, nil)
		t.Cleanup(func() { require.NoError(t, service.Close()) })
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, err := service.batchGetNodesWithoutEmbeddings(ctx, nil)

		require.NoError(t, err)
		require.Empty(t, nodes)
		require.Zero(t, engine.optionalCalls, "an empty request runs no optional read")
		require.Empty(t, observer.RecordedKinds(), "an empty request reports no read")
	})

	// The continuation page keeps its own empty-request semantics: it asks the
	// optional reader once, as it always did, and returns what it answered.
	t.Run("empty continuation request", func(t *testing.T) {
		engine := &embeddingFreeBatchDeclineEngine{Engine: base}
		observer := &fixtureowner.QueryReadObserver{}
		ctx := queryprofile.WithObserver(context.Background(), observer)

		nodes, err := batchContinuationNodesWithoutEmbeddings(ctx, engine, nil)

		require.NoError(t, err)
		require.Empty(t, nodes)
		require.Equal(t, 1, engine.optionalCalls)
		require.Equal(t, 1, engine.bulkReads, "the declined request still takes this caller's own fallback")
	})
}

// TestCompleteContinuationProjectionReportsTheCatalogItRead holds the schema
// constraint catalog's record to what the acquisition returned, before the
// temporal filter selects from it: a schema holding no constraint is an observed
// zero, and a temporal entry the projection keeps does not narrow the record.
func TestCompleteContinuationProjectionReportsTheCatalogItRead(t *testing.T) {
	base := continuationTestEngine(t)
	service := NewService(base, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	observer := &fixtureowner.QueryReadObserver{}
	ctx := queryprofile.WithObserver(context.Background(), observer)
	require.Empty(t, service.completeContinuationProjectionProperties(ctx, DefaultSearchOptions(), SearchContinuationRequest{}))
	rows, hits, recorded := observer.Reads(queryprofile.KindSchemaConstraintCatalog, "")
	require.True(t, recorded, "the catalog is read even though this schema holds no constraint")
	require.Zero(t, rows, "an empty catalog is an observed zero")
	require.EqualValues(t, 1, hits, "one acquisition, whatever it returned")

	schema := base.GetSchema()
	require.NotNil(t, schema)
	require.NoError(t, schema.AddConstraint(storage.Constraint{
		Name:       "c_document_validity",
		Type:       storage.ConstraintTemporal,
		EntityType: storage.ConstraintEntityNode,
		Label:      "Document",
		Properties: []string{"entity_id", "valid_from", "valid_to"},
	}))
	require.NoError(t, schema.AddConstraint(storage.Constraint{
		Name:       "c_document_slug",
		Type:       storage.ConstraintUnique,
		EntityType: storage.ConstraintEntityNode,
		Label:      "Document",
		Properties: []string{"slug"},
	}))

	observer = &fixtureowner.QueryReadObserver{}
	ctx = queryprofile.WithObserver(context.Background(), observer)
	require.Equal(t,
		[]string{"entity_id", "valid_from", "valid_to"},
		service.completeContinuationProjectionProperties(ctx, DefaultSearchOptions(), SearchContinuationRequest{}))
	rows, hits, recorded = observer.Reads(queryprofile.KindSchemaConstraintCatalog, "")
	require.True(t, recorded)
	require.EqualValues(t, 2, rows, "both catalog entries the acquisition returned, not the temporal subset the projection keeps")
	require.EqualValues(t, 1, hits)
}

// TestCompleteContinuationScanIsObservedOnceBeforeEligibility holds the catalog
// scan's record to the nodes the scan actually handed over: every streamed node
// is one read, the ones the Types predicate then excludes included, and the
// projected reader and the fallback stream share the one wrapper.
func TestCompleteContinuationScanIsObservedOnceBeforeEligibility(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		engine func(*testing.T, *storage.MemoryEngine) storage.Engine
	}{
		{
			name: "projected prefix reader",
			engine: func(t *testing.T, base *storage.MemoryEngine) storage.Engine {
				engine := storage.Engine(base)
				_, ok := engine.(storage.ProjectedPrefixNodeReader)
				require.True(t, ok, "the fixture's engine answers the projected prefix scan")
				return engine
			},
		},
		{
			name: "fallback stream",
			engine: func(t *testing.T, base *storage.MemoryEngine) storage.Engine {
				engine := storage.Engine(&engineWithoutOptionalReads{Engine: base, mutations: base})
				_, ok := engine.(storage.ProjectedPrefixNodeReader)
				require.False(t, ok, "this engine answers the fallback stream instead")
				return engine
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			base := continuationTestEngine(t)
			for index, labels := range [][]string{{"Document"}, {"Document"}, {"Other"}} {
				_, err := base.CreateNode(t.Context(), &storage.Node{
					ID:     storage.NodeID(fmt.Sprintf("nornic:doc-%d", index)),
					Labels: labels,
				})
				require.NoError(t, err)
			}
			engine := testCase.engine(t, base)
			service := NewService(engine, DefaultVectorDimensions, nil)
			t.Cleanup(func() { require.NoError(t, service.Close()) })

			observer := &fixtureowner.QueryReadObserver{}
			ctx := queryprofile.WithObserver(context.Background(), observer)
			options := DefaultSearchOptions()
			options.Types = []string{"Document"}
			stream, err := service.newIDContinuationStream(ctx, *options, SearchContinuationRequest{Mode: SearchContinuationID})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, stream.Close()) })

			rows, hits, recorded := observer.Reads(queryprofile.KindAllNodesScan, "")
			require.True(t, recorded, "the build reports the node population it scanned")
			require.EqualValues(t, 3, rows, "every node the scan handed over, before the Types predicate excluded one")
			require.EqualValues(t, 3, hits, "the scan is observed once, not once per path")

			page, err := stream.Pull(ctx, 0, 10)
			require.NoError(t, err)
			require.Len(t, page.Rows, 2, "the eligible population is narrower than the scan")
		})
	}
}

// TestContinuationPullsObserveOnlyThePageTheyHydrate proves the stream retains no
// observer: the build's scan and catalog acquisition belong to the statement
// that built it, and each pull reports only the page it hydrated.
func TestContinuationPullsObserveOnlyThePageTheyHydrate(t *testing.T) {
	base := continuationTestEngine(t)
	engine := storage.NewNamespacedEngine(base, "nornic")
	for index := 0; index < 4; index++ {
		_, err := engine.CreateNode(t.Context(), &storage.Node{ID: storage.NodeID(fmt.Sprintf("doc-%d", index))})
		require.NoError(t, err)
	}
	service := NewService(engine, DefaultVectorDimensions, nil)
	t.Cleanup(func() { require.NoError(t, service.Close()) })

	buildObserver := &fixtureowner.QueryReadObserver{}
	buildCtx := queryprofile.WithObserver(context.Background(), buildObserver)
	stream, err := service.newIDContinuationStream(buildCtx, *DefaultSearchOptions(), SearchContinuationRequest{Mode: SearchContinuationID})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, stream.Close()) })

	rows, hits, recorded := buildObserver.Reads(queryprofile.KindAllNodesScan, "")
	require.True(t, recorded, "the build reports the population it scanned")
	require.EqualValues(t, 4, rows)
	require.EqualValues(t, 4, hits)
	_, _, catalogRecorded := buildObserver.Reads(queryprofile.KindSchemaConstraintCatalog, "")
	require.True(t, catalogRecorded, "the build acquires the constraint catalog")
	_, _, hydrated := buildObserver.Reads(queryprofile.KindNodeBatchByIDSeek, "")
	require.False(t, hydrated, "building the catalog hydrates no page")

	firstObserver := &fixtureowner.QueryReadObserver{}
	first, err := stream.Pull(queryprofile.WithObserver(context.Background(), firstObserver), 0, 2)
	require.NoError(t, err)
	require.Len(t, first.Rows, 2)
	rows, hits, recorded = firstObserver.Reads(queryprofile.KindNodeBatchByIDSeek, "")
	require.True(t, recorded, "the pull reports the page it hydrated")
	require.EqualValues(t, 2, rows)
	require.EqualValues(t, 1, hits, "one hydration request per page")
	require.Equal(t, []string{queryprofile.KindNodeBatchByIDSeek}, firstObserver.RecordedKinds(),
		"a pull reports its own page and neither rescans nor re-acquires the build's catalog")
	require.Empty(t, firstObserver.Callbacks())

	nextObserver := &fixtureowner.QueryReadObserver{}
	next, err := stream.Pull(queryprofile.WithObserver(context.Background(), nextObserver), 2, 2)
	require.NoError(t, err)
	require.Len(t, next.Rows, 2)
	rows, hits, recorded = nextObserver.Reads(queryprofile.KindNodeBatchByIDSeek, "")
	require.True(t, recorded)
	require.EqualValues(t, 2, rows, "the next pull hydrates only its own window")
	require.EqualValues(t, 1, hits)
	require.Equal(t, []string{queryprofile.KindNodeBatchByIDSeek}, nextObserver.RecordedKinds())
}
