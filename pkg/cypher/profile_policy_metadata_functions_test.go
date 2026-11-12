package cypher

// PROFILE for the scalar knowledge-policy functions: policy(), decay() and
// decayScore().
//
// Each of these functions reads one entity's stored access metadata - the same
// point read kp.resolve performs - through the canonical ctx-first access owner,
// so a PROFILE statement states what that request returned instead of leaving
// the function's read uncounted. The record is the read's own answer: the entry
// the point read found is one row, an entity the store holds no entry for is an
// observed zero, and a call that fails after the read keeps the request it
// already made.
//
// Every case runs the statement through the public entry point, so PROFILE's
// owner installs the production collector, and each runs in both parsers. The
// plain and PROFILE statements must return the same columns and rows, and the
// storage read itself is the control for every record.

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

const (
	// policyMetadataFunctionNodeID is the one entity these statements score.
	policyMetadataFunctionNodeID = "nornic:policy_metadata_profiled"
	// policyMetadataFunctionAccessCount is the access count the stored entry
	// holds, so policy() answering it proves the function read that entry.
	policyMetadataFunctionAccessCount = int64(7)
)

// policyMetadataFunctionLastAccessedAt fixes the stored entry's last access so
// the fixture and its records never depend on when the test runs.
var policyMetadataFunctionLastAccessedAt = time.Unix(1715100000, 0).UnixNano()

// policyMetadataFunctionExecutor returns the public executor over a
// decay-enabled engine holding one :MemoryEpisode node. A non-nil accessCount
// also stores that node's access metadata entry; a nil one stores none, so the
// store holds no entry for the node.
func policyMetadataFunctionExecutor(t *testing.T, accessCount *int64) *StorageExecutor {
	t.Helper()
	engine, exec := setupDecayEngine(t)
	_, nodeErr := engine.CreateNode(t.Context(), makeNode(policyMetadataFunctionNodeID, []string{"MemoryEpisode"}, 2*time.Hour))
	require.NoError(t, nodeErr)
	if accessCount != nil {
		_, accessErr := engine.WriteAccessMeta(t.Context(), knowledgepolicy.AccessMetaWriteRequest{
			Mode:     knowledgepolicy.RecordState,
			EntityID: policyMetadataFunctionNodeID,
			Entry: &knowledgepolicy.AccessMetaEntry{
				TargetID:    policyMetadataFunctionNodeID,
				TargetScope: knowledgepolicy.ScopeNode,
				Fixed: knowledgepolicy.AccessMetaFixedFields{
					AccessCount:    *accessCount,
					LastAccessedAt: policyMetadataFunctionLastAccessedAt,
				},
			},
		})
		require.NoError(t, accessErr)
	}
	return exec
}

// policyMetadataFunctionStoredEntry is the storage read the record must state:
// the same point read the function performs, asked again by the test.
func policyMetadataFunctionStoredEntry(t *testing.T, exec *StorageExecutor) *knowledgepolicy.AccessMetaEntry {
	t.Helper()
	be := unwrapBadgerEngine(exec.storage)
	require.NotNil(t, be)
	meta, readErr := be.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{
		Mode:     knowledgepolicy.AccessMetaReadPoint,
		EntityID: storage.QualifiedEntityID(exec.storage, policyMetadataFunctionNodeID),
	})
	require.NoError(t, readErr)
	return meta.Entry
}

// policy() answers the stored entry the function read, and the record states
// that one request returned it.
func TestProfileKnowledgePolicyFunctionsReportTheirAccessMetadataRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		count := policyMetadataFunctionAccessCount
		exec := policyMetadataFunctionExecutor(t, &count)
		stored := policyMetadataFunctionStoredEntry(t, exec)
		require.NotNil(t, stored, "the fixture must hold an access metadata entry")
		require.Equal(t, policyMetadataFunctionAccessCount, stored.Fixed.AccessCount, "the storage read is the control")

		result, plan := profileEquivalentObservedStatement(t, exec,
			`MATCH (n:MemoryEpisode) RETURN policy(n).accessCount AS accessCount`)
		require.Equal(t, [][]interface{}{{policyMetadataFunctionAccessCount}}, result.Rows,
			"policy() answers the entry its access metadata read returned")
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyAccessMetaPoint, 1)
		require.EqualValues(t, 2, plan.TotalDBHits,
			"the pattern read the one node and the function read one access metadata entry")
	})
}

// An entity the store holds no entry for is an observed zero: the point read
// still happened, and policy() answers the entity it holds nothing about.
func TestProfileKnowledgePolicyFunctionsReportAnAbsentAccessMetadataEntry(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec := policyMetadataFunctionExecutor(t, nil)
		require.Nil(t, policyMetadataFunctionStoredEntry(t, exec),
			"the storage read is the control: this store holds no entry")

		result, plan := profileEquivalentObservedStatement(t, exec,
			`MATCH (n:MemoryEpisode) RETURN policy(n).targetId AS targetId`)
		require.Equal(t, [][]interface{}{{policyMetadataFunctionNodeID}}, result.Rows,
			"policy() answers the entity it holds no entry for")
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyAccessMetaPoint, 0)
		require.EqualValues(t, 2, plan.TotalDBHits,
			"the absent entry is still one request, not a missing measurement")
	})
}

// A call that cannot answer - an option this function doesn't define - still
// reports the access metadata read it performed before it failed, so the
// statement's profile never understates the requests the function made.
func TestProfileKnowledgePolicyFunctionsReportTheReadOfAFailedCall(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		count := policyMetadataFunctionAccessCount
		exec := policyMetadataFunctionExecutor(t, &count)
		require.NotNil(t, policyMetadataFunctionStoredEntry(t, exec), "the fixture must hold an entry")

		result, plan := profileEquivalentObservedStatement(t, exec,
			`MATCH (n:MemoryEpisode) RETURN decayScore(n, {notAnOption: 'x'}) AS score`)
		require.Len(t, result.Rows, 1)
		require.Nil(t, result.Rows[0][0], "an option this function doesn't define leaves the call unanswered")
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyAccessMetaPoint, 1)
		require.EqualValues(t, 2, plan.TotalDBHits,
			"the call that could not answer still made its access metadata read")
	})
}
