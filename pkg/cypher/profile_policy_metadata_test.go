package cypher

// PROFILE for the knowledge-policy metadata procedures: kp.decayInfo, kp.info,
// kp.profiles, kp.policies, kp.resolve and kp.deindexStatus.
//
// Each reads this database's own state - the schema's policy catalogs, one
// entity and its stored access metadata, or the pending deindex queue - and
// reports every request through the canonical access owner, so a PROFILE
// statement states the population each request returned instead of calling the
// implementation unknown. The record is the read's own answer: the entries the
// catalog, point read or scan returned, never the rows the procedure builds from
// them and never a count taken from its output. kp.decayInfo reads no database
// state at all, so its zero is certified rather than counted.
//
// Ordinary and PROFILE statements must return identical columns and rows, and
// every record is held to the population the logical read API answers with. A
// replacement of one of these names registered through the exported API stays
// opaque; the shared replacement control for reviewed registrations covers it.

import (
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// policyMetadataRecord requires one access record of kind and holds it to the
// population its read returned and the single request it made.
func policyMetadataRecord(t *testing.T, plan *ExecutionPlan, kind string, rows int64) *PlanOperator {
	t.Helper()
	record := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, record, "%s must be reported as a plan operator", kind)
	require.True(t, record.RowsObserved, "%s reports the population it read", kind)
	require.True(t, record.DBHitsObserved, "%s reports the requests it made", kind)
	require.Equal(t, rows, record.ActualRows, "%s counts the population the read returned", kind)
	require.EqualValues(t, 1, record.DBHits, "%s is one request", kind)
	return record
}

// A configured database answers each policy catalog with the entries it holds.
// The procedures' own output rows are one view of those catalogs - kp.info lists
// one summary row for three catalogs - so a record is held to the catalog
// population the logical getter returns, never to a row count derived from the
// procedure's output.
func TestProfilePolicyMetadataCatalogProceduresReportTheCatalogsTheyRead(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)
		for _, statement := range []string{
			`CREATE DECAY PROFILE policy_metadata_base OPTIONS { halfLifeSeconds: 3600, function: 'none', scope: 'NODE', scoreFrom: 'CREATED' }`,
			`CREATE DECAY PROFILE policy_metadata_binding FOR (n:Fact) APPLY { DECAY PROFILE 'policy_metadata_base' }`,
			`CREATE PROMOTION PROFILE policy_metadata_boost OPTIONS { scope: 'NODE', multiplier: 1.1, scoreFloor: 0.1, scoreCap: 0.9 }`,
			`CREATE PROMOTION POLICY policy_metadata_policy FOR (n:Fact) APPLY { ON ACCESS { SET n.accessCount = n.accessCount + 1 } }`,
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}

		// The logical read APIs are the control: the records must state the
		// populations these getters answer with.
		schema := exec.storage.GetSchema()
		bundles, bindings := schema.ShowDecayProfiles()
		decayEntries := int64(len(bundles) + len(bindings))
		promotionProfiles := int64(len(schema.ShowPromotionProfiles()))
		promotionPolicies := int64(len(schema.ShowPromotionPolicies()))
		require.Positive(t, decayEntries)
		require.Positive(t, promotionProfiles)
		require.Positive(t, promotionPolicies)

		result, plan := profileEquivalentObservedStatement(t, exec, `CALL kp.profiles() YIELD kind, Name RETURN kind, Name`)
		require.Len(t, result.Rows, int(decayEntries), "one row per decay profile catalog entry")
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyDecayProfileCatalog, decayEntries)
		require.EqualValues(t, 1, plan.TotalDBHits, "the decay profile catalog is this statement's only read")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.policies() YIELD kind, Name RETURN kind, Name`)
		require.Len(t, result.Rows, int(promotionProfiles+promotionPolicies),
			"one row per promotion profile and policy")
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionProfileCatalog, promotionProfiles)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionPolicyCatalog, promotionPolicies)
		require.EqualValues(t, 2, plan.TotalDBHits, "both promotion catalogs are reported")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.info() YIELD enabled, decayProfiles, decayBindings, promotionProfiles, promotionPolicies RETURN enabled, decayProfiles, decayBindings, promotionProfiles, promotionPolicies`)
		require.Len(t, result.Rows, 1, "the summary is one row, not the catalogs it counted")
		require.Equal(t, len(bundles), result.Rows[0][1], "the summary counts the decay profiles it read")
		require.Equal(t, len(bindings), result.Rows[0][2], "the summary counts the decay bindings it read")
		require.Equal(t, int(promotionProfiles), result.Rows[0][3])
		require.Equal(t, int(promotionPolicies), result.Rows[0][4])
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyDecayProfileCatalog, decayEntries)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionProfileCatalog, promotionProfiles)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionPolicyCatalog, promotionPolicies)
		require.EqualValues(t, 3, plan.TotalDBHits, "the summary read all three catalogs")
	})
}

// kp.resolve reads the entity it scores and that entity's stored access
// metadata. Both point reads are reported where they happen, so an entity the
// store does not hold - and a store holding no access entry - are observed zeros
// rather than missing observations.
func TestProfileKnowledgePolicyResolveReportsItsPointReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)
		for _, statement := range []string{
			`CREATE DECAY PROFILE resolve_base OPTIONS { halfLifeSeconds: 3600, function: 'none', scope: 'NODE', scoreFrom: 'CREATED' }`,
			`CREATE DECAY PROFILE resolve_binding FOR (n:Fact) APPLY { DECAY PROFILE 'resolve_base' }`,
		} {
			_, err := exec.Execute(ctx, statement, nil)
			require.NoError(t, err, statement)
		}
		createdAt := time.Unix(1700000000, 0)
		_, err := exec.storage.CreateNode(ctx, &storage.Node{
			ID: "resolve-node", Labels: []string{"Fact"}, CreatedAt: createdAt,
		})
		require.NoError(t, err)
		_, err = exec.storage.CreateNode(ctx, &storage.Node{
			ID: "resolve-other", Labels: []string{"Fact"}, CreatedAt: createdAt,
		})
		require.NoError(t, err)
		require.NoError(t, exec.storage.CreateEdge(ctx, &storage.Edge{
			ID: "resolve-edge", StartNode: "resolve-node", EndNode: "resolve-other",
			Type: "LINKS", CreatedAt: createdAt,
		}))

		result, plan := profileEquivalentObservedStatement(t, exec, `CALL kp.resolve('resolve-node') YIELD TargetID, TargetScope, FinalScore RETURN TargetID, TargetScope, FinalScore`)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "resolve-node", result.Rows[0][0])
		require.Equal(t, "NODE", result.Rows[0][1])
		policyMetadataRecord(t, plan, profileKindNodeByIdSeek, 1)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyAccessMetaPoint, 0)
		require.Nil(t, planOperatorOfType(plan.Root, queryprofile.KindRelationshipByIDSeek),
			"the node answered, so no relationship read was attempted")
		require.EqualValues(t, 2, plan.TotalDBHits, "the node read and its access metadata read are reported")

		// The access metadata record states the population the storage read
		// answers with: this database holds no entry for the node.
		be := unwrapBadgerEngine(exec.storage)
		require.NotNil(t, be)
		meta, err := be.ReadAccessMeta(knowledgepolicy.AccessMetaReadRequest{
			Mode:     knowledgepolicy.AccessMetaReadPoint,
			EntityID: storage.QualifiedEntityID(exec.storage, "resolve-node"),
		})
		require.NoError(t, err)
		require.Nil(t, meta.Entry)

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.resolve('resolve-edge') YIELD TargetID, TargetScope RETURN TargetID, TargetScope`)
		require.Len(t, result.Rows, 1)
		require.Equal(t, "resolve-edge", result.Rows[0][0])
		require.Equal(t, "EDGE", result.Rows[0][1])
		policyMetadataRecord(t, plan, profileKindNodeByIdSeek, 0)
		policyMetadataRecord(t, plan, queryprofile.KindRelationshipByIDSeek, 1)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyAccessMetaPoint, 0)
		require.EqualValues(t, 3, plan.TotalDBHits,
			"the node attempt, the relationship read and the access metadata read are reported")

		// A dry-run resolution reads the compiled catalog in memory and no
		// entity, so it reports a certified zero.
		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.resolve('', 'Fact', '') YIELD TargetID, TargetScope RETURN TargetID, TargetScope`)
		require.Equal(t, "dry-run", result.Rows[0][0])
		require.Zero(t, plan.TotalDBHits, "a dry-run resolution reads no entity and no access metadata")
	})
}

// kp.deindexStatus reads the stored work queue. The record is the pending items
// the scan returned - not the status row the procedure answers with - and an
// empty queue is an observed zero of a scan that still ran.
func TestProfileKnowledgePolicyDeindexStatusReportsTheQueueScan(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		be := unwrapBadgerEngine(exec.storage)
		require.NotNil(t, be)
		require.NoError(t, be.PutDeindexWorkItem(t.Context(), &storage.DeindexWorkItem{
			WorkItemID:  "policy-metadata-work-1",
			TargetID:    "policy-metadata-target-1",
			TargetScope: "NODE",
			EnqueuedAt:  1715000000000,
			Status:      "pending",
		}))
		pending, err := be.ScanPendingDeindexWorkItems()
		require.NoError(t, err)
		require.Len(t, pending, 1, "the logical scan is the control for the record")

		result, plan := profileEquivalentObservedStatement(t, exec, `CALL kp.deindexStatus() YIELD pending_count, workItemId RETURN pending_count, workItemId`)
		require.Len(t, result.Rows, len(pending))
		require.Equal(t, len(pending), result.Rows[0][0])
		require.Equal(t, "policy-metadata-work-1", result.Rows[0][1])
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPendingDeindexScan, int64(len(pending)))
		require.EqualValues(t, 1, plan.TotalDBHits)
	})
}

// Empty and absent state is an observed zero, not a missing measurement: the
// catalog acquisitions and the queue scan ran, and kp.decayInfo reads no
// database state at all, so its zero is certified.
func TestProfilePolicyMetadataProceduresOnEmptyStateReportObservedZeros(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		schema := exec.storage.GetSchema()
		bundles, bindings := schema.ShowDecayProfiles()
		require.Empty(t, bundles, "the fixture starts with no decay profile")
		require.Empty(t, bindings)
		require.Empty(t, schema.ShowPromotionProfiles())
		require.Empty(t, schema.ShowPromotionPolicies())

		result, plan := profileEquivalentObservedStatement(t, exec, `CALL kp.decayInfo() YIELD enabled RETURN enabled`)
		require.Len(t, result.Rows, 1)
		require.Zero(t, plan.TotalDBHits,
			"the decay info answer is engine metadata and fixed values, not a database read")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.profiles() YIELD kind, Name RETURN kind, Name`)
		require.Empty(t, result.Rows)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyDecayProfileCatalog, 0)
		require.EqualValues(t, 1, plan.TotalDBHits, "the acquisition ran even though it listed nothing")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.policies() YIELD kind, Name RETURN kind, Name`)
		require.Empty(t, result.Rows)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionProfileCatalog, 0)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionPolicyCatalog, 0)
		require.EqualValues(t, 2, plan.TotalDBHits, "both empty catalogs are reported")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.info() YIELD decayProfiles, decayBindings RETURN decayProfiles, decayBindings`)
		require.Len(t, result.Rows, 1)
		require.Equal(t, 0, result.Rows[0][0])
		require.Equal(t, 0, result.Rows[0][1])
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyDecayProfileCatalog, 0)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionProfileCatalog, 0)
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPromotionPolicyCatalog, 0)
		require.EqualValues(t, 3, plan.TotalDBHits, "the summary still read all three catalogs")

		result, plan = profileEquivalentObservedStatement(t, exec, `CALL kp.deindexStatus() YIELD pending_count, supported RETURN pending_count, supported`)
		require.Len(t, result.Rows, 1, "an empty queue answers one status row")
		require.Equal(t, 0, result.Rows[0][0])
		require.Equal(t, true, result.Rows[0][1])
		policyMetadataRecord(t, plan, profileKindKnowledgePolicyPendingDeindexScan, 0)
		require.EqualValues(t, 1, plan.TotalDBHits, "the scan ran even though it found no pending work")
	})
}
