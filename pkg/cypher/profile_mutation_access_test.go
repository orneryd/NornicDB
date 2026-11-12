package cypher

// Public PROFILE controls for the built-in write statements' access ledger:
// CREATE, MATCH … SET, MATCH … REMOVE and MATCH … DETACH DELETE of nodes and
// relationships, and the MERGE routes that create or only match.
//
// Every test runs StorageExecutor.Execute, the public entry point Bolt and HTTP
// use, on both configured parsers, and asserts only what the supported contract
// states:
//
//   - PROFILE reports the logical access a statement actually requested: the
//     reads its canonical readers performed and one record per native mutation
//     request the transaction accepted or refused. It is not the statement's
//     final cardinality, and it is not derived from ExecuteStats (a MERGE that
//     matches writes nothing and still read its candidate population).
//   - A native mutation request is one staging call, not a physical write: the
//     transaction stages the entity and the commit decides what reaches
//     storage. A call that returned no error is one acknowledged row, a call
//     that failed is one attempt with no acknowledged row, and both are one
//     hit. A batch the transaction stages in one native call is one request
//     (NodeCreateRequest, RelationshipCreateRequest, …), while the node create
//     route that issues one call per node reports one request per node.
//   - A cascade stays one node-deletion request: the relationships the
//     transaction deletes with their node are not independent requests, so
//     DETACH DELETE reports no relationship-deletion request for them.
//   - A statement whose every read and every native request is accounted for
//     leaves no unobserved boundary, so its db hits are measured
//     (TotalDBHitsObserved). A statement that reads no graph entity reports no
//     graph read: a CREATE that matches nothing reports requests, never a
//     fabricated scan.
//   - A custom (opaque) callback stays explicitly unobserved, so accounting for
//     the built-in write roots does not certify a statement that ran one.
//
// The mutation work itself goes through one shared owner per request
// (transactionStorageWrapper.createNodes, createEdges, UpdateNode, UpdateEdge,
// DeleteGraphNode, DeleteEdge), which is where a request is counted once rather
// than in every clause, plan and batch route.

import (
	"testing"

	"github.com/orneryd/nornicdb/apoc/registry"
	"github.com/stretchr/testify/require"
)

// requireWriteAccountingObserved asserts the profile certified every access the
// statement made: nothing is left unobserved, so the plan's db hits are the
// statement's own. A write statement the ledger only partly describes must fail
// this, and so must one whose accesses were never counted at all.
func requireWriteAccountingObserved(t *testing.T, plan *ExecutionPlan, description string) {
	t.Helper()
	require.NotNil(t, plan, description)
	require.Empty(t, plan.Unobserved,
		"%s: every read and every native mutation request it made is accounted for", description)
	require.True(t, plan.TotalDBHitsObserved,
		"%s: its db hits are measured, not an unknown or a planner estimate", description)
}

// requireRequestRecord returns the plan's record of one native mutation request
// kind, asserting it is a measured request count whose rows are the requests the
// transaction acknowledged: never more rows than requests, since a failed
// request is an attempt without an acknowledged row.
func requireRequestRecord(t *testing.T, plan *ExecutionPlan, kind, description string) *PlanOperator {
	t.Helper()
	op := planOperatorOfType(plan.Root, kind)
	require.NotNil(t, op, "%s: the statement reports the %s it issued", description, kind)
	require.True(t, op.DBHitsObserved, "%s: the request count is measured", description)
	require.True(t, op.RowsObserved, "%s: the acknowledged requests are a measured row count", description)
	require.LessOrEqual(t, op.ActualRows, op.DBHits,
		"%s: a failed request is one attempt and no acknowledged row", description)
	return op
}

// requireNoRequestRecord asserts the statement issued no request of that kind:
// it either wrote nothing or wrote only what another request staged.
func requireNoRequestRecord(t *testing.T, plan *ExecutionPlan, kind, description string) {
	t.Helper()
	require.Nil(t, planOperatorOfType(plan.Root, kind),
		"%s: it issued no %s", description, kind)
}

// requireNoGraphReadAccess asserts the statement reported no graph read: it
// matched no node, read no index entry and expanded no relationship, so a scan
// or seek operator in its plan would be fabricated from its written entities.
func requireNoGraphReadAccess(t *testing.T, plan *ExecutionPlan, description string) {
	t.Helper()
	for _, kind := range []string{
		profileKindNodeByLabelScan,
		profileKindAllNodesScan,
		profileKindNodeByIdSeek,
		profileKindNodeIndexSeek,
		profileKindExpand,
	} {
		require.Nil(t, planOperatorOfType(plan.Root, kind),
			"%s: it read no graph entity, so it records no %s", description, kind)
	}
}

// requireScanAccessRecord asserts the access operator the canonical node scan
// reported: the label it read, the entities it read before any residual filter,
// and the one hit each of them is.
func requireScanAccessRecord(t *testing.T, plan *ExecutionPlan, label string, entities int64, description string) *PlanOperator {
	t.Helper()
	op := planOperatorOfType(plan.Root, profileKindNodeByLabelScan)
	require.NotNil(t, op, "%s: the label scan reports the population it read", description)
	require.Equal(t, label, op.Description,
		"%s: the record names the label the scan read", description)
	require.True(t, op.RowsObserved, "%s: the scan's rows are a measured count", description)
	require.True(t, op.DBHitsObserved, "%s: the scan's hits are a measured count", description)
	require.Equal(t, entities, op.ActualRows,
		"%s: every entity the scan read is a row of the access record", description)
	require.Equal(t, entities, op.DBHits,
		"%s: reading an entity is one hit", description)
	return op
}

// TestProfileCreateReportsAcceptedWritesAndNoGraphReads proves a CREATE that
// matches nothing exposes the requests it issued as measured hits: one native
// create request per node and one for the relationship, and no graph read it
// never performed.
func TestProfileCreateReportsAcceptedWritesAndNoGraphReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)

		result, err := exec.Execute(ctx,
			`PROFILE CREATE (a:WriteAudit {name: 'a'}), (b:WriteAudit {name: 'b'}), (a)-[:LINKS {w: 1}]->(b)`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement that ends in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 2, result.Stats.NodesCreated)
		require.Equal(t, 1, result.Stats.RelationshipsCreated)
		require.Equal(t, 3, result.Stats.PropertiesSet,
			"the two node names and the relationship's weight")

		plan := executionPlanFromResult(t, result)
		require.Equal(t, ModeProfile, plan.Mode)
		requireWriteAccountingObserved(t, plan,
			"a CREATE whose every request went through the shared create owners")
		requireNoGraphReadAccess(t, plan, "a CREATE that matches nothing")

		// The node create route stages one node per native call, so two nodes
		// are two requests, each acknowledged.
		nodes := requireRequestRecord(t, plan, profileKindNodeCreateRequest,
			"the two nodes the CREATE wrote")
		require.Equal(t, int64(2), nodes.DBHits, "one native create request per node")
		require.Equal(t, int64(2), nodes.ActualRows, "each accepted request is one acknowledged row")
		relationships := requireRequestRecord(t, plan, profileKindRelationshipCreateRequest,
			"the relationship the CREATE wrote")
		require.Equal(t, int64(1), relationships.DBHits, "one native create request for the relationship")
		require.Equal(t, int64(1), relationships.ActualRows)
		require.Equal(t, int64(3), plan.TotalDBHits,
			"the three requests are the only accesses the statement made")

		stored, err := exec.Execute(ctx,
			`MATCH (a:WriteAudit)-[r:LINKS]->(b:WriteAudit) RETURN a.name AS a, r.w AS w, b.name AS b`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a", int64(1), "b"}}, stored.Rows,
			"the profiled statement wrote exactly the graph it reported")

		count, err := exec.Execute(ctx, `MATCH (n:WriteAudit) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, count.Rows,
			"two nodes, not one per written property and not the final row count")
	})
}

// TestProfileCreateReportsOneAccessPerAcceptedWrite proves the reported hits
// follow the requests the statement issued: a batch of three node creates is
// three native requests, exactly as a scan of three entities reads three.
func TestProfileCreateReportsOneAccessPerAcceptedWrite(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		single, singleCtx := newProfileRuntimeTestExecutor(t)
		one, err := single.Execute(singleCtx, `PROFILE CREATE (n:WriteScaleOne {v: 1})`, nil)
		require.NoError(t, err)
		require.Equal(t, 1, one.Stats.NodesCreated)
		onePlan := executionPlanFromResult(t, one)
		requireWriteAccountingObserved(t, onePlan, "one accepted write")
		requireNoGraphReadAccess(t, onePlan, "a single CREATE reads nothing")
		oneRequest := requireRequestRecord(t, onePlan, profileKindNodeCreateRequest, "one node create")
		require.Equal(t, int64(1), oneRequest.DBHits)
		require.Equal(t, int64(1), oneRequest.ActualRows)
		require.Equal(t, int64(1), onePlan.TotalDBHits)

		batch, batchCtx := newProfileRuntimeTestExecutor(t)
		three, err := batch.Execute(batchCtx,
			`PROFILE CREATE (a:WriteScale {v: 1}), (b:WriteScale {v: 2}), (c:WriteScale {v: 3})`, nil)
		require.NoError(t, err)
		require.Equal(t, 3, three.Stats.NodesCreated)
		threePlan := executionPlanFromResult(t, three)
		requireWriteAccountingObserved(t, threePlan, "three accepted writes")
		requireNoGraphReadAccess(t, threePlan, "a batched CREATE reads nothing")
		threeRequests := requireRequestRecord(t, threePlan, profileKindNodeCreateRequest, "three node creates")
		require.Equal(t, int64(3), threeRequests.DBHits,
			"the node create route issues one native create request per node")
		require.Equal(t, int64(3), threeRequests.ActualRows,
			"each of the three requests was acknowledged")
		require.Equal(t, int64(3), threePlan.TotalDBHits)

		require.Greater(t, threePlan.TotalDBHits, onePlan.TotalDBHits,
			"three accepted writes report more hits than one: the reported hits follow the requests actually issued")
	})
}

// TestProfileMatchSetReportsTheScanReadAndTheAcceptedWrites proves a write
// statement keeps the reads its MATCH's canonical reader performed (the whole
// scanned population, before the WHERE) and adds one native update request per
// entity it stored.
func TestProfileMatchSetReportsTheScanReadAndTheAcceptedWrites(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			`CREATE (:WriteSet {name: 'a'})`,
			`CREATE (:WriteSet {name: 'b'})`,
			`CREATE (:WriteSet {name: 'c'})`)

		result, err := exec.Execute(ctx, `PROFILE MATCH (n:WriteSet) SET n.tag = 'x'`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement that ends in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 3, result.Stats.PropertiesSet, "one accepted property write per matched node")

		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "MATCH … SET")
		requireScanAccessRecord(t, plan, "WriteSet", 3, "the MATCH of the SET statement")
		updates := requireRequestRecord(t, plan, profileKindNodeUpdateRequest, "the property writes")
		require.Equal(t, int64(3), updates.DBHits, "one native node-update request per written entity")
		require.Equal(t, int64(3), updates.ActualRows, "each update request was acknowledged")
		require.Equal(t, int64(6), plan.TotalDBHits,
			"the three accepted updates are hits on top of the scan's three")

		stored, err := exec.Execute(ctx,
			`MATCH (n:WriteSet) RETURN n.name AS name, n.tag AS tag ORDER BY name`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a", "x"}, {"b", "x"}, {"c", "x"}}, stored.Rows)
	})
}

// TestProfileMatchSetReportsMoreHitsWhenItWritesMoreEntities proves the writes,
// not the reads, move the reported total: both statements scan the same
// population, and the one that issues three update requests reports more hits
// than the one whose WHERE admits a single row.
func TestProfileMatchSetReportsMoreHitsWhenItWritesMoreEntities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		seeds := []string{
			`CREATE (:WriteSetRank {name: 'a'})`,
			`CREATE (:WriteSetRank {name: 'b'})`,
			`CREATE (:WriteSetRank {name: 'c'})`,
		}

		one, oneCtx := newProfileRuntimeTestExecutor(t, seeds...)
		oneResult, err := one.Execute(oneCtx,
			`PROFILE MATCH (n:WriteSetRank) WHERE n.name = 'a' SET n.tag = 'x'`, nil)
		require.NoError(t, err)
		require.Equal(t, 1, oneResult.Stats.PropertiesSet, "one admitted row writes one entity")
		onePlan := executionPlanFromResult(t, oneResult)
		requireWriteAccountingObserved(t, onePlan, "a SET of one entity")
		requireScanAccessRecord(t, onePlan, "WriteSetRank", 3, "the MATCH that admitted one row")
		oneUpdates := requireRequestRecord(t, onePlan, profileKindNodeUpdateRequest, "the one property write")
		require.Equal(t, int64(1), oneUpdates.DBHits, "one admitted entity is one update request")
		require.Equal(t, int64(4), onePlan.TotalDBHits, "the scan's three and the one update")

		three, threeCtx := newProfileRuntimeTestExecutor(t, seeds...)
		threeResult, err := three.Execute(threeCtx,
			`PROFILE MATCH (n:WriteSetRank) SET n.tag = 'x'`, nil)
		require.NoError(t, err)
		require.Equal(t, 3, threeResult.Stats.PropertiesSet, "three rows write three entities")
		threePlan := executionPlanFromResult(t, threeResult)
		requireWriteAccountingObserved(t, threePlan, "a SET of three entities")
		requireScanAccessRecord(t, threePlan, "WriteSetRank", 3, "the MATCH without a WHERE")
		threeUpdates := requireRequestRecord(t, threePlan, profileKindNodeUpdateRequest, "the three property writes")
		require.Equal(t, int64(3), threeUpdates.DBHits)
		require.Equal(t, int64(6), threePlan.TotalDBHits, "the scan's three and the three updates")

		require.Greater(t, threePlan.TotalDBHits, onePlan.TotalDBHits,
			"the statement that wrote three entities reports more hits than the one that wrote one, over the same scan")
	})
}

// TestProfileMatchRemoveReportsTheScanReadAndTheAcceptedWrites proves REMOVE —
// which stores its writes through the same entity-update owner as SET — is
// accounted for like SET, and that the properties it removed are gone.
func TestProfileMatchRemoveReportsTheScanReadAndTheAcceptedWrites(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			`CREATE (:WriteRemove {name: 'a', keep: 1})`,
			`CREATE (:WriteRemove {name: 'b', keep: 1})`,
			`CREATE (:WriteRemove {name: 'c', keep: 1})`)

		result, err := exec.Execute(ctx, `PROFILE MATCH (n:WriteRemove) REMOVE n.keep`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement that ends in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 3, result.Stats.PropertiesSet,
			"a removed property counts as a property write, as in Neo4j")

		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "MATCH … REMOVE")
		requireScanAccessRecord(t, plan, "WriteRemove", 3, "the MATCH of the REMOVE statement")
		updates := requireRequestRecord(t, plan, profileKindNodeUpdateRequest, "the property removals")
		require.Equal(t, int64(3), updates.DBHits, "one native node-update request per removed property")
		require.Equal(t, int64(3), updates.ActualRows)
		require.Equal(t, int64(6), plan.TotalDBHits,
			"the three accepted removals are hits on top of the scan's three")

		stored, err := exec.Execute(ctx,
			`MATCH (n:WriteRemove) RETURN n.name AS name, n.keep AS keep ORDER BY name`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"a", nil}, {"b", nil}, {"c", nil}}, stored.Rows,
			"every removed property is gone")
	})
}

// TestProfileMatchDetachDeleteReportsTheScanReadAndTheAcceptedDeletes proves
// DETACH DELETE reports one node-deletion request per deleted node, on top of
// the population its MATCH read, and does not claim the cascade's relationship
// deletions as requests of their own.
func TestProfileMatchDetachDeleteReportsTheScanReadAndTheAcceptedDeletes(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			`CREATE (a:WriteDelete {name: 'a'}), (b:WriteDelete {name: 'b'}), (c:WriteDelete {name: 'c'}), (c)-[:DELETES]->(a)`)

		result, err := exec.Execute(ctx, `PROFILE MATCH (n:WriteDelete) DETACH DELETE n`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement that ends in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 3, result.Stats.NodesDeleted)
		require.Equal(t, 1, result.Stats.RelationshipsDeleted,
			"the detached node's incident relationship is deleted once")

		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "MATCH … DETACH DELETE")
		requireScanAccessRecord(t, plan, "WriteDelete", 3, "the MATCH of the DELETE statement")
		deletes := requireRequestRecord(t, plan, profileKindNodeDeleteRequest, "the node deletions")
		require.Equal(t, int64(3), deletes.DBHits, "one native deletion request per deleted node")
		require.Equal(t, int64(3), deletes.ActualRows, "each deletion request was acknowledged")
		requireNoRequestRecord(t, plan, profileKindRelationshipDeleteRequest,
			"a cascade is the node-deletion request's own work")
		require.GreaterOrEqual(t, plan.TotalDBHits, int64(6),
			"the three node deletions are hits on top of the scan's three and the incidence reads the DELETE performed")

		nodes, err := exec.Execute(ctx, `MATCH (n:WriteDelete) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, nodes.Rows)
		edges, err := exec.Execute(ctx, `MATCH ()-[r:DELETES]->() RETURN count(r) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, edges.Rows)
	})
}

// TestProfileDeleteRelationshipReportsOneRelationshipDeleteRequest proves a
// DELETE of a relationship is its own request, and that it is never confused
// with the node-deletion request a cascade performs.
func TestProfileDeleteRelationshipReportsOneRelationshipDeleteRequest(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			`CREATE (a:WriteDropEdge {name: 'a'}), (b:WriteDropEdge {name: 'b'}), (a)-[:REMOVES]->(b)`)

		result, err := exec.Execute(ctx,
			`PROFILE MATCH (a:WriteDropEdge)-[r:REMOVES]->(b:WriteDropEdge) DELETE r`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows, "a statement that ends in a write returns no rows")
		require.NotNil(t, result.Stats)
		require.Equal(t, 1, result.Stats.RelationshipsDeleted)
		require.Zero(t, result.Stats.NodesDeleted)

		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "MATCH … DELETE r")
		deleted := requireRequestRecord(t, plan, profileKindRelationshipDeleteRequest, "the relationship deletion")
		require.Equal(t, int64(1), deleted.DBHits, "one native relationship-deletion request")
		require.Equal(t, int64(1), deleted.ActualRows, "the request was acknowledged")
		requireNoRequestRecord(t, plan, profileKindNodeDeleteRequest, "the statement deletes no node")

		edges, err := exec.Execute(ctx, `MATCH ()-[r:REMOVES]->() RETURN count(r) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(0)}}, edges.Rows)
		nodes, err := exec.Execute(ctx, `MATCH (n:WriteDropEdge) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, nodes.Rows,
			"deleting the relationship leaves both endpoints")
	})
}

// TestProfileWriteThatMatchesNothingIssuesNoRequest proves an empty write makes
// no request at all: the scan is a measured zero and the statement issues no
// update, so its db hits are zero rather than an invented request.
func TestProfileWriteThatMatchesNothingIssuesNoRequest(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t)

		result, err := exec.Execute(ctx, `PROFILE MATCH (n:WriteAbsent) SET n.tag = 'x'`, nil)
		require.NoError(t, err)
		require.Empty(t, result.Rows)
		require.NotNil(t, result.Stats)
		require.Zero(t, result.Stats.PropertiesSet)

		plan := executionPlanFromResult(t, result)
		requireWriteAccountingObserved(t, plan, "a write that matched nothing")
		requireScanAccessRecord(t, plan, "WriteAbsent", 0, "the scan of a label with no nodes")
		requireNoRequestRecord(t, plan, profileKindNodeUpdateRequest, "nothing was written")
		require.Zero(t, plan.TotalDBHits, "the empty scan and the absent requests are zero hits")
	})
}

// TestProfileMergeReportsItsCandidateReadWhenItOnlyMatches proves the MERGE
// that writes nothing is still observed: it read its candidate population and
// issued no request, and ExecuteStats cannot supply that count (they are all
// zero). A profile that reports no access here has dropped the preparation
// read.
func TestProfileMergeReportsItsCandidateReadWhenItOnlyMatches(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t, `CREATE (:WriteMerge {key: 'k1'})`)

		matched, err := exec.Execute(ctx, `PROFILE MERGE (n:WriteMerge {key: 'k1'}) RETURN n.key AS key`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"k1"}}, matched.Rows)
		require.NotNil(t, matched.Stats)
		require.Zero(t, matched.Stats.NodesCreated, "a matching MERGE writes nothing")
		require.Zero(t, matched.Stats.PropertiesSet)

		matchedPlan := executionPlanFromResult(t, matched)
		requireWriteAccountingObserved(t, matchedPlan, "a MERGE that only matched")
		require.Positive(t, matchedPlan.TotalDBHits,
			"the candidate population it read is a measured access, never an ExecuteStats or final-row count")
		requireNoRequestRecord(t, matchedPlan, profileKindNodeCreateRequest,
			"a matching MERGE stages no node")
		requireNoRequestRecord(t, matchedPlan, profileKindNodeUpdateRequest,
			"a matching MERGE with no ON MATCH SET writes no property")

		created, err := exec.Execute(ctx, `PROFILE MERGE (n:WriteMerge {key: 'k2'}) RETURN n.key AS key`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"k2"}}, created.Rows)
		require.NotNil(t, created.Stats)
		require.Equal(t, 1, created.Stats.NodesCreated)
		createdPlan := executionPlanFromResult(t, created)
		requireWriteAccountingObserved(t, createdPlan, "a MERGE that created its pattern")
		require.Positive(t, createdPlan.TotalDBHits,
			"the accepted create is a hit of its own")
		createRequests := requireRequestRecord(t, createdPlan, profileKindNodeCreateRequest,
			"the node the MERGE created")
		require.Equal(t, int64(1), createRequests.DBHits, "one native create request")
		require.Equal(t, int64(1), createRequests.ActualRows)

		count, err := exec.Execute(ctx, `MATCH (n:WriteMerge) RETURN count(n) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(2)}}, count.Rows,
			"one matched node plus one created node")
	})
}

// TestProfileRelationshipMergeReportsAcceptedWrites proves the relationship
// MERGE route reports the relationships it created and, on the run that only
// matches, reports the reads that matched them without inventing a create
// request.
func TestProfileRelationshipMergeReportsAcceptedWrites(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, ctx := newProfileRuntimeTestExecutor(t,
			`CREATE (:WriteEndpoint {name: 'a'})`,
			`CREATE (:WriteEndpoint {name: 'b'})`)

		const merge = `PROFILE MATCH (a:WriteEndpoint), (b:WriteEndpoint) MERGE (a)-[r:KNOWS {since: 2019}]->(b)`
		created, err := exec.Execute(ctx, merge, nil)
		require.NoError(t, err)
		require.NotNil(t, created.Stats)
		require.Equal(t, 4, created.Stats.RelationshipsCreated,
			"one relationship for each of the two endpoints' four ordered pairs")
		createdPlan := executionPlanFromResult(t, created)
		requireWriteAccountingObserved(t, createdPlan, "a relationship MERGE that created its pattern")
		createdRequests := requireRequestRecord(t, createdPlan, profileKindRelationshipCreateRequest,
			"the relationships the MERGE created")
		require.Equal(t, int64(4), createdRequests.DBHits,
			"the row-ordered route issues one native relationship create request per created relationship")
		require.Equal(t, int64(4), createdRequests.ActualRows,
			"each create request was acknowledged")

		matched, err := exec.Execute(ctx, merge, nil)
		require.NoError(t, err)
		require.NotNil(t, matched.Stats)
		require.Zero(t, matched.Stats.RelationshipsCreated, "a matching MERGE writes nothing")
		matchedPlan := executionPlanFromResult(t, matched)
		requireWriteAccountingObserved(t, matchedPlan, "a relationship MERGE that only matched")
		require.Positive(t, matchedPlan.TotalDBHits,
			"the nodes its MATCH read and the relationships it matched are measured accesses")
		requireNoRequestRecord(t, matchedPlan, profileKindRelationshipCreateRequest,
			"a matching MERGE stages no relationship")

		count, err := exec.Execute(ctx, `MATCH ()-[r:KNOWS]->() RETURN count(r) AS c`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{int64(4)}}, count.Rows,
			"the matching run wrote no fifth relationship")
	})
}

// TestProfileWriteKeepsAnOpaqueCallbackUnobserved proves accounting for the
// built-in write roots must not certify a custom callback: the CREATE's own
// request is measured, and the statement stays unobserved for the callback
// alone.
func TestProfileWriteKeepsAnOpaqueCallbackUnobserved(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		handler, err := registry.Compile(func() string { return "opaque" }, registry.OpaqueReads)
		require.NoError(t, err)
		previous := PluginFunctionLookup
		t.Cleanup(func() { PluginFunctionLookup = previous })
		PluginFunctionLookup = func(name string) (*PluginFunctionHandler, bool) {
			if name == "profile.opaque_write" {
				return handler, true
			}
			if previous != nil {
				return previous(name)
			}
			return nil, false
		}

		exec, ctx := newProfileRuntimeTestExecutor(t)
		result, err := exec.Execute(ctx,
			`PROFILE CREATE (n:WriteOpaque {v: profile.opaque_write()}) RETURN n.v AS v`, nil)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{"opaque"}}, result.Rows)
		require.NotNil(t, result.Stats)
		require.Equal(t, 1, result.Stats.NodesCreated)

		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved,
			"an observed write never certifies the rest of an opaque callback's access")
		require.NotEmpty(t, plan.Unobserved)
		for _, reason := range plan.Unobserved {
			require.NotContains(t, reason, "write statement storage accesses",
				"a built-in write's own accesses are accounted for, so only the custom callback leaves the statement unobserved")
		}
		createRequests := requireRequestRecord(t, plan, profileKindNodeCreateRequest,
			"the node the CREATE wrote")
		require.Equal(t, int64(1), createRequests.DBHits,
			"the opaque callback leaves the statement's own request measured")
		require.Equal(t, int64(1), createRequests.ActualRows)
	})
}
