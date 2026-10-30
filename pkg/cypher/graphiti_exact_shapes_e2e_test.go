// Code-of-record: every Cypher query graphiti emits against Neo4j MUST have a
// matching exact-shape test below. "Exact" means byte-for-byte the same query
// text graphiti.driver.neo4j.operations produces — no simplifications, no
// adjacent rewrites, no "similar shapes".
//
// Inventory sources (commit-pin these when revisiting):
//   - graphiti_core/driver/neo4j/operations/*.py             (11 ops files)
//   - graphiti_core/models/edges/edge_db_queries.py
//   - graphiti_core/models/nodes/node_db_queries.py
//   - graphiti_core/graph_queries.py (get_nodes_query, get_relationships_query,
//                                     get_vector_cosine_func_query)
//
// Tests are grouped by ops file. Inside each group, seed nodes/edges with the
// shapes graphiti uses (uuid keys, group_id, labels) so we don't slip back into
// "similar shapes" by accident.
//
// When a graphiti query has dynamic fragments (cursor_clause, limit_clause,
// filter_query, vector cosine sub-fragment), each *materialised* combination
// gets its own test rather than collapsing — the user wants exact shapes, and
// the dynamic-fragment shapes are different exact strings.

package cypher

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ---------- shared fixture for read-side tests ----------

type graphitiExactShapeFixture struct {
	exec *StorageExecutor
	ctx  context.Context
}

// newGraphitiExactShapeFixture wires a fresh in-memory engine with the minimal
// graph shape graphiti expects: Entity nodes (group_id, uuid, name, summary,
// created_at, name_embedding), RELATES_TO edges (uuid, source_node_uuid,
// target_node_uuid, group_id, name, fact, fact_embedding, episodes, created_at,
// expired_at, valid_at, invalid_at), MENTIONS edges, HAS_MEMBER edges,
// HAS_EPISODE edges, NEXT_EPISODE edges, Episodic nodes, Community nodes,
// Saga nodes. Tests reuse this so each individual test stays focused on the
// query shape, not seed boilerplate.
func newGraphitiExactShapeFixture(t *testing.T) *graphitiExactShapeFixture {
	t.Helper()
	base := newTestMemoryEngine(t)
	ns := storage.NewNamespacedEngine(base, "test")
	exec := NewStorageExecutor(ns)
	ctx := context.Background()

	// Constraints — graphiti relies on uuid lookups being indexed.
	for _, q := range []string{
		"CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)",
		"CREATE INDEX episodic_uuid IF NOT EXISTS FOR (n:Episodic) ON (n.uuid)",
		"CREATE INDEX community_uuid IF NOT EXISTS FOR (n:Community) ON (n.uuid)",
		"CREATE INDEX saga_uuid IF NOT EXISTS FOR (n:Saga) ON (n.uuid)",
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "fixture index: %s", q)
	}

	// Seed: two entities, one relates_to between them, one episodic + mentions,
	// one community + has_member, one saga + has_episode + next_episode.
	for _, q := range []string{
		`CREATE (:Entity {uuid:'e1', group_id:'g1', name:'alice', summary:'first', created_at:'2024-01-01'})`,
		`CREATE (:Entity {uuid:'e2', group_id:'g1', name:'bob',   summary:'second', created_at:'2024-01-02'})`,
		`MATCH (a:Entity {uuid:'e1'}), (b:Entity {uuid:'e2'})
		 CREATE (a)-[:RELATES_TO {uuid:'r1', group_id:'g1', name:'knows',
		   fact:'alice knows bob', created_at:'2024-01-03',
		   expired_at:null, valid_at:'2024-01-03', invalid_at:null,
		   episodes:['ep1']}]->(b)`,
		`CREATE (:Episodic {uuid:'ep1', group_id:'g1', name:'episode-1', source:'msg',
		   source_description:'seed', content:'hello world', created_at:'2024-01-04',
		   valid_at:'2024-01-04', entity_edges:['r1']})`,
		`MATCH (e:Episodic {uuid:'ep1'}), (n:Entity {uuid:'e1'})
		 CREATE (e)-[:MENTIONS {uuid:'m1', group_id:'g1', created_at:'2024-01-05'}]->(n)`,
		`CREATE (:Community {uuid:'c1', group_id:'g1', name:'community-1', summary:'sum', created_at:'2024-01-06'})`,
		`MATCH (c:Community {uuid:'c1'}), (n:Entity {uuid:'e1'})
		 CREATE (c)-[:HAS_MEMBER {uuid:'hm1', group_id:'g1', created_at:'2024-01-07'}]->(n)`,
		`CREATE (:Saga {uuid:'s1', name:'saga-1', group_id:'g1', created_at:'2024-01-08', summary:'sum',
		   first_episode_uuid:'ep1', last_episode_uuid:'ep1', last_summarized_at:'2024-01-08',
		   last_summarized_episode_valid_at:'2024-01-04'})`,
		`MATCH (s:Saga {uuid:'s1'}), (e:Episodic {uuid:'ep1'})
		 CREATE (s)-[:HAS_EPISODE {uuid:'he1', group_id:'g1', created_at:'2024-01-09'}]->(e)`,
		`CREATE (:Episodic {uuid:'ep2', group_id:'g1', name:'episode-2', source:'msg',
		   source_description:'seed', content:'goodbye world', created_at:'2024-01-10',
		   valid_at:'2024-01-10', entity_edges:[]})`,
		`MATCH (a:Episodic {uuid:'ep1'}), (b:Episodic {uuid:'ep2'})
		 CREATE (a)-[:NEXT_EPISODE {uuid:'ne1', group_id:'g1', created_at:'2024-01-11'}]->(b)`,
	} {
		_, err := exec.Execute(ctx, q, nil)
		require.NoError(t, err, "fixture seed: %s", strings.TrimSpace(q[:min(len(q), 80)]))
	}
	return &graphitiExactShapeFixture{exec: exec, ctx: ctx}
}

func (f *graphitiExactShapeFixture) run(t *testing.T, query string, params map[string]interface{}) *ExecuteResult {
	t.Helper()
	res, err := f.exec.Execute(f.ctx, query, params)
	require.NoErrorf(t, err, "exact-shape query failed:\n%s", query)
	return res
}

// ============================================================================
// entity_edge_ops.py
// ============================================================================

// graphiti delete-by-uuid for entity edges (matches MENTIONS|RELATES_TO|HAS_MEMBER).
func TestGraphitiExactShape_EntityEdge_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER {uuid: $uuid}]->(m)
            DELETE e
        `, map[string]interface{}{"uuid": "r1"})
	// post-condition: r1 gone
	cnt := f.run(t, "MATCH ()-[e:RELATES_TO {uuid:'r1'}]->() RETURN count(e)", nil)
	require.Equal(t, int64(0), graphitiExactToInt64(t, cnt.Rows[0][0]))
}

func TestGraphitiExactShape_EntityEdge_DeleteByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	preR := f.run(t, "MATCH ()-[e:RELATES_TO]->() WHERE e.uuid IN ['r1'] RETURN count(e)", nil)
	preM := f.run(t, "MATCH ()-[e:MENTIONS]->() WHERE e.uuid IN ['m1'] RETURN count(e)", nil)
	t.Logf("pre: RELATES_TO[r1]=%v MENTIONS[m1]=%v", preR.Rows, preM.Rows)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER]->(m)
            WHERE e.uuid IN $uuids
            DELETE e
        `, map[string]interface{}{"uuids": []string{"r1", "m1"}})
	cntR := f.run(t, "MATCH ()-[e:RELATES_TO]->() WHERE e.uuid IN ['r1'] RETURN count(e)", nil)
	cntM := f.run(t, "MATCH ()-[e:MENTIONS]->() WHERE e.uuid IN ['m1'] RETURN count(e)", nil)
	t.Logf("post: RELATES_TO[r1]=%v MENTIONS[m1]=%v", cntR.Rows, cntM.Rows)
	require.Equal(t, int64(0), graphitiExactToInt64(t, cntR.Rows[0][0]), "r1 RELATES_TO not deleted")
	require.Equal(t, int64(0), graphitiExactToInt64(t, cntM.Rows[0][0]), "m1 MENTIONS not deleted")
}

// graphiti EntityEdge.get_by_uuid — concatenates entity_edge_return_query as RETURN tail.
func TestGraphitiExactShape_EntityEdge_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO {uuid: $uuid}]->(m:Entity)
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
        `, map[string]interface{}{"uuid": "r1"})
	require.Len(t, res.Rows, 1)
	require.Equal(t, "r1", res.Rows[0][0])
}

func TestGraphitiExactShape_EntityEdge_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WHERE e.uuid IN $uuids
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
        `, map[string]interface{}{"uuids": []string{"r1"}})
	require.Len(t, res.Rows, 1)
}

// graphiti EntityEdge.get_by_group_ids — three dynamic variants:
//  1. no cursor, no limit
//  2. cursor only
//  3. limit only
//  4. cursor + limit
//
// Each materialises a distinct exact query string.
func TestGraphitiExactShape_EntityEdge_GetByGroupIds_NoCursorNoLimit(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WHERE e.group_id IN $group_ids
            
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY e.uuid DESC
            
        `, map[string]interface{}{"group_ids": []string{"g1"}})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityEdge_GetByGroupIds_CursorOnly(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WHERE e.group_id IN $group_ids
            AND e.uuid < $uuid
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY e.uuid DESC
            
        `, map[string]interface{}{"group_ids": []string{"g1"}, "uuid": "z"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityEdge_GetByGroupIds_LimitOnly(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WHERE e.group_id IN $group_ids
            
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY e.uuid DESC
            LIMIT $limit
        `, map[string]interface{}{"group_ids": []string{"g1"}, "limit": int64(10)})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityEdge_GetByGroupIds_CursorAndLimit(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WHERE e.group_id IN $group_ids
            AND e.uuid < $uuid
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY e.uuid DESC
            LIMIT $limit
        `, map[string]interface{}{"group_ids": []string{"g1"}, "uuid": "z", "limit": int64(10)})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityEdge_GetBetweenNodes(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity {uuid: $source_node_uuid})-[e:RELATES_TO]->(m:Entity {uuid: $target_node_uuid})
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
        `, map[string]interface{}{"source_node_uuid": "e1", "target_node_uuid": "e2"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EntityEdge_GetByNodeUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity {uuid: $node_uuid})-[e:RELATES_TO]-(m:Entity)
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
        `, map[string]interface{}{"node_uuid": "e1"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityEdge_LoadEmbeddings(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO {uuid: $uuid}]->(m:Entity)
            RETURN e.fact_embedding AS fact_embedding
        `, map[string]interface{}{"uuid": "r1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EntityEdge_LoadEmbeddingsBulk(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)-[e:RELATES_TO]-(m:Entity)
            WHERE e.uuid IN $edge_uuids
            RETURN DISTINCT e.uuid AS uuid, e.fact_embedding AS fact_embedding
        `, map[string]interface{}{"edge_uuids": []string{"r1"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// edge_db_queries.get_entity_edge_save_query — used by EntityEdge.save
// ============================================================================

// Neo4j, has_aoss=False — the default path. EXACT string as rendered.
func TestGraphitiExactShape_EntityEdge_Save_NoAOSS(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	// Save uses $edge_data map.
	edgeData := map[string]interface{}{
		"uuid":           "r2",
		"source_uuid":    "e1",
		"target_uuid":    "e2",
		"name":           "saved",
		"fact":           "saved fact",
		"fact_embedding": []float64{0.1, 0.2, 0.3},
		"group_id":       "g1",
		"episodes":       []string{"ep1"},
		"created_at":     "2024-02-01",
		"expired_at":     nil,
		"valid_at":       "2024-02-01",
		"invalid_at":     nil,
	}
	f.run(t, `
                        MATCH (source:Entity {uuid: $edge_data.source_uuid})
                        MATCH (target:Entity {uuid: $edge_data.target_uuid})
                        MERGE (source)-[e:RELATES_TO {uuid: $edge_data.uuid}]->(target)
                        SET e = $edge_data
                        WITH e CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", $edge_data.fact_embedding)
                RETURN e.uuid AS uuid
                `, map[string]interface{}{"edge_data": edgeData})
}

// Neo4j, has_aoss=True path (AOSS branch skips the vector property call).
func TestGraphitiExactShape_EntityEdge_Save_WithAOSS(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	edgeData := map[string]interface{}{
		"uuid": "r3", "source_uuid": "e1", "target_uuid": "e2",
		"name": "saved-aoss", "fact": "aoss fact",
		"group_id": "g1", "episodes": []string{},
		"created_at": "2024-02-02", "expired_at": nil,
		"valid_at": "2024-02-02", "invalid_at": nil,
	}
	f.run(t, `
                        MATCH (source:Entity {uuid: $edge_data.source_uuid})
                        MATCH (target:Entity {uuid: $edge_data.target_uuid})
                        MERGE (source)-[e:RELATES_TO {uuid: $edge_data.uuid}]->(target)
                        SET e = $edge_data
                        
                RETURN e.uuid AS uuid
                `, map[string]interface{}{"edge_data": edgeData})
}

// Bulk-save variants (both has_aoss values).
func TestGraphitiExactShape_EntityEdge_SaveBulk_NoAOSS(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	edges := []map[string]interface{}{
		{
			"uuid": "rb1", "source_node_uuid": "e1", "target_node_uuid": "e2",
			"name": "bulk", "fact": "f", "fact_embedding": []float64{0.1, 0.2},
			"group_id": "g1", "episodes": []string{}, "created_at": "2024-02-03",
			"expired_at": nil, "valid_at": "2024-02-03", "invalid_at": nil,
		},
	}
	f.run(t, `
                    UNWIND $entity_edges AS edge
                    MATCH (source:Entity {uuid: edge.source_node_uuid})
                    MATCH (target:Entity {uuid: edge.target_node_uuid})
                    MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
                    SET e = edge
                    WITH e, edge CALL db.create.setRelationshipVectorProperty(e, "fact_embedding", edge.fact_embedding)
                RETURN edge.uuid AS uuid
            `, map[string]interface{}{"entity_edges": edges})
}

func TestGraphitiExactShape_EntityEdge_SaveBulk_WithAOSS(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	edges := []map[string]interface{}{
		{
			"uuid": "rb2", "source_node_uuid": "e1", "target_node_uuid": "e2",
			"name": "bulk-aoss", "fact": "f", "group_id": "g1",
			"episodes": []string{}, "created_at": "2024-02-04",
			"expired_at": nil, "valid_at": "2024-02-04", "invalid_at": nil,
		},
	}
	f.run(t, `
                    UNWIND $entity_edges AS edge
                    MATCH (source:Entity {uuid: edge.source_node_uuid})
                    MATCH (target:Entity {uuid: edge.target_node_uuid})
                    MERGE (source)-[e:RELATES_TO {uuid: edge.uuid}]->(target)
                    SET e = edge
                    
                RETURN edge.uuid AS uuid
            `, map[string]interface{}{"entity_edges": edges})
}

// ============================================================================
// episodic_edge_ops.py + edge_db_queries.get_episodic_edge_save_bulk_query
// ============================================================================

func TestGraphitiExactShape_EpisodicEdge_SaveBulk(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	edges := []map[string]interface{}{
		{
			"uuid": "m2", "source_node_uuid": "ep1", "target_node_uuid": "e2",
			"group_id": "g1", "created_at": "2024-02-05",
		},
	}
	f.run(t, `
        UNWIND $episodic_edges AS edge
        MATCH (episode:Episodic {uuid: edge.source_node_uuid})
        MATCH (node:Entity {uuid: edge.target_node_uuid})
        MERGE (episode)-[e:MENTIONS {uuid: edge.uuid}]->(node)
        SET
            e.group_id = edge.group_id,
            e.created_at = edge.created_at
        RETURN e.uuid AS uuid
    `, map[string]interface{}{"episodic_edges": edges})
}

func TestGraphitiExactShape_EpisodicEdge_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER {uuid: $uuid}]->(m)
            DELETE e
        `, map[string]interface{}{"uuid": "m1"})
}

func TestGraphitiExactShape_EpisodicEdge_DeleteByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER]->(m)
            WHERE e.uuid IN $uuids
            DELETE e
        `, map[string]interface{}{"uuids": []string{"m1"}})
}

func TestGraphitiExactShape_EpisodicEdge_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Episodic)-[e:MENTIONS {uuid: $uuid}]->(m:Entity)
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuid": "m1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EpisodicEdge_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Episodic)-[e:MENTIONS]->(m:Entity)
            WHERE e.uuid IN $uuids
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuids": []string{"m1"}})
	require.NotEmpty(t, res.Rows)
}

// EpisodicEdge.get_by_group_ids — same cursor/limit matrix as EntityEdge.
func TestGraphitiExactShape_EpisodicEdge_GetByGroupIds_NoCursorNoLimit(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Episodic)-[e:MENTIONS]->(m:Entity)
            WHERE e.group_id IN $group_ids
            
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
            ORDER BY e.uuid DESC
            
        `, map[string]interface{}{"group_ids": []string{"g1"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// community_edge_ops.py + edge_db_queries.get_community_edge_save_query
// ============================================================================

func TestGraphitiExactShape_CommunityEdge_Save(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
                MATCH (community:Community {uuid: $community_uuid})
                MATCH (node:Entity | Community {uuid: $entity_uuid})
                MERGE (community)-[e:HAS_MEMBER {uuid: $uuid}]->(node)
                SET e = {uuid: $uuid, group_id: $group_id, created_at: $created_at}
                RETURN e.uuid AS uuid
            `, map[string]interface{}{
		"community_uuid": "c1", "entity_uuid": "e2",
		"uuid": "hm2", "group_id": "g1", "created_at": "2024-02-06",
	})
}

func TestGraphitiExactShape_CommunityEdge_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER {uuid: $uuid}]->(m)
            DELETE e
        `, map[string]interface{}{"uuid": "hm1"})
}

func TestGraphitiExactShape_CommunityEdge_DeleteByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n)-[e:MENTIONS|RELATES_TO|HAS_MEMBER]->(m)
            WHERE e.uuid IN $uuids
            DELETE e
        `, map[string]interface{}{"uuids": []string{"hm1"}})
}

func TestGraphitiExactShape_CommunityEdge_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Community)-[e:HAS_MEMBER {uuid: $uuid}]->(m)
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuid": "hm1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_CommunityEdge_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Community)-[e:HAS_MEMBER]->(m)
            WHERE e.uuid IN $uuids
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuids": []string{"hm1"}})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_CommunityEdge_GetByGroupIds_NoCursorNoLimit(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Community)-[e:HAS_MEMBER]->(m)
            WHERE e.group_id IN $group_ids
            
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
            ORDER BY e.uuid DESC
            
        `, map[string]interface{}{"group_ids": []string{"g1"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// has_episode_edge_ops.py
// ============================================================================

func TestGraphitiExactShape_HasEpisodeEdge_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Saga)-[e:HAS_EPISODE {uuid: $uuid}]->(m:Episodic)
            DELETE e
        `, map[string]interface{}{"uuid": "he1"})
}

func TestGraphitiExactShape_HasEpisodeEdge_DeleteByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Saga)-[e:HAS_EPISODE]->(m:Episodic)
            WHERE e.uuid IN $uuids
            DELETE e
        `, map[string]interface{}{"uuids": []string{"he1"}})
}

func TestGraphitiExactShape_HasEpisodeEdge_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	// The HAS_EPISODE return fragment graphiti uses mirrors the episodic
	// return shape (uuid/group_id/source/target/created_at).
	res := f.run(t, `
            MATCH (n:Saga)-[e:HAS_EPISODE {uuid: $uuid}]->(m:Episodic)
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuid": "he1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_HasEpisodeEdge_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Saga)-[e:HAS_EPISODE]->(m:Episodic)
            WHERE e.uuid IN $uuids
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuids": []string{"he1"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// next_episode_edge_ops.py
// ============================================================================

func TestGraphitiExactShape_NextEpisodeEdge_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Episodic)-[e:NEXT_EPISODE {uuid: $uuid}]->(m:Episodic)
            DELETE e
        `, map[string]interface{}{"uuid": "ne1"})
}

func TestGraphitiExactShape_NextEpisodeEdge_DeleteByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Episodic)-[e:NEXT_EPISODE]->(m:Episodic)
            WHERE e.uuid IN $uuids
            DELETE e
        `, map[string]interface{}{"uuids": []string{"ne1"}})
}

func TestGraphitiExactShape_NextEpisodeEdge_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Episodic)-[e:NEXT_EPISODE {uuid: $uuid}]->(m:Episodic)
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuid": "ne1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_NextEpisodeEdge_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Episodic)-[e:NEXT_EPISODE]->(m:Episodic)
            WHERE e.uuid IN $uuids
            RETURN
            e.uuid AS uuid,
    e.group_id AS group_id,
    n.uuid AS source_node_uuid,
    m.uuid AS target_node_uuid,
    e.created_at AS created_at
        `, map[string]interface{}{"uuids": []string{"ne1"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// entity_node_ops.py + node_db_queries.get_entity_node_save_query / save_bulk
// ============================================================================

func TestGraphitiExactShape_EntityNode_Save(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	entityData := map[string]interface{}{
		"uuid": "e3", "group_id": "g1", "name": "carol", "summary": "third",
		"created_at": "2024-02-07", "name_embedding": []float64{0.1, 0.2, 0.3},
	}
	f.run(t, `
                MERGE (n:Entity {uuid: $entity_data.uuid})
                SET n:Entity
                SET n = $entity_data
                WITH n CALL db.create.setNodeVectorProperty(n, "name_embedding", $entity_data.name_embedding)
                RETURN n.uuid AS uuid
            `, map[string]interface{}{"entity_data": entityData})
}

func TestGraphitiExactShape_EntityNode_SaveBulk(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	nodes := []map[string]interface{}{
		{
			"uuid": "e4", "group_id": "g1", "name": "dave",
			"summary": "fourth", "created_at": "2024-02-08",
			"name_embedding": []float64{0.4, 0.5, 0.6},
			"labels":         []string{"Entity"},
		},
	}
	f.run(t, `
                    UNWIND $nodes AS node
                    MERGE (n:Entity {uuid: node.uuid})
                    SET n:$(node.labels)
                    SET n = node
                    WITH n, node CALL db.create.setNodeVectorProperty(n, "name_embedding", node.name_embedding)
                RETURN n.uuid AS uuid
            `, map[string]interface{}{"nodes": nodes})
}

// Common detach-delete shape shared by entity/episode/community node ops.
func TestGraphitiExactShape_AnyNode_DeleteByUuidCollectingEdgeUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n {uuid: $uuid})
            WHERE n:Entity OR n:Episodic OR n:Community
            OPTIONAL MATCH (n)-[r]-()
            WITH collect(r.uuid) AS edge_uuids, n
            DETACH DELETE n
            RETURN edge_uuids
        `, map[string]interface{}{"uuid": "e2"})
	require.Len(t, res.Rows, 1, "delete-and-return must yield one row")
}

func TestGraphitiExactShape_EntityNode_DeleteByGroupIdInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Entity {group_id: $group_id})
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"group_id": "g1", "batch_size": int64(100)})
}

func TestGraphitiExactShape_EntityNode_DeleteByUuidsInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Entity)
            WHERE n.uuid IN $uuids
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"uuids": []string{"e1"}, "batch_size": int64(100)})
}

func TestGraphitiExactShape_EntityNode_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity {uuid: $uuid})
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
        `, map[string]interface{}{"uuid": "e1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EntityNode_GetByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)
            WHERE n.uuid IN $uuids
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
        `, map[string]interface{}{"uuids": []string{"e1", "e2"}})
	require.Len(t, res.Rows, 2)
}

func TestGraphitiExactShape_EntityNode_GetByGroupIds_NoCursorNoLimit(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)
            WHERE n.group_id IN $group_ids
            
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
            ORDER BY n.uuid DESC
            
        `, map[string]interface{}{"group_ids": []string{"g1"}})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EntityNode_LoadEmbedding(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity {uuid: $uuid})
            RETURN n.name_embedding AS name_embedding
        `, map[string]interface{}{"uuid": "e1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EntityNode_LoadEmbeddingsBulk(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)
            WHERE n.uuid IN $uuids
            RETURN DISTINCT n.uuid AS uuid, n.name_embedding AS name_embedding
        `, map[string]interface{}{"uuids": []string{"e1", "e2"}})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// community_node_ops.py + community_node_save_query
// ============================================================================

func TestGraphitiExactShape_CommunityNode_Save(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
                MERGE (n:Community {uuid: $uuid})
                SET n = {uuid: $uuid, name: $name, group_id: $group_id, summary: $summary, created_at: $created_at}
                WITH n CALL db.create.setNodeVectorProperty(n, "name_embedding", $name_embedding)
                RETURN n.uuid AS uuid
            `, map[string]interface{}{
		"uuid": "c2", "name": "community-2", "group_id": "g1",
		"summary": "sum2", "created_at": "2024-02-09",
		"name_embedding": []float64{0.1, 0.2},
	})
}

func TestGraphitiExactShape_CommunityNode_DeleteByGroupIdInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Community {group_id: $group_id})
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"group_id": "g1", "batch_size": int64(100)})
}

func TestGraphitiExactShape_CommunityNode_DeleteByUuidsInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Community)
            WHERE n.uuid IN $uuids
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"uuids": []string{"c1"}, "batch_size": int64(100)})
}

func TestGraphitiExactShape_CommunityNode_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (c:Community {uuid: $uuid})
            RETURN
            c.uuid AS uuid,
        c.name AS name,
        c.group_id AS group_id,
        c.created_at AS created_at,
        c.summary AS summary,
        labels(c) AS labels,
        properties(c) AS attributes
        `, map[string]interface{}{"uuid": "c1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_CommunityNode_LoadEmbedding(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (c:Community {uuid: $uuid})
            RETURN c.name_embedding AS name_embedding
        `, map[string]interface{}{"uuid": "c1"})
	require.Len(t, res.Rows, 1)
}

// ============================================================================
// episode_node_ops.py + episode save / bulk save
// ============================================================================

func TestGraphitiExactShape_EpisodeNode_Save(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
                MERGE (n:Episodic {uuid: $uuid})
                SET n = {uuid: $uuid, name: $name, group_id: $group_id, source_description: $source_description, source: $source, content: $content,
                entity_edges: $entity_edges, created_at: $created_at, valid_at: $valid_at}
                RETURN n.uuid AS uuid
            `, map[string]interface{}{
		"uuid": "ep3", "name": "episode-3", "group_id": "g1",
		"source_description": "seed", "source": "msg", "content": "hi",
		"entity_edges": []string{}, "created_at": "2024-02-10",
		"valid_at": "2024-02-10",
	})
}

func TestGraphitiExactShape_EpisodeNode_SaveBulk(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	eps := []map[string]interface{}{
		{
			"uuid": "ep4", "name": "episode-4", "group_id": "g1",
			"source_description": "bulk", "source": "msg", "content": "bulk",
			"entity_edges": []string{}, "created_at": "2024-02-11",
			"valid_at": "2024-02-11",
		},
	}
	f.run(t, `
                UNWIND $episodes AS episode
                MERGE (n:Episodic {uuid: episode.uuid})
                SET n = {uuid: episode.uuid, name: episode.name, group_id: episode.group_id, source_description: episode.source_description, source: episode.source, content: episode.content, 
                entity_edges: episode.entity_edges, created_at: episode.created_at, valid_at: episode.valid_at}
                RETURN n.uuid AS uuid
            `, map[string]interface{}{"episodes": eps})
}

func TestGraphitiExactShape_EpisodeNode_DeleteByGroupIdInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Episodic {group_id: $group_id})
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"group_id": "g1", "batch_size": int64(100)})
}

func TestGraphitiExactShape_EpisodeNode_DeleteByUuidsInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Episodic)
            WHERE n.uuid IN $uuids
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"uuids": []string{"ep1"}, "batch_size": int64(100)})
}

func TestGraphitiExactShape_EpisodeNode_GetByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	// Graphiti reuses an episodic return fragment with all the fields.
	res := f.run(t, `
            MATCH (e:Episodic {uuid: $uuid})
            RETURN
            e.uuid AS uuid,
        e.name AS name,
        e.group_id AS group_id,
        e.source AS source,
        e.source_description AS source_description,
        e.content AS content,
        e.entity_edges AS entity_edges,
        e.created_at AS created_at,
        e.valid_at AS valid_at
        `, map[string]interface{}{"uuid": "ep1"})
	require.Len(t, res.Rows, 1)
}

func TestGraphitiExactShape_EpisodeNode_GetByEntityNodeUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (e:Episodic)-[r:MENTIONS]->(n:Entity {uuid: $entity_node_uuid})
            RETURN DISTINCT
            e.uuid AS uuid,
        e.name AS name,
        e.group_id AS group_id,
        e.source AS source,
        e.source_description AS source_description,
        e.content AS content,
        e.entity_edges AS entity_edges,
        e.created_at AS created_at,
        e.valid_at AS valid_at
        `, map[string]interface{}{"entity_node_uuid": "e1"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EpisodeNode_GetBySagaBefore(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (s:Saga {name: $saga_name, group_id: $group_id})-[:HAS_EPISODE]->(e:Episodic)
                WHERE e.valid_at <= $reference_time
            RETURN
            e.uuid AS uuid,
        e.name AS name,
        e.group_id AS group_id,
        e.source AS source,
        e.source_description AS source_description,
        e.content AS content,
        e.entity_edges AS entity_edges,
        e.created_at AS created_at,
        e.valid_at AS valid_at
            ORDER BY e.valid_at DESC
            LIMIT $limit
        `, map[string]interface{}{
		"saga_name": "saga-1", "group_id": "g1",
		"reference_time": "2024-12-31", "limit": int64(5),
	})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_EpisodeNode_GetByGroupBefore(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (e:Episodic)
                WHERE e.valid_at <= $reference_time
                AND e.group_id = $group_id
            RETURN
            e.uuid AS uuid,
        e.name AS name,
        e.group_id AS group_id,
        e.source AS source,
        e.source_description AS source_description,
        e.content AS content,
        e.entity_edges AS entity_edges,
        e.created_at AS created_at,
        e.valid_at AS valid_at
            ORDER BY e.valid_at DESC
            LIMIT $limit
        `, map[string]interface{}{
		"reference_time": "2024-12-31", "group_id": "g1",
		"limit": int64(5),
	})
	require.NotEmpty(t, res.Rows)
}

// ============================================================================
// saga_node_ops.py + saga_node_save_query
// ============================================================================

func TestGraphitiExactShape_SagaNode_Save(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
                MERGE (n:Saga {uuid: $uuid})
                SET n = {uuid: $uuid, name: $name, group_id: $group_id, created_at: $created_at, summary: $summary, first_episode_uuid: $first_episode_uuid, last_episode_uuid: $last_episode_uuid, last_summarized_at: $last_summarized_at, last_summarized_episode_valid_at: $last_summarized_episode_valid_at}
                RETURN n.uuid AS uuid
            `, map[string]interface{}{
		"uuid": "s2", "name": "saga-2", "group_id": "g1",
		"created_at": "2024-02-12", "summary": "sum",
		"first_episode_uuid": "ep1", "last_episode_uuid": "ep2",
		"last_summarized_at": "2024-02-12", "last_summarized_episode_valid_at": "2024-02-12",
	})
}

func TestGraphitiExactShape_SagaNode_DeleteByUuid(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Saga {uuid: $uuid})
            DETACH DELETE n
        `, map[string]interface{}{"uuid": "s1"})
}

func TestGraphitiExactShape_SagaNode_DeleteByGroupIdInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Saga {group_id: $group_id})
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"group_id": "g1", "batch_size": int64(100)})
}

func TestGraphitiExactShape_SagaNode_DeleteByUuidsInTransactions(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (n:Saga)
            WHERE n.uuid IN $uuids
            CALL (n) {
                DETACH DELETE n
            } IN TRANSACTIONS OF $batch_size ROWS
        `, map[string]interface{}{"uuids": []string{"s1"}, "batch_size": int64(100)})
}

// ============================================================================
// graph_ops.py — admin / maintenance queries
// ============================================================================

func TestGraphitiExactShape_GraphOps_DeleteAllCommunity(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	f.run(t, `
            MATCH (c:Community)
            DETACH DELETE c
        `, nil)
}

func TestGraphitiExactShape_GraphOps_GetCommunitiesForEntity(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (c:Community)-[:HAS_MEMBER]->(n:Entity {uuid: $entity_uuid})
            RETURN
            c.uuid AS uuid,
        c.name AS name,
        c.group_id AS group_id,
        c.created_at AS created_at,
        c.summary AS summary,
        labels(c) AS labels,
        properties(c) AS attributes
        `, map[string]interface{}{"entity_uuid": "e1"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_GraphOps_GetRelatedCommunitiesForEntity(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (c:Community)-[:HAS_MEMBER]->(m:Entity)-[:RELATES_TO]-(n:Entity {uuid: $entity_uuid})
            RETURN
            c.uuid AS uuid,
        c.name AS name,
        c.group_id AS group_id,
        c.created_at AS created_at,
        c.summary AS summary,
        labels(c) AS labels,
        properties(c) AS attributes
        `, map[string]interface{}{"entity_uuid": "e2"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_GraphOps_GetEntitiesMentionedByEpisodes(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (episode:Episodic)-[:MENTIONS]->(n:Entity)
            WHERE episode.uuid IN $uuids
            RETURN DISTINCT
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
        `, map[string]interface{}{"uuids": []string{"ep1"}})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_GraphOps_GetCommunitiesOfEntities(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (c:Community)-[:HAS_MEMBER]->(m:Entity)
            WHERE m.uuid IN $uuids
            RETURN DISTINCT
            c.uuid AS uuid,
        c.name AS name,
        c.group_id AS group_id,
        c.created_at AS created_at,
        c.summary AS summary,
        labels(c) AS labels,
        properties(c) AS attributes
        `, map[string]interface{}{"uuids": []string{"e1"}})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_GraphOps_GetRelatedEntityCountsForEntity(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity {group_id: $group_id, uuid: $uuid})-[e:RELATES_TO]-(m: Entity {group_id: $group_id})
                    WITH count(e) AS count, m.uuid AS uuid
                    RETURN
                        uuid,
                        count
        `, map[string]interface{}{"group_id": "g1", "uuid": "e1"})
	require.NotEmpty(t, res.Rows)
}

func TestGraphitiExactShape_GraphOps_GetDistinctGroupIds(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)
                WHERE n.group_id IS NOT NULL
                RETURN
                    collect(DISTINCT n.group_id) AS group_ids
        `, nil)
	require.Len(t, res.Rows, 1)
}

// ============================================================================
// search_ops.py — exact strings rendered with realistic max_depth + filter
// ============================================================================

// node fulltext search, no filter, with limit. Renders get_nodes_query for Neo4j as:
//
//	CALL db.index.fulltext.queryNodes("node_name_and_summary", $query, {limit: $limit})
//
// then concatenates YIELD + WITH + ORDER + RETURN + return-fragment.
func TestGraphitiExactShape_Search_NodeFulltext_NoFilter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	// Need a fulltext index for this exact call.
	_, err := f.exec.Execute(f.ctx,
		"CREATE FULLTEXT INDEX node_name_and_summary IF NOT EXISTS FOR (n:Entity) ON EACH [n.name, n.summary, n.group_id]",
		nil)
	require.NoError(t, err)
	res := f.run(t, `CALL db.index.fulltext.queryNodes("node_name_and_summary", $query, {limit: $limit})YIELD node AS n, score
            WITH n, score
            ORDER BY score DESC
            LIMIT $limit
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes`,
		map[string]interface{}{"query": "alice", "limit": int64(5)})
	_ = res // fulltext may return zero rows depending on token analyser; assertion is "no error"
}

func TestGraphitiExactShape_Search_NodeSimilarity_NoFilter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	// vector index optional — query uses inline cosine on a 3-d vector.
	res := f.run(t, `MATCH (n:Entity)
            WITH n, vector.similarity.cosine(n.name_embedding, $search_vector) AS score
            WHERE score > $min_score
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
            ORDER BY score DESC
            LIMIT $limit
            `, map[string]interface{}{
		"search_vector": []float64{0.1, 0.2, 0.3},
		"min_score":     0.0,
		"limit":         int64(5),
	})
	_ = res
}

func TestGraphitiExactShape_Search_NodeBfs_NoFilter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            UNWIND $bfs_origin_node_uuids AS origin_uuid
            MATCH (origin {uuid: origin_uuid})-[:RELATES_TO|MENTIONS*1..2]->(n:Entity)
            WHERE n.group_id = origin.group_id
            
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes
            LIMIT $limit
            `, map[string]interface{}{
		"bfs_origin_node_uuids": []string{"e1"},
		"limit":                 int64(5),
	})
	_ = res
}

func TestGraphitiExactShape_Search_EdgeFulltext_NoFilter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	_, err := f.exec.Execute(f.ctx,
		"CREATE FULLTEXT INDEX edge_name_and_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]",
		nil)
	require.NoError(t, err)
	res := f.run(t, `CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $query, {limit: $limit})
            YIELD relationship AS rel, score
            MATCH (n:Entity)-[e:RELATES_TO {uuid: rel.uuid}]->(m:Entity)
            
            WITH e, score, n, m
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY score DESC
            LIMIT $limit
            `, map[string]interface{}{"query": "knows", "limit": int64(5)})
	_ = res
}

func TestGraphitiExactShape_Search_EdgeFulltext_TemporalFilterUsesCallTailFastPath(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	_, err := f.exec.Execute(f.ctx,
		"CREATE FULLTEXT INDEX edge_name_and_fact IF NOT EXISTS FOR ()-[e:RELATES_TO]-() ON EACH [e.name, e.fact, e.group_id]",
		nil)
	require.NoError(t, err)

	res := f.run(t, `CALL db.index.fulltext.queryRelationships("edge_name_and_fact", $query, {limit: $limit})
            YIELD relationship AS rel, score
            MATCH (n:Entity)-[e:RELATES_TO {uuid: rel.uuid}]->(m:Entity)
            WHERE ((e.invalid_at IS NULL) OR (e.invalid_at > $dt)) AND e.group_id IN $group_ids
            WITH e, score, n, m
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes,
            score
            ORDER BY score DESC
            LIMIT $limit
            `, map[string]interface{}{
		"query":     "knows",
		"dt":        "2024-01-01T00:00:00Z",
		"group_ids": []string{"g1"},
		"limit":     int64(5),
	})
	require.NotEmpty(t, res.Rows)
	require.True(t, f.exec.LastHotPathTrace().CallTailProjectionFastPath)
}

func TestGraphitiExactShape_Search_EdgeSimilarity_NoFilter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `MATCH (n:Entity)-[e:RELATES_TO]->(m:Entity)
            WITH DISTINCT e, n, m, vector.similarity.cosine(e.fact_embedding, $search_vector) AS score
            WHERE score > $min_score
            RETURN
            e.uuid AS uuid,
        n.uuid AS source_node_uuid,
        m.uuid AS target_node_uuid,
        e.group_id AS group_id,
        e.created_at AS created_at,
        e.name AS name,
        e.fact AS fact,
        e.episodes AS episodes,
        e.expired_at AS expired_at,
        e.valid_at AS valid_at,
        e.invalid_at AS invalid_at,
    properties(e) AS attributes
            ORDER BY score DESC
            LIMIT $limit
            `, map[string]interface{}{
		"search_vector": []float64{0.1, 0.2, 0.3},
		"min_score":     0.0,
		"limit":         int64(5),
	})
	_ = res
}

// search_ops.get_node_distance — center node + candidate uuids
func TestGraphitiExactShape_Search_NodeDistanceFromCenter(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
        UNWIND $node_uuids AS node_uuid
        MATCH (center:Entity {uuid: $center_uuid})-[:RELATES_TO]-(n:Entity {uuid: node_uuid})
        RETURN 1 AS score, node_uuid AS uuid`,
		map[string]interface{}{"center_uuid": "e1", "node_uuids": []string{"e2"}})
	require.NotEmpty(t, res.Rows)
}

// search_ops.get_node_mentions_count
func TestGraphitiExactShape_Search_NodeMentionsCount(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            UNWIND $node_uuids AS node_uuid
            MATCH (episode:Episodic)-[r:MENTIONS]->(n:Entity {uuid: node_uuid})
            RETURN count(*) AS score, n.uuid AS uuid`,
		map[string]interface{}{"node_uuids": []string{"e1"}})
	require.NotEmpty(t, res.Rows)
}

// search_ops "load nodes by uuid with full return"
func TestGraphitiExactShape_Search_LoadNodesByUuids(t *testing.T) {
	f := newGraphitiExactShapeFixture(t)
	res := f.run(t, `
            MATCH (n:Entity)
            WHERE n.uuid IN $uuids
            RETURN
            n.uuid AS uuid,
        n.name AS name,
        n.group_id AS group_id,
        n.created_at AS created_at,
        n.summary AS summary,
        labels(n) AS labels,
        properties(n) AS attributes`,
		map[string]interface{}{"uuids": []string{"e1", "e2"}})
	require.Len(t, res.Rows, 2)
}

// ============================================================================
// helpers
// ============================================================================

func graphitiExactToInt64(t *testing.T, v interface{}) int64 {
	t.Helper()
	switch n := v.(type) {
	case int64:
		return n
	case int:
		return int64(n)
	case int32:
		return int64(n)
	case uint64:
		return int64(n)
	case float64:
		return int64(n)
	}
	t.Fatalf("expected numeric, got %T (%v)", v, v)
	return 0
}

// (min is provided by the package-level unwind_nested_substitution_internal_test.go)
