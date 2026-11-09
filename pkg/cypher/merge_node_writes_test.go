package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// TestMergeNodeWritesOf pins which MERGE-bound variables a statement writes
// (NOR-117): those writes make the MERGE take its key lock.
func TestMergeNodeWritesOf(t *testing.T) {
	merge := func(text string) pipelineClause { return pipelineClause{kind: pipelineClauseMerge, text: text} }
	clause := func(kind pipelineClauseKind, text string) pipelineClause {
		return pipelineClause{kind: kind, text: text}
	}
	for _, c := range []struct {
		name     string
		clauses  []pipelineClause
		written  []string
		unseen   []string
		writeAll bool
	}{
		{"read only", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseReturn, "RETURN n")}, nil, []string{"n"}, false},
		{"on match", []pipelineClause{merge("MERGE (n:Doc {id: 1}) ON MATCH SET n.x = 1")}, []string{"n"}, nil, false},
		{"own on create", []pipelineClause{merge("MERGE (n:Doc {id: 1}) ON CREATE SET n.x = 1, n:Seen")}, nil, []string{"n"}, false},
		{"other's on create", []pipelineClause{merge("MERGE (m:Doc {id: 2})"), merge("MERGE (n:Doc {id: 1}) ON CREATE SET m.x = 1")}, []string{"m"}, []string{"n"}, false},
		{"set label and map", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseSet, "SET n:Seen, m += $p")}, []string{"n", "m"}, nil, false},
		{"relationship set", []pipelineClause{merge("MERGE (n:Doc {id: 1})-[r:R]->(m:Doc {id: 2})"), clause(pipelineClauseSet, "SET r.w = 1")}, []string{"r"}, []string{"n", "m"}, false},
		{"remove", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseRemove, "REMOVE n.x, m:L")}, []string{"n", "m"}, nil, false},
		{"bad remove", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseRemove, "REMOVE n")}, nil, nil, true},
		{"unreadable set", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseSet, "SET n.x")}, nil, nil, true},
		{"delete", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseDelete, "DETACH DELETE n")}, []string{"n"}, []string{"m"}, false},
		{"delete expression", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseDelete, "DELETE head([n])")}, nil, nil, true},
		{"with, no rename", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseWith, "WITH n"), clause(pipelineClauseSet, "SET n.x = 1")}, []string{"n"}, []string{"m"}, false},
		{"with rename", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseWith, "WITH n AS m"), clause(pipelineClauseSet, "SET m.x = 1")}, nil, nil, true},
		{"let", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseLet, "LET m = n"), clause(pipelineClauseSet, "SET m.x = 1")}, nil, nil, true},
		{"foreach", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseForeach, "FOREACH (x IN [1] | SET n.x = x)")}, nil, nil, true},
		{"subquery", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseCallSubquery, "CALL { WITH n SET n.x = 1 }")}, nil, nil, true},
		{"read-only procedure", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseCall, "CALL db.labels() YIELD label")}, nil, []string{"n"}, false},
		{"unknown procedure", []pipelineClause{merge("MERGE (n:Doc {id: 1})"), clause(pipelineClauseCall, "CALL no.such.procedure()")}, nil, nil, true},
	} {
		t.Run(c.name, func(t *testing.T) {
			writes := mergeNodeWritesFromContext(withMergeNodeWrites(t.Context(), c.clauses))
			require.Equal(t, c.writeAll, writes.all)
			for _, variable := range c.written {
				require.True(t, writes.writes(variable), variable)
			}
			for _, variable := range c.unseen {
				require.False(t, writes.writes(variable), variable)
			}
			require.False(t, writes.writes(""))
		})
	}
	var none *mergeNodeWrites
	require.False(t, none.writes("n"))
	require.Nil(t, mergeNodeWritesFromContext(context.Background()))

	// A nested clause list (a subquery, a FOREACH body) adds its own writes
	// to the enclosing statement's.
	outer := withMergeNodeWrites(context.Background(), []pipelineClause{merge("MERGE (a:Doc {id: 1})"), clause(pipelineClauseSet, "SET a.x = 1")})
	inner := mergeNodeWritesFromContext(withMergeNodeWrites(outer, []pipelineClause{merge("MERGE (b:Doc {id: 2}) ON MATCH SET b.x = 1")}))
	require.True(t, inner.writes("a"))
	require.True(t, inner.writes("b"))
	require.False(t, inner.writes("c"))
	everything := withMergeNodeWrites(context.Background(), []pipelineClause{clause(pipelineClauseForeach, "FOREACH (x IN [1] | MERGE (c:Doc {id: x}))")})
	require.True(t, mergeNodeWritesFromContext(withMergeNodeWrites(everything, nil)).writes("c"))
}

// recordingMergePreparer is storage with the MERGE key capability that isn't
// the transaction wrapper (a composite participant's view, for instance).
type recordingMergePreparer struct {
	storage.Engine
	writes []bool
}

func (p *recordingMergePreparer) PrepareMergeNode(context.Context, []string, map[string]interface{}) error {
	p.writes = append(p.writes, false)
	return nil
}

func (p *recordingMergePreparer) PrepareMergeNodeForWrite(context.Context, []string, map[string]interface{}) error {
	p.writes = append(p.writes, true)
	return nil
}

// TestPrepareMergeKeysUsesTheStorageCapability: MERGE key admission goes to
// whatever storage offers storage.MergeNodePreparer, read or write; storage
// without it, or a view without a transaction, admits nothing.
func TestPrepareMergeKeysUsesTheStorageCapability(t *testing.T) {
	ctx := context.Background()
	labels, props := []string{"Doc"}, map[string]interface{}{"id": 1}
	preparer := &recordingMergePreparer{Engine: newTestMemoryEngine(t)}
	require.NoError(t, prepareMergeKeys(ctx, preparer, labels, props, false))
	require.NoError(t, prepareMergeKeys(ctx, preparer, labels, props, true))
	require.NoError(t, prepareMergeKeys(ctx, preparer, nil, props, true))
	require.Equal(t, []bool{false, true}, preparer.writes)
	require.NoError(t, prepareMergeKeys(ctx, newTestMemoryEngine(t), labels, props, true))
	empty := &transactionStorageWrapper{}
	require.NoError(t, empty.PrepareMergeNode(ctx, labels, props))
	require.NoError(t, empty.PrepareMergeNodeForWrite(ctx, labels, props))
}
