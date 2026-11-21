package cypher

// v1.5.2 close-out lane L4: statement-cost register rows. Each check builds the
// same graph with 200 and with 20,000 unrelated entities and compares the
// statement's PROFILE storage reads (time when reads are not observed). The
// statement's own answer is the same in both stores.

import (
	"fmt"
	"testing"
)

// H-416: a labelled pattern with an unindexed inline property reads only its
// label, never the nodes of other labels.
func TestCloseoutL4H416LabelledUnindexedInlineProperty(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-416", "labelled-unindexed-inline", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Batch(t, exec, 100, "UNWIND range($start, $end - 1) AS k CREATE (:L416 {p: k})")
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:Other416 {p: k})")
		}, "MATCH (n:L416 {p: $v}) RETURN n.p", map[string]any{"v": int64(50)}, 1)
	})
}

// H-413 (1): a keyset page over an indexed id with an extra or a negated
// label reads a window of the index, not the label.
func TestCloseoutL4H413KeysetPageExtraLabel(t *testing.T) {
	build := func(t *testing.T, exec *StorageExecutor, population int) {
		l4Run(t, exec, "CREATE INDEX l4_g413_id FOR (n:G413) ON (n.id)", nil)
		l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:G413:E413 {id: k})")
	}
	params := map[string]any{"after": int64(10)}
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-413", "keyset-negated-label", build, "MATCH (n:G413) WHERE n.id > $after AND NOT n:X413 RETURN n.id ORDER BY n.id LIMIT 20", params, 20)
		l4Scaling(t, "H-413", "keyset-extra-label", build, "MATCH (n:G413:E413) WHERE n.id > $after RETURN n.id ORDER BY n.id LIMIT 20", params, 20)
	})
	// PROFILE reports the executed statement: its rows and observed reads.
	exec := NewStorageExecutor(l4MemStore(t, "l4"))
	build(t, exec, 500)
	result, err := exec.Execute(t.Context(), "PROFILE MATCH (n:G413) WHERE n.id > $after AND NOT n:X413 RETURN n.id ORDER BY n.id LIMIT 20", params)
	if err != nil {
		l4Report(t, "H-413", "profile-observed", false, map[string]any{"error": err.Error()})
		return
	}
	plan, _ := result.Metadata["plan"].(*ExecutionPlan)
	ok := plan != nil && plan.Root != nil && plan.Root.RowsObserved && plan.Root.ActualRows == int64(len(result.Rows)) && plan.TotalDBHitsObserved
	detail := map[string]any{"rows": len(result.Rows), "planString": result.Metadata["planString"]}
	if plan != nil && plan.Root != nil {
		detail["rootRows"] = plan.Root.ActualRows
		detail["totalDbHits"] = plan.TotalDBHits
		detail["dbHitsObserved"] = plan.TotalDBHitsObserved
	}
	l4Report(t, "H-413", "profile-observed", ok, detail)
}

// H-413 (2): MATCH (member:PDIdentity {survivor: g.id}) after an id seek
// seeks the survivor index.
func TestCloseoutL4H413SurvivorSeek(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-413", "survivor-seek", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_party_survivor FOR (n:PDIdentity) ON (n.survivor)", nil)
			l4Run(t, exec, "CREATE INDEX l4_graph_id FOR (n:PDGraphNode) ON (n.id)", nil)
			l4Run(t, exec, "CREATE (:PDGraphNode:PDIdentity {id: 'c', survivor: 'c'})", nil)
			l4Run(t, exec, "UNWIND range(1, 5) AS k CREATE (:PDGraphNode:PDIdentity {id: 'm' + toString(k), survivor: 'c'})", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:PDGraphNode:PDIdentity {id: 'd' + toString(k), survivor: 'd' + toString(k)})")
		}, "MATCH (g:PDGraphNode {id: $c}) MATCH (member:PDIdentity {survivor: g.id}) RETURN count(member) AS members", map[string]any{"c": "c"}, 1)
	})
}

// H-413 (3): a context read whose second MATCH ends at the bound context and
// filters its start side starts from the bound end.
func TestCloseoutL4H413BoundEndWithStartFilter(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-413", "bound-end-start-filter", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_context_id FOR (n:PDContext) ON (n.id)", nil)
			l4Run(t, exec, "CREATE (c:PDGraphNode:PDContext {id: 'ctx'}) WITH c UNWIND range(1, 5) AS k CREATE (:Src413 {kind: 'a'})-[:IN413]->(c)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:Src413 {kind: 'a'})-[:IN413]->(:PDGraphNode:PDContext {id: 'o' + toString(k)})")
		}, "UNWIND $ids AS context_id MATCH (c:PDGraphNode:PDContext {id: context_id}) MATCH (s:Src413)-[:IN413]->(c) WHERE s.kind = 'a' RETURN count(s) AS sources", map[string]any{"ids": []any{"ctx"}}, 1)
	})
}

// H-417 (2): a required MATCH from a variable an OPTIONAL MATCH left null
// produces no rows without walking the relationships.
func TestCloseoutL4H417RequiredMatchFromNullBound(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-417", "required-match-null-bound", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_a417_id FOR (n:A417) ON (n.id)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:A417 {id: 'a' + toString(k)})-[:R417]->(:B417)")
		}, "OPTIONAL MATCH (a:A417 {id: 'missing'}) MATCH (a)-[:R417]->(b) RETURN count(b) AS n", nil, 1)
	})
}

// H-418 (1): a second MATCH ending at a node bound by the first starts from
// that node, not from every PDDocument.
func TestCloseoutL4H418SecondMatchFromBoundEnd(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-418", "second-match-bound-end", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_held_thing FOR (n:Held418) ON (n.thing_id)", nil)
			l4Run(t, exec, "CREATE (h:Held418 {thing_id: 't'}) WITH h UNWIND range(1, 3) AS k CREATE (:Doc418)-[:HAS418]->(h)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:Doc418)-[:HAS418]->(:Held418 {thing_id: 'h' + toString(k)})")
		}, "MATCH (h:Held418 {thing_id: $id}) MATCH (holder:Doc418)-[:HAS418]->(h) RETURN count(holder) AS holders", map[string]any{"id": "t"}, 1)
	})
}

// H-418 (2): a relationship pattern with a uniquely constrained relationship
// property is seeded from that constraint's index.
func TestCloseoutL4H418RelationshipUniqueSeed(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-418", "relationship-unique-seed", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE CONSTRAINT l4_rel418_id FOR ()-[r:PD_RELATION]-() REQUIRE r.id IS UNIQUE", nil)
			l4Run(t, exec, "CREATE (:N418 {id: 'from'})-[:PD_RELATION {id: 'link'}]->(:N418 {id: 'to'})", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:N418 {id: 'n' + toString(k)})-[:PD_RELATION {id: 'r' + toString(k)}]->(:N418 {id: 'm' + toString(k)})")
		}, "MATCH (a)-[r:PD_RELATION {id: $link}]->(b) WHERE a.id = $from RETURN r.id, b.id", map[string]any{"link": "link", "from": "from"}, 1)
	})
}

// H-418 (3): an IN list over an indexed property that hits nothing, or is
// empty, reads nothing; it does not fall back to the whole label.
func TestCloseoutL4H418InListZeroHitAndEmpty(t *testing.T) {
	build := func(t *testing.T, exec *StorageExecutor, population int) {
		l4Run(t, exec, "CREATE INDEX l4_src418_doc FOR (n:Src418) ON (n.document_id)", nil)
		l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:Src418 {document_id: 'd' + toString(k)})-[:R418]->(:T418)")
	}
	query := "MATCH (source:Src418)-[:R418]->(n) WHERE source.document_id IN $documents RETURN count(n) AS n"
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-418", "in-zero-hit", build, query, map[string]any{"documents": []any{"missing"}}, 1)
		l4Scaling(t, "H-418", "in-empty-list", build, query, map[string]any{"documents": []any{}}, 1)
		// Answers: zero for a missing key and for an empty list, one for a hit.
		exec := NewStorageExecutor(l4MemStore(t, "l4"))
		build(t, exec, 50)
		answers := map[string]any{}
		ok := true
		for name, documents := range map[string][]any{"missing": {"missing"}, "empty": {}, "hit": {"d7"}} {
			rows := l4Run(t, exec, query, map[string]any{"documents": documents})
			answers[name] = rows
			want := int64(0)
			if name == "hit" {
				want = 1
			}
			ok = ok && len(rows) == 1 && fmt.Sprint(rows[0][0]) == fmt.Sprint(want)
		}
		l4Report(t, "H-418", "in-list-answers", ok, map[string]any{"answers": answers})
	})
}

// H-419: an ORDER BY ... LIMIT keyset page over an indexed subject reads a
// window, not the whole collection.
func TestCloseoutL4H419KeysetPageWindow(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-419", "keyset-subject-window", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "CREATE INDEX l4_outcome_subject FOR (n:Outcome419) ON (n.subject)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:Outcome419 {collection: 'c', subject: 's' + right('000000' + toString(k), 6)})")
		}, "MATCH (o:Outcome419) WHERE o.subject > $after AND o.collection = $c RETURN o.subject ORDER BY o.subject LIMIT 10", map[string]any{"after": "s000005", "c": "c"}, 10)
	})
}

// H-397 (1): an aggregation over one relationship type with an unindexed
// relationship-property filter reads that type, not relationships of other
// types.
func TestCloseoutL4H397RelationshipTypeAggregation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		l4Scaling(t, "H-397", "relationship-type-aggregation", func(t *testing.T, exec *StorageExecutor, population int) {
			l4Run(t, exec, "UNWIND range(0, 49) AS k CREATE (a:N397 {id: k}) WITH a UNWIND range(1, 4) AS j CREATE (a)-[:PD_RELATION {kind: CASE WHEN j % 2 = 0 THEN 'agent' ELSE 'user' END}]->(:M397)", nil)
			l4Batch(t, exec, population, "UNWIND range($start, $end - 1) AS k CREATE (:X397)-[:OTHER397 {kind: 'agent'}]->(:Y397)")
		}, "MATCH (a)-[r:PD_RELATION]->(b) WHERE r.kind IN ['agent', 'user'] RETURN a.id, count(r)", nil, 50)
	})
}
