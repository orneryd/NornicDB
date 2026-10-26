// Author: Codex implementation lane, 2026-10-26 (NOR-148).
package cypher

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Frozen release oracle: the d663 population, document-frequency and matching
// sequence, retained only in tests to check the maintained public path.
func fulltextReleaseOracle(t *testing.T, store storage.Engine, text string) [][]any {
	t.Helper()
	parsed, err := ParseFulltextQuery(text)
	require.NoError(t, err)
	props := []string{"text", "title"}
	nodes, err := store.AllNodes()
	require.NoError(t, err)
	docs := make(map[string]*ftDoc)
	identities := make(map[string]any)
	ctx := &ftEvalCtx{DefaultFields: props, avgDocLen: 100, docFreq: make(map[string]int)}
	var length int
	for _, node := range nodes {
		if !matchesLabels(node, []string{"Source"}) {
			continue
		}
		doc := buildNodeFulltextDoc(node, props)
		if doc == nil {
			continue
		}
		docs[string(node.ID)] = doc
		identities[string(node.ID)] = node.Properties["id"]
		length += doc.contentTokenN
		for _, term := range parsed.PrimaryTerms() {
			if strings.Contains(doc.contentLower, term) {
				ctx.docFreq[term]++
			}
		}
	}
	ctx.totalDocs = len(docs)
	if len(docs) > 0 {
		ctx.avgDocLen = float64(length) / float64(len(docs))
	}
	rows := make([][]any, 0)
	for id, doc := range docs {
		matched, score := parsed.Match(ctx, doc)
		if !matched {
			continue
		}
		if score <= 0 {
			score = 1
		}
		rows = append(rows, []any{identities[id], score, id})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i][1] == rows[j][1] {
			return rows[i][2].(string) < rows[j][2].(string)
		}
		return rows[i][1].(float64) > rows[j][1].(float64)
	})
	for i := range rows {
		rows[i] = rows[i][:2]
	}
	return rows
}

func TestFulltextMaintainedPublicMatchesRelease(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 240)
	queries := []string{
		"hotel", "ote", "ot", "hotel services", "hotel AND services", "hotel OR missing", "hotel -routine", "NOT missing",
		`"hotel services"`, `title:Correspondence AND (hotel OR missing)`, `title:Correspondence OR hotel`,
		`(hotel OR missing) AND title:Correspondence`, `hotel^2`, `hotel~1`, `ho*`, `/hotel/`, `title:[A TO Z]`,
		`+hotel -missing`, `title:missing OR hotel`, `title:Correspondence AND -hotel`, `"pines la"`,
	}
	check := func() {
		t.Helper()
		for _, text := range queries {
			expected := fulltextReleaseOracle(t, store, text)
			got, err := exec.Execute(context.Background(), "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score", map[string]any{"query": text})
			require.NoError(t, err, text)
			require.Equal(t, expected, got.Rows, text)
		}
	}
	check()
	// Both raw storage writes and Cypher writes must reach the same owner.
	node, err := store.GetNode("n000040")
	require.NoError(t, err)
	node.Properties["text"] = "hotel hotel services"
	require.NoError(t, store.UpdateNode(node))
	check()
	_, err = exec.Execute(context.Background(), "CREATE (:Source {id:'added', text:'hotel services hotel', title:'Added'})", nil)
	require.NoError(t, err)
	check()
	require.NoError(t, store.DeleteNode("n000000"))
	check()
	_, err = exec.Execute(context.Background(), "MATCH (n:Source {id:'added'}) SET n:Unrelated REMOVE n:Source", nil)
	require.NoError(t, err)
	check()
}

func TestFulltextMaintainedPublicRestartRestore(t *testing.T) {
	dir := t.TempDir()
	base, err := storage.NewBadgerEngine(filepath.Join(dir, "store"))
	require.NoError(t, err)
	defer func() { require.NoError(t, base.Close()) }()
	store := storage.NewNamespacedEngine(base, "fulltext_lifecycle")
	exec := NewStorageExecutor(store)
	ctx := context.Background()
	for _, query := range []string{
		"CREATE FULLTEXT INDEX source_text FOR (n:Source) ON EACH [n.text, n.title]",
		"CREATE (:Source {id:'one', text:'hotel services hotel'}), (:Source {id:'two', text:'routine services'})",
	} {
		_, err = exec.Execute(ctx, query, nil)
		require.NoError(t, err)
	}
	query := "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN node.id, score"
	before, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, before.Rows, 1)
	backup := filepath.Join(dir, "backup.bak")
	require.NoError(t, base.Backup(backup))
	_, err = exec.Execute(ctx, "MATCH (n:Source {id:'one'}) SET n.text='routine'", nil)
	require.NoError(t, err)
	mutated, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Empty(t, mutated.Rows)
	require.NoError(t, base.Restore(backup))
	restored, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, before.Rows, restored.Rows)
	require.NoError(t, base.Close())
	base, err = storage.NewBadgerEngine(filepath.Join(dir, "store"))
	require.NoError(t, err)
	exec = NewStorageExecutor(storage.NewNamespacedEngine(base, "fulltext_lifecycle"))
	restarted, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, before.Rows, restarted.Rows)
	t.Logf("restart/restore retained %s", fmt.Sprint(before.Rows))
}

func TestFulltextMaintainedPublicTransactionAndScopes(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 160)
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score"
	params := map[string]any{"query": "transactionword"}
	_, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	for _, statement := range []string{"BEGIN", "CREATE (:Source {id:'pending', text:'transactionword'})"} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	inside, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, inside.Rows, 1)
	outside := NewStorageExecutor(store)
	outside.SetSearchService(exec.search())
	uncommitted, err := outside.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Empty(t, uncommitted.Rows)
	_, err = exec.Execute(ctx, "ROLLBACK", nil)
	require.NoError(t, err)
	rolledBack, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Empty(t, rolledBack.Rows)
	for _, statement := range []string{"BEGIN", "CREATE (:Source {id:'committed', text:'transactionword'})", "COMMIT"} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	committed, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, committed.Rows, 1)
	require.Equal(t, "committed", committed.Rows[0][0])
	for _, statement := range []string{"DROP INDEX source_text", "CREATE FULLTEXT INDEX source_text FOR (n:Unrelated) ON EACH [n.text]"} {
		_, err = exec.Execute(ctx, statement, nil)
		require.NoError(t, err, statement)
	}
	redefined, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Empty(t, redefined.Rows, "recreating the same index name must replace its field/label definition")
}

func TestFulltextMaintainedPublicRelationshipWrites(t *testing.T) {
	exec, _ := fulltextMaintainedFixture(t, 8)
	ctx := context.Background()
	for _, statement := range []string{
		"CREATE FULLTEXT INDEX relation_text FOR ()-[r:LINK]-() ON EACH [r.text]",
		"MATCH (a {id:'n000000'}), (b {id:'n000004'}) CREATE (a)-[:LINK {id:'link', text:'hotel services'}]->(b)",
	} {
		_, err := exec.Execute(ctx, statement, nil)
		require.NoError(t, err)
	}
	query := "CALL db.index.fulltext.queryRelationships('relation_text', 'hotel') YIELD relationship, score RETURN relationship.id, score"
	first, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, first.Rows, 1)
	require.Equal(t, "link", first.Rows[0][0])
	_, err = exec.Execute(ctx, "MATCH ()-[r:LINK {id:'link'}]->() SET r.text='routine'", nil)
	require.NoError(t, err)
	updated, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Empty(t, updated.Rows)
	_, err = exec.Execute(ctx, "MATCH ()-[r:LINK {id:'link'}]->() SET r.text='hotel services'", nil)
	require.NoError(t, err)
	back, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, first.Rows, back.Rows)
	_, err = exec.Execute(ctx, "MATCH ()-[r:LINK {id:'link'}]->() DELETE r", nil)
	require.NoError(t, err)
	deleted, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Empty(t, deleted.Rows)
}

func TestFulltextMaintainedPublicDelayedNotifications(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 80)
	ctx := context.Background()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	publisher := store.GetInnerEngine().(*storage.BadgerEngine)
	publisher.OnNodeUpdated(func(node *storage.Node) {
		if strings.HasSuffix(string(node.ID), ":n000004") {
			close(entered)
			<-release
		}
	})
	query := "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score"
	_, err := exec.Execute(ctx, query, map[string]any{"query": "hotel"})
	require.NoError(t, err)
	barrier, err := store.GetNode("n000004")
	require.NoError(t, err)
	barrier.Properties["text"] = "notification barrier"
	finished := make(chan error, 1)
	go func() { finished <- store.UpdateNode(barrier) }()
	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("storage notification did not enter its barrier")
	}
	changed, err := store.GetNode("n000040")
	require.NoError(t, err)
	changed.Properties["text"] = "visiblebeforequeuednotification"
	require.NoError(t, store.UpdateNode(changed))
	params := map[string]any{"query": "visiblebeforequeuednotification"}
	during, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Len(t, during.Rows, 1, "a stored write must be searchable before its deferred notification arrives")
	require.Equal(t, "n000040", during.Rows[0][0])
	unblock()
	require.NoError(t, <-finished)
	after, err := exec.Execute(ctx, query, params)
	require.NoError(t, err)
	require.Equal(t, during.Rows, after.Rows, "delayed pre-baseline notifications must not restore older documents")
}

func TestFulltextMaintainedPublicSuppressionAndReveal(t *testing.T) {
	exec, _ := fulltextMaintainedFixture(t, 80)
	ctx := context.Background()
	query := "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN node.id, score"
	before, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, before.Rows, 2)
	_, err = exec.Execute(ctx, "MATCH (n:Source {id:'n000000'}) CALL kp.suppress(n)", nil)
	require.NoError(t, err)
	hidden, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Len(t, hidden.Rows, 1)
	revealed, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN reveal(node).id, score", nil)
	require.NoError(t, err)
	require.Equal(t, before.Rows, revealed.Rows)
	again, err := exec.Execute(ctx, query, nil)
	require.NoError(t, err)
	require.Equal(t, hidden.Rows, again.Rows)
}

func TestFulltextMaintainedPublicWildcardPresenceOrder(t *testing.T) {
	exec, store := fulltextMaintainedFixture(t, 80)
	ctx := context.Background()
	_, err := exec.Execute(ctx, "CREATE (:Source {id:'space', text:'   '}), (:Source {id:'empty', text:''})", nil)
	require.NoError(t, err)
	for phase := 0; phase < 2; phase++ {
		if phase == 1 {
			node, err := store.GetNode("n000040")
			require.NoError(t, err)
			node.Properties["text"] = ""
			node.Properties["title"] = ""
			require.NoError(t, store.UpdateNode(node))
		}
		for _, term := range []string{"*", "*:*", "text:*", "title:*"} {
			nodes, err := store.AllNodes()
			require.NoError(t, err)
			want := make([][]any, 0)
			for _, node := range nodes {
				if !matchesLabels(node, []string{"Source"}) {
					continue
				}
				match := extractTextContent(node, []string{"text", "title"}) != ""
				if property, presence := fulltextFieldPresenceQuery(term); presence {
					match = nodeHasNonEmptyProperty(node, property)
				}
				if match {
					want = append(want, []any{node.Properties["id"], float64(1)})
				}
			}
			for repeat := 0; repeat < 2; repeat++ {
				got, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('source_text', $query) YIELD node, score RETURN node.id, score", map[string]any{"query": term})
				require.NoError(t, err)
				require.Equal(t, want, got.Rows, "phase %d query %s", phase, term)
			}
		}
		// Presence-only rows must not change the ranked corpus's IDF/length.
		want := fulltextReleaseOracle(t, store, "hotel")
		got, err := exec.Execute(ctx, "CALL db.index.fulltext.queryNodes('source_text', 'hotel') YIELD node, score RETURN node.id, score", nil)
		require.NoError(t, err)
		require.Equal(t, want, got.Rows)
	}
}
