package nornicdb

// Report gap C37, embedded DB route (test-side only): change the node that
// DB.GetNode returns (title and the first element of a list property), then
// read the node with a later Cypher statement and with DB.GetNode again.
// Property decay off and on. Diagnostic: logs GAPC37DB lines.

import (
	"context"
	"fmt"
	"testing"
)

func TestGapC37DBGetNode(t *testing.T) {
	for _, decay := range []bool{false, true} {
		t.Run(fmt.Sprintf("decay=%v", decay), func(t *testing.T) {
			cfg := DefaultConfig()
			cfg.Memory.DecayEnabled = decay
			cfg.Memory.AutoLinksEnabled = false
			cfg.Memory.EmbeddingEnabled = false
			cfg.Database.AsyncWritesEnabled = false
			db, err := Open(t.TempDir(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			ctx := context.Background()
			n, err := db.CreateNode(ctx, []string{"Doc"}, map[string]interface{}{"title": "a", "tags": []interface{}{"original"}})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.ExecuteCypher(ctx, "MATCH (d:Doc) RETURN d.title", nil); err != nil {
				t.Fatal(err)
			}
			got, err := db.GetNode(ctx, n.ID)
			if err != nil {
				t.Fatal(err)
			}
			got.Properties["title"] = "mutated-title"
			listMutated := false
			if l, ok := got.Properties["tags"].([]interface{}); ok && len(l) > 0 {
				l[0] = "mutated-list"
				listMutated = true
			}
			res, err := db.ExecuteCypher(ctx, "MATCH (d:Doc) RETURN d.title AS title, d.tags AS tags", nil)
			if err != nil {
				t.Fatal(err)
			}
			again, err := db.GetNode(ctx, n.ID)
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("GAPC37DB decay=%v tags_type=%T list_mutated=%t | later Cypher rows=%v | later DB.GetNode title=%v tags=%v",
				decay, got.Properties["tags"], listMutated, res.Rows, again.Properties["title"], again.Properties["tags"])
		})
	}
}
