package cypher

// v1.5.2 close-out lane L4, PD-22: the slowest interactive read (the
// application's SAME_PARTY statement, text in pdverify_v1_queries.json) at
// library scale: 5,280 documents, 172,000 nodes, 290,000 relationships, the
// population of pdvRelated (kept copy of the earlier verification). The
// reported cost was 921 s on b6d8e762a with the private corpus; an
// interactive read is expected to answer in seconds. Threshold: 30 s.

import (
	"context"
	"testing"
	"time"
)

func TestCloseoutL4PD22SameParty(t *testing.T) {
	_, s, e := pdvStore(t)
	for i := 0; i < 172000; i++ {
		id := "n" + itoaL4(i)
		labels := []string{"PDGraphNode"}
		p := map[string]any{"id": id}
		if i < 5280 {
			labels = append(labels, "PDDocument")
		}
		if i >= 5280 && i < 5580 {
			labels = append(labels, "PDIdentity")
			p["status"] = "confirmed"
			p["survivor"] = id
			p["name_key"] = "party" + itoaL4((i-5280)/2)
			p["kind"] = "person"
			p["collection_id"] = "collection"
		}
		if i >= 5580 && i < 5880 {
			labels = []string{"PDHeld"}
			p["thing_id"] = "n" + itoaL4(5280+i-5580)
			p["holders"] = int64(18)
		}
		if i == 5880 {
			labels = []string{"PDFreshness"}
			p["id"] = "holdings"
			p["documents"] = int64(5280)
		}
		pdvNode(t, s, id, labels, p)
	}
	for i := 0; i < 290000; i++ {
		a := "n" + itoaL4(i%5280)
		b := "n" + itoaL4(5881+i%(172000-5881))
		kind := "PD_RELATION"
		props := map[string]any{"id": "e" + itoaL4(i)}
		if i < 5280 {
			kind = "PD_HAS"
			b = "n" + itoaL4(5580+i%300)
			props = map[string]any{"identity": true}
		}
		pdvEdge(t, s, "e"+itoaL4(i), a, b, kind, props)
	}
	for _, q := range []string{
		"CREATE CONSTRAINT pd_document_id IF NOT EXISTS FOR (d:PDDocument) REQUIRE d.id IS UNIQUE",
		"CREATE CONSTRAINT pd_held_thing IF NOT EXISTS FOR (h:PDHeld) REQUIRE h.thing_id IS UNIQUE",
		"CREATE CONSTRAINT pd_freshness_id IF NOT EXISTS FOR (f:PDFreshness) REQUIRE f.id IS UNIQUE",
		"CREATE INDEX pd_party_survivor FOR (n:PDIdentity) ON (n.survivor)",
		"CREATE INDEX pd_party_anchor FOR (n:PDIdentityAnchor) ON (n.party)",
		"CREATE INDEX pd_graph_node_id FOR (n:PDGraphNode) ON (n.id)",
	} {
		pdvExec(t, e, q, nil)
	}
	query := pdvQueries(t)["related.SAME_PARTY"]
	params := map[string]any{"limit": int64(20), "per_item": int64(10), "start_id": "n0", "collection_ids": nil}
	for _, explicit := range []bool{false, true} {
		if explicit {
			pdvExec(t, e, "BEGIN", nil)
		}
		start := time.Now()
		result, err := e.Execute(context.Background(), query, params)
		elapsed := time.Since(start)
		if explicit {
			pdvExec(t, e, "ROLLBACK", nil)
		}
		rows := 0
		if result != nil {
			rows = len(result.Rows)
		}
		check := "same-party-implicit"
		if explicit {
			check = "same-party-explicit"
		}
		l4Report(t, "PD-22", check, err == nil && rows == 1 && elapsed < 30*time.Second, map[string]any{
			"seconds": elapsed.Seconds(), "rows": rows, "error": errString(err),
		})
	}
}

func itoaL4(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
