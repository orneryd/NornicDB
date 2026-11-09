package fabric

import (
	"testing"
)

func TestPlanCache_NewDefaultsMaxSize(t *testing.T) {
	pc := NewPlanCache(0)
	if pc.maxSize != 500 {
		t.Fatalf("expected default maxSize 500, got %d", pc.maxSize)
	}
	pc2 := NewPlanCache(-1)
	if pc2.maxSize != 500 {
		t.Fatalf("expected default maxSize 500 for -1, got %d", pc2.maxSize)
	}
}

func TestPlanCache_PutAndGet(t *testing.T) {
	pc := NewPlanCache(10)
	frag := &FragmentExec{Query: "MATCH (n) RETURN n", Columns: []string{"n"}}

	pc.Put("MATCH (n) RETURN n", "neo4j", frag)

	got, ok := pc.Get("MATCH (n) RETURN n", "neo4j")
	if !ok {
		t.Fatal("expected cache hit")
	}
	exec, _ := got.(*FragmentExec)
	if exec.Query != "MATCH (n) RETURN n" {
		t.Fatalf("expected query 'MATCH (n) RETURN n', got %q", exec.Query)
	}
}

func TestPlanCache_Miss(t *testing.T) {
	pc := NewPlanCache(10)

	_, ok := pc.Get("MATCH (n) RETURN n", "neo4j")
	if ok {
		t.Fatal("expected cache miss")
	}

	_, misses, _ := pc.Stats()
	if misses != 1 {
		t.Fatalf("expected 1 miss, got %d", misses)
	}
}

func TestPlanCache_NilFragmentIgnored(t *testing.T) {
	pc := NewPlanCache(10)
	pc.Put("query", "db", nil)

	_, _, size := pc.Stats()
	if size != 0 {
		t.Fatalf("expected size 0 after nil put, got %d", size)
	}
}

func TestPlanCache_DuplicatePutIgnored(t *testing.T) {
	pc := NewPlanCache(10)
	frag1 := &FragmentExec{Query: "q1", Columns: []string{"a"}}
	frag2 := &FragmentExec{Query: "q2", Columns: []string{"b"}}

	pc.Put("query", "db", frag1)
	pc.Put("query", "db", frag2)

	got, ok := pc.Get("query", "db")
	if !ok {
		t.Fatal("expected cache hit")
	}
	exec := got.(*FragmentExec)
	if exec.Query != "q1" {
		t.Fatalf("expected original fragment q1, got %q", exec.Query)
	}
}

func TestPlanCache_LRUEviction(t *testing.T) {
	pc := NewPlanCache(2)

	pc.Put("q1", "db", &FragmentExec{Query: "q1", Columns: []string{"a"}})
	pc.Put("q2", "db", &FragmentExec{Query: "q2", Columns: []string{"b"}})
	// Access q1 to make it most-recently-used
	pc.Get("q1", "db")
	// Adding q3 should evict q2 (least recently used)
	pc.Put("q3", "db", &FragmentExec{Query: "q3", Columns: []string{"c"}})

	if _, ok := pc.Get("q2", "db"); ok {
		t.Fatal("expected q2 to be evicted")
	}
	if _, ok := pc.Get("q1", "db"); !ok {
		t.Fatal("expected q1 to still be cached")
	}
	if _, ok := pc.Get("q3", "db"); !ok {
		t.Fatal("expected q3 to be cached")
	}
}

func TestPlanCache_Stats(t *testing.T) {
	pc := NewPlanCache(10)
	frag := &FragmentExec{Query: "q", Columns: []string{"a"}}
	pc.Put("q", "db", frag)

	pc.Get("q", "db")       // hit
	pc.Get("q", "db")       // hit
	pc.Get("missing", "db") // miss

	hits, misses, size := pc.Stats()
	if hits != 2 {
		t.Fatalf("expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Fatalf("expected 1 miss, got %d", misses)
	}
	if size != 1 {
		t.Fatalf("expected size 1, got %d", size)
	}
}

func TestPlanCache_Clear(t *testing.T) {
	pc := NewPlanCache(10)
	pc.Put("q1", "db", &FragmentExec{Query: "q1", Columns: []string{"a"}})
	pc.Put("q2", "db", &FragmentExec{Query: "q2", Columns: []string{"b"}})

	pc.Clear()

	_, _, size := pc.Stats()
	if size != 0 {
		t.Fatalf("expected size 0 after clear, got %d", size)
	}
	if _, ok := pc.Get("q1", "db"); ok {
		t.Fatal("expected cache miss after clear")
	}
}

// Author: #1640 lane (Codex). Source-preserving plans may only share an entry
// for the exact accepted query and admitted database identity.
func TestPlanCache_ExactIdentity(t *testing.T) {
	for _, sample := range []struct{ name, query, database string }{
		{"literal whitespace", "RETURN 'a b' AS out", "db"},
		{"clause whitespace", "RETURN  'a  b' AS out", "db"},
		{"comment whitespace", "RETURN /* a b */ 'a  b' AS out", "db"},
		{"query case", "return 'a  b' AS out", "db"},
		{"database whitespace", "RETURN 'a  b' AS out", "d b"},
		{"database case", "RETURN 'a  b' AS out", "DB"},
	} {
		t.Run(sample.name, func(t *testing.T) {
			pc := NewPlanCache(10)
			originalQuery, originalDB := "RETURN 'a  b' AS out", "db"
			if sample.name == "database whitespace" {
				originalDB = "d  b"
			}
			if sample.name == "comment whitespace" {
				originalQuery = "RETURN /* a  b */ 'a  b' AS out"
			}
			original := &FragmentExec{Query: originalQuery, Columns: []string{"out"}}
			pc.Put(originalQuery, originalDB, original)
			if _, ok := pc.Get(sample.query, sample.database); ok {
				t.Fatal("different accepted identity reused a plan")
			}
			variant := &FragmentExec{Query: sample.query, Columns: []string{"out"}}
			pc.Put(sample.query, sample.database, variant)
			if got, ok := pc.Get(originalQuery, originalDB); !ok || got != original {
				t.Fatal("original plan changed")
			}
			if got, ok := pc.Get(sample.query, sample.database); !ok || got != variant {
				t.Fatal("variant lost its own source plan")
			}
			hits, misses, size := pc.Stats()
			if hits != 2 || misses != 1 || size != 2 {
				t.Fatalf("unexpected hit/miss/size: %d/%d/%d", hits, misses, size)
			}
		})
	}
	// A delimited string is not an injective encoding of two arbitrary identities.
	pc := NewPlanCache(10)
	pc.Put("c", "a|b", &FragmentExec{Query: "c"})
	if _, ok := pc.Get("b|c", "a"); ok {
		t.Fatal("database/query separator collision")
	}
}

func BenchmarkPlanCacheExactHit(b *testing.B) {
	pc := NewPlanCache(10)
	query := "USE comp CALL { USE comp.part RETURN 'a  b' AS out } RETURN out"
	pc.Put(query, "comp", &FragmentExec{Query: query})
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, ok := pc.Get(query, "comp"); !ok {
			b.Fatal("miss")
		}
	}
}

func TestPlanCache_DifferentSessionDBsAreSeparate(t *testing.T) {
	pc := NewPlanCache(10)
	frag1 := &FragmentExec{Query: "q", Columns: []string{"a"}}
	frag2 := &FragmentExec{Query: "q", Columns: []string{"b"}}

	pc.Put("MATCH (n) RETURN n", "db1", frag1)
	pc.Put("MATCH (n) RETURN n", "db2", frag2)

	got1, ok := pc.Get("MATCH (n) RETURN n", "db1")
	if !ok {
		t.Fatal("expected hit for db1")
	}
	got2, ok := pc.Get("MATCH (n) RETURN n", "db2")
	if !ok {
		t.Fatal("expected hit for db2")
	}
	if got1.(*FragmentExec).Columns[0] != "a" || got2.(*FragmentExec).Columns[0] != "b" {
		t.Fatal("expected different fragments for different session DBs")
	}
}
