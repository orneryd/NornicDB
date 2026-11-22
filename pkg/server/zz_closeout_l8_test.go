package server

// Close-out lane L8 (v1.5.2 verification campaign, 2026-11-22): whole-server
// tests for hunt leads L-H5-001, L-H9-011 to -014, -017, -018 (procedures and
// Go API names) and the Neo4j reference runs for the L-H4 leads. Test-only,
// not for merge. Every statement or request logs one L8_* line.
//
// Run: go test ./pkg/server -run '^TestL8' -count=1 -timeout=30m -v
//      (with --ref neo-ref for TestL8Reference)

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

func l8Log(t *testing.T, kind string, fields map[string]any) {
	t.Helper()
	fields["parser"] = nornicConfig.GetParserType()
	data, _ := json.Marshal(fields)
	t.Logf("%s %s", kind, data)
}

func l8Server(t *testing.T) *huntServer {
	t.Helper()
	return huntOpen(t, filepath.Join(t.TempDir(), "store"), huntOptions{QueryCache: true})
}

func l8Q(t *testing.T, h *huntServer, lead, query string, params map[string]any) differential.Outcome {
	t.Helper()
	o := h.Run(routeBoltAuto, query, params, time.Minute)
	l8Log(t, "L8_STMT", map[string]any{"lead": lead, "query": query, "code": o.Code, "message": o.Message, "rows": fmt.Sprint(o.Rows)})
	return o
}

func l8HTTP(t *testing.T, h *huntServer, lead, method, path, body string) (int, string) {
	t.Helper()
	request, err := http.NewRequest(method, h.http.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	request.Header.Set("Authorization", h.token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		l8Log(t, "L8_HTTP", map[string]any{"lead": lead, "method": method, "path": path, "error": err.Error()})
		return -1, err.Error()
	}
	defer response.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(response.Body, 1<<16))
	text := string(data)
	short := text
	if len(short) > 600 {
		short = short[:600] + "..."
	}
	l8Log(t, "L8_HTTP", map[string]any{"lead": lead, "method": method, "path": path, "body": body, "status": response.StatusCode, "response": short})
	return response.StatusCode, text
}

func l8ForParsers(t *testing.T, body func(t *testing.T)) {
	for _, parser := range []string{nornicConfig.ParserTypeNornic, nornicConfig.ParserTypeANTLR} {
		t.Run(parser, func(t *testing.T) {
			previous := nornicConfig.GetParserType()
			nornicConfig.SetParserType(parser)
			defer nornicConfig.SetParserType(previous)
			body(t)
		})
	}
}

func l8One(o differential.Outcome) string {
	if o.Code != "" || len(o.Rows) != 1 || len(o.Rows[0]) != 1 {
		return "?" + o.Code + fmt.Sprint(o.Rows)
	}
	return fmt.Sprint(o.Rows[0][0])
}

// ---------------------------------------------------------------- L-H5-001
// The lead: a committed CREATE (:H5RB {key: k}) is not found by its own
// client's next MATCH (n:H5RB {key: k}) while another client drops and
// re-creates the index on (H5RB.key). Deterministic form: park the new
// index's fill (the storage test hook, at the fill's entry, before any value
// is written) and read the node committed before the index existed.
func TestL8_H5_001_ReadDuringIndexFill(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		if o := l8Q(t, h, "L-H5-001", "CREATE (:H5RB {key: 'k1'})", nil); o.Failed() {
			t.Fatal(o.Message)
		}
		before := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", map[string]any{"k": "k1"})
		parked, release := make(chan struct{}), make(chan struct{})
		armed := true
		restore := storage.SetSchemaIndexFillHookForTesting(func(indexName, label string) error {
			if armed && label == "H5RB" {
				armed = false
				close(parked)
				<-release
			}
			return nil
		})
		defer restore()
		created := make(chan differential.Outcome, 1)
		go func() {
			created <- h.Run(routeBoltAuto, "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil, time.Minute)
		}()
		select {
		case <-parked:
		case <-time.After(30 * time.Second):
			close(release)
			t.Fatal("index fill never started")
		}
		during := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", map[string]any{"k": "k1"})
		duringScan := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB) WHERE n.key + '' = $k RETURN count(n) AS c", map[string]any{"k": "k1"})
		duringLabel := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB) RETURN count(n) AS c", nil)
		indexes := l8Q(t, h, "L-H5-001", "SHOW INDEXES YIELD name, state, labelsOrTypes, properties RETURN name, state", nil)
		// Diag: which indexes the seek path may use while the fill is parked.
		seekable := []string{}
		schema := storage.NewNamespacedEngine(h.db.GetBaseStorageForManager(), "nornic").GetSchema()
		if schema != nil {
			list, err := schema.SeekableCompositeIndexesForLabel(context.Background(), "H5RB")
			for _, idx := range list {
				seekable = append(seekable, idx.Name)
			}
			l8Log(t, "L8_OWNER", map[string]any{"lead": "L-H5-001", "step": "SeekableCompositeIndexesForLabel during parked fill", "seekable": seekable, "err": fmt.Sprint(err)})
		}
		close(release)
		createOutcome := <-created
		after := l8Q(t, h, "L-H5-001", "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", map[string]any{"k": "k1"})
		l8Log(t, "L8_H5", map[string]any{"before": l8One(before), "during_index_seek": l8One(during), "during_scan": l8One(duringScan), "during_label_count": l8One(duringLabel), "indexes_during": fmt.Sprint(indexes.Rows), "create_index": createOutcome.Code, "after": l8One(after)})
		if l8One(during) != l8One(before) {
			t.Errorf("committed node not found while the index fill runs: before %s, during %s, after %s (scan during %s)", l8One(before), l8One(during), l8One(after), l8One(duringScan))
		}
	})
}

// Control: with the fill not parked, the read after CREATE INDEX finds the node.
func TestL8_H5_001_Control(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		l8Q(t, h, "L-H5-001", "CREATE (:H5RB {key: 'k1'})", nil)
		l8Q(t, h, "L-H5-001", "CREATE INDEX h5_rb IF NOT EXISTS FOR (n:H5RB) ON (n.key)", nil)
		if got := l8One(l8Q(t, h, "L-H5-001", "MATCH (n:H5RB {key: $k}) RETURN count(n) AS c", map[string]any{"k": "k1"})); !strings.Contains(got, "1") {
			t.Errorf("control: %s", got)
		}
	})
}

// ---------------------------------------------------------------- L-H9-011 / -013
func TestL8_H9_011_NestedMapProperty(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		// docs/getting-started/first-queries.md:228-237, then :253, as written.
		create := l8Q(t, h, "L-H9-011", "CREATE (doc:Document {\n  title: \"README\",\n  metadata: {\n    author: \"Alice\",\n    version: \"1.0\",\n    tags: [\"documentation\", \"guide\"]\n  }\n})\nRETURN doc", nil)
		read := l8Q(t, h, "L-H9-011", "MATCH (doc:Document)\nRETURN doc.metadata.author, doc.metadata.tags", nil)
		if create.Failed() || len(read.Rows) != 1 {
			t.Errorf("the page's JSON-like property block: CREATE %s %s; the page's read returns %d rows (page: Alice, [documentation, guide])", create.Code, create.Message, len(read.Rows))
		}
	})
}

func TestL8_H9_011_Control(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	if o := l8Q(t, h, "L-H9-011", "CREATE (person:Person {name: \"Alice\", skills: [\"Python\", \"Go\"]}) RETURN person.skills AS s", nil); o.Failed() {
		t.Errorf("list property block failed: %s", o.Message)
	}
}

func TestL8_H9_013_DashDashComment(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		// docs/skills/neo4j-migration.skill.md:148-149 as written.
		o := l8Q(t, h, "L-H9-013", "\n-- Confirm no orphan _Migrated remains after promotion\nMATCH (n:_Migrated) RETURN count(n);", nil)
		if o.Failed() {
			t.Errorf("verification block as written: %s %s", o.Code, o.Message)
		}
	})
}

func TestL8_H9_013_Control(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	if o := l8Q(t, h, "L-H9-013", "\n// Confirm no orphan _Migrated remains after promotion\nMATCH (n:_Migrated) RETURN count(n);", nil); o.Failed() {
		t.Errorf("// comment form failed: %s", o.Message)
	}
}

// ---------------------------------------------------------------- L-H9-012
func TestL8_H9_012_CSVExportAllStream(t *testing.T) {
	l8ForParsers(t, func(t *testing.T) {
		h := l8Server(t)
		defer h.Close(t)
		l8Q(t, h, "L-H9-012", "CREATE (:CSV150 {a: 1, b: 'one'}), (:CSV150 {b: 'two', c: true})", nil)
		query := l8Q(t, h, "L-H9-012", "CALL apoc.export.csv.query('MATCH (n:CSV150) RETURN n.a AS a, n.b AS b ORDER BY n.b', null, {stream: true}) YIELD data RETURN data", nil)
		all := l8Q(t, h, "L-H9-012", "CALL apoc.export.csv.all(null, {stream: true}) YIELD data RETURN data", nil)
		l8Q(t, h, "L-H9-012", "CALL apoc.export.csv.all(null, {stream: true})", nil)
		l8Q(t, h, "L-H9-012", "SHOW PROCEDURES YIELD name, signature WHERE name STARTS WITH 'apoc.export.csv' RETURN name, signature", nil)
		if query.Failed() {
			t.Logf("control (query export) failed too: %s", query.Message)
		}
		if all.Failed() {
			t.Errorf("apoc.export.csv.all(null, {stream: true}) YIELD data: %s %s", all.Code, all.Message)
		}
	})
}

// ---------------------------------------------------------------- L-H9-014
func TestL8_H9_014_DatabaseConfigTypedValue(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	key := "db.nornic.search.bm25.enabled"
	effective := func() string {
		_, body := l8HTTP(t, h, "L-H9-014", "GET", "/admin/databases/nornic/config", "")
		var decoded map[string]any
		_ = json.Unmarshal([]byte(body), &decoded)
		eff, _ := decoded["effective"].(map[string]any)
		ov, _ := decoded["overrides"].(map[string]any)
		return fmt.Sprintf("effective=%v override=%v", eff[key], ov[key])
	}
	before := effective()
	typed, _ := l8HTTP(t, h, "L-H9-014", "PUT", "/admin/databases/nornic/config", `{"overrides":{"`+key+`":false}}`)
	afterTyped := effective()
	text, _ := l8HTTP(t, h, "L-H9-014", "PUT", "/admin/databases/nornic/config", `{"overrides":{"`+key+`":"false"}}`)
	afterText := effective()
	dims, _ := l8HTTP(t, h, "L-H9-014", "PUT", "/admin/databases/nornic/config", `{"overrides":{"db.nornic.embedding.dimensions":1536}}`)
	l8Log(t, "L8_H9_014", map[string]any{"before": before, "typed_status": typed, "after_typed": afterTyped, "string_status": text, "after_string": afterText, "number_status": dims})
	// The documented request bodies (configuration.md:182, vector-search.md:41,
	// transaction-memory-limits.md:25, cypher-functions/README.md:686) all
	// carry string values; the string form must take effect.
	if text != http.StatusOK || !strings.Contains(afterText, "override=false") {
		t.Errorf("documented string form: %d %s", text, afterText)
	}
}

// ---------------------------------------------------------------- L-H9-017
func TestL8_H9_017_DocumentedEndpoints(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	l8Q(t, h, "L-H9-017", "CREATE (:Doc {name: 'a', embedding: [0.1, 0.2, 0.3]})", nil)
	nodeID, shortID := "", ""
	if result, err := neo4jdriver.ExecuteQuery(context.Background(), h.driver, "MATCH (n:Doc) RETURN elementId(n) AS id", nil, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("nornic")); err == nil && len(result.Records) == 1 {
		nodeID, _ = result.Records[0].Values[0].(string)
		shortID = nodeID
		if i := strings.LastIndex(nodeID, ":"); i >= 0 {
			shortID = nodeID[i+1:]
		}
	}
	probes := []struct{ method, path, body string }{
		{"POST", "/nornicdb/similar", `{"node_id":"` + nodeID + `","limit":5}`},
		{"POST", "/nornicdb/similar", `{"node_id":"` + shortID + `","limit":5}`},
		{"POST", "/nornicdb/similar", `{"node_id":"missing","limit":5}`},
		{"GET", "/gdpr/export", ""},
		{"POST", "/gdpr/export", `{"user_id":"u1"}`},
		{"DELETE", "/nornicdb/gdpr/", ""},
		{"DELETE", "/nornicdb/gdpr/delete", ""},
		{"POST", "/gdpr/delete", `{"user_id":"u1"}`},
		{"GET", "/admin/", ""},
		{"GET", "/admin/stats", ""},
		{"GET", "/db/tenant_a/stats", ""},
		{"GET", "/db/nornic/stats", ""},
		{"GET", "/mvcc/prune", ""},
		{"POST", "/mvcc/prune", "{}"},
		{"GET", "/admin/databases/nornic/mvcc/prune", ""},
		{"POST", "/admin/databases/nornic/mvcc/prune", "{}"},
		{"GET", "/nornicdb/health", ""},
		{"GET", "/health", ""},
	}
	for _, p := range probes {
		l8HTTP(t, h, "L-H9-017", p.method, p.path, p.body)
	}
	// POST /db/<db>/tx/<id> with a real open transaction id.
	status, body := l8HTTP(t, h, "L-H9-017", "POST", "/db/nornic/tx", `{"statements":[{"statement":"RETURN 1"}]}`)
	var opened struct {
		Commit string `json:"commit"`
	}
	_ = json.Unmarshal([]byte(body), &opened)
	if status == http.StatusCreated && opened.Commit != "" {
		txPath := strings.TrimSuffix(opened.Commit[strings.Index(opened.Commit, "/db/"):], "/commit")
		l8HTTP(t, h, "L-H9-017", "POST", txPath, `{"statements":[{"statement":"RETURN 2"}]}`)
		l8HTTP(t, h, "L-H9-017", "DELETE", txPath, "")
		l8HTTP(t, h, "L-H9-017", "POST", txPath, `{"statements":[{"statement":"RETURN 3"}]}`)
	}
}

// ---------------------------------------------------------------- L-H9-018 (procedures, Go API)
func l8RawRows(t *testing.T, kind string) []map[string]string {
	t.Helper()
	file, err := os.Open(filepath.Join("testdata", "l8", "h9-leads-raw-r1.tsv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 1<<20), 1<<20)
	var header []string
	var rows []map[string]string
	for scanner.Scan() {
		cols := strings.Split(scanner.Text(), "\t")
		if header == nil {
			header = cols
			continue
		}
		row := map[string]string{}
		for i, h := range header {
			if i < len(cols) {
				row[h] = cols[i]
			}
		}
		if row["kind"] == kind {
			rows = append(rows, row)
		}
	}
	return rows
}

func l8DocLine(documented string) string {
	where := documented
	if i := strings.LastIndex(where, "="); i >= 0 {
		where = where[i+1:]
	}
	parts := strings.Split(where, ":")
	if len(parts) < 2 {
		return ""
	}
	data, err := os.ReadFile(filepath.Join("..", "..", parts[0]))
	if err != nil {
		return "unreadable: " + err.Error()
	}
	lines := strings.Split(string(data), "\n")
	var n int
	fmt.Sscanf(parts[1], "%d", &n)
	if n < 1 || n > len(lines) {
		return ""
	}
	return strings.TrimSpace(lines[n-1])
}

// l8GoNames are the exported functions (pkg.Name) and methods (Type.Name) of
// the Go packages the documentation's Go-API names point at.
func l8GoNames(t *testing.T) map[string]bool {
	names := map[string]bool{}
	for _, dir := range []string{"apoc", "nornicdb", "cypher", "storage"} {
		fset := token.NewFileSet()
		pkgs, err := parser.ParseDir(fset, filepath.Join("..", dir), func(info os.FileInfo) bool { return !strings.HasSuffix(info.Name(), "_test.go") }, 0)
		if err != nil {
			t.Logf("parse %s: %v", dir, err)
			continue
		}
		for _, pkg := range pkgs {
			for _, file := range pkg.Files {
				for _, decl := range file.Decls {
					fn, ok := decl.(*ast.FuncDecl)
					if !ok || !fn.Name.IsExported() {
						continue
					}
					if fn.Recv == nil || len(fn.Recv.List) == 0 {
						names[dir+"."+fn.Name.Name] = true
						continue
					}
					recv := fn.Recv.List[0].Type
					if star, ok := recv.(*ast.StarExpr); ok {
						recv = star.X
					}
					if ident, ok := recv.(*ast.Ident); ok {
						names[ident.Name+"."+fn.Name.Name] = true
						names["method."+fn.Name.Name] = true
					}
				}
			}
		}
	}
	return names
}

func TestL8_H9_018_Procedures(t *testing.T) {
	h := l8Server(t)
	defer h.Close(t)
	ctx := context.Background()
	available := map[string]string{}
	for _, q := range []string{"SHOW PROCEDURES YIELD name RETURN name", "SHOW FUNCTIONS YIELD name RETURN name"} {
		result, err := neo4jdriver.ExecuteQuery(ctx, h.driver, q, nil, neo4jdriver.EagerResultTransformer, neo4jdriver.ExecuteQueryWithDatabase("nornic"))
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		for _, record := range result.Records {
			if name, ok := record.Values[0].(string); ok {
				available[strings.ToLower(name)] = strings.Fields(q)[1]
			}
		}
	}
	goNames := l8GoNames(t)
	counts := map[string]int{}
	for _, row := range l8RawRows(t, "procedure") {
		name := row["name"]
		parts := strings.Split(name, ".")
		last := parts[len(parts)-1]
		class := ""
		found := available[strings.ToLower(name)]
		switch {
		case found != "":
			class = "registered (" + found + ")"
		case last != "" && last[0] >= 'A' && last[0] <= 'Z':
			// A Go identifier: apoc.Call, db.Backup, nornicdb.Open.
			if goNames[name] || goNames["method."+last] || goNames["nornicdb."+last] || goNames["apoc."+last] {
				class = "go-api exists"
			} else {
				class = "go-api missing"
			}
		default:
			class = "not registered"
		}
		counts[class]++
		l8Log(t, "L8_PROC", map[string]any{"item": row["item"], "name": name, "documented": row["documented"], "class": class, "doc_line": l8DocLine(row["documented"])})
	}
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	l8Log(t, "L8_PROC_SUMMARY", map[string]any{"counts": counts, "classes": keys, "registered_total": len(available)})
}

