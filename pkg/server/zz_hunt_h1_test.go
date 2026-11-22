package server

// Hunt ground H1: the same statement through two paths must return the same
// rows and the same error class.
//
// Population: testdata/hunt/h1-corpus.json.gz (frozen at hunt opening; built by
// sig/handoff/v152-verification/hunt/tools/extract_h1.py). Each group (one
// test function, one corpus file or one TCK scenario) runs in order: its setup
// statements, then its statements, on a database emptied before the group.
//
// Path pairs (HUNT_H1_PAIR):
//
//	parser     default parser / ANTLR parser            (Bolt auto-commit)
//	tx         Bolt auto-commit / Bolt explicit transaction
//	protocol   Bolt auto-commit / HTTP /tx/commit       (protocol-neutral values)
//	cache      query cache on (each read run twice, second answer) / off
//	index      without / with a range index on every label-property pair the group names
//	durability durable writes / asynchronous writes
//	restart    the group's reads after the sequence / the same reads after a clean restart
//	backup     the group's reads after the sequence / the same reads on a restore of a backup
//
// Oracle: per statement, both outcomes fail with the same status code, or both
// succeed and differential.Same holds (the repository's own comparison rules:
// order only under ORDER BY, collect() order, token lists, arbitrary rows).
// Statements whose values cannot be equal across two databases (rand(),
// randomUUID(), timestamp(), id(), elementId(), the clock functions without
// arguments) compare columns and row count only. Write groups also compare the
// whole graph afterwards (nodes and relationships as multisets).
//
// Run: HUNT_H1_PAIR=<pair> [HUNT_H1_SHARD=i/n] [HUNT_H1_SOURCE=repro|seven|tck]
//      go test ./pkg/server -run '^TestHuntH1PathPairs$' -count=1 -timeout=6h -v
// Output: one "HUNT_LEAD {...}" line per disagreement, one "HUNT_COVERAGE {...}".

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	neo4jdriver "github.com/neo4j/neo4j-go-driver/v5/neo4j"
	nornicConfig "github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/testing/cypher/differential"
)

type huntH1Statement struct {
	ID     string          `json:"id"`
	Line   int             `json:"line"`
	Query  string          `json:"query"`
	Params json.RawMessage `json:"params"`
	Flags  []string        `json:"flags"`
}

type huntH1Group struct {
	ID         string            `json:"id"`
	Source     string            `json:"source"`
	File       string            `json:"file"`
	Name       string            `json:"name"`
	Setup      []string          `json:"setup"`
	Statements []huntH1Statement `json:"statements"`
}

type huntH1Corpus struct {
	Pin    string        `json:"pin"`
	Groups []huntH1Group `json:"groups"`
}

func loadHuntH1Corpus(t *testing.T) huntH1Corpus {
	t.Helper()
	path := huntEnv("HUNT_H1_CORPUS", filepath.Join("testdata", "hunt", "h1-corpus.json.gz"))
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open corpus: %v", err)
	}
	defer file.Close()
	reader, err := gzip.NewReader(file)
	if err != nil {
		t.Fatalf("gzip corpus: %v", err)
	}
	var corpus huntH1Corpus
	if err := json.NewDecoder(reader).Decode(&corpus); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	return corpus
}

var (
	huntNondeterministicRE = regexp.MustCompile(`(?i)\b(rand|randomUUID|timestamp|elementId|id|linenumber|file)\s*\(|\b(datetime|date|time|localtime|localdatetime)(\.(realtime|statement|transaction))?\s*\(\s*\)|\bdatetime\.|\bdb\.info\b|\bdbms\.|SHOW\s+(TRANSACTIONS|DATABASES?|SETTINGS|PROCEDURES|FUNCTIONS|INDEXES|CONSTRAINTS|USERS|ROLES)`)
	huntDatabaseDDLRE      = regexp.MustCompile(`(?i)\b(CREATE|DROP|ALTER|START|STOP)\s+(OR\s+REPLACE\s+)?(COMPOSITE\s+)?(DATABASE|ALIAS|USER|ROLE)\b|\bGRANT\b|\bDENY\b|\bREVOKE\b`)
	huntLabelRE            = regexp.MustCompile(`:\s*([A-Za-z_][A-Za-z0-9_]*)`)
	huntPropertyRE         = regexp.MustCompile(`(?:\.|\{\s*|,\s*)([A-Za-z_][A-Za-z0-9_]*)\s*[:=<>]|\.([A-Za-z_][A-Za-z0-9_]*)\b`)
)

func huntValuesComparable(query string) bool { return !huntNondeterministicRE.MatchString(query) }

// huntAgree is the H1 oracle for one statement.
func huntAgree(query string, a, b differential.Outcome) (bool, string) {
	switch {
	case a.Failed() && b.Failed():
		if a.Code == b.Code {
			return true, ""
		}
		return false, "error class differs"
	case a.Failed() != b.Failed():
		return false, "one path failed, the other returned rows"
	}
	if !huntValuesComparable(query) {
		if strings.Join(a.Columns, "\x00") == strings.Join(b.Columns, "\x00") && len(a.Rows) == len(b.Rows) {
			return true, ""
		}
		return false, "columns or row count differ (values not comparable)"
	}
	if differential.Same(query, a, b) {
		return true, ""
	}
	return false, "rows differ"
}

// huntIndexStatements are the range indexes the index pair adds: one per
// label and property name the group's text mentions (capped at 64).
func huntIndexStatements(group huntH1Group) []string {
	labels, properties := map[string]bool{}, map[string]bool{}
	texts := append(append([]string(nil), group.Setup...), func() []string {
		var out []string
		for _, s := range group.Statements {
			out = append(out, s.Query)
		}
		return out
	}()...)
	for _, text := range texts {
		for _, m := range huntLabelRE.FindAllStringSubmatch(text, -1) {
			labels[m[1]] = true
		}
		for _, m := range huntPropertyRE.FindAllStringSubmatch(text, -1) {
			name := m[1]
			if name == "" {
				name = m[2]
			}
			properties[name] = true
		}
	}
	var ls, ps []string
	for l := range labels {
		ls = append(ls, l)
	}
	for p := range properties {
		ps = append(ps, p)
	}
	sort.Strings(ls)
	sort.Strings(ps)
	var out []string
	for _, l := range ls {
		for _, p := range ps {
			if len(out) >= 64 {
				return out
			}
			out = append(out, fmt.Sprintf("CREATE INDEX `hunt_%s_%s` IF NOT EXISTS FOR (n:`%s`) ON (n.`%s`)", l, p, l, p))
		}
	}
	return out
}

// huntParams binds a statement's parameters: a JSON map as it is; TCK
// literal parameters evaluated as Cypher literals on server h.
func huntParams(h *huntServer, raw json.RawMessage) map[string]any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return nil
	}
	literals, ok := decoded["tck_literal_params"].(map[string]any)
	if !ok {
		return decoded
	}
	var parts []string
	for k, v := range literals {
		parts = append(parts, fmt.Sprintf("`%s`: %v", k, v))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	session := h.driver.NewSession(ctx, neo4jdriver.SessionConfig{DatabaseName: "nornic"})
	defer session.Close(context.Background())
	result, err := session.Run(ctx, "RETURN {"+strings.Join(parts, ", ")+"} AS p", nil)
	if err != nil {
		return nil
	}
	record, err := result.Single(ctx)
	if err != nil {
		return nil
	}
	params, _ := record.Values[0].(map[string]any)
	return params
}

// huntSide runs groups on one path.
type huntSide struct {
	name    string
	route   huntRoute
	options huntOptions
	parser  string
	indexes bool
	repeat  bool // cache side: reads run twice, the second answer counts
	server  *huntServer
	t       *testing.T
	base    string
	opened  int
}

func (s *huntSide) open() {
	s.opened++
	dir := filepath.Join(s.base, fmt.Sprintf("%s-%d", s.name, s.opened))
	s.server = huntOpen(s.t, dir, s.options)
}

func (s *huntSide) ensureEmpty(fresh bool) {
	if s.server != nil && !fresh && s.server.huntReset() {
		return
	}
	if s.server != nil {
		s.server.Close(s.t)
	}
	s.open()
}

// runGroup runs one group and returns each statement's outcome and the graph
// afterwards. selfLeads collects the cache side's first/second disagreements.
func (s *huntSide) runGroup(group huntH1Group, timeout time.Duration, selfLead func(huntH1Statement, differential.Outcome, differential.Outcome)) ([]differential.Outcome, []differential.Outcome) {
	if s.parser != "" {
		nornicConfig.SetParserType(s.parser)
	}
	if s.indexes {
		for _, statement := range huntIndexStatements(group) {
			_ = s.server.Run(routeBoltAuto, statement, nil, timeout)
		}
	}
	for _, statement := range group.Setup {
		_ = s.server.Run(routeBoltAuto, statement, nil, timeout)
	}
	outcomes := make([]differential.Outcome, len(group.Statements))
	for i, statement := range group.Statements {
		params := huntParams(s.server, statement.Params)
		outcomes[i] = s.server.Run(s.route, statement.Query, params, timeout)
		if s.repeat && !huntWrites(statement.Query) {
			second := s.server.Run(s.route, statement.Query, params, timeout)
			if ok, _ := huntAgree(statement.Query, outcomes[i], second); !ok && selfLead != nil {
				selfLead(statement, outcomes[i], second)
			}
			outcomes[i] = second
		}
	}
	return outcomes, s.server.huntGraphState()
}

func TestHuntH1PathPairs(t *testing.T) {
	pair := os.Getenv("HUNT_H1_PAIR")
	if pair == "" {
		t.Skip("set HUNT_H1_PAIR to one of parser, tx, protocol, cache, index, durability, restart, backup")
	}
	corpus := loadHuntH1Corpus(t)
	shard := os.Getenv("HUNT_H1_SHARD")
	source := os.Getenv("HUNT_H1_SOURCE")
	timeout := time.Duration(huntEnvInt("HUNT_STATEMENT_TIMEOUT_SECONDS", 20)) * time.Second
	defer nornicConfig.SetParserType(nornicConfig.ParserTypeNornic)
	base, err := os.MkdirTemp(huntEnv("HUNT_TMP", os.TempDir()), "hunt-h1-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(base)

	defaults := huntOptions{QueryCache: true}
	a := &huntSide{name: "a", route: routeBoltAuto, options: defaults, parser: nornicConfig.ParserTypeNornic, t: t, base: base}
	b := &huntSide{name: "b", route: routeBoltAuto, options: defaults, parser: nornicConfig.ParserTypeNornic, t: t, base: base}
	switch pair {
	case "parser":
		b.parser = nornicConfig.ParserTypeANTLR
	case "tx":
		b.route = routeBoltExplicit
	case "protocol":
		b.route = routeHTTPCommit
	case "cache":
		a.repeat = true
		b.options.QueryCache = false
	case "index":
		b.indexes = true
	case "durability":
		b.options.AsyncWrites = true
	case "restart", "backup":
		// one server; the second answer is read after a restart or on a restore
	default:
		t.Fatalf("unknown HUNT_H1_PAIR %q", pair)
	}
	sides := []*huntSide{a}
	if pair != "restart" && pair != "backup" {
		sides = append(sides, b)
	}
	var restoreTarget *huntServer
	defer func() {
		for _, side := range sides {
			if side.server != nil {
				side.server.Close(t)
			}
		}
		if restoreTarget != nil {
			restoreTarget.Close(t)
		}
	}()

	started := time.Now()
	groups, statements, compared, leads, skippedFmt, reopened := 0, 0, 0, 0, 0, 0
	lead := func(group huntH1Group, statement *huntH1Statement, verdict string, x, y differential.Outcome) {
		leads++
		entry := map[string]any{"ground": "H1", "pair": pair, "group": group.ID, "file": group.File, "name": group.Name,
			"verdict": verdict, "a": x, "b": y, "setup": group.Setup}
		if statement != nil {
			entry["statement"] = statement.ID
			entry["query"] = statement.Query
			entry["params"] = string(statement.Params)
			entry["line"] = statement.Line
		} else {
			entry["statement"] = group.ID + ":graph"
		}
		var sequence []string
		for _, s := range group.Statements {
			sequence = append(sequence, s.Query)
		}
		entry["sequence"] = sequence
		huntLead(t, entry)
	}

	for index, group := range corpus.Groups {
		if source != "" && group.Source != source {
			continue
		}
		if !huntShard(shard, index) {
			continue
		}
		var runnable []huntH1Statement
		for _, statement := range group.Statements {
			if huntContains(statement.Flags, "fmt") {
				skippedFmt++
				continue
			}
			runnable = append(runnable, statement)
		}
		if len(runnable) == 0 {
			continue
		}
		group.Statements = runnable
		groups++
		statements += len(runnable)
		ddl := false
		for _, text := range append(append([]string(nil), group.Setup...), func() []string {
			var q []string
			for _, s := range runnable {
				q = append(q, s.Query)
			}
			return q
		}()...) {
			if huntDatabaseDDLRE.MatchString(text) {
				ddl = true
			}
		}

		switch pair {
		case "restart", "backup":
			a.ensureEmpty(ddl)
			if a.server == nil {
				continue
			}
			_, _ = a.runGroup(group, timeout, nil)
			var reads []huntH1Statement
			for _, statement := range runnable {
				if !huntWrites(statement.Query) {
					reads = append(reads, statement)
				}
			}
			before := make([]differential.Outcome, len(reads))
			for i, statement := range reads {
				before[i] = a.server.Run(routeBoltAuto, statement.Query, huntParams(a.server, statement.Params), timeout)
			}
			beforeGraph := a.server.huntGraphState()
			var target *huntServer
			if pair == "restart" {
				dir := a.server.dir
				a.server.Close(t)
				a.server = huntOpen(t, dir, a.options)
				reopened++
				target = a.server
			} else {
				backupPath := filepath.Join(base, fmt.Sprintf("backup-%d.bak", index))
				if err := a.server.db.Backup(context.Background(), backupPath); err != nil {
					lead(group, nil, "backup failed: "+err.Error(), differential.Outcome{}, differential.Outcome{Code: "backup"})
					a.server.Close(t)
					a.server = nil
					continue
				}
				if restoreTarget == nil {
					restoreTarget = huntOpen(t, filepath.Join(base, "restore-target"), defaults)
				}
				if err := restoreTarget.db.Restore(context.Background(), backupPath); err != nil {
					lead(group, nil, "restore failed: "+err.Error(), differential.Outcome{}, differential.Outcome{Code: "restore"})
					restoreTarget.Close(t)
					restoreTarget = nil
					_ = os.Remove(backupPath)
					continue
				}
				_ = os.Remove(backupPath)
				target = restoreTarget
			}
			for i, statement := range reads {
				after := target.Run(routeBoltAuto, statement.Query, huntParams(target, statement.Params), timeout)
				compared++
				if ok, verdict := huntAgree(statement.Query, before[i], after); !ok {
					st := statement
					lead(group, &st, verdict, before[i], after)
				}
			}
			afterGraph := target.huntGraphState()
			for k := range beforeGraph {
				compared++
				if ok, verdict := huntAgree("MATCH (n) RETURN n", beforeGraph[k], afterGraph[k]); !ok {
					lead(group, nil, "graph "+verdict, beforeGraph[k], afterGraph[k])
				}
			}
		default:
			for _, side := range sides {
				side.ensureEmpty(ddl)
			}
			if ddl {
				reopened++
			}
			outA, graphA := a.runGroup(group, timeout, func(statement huntH1Statement, first, second differential.Outcome) {
				st := statement
				lead(group, &st, "cache: second run differs from first", first, second)
			})
			outB, graphB := b.runGroup(group, timeout, nil)
			for i, statement := range runnable {
				x, y := outA[i], outB[i]
				if pair == "protocol" {
					x = huntNeutralOutcome(x)
				}
				compared++
				if ok, verdict := huntAgree(statement.Query, x, y); !ok {
					st := statement
					lead(group, &st, verdict, x, y)
				}
			}
			for k := range graphA {
				compared++
				if ok, verdict := huntAgree("MATCH (n) RETURN n", graphA[k], graphB[k]); !ok {
					lead(group, nil, "graph "+verdict, graphA[k], graphB[k])
				}
			}
		}
	}
	huntCoverage(t, map[string]any{"ground": "H1", "pair": pair, "shard": shard, "source": source, "pin": corpus.Pin,
		"groups": groups, "statements": statements, "comparisons": compared, "leads": leads, "skippedFormatted": skippedFmt,
		"reopened": reopened, "seconds": int(time.Since(started).Seconds())})
}

func huntContains(values []string, want string) bool {
	for _, v := range values {
		if v == want {
			return true
		}
	}
	return false
}
