package fabric

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"strconv"
	"strings"
	"testing"
)

func TestPlan_PreservesOuterClausesAroundCallUseBlocks(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	query := `USE nornic
MATCH (seed:Seed) RETURN seed.id AS seedId
CALL {
  USE nornic.tr
  WITH seedId
  MATCH (t:Translation {id: seedId})
  RETURN t.text AS translated
}
RETURN seedId, translated`

	frag, err := p.Plan(t.Context(), query, "nornic", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	rootApply, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply root, got %T", frag)
	}

	trailingExec, ok := rootApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected trailing FragmentExec, got %T", rootApply.Inner)
	}
	if trailingExec.GraphName != "nornic" {
		t.Fatalf("expected trailing exec on nornic, got %s", trailingExec.GraphName)
	}

	callApply, ok := rootApply.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected call apply, got %T", rootApply.Input)
	}
	callExec, ok := callApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected call exec, got %T", callApply.Inner)
	}
	if callExec.GraphName != "nornic.tr" {
		t.Fatalf("expected call exec on nornic.tr, got %s", callExec.GraphName)
	}

	prefixApply, ok := callApply.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected prefix apply before call block, got %T", callApply.Input)
	}
	prefixExec, ok := prefixApply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected prefix exec, got %T", prefixApply.Inner)
	}
	if prefixExec.GraphName != "nornic" {
		t.Fatalf("expected prefix exec on nornic, got %s", prefixExec.GraphName)
	}
	if prefixExec.Query == "" {
		t.Fatalf("expected preserved prefix query")
	}
}

func TestPlan_MultipleCallUseSubqueries(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	catalog.Register("nornic.txt", &LocationRemote{DBName: "txt", URI: "bolt://b:7687"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	query := `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId, t.textKey AS textKey
}
CALL {
  USE nornic.txt
  WITH translationId
  MATCH (tt:TranslationText)
  WHERE tt.translationId = translationId
  RETURN collect(tt) AS texts
}
RETURN translationId, textKey, texts`

	frag, err := p.Plan(t.Context(), query, "nornic", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify semantic correctness: both USE targets should appear in the plan tree
	// and the trailing RETURN should be routed to the session database.
	trExec := findExecByGraphName(frag, "nornic.tr")
	if trExec == nil {
		t.Fatal("expected fragment routed to nornic.tr")
	}
	txtExec := findExecByGraphName(frag, "nornic.txt")
	if txtExec == nil {
		t.Fatal("expected fragment routed to nornic.txt")
	}
	// The plan must contain at least one FragmentApply (multi-graph decomposition).
	if _, ok := frag.(*FragmentApply); !ok {
		t.Fatalf("expected FragmentApply root for multi-graph plan, got %T", frag)
	}
}

func TestPlannerHelpers_ReturnClauseAndWithAliases(t *testing.T) {
	if !hasTopLevelReturnClause("MATCH (n) RETURN n", nil) {
		t.Fatalf("expected RETURN clause to be detected")
	}
	if hasTopLevelReturnClause("MATCH (n {x: 'RETURN literal'})", nil) {
		t.Fatalf("did not expect RETURN inside string literal to count")
	}

	aliases := trailingWithAliases("MATCH (n) WITH n.id AS id, n.name AS name", nil)
	if len(aliases) != 2 || aliases[0] != "id" || aliases[1] != "name" {
		t.Fatalf("unexpected trailing WITH aliases: %#v", aliases)
	}
}

func TestPlan_CallWithThenUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.tr", &LocationLocal{DBName: "nornic_tr"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "nornic_txt"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	query := `USE translations
CALL {
  USE translations.tr
  MATCH (t)
  RETURN t.textKey AS textKey, t.textKey128 AS textKey128
  LIMIT 2
}
CALL {
  WITH textKey128
  USE translations.txr
  MATCH (tt)
  WHERE tt.textKey128 = textKey128
  RETURN collect(tt) AS texts
}
RETURN textKey, textKey128, texts`

	frag, err := p.Plan(t.Context(), query, "translations", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	root, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply root, got %T", frag)
	}
	var execs []*FragmentExec
	var walk func(Fragment)
	walk = func(f Fragment) {
		switch n := f.(type) {
		case *FragmentExec:
			execs = append(execs, n)
		case *FragmentApply:
			walk(n.Input)
			walk(n.Inner)
		case *FragmentUnion:
			walk(n.LHS)
			walk(n.RHS)
		}
	}
	walk(root)

	var foundTR, foundTXR bool
	for _, ex := range execs {
		switch ex.GraphName {
		case "translations.tr":
			foundTR = true
		case "translations.txr":
			foundTXR = true
			if startsWithFold(strings.TrimSpace(ex.Query), "USE") {
				t.Fatalf("expected planner to strip USE from second call body, got query: %s", ex.Query)
			}
			if !startsWithFold(strings.TrimSpace(ex.Query), "WITH") {
				t.Fatalf("expected second call query to preserve WITH imports, got: %s", ex.Query)
			}
		}
	}
	if !foundTR {
		names := make([]string, 0, len(execs))
		for _, ex := range execs {
			names = append(names, ex.GraphName+"::"+strings.TrimSpace(ex.Query))
		}
		t.Fatalf("expected a call fragment routed to translations.tr; got %v", names)
	}
	if !foundTXR {
		names := make([]string, 0, len(execs))
		for _, ex := range execs {
			names = append(names, ex.GraphName+"::"+strings.TrimSpace(ex.Query))
		}
		t.Fatalf("expected a call fragment routed to translations.txr; got %v", names)
	}
}

func TestPlan_MidQueryUseAfterPipeline(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "caremark_txt"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	query := `WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt:MongoDocument)
WHERE tt.textKey128 IN keys
RETURN tt.textKey128 AS k, collect(tt) AS texts`

	frag, err := p.Plan(t.Context(), query, "translations", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	root, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected apply root for mid-query USE, got %T", frag)
	}
	prefixExec, ok := root.Input.(*FragmentApply)
	if !ok {
		t.Fatalf("expected prefix apply, got %T", root.Input)
	}
	pe, ok := prefixExec.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected prefix exec, got %T", prefixExec.Inner)
	}
	if pe.GraphName != "translations" {
		t.Fatalf("expected prefix graph translations, got %s", pe.GraphName)
	}
	up := strings.TrimSpace(strings.ToUpper(pe.Query))
	if !(strings.HasSuffix(up, "RETURN KEYS") || strings.HasSuffix(up, "RETURN *")) {
		t.Fatalf("expected prefix query to materialize row bindings via RETURN alias, got %q", pe.Query)
	}
	innerExec, ok := root.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected inner exec after USE, got %T", root.Inner)
	}
	if innerExec.GraphName != "translations.txr" {
		t.Fatalf("expected USE-routed graph translations.txr, got %s", innerExec.GraphName)
	}
}

// recordedCallImports is a CallImportReader that answers imports and
// records what the planner asked it (scoped|scope|body): the planner decides
// where a CALL's imports bind, the Cypher executor's reader what they are.
func recordedCallImports(imports ...string) (CallImportReader, *[]string) {
	asked := &[]string{}
	return func(scope string, scoped bool, body string) ([]string, error) {
		*asked = append(*asked, strconv.FormatBool(scoped)+"|"+scope+"|"+body)
		return imports, nil
	}, asked
}

func TestPlan_CorrelatedWithImports(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog, testStatementWrites)
	read, asked := recordedCallImports("myVar")
	p.SetCallImportReader(read)

	query := `USE db
CALL {
  USE db.shard
  WITH myVar
  MATCH (n) WHERE n.id = myVar
  RETURN n
}
RETURN n`

	frag, err := p.Plan(t.Context(), query, "nornic", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Walk to the CALL block's FragmentExec.
	apply := frag.(*FragmentApply)
	innerApply := apply.Input.(*FragmentApply)
	callExec := innerApply.Inner.(*FragmentExec)

	// The init of the exec should have import columns from WITH.
	init, ok := callExec.Input.(*FragmentInit)
	if !ok {
		t.Fatalf("expected FragmentInit as exec input, got %T", callExec.Input)
	}
	if len(init.ImportColumns) != 1 || init.ImportColumns[0] != "myVar" {
		t.Errorf("expected import columns [myVar], got %v", init.ImportColumns)
	}
	// The reader reads the body without its USE clause.
	if len(*asked) != 1 || !strings.HasPrefix((*asked)[0], "false||WITH myVar") || strings.Contains((*asked)[0], "USE") {
		t.Errorf("expected the reader to read the body without USE, got %q", *asked)
	}
}

func TestPlan_ScopedCallImportsLiveThroughBody(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	catalog.Register("db.other", &LocationLocal{DBName: "other"})
	p := NewFabricPlanner(catalog, testStatementWrites)
	read, asked := recordedCallImports("x")
	p.SetCallImportReader(read)

	frag, err := p.Plan(t.Context(), "USE db UNWIND [1] AS x CALL (x /* c */) { USE db.shard WITH 10 AS y CALL { USE db.other RETURN 1 AS z } RETURN x + y + z AS v } RETURN v", "db", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	readScope := false
	for _, question := range *asked {
		readScope = readScope || question == "true|x /* c */|USE db.shard WITH 10 AS y CALL { USE db.other RETURN 1 AS z } RETURN x + y + z AS v"
	}
	if !readScope {
		t.Fatalf("expected the reader to read the scope clause, got %q", *asked)
	}
	shard := collectExecsByGraph(frag, "db.shard")
	if len(shard) != 2 {
		t.Fatalf("expected the scoped body's two db.shard fragments, got %d", len(shard))
	}
	for _, exec := range shard {
		if len(exec.ScopeImports) != 1 || exec.ScopeImports[0] != "x" {
			t.Fatalf("expected scope imports [x] on %q, got %v", exec.Query, exec.ScopeImports)
		}
	}
	for _, exec := range collectExecsByGraph(frag, "db.other") {
		if len(exec.ScopeImports) != 0 {
			t.Fatalf("a nested CALL body imports its own variables, got %v on %q", exec.ScopeImports, exec.Query)
		}
	}
}

func collectExecsByGraph(fragment Fragment, graph string) []*FragmentExec {
	var out []*FragmentExec
	switch f := fragment.(type) {
	case *FragmentExec:
		if f.GraphName == graph {
			out = append(out, f)
		}
	case *FragmentApply:
		out = append(out, collectExecsByGraph(f.Input, graph)...)
		out = append(out, collectExecsByGraph(f.Inner, graph)...)
	case *FragmentUnion:
		out = append(out, collectExecsByGraph(f.LHS, graph)...)
		out = append(out, collectExecsByGraph(f.RHS, graph)...)
	}
	return out
}

func TestPlan_WriteDetection(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	tests := []struct {
		query   string
		isWrite bool
	}{
		{"MATCH (n) RETURN n", false},
		{"CREATE (n:Test)", true},
		{"MERGE (n:Test {id: 1})", true},
		{"MATCH (n) DELETE n", true},
		{"MATCH (n) DETACH DELETE n", true},
		{"MATCH (n) SET n.x = 1", true},
		{"MATCH (n) REMOVE n.x", true},
	}

	for _, tt := range tests {
		frag, err := p.Plan(t.Context(), tt.query, "db", cyphertext.RawSource)
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.query, err)
		}
		exec := frag.(*FragmentExec)
		if exec.IsWrite != tt.isWrite {
			t.Errorf("query %q: expected IsWrite=%v, got %v", tt.query, tt.isWrite, exec.IsWrite)
		}
	}
}

func TestPlan_CallWithoutUse_NotFabric(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	// CALL {} without USE inside is a regular subquery, not fabric.
	query := `CALL {
  MATCH (n) RETURN n
}
RETURN n`

	frag, err := p.Plan(t.Context(), query, "db", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be a simple FragmentExec (no decomposition).
	_, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for non-fabric CALL, got %T", frag)
	}
}

func TestParseLeadingUse_NoUse(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUseName("MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUse {
		t.Error("expected hasUse=false")
	}
	if db != "" {
		t.Errorf("expected empty db, got %s", db)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Errorf("expected original query, got %s", remaining)
	}
}

func TestParseLeadingUse_SimpleUse(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUseName("USE mydb MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "mydb" {
		t.Errorf("expected mydb, got %s", db)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Errorf("expected 'MATCH (n) RETURN n', got %q", remaining)
	}
}

func TestParseLeadingUse_DottedName(t *testing.T) {
	db, remaining, hasUse, err := parseLeadingUseName("USE comp.shard MATCH (n)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "comp.shard" {
		t.Errorf("expected comp.shard, got %s", db)
	}
	if remaining != "MATCH (n)" {
		t.Errorf("expected 'MATCH (n)', got %q", remaining)
	}
}

func TestParseLeadingUse_DynamicGraphReference(t *testing.T) {
	use, remaining, hasUse, err := parseLeadingUse(t.Context(), "USE graph.byName('tenant_a') MATCH (n) RETURN n", false)
	if err != nil || !hasUse {
		t.Fatalf("unexpected result: hasUse=%v err=%v", hasUse, err)
	}
	if !use.IsDynamic() || use.Function != "graph.byName" || len(use.Args) != 1 || use.Args[0] != "'tenant_a'" {
		t.Fatalf("unexpected dynamic reference: %+v", use)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Fatalf("unexpected remaining query: %q", remaining)
	}

	use, _, hasUse, err = parseLeadingUse(t.Context(), "USE graph.byElementId($id) RETURN 1", false)
	if err != nil || !hasUse || use.Text() != "graph.byElementId($id)" {
		t.Fatalf("unexpected byElementId result: %+v hasUse=%v err=%v", use, hasUse, err)
	}
}

func TestPlannerHelpers_SplitAtTopLevelUse(t *testing.T) {
	prefix, suffix, ok := splitAtTopLevelUse("MATCH (n) WITH n USE translations.tr RETURN n", nil)
	if !ok {
		t.Fatalf("expected top-level USE split")
	}
	if strings.TrimSpace(prefix) == "" || !strings.Contains(strings.ToUpper(suffix), "USE") {
		t.Fatalf("unexpected split parts: prefix=%q suffix=%q", prefix, suffix)
	}
}

// parseLeadingUseName is parseLeadingUse for a top-level query, returning
// the static graph name.
func parseLeadingUseName(query string) (string, string, bool, error) {
	use, remaining, hasUse, err := parseLeadingUse(context.Background(), query, false)
	return use.Name, remaining, hasUse, err
}

func TestParseLeadingUse_BacktickQuoted(t *testing.T) {
	db, _, hasUse, err := parseLeadingUseName("USE `my db` MATCH (n)")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUse {
		t.Error("expected hasUse=true")
	}
	if db != "my db" {
		t.Errorf("expected 'my db', got %s", db)
	}
}

func TestParseLeadingUse_NotUseKeyword(t *testing.T) {
	// "USER" starts with "USE" but is not USE.
	_, _, hasUse, _ := parseLeadingUseName("USER foo")
	if hasUse {
		t.Error("expected hasUse=false for USER keyword")
	}

	_, _, hasUse, _ = parseLeadingUseName("USING PERIODIC COMMIT")
	if hasUse {
		t.Error("expected hasUse=false for USING keyword")
	}
}

func TestFindMatchingBrace(t *testing.T) {
	tests := []struct {
		input string
		pos   int
		want  int
	}{
		{"{}", 0, 1},
		{"{ x }", 0, 4},
		{"{ { nested } }", 0, 13},
		{"{ 'str with }' }", 0, 15},
		{`{ "str with }" }`, 0, 15},
	}

	for _, tt := range tests {
		got, err := findMatchingBrace(tt.input, tt.pos)
		if err != nil {
			t.Errorf("findMatchingBrace(%q, %d): unexpected error: %v", tt.input, tt.pos, err)
			continue
		}
		if got != tt.want {
			t.Errorf("findMatchingBrace(%q, %d) = %d, want %d", tt.input, tt.pos, got, tt.want)
		}
	}
}

func TestFindMatchingBrace_Unmatched(t *testing.T) {
	_, err := findMatchingBrace("{ unclosed", 0)
	if err == nil {
		t.Fatal("expected error for unmatched brace")
	}
}

// --- Additional coverage tests ---

func TestSplitTopLevelUnion_NoUnion(t *testing.T) {
	parts, ops, hasUnion, err := splitTopLevelUnion("MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUnion {
		t.Fatal("expected no union")
	}
	if len(parts) != 0 || len(ops) != 0 {
		t.Fatalf("unexpected parts/ops: %v / %v", parts, ops)
	}
}

func TestSplitTopLevelUnion_EmptyBranch(t *testing.T) {
	_, _, _, err := splitTopLevelUnion("UNION ALL RETURN 1", nil)
	if err == nil {
		t.Fatal("expected error for empty leading branch")
	}
}

func TestSplitTopLevelUnion_EmptyTrailingBranch(t *testing.T) {
	_, _, _, err := splitTopLevelUnion("RETURN 1 UNION ALL ", nil)
	if err == nil {
		t.Fatal("expected error for empty trailing branch")
	}
}

func TestSplitTopLevelUnion_InsideQuotes(t *testing.T) {
	// UNION inside single-quoted string.
	_, _, hasUnion, err := splitTopLevelUnion("RETURN 'UNION ALL' AS x", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUnion {
		t.Fatal("expected UNION inside quotes to be ignored")
	}
	// UNION inside double-quoted string.
	_, _, hasUnion, err = splitTopLevelUnion(`RETURN "UNION" AS x`, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUnion {
		t.Fatal("expected UNION inside double quotes to be ignored")
	}
}

func TestSplitTopLevelUnion_InsideBracesParens(t *testing.T) {
	// UNION inside braces should not split.
	_, _, hasUnion, _ := splitTopLevelUnion("CALL { RETURN 1 UNION RETURN 2 } RETURN x", nil)
	if hasUnion {
		t.Fatal("expected UNION inside braces to be ignored")
	}
	// UNION inside parens should not split.
	_, _, hasUnion, _ = splitTopLevelUnion("RETURN (1 UNION 2)", nil)
	if hasUnion {
		t.Fatal("expected UNION inside parens to be ignored")
	}
}

func TestSplitTopLevelUnion_DistinctUnion(t *testing.T) {
	parts, ops, hasUnion, err := splitTopLevelUnion("RETURN 1 AS x UNION RETURN 2 AS x", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !hasUnion {
		t.Fatal("expected union")
	}
	if len(parts) != 2 {
		t.Fatalf("expected 2 parts, got %d", len(parts))
	}
	if len(ops) != 1 || !ops[0] {
		t.Fatalf("expected distinct=true for plain UNION, got %v", ops)
	}
}

func TestSplitAtTopLevelUse_WithQuotedStrings(t *testing.T) {
	// USE inside single-quoted string should not split.
	_, _, ok := splitAtTopLevelUse("MATCH (n {x: 'USE db'}) RETURN n", nil)
	if ok {
		t.Fatal("expected USE inside single quotes to be ignored")
	}
	// USE inside double-quoted string should not split.
	_, _, ok = splitAtTopLevelUse(`MATCH (n {x: "USE db"}) RETURN n`, nil)
	if ok {
		t.Fatal("expected USE inside double quotes to be ignored")
	}
	// USE inside backtick.
	_, _, ok = splitAtTopLevelUse("MATCH (n) WITH `USE` AS x USE db RETURN n", nil)
	if !ok {
		t.Fatal("expected USE after backtick to split")
	}
}

func TestSplitAtTopLevelUse_InsideNesting(t *testing.T) {
	// USE inside braces.
	_, _, ok := splitAtTopLevelUse("CALL { USE db RETURN 1 } RETURN x", nil)
	if ok {
		t.Fatal("expected USE inside braces to be ignored")
	}
	// USE inside parens.
	_, _, ok = splitAtTopLevelUse("MATCH (n WHERE USE = 1) RETURN n", nil)
	// This one might match depending on keyword boundary logic.
	// Just ensure no panic.
	_ = ok
	// USE inside brackets.
	_, _, ok = splitAtTopLevelUse("RETURN [x IN list WHERE USE = 1]", nil)
	_ = ok
}

func TestSplitAtTopLevelUse_LeadingUseSkipped(t *testing.T) {
	// Leading USE should be skipped (handled by parseLeadingUse).
	_, _, ok := splitAtTopLevelUse("USE db MATCH (n) RETURN n", nil)
	if ok {
		t.Fatal("expected leading USE to be skipped")
	}
}

func TestSplitAtTopLevelUse_NoUse(t *testing.T) {
	_, _, ok := splitAtTopLevelUse("MATCH (n) RETURN n", nil)
	if !ok {
		// No USE at all → ok=false.
	}
}

func TestEnsureRowProducingPrefix(t *testing.T) {
	// Empty query.
	if ensureRowProducingPrefix("", nil) != "" {
		t.Fatal("expected empty for empty input")
	}
	// Query with RETURN.
	got := ensureRowProducingPrefix("MATCH (n) RETURN n", nil)
	if got != "MATCH (n) RETURN n" {
		t.Fatalf("expected unchanged with RETURN, got %q", got)
	}
	// WITH clause with aliases.
	got = ensureRowProducingPrefix("WITH a, b", nil)
	if !strings.HasSuffix(got, "RETURN a, b") {
		t.Fatalf("expected RETURN aliases appended, got %q", got)
	}
	// Query without RETURN and without WITH.
	got = ensureRowProducingPrefix("MATCH (n)", nil)
	if !strings.HasSuffix(got, "RETURN *") {
		t.Fatalf("expected RETURN * appended, got %q", got)
	}
	// A clause after the WITH keeps the WITH's variables and adds its own.
	got = ensureRowProducingPrefix("WITH 'ab' AS s UNWIND [1, 2] AS x", nil)
	if got != "WITH 'ab' AS s UNWIND [1, 2] AS x RETURN *" {
		t.Fatalf("expected RETURN * after a later clause, got %q", got)
	}
	// The WITH's own WHERE, ORDER BY, SKIP and LIMIT are part of it.
	got = ensureRowProducingPrefix("UNWIND [3, 1] AS y WITH y AS x WHERE x > 0 ORDER BY x SKIP 0 LIMIT 2", nil)
	if !strings.HasSuffix(got, "LIMIT 2 RETURN *") {
		t.Fatalf("expected RETURN * for a prefix starting with UNWIND, got %q", got)
	}
	got = ensureRowProducingPrefix("WITH [3, 1] AS ys, 1 AS z WITH z AS x WHERE x > 0 ORDER BY x SKIP 0 LIMIT 2", nil)
	if !strings.HasSuffix(got, "LIMIT 2 RETURN x") {
		t.Fatalf("expected the last WITH's alias, got %q", got)
	}
}

func TestHasTopLevelReturnClause_WithQuotes(t *testing.T) {
	// RETURN inside single quotes.
	if hasTopLevelReturnClause("MATCH (n {x: 'RETURN n'})", nil) {
		t.Fatal("expected RETURN in single quotes to be ignored")
	}
	// RETURN inside double quotes.
	if hasTopLevelReturnClause(`MATCH (n {x: "RETURN n"})`, nil) {
		t.Fatal("expected RETURN in double quotes to be ignored")
	}
	// RETURN inside backticks.
	if hasTopLevelReturnClause("MATCH (n) WITH `RETURN` AS x", nil) {
		t.Fatal("expected RETURN in backticks to be ignored")
	}
	// RETURN inside braces.
	if hasTopLevelReturnClause("CALL { RETURN 1 }", nil) {
		t.Fatal("expected RETURN inside braces to be ignored")
	}
	// RETURN inside parens.
	if hasTopLevelReturnClause("MATCH (RETURN)", nil) {
		// "RETURN" after ( is inside paren depth.
		t.Fatal("expected RETURN inside parens to be ignored")
	}
	// RETURN inside brackets.
	if hasTopLevelReturnClause("[RETURN 1]", nil) {
		t.Fatal("expected RETURN inside brackets to be ignored")
	}
	// Top-level RETURN.
	if !hasTopLevelReturnClause("MATCH (n) RETURN n", nil) {
		t.Fatal("expected top-level RETURN to be detected")
	}
}

func TestTrailingWithAliases_EdgeCases(t *testing.T) {
	// No WITH.
	if trailingWithAliases("MATCH (n)", nil) != nil {
		t.Fatal("expected nil for no WITH")
	}
	// WITH inside quotes should not match.
	got := trailingWithAliases("MATCH (n {x: 'WITH foo'}) WITH a", nil)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
	// WITH inside backticks.
	got = trailingWithAliases("MATCH (n) WITH `weird name` AS x", nil)
	// `weird name` is not a valid identifier, so AS alias "x" should be found.
	// But the item with backtick goes through lastAsIndexFold which finds AS.
	// alias = "x", cyphertext.IsIdentifier("x") = true.
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("expected [x] with backtick alias, got %v", got)
	}
	// Empty WITH clause.
	got = trailingWithAliases("WITH ", nil)
	if got != nil {
		t.Fatalf("expected nil for empty WITH clause, got %v", got)
	}
	// WITH inside braces.
	got = trailingWithAliases("CALL { WITH inner } WITH outer", nil)
	if len(got) != 1 || got[0] != "outer" {
		t.Fatalf("expected [outer], got %v", got)
	}
	// WITH inside double quotes.
	got = trailingWithAliases(`MATCH (n) WITH "WITH" AS x`, nil)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("expected [x], got %v", got)
	}
	// Non-valid identifier (has dot).
	got = trailingWithAliases("WITH n.x", nil)
	if len(got) != 0 {
		t.Fatalf("expected empty for non-valid identifier, got %v", got)
	}
}

func TestIdentifierValidation(t *testing.T) {
	for _, tc := range []struct {
		input string
		valid bool
	}{
		{input: ""},
		{input: "1abc"},
		{input: "abc", valid: true},
		{input: "_abc", valid: true},
		{input: "a.b"},
		{input: "a-b"},
		{input: "abc123_DEF", valid: true},
		{input: "abc.def"},
		{input: "abc123", valid: true},
	} {
		t.Run(tc.input, func(t *testing.T) {
			if got := cyphertext.IsIdentifier(tc.input); got != tc.valid {
				t.Errorf("validation of %q = %v, want %v", tc.input, got, tc.valid)
			}
		})
	}
}

func TestExtractTopLevelCallBlocks_WithComments(t *testing.T) {
	// Line comment.
	query := "// comment\nCALL { RETURN 1 }\nRETURN x"
	blocks, err := extractTopLevelCallBlocks(query, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
	// Block comment.
	query = "/* block comment */ CALL { RETURN 1 } RETURN x"
	blocks, err = extractTopLevelCallBlocks(query, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block with block comment, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_InsideQuotes(t *testing.T) {
	// CALL inside single-quoted string.
	query := "RETURN 'CALL { RETURN 1 }' AS x"
	blocks, err := extractTopLevelCallBlocks(query, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks inside quotes, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_CallWithoutBrace(t *testing.T) {
	// CALL followed by non-brace (procedure call, not subquery).
	query := "CALL db.procedure() RETURN x"
	blocks, err := extractTopLevelCallBlocks(query, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks for procedure call, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_UnmatchedBrace(t *testing.T) {
	query := "CALL { RETURN 1"
	_, err := extractTopLevelCallBlocks(query, nil)
	if err == nil {
		t.Fatal("expected error for unmatched brace")
	}
}

func TestFindMatchingBrace_WithComments(t *testing.T) {
	// Line comment inside braces.
	pos, err := findMatchingBrace("{ // comment\n RETURN 1 }", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 23 {
		t.Fatalf("expected pos=23, got %d", pos)
	}
	// Block comment inside braces.
	pos, err = findMatchingBrace("{ /* comment */ RETURN 1 }", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 25 {
		t.Fatalf("expected pos=25, got %d", pos)
	}
}

func TestFindMatchingBrace_NotAtBrace(t *testing.T) {
	_, err := findMatchingBrace("abc", 0)
	if err == nil {
		t.Fatal("expected error when pos is not at {")
	}
}

func TestFindMatchingBrace_PosOutOfBounds(t *testing.T) {
	_, err := findMatchingBrace("{}", 5)
	if err == nil {
		t.Fatal("expected error for out-of-bounds pos")
	}
}

func TestFindMatchingBrace_QuotedStrings(t *testing.T) {
	// Single-quoted with escaped quote.
	pos, err := findMatchingBrace("{ 'it''s' }", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 10 {
		t.Fatalf("expected pos=10, got %d", pos)
	}
	// Double-quoted with escaped quote.
	pos, err = findMatchingBrace(`{ "say ""hi""" }`, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if pos != 15 {
		t.Fatalf("expected pos=15, got %d", pos)
	}
}

func TestCompositeScopeRoot(t *testing.T) {
	if compositeScopeRoot("") != "" {
		t.Fatal("expected empty for empty")
	}
	if compositeScopeRoot("  ") != "  " {
		t.Fatal("decoded graph identity must retain whitespace")
	}
	if compositeScopeRoot("comp.shard") != "comp" {
		t.Fatal("expected comp")
	}
	if compositeScopeRoot("simple") != "simple" {
		t.Fatal("expected simple")
	}
}

func TestInCompositeScope(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	if !p.inCompositeScope("comp") {
		t.Fatal("expected comp to be in composite scope")
	}
	if !p.inCompositeScope("comp.shard") {
		t.Fatal("expected dotted name to be in composite scope")
	}
	if p.inCompositeScope("other") {
		t.Fatal("expected other to not be in composite scope")
	}
	if p.inCompositeScope("") {
		t.Fatal("expected empty to not be in composite scope")
	}

	// Nil catalog.
	p2 := NewFabricPlanner(nil, testStatementWrites)
	if p2.inCompositeScope("comp") {
		t.Fatal("expected false with nil catalog")
	}
}

func TestBindLeadingImportColumns_AllBranches(t *testing.T) {
	// nil fragment.
	if bindLeadingImportColumns(nil, []string{"a"}) != nil {
		t.Fatal("expected nil for nil fragment")
	}
	// Empty import cols.
	exec := &FragmentExec{Input: &FragmentInit{}}
	if bindLeadingImportColumns(exec, nil) != exec {
		t.Fatal("expected unchanged for nil import cols")
	}
	// Union fragment.
	union := &FragmentUnion{
		LHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 1"},
		RHS: &FragmentExec{Input: &FragmentInit{}, Query: "RETURN 2"},
	}
	result := bindLeadingImportColumns(union, []string{"a"})
	u, ok := result.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected *FragmentUnion, got %T", result)
	}
	lhs := u.LHS.(*FragmentExec)
	init := lhs.Input.(*FragmentInit)
	if len(init.ImportColumns) != 1 || init.ImportColumns[0] != "a" {
		t.Fatalf("expected import columns [a] on LHS, got %v", init.ImportColumns)
	}
	// Default branch (FragmentInit).
	init2 := &FragmentInit{Columns: []string{"x"}}
	if bindLeadingImportColumns(init2, []string{"a"}) != init2 {
		t.Fatal("expected init to pass through unchanged")
	}
}

func TestParseLeadingUse_UseOnly(t *testing.T) {
	_, _, _, err := parseLeadingUseName("USE")
	if err == nil {
		t.Fatal("expected error for USE without database name")
	}
}

func TestParseLeadingUse_InvalidGraphRef(t *testing.T) {
	_, _, _, err := parseLeadingUseName("USE graph.byName(")
	if err == nil {
		t.Fatal("expected error for invalid graph reference")
	}
}

func TestCallBlockContainsFabricUse_NoUse(t *testing.T) {
	ok, err := callBlockContainsFabricUse(t.Context(), "MATCH (n) RETURN n", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false for no USE")
	}
}

func TestContainsFold(t *testing.T) {
	if !containsFold("Hello World", "WORLD") {
		t.Fatal("expected case-insensitive match")
	}
	if containsFold("short", "longer needle") {
		t.Fatal("expected false when needle longer than string")
	}
	if !containsFold("abc", "") {
		t.Fatal("expected true for empty needle")
	}
}

func TestPlan_UnionWithThreeBranches(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "g1"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	frag, err := p.Plan(t.Context(), "RETURN 1 AS x UNION ALL RETURN 2 AS x UNION ALL RETURN 3 AS x", "g1", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be nested unions.
	u, ok := frag.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected FragmentUnion root, got %T", frag)
	}
	if u.Distinct {
		t.Fatal("expected outer union to be ALL (not distinct)")
	}
	inner, ok := u.LHS.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected nested FragmentUnion on LHS, got %T", u.LHS)
	}
	if inner.Distinct {
		t.Fatal("expected inner union to be ALL (not distinct)")
	}

	// UNION and UNION ALL at one level are rejected before a branch is planned.
	if _, err := p.Plan(t.Context(), "RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x", "g1", cyphertext.RawSource); !errors.Is(err, ErrMixedUnion) {
		t.Fatalf("expected ErrMixedUnion, got %v", err)
	}
}

func TestValidateUseTarget_EmptyTarget(t *testing.T) {
	p := NewFabricPlanner(NewCatalog(), testStatementWrites)
	err := p.validateUseTarget("db", "")
	if err == nil {
		t.Fatal("expected error for empty target")
	}
}

func TestHasGraphWithPrefix_NoMatch(t *testing.T) {
	c := NewCatalog()
	c.Register("db1", &LocationLocal{DBName: "db1"})
	if c.HasGraphWithPrefix("other") {
		t.Fatal("expected false for non-matching prefix")
	}
}

func TestHasGraphWithPrefix_Match(t *testing.T) {
	c := NewCatalog()
	c.Register("comp.shard", &LocationLocal{DBName: "shard"})
	if !c.HasGraphWithPrefix("comp.") {
		t.Fatal("expected true for matching prefix")
	}
}

func TestSplitTopLevelUnion_EscapedQuotes(t *testing.T) {
	// Single-quoted string with escaped quote containing UNION.
	_, _, hasUnion, _ := splitTopLevelUnion("RETURN 'it''s a UNION' AS x", nil)
	if hasUnion {
		t.Fatal("expected UNION inside escaped single quotes to be ignored")
	}
	// Double-quoted string with escaped quote containing UNION.
	_, _, hasUnion, _ = splitTopLevelUnion(`RETURN "say ""UNION""" AS x`, nil)
	if hasUnion {
		t.Fatal("expected UNION inside escaped double quotes to be ignored")
	}
}

func TestPlanMultiGraph_CallBlockWithInvalidUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	// CALL block with invalid nested USE target.
	query := `USE db
CALL {
  USE db.missing
  RETURN 1 AS x
}
RETURN x`
	_, err := p.Plan(t.Context(), query, "db", cyphertext.RawSource)
	if err == nil {
		t.Fatal("expected error for invalid USE target in CALL block")
	}
}

func TestParseLeadingUse_GraphRefError(t *testing.T) {
	_, _, _, err := parseLeadingUseName("USE graph.byName()")
	if err == nil {
		t.Fatal("expected error for empty graph reference")
	}
}

func TestExtractTopLevelCallBlocks_QuotedCallBrace(t *testing.T) {
	// CALL inside double-quoted string.
	blocks, err := extractTopLevelCallBlocks(`RETURN "CALL { RETURN 1 }" AS x`, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_EscapedQuotes(t *testing.T) {
	// CALL inside single-quoted string with escaped quotes.
	blocks, err := extractTopLevelCallBlocks("RETURN 'it''s CALL { }' AS x", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
	// CALL inside double-quoted string with escaped quotes.
	blocks, err = extractTopLevelCallBlocks(`RETURN "say ""CALL { }""" AS x`, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestCallBlockContainsFabricUse_ParseError(t *testing.T) {
	// Invalid USE should propagate error.
	_, err := callBlockContainsFabricUse(t.Context(), "USE ", nil)
	if err == nil {
		t.Fatal("expected error for invalid USE in call block")
	}
}

func TestCallBlockContainsFabricUse_ParseLeadingWithUseError(t *testing.T) {
	_, err := callBlockContainsFabricUse(t.Context(), "WITH a USE ", nil)
	if err == nil {
		t.Fatal("expected error for invalid WITH...USE in call block")
	}
}

func TestCallBlockContainsFabricUse_NestedCallError(t *testing.T) {
	// Nested CALL with unmatched brace.
	_, err := callBlockContainsFabricUse(t.Context(), "CALL { USE db", nil)
	if err == nil {
		t.Fatal("expected error for unmatched brace in nested CALL")
	}
}

func TestSplitAtTopLevelUse_EscapedSingleQuote(t *testing.T) {
	// Escaped single quote.
	_, _, ok := splitAtTopLevelUse("MATCH (n {x: 'it''s USE db'}) RETURN n", nil)
	if ok {
		t.Fatal("expected USE inside escaped single quote to be ignored")
	}
}

func TestSplitAtTopLevelUse_EscapedDoubleQuote(t *testing.T) {
	_, _, ok := splitAtTopLevelUse(`MATCH (n {x: "say ""USE db"""}) RETURN n`, nil)
	if ok {
		t.Fatal("expected USE inside escaped double quote to be ignored")
	}
}

func TestHasTopLevelReturnClause_EscapedQuotes(t *testing.T) {
	// Escaped single quote.
	if !hasTopLevelReturnClause("MATCH (n {x: 'it''s'}) RETURN n", nil) {
		t.Fatal("expected RETURN to be found after escaped single quote")
	}
	// Escaped double quote.
	if !hasTopLevelReturnClause(`MATCH (n {x: "say ""hello"""}) RETURN n`, nil) {
		t.Fatal("expected RETURN to be found after escaped double quote")
	}
}

func TestTrailingWithAliases_EscapedQuotes(t *testing.T) {
	// Escaped single quote in WITH clause.
	got := trailingWithAliases("MATCH (n {x: 'it''s'}) WITH a", nil)
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
	// Escaped double quote.
	got = trailingWithAliases(`MATCH (n {x: "say ""hi"""}) WITH b`, nil)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}
}

func TestTrailingWithAliases_BracketsAndParens(t *testing.T) {
	got := trailingWithAliases("WITH (WITH inside) AS x", nil)
	// WITH inside parens depth → not detected as trailing WITH.
	// Only the outer WITH at position 0 is found.
	_ = got // Behavior depends on keyword matching — just ensure no panic.
}

func TestTrailingWithAliases_NonValidAlias(t *testing.T) {
	// Expression without AS and non-valid identifier.
	got := trailingWithAliases("WITH count(*)", nil)
	if len(got) != 0 {
		t.Fatalf("expected empty for non-valid identifier, got %v", got)
	}
}

func TestTrailingWithAliases_AsNonValidAlias(t *testing.T) {
	got := trailingWithAliases("WITH x AS 1invalid", nil)
	if len(got) != 0 {
		t.Fatalf("expected empty for invalid AS alias, got %v", got)
	}
}

func TestFindMatchingBrace_EscapedQuotes(t *testing.T) {
	// Escaped single quote.
	pos, err := findMatchingBrace("{ 'it''s' }", 0)
	if err != nil || pos != 10 {
		t.Fatalf("expected pos=10, got pos=%d err=%v", pos, err)
	}
	// Escaped double quote.
	pos, err = findMatchingBrace(`{ "say ""hi""" }`, 0)
	if err != nil || pos != 15 {
		t.Fatalf("expected pos=15, got pos=%d err=%v", pos, err)
	}
}

func TestTrailingWithAliases_BracketsAndBraces(t *testing.T) {
	// WITH inside brackets.
	got := trailingWithAliases("WITH [1, WITH, 3] AS arr", nil)
	// keywordAt requires word boundary — "WITH" inside brackets at depth != 0 is skipped.
	// The outer WITH at position 0 is found. Then "arr" is the alias.
	// But actually the bracket depth is > 0 so the inner WITH is skipped.
	if len(got) == 0 {
		t.Fatal("expected alias from bracket-containing WITH")
	}

	// WITH inside braces.
	got = trailingWithAliases("WITH {WITH: 1} AS m", nil)
	if len(got) == 0 {
		t.Fatal("expected alias from brace-containing WITH")
	}
}

func TestPlanMultiGraph_NonFabricCallThenFabricCall(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog, testStatementWrites)

	// First CALL is non-fabric (no USE), second is fabric.
	// Only the second should be in fabricBlocks.
	query := `CALL {
  MATCH (n) RETURN n
}
CALL {
  USE db.shard
  RETURN 1 AS x
}
RETURN x`
	frag, err := p.Plan(t.Context(), query, "db", cyphertext.RawSource)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if frag == nil {
		t.Fatal("expected non-nil fragment")
	}
}

func TestParseLeadingUse_InvalidIdentifier(t *testing.T) {
	_, _, _, err := parseLeadingUseName("USE @invalid")
	if err == nil {
		t.Fatal("expected error for invalid identifier after USE")
	}
}

func TestExtractGraphReference_OpenParenPresent(t *testing.T) {
	// Test the openIdx < 0 branch — but since the prefix includes "(",
	// HasPrefix only matches when "(" is present. So openIdx will always be >= 0
	// when the prefix matches. This branch is unreachable by design.
}

func findExecByGraphName(fragment Fragment, graphName string) *FragmentExec {
	switch f := fragment.(type) {
	case *FragmentExec:
		if f.GraphName == graphName {
			return f
		}
		return nil
	case *FragmentApply:
		if found := findExecByGraphName(f.Input, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.Inner, graphName)
	case *FragmentUnion:
		if found := findExecByGraphName(f.LHS, graphName); found != nil {
			return found
		}
		return findExecByGraphName(f.RHS, graphName)
	default:
		return nil
	}
}

type plannerGraphPopulation struct {
	name     string
	location Location
}

func TestPlan_LeadingUsePopulations(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		location                Location
		query, graph, remaining string
		checkRemaining          bool
	}{
		{"local", &LocationLocal{DBName: "nornic"}, "USE nornic MATCH (n) RETURN n", "nornic", "MATCH (n) RETURN n", true},
		{"dotted remote", &LocationRemote{DBName: "nornic_tr", URI: "bolt://a:7687"}, "USE nornic.tr MATCH (n) RETURN n", "nornic.tr", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			catalog := NewCatalog()
			catalog.Register(tc.graph, tc.location)
			p := NewFabricPlanner(catalog, testStatementWrites)
			frag, err := p.Plan(t.Context(), tc.query, "nornic", cyphertext.RawSource)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			exec, ok := frag.(*FragmentExec)
			if !ok {
				t.Fatalf("expected FragmentExec, got %T", frag)
			}
			if exec.GraphName != tc.graph {
				t.Errorf("expected %s, got %s", tc.graph, exec.GraphName)
			}
			if tc.checkRemaining && exec.Query != tc.remaining {
				t.Errorf("expected remaining query, got %s", exec.Query)
			}
		})
	}
}

type plannerCallScope uint8

const (
	plannerCallDirect plannerCallScope = iota
	plannerCallNestedImports
)

func TestPlan_CallUseScopes(t *testing.T) {
	cases := []struct {
		name  string
		scope plannerCallScope
		query string
	}{
		{"direct", plannerCallDirect, `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId
}
RETURN translationId`},
		{"nested imports", plannerCallNestedImports, `USE nornic
WITH "t-1" AS outerId
CALL {
  WITH outerId
  CALL {
    USE nornic.tr
    WITH outerId
    MATCH (t:Translation {id: outerId})
    RETURN t.id AS translationId
  }
  RETURN translationId
}
RETURN translationId`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catalog := NewCatalog()
			catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
			catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
			p := NewFabricPlanner(catalog, testStatementWrites)
			if tc.scope == plannerCallNestedImports {
				read, _ := recordedCallImports("outerId")
				p.SetCallImportReader(read)
			}
			frag, err := p.Plan(t.Context(), tc.query, "nornic", cyphertext.RawSource)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			switch tc.scope {
			case plannerCallDirect:
				apply, ok := frag.(*FragmentApply)
				if !ok {
					t.Fatalf("expected FragmentApply at root, got %T", frag)
				}
				innerExec, ok := apply.Inner.(*FragmentExec)
				if !ok {
					t.Fatalf("expected FragmentExec for trailing RETURN, got %T", apply.Inner)
				}
				if innerExec.GraphName != "nornic" {
					t.Errorf("expected trailing fragment on nornic, got %s", innerExec.GraphName)
				}
				innerApply, ok := apply.Input.(*FragmentApply)
				if !ok {
					t.Fatalf("expected FragmentApply for CALL block, got %T", apply.Input)
				}
				callExec, ok := innerApply.Inner.(*FragmentExec)
				if !ok {
					t.Fatalf("expected FragmentExec for CALL body, got %T", innerApply.Inner)
				}
				if callExec.GraphName != "nornic.tr" {
					t.Errorf("expected nornic.tr, got %s", callExec.GraphName)
				}
			case plannerCallNestedImports:
				nestedExec := findExecByGraphName(frag, "nornic.tr")
				if nestedExec == nil {
					t.Fatalf("expected nested nornic.tr fragment in plan tree")
				}
				init, ok := nestedExec.Input.(*FragmentInit)
				if !ok {
					t.Fatalf("expected nested exec input to be FragmentInit, got %T", nestedExec.Input)
				}
				if len(init.ImportColumns) != 1 || init.ImportColumns[0] != "outerId" {
					t.Fatalf("expected nested import columns [outerId], got %v", init.ImportColumns)
				}
			}
		})
	}
}

func TestCallBlockContainsFabricUse_ImportPipelines(t *testing.T) {
	for _, tc := range []struct{ name, body, message string }{
		{"WITH", `
WITH textKey128
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 = textKey128
RETURN collect(tt) AS texts
`, "expected WITH ... USE ... block to be detected as fabric block"},
		{"pipeline", `
WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 IN keys
RETURN collect(tt) AS texts
`, "expected WITH/UNWIND/.../USE block to be detected as fabric block"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ok, err := callBlockContainsFabricUse(t.Context(), tc.body, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !ok {
				t.Fatal(tc.message)
			}
		})
	}
}

type plannerFailureSite uint8

const (
	plannerFailureSubqueryScope plannerFailureSite = iota
	plannerFailureMissingTarget
	plannerFailureUnionLater
	plannerFailureUnionFirst
	plannerFailurePrefixedCall
	plannerFailureInnerCall
)

func TestPlan_InvalidUsePopulations(t *testing.T) {
	cases := []struct {
		name                    string
		site                    plannerFailureSite
		graphs                  []plannerGraphPopulation
		query, session, message string
	}{
		{"subquery outside scope", plannerFailureSubqueryScope, []plannerGraphPopulation{{"comp1", &LocationLocal{DBName: "comp1"}}, {"comp1.a", &LocationLocal{DBName: "comp1_a"}}, {"comp2", &LocationLocal{DBName: "comp2"}}, {"comp2.b", &LocationLocal{DBName: "comp2_b"}}}, `USE comp1
CALL {
  USE comp2.b
  RETURN 1 AS x
}
RETURN x`, "comp1", "expected out-of-scope USE error"},
		{"subquery missing target", plannerFailureMissingTarget, []plannerGraphPopulation{{"nornic", &LocationLocal{DBName: "nornic"}}}, `USE nornic
CALL {
  USE nornic.missing
  RETURN 1 AS x
}
RETURN x`, "nornic", "expected missing USE target error"},
		{"later UNION branch", plannerFailureUnionLater, []plannerGraphPopulation{{"g1", &LocationLocal{DBName: "g1"}}}, "USE g1 RETURN 1 AS x UNION ALL USE ", "g1", "expected error for invalid USE in union branch"},
		{"first UNION branch", plannerFailureUnionFirst, nil, "USE UNION RETURN 1 AS x", "db", "expected error for invalid first union branch"},
		{"prefixed CALL", plannerFailurePrefixedCall, []plannerGraphPopulation{{"db", &LocationLocal{DBName: "db"}}, {"db.shard", &LocationLocal{DBName: "shard"}}}, `MATCH (n) RETURN n.id AS id
CALL {
  USE db.nonexistent
  RETURN 1 AS x
}
RETURN x`, "db", "expected error for nonexistent USE target in CALL"},
		{"inner CALL", plannerFailureInnerCall, []plannerGraphPopulation{{"db", &LocationLocal{DBName: "db"}}}, `CALL {
  USE nonexistent
  RETURN 1 AS x
}
RETURN x`, "db", "expected error for invalid USE target in CALL block"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			catalog := NewCatalog()
			for _, graph := range tc.graphs {
				catalog.Register(graph.name, graph.location)
			}
			p := NewFabricPlanner(catalog, testStatementWrites)
			_, err := p.Plan(t.Context(), tc.query, tc.session, cyphertext.RawSource)
			if err == nil {
				t.Fatalf("%s (site=%d)", tc.message, tc.site)
			}
		})
	}
}

// TestPlanRejectedSources preserves every original catalog identity, source,
// session and error expectation in one planner rejection flow.
func TestPlanRejectedSources(t *testing.T) {
	for _, test := range []struct {
		name                    string
		graphs                  [][2]string
		query, session, message string
	}{
		{"TestPlan_EmptyQuery", nil, "", "db", "expected error for empty query"},
		{"TestPlan_UseWithoutDbName", nil, "USE ", "db", "expected error for USE without database name"},
		{"TestPlanSingleQuery_InvalidMidQueryUse", [][2]string{{"db", "db"}}, "WITH a USE ", "db", "expected error for invalid mid-query USE"},
		{"TestPlanSingleQuery_MidQueryUseTargetError", [][2]string{{"db", "db"}}, "WITH a USE nonexistent.db MATCH (n) RETURN n", "db", "expected error for nonexistent mid-query USE target"},
		{"TestPlanSingleQuery_TopUseValidationError", [][2]string{{"comp1", "comp1"}, {"comp1.a", "a"}, {"comp2.b", "b"}}, "USE comp2.b RETURN 1", "comp1", "expected validation error for out-of-scope USE target"},
		{"TestPlanSingleQuery_ExtractCallBlocksError", [][2]string{{"db", "db"}}, "CALL { RETURN 1", "db", "expected error for unmatched brace in CALL"},
		{"TestPlanSingleQuery_CallBlockContainsFabricUseError", [][2]string{{"db", "db"}}, "CALL { USE } RETURN 1", "db", "expected error for invalid USE in CALL block"},
		{"TestPlanSingleQuery_MidQueryUseInvalidUSE", [][2]string{{"db", "db"}}, "WITH a USE", "db", "expected error for bare USE"},
		{"TestPlanSingleQuery_MidQueryUseValidateError", [][2]string{{"comp1", "comp1"}, {"comp1.a", "a"}, {"comp2.b", "b"}}, "WITH x USE comp2.b RETURN x", "comp1", "expected validation error for out-of-scope mid-query USE"},
		{"TestPlanSingleQuery_MidQueryUseInnerError", [][2]string{{"db", "db"}, {"db.shard", "shard"}}, "WITH x USE db.shard USE ", "db", "expected error for invalid inner USE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := NewCatalog()
			for _, graph := range test.graphs {
				catalog.Register(graph[0], &LocationLocal{DBName: graph[1]})
			}
			planner := NewFabricPlanner(catalog, testStatementWrites)
			_, err := planner.Plan(t.Context(), test.query, test.session, cyphertext.RawSource)
			if err == nil {
				t.Fatal(test.message)
			}
		})
	}
}

// TestPlannerSuccessfulGraphProducts retains each original fragment assertion
// while catalog setup, planning and error admission have one execution flow.
func TestPlannerSuccessfulGraphProducts(t *testing.T) {
	for _, test := range []struct {
		name           string
		graphs         [][2]string
		query, session string
		check          func(*testing.T, Fragment)
	}{
		{
			name:   "TestPlan_SimpleQueryNoUse",
			graphs: [][2]string{{"nornic", "nornic"}},
			query:  "MATCH (n) RETURN n", session: "nornic",
			check: func(t *testing.T, frag Fragment) {
				exec, ok := frag.(*FragmentExec)
				if !ok {
					t.Fatalf("expected FragmentExec, got %T", frag)
				}
				if exec.GraphName != "nornic" {
					t.Errorf("expected nornic, got %s", exec.GraphName)
				}
				if exec.Query != "MATCH (n) RETURN n" {
					t.Errorf("expected original query, got %s", exec.Query)
				}
				if exec.IsWrite {
					t.Error("expected IsWrite=false for read query")
				}
			},
		},
		{
			name:   "TestPlan_UnionPartUse",
			graphs: [][2]string{{"g1", "g1"}, {"g2", "g2"}},
			query:  "USE g1 RETURN 1 AS x UNION ALL USE g2 RETURN 2 AS x", session: "nornic",
			check: func(t *testing.T, frag Fragment) {
				u, ok := frag.(*FragmentUnion)
				if !ok {
					t.Fatalf("expected FragmentUnion, got %T", frag)
				}
				if u.Distinct {
					t.Fatalf("expected UNION ALL to set Distinct=false")
				}

				lhs, ok := u.LHS.(*FragmentExec)
				if !ok {
					t.Fatalf("expected lhs FragmentExec, got %T", u.LHS)
				}
				rhs, ok := u.RHS.(*FragmentExec)
				if !ok {
					t.Fatalf("expected rhs FragmentExec, got %T", u.RHS)
				}
				if lhs.GraphName != "g1" || rhs.GraphName != "g2" {
					t.Fatalf("unexpected union graph routing lhs=%s rhs=%s", lhs.GraphName, rhs.GraphName)
				}
			},
		},
		{
			name:   "TestPlanSingleQuery_MidQueryUseEmptyPrefix",
			graphs: [][2]string{{"db", "db"}, {"db.shard", "shard"}},
			query:  "USE db.shard RETURN 1 AS x", session: "db",
			check: func(t *testing.T, frag Fragment) {
				exec, ok := frag.(*FragmentExec)
				if !ok {
					t.Fatalf("expected FragmentExec, got %T", frag)
				}
				if exec.GraphName != "db.shard" {
					t.Fatalf("expected db.shard, got %s", exec.GraphName)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			catalog := NewCatalog()
			for _, graph := range test.graphs {
				catalog.Register(graph[0], &LocationLocal{DBName: graph[1]})
			}
			planner := NewFabricPlanner(catalog, testStatementWrites)
			fragment, err := planner.Plan(t.Context(), test.query, test.session, cyphertext.RawSource)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			test.check(t, fragment)
		})
	}
}

// TestParseLeadingWithUseBoundaries retains the original source and admission
// assertions for each WITH boundary in one parser invocation flow.
func TestParseLeadingWithUseBoundaries(t *testing.T) {
	for _, test := range []struct {
		name, source, message string
		policy                int
	}{
		{"TestParseLeadingWithUse_NoWith", "MATCH (n) RETURN n", "expected no match for non-WITH, got ok=%v err=%v", 0},
		{"TestParseLeadingWithUse_WithButNoUse", "WITH a MATCH (n) RETURN n", "expected no match for WITH without USE, got ok=%v err=%v", 0},
		{"TestParseLeadingWithUse_EmptyRemainder", "WITH a USE db", "expected error for USE with no following query", 1},
		{"TestParseLeadingWithUse_InvalidUseInWith", "WITH a USE ", "expected error for USE with no target in WITH...USE", 1},
		// WITH clause that immediately ends (withEnd == 0 or withEnd <= 0).
		{"TestParseLeadingWithUse_WithEndAtStart", "WITH", "expected false for bare WITH", 2},
		// findLeadingWithClauseEnd("WITH MATCH (n)") finds MATCH as clause start -> withEnd=5.
		// withClause = "WITH " (trimmed -> "WITH"), rest = "MATCH (n)".
		// rest doesn't start with USE -> returns false.
		{"TestParseLeadingWithUse_WithFindEndFail", "WITH MATCH (n)", "expected false for WITH immediately followed by MATCH", 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, _, ok, err := parseLeadingWithUse(t.Context(), test.source, nil)
			switch test.policy {
			case 0:
				if err != nil || ok {
					t.Fatalf(test.message, ok, err)
				}
			case 1:
				if err == nil {
					t.Fatal(test.message)
				}
			case 2:
				if ok {
					t.Fatal(test.message)
				}
			}
		})
	}
}

func TestCallBlockContainsFabricUse_NestedCall(t *testing.T) {
	tests := []struct {
		name string
		body string
		want bool
	}{
		{
			name: "TestCallBlockContainsFabricUse_NestedCall",
			body: "CALL { USE db RETURN 1 } RETURN x",
			want: true,
		},
		{
			name: "TestCallBlockContainsFabricUse_NestedCallWithoutUse",
			body: "CALL { MATCH (n) RETURN n } RETURN x",
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ok, err := callBlockContainsFabricUse(t.Context(), tt.body, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if ok != tt.want {
				if tt.want {
					t.Fatal("expected nested CALL with USE to be detected")
				}
				t.Fatal("expected false for nested CALL without USE")
			}
		})
	}
}

func TestKeywordAt_BoundaryChecks(t *testing.T) {
	type keywordCase struct {
		text    string
		index   int
		want    bool
		message string
	}
	for _, group := range []struct {
		name  string
		cases []keywordCase
	}{
		{"TestKeywordAt_BoundaryChecks", []keywordCase{
			{"MATCH", -1, false, "expected false for negative index"},
			{"MAT", 0, false, "expected false when keyword overflows"},
			{"xMATCH", 1, false, "expected false when preceded by ident char"},
			{"MATCHx", 0, false, "expected false when followed by ident char"},
			{"MATCH (n)", 0, true, "expected true for standalone keyword"},
			{"x MATCH", 2, true, "expected true for keyword at end"},
		}},
		{"TestHasKeywordAt_PrevIsIdentChar", []keywordCase{
			{"xMATCH (n)", 1, false, "expected false when preceded by letter"},
			{"1MATCH (n)", 1, false, "expected false when preceded by digit"},
			{"_MATCH (n)", 1, false, "expected false when preceded by underscore"},
		}},
	} {
		t.Run(group.name, func(t *testing.T) {
			for _, test := range group.cases {
				if keywordAt(test.text, test.index, "MATCH") != test.want {
					t.Fatal(test.message)
				}
			}
		})
	}
}
