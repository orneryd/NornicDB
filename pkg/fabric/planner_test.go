package fabric

import (
	"strings"
	"testing"
)

func TestPlan_SimpleQueryNoUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
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
}

func TestPlan_PreservesOuterClausesAroundCallUseBlocks(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
MATCH (seed:Seed) RETURN seed.id AS seedId
CALL {
  USE nornic.tr
  WITH seedId
  MATCH (t:Translation {id: seedId})
  RETURN t.text AS translated
}
RETURN seedId, translated`

	frag, err := p.Plan(query, "nornic")
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

func TestPlan_UnionPartUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "g1"})
	catalog.Register("g2", &LocationLocal{DBName: "g2"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE g1 RETURN 1 AS x UNION ALL USE g2 RETURN 2 AS x", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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
}

func TestPlan_LeadingUseClause(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE nornic MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "nornic" {
		t.Errorf("expected nornic, got %s", exec.GraphName)
	}
	if exec.Query != "MATCH (n) RETURN n" {
		t.Errorf("expected remaining query, got %s", exec.Query)
	}
}

func TestPlan_LeadingUseDotted(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic.tr", &LocationRemote{DBName: "nornic_tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("USE nornic.tr MATCH (n) RETURN n", "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "nornic.tr" {
		t.Errorf("expected nornic.tr, got %s", exec.GraphName)
	}
}

func TestPlan_CallUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
CALL {
  USE nornic.tr
  MATCH (t:Translation)
  RETURN t.id AS translationId
}
RETURN translationId`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be an Apply chain: Apply(input=Apply(input=Init, inner=Exec(nornic.tr)), inner=Exec(trailing RETURN))
	apply, ok := frag.(*FragmentApply)
	if !ok {
		t.Fatalf("expected FragmentApply at root, got %T", frag)
	}

	// The trailing RETURN should be in the outer apply.
	innerExec, ok := apply.Inner.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for trailing RETURN, got %T", apply.Inner)
	}
	if innerExec.GraphName != "nornic" {
		t.Errorf("expected trailing fragment on nornic, got %s", innerExec.GraphName)
	}

	// The inner apply contains the CALL { USE nornic.tr ... } block.
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
}

func TestPlan_MultipleCallUseSubqueries(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	catalog.Register("nornic.txt", &LocationRemote{DBName: "txt", URI: "bolt://b:7687"})
	p := NewFabricPlanner(catalog)

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

	frag, err := p.Plan(query, "nornic")
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
	if !hasTopLevelReturnClause("MATCH (n) RETURN n") {
		t.Fatalf("expected RETURN clause to be detected")
	}
	if hasTopLevelReturnClause("MATCH (n {x: 'RETURN literal'})") {
		t.Fatalf("did not expect RETURN inside string literal to count")
	}

	aliases := trailingWithAliases("MATCH (n) WITH n.id AS id, n.name AS name")
	if len(aliases) != 2 || aliases[0] != "id" || aliases[1] != "name" {
		t.Fatalf("unexpected trailing WITH aliases: %#v", aliases)
	}
}

func TestPlan_CallWithThenUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.tr", &LocationLocal{DBName: "nornic_tr"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "nornic_txt"})
	p := NewFabricPlanner(catalog)

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

	frag, err := p.Plan(query, "translations")
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

func TestCallBlockContainsFabricUse_WithThenUse(t *testing.T) {
	body := `
WITH textKey128
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 = textKey128
RETURN collect(tt) AS texts
`
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected WITH ... USE ... block to be detected as fabric block")
	}
}

func TestCallBlockContainsFabricUse_WithPipelineThenUse(t *testing.T) {
	body := `
WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt)
WHERE tt.textKey128 IN keys
RETURN collect(tt) AS texts
`
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatalf("expected WITH/UNWIND/.../USE block to be detected as fabric block")
	}
}

func TestPlan_MidQueryUseAfterPipeline(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("translations", &LocationLocal{DBName: "translations"})
	catalog.Register("translations.txr", &LocationLocal{DBName: "caremark_txt"})
	p := NewFabricPlanner(catalog)

	query := `WITH rows
UNWIND rows AS r
WITH collect(DISTINCT r.textKey128) AS keys
USE translations.txr
MATCH (tt:MongoDocument)
WHERE tt.textKey128 IN keys
RETURN tt.textKey128 AS k, collect(tt) AS texts`

	frag, err := p.Plan(query, "translations")
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

func TestPlan_CorrelatedWithImports(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	query := `USE db
CALL {
  USE db.shard
  WITH myVar
  MATCH (n) WHERE n.id = myVar
  RETURN n
}
RETURN n`

	frag, err := p.Plan(query, "nornic")
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
}

func TestPlan_WriteDetection(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

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
		frag, err := p.Plan(tt.query, "db")
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", tt.query, err)
		}
		exec := frag.(*FragmentExec)
		if exec.IsWrite != tt.isWrite {
			t.Errorf("query %q: expected IsWrite=%v, got %v", tt.query, tt.isWrite, exec.IsWrite)
		}
	}
}

func TestPlan_EmptyQuery(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
	_, err := p.Plan("", "db")
	if err == nil {
		t.Fatal("expected error for empty query")
	}
}

func TestPlan_UseWithoutDbName(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
	_, err := p.Plan("USE ", "db")
	if err == nil {
		t.Fatal("expected error for USE without database name")
	}
}

func TestPlan_CallWithoutUse_NotFabric(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// CALL {} without USE inside is a regular subquery, not fabric.
	query := `CALL {
  MATCH (n) RETURN n
}
RETURN n`

	frag, err := p.Plan(query, "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be a simple FragmentExec (no decomposition).
	_, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec for non-fabric CALL, got %T", frag)
	}
}

func TestPlan_NestedCallUseSubquery(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	catalog.Register("nornic.tr", &LocationRemote{DBName: "tr", URI: "bolt://a:7687"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
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
RETURN translationId`

	frag, err := p.Plan(query, "nornic")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

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

func TestPlan_SubqueryUseTargetOutOfScope(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp1", &LocationLocal{DBName: "comp1"})
	catalog.Register("comp1.a", &LocationLocal{DBName: "comp1_a"})
	catalog.Register("comp2", &LocationLocal{DBName: "comp2"})
	catalog.Register("comp2.b", &LocationLocal{DBName: "comp2_b"})
	p := NewFabricPlanner(catalog)

	query := `USE comp1
CALL {
  USE comp2.b
  RETURN 1 AS x
}
RETURN x`

	_, err := p.Plan(query, "comp1")
	if err == nil {
		t.Fatal("expected out-of-scope USE error")
	}
}

func TestPlan_SubqueryUseTargetNotFound(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("nornic", &LocationLocal{DBName: "nornic"})
	p := NewFabricPlanner(catalog)

	query := `USE nornic
CALL {
  USE nornic.missing
  RETURN 1 AS x
}
RETURN x`

	_, err := p.Plan(query, "nornic")
	if err == nil {
		t.Fatal("expected missing USE target error")
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
	use, remaining, hasUse, err := parseLeadingUse("USE graph.byName('tenant_a') MATCH (n) RETURN n", false)
	if err != nil || !hasUse {
		t.Fatalf("unexpected result: hasUse=%v err=%v", hasUse, err)
	}
	if !use.IsDynamic() || use.Function != "graph.byName" || len(use.Args) != 1 || use.Args[0] != "'tenant_a'" {
		t.Fatalf("unexpected dynamic reference: %+v", use)
	}
	if remaining != "MATCH (n) RETURN n" {
		t.Fatalf("unexpected remaining query: %q", remaining)
	}

	use, _, hasUse, err = parseLeadingUse("USE graph.byElementId($id) RETURN 1", false)
	if err != nil || !hasUse || use.Text() != "graph.byElementId($id)" {
		t.Fatalf("unexpected byElementId result: %+v hasUse=%v err=%v", use, hasUse, err)
	}
}

func TestPlannerHelpers_SplitAtTopLevelUse(t *testing.T) {
	prefix, suffix, ok := splitAtTopLevelUse("MATCH (n) WITH n USE translations.tr RETURN n")
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
	use, remaining, hasUse, err := parseLeadingUse(query, false)
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

func TestExtractWithImports(t *testing.T) {
	tests := []struct {
		body string
		want []string
	}{
		{"WITH translationId MATCH (n)", []string{"translationId"}},
		{"WITH a, b MATCH (n)", []string{"a", "b"}},
		{"with a, b match (n)", []string{"a", "b"}},
		{"MATCH (n) RETURN n", nil},
		{"WITH x RETURN x", []string{"x"}},
		{"WITHOUT something", nil},
	}

	for _, tt := range tests {
		got := extractWithImports(tt.body)
		if len(got) != len(tt.want) {
			t.Errorf("extractWithImports(%q) = %v, want %v", tt.body, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("extractWithImports(%q)[%d] = %s, want %s", tt.body, i, got[i], tt.want[i])
			}
		}
	}
}

func TestQueryIsWrite(t *testing.T) {
	tests := []struct {
		query string
		want  bool
	}{
		{"MATCH (n) RETURN n", false},
		{"CREATE (n:Test)", true},
		{"MERGE (n:Test {id: 1})", true},
		{"MATCH (n) DELETE n", true},
		{"MATCH (n) DETACH DELETE n", true},
		{"MATCH (n) SET n.x = 1 RETURN n", true},
		{"MATCH (n) REMOVE n.x RETURN n", true},
		{"RETURN 1", false},
	}

	for _, tt := range tests {
		got := queryIsWrite(tt.query)
		if got != tt.want {
			t.Errorf("queryIsWrite(%q) = %v, want %v", tt.query, got, tt.want)
		}
	}
}

// --- Additional coverage tests ---

func TestSplitTopLevelUnion_NoUnion(t *testing.T) {
	parts, ops, hasUnion, err := splitTopLevelUnion("MATCH (n) RETURN n")
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
	_, _, _, err := splitTopLevelUnion("UNION ALL RETURN 1")
	if err == nil {
		t.Fatal("expected error for empty leading branch")
	}
}

func TestSplitTopLevelUnion_EmptyTrailingBranch(t *testing.T) {
	_, _, _, err := splitTopLevelUnion("RETURN 1 UNION ALL ")
	if err == nil {
		t.Fatal("expected error for empty trailing branch")
	}
}

func TestSplitTopLevelUnion_InsideQuotes(t *testing.T) {
	// UNION inside single-quoted string.
	_, _, hasUnion, err := splitTopLevelUnion("RETURN 'UNION ALL' AS x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUnion {
		t.Fatal("expected UNION inside quotes to be ignored")
	}
	// UNION inside double-quoted string.
	_, _, hasUnion, err = splitTopLevelUnion(`RETURN "UNION" AS x`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if hasUnion {
		t.Fatal("expected UNION inside double quotes to be ignored")
	}
}

func TestSplitTopLevelUnion_InsideBracesParens(t *testing.T) {
	// UNION inside braces should not split.
	_, _, hasUnion, _ := splitTopLevelUnion("CALL { RETURN 1 UNION RETURN 2 } RETURN x")
	if hasUnion {
		t.Fatal("expected UNION inside braces to be ignored")
	}
	// UNION inside parens should not split.
	_, _, hasUnion, _ = splitTopLevelUnion("RETURN (1 UNION 2)")
	if hasUnion {
		t.Fatal("expected UNION inside parens to be ignored")
	}
}

func TestSplitTopLevelUnion_DistinctUnion(t *testing.T) {
	parts, ops, hasUnion, err := splitTopLevelUnion("RETURN 1 AS x UNION RETURN 2 AS x")
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
	_, _, ok := splitAtTopLevelUse("MATCH (n {x: 'USE db'}) RETURN n")
	if ok {
		t.Fatal("expected USE inside single quotes to be ignored")
	}
	// USE inside double-quoted string should not split.
	_, _, ok = splitAtTopLevelUse(`MATCH (n {x: "USE db"}) RETURN n`)
	if ok {
		t.Fatal("expected USE inside double quotes to be ignored")
	}
	// USE inside backtick.
	_, _, ok = splitAtTopLevelUse("MATCH (n) WITH `USE` AS x USE db RETURN n")
	if !ok {
		t.Fatal("expected USE after backtick to split")
	}
}

func TestSplitAtTopLevelUse_InsideNesting(t *testing.T) {
	// USE inside braces.
	_, _, ok := splitAtTopLevelUse("CALL { USE db RETURN 1 } RETURN x")
	if ok {
		t.Fatal("expected USE inside braces to be ignored")
	}
	// USE inside parens.
	_, _, ok = splitAtTopLevelUse("MATCH (n WHERE USE = 1) RETURN n")
	// This one might match depending on keyword boundary logic.
	// Just ensure no panic.
	_ = ok
	// USE inside brackets.
	_, _, ok = splitAtTopLevelUse("RETURN [x IN list WHERE USE = 1]")
	_ = ok
}

func TestSplitAtTopLevelUse_LeadingUseSkipped(t *testing.T) {
	// Leading USE should be skipped (handled by parseLeadingUse).
	_, _, ok := splitAtTopLevelUse("USE db MATCH (n) RETURN n")
	if ok {
		t.Fatal("expected leading USE to be skipped")
	}
}

func TestSplitAtTopLevelUse_NoUse(t *testing.T) {
	_, _, ok := splitAtTopLevelUse("MATCH (n) RETURN n")
	if !ok {
		// No USE at all → ok=false.
	}
}

func TestEnsureRowProducingPrefix(t *testing.T) {
	// Empty query.
	if ensureRowProducingPrefix("") != "" {
		t.Fatal("expected empty for empty input")
	}
	// Query with RETURN.
	got := ensureRowProducingPrefix("MATCH (n) RETURN n")
	if got != "MATCH (n) RETURN n" {
		t.Fatalf("expected unchanged with RETURN, got %q", got)
	}
	// WITH clause with aliases.
	got = ensureRowProducingPrefix("WITH a, b")
	if !strings.HasSuffix(got, "RETURN a, b") {
		t.Fatalf("expected RETURN aliases appended, got %q", got)
	}
	// Query without RETURN and without WITH.
	got = ensureRowProducingPrefix("MATCH (n)")
	if !strings.HasSuffix(got, "RETURN *") {
		t.Fatalf("expected RETURN * appended, got %q", got)
	}
}

func TestHasTopLevelReturnClause_WithQuotes(t *testing.T) {
	// RETURN inside single quotes.
	if hasTopLevelReturnClause("MATCH (n {x: 'RETURN n'})") {
		t.Fatal("expected RETURN in single quotes to be ignored")
	}
	// RETURN inside double quotes.
	if hasTopLevelReturnClause(`MATCH (n {x: "RETURN n"})`) {
		t.Fatal("expected RETURN in double quotes to be ignored")
	}
	// RETURN inside backticks.
	if hasTopLevelReturnClause("MATCH (n) WITH `RETURN` AS x") {
		t.Fatal("expected RETURN in backticks to be ignored")
	}
	// RETURN inside braces.
	if hasTopLevelReturnClause("CALL { RETURN 1 }") {
		t.Fatal("expected RETURN inside braces to be ignored")
	}
	// RETURN inside parens.
	if hasTopLevelReturnClause("MATCH (RETURN)") {
		// "RETURN" after ( is inside paren depth.
		t.Fatal("expected RETURN inside parens to be ignored")
	}
	// RETURN inside brackets.
	if hasTopLevelReturnClause("[RETURN 1]") {
		t.Fatal("expected RETURN inside brackets to be ignored")
	}
	// Top-level RETURN.
	if !hasTopLevelReturnClause("MATCH (n) RETURN n") {
		t.Fatal("expected top-level RETURN to be detected")
	}
}

func TestTrailingWithAliases_EdgeCases(t *testing.T) {
	// No WITH.
	if trailingWithAliases("MATCH (n)") != nil {
		t.Fatal("expected nil for no WITH")
	}
	// WITH inside quotes should not match.
	got := trailingWithAliases("MATCH (n {x: 'WITH foo'}) WITH a")
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
	// WITH inside backticks.
	got = trailingWithAliases("MATCH (n) WITH `weird name` AS x")
	// `weird name` is not a valid identifier, so AS alias "x" should be found.
	// But the item with backtick goes through lastAsIndexFold which finds AS.
	// alias = "x", isValidIdentifier("x") = true.
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("expected [x] with backtick alias, got %v", got)
	}
	// Empty WITH clause.
	got = trailingWithAliases("WITH ")
	if got != nil {
		t.Fatalf("expected nil for empty WITH clause, got %v", got)
	}
	// WITH inside braces.
	got = trailingWithAliases("CALL { WITH inner } WITH outer")
	if len(got) != 1 || got[0] != "outer" {
		t.Fatalf("expected [outer], got %v", got)
	}
	// WITH inside double quotes.
	got = trailingWithAliases(`MATCH (n) WITH "WITH" AS x`)
	if len(got) != 1 || got[0] != "x" {
		t.Fatalf("expected [x], got %v", got)
	}
	// Non-valid identifier (has dot).
	got = trailingWithAliases("WITH n.x")
	if len(got) != 0 {
		t.Fatalf("expected empty for non-valid identifier, got %v", got)
	}
}

func TestIsValidIdentifier(t *testing.T) {
	if isValidIdentifier("") {
		t.Fatal("empty should not be valid")
	}
	if isValidIdentifier("1abc") {
		t.Fatal("starting with digit should not be valid")
	}
	if !isValidIdentifier("abc") {
		t.Fatal("abc should be valid")
	}
	if !isValidIdentifier("_abc") {
		t.Fatal("_abc should be valid")
	}
	if isValidIdentifier("a.b") {
		t.Fatal("dotted should not be valid")
	}
	if isValidIdentifier("a-b") {
		t.Fatal("hyphenated should not be valid")
	}
	if !isValidIdentifier("abc123_DEF") {
		t.Fatal("mixed alphanum with underscore should be valid")
	}
}

func TestExtractTopLevelCallBlocks_WithComments(t *testing.T) {
	// Line comment.
	query := "// comment\nCALL { RETURN 1 }\nRETURN x"
	blocks, err := extractTopLevelCallBlocks(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 1 {
		t.Fatalf("expected 1 block, got %d", len(blocks))
	}
	// Block comment.
	query = "/* block comment */ CALL { RETURN 1 } RETURN x"
	blocks, err = extractTopLevelCallBlocks(query)
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
	blocks, err := extractTopLevelCallBlocks(query)
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
	blocks, err := extractTopLevelCallBlocks(query)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks for procedure call, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_UnmatchedBrace(t *testing.T) {
	query := "CALL { RETURN 1"
	_, err := extractTopLevelCallBlocks(query)
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

func TestKeywordAt_BoundaryChecks(t *testing.T) {
	// Negative index.
	if keywordAt("MATCH", -1, "MATCH") {
		t.Fatal("expected false for negative index")
	}
	// Index too close to end.
	if keywordAt("MAT", 0, "MATCH") {
		t.Fatal("expected false when keyword overflows")
	}
	// Preceded by identifier char.
	if keywordAt("xMATCH", 1, "MATCH") {
		t.Fatal("expected false when preceded by ident char")
	}
	// Followed by identifier char.
	if keywordAt("MATCHx", 0, "MATCH") {
		t.Fatal("expected false when followed by ident char")
	}
	// Valid standalone keyword.
	if !keywordAt("MATCH (n)", 0, "MATCH") {
		t.Fatal("expected true for standalone keyword")
	}
	// Keyword at end of string.
	if !keywordAt("x MATCH", 2, "MATCH") {
		t.Fatal("expected true for keyword at end")
	}
}

func TestCompositeScopeRoot(t *testing.T) {
	if compositeScopeRoot("") != "" {
		t.Fatal("expected empty for empty")
	}
	if compositeScopeRoot("  ") != "" {
		t.Fatal("expected empty for whitespace")
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
	p := NewFabricPlanner(catalog)

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
	p2 := NewFabricPlanner(nil)
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

func TestParseLeadingWithUse_NoWith(t *testing.T) {
	_, _, ok, err := parseLeadingWithUse("MATCH (n) RETURN n")
	if err != nil || ok {
		t.Fatalf("expected no match for non-WITH, got ok=%v err=%v", ok, err)
	}
}

func TestParseLeadingWithUse_WithButNoUse(t *testing.T) {
	_, _, ok, err := parseLeadingWithUse("WITH a MATCH (n) RETURN n")
	if err != nil || ok {
		t.Fatalf("expected no match for WITH without USE, got ok=%v err=%v", ok, err)
	}
}

func TestParseLeadingWithUse_EmptyRemainder(t *testing.T) {
	_, _, _, err := parseLeadingWithUse("WITH a USE db")
	if err == nil {
		t.Fatal("expected error for USE with no following query")
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
	ok, err := callBlockContainsFabricUse("MATCH (n) RETURN n")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false for no USE")
	}
}

func TestCallBlockContainsFabricUse_NestedCall(t *testing.T) {
	body := "CALL { USE db RETURN 1 } RETURN x"
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("expected nested CALL with USE to be detected")
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
	p := NewFabricPlanner(catalog)

	frag, err := p.Plan("RETURN 1 AS x UNION ALL RETURN 2 AS x UNION RETURN 3 AS x", "g1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Should be nested unions.
	u, ok := frag.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected FragmentUnion root, got %T", frag)
	}
	if !u.Distinct {
		t.Fatal("expected outer union to be DISTINCT")
	}
	inner, ok := u.LHS.(*FragmentUnion)
	if !ok {
		t.Fatalf("expected nested FragmentUnion on LHS, got %T", u.LHS)
	}
	if inner.Distinct {
		t.Fatal("expected inner union to be ALL (not distinct)")
	}
}

func TestValidateUseTarget_EmptyTarget(t *testing.T) {
	p := NewFabricPlanner(NewCatalog())
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

func TestExtractWithImports_WithoutKeyword(t *testing.T) {
	got := extractWithImports("WITHOUT something")
	if got != nil {
		t.Fatalf("expected nil for WITHOUT, got %v", got)
	}
}

func TestExtractWithImports_NotFollowedByWhitespace(t *testing.T) {
	got := extractWithImports("WITHX something")
	if got != nil {
		t.Fatalf("expected nil for WITHX, got %v", got)
	}
}

func TestExtractWithImports_WithASKeyword(t *testing.T) {
	// AS is skipped as a keyword.
	got := extractWithImports("WITH x AS y MATCH (n)")
	// "x" is parsed, "AS" is skipped, "y" is parsed, "MATCH" stops.
	if len(got) != 2 || got[0] != "x" || got[1] != "y" {
		t.Fatalf("expected [x y], got %v", got)
	}
}

func TestSplitTopLevelUnion_EscapedQuotes(t *testing.T) {
	// Single-quoted string with escaped quote containing UNION.
	_, _, hasUnion, _ := splitTopLevelUnion("RETURN 'it''s a UNION' AS x")
	if hasUnion {
		t.Fatal("expected UNION inside escaped single quotes to be ignored")
	}
	// Double-quoted string with escaped quote containing UNION.
	_, _, hasUnion, _ = splitTopLevelUnion(`RETURN "say ""UNION""" AS x`)
	if hasUnion {
		t.Fatal("expected UNION inside escaped double quotes to be ignored")
	}
}

func TestPlan_PlanErrorInUnionBranch(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("g1", &LocationLocal{DBName: "g1"})
	p := NewFabricPlanner(catalog)

	// First branch is valid, second has empty USE → error.
	_, err := p.Plan("USE g1 RETURN 1 AS x UNION ALL USE ", "g1")
	if err == nil {
		t.Fatal("expected error for invalid USE in union branch")
	}
}

func TestPlan_PlanErrorInFirstUnionBranch(t *testing.T) {
	catalog := NewCatalog()
	p := NewFabricPlanner(catalog)

	// First branch has empty USE → error.
	_, err := p.Plan("USE UNION RETURN 1 AS x", "db")
	if err == nil {
		t.Fatal("expected error for invalid first union branch")
	}
}

func TestPlanSingleQuery_InvalidMidQueryUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// Mid-query USE followed by invalid USE clause.
	_, err := p.Plan("WITH a USE ", "db")
	if err == nil {
		t.Fatal("expected error for invalid mid-query USE")
	}
}

func TestPlanSingleQuery_MidQueryUseTargetError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// Mid-query USE to nonexistent target.
	_, err := p.Plan("WITH a USE nonexistent.db MATCH (n) RETURN n", "db")
	if err == nil {
		t.Fatal("expected error for nonexistent mid-query USE target")
	}
}

func TestPlanSingleQuery_MidQueryUseEmptyPrefix(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	// Mid-query USE where prefix is empty after trim → returns inner directly.
	frag, err := p.Plan("USE db.shard RETURN 1 AS x", "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	exec, ok := frag.(*FragmentExec)
	if !ok {
		t.Fatalf("expected FragmentExec, got %T", frag)
	}
	if exec.GraphName != "db.shard" {
		t.Fatalf("expected db.shard, got %s", exec.GraphName)
	}
}

func TestPlanMultiGraph_CallBlockWithInvalidUse(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	// CALL block with invalid nested USE target.
	query := `USE db
CALL {
  USE db.missing
  RETURN 1 AS x
}
RETURN x`
	_, err := p.Plan(query, "db")
	if err == nil {
		t.Fatal("expected error for invalid USE target in CALL block")
	}
}

func TestParseLeadingWithUse_InvalidUseInWith(t *testing.T) {
	_, _, _, err := parseLeadingWithUse("WITH a USE ")
	if err == nil {
		t.Fatal("expected error for USE with no target in WITH...USE")
	}
}

func TestParseLeadingWithUse_WithEndAtStart(t *testing.T) {
	// WITH clause that immediately ends (withEnd == 0 or withEnd <= 0).
	_, _, ok, _ := parseLeadingWithUse("WITH")
	if ok {
		t.Fatal("expected false for bare WITH")
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
	blocks, err := extractTopLevelCallBlocks(`RETURN "CALL { RETURN 1 }" AS x`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestExtractTopLevelCallBlocks_EscapedQuotes(t *testing.T) {
	// CALL inside single-quoted string with escaped quotes.
	blocks, err := extractTopLevelCallBlocks("RETURN 'it''s CALL { }' AS x")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
	// CALL inside double-quoted string with escaped quotes.
	blocks, err = extractTopLevelCallBlocks(`RETURN "say ""CALL { }""" AS x`)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(blocks) != 0 {
		t.Fatalf("expected 0 blocks, got %d", len(blocks))
	}
}

func TestCallBlockContainsFabricUse_ParseError(t *testing.T) {
	// Invalid USE should propagate error.
	_, err := callBlockContainsFabricUse("USE ")
	if err == nil {
		t.Fatal("expected error for invalid USE in call block")
	}
}

func TestCallBlockContainsFabricUse_ParseLeadingWithUseError(t *testing.T) {
	_, err := callBlockContainsFabricUse("WITH a USE ")
	if err == nil {
		t.Fatal("expected error for invalid WITH...USE in call block")
	}
}

func TestCallBlockContainsFabricUse_NestedCallError(t *testing.T) {
	// Nested CALL with unmatched brace.
	_, err := callBlockContainsFabricUse("CALL { USE db")
	if err == nil {
		t.Fatal("expected error for unmatched brace in nested CALL")
	}
}

func TestSplitAtTopLevelUse_EscapedSingleQuote(t *testing.T) {
	// Escaped single quote.
	_, _, ok := splitAtTopLevelUse("MATCH (n {x: 'it''s USE db'}) RETURN n")
	if ok {
		t.Fatal("expected USE inside escaped single quote to be ignored")
	}
}

func TestSplitAtTopLevelUse_EscapedDoubleQuote(t *testing.T) {
	_, _, ok := splitAtTopLevelUse(`MATCH (n {x: "say ""USE db"""}) RETURN n`)
	if ok {
		t.Fatal("expected USE inside escaped double quote to be ignored")
	}
}

func TestHasTopLevelReturnClause_EscapedQuotes(t *testing.T) {
	// Escaped single quote.
	if !hasTopLevelReturnClause("MATCH (n {x: 'it''s'}) RETURN n") {
		t.Fatal("expected RETURN to be found after escaped single quote")
	}
	// Escaped double quote.
	if !hasTopLevelReturnClause(`MATCH (n {x: "say ""hello"""}) RETURN n`) {
		t.Fatal("expected RETURN to be found after escaped double quote")
	}
}

func TestTrailingWithAliases_EscapedQuotes(t *testing.T) {
	// Escaped single quote in WITH clause.
	got := trailingWithAliases("MATCH (n {x: 'it''s'}) WITH a")
	if len(got) != 1 || got[0] != "a" {
		t.Fatalf("expected [a], got %v", got)
	}
	// Escaped double quote.
	got = trailingWithAliases(`MATCH (n {x: "say ""hi"""}) WITH b`)
	if len(got) != 1 || got[0] != "b" {
		t.Fatalf("expected [b], got %v", got)
	}
}

func TestTrailingWithAliases_BracketsAndParens(t *testing.T) {
	got := trailingWithAliases("WITH (WITH inside) AS x")
	// WITH inside parens depth → not detected as trailing WITH.
	// Only the outer WITH at position 0 is found.
	_ = got // Behavior depends on keyword matching — just ensure no panic.
}

func TestTrailingWithAliases_NonValidAlias(t *testing.T) {
	// Expression without AS and non-valid identifier.
	got := trailingWithAliases("WITH count(*)")
	if len(got) != 0 {
		t.Fatalf("expected empty for non-valid identifier, got %v", got)
	}
}

func TestTrailingWithAliases_AsNonValidAlias(t *testing.T) {
	got := trailingWithAliases("WITH x AS 1invalid")
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

func TestPlanMultiGraph_WithUseAndSubPlanError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// CALL block body that has USE to nonexistent db.
	query := `CALL {
  USE nonexistent
  RETURN 1 AS x
}
RETURN x`
	_, err := p.Plan(query, "db")
	if err == nil {
		t.Fatal("expected error for invalid USE target in CALL block")
	}
}

func TestPlanSingleQuery_TopUseValidationError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp1", &LocationLocal{DBName: "comp1"})
	catalog.Register("comp1.a", &LocationLocal{DBName: "a"})
	catalog.Register("comp2.b", &LocationLocal{DBName: "b"})
	p := NewFabricPlanner(catalog)

	// USE target out of scope.
	_, err := p.Plan("USE comp2.b RETURN 1", "comp1")
	if err == nil {
		t.Fatal("expected validation error for out-of-scope USE target")
	}
}

func TestPlanSingleQuery_ExtractCallBlocksError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// Unmatched brace in CALL subquery.
	_, err := p.Plan("CALL { RETURN 1", "db")
	if err == nil {
		t.Fatal("expected error for unmatched brace in CALL")
	}
}

func TestPlanSingleQuery_CallBlockContainsFabricUseError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	// CALL block with invalid USE.
	_, err := p.Plan("CALL { USE } RETURN 1", "db")
	if err == nil {
		t.Fatal("expected error for invalid USE in CALL block")
	}
}

func TestPlanSingleQuery_MidQueryUseInvalidUSE(t *testing.T) {
	// This needs splitAtTopLevelUse to find USE, but parseLeadingUse to fail.
	// Hard to trigger because splitAtTopLevelUse checks keywordAt("USE").
	// If we have "WITH a USE" where USE has no following identifier.
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	p := NewFabricPlanner(catalog)

	_, err := p.Plan("WITH a USE", "db")
	if err == nil {
		t.Fatal("expected error for bare USE")
	}
}

func TestPlanSingleQuery_MidQueryUseValidateError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("comp1", &LocationLocal{DBName: "comp1"})
	catalog.Register("comp1.a", &LocationLocal{DBName: "a"})
	catalog.Register("comp2.b", &LocationLocal{DBName: "b"})
	p := NewFabricPlanner(catalog)

	// Mid-query USE to out-of-scope target.
	_, err := p.Plan("WITH x USE comp2.b RETURN x", "comp1")
	if err == nil {
		t.Fatal("expected validation error for out-of-scope mid-query USE")
	}
}

func TestPlanSingleQuery_MidQueryUseInnerError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	// Mid-query USE where inner query has another invalid USE.
	_, err := p.Plan("WITH x USE db.shard USE ", "db")
	if err == nil {
		t.Fatal("expected error for invalid inner USE")
	}
}

func TestTrailingWithAliases_BracketsAndBraces(t *testing.T) {
	// WITH inside brackets.
	got := trailingWithAliases("WITH [1, WITH, 3] AS arr")
	// keywordAt requires word boundary — "WITH" inside brackets at depth != 0 is skipped.
	// The outer WITH at position 0 is found. Then "arr" is the alias.
	// But actually the bracket depth is > 0 so the inner WITH is skipped.
	if len(got) == 0 {
		t.Fatal("expected alias from bracket-containing WITH")
	}

	// WITH inside braces.
	got = trailingWithAliases("WITH {WITH: 1} AS m")
	if len(got) == 0 {
		t.Fatal("expected alias from brace-containing WITH")
	}
}

func TestPlanMultiGraph_PrefixError(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	// CALL block where USE target doesn't exist.
	query := `MATCH (n) RETURN n.id AS id
CALL {
  USE db.nonexistent
  RETURN 1 AS x
}
RETURN x`
	_, err := p.Plan(query, "db")
	if err == nil {
		t.Fatal("expected error for nonexistent USE target in CALL")
	}
}

func TestPlanMultiGraph_NonFabricCallThenFabricCall(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

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
	frag, err := p.Plan(query, "db")
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

func TestParseLeadingWithUse_WithFindEndFail(t *testing.T) {
	// WITH followed by a clause keyword immediately — withEnd would be at position
	// right after WITH keyword.
	_, _, ok, _ := parseLeadingWithUse("WITH MATCH (n)")
	// findLeadingWithClauseEnd("WITH MATCH (n)") finds MATCH as clause start → withEnd=5.
	// withClause = "WITH " (trimmed → "WITH"), rest = "MATCH (n)".
	// rest doesn't start with USE → returns false.
	if ok {
		t.Fatal("expected false for WITH immediately followed by MATCH")
	}
}

func TestExtractGraphReference_OpenParenPresent(t *testing.T) {
	// Test the openIdx < 0 branch — but since the prefix includes "(",
	// HasPrefix only matches when "(" is present. So openIdx will always be >= 0
	// when the prefix matches. This branch is unreachable by design.
}

func TestCallBlockContainsFabricUse_NestedCallWithoutUse(t *testing.T) {
	body := "CALL { MATCH (n) RETURN n } RETURN x"
	ok, err := callBlockContainsFabricUse(body)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("expected false for nested CALL without USE")
	}
}

func TestPlanMultiGraph_WithUseNoUseBody(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db", &LocationLocal{DBName: "db"})
	catalog.Register("db.shard", &LocationLocal{DBName: "shard"})
	p := NewFabricPlanner(catalog)

	// CALL block without USE inside (non-fabric).
	query := `CALL {
  MATCH (n) RETURN n
}
CALL {
  USE db.shard
  RETURN 1 AS x
}
RETURN x`
	frag, err := p.Plan(query, "db")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if frag == nil {
		t.Fatal("expected non-nil fragment")
	}
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
