package cypher

import (
	"context"
	"fmt"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	cypherantlr "github.com/orneryd/nornicdb/pkg/cypher/antlr"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/multidb"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// This is a deterministic generated property, not a list of spelling-specific
// regressions. The generator knows the statement body; only the grammar owner
// supplies the selector's identity, arguments and source extent. Each consumer
// must preserve the body contract and agree with that product. Dynamic needles
// deliberately are outside the static guard's contract.
type usePropertyStatement struct {
	name, query, body, rewritten, selector string
	writes                                 bool
	uses                                   int
	rows                                   [][]interface{}
}

func generatedUseStatements() []usePropertyStatement {
	type shape struct {
		name   string
		build  func(string, string) string
		writes bool
		uses   int
	}
	shapes := []shape{
		{"read", func(u, _ string) string { return u + "MATCH (n:UseProperty) RETURN abs(n.v) AS x" }, false, 1},
		{"write", func(u, _ string) string { return u + "CREATE (:UsePropertyWrite) RETURN abs(-1) AS x" }, true, 1},
		{"procedure", func(u, _ string) string { return u + "CALL db.ping() RETURN 1 AS x" }, false, 1},
		{"call", func(u, _ string) string { return "CALL { " + u + "RETURN abs(-1) AS x } RETURN x" }, false, 1},
		{"nested_write", func(u, _ string) string {
			return "CALL { CALL { " + u + "CREATE (:UsePropertyWrite) RETURN 1 AS x } RETURN x } RETURN x"
		}, true, 1},
		{"union", func(u, _ string) string { return u + "RETURN abs(-1) AS x UNION ALL " + u + "RETURN abs(-1) AS x" }, false, 2},
		{"skip_limit", func(u, _ string) string { return u + "MATCH (n:UseProperty) SKIP 0 LIMIT 1 RETURN n.v AS x" }, false, 1},
		{"literal", func(u, payload string) string { return u + "RETURN '" + payload + "' AS x" }, false, 1},
	}
	references := []string{"g", "graph", "match", "limit", "`limit`", "cmp.a", "`cmp`.`a`", "graph.byName('cmp.a')", "graph.byName(toLower('CMP.A'))", "`graph`.`byName`('cmp.a')", "graph.byElementId($element)", "graph /* reference */ . byName('cmp.a')"}
	gaps := []string{" ", " /* USE GRAPH CREATE */ ", "\n// USE GRAPH CREATE\n", "\u000C", "\u00a0", "\u2003"}
	var statements []usePropertyStatement
	for r, reference := range references {
		for modifier, graph := range []string{"", "GRAPH "} {
			for g, gap := range gaps {
				keyword := []string{"USE", "use", "UsE"}[(r+modifier+g)%3]
				graph = []string{strings.ToUpper(graph), strings.ToLower(graph), "GrApH "}[(r+g)%3]
				if modifier == 0 {
					graph = ""
				}
				selector := keyword + gap + strings.ReplaceAll(graph, " ", gap) + reference
				payload := []string{"CREATE (:Probe) USE GRAPH cmp.decoy", "USE cmp.decoy CREATE (:Probe) GRAPH", "GRAPH USE match CREATE"}[(r+g)%3]
				for _, shape := range shapes {
					query := shape.build(selector+gap, payload)
					rewritten := query
					if shape.name == "skip_limit" {
						rewritten = strings.Replace(query, " SKIP 0", " WITH * SKIP 0", 1)
					}
					rows := [][]interface{}{{int64(1)}}
					if shape.name == "union" {
						rows = append(rows, []interface{}{int64(1)})
					}
					if shape.name == "literal" {
						rows = [][]interface{}{{payload}}
					}
					statements = append(statements, usePropertyStatement{
						name:  fmt.Sprintf("r%d/modifier%d/gap%d/%s", r, modifier, g, shape.name),
						query: query, body: shape.build("", payload), rewritten: rewritten,
						selector: selector, writes: shape.writes, uses: shape.uses,
						rows: rows,
					})
				}
			}
		}
	}
	// No selector: literal/comment source must neither select nor reserve a writer.
	for i, literal := range []string{"'CREATE (:Probe) USE GRAPH cmp.decoy'", "\"USE cmp.decoy CREATE (:Probe) GRAPH\"", "'USE graph.byName(\"cmp.decoy\") CREATE'"} {
		query := "RETURN " + literal + " AS x /* USE GRAPH cmp.decoy CREATE */"
		statements = append(statements, usePropertyStatement{name: fmt.Sprintf("no_use/%d", i), query: query, body: query, rewritten: query, rows: [][]interface{}{{strings.Trim(literal, "'\"")}}})
	}
	return statements
}

// A hook changes the input to exactly one real consumer, at its call boundary.
// Negative controls install an independent text reader here. Keeping injection
// in this test adapter avoids a mutable production hook or changing the oracle.
type usePropertyHook func(consumer, query string) string

var usePropertyConsumers = []string{"contract", "permissions", "procedures", "functions", "cache", "dependencies", "routing", "transaction", "desugar"}

type usePropertyFixture struct {
	exec    *StorageExecutor
	manager *multidb.DatabaseManager
	planner *fabric.FabricPlanner
	params  map[string]interface{}
}

func newUsePropertyFixture(t *testing.T) *usePropertyFixture {
	t.Helper()
	manager, err := multidb.NewDatabaseManager(t.Context(), newTestMemoryEngine(t), nil)
	require.NoError(t, err)
	fixtureowner.Own(t, manager)
	for _, name := range []string{"g", "graph", "match", "limit", "use_property_shard"} {
		require.NoError(t, manager.CreateDatabase(t.Context(), multidb.DatabaseCreateRequest{Name: name}))
		store, err := manager.GetStorage(name)
		require.NoError(t, err)
		_, err = store.CreateNode(&storage.Node{ID: "seed", Labels: []string{"UseProperty"}, Properties: map[string]interface{}{"v": int64(1)}})
		require.NoError(t, err)
	}
	require.NoError(t, manager.CreateCompositeDatabase(t.Context(), "cmp", []multidb.ConstituentRef{{Alias: "a", DatabaseName: "use_property_shard", Type: "local", AccessMode: "read_write"}}))
	store, err := manager.GetStorage("cmp")
	require.NoError(t, err)
	exec := NewStorageExecutor(store, QueryCachePolicy{MaxEntries: 64, TTL: time.Hour})
	exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: manager})
	require.NoError(t, exec.UseDatabaseStatementCaches(manager, "cmp"))
	result, err := exec.Execute(t.Context(), "USE cmp.a MATCH (n:UseProperty) RETURN elementId(n) AS e", nil)
	require.NoError(t, err)
	require.Len(t, result.Rows, 1)
	catalog := fabric.NewCatalog()
	require.NoError(t, catalog.PopulateFromManager(manager))
	return &usePropertyFixture{exec: exec, manager: manager, planner: fabric.NewFabricPlanner(catalog), params: map[string]interface{}{"element": result.Rows[0][0]}}
}

func usePropertyFunctions(query string) ([]string, error) {
	functions := []string{}
	err := forEachFunctionCall(query, func(_, normalized string) error { functions = append(functions, normalized); return nil })
	sort.Strings(functions)
	return functions, err
}

func usePropertyProcedureNames(ctx context.Context, query string) []string {
	result := []string{}
	for _, procedure := range RegisteredProceduresForCalls(ctx, query, RawSource) {
		result = append(result, procedure.Name)
	}
	return result
}

func (f *usePropertyFixture) check(ctx context.Context, statement usePropertyStatement, hook usePropertyHook) map[string]error {
	issues := map[string]error{}
	input := func(consumer string) string {
		if hook != nil {
			return hook(consumer, statement.query)
		}
		return statement.query
	}
	check := func(consumer string, got, want interface{}) {
		if !reflect.DeepEqual(got, want) {
			issues[consumer] = fmt.Errorf("got %#v, want %#v", got, want)
		}
	}
	owner := cypherantlr.UseReference{}
	if statement.uses > 0 {
		var err error
		owner, err = cypherantlr.ReadUseReference(ctx, cyphertext.NewSource(ctx, statement.selector+" RETURN 1", cyphertext.RawSource))
		if err != nil {
			return map[string]error{"owner": err}
		}
		if owner.End != len(statement.selector) {
			return map[string]error{"owner": fmt.Errorf("extent %d != %d", owner.End, len(statement.selector))}
		}
	}
	// Removing the generated selectors gives an independent body baseline. Real
	// argument functions still count; the graph-reference pseudo-function does not.
	body, _ := desugarStandaloneOrderClauses(statement.body)
	wantContract, err := readStatementExecutionContract(ctx, body)
	if err != nil {
		return map[string]error{"baseline": err}
	}
	if wantContract.writes != statement.writes {
		return map[string]error{"baseline": fmt.Errorf("body write contract disagrees with generator")}
	}
	contractQuery, _ := desugarStandaloneOrderClauses(input("contract"))
	contract, err := readStatementExecutionContract(ctx, contractQuery)
	if err != nil {
		issues["contract"] = err
	} else {
		check("contract", contract, wantContract)
	}
	permissionQuery, _ := desugarStandaloneOrderClauses(input("permissions"))
	check("permissions", QueryPermissionRequirements(ctx, permissionQuery, RawSource), QueryPermissionRequirements(ctx, body, RawSource))
	procedureQuery, _ := desugarStandaloneOrderClauses(input("procedures"))
	check("procedures", usePropertyProcedureNames(ctx, procedureQuery), usePropertyProcedureNames(ctx, body))
	wantFunctions, err := usePropertyFunctions(body)
	if err != nil {
		return map[string]error{"baseline": err}
	}
	for i := 0; i < statement.uses; i++ {
		for _, argument := range owner.Args {
			functions, err := usePropertyFunctions(argument)
			if err != nil {
				return map[string]error{"owner": err}
			}
			wantFunctions = append(wantFunctions, functions...)
		}
	}
	sort.Strings(wantFunctions)
	functions, err := usePropertyFunctions(input("functions"))
	if err != nil {
		issues["functions"] = err
	} else {
		check("functions", functions, wantFunctions)
	}
	cacheQuery, _ := desugarStandaloneOrderClauses(input("cache"))
	wantCache := compileExecutionPolicy(ctx, body, 0).cacheable
	for _, argument := range owner.Args {
		wantCache = wantCache && compileExecutionPolicy(ctx, "RETURN "+argument, 0).cacheable
	}
	check("cache", compileExecutionPolicy(ctx, cacheQuery, 0).cacheable, wantCache)
	policy := cachedExecutionPolicy(ctx, cacheQuery)
	check("cache", policy.cacheable, wantCache)
	check("cache", policy == cachedExecutionPolicy(ctx, cacheQuery), true)
	check("cache", f.exec.canUseResultCache(ctx, &QueryInfo{IsReadOnly: !statement.writes}, cacheQuery), wantCache)
	// Raw selector identity is a separate observation from cache policy. A
	// dynamic selector acquires dependencies when its runtime target is resolved;
	// the static-name population must be tracked before routing or normalization.
	if owner.Function == "" {
		scopes, valid := f.exec.cacheDependencyScopes(input("dependencies"))
		check("dependencies", valid, true)
		if statement.uses == 0 {
			check("dependencies", len(scopes), 0)
		} else {
			wantScope, err := f.manager.StatementCacheScope(owner.Name)
			if err != nil {
				issues["dependencies"] = err
			} else {
				check("dependencies", containsStatementScope(scopes, wantScope), true)
				for _, scope := range scopes {
					check("dependencies", scope == wantScope, true)
				}
			}
		}
	}
	check("desugar", func() string { q, _ := desugarStandaloneOrderClauses(input("desugar")); return q }(), statement.rewritten)
	// Check every grammar-owned selector in the complete statement too, including
	// nested CALL and both UNION branches. This catches missing generator slots.
	normalized, _ := desugarStandaloneOrderClauses(statement.query)
	syntax, err := cypherantlr.ParseSyntax(ctx, normalized, cypherantlr.FragmentEntry, false, cypherantlr.RawSource)
	if err != nil {
		issues["owner"] = err
		return issues
	}
	count := 0
	syntax.Walk(func(node cypherantlr.SyntaxNode) func() {
		if node.Rule() == cypherantlr.CypherParserRULE_useClause {
			count++
			ref := node.UseReference(nil)
			if ref.Name != owner.Name || ref.Function != owner.Function || !reflect.DeepEqual(ref.Args, owner.Args) {
				issues["owner"] = fmt.Errorf("full-statement selector disagrees with leading owner product")
			}
			start, _ := node.Span()
			routed, _, present, routeErr := fabric.ReadUseClause(ctx, normalized[start:], true, cyphertext.RawSource)
			if routeErr != nil || !present || routed.Name != ref.Name || routed.Function != ref.Function || !reflect.DeepEqual(routed.Args, ref.Args) || routed.End != ref.End-start {
				issues["routing"] = fmt.Errorf("routing product/extent disagrees with syntax owner: %#v, %v", routed, routeErr)
			}
		}
		return nil
	})
	check("owner", count, statement.uses)
	routeQuery, _ := desugarStandaloneOrderClauses(input("routing"))
	plan, err := f.planner.Plan(ctx, routeQuery, "cmp", cyphertext.RawSource)
	if err != nil {
		issues["routing"] = err
	} else {
		selected := 0
		var visit func(fabric.Fragment)
		visit = func(fragment fabric.Fragment) {
			switch node := fragment.(type) {
			case *fabric.FragmentExec:
				visit(node.Input)
				if node.Graph != nil {
					if node.Graph.Function != owner.Function || !reflect.DeepEqual(node.Graph.Args, owner.Args) {
						issues["routing"] = fmt.Errorf("dynamic target disagrees with owner: %#v", node.Graph)
					}
					selected++
				} else if node.GraphName != "cmp" {
					if node.GraphName != owner.Name {
						issues["routing"] = fmt.Errorf("target %q != owner %q", node.GraphName, owner.Name)
					}
					selected++
				}
			case *fabric.FragmentApply:
				visit(node.Input)
				visit(node.Inner)
			case *fabric.FragmentUnion:
				visit(node.LHS)
				visit(node.RHS)
			case *fabric.FragmentLeaf:
				visit(node.Input)
			}
		}
		visit(plan)
		if statement.uses > 0 && selected == 0 {
			issues["routing"] = fmt.Errorf("no owner-selected fragment")
		}
	}
	// Public BEGIN/Execute/ROLLBACK exercises admission, preparation, dynamic
	// resolution and dispatch, rather than manually opening a named participant.
	if err := f.checkTransaction(ctx, statement, owner, input("transaction")); err != nil {
		issues["transaction"] = err
	}
	return issues
}

func (f *usePropertyFixture) checkTransaction(ctx context.Context, statement usePropertyStatement, owner cypherantlr.UseReference, query string) error {
	// An ordinary graph stays in its own transaction; selecting it from a
	// composite transaction is intentionally forbidden by the public contract.
	exec := f.exec
	if owner.Name != "" && !strings.Contains(owner.Name, ".") {
		store, err := f.manager.GetStorage(owner.Name)
		if err != nil {
			return err
		}
		exec = NewStorageExecutor(store)
		exec.SetDatabaseManager(&testDatabaseManagerAdapter{manager: f.manager})
	}
	if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
		return err
	}
	defer func() {
		if exec.txContext != nil {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
		}
	}()
	tx, composite := exec.txContext.tx.(*fabric.FabricTransaction)
	local, ordinary := exec.txContext.tx.(*storage.BadgerTransaction)
	if !composite && !ordinary {
		return fmt.Errorf("unknown transaction type")
	}
	// The historical literal-to-write schedule is repeated before every generated
	// statement, so an independently inferred writer cannot poison the next one.
	if _, err := exec.Execute(ctx, "RETURN 'CREATE (:Probe) USE GRAPH cmp.decoy' AS template", nil); err != nil {
		return err
	}
	if composite && tx.WriteShard() != "" {
		return fmt.Errorf("literal reserved writer %q", tx.WriteShard())
	}
	if ordinary && local.OperationCount() != 0 {
		return fmt.Errorf("literal staged a write")
	}
	result, err := exec.Execute(ctx, query, f.params)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(result.Rows, statement.rows) {
		return fmt.Errorf("rows %#v != %#v", result.Rows, statement.rows)
	}
	target := owner.Name
	if owner.Function != "" {
		target = "cmp.a"
	}
	physical := target
	if target == "cmp.a" {
		physical = "use_property_shard"
	}
	wantWriter := ""
	if statement.writes {
		wantWriter = "local:" + physical
	}
	if composite && tx.WriteShard() != wantWriter {
		return fmt.Errorf("writer %q != %q; participants %v", tx.WriteShard(), wantWriter, tx.Participants())
	}
	if ordinary {
		if exec.databaseName() != physical {
			return fmt.Errorf("ordinary transaction selected %q != %q", exec.databaseName(), physical)
		}
		if (local.OperationCount() > 0) != statement.writes {
			return fmt.Errorf("ordinary write operations %d disagree with owner/body", local.OperationCount())
		}
	}
	if composite && target != "" {
		found := false
		for _, participant := range tx.Participants() {
			if participant == "local:"+physical {
				found = true
			}
			if participant != "local:"+physical && participant != "local:cmp" {
				return fmt.Errorf("unexpected participant %q", participant)
			}
		}
		if !found {
			return fmt.Errorf("owner target has no participant: %v", tx.Participants())
		}
	}
	if _, err := exec.Execute(ctx, "ROLLBACK", nil); err != nil {
		return err
	}
	if statement.writes {
		store, err := f.manager.GetStorage(physical)
		if err != nil {
			return err
		}
		nodes, err := store.GetNodesByLabel("UsePropertyWrite")
		if err != nil {
			return err
		}
		if len(nodes) != 0 {
			return fmt.Errorf("rollback left %d writes", len(nodes))
		}
	}
	return nil
}

func TestUseOwnerGeneratedConsumers(t *testing.T) {
	fixture := newUsePropertyFixture(t)
	statements := generatedUseStatements()
	require.Len(t, statements, 1155)
	for _, statement := range statements {
		t.Run(statement.name, func(t *testing.T) {
			for consumer, err := range fixture.check(t.Context(), statement, nil) {
				t.Errorf("%s: %v\nquery: %s", consumer, err, statement.query)
			}
		})
	}
}

func TestUseOwnerGeneratedNegativeControls(t *testing.T) {
	fixture := newUsePropertyFixture(t)
	statements := generatedUseStatements()
	// This deliberately wrong reader searches scalar text as if it were USE
	// syntax and replaces the consumer's statement with an invented graph write.
	// The same checker runs real production consumers on that input. No expected
	// value or observed result is tampered with to manufacture a failing control.
	textReader := func(query string) string {
		for _, needle := range []string{"(?i)USE", "(?i)GRAPH"} {
			re, err := GetCachedRegex(needle)
			if err == nil && re.MatchString(query) {
				return "USE GRAPH cmp.a CREATE (:UsePropertyWrite) WITH abs(-1) AS injected CALL db.ping() YIELD success " + query
			}
		}
		return query
	}
	for _, consumer := range usePropertyConsumers {
		t.Run(consumer, func(t *testing.T) {
			for _, statement := range statements[len(statements)-3:] {
				require.Empty(t, fixture.check(t.Context(), statement, nil), "unmutated control")
				hook := func(name, query string) string {
					if name == consumer {
						return textReader(query)
					}
					return query
				}
				issues := fixture.check(t.Context(), statement, hook)
				require.Error(t, issues[consumer], "property survived the independent %s text reader for %s", consumer, statement.name)
				if consumer == "transaction" {
					require.Contains(t, issues[consumer].Error(), "writer", "the valid mutant must fail the writer observation")
				}
				for name := range issues {
					require.Equal(t, consumer, name, "mutation leaked across consumer boundary")
				}
			}
		})
	}
}

func TestUseOwnerUnicodeBoundary(t *testing.T) {
	fixture := newUsePropertyFixture(t)
	for _, gap := range []string{"\u00a0", "\u2003"} {
		for _, selector := range []string{"USE" + gap + "cmp.a", "USE GRAPH" + gap + "cmp.a"} {
			query := selector + " CREATE (:UsePropertyWrite) RETURN 1 AS x"
			statement := usePropertyStatement{query: query, body: "CREATE (:UsePropertyWrite) RETURN 1 AS x", rewritten: query, selector: selector, writes: true, uses: 1, rows: [][]interface{}{{int64(1)}}}
			require.Empty(t, fixture.check(t.Context(), statement, nil), query)
		}
	}
}
