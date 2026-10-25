package cypher

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/scanner"
	"go/token"
	"os"
	"strings"
	"testing"
)

type historicalBoundRule struct {
	ID, Owner, Code, Detail, MessageFactory, Decision string
	SemanticDisposition                               string
	Cases                                             []string
}
type historicalBoundOrigin struct {
	Ref, File, Symbol, Source, SHA256 string
	Sites                             []string
}
type historicalBoundRules struct {
	Rules                           []historicalBoundRule
	Origins                         []historicalBoundOrigin
	Cases                           []boundRuleCase
	Observations, ANTLRObservations []boundRuleObservation
	PreChange                       boundRulePreChange
	AdditionalPreChange             []boundRulePreChange
	Provenance                      boundRuleProvenance
}

type boundRuleMethod struct {
	Symbol, Method, Parser string
	Command                json.RawMessage
}
type boundRuleMember struct{ Symbol, Disposition, Reason string }
type boundRuleProvenance struct {
	Records []boundRuleMethod
	Union   []boundRuleMember
}

func boundRuleProvenanceCoverage(pop historicalBoundRules) []string {
	var problems []string
	members := map[string]boundRuleMember{}
	for _, member := range pop.Provenance.Union {
		members[member.Symbol] = member
		if member.Disposition == "" || member.Reason == "" {
			problems = append(problems, "rule-union member lacks semantic disposition: "+member.Symbol)
		}
	}
	historical := map[string]bool{}
	for _, origin := range pop.Origins {
		historical[origin.Symbol] = true
	}
	for symbol := range historical {
		if _, known := members[symbol]; !known {
			problems = append(problems, "historical rule absent from discovery union: "+symbol)
		}
		methods := map[string]bool{}
		for _, receipt := range pop.Provenance.Records {
			if receipt.Symbol != symbol || len(receipt.Command) == 0 {
				continue
			}
			methods[receipt.Method] = true
			if receipt.Method == "path" {
				methods["path-"+receipt.Parser] = true
			}
		}
		for _, method := range []string{"explain", "rg", "path-default", "path-antlr"} {
			if !methods[method] {
				problems = append(problems, "historical rule lacks discovery command "+method+": "+symbol)
			}
		}
	}
	return problems
}

type boundRuleSnapshot struct{ File, Source, SHA256 string }
type boundRulePreChange struct {
	Ref            string
	BaselineFiles  map[string]string
	Snapshots      []boundRuleSnapshot
	Removals       []historicalBoundRule
	ExtraFactories []string
}

// The immediate pre-change source complements the two historical call closures.
// Every removed diagnostic in any changed production file needs its own semantic
// disposition. A newly changed file without a before image fails closed.
func boundRemovedDiagnostics(pop historicalBoundRules) []string {
	var problems []string
	for _, baseline := range pop.AdditionalPreChange {
		part := pop
		part.PreChange = baseline
		part.AdditionalPreChange = nil
		problems = append(problems, boundRemovedDiagnostics(part)...)
	}
	snapshots := map[string]boundRuleSnapshot{}
	for _, snapshot := range pop.PreChange.Snapshots {
		snapshots[snapshot.File] = snapshot
	}
	rows := map[string]historicalBoundRule{}
	for _, row := range pop.PreChange.Removals {
		rows[row.ID] = row
	}
	observed := map[string]bool{}
	for file, expected := range pop.PreChange.BaselineFiles {
		current, err := os.ReadFile(strings.TrimPrefix(file, "pkg/cypher/"))
		if err != nil && !os.IsNotExist(err) {
			problems = append(problems, err.Error())
			continue
		}
		text := strings.ReplaceAll(string(current), "\r\n", "\n")
		digest := sha256.Sum256([]byte(text))
		if hex.EncodeToString(digest[:]) == expected {
			continue
		}
		snapshot, known := snapshots[file]
		if !known {
			problems = append(problems, "changed file lacks pre-change diagnostic source: "+file)
			continue
		}
		digest = sha256.Sum256([]byte(snapshot.Source))
		if snapshot.SHA256 != expected || hex.EncodeToString(digest[:]) != expected {
			problems = append(problems, "pre-change source changed: "+file)
			continue
		}
		old, err := boundDiagnosticSites(file, snapshot.Source, pop.PreChange.ExtraFactories...)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		now := map[string][]boundDiagnosticSite{}
		if text != "" {
			now, err = boundDiagnosticSites(file, text, pop.PreChange.ExtraFactories...)
			if err != nil {
				problems = append(problems, err.Error())
				continue
			}
		}
		for symbol, sites := range old {
			remaining := map[string]int{}
			for _, site := range now[symbol] {
				remaining[site.text]++
			}
			for _, site := range sites {
				if remaining[site.text] > 0 {
					remaining[site.text]--
					continue
				}
				id := fmt.Sprintf("%s:%s:%s:%d", pop.PreChange.Ref, symbol, site.factory, site.ordinal)
				observed[id] = true
				row, known := rows[id]
				if !known {
					problems = append(problems, "removed diagnostic has no surviving owner row: "+id)
					continue
				}
				if row.Owner == "" || row.Code == "" || row.Detail == "" || row.MessageFactory == "" || row.Decision == "" || len(row.Cases) == 0 {
					problems = append(problems, "removed diagnostic lacks owner/diagnostic/disposition/public proof: "+id)
					continue
				}
				parts := strings.SplitN(row.Owner, ":", 2)
				if len(parts) != 2 {
					problems = append(problems, "invalid surviving owner: "+row.Owner)
					continue
				}
				source, err := parser.ParseFile(token.NewFileSet(), strings.TrimPrefix(parts[0], "pkg/cypher/"), nil, 0)
				if err != nil {
					problems = append(problems, err.Error())
					continue
				}
				ownerFound := false
				for _, decl := range source.Decls {
					if fn, ok := decl.(*ast.FuncDecl); ok && boundRuleSymbol(fn) == parts[1] {
						ownerFound = true
					}
				}
				if !ownerFound {
					problems = append(problems, "removed diagnostic owner absent: "+row.Owner)
				}
				for _, id := range row.Cases {
					caseFound := false
					for _, sample := range pop.Cases {
						if sample.ID == id {
							caseFound = true
						}
					}
					if !caseFound {
						problems = append(problems, "removed diagnostic public case absent: "+id)
					}
				}
			}
		}
	}
	for id := range rows {
		if !observed[id] {
			problems = append(problems, "stale removed diagnostic row: "+id)
		}
	}
	return problems
}

type boundDiagnosticSite struct {
	factory, text string
	ordinal       int
}

func boundDiagnosticSites(file, source string, extraFactories ...string) (map[string][]boundDiagnosticSite, error) {
	fset := token.NewFileSet()
	parsed, err := parser.ParseFile(fset, file, source, 0)
	if err != nil {
		return nil, err
	}
	result := map[string][]boundDiagnosticSite{}
	for _, decl := range parsed.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		symbol := boundRuleSymbol(fn)
		ordinals := map[string]int{}
		ast.Inspect(fn.Body, func(node ast.Node) bool {
			factory := ""
			start := token.NoPos
			switch n := node.(type) {
			case *ast.CallExpr:
				switch fun := n.Fun.(type) {
				case *ast.Ident:
					factory = fun.Name
					start = fun.Pos()
				case *ast.SelectorExpr:
					factory = fun.Sel.Name
					start = fun.Sel.Pos()
				}
				switch factory {
				case "localizedStatusError", "newSemanticError", "localizedError", "Errorf", "New":
				default:
					matched := false
					for _, extra := range extraFactories {
						matched = matched || factory == extra
					}
					if !matched {
						factory = ""
					}
				}
			case *ast.CompositeLit:
				if id, ok := n.Type.(*ast.Ident); ok && id.Name == "classifiedCypherError" {
					factory = id.Name
					start = id.Pos()
				}
			}
			if factory == "" {
				return true
			}
			raw := source[fset.Position(start).Offset:fset.Position(node.End()).Offset]
			var scan scanner.Scanner
			tokens := token.NewFileSet()
			scan.Init(tokens.AddFile("diagnostic", -1, len(raw)), []byte(raw), nil, 0)
			var normalized strings.Builder
			for {
				_, kind, literal := scan.Scan()
				if kind == token.EOF {
					break
				}
				if kind == token.SEMICOLON {
					continue
				}
				if literal != "" {
					normalized.WriteString(literal)
				} else {
					normalized.WriteString(kind.String())
				}
			}
			ordinals[factory]++
			result[symbol] = append(result[symbol], boundDiagnosticSite{factory, normalized.String(), ordinals[factory]})
			return true
		})
	}
	return result, nil
}

func TestCanonicalBoundRemovedDiagnostics(t *testing.T) {
	data, err := os.ReadFile("testdata/bound_validation_rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var pop historicalBoundRules
	if err = json.Unmarshal(data, &pop); err != nil {
		t.Fatal(err)
	}
	if pop.PreChange.Ref == "" || len(pop.PreChange.BaselineFiles) == 0 || len(pop.PreChange.Removals) == 0 {
		t.Fatal("immediate pre-change diagnostic population absent")
	}
	if problems := boundRemovedDiagnostics(pop); len(problems) != 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
	t.Logf("BOUND_REMOVED_DIAGNOSTICS baseline_files=%d changed_files=%d removed_sites=%d", len(pop.PreChange.BaselineFiles), len(pop.PreChange.Snapshots), len(pop.PreChange.Removals))
	t.Run("removed-diagnostic-without-owner-fails", func(t *testing.T) {
		mutant := pop
		mutant.PreChange.Removals = pop.PreChange.Removals[1:]
		if len(boundRemovedDiagnostics(mutant)) == 0 {
			t.Fatal("removed diagnostic without owner escaped")
		}
	})
	for _, baseline := range pop.AdditionalPreChange {
		t.Run(baseline.Ref+"-removed-diagnostic-without-owner-fails", func(t *testing.T) {
			mutant := pop
			mutant.AdditionalPreChange = nil
			mutant.PreChange = baseline
			mutant.PreChange.Removals = baseline.Removals[1:]
			if len(boundRemovedDiagnostics(mutant)) == 0 {
				t.Fatal("removed parent/current diagnostic without owner escaped")
			}
		})
	}
}

func boundRuleSymbol(fn *ast.FuncDecl) string {
	name := fn.Name.Name
	if fn.Recv != nil {
		receiver := fn.Recv.List[0].Type
		if ptr, ok := receiver.(*ast.StarExpr); ok {
			receiver = ptr.X
		}
		if id, ok := receiver.(*ast.Ident); ok {
			name = id.Name + "." + name
		}
	}
	return name
}

// This checks a fixed historical diagnostic population and its domain-specific
// dispositions. Generic maintained-source census extraction belongs to B0.
func boundRuleCoverage(pop historicalBoundRules) []string {
	var problems []string
	rows := map[string]historicalBoundRule{}
	cases := map[string]bool{}
	for _, sample := range pop.Cases {
		cases[sample.ID] = true
	}
	owners := map[string]bool{}
	for _, rule := range pop.Rules {
		if _, duplicate := rows[rule.ID]; duplicate {
			problems = append(problems, "duplicate rule: "+rule.ID)
		}
		rows[rule.ID] = rule
		if rule.Owner == "" || rule.Code == "" || rule.Detail == "" || rule.MessageFactory == "" || rule.SemanticDisposition == "" || len(rule.Cases) == 0 {
			problems = append(problems, "rule lacks owner/diagnostic/semantic disposition/public proof: "+rule.ID)
			continue
		}
		for _, id := range rule.Cases {
			if !cases[id] {
				problems = append(problems, "missing public case: "+id)
			}
		}
		if owners[rule.Owner] {
			continue
		}
		parts := strings.SplitN(rule.Owner, ":", 2)
		if len(parts) != 2 {
			problems = append(problems, "invalid owner: "+rule.Owner)
			continue
		}
		source, err := parser.ParseFile(token.NewFileSet(), strings.TrimPrefix(parts[0], "pkg/cypher/"), nil, 0)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		for _, decl := range source.Decls {
			if fn, ok := decl.(*ast.FuncDecl); ok && boundRuleSymbol(fn) == parts[1] {
				owners[rule.Owner] = true
			}
		}
		if !owners[rule.Owner] {
			problems = append(problems, "owner absent: "+rule.Owner)
		}
	}
	reached := map[string]bool{}
	for _, origin := range pop.Origins {
		digest := sha256.Sum256([]byte(origin.Source))
		if hex.EncodeToString(digest[:]) != origin.SHA256 {
			problems = append(problems, "historical source changed: "+origin.Symbol)
		}
		source, err := parser.ParseFile(token.NewFileSet(), origin.File, "package cypher\n"+origin.Source, 0)
		if err != nil {
			problems = append(problems, err.Error())
			continue
		}
		observed := map[string]bool{}
		ordinals := map[string]int{}
		record := func(factory string) {
			ordinals[factory]++
			id := fmt.Sprintf("%s:%s:%s:%d", origin.Ref, origin.Symbol, factory, ordinals[factory])
			observed[id] = true
			reached[id] = true
			if _, known := rows[id]; !known {
				problems = append(problems, "diagnostic lacks disposition: "+id)
			}
		}
		ast.Inspect(source, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.CallExpr:
				name := ""
				switch f := n.Fun.(type) {
				case *ast.Ident:
					name = f.Name
				case *ast.SelectorExpr:
					name = f.Sel.Name
				}
				switch name {
				case "localizedStatusError", "newSemanticError", "localizedError", "Errorf", "New":
					record(name)
				}
			case *ast.CompositeLit:
				if id, ok := n.Type.(*ast.Ident); ok && id.Name == "classifiedCypherError" {
					record(id.Name)
				}
			}
			return true
		})
		for _, id := range origin.Sites {
			if !observed[id] {
				problems = append(problems, "historical site stale: "+id)
			}
		}
		if len(observed) != len(origin.Sites) {
			problems = append(problems, "historical site population changed: "+origin.Symbol)
		}
	}
	for id := range rows {
		if !reached[id] {
			problems = append(problems, "rule not in historical population: "+id)
		}
	}
	problems = append(problems, boundRuleProvenanceCoverage(pop)...)
	for _, observations := range [][]boundRuleObservation{pop.Observations, pop.ANTLRObservations} {
		leaves := map[string]bool{}
		for _, sample := range observations {
			leaves[sample.ID+"/"+sample.Input+"/"+sample.Mode] = true
		}
		for id := range cases {
			for _, input := range []string{"ordinary", "empty", "one"} {
				for _, mode := range []string{"RUN", "EXPLAIN", "PROFILE"} {
					if !leaves[id+"/"+input+"/"+mode] {
						problems = append(problems, "public boundary untested: "+id+"/"+input+"/"+mode)
					}
				}
			}
		}
	}
	return problems
}

func TestCanonicalBoundRulePopulation(t *testing.T) {
	data, err := os.ReadFile("testdata/bound_validation_rules.json")
	if err != nil {
		t.Fatal(err)
	}
	var pop historicalBoundRules
	if err = json.Unmarshal(data, &pop); err != nil {
		t.Fatal(err)
	}
	if problems := boundRuleCoverage(pop); len(problems) != 0 {
		t.Fatalf("%s", strings.Join(problems, "\n"))
	}
	t.Logf("BOUND_RULE_POPULATION origins=%d diagnostic_sites=%d public_cases=%d", len(pop.Origins), len(pop.Rules), len(pop.Cases))
	t.Run("unowned-rule-fails", func(t *testing.T) {
		mutant := pop
		mutant.Rules = append([]historicalBoundRule(nil), pop.Rules...)
		mutant.Rules[0].Owner = ""
		if len(boundRuleCoverage(mutant)) == 0 {
			t.Fatal("unowned validation rule escaped the production data check")
		}
	})
	t.Run("omitted-diagnostic-fails", func(t *testing.T) {
		mutant := pop
		mutant.Rules = pop.Rules[1:]
		if len(boundRuleCoverage(mutant)) == 0 {
			t.Fatal("historical diagnostic escaped the population check")
		}
	})
	t.Run("missing-graph-provenance-fails", func(t *testing.T) {
		mutant := pop
		mutant.Provenance.Records = nil
		if len(boundRuleCoverage(mutant)) == 0 {
			t.Fatal("historical rule without graph/rg commands escaped the production data check")
		}
	})
}
