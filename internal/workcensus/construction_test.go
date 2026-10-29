package workcensus

import (
	"encoding/json"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// This client keeps construction of source/SSA facts in the shared owner. The
// module-wide source population includes the new parent's stored-effect checker.
func constructionConfig() Config {
	c := Config{Dir: "../..", Patterns: []string{"./..."}, BuildFlags: []string{"-tags=noui,nolocalllm"}, SourceRoots: []SourceRoot{{Path: ".", Languages: map[string]string{".go": "go"}, ExcludeDirs: []string{".git", "vendor", "node_modules", "testdata"}, ExcludeFiles: []string{"*_test.go"}}}}
	for pkg, names := range map[string][]string{
		"golang.org/x/tools/go/packages":      {"Load"},
		"golang.org/x/tools/go/ssa/ssautil":   {"Packages", "AllPackages"},
		"golang.org/x/tools/go/callgraph/cha": {"CallGraph"},
		"golang.org/x/tools/go/callgraph/vta": {"CallGraph"},
		"go/parser":                           {"ParseFile", "ParseDir"},
	} {
		c.Selectors = append(c.Selectors, Selector{Kind: "analysis construction", Node: "call", Reachability: "all", Packages: []string{pkg}, Names: names})
	}
	return c
}

func TestAnalysisConstructionDiscovery(t *testing.T) {
	if flag.Lookup("test.run").Value.String() != "^TestAnalysisConstructionDiscovery$" {
		t.Skip("explicit discovery-only selector required")
	}
	actual := mustExtract(t, constructionConfig())
	for _, member := range actual.Members {
		b, _ := json.Marshal(member)
		fmt.Println("CONSTRUCTION_MEMBER " + string(b))
	}
	t.Fatal("construction discovery does not grant approval")
}

func TestAnalysisConstructionOwnership(t *testing.T) {
	c := constructionConfig()
	actual := mustExtract(t, c)
	approval := approved(t, "construction.json")
	policy := Policy{Classes: []string{"shared engine", "distinct source operation"}, ClassKinds: map[string]string{"shared engine": "review", "distinct source operation": "review"}, RequireOwnerDigest: true, RequireReviewOwner: true, Owners: []string{"#1667"}}
	if failures := Validate(actual, approval, policy); len(failures) != 0 {
		t.Fatal(failures)
	}
	// Planted outside all original owner packages. Import spelling is client
	// data; the real shared extraction resolves the new alias and declaration.
	root, err := filepath.Abs(c.Dir)
	if err != nil {
		t.Fatal(err)
	}
	c.Overlay = map[string][]byte{filepath.Join(root, "plugins/census_probe/load.go"): []byte("package census_probe\nimport unfamiliar \"golang.org/x/tools/go/packages\"\nfunc Arbitrary(){ _,_ = unfamiliar.Load(nil, \"./...\") }\n")}
	mutated := mustExtract(t, c)
	failures := Validate(mutated, approval, policy)
	if len(failures) != 1 || !strings.HasPrefix(failures[0], "UNLISTED plugins/census_probe/load.go:") {
		t.Fatal("new outside-package loader escaped the real gate", failures)
	}
}
