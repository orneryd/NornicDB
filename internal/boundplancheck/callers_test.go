//go:build boundplan_census_operator

// Author: #1640 lane (Claude, Rizheg). Analysis-only client. Records every
// compiled caller of each direct member, inside or outside the population, so
// a member class is closed by its complete caller set and public reachability.
package boundplancheck

import (
	"archive/zip"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"golang.org/x/tools/go/ssa"
)

// Public entry points. A member reached from none of them has no public
// behaviour to prove and is a deletion candidate, not an approved owner.
var boundPublicRoots = []workcensus.Root{
	{Package: boundModule + "pkg/cypher", Name: "Execute", Receiver: "StorageExecutor", Required: true},
	{Package: boundModule + "pkg/cypher", Name: "StatementFactsWith", Receiver: "StorageExecutor", Required: true},
	{Package: boundModule + "pkg/fabric", Name: "Execute", Receiver: "QueryGateway", Required: true},
	{Package: boundModule + "pkg/fabric", Name: "Execute", Receiver: "FabricExecutor", Required: true},
	{Package: boundModule + "pkg/bolt", Name: "Execute", Receiver: "boltQueryExecutorAdapter", Required: true},
	{Package: boundModule + "pkg/bolt", Name: "Execute", Receiver: "transactionalBoltQueryExecutorAdapter", Required: true},
}

type boundCallerEdge struct {
	Caller string `json:"caller"`
	Member bool   `json:"member"`
	Test   bool   `json:"test"`
	Site   string `json:"site"`
}

type boundCallerReceipt struct {
	Author  string                       `json:"author"`
	Callers map[string][]boundCallerEdge `json:"callers"`
	Roots   map[string][]string          `json:"roots"`
	Counts  map[string]int               `json:"counts"`
}

func boundRootName(root workcensus.Root) string {
	return strings.TrimPrefix(root.Package, boundModule) + "." + root.Receiver + "." + root.Name
}

// Closures and generic instances belong to their enclosing source function.
func boundEnclosing(fn *ssa.Function) *ssa.Function {
	for fn != nil {
		if origin := fn.Origin(); origin != nil {
			fn = origin
		}
		if fn.Parent() == nil {
			return fn
		}
		fn = fn.Parent()
	}
	return nil
}

func boundMemberCallers(t *testing.T, receipt boundCompiledReceipt) boundCallerReceipt {
	t.Helper()
	program := receipt.program
	declared := map[ast.Node]string{}
	for _, member := range receipt.Inventory.Members {
		if member.Syntax != nil {
			declared[member.Syntax.Node] = member.ID
		}
	}
	memberOf := func(fn *ssa.Function) (string, bool) {
		top := boundEnclosing(fn)
		if top == nil || top.Syntax() == nil {
			return "", false
		}
		id, ok := declared[top.Syntax()]
		return id, ok
	}
	out := boundCallerReceipt{Author: "#1640 lane (Claude, Rizheg)", Callers: map[string][]boundCallerEdge{}, Roots: map[string][]string{}, Counts: map[string]int{}}
	seen := map[string]bool{}
	for _, fn := range program.Functions {
		id, ok := memberOf(fn)
		if !ok {
			continue
		}
		if _, ok := out.Callers[id]; !ok {
			out.Callers[id] = []boundCallerEdge{}
		}
		node := program.CallGraph.Nodes[fn]
		if node == nil {
			continue
		}
		for _, edge := range node.In {
			caller := boundEnclosing(edge.Caller.Func)
			if caller == nil {
				continue
			}
			callerID, member := memberOf(edge.Caller.Func)
			// A member's own closures and recursion are not callers.
			if member && callerID == id {
				continue
			}
			name := caller.String()
			if member {
				name = callerID
			}
			site := ""
			if edge.Site != nil {
				position := program.FileSet.Position(edge.Site.Pos())
				if relative, err := filepath.Rel(program.Root, position.Filename); err == nil {
					site = filepath.ToSlash(relative) + ":" + strconv.Itoa(position.Line)
				}
			}
			key := id + "\x00" + name + "\x00" + site
			if seen[key] {
				continue
			}
			seen[key] = true
			file, _, _ := strings.Cut(site, ":")
			out.Callers[id] = append(out.Callers[id], boundCallerEdge{Caller: name, Member: member, Test: strings.HasSuffix(file, "_test.go"), Site: site})
		}
	}
	for _, root := range boundPublicRoots {
		reach, err := program.ReachableFrom([]workcensus.Root{root})
		if err != nil {
			t.Fatal(err)
		}
		label := boundRootName(root)
		hit := map[string]bool{}
		for fn := range reach.Functions {
			if id, ok := memberOf(fn); ok && !hit[id] {
				hit[id] = true
				out.Roots[id] = append(out.Roots[id], label)
			}
		}
	}
	for id := range out.Callers {
		edges := out.Callers[id]
		sort.Slice(edges, func(i, j int) bool { return edges[i].Caller+edges[i].Site < edges[j].Caller+edges[j].Site })
		sort.Strings(out.Roots[id])
		if len(out.Roots[id]) == 0 {
			out.Counts["no-public-root"]++
		}
	}
	out.Counts["members"] = len(receipt.Inventory.Members)
	out.Counts["members-with-ssa-function"] = len(out.Callers)
	return out
}

func TestBoundPlanMemberCallerInventory(t *testing.T) {
	callers := boundMemberCallers(t, boundCompiledQuery(t, boundPlanCensusConfig()))
	raw, err := json.Marshal(callers)
	if err != nil {
		t.Fatal(err)
	}
	var compressed bytes.Buffer
	archive := zip.NewWriter(&compressed)
	entry, err := archive.Create("member-callers.json")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = entry.Write(raw); err != nil {
		t.Fatal(err)
	}
	if err = archive.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("BOUND_PLAN_CALLER_COUNTS %v", callers.Counts)
	t.Logf("BOUND_PLAN_CALLER_ZIP %s", base64.StdEncoding.EncodeToString(compressed.Bytes()))
}

// Controls: a direct caller outside the population, a caller that reaches the
// member only through its own closure, and a same-spelling function in another
// package. The member's signature carries no fact type, so the planted callers
// stay outside the population. Both must be recorded as external callers with
// their planted sites; the decoy must not appear anywhere; removing the plants
// must leave no planted edge.
func TestBoundPlanCallerInventoryDetectsOutsideCallers(t *testing.T) {
	config := boundPlanCensusConfig()
	config.Overlay = map[string][]byte{
		filepath.Join(config.Dir, "pkg/cypher/r73_planted_caller.go"): []byte(`package cypher
func plantedDirectCaller() bool { return containsNotInOperator("RETURN 1") }
func plantedClosureCaller() bool {
 check := func() bool { return containsNotInOperator("RETURN 2") }
 return check()
}
`),
		filepath.Join(config.Dir, "pkg/server/r73_planted_caller.go"): []byte(`package server
func containsNotInOperator(string) bool { return false }
func plantedDecoyCaller() bool { return containsNotInOperator("RETURN 3") }
`),
	}
	callers := boundMemberCallers(t, boundCompiledQuery(t, config))
	id := ""
	for candidate := range callers.Callers {
		if strings.HasSuffix(candidate, "pkg/cypher.containsNotInOperator/bound-plan-candidate#1") {
			id = candidate
		}
	}
	if id == "" {
		t.Fatal("control member containsNotInOperator missing from caller inventory")
	}
	for _, caller := range []string{"pkg/cypher.plantedDirectCaller", "pkg/cypher.plantedClosureCaller"} {
		found := false
		for _, edge := range callers.Callers[id] {
			if strings.HasSuffix(edge.Caller, caller) && !edge.Member && strings.HasPrefix(edge.Site, "pkg/cypher/r73_planted_caller.go:") {
				found = true
			}
		}
		if !found {
			t.Fatalf("outside caller %s of %s not recorded: %v", caller, id, callers.Callers[id])
		}
	}
	for id, edges := range callers.Callers {
		for _, edge := range edges {
			if strings.HasSuffix(edge.Caller, "pkg/server.plantedDecoyCaller") {
				t.Fatalf("same-spelling decoy recorded as caller of %s", id)
			}
		}
	}
	config.Overlay = nil
	restored := boundMemberCallers(t, boundCompiledQuery(t, config))
	for id, edges := range restored.Callers {
		for _, edge := range edges {
			if strings.Contains(edge.Site, "r73_planted_caller.go") {
				t.Fatalf("plant removal left caller edge on %s", id)
			}
		}
	}
	t.Logf("CALLER_PLANT direct=recorded closure=recorded decoy=excluded removal=clean members=%d", len(restored.Callers))
}
