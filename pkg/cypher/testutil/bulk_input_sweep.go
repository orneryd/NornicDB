package testutil

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/stretchr/testify/require"
)

// The bulk-input cancellation sweep. Its inventory is generated from the
// registries: one call for each argument of a registered function (SHOW
// FUNCTIONS) or procedure (the procedure registry) that takes a list, a map
// or any value, given a large one. Each call runs through a transport and is
// canceled shortly after it starts; it must end within a budget of its
// cancellation. A pass over its input that does not observe the statement
// runs to its end instead, far past the budget.

// BulkInputSize is the size of the large inputs.
type BulkInputSize struct {
	Items int // a list's items
	Keys  int // a map's keys
	Nodes int // the fixture's nodes: a list of nodes or relationships
}

// DefaultBulkInputSize is large enough that an unobserved sort, or a quadratic
// pass, runs for seconds.
var DefaultBulkInputSize = BulkInputSize{Items: 1 << 19, Keys: 1 << 17, Nodes: 1 << 13}

// BulkInputCall is one generated call.
type BulkInputCall struct {
	Kind     string // function or procedure
	Name     string
	Argument string // the argument given the large input
	Input    string // the large input: list, floats, strings, maps, lists, map, nodes or relationships
	Writes   bool   // a procedure that may change the database: the fixture is rebuilt after it
	Query    string
	Params   map[string]interface{}
}

// ID names the call in a subtest.
func (c BulkInputCall) ID() string {
	return c.Kind + "/" + c.Name + "/" + c.Argument + "/" + c.Input
}

// BulkInputFixture is the statements that build the fixture graph (Nodes
// :BulkInput nodes in a chain of :NEXT relationships), the first removing an
// earlier one.
func BulkInputFixture(size BulkInputSize) []string {
	return []string{
		"MATCH (n:BulkInput) DETACH DELETE n",
		fmt.Sprintf("UNWIND range(1, %d) AS i CREATE (n:BulkInput {i: i, name: toString(i)}) "+
			"WITH collect(n) AS nodes UNWIND range(0, size(nodes) - 2) AS k "+
			"WITH nodes[k] AS a, nodes[k + 1] AS b CREATE (a)-[:NEXT {i: a.i}]->(b)", size.Nodes),
	}
}

// bulkInputs are the large values a call's parameters pass.
func bulkInputs(size BulkInputSize) map[string]interface{} {
	list := make([]interface{}, size.Items)
	floats := make([]interface{}, size.Items)
	strs := make([]interface{}, size.Items)
	maps := make([]interface{}, size.Items)
	lists := make([]interface{}, size.Items)
	for i := range list {
		// Descending, so a sort has work to do.
		value := int64(size.Items - i)
		list[i] = value
		floats[i] = float64(value) / 2
		strs[i] = fmt.Sprintf("s%07d", value)
		maps[i] = map[string]interface{}{"k": value}
		lists[i] = []interface{}{value}
	}
	object := make(map[string]interface{}, size.Keys)
	for i := 0; i < size.Keys; i++ {
		object[fmt.Sprintf("k%07d", i)] = int64(i)
	}
	return map[string]interface{}{
		"bulkList": list, "bulkFloats": floats, "bulkStrings": strs,
		"bulkMaps": maps, "bulkLists": lists, "bulkMap": object,
	}
}

// bulkInputKinds is the large inputs an argument of the type takes.
func bulkInputKinds(typeName string) []string {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	t = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(t, " NOT NULL"), "?"))
	switch {
	case t == "ANY":
		return []string{"list", "map"}
	case strings.HasPrefix(t, "MAP"):
		return []string{"map"}
	case strings.HasPrefix(t, "LIST"):
		element := strings.TrimSpace(strings.TrimPrefix(t, "LIST"))
		element = strings.TrimSpace(strings.TrimPrefix(element, "OF"))
		element = strings.TrimSuffix(strings.TrimPrefix(element, "<"), ">")
		element = strings.TrimSpace(strings.TrimSuffix(element, "?"))
		switch {
		case element == "STRING":
			return []string{"strings"}
		case element == "FLOAT":
			return []string{"floats"}
		case strings.HasPrefix(element, "MAP"):
			return []string{"maps"}
		case strings.HasPrefix(element, "LIST"):
			return []string{"lists"}
		case element == "NODE":
			return []string{"nodes"}
		case element == "RELATIONSHIP":
			return []string{"relationships"}
		}
		return []string{"list"}
	}
	return nil
}

// bulkInputExpr is the expression of a large input, and the binding it needs.
func bulkInputExpr(kind string) (expr, param string) {
	switch kind {
	case "nodes", "relationships":
		return kind, ""
	}
	param = "bulk" + strings.ToUpper(kind[:1]) + kind[1:]
	return "$" + param, param
}

// smallArgument is a small value of the type, and the binding it needs.
func smallArgument(typeName string) string {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	t = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(t, " NOT NULL"), "?"))
	switch {
	case t == "STRING":
		return "'a'"
	case t == "INTEGER", t == "NUMBER", t == "ANY":
		return "1"
	case t == "FLOAT":
		return "1.0"
	case t == "BOOLEAN":
		return "true"
	case strings.HasPrefix(t, "MAP"):
		return "{}"
	case strings.HasPrefix(t, "LIST"):
		return "[]"
	case t == "NODE":
		return "one"
	case t == "RELATIONSHIP":
		return "oneRel"
	case t == "DATE":
		return "date('2020-01-01')"
	case t == "DATETIME", t == "ZONED DATETIME":
		return "datetime('2020-01-01T00:00:00Z')"
	case t == "LOCALDATETIME", t == "LOCAL DATETIME":
		return "localdatetime('2020-01-01T00:00:00')"
	case t == "TIME", t == "ZONED TIME":
		return "time('00:00:00Z')"
	case t == "LOCALTIME", t == "LOCAL TIME":
		return "localtime('00:00:00')"
	case t == "DURATION":
		return "duration('P1D')"
	case t == "POINT":
		return "point({x: 0.0, y: 0.0})"
	}
	return "null"
}

type bulkInputArgument struct {
	name, typeName string
	defaulted      bool
}

// bulkInputQuery binds what the arguments use (the fixture's nodes and
// relationships, one node and relationship), then the call.
func bulkInputQuery(arguments []string, call func(args string) string) string {
	var prefix strings.Builder
	var carried []string
	// bind matches clause and carries the earlier bindings with projections.
	bind := func(clause string, projections, names []string) {
		prefix.WriteString(clause + " WITH " + strings.Join(append(append([]string(nil), carried...), projections...), ", ") + " ")
		carried = append(carried, names...)
	}
	uses := func(names ...string) bool {
		for _, argument := range arguments {
			for _, name := range names {
				if argument == name {
					return true
				}
			}
		}
		return false
	}
	if uses("nodes") {
		bind("MATCH (bulkNode:BulkInput)", []string{"collect(bulkNode) AS nodes"}, []string{"nodes"})
	}
	if uses("relationships") {
		bind("MATCH (:BulkInput)-[bulkRel:NEXT]->(:BulkInput)", []string{"collect(bulkRel) AS relationships"}, []string{"relationships"})
	}
	if uses("one", "oneRel") {
		bind("MATCH (one:BulkInput {i: 1})-[oneRel:NEXT]->()", []string{"one", "oneRel"}, []string{"one", "oneRel"})
	}
	return prefix.String() + call(strings.Join(arguments, ", "))
}

// BulkInputCalls generates the sweep's calls from the executor's registries.
func BulkInputCalls(t testing.TB, executor *cypher.StorageExecutor, size BulkInputSize) []BulkInputCall {
	t.Helper()
	inputs := bulkInputs(size)
	var calls []BulkInputCall
	generate := func(kind, name string, arguments []bulkInputArgument, writes bool, call func(args string) string) {
		for index, argument := range arguments {
			for _, input := range bulkInputKinds(argument.typeName) {
				expr, param := bulkInputExpr(input)
				var values []string
				for other, given := range arguments {
					if other > index && given.defaulted {
						break
					}
					if other == index {
						values = append(values, expr)
					} else {
						values = append(values, smallArgument(given.typeName))
					}
				}
				params := map[string]interface{}{}
				if param != "" {
					params[param] = inputs[param]
				}
				calls = append(calls, BulkInputCall{
					Kind: kind, Name: name, Argument: argument.name, Input: input, Writes: writes,
					Query: bulkInputQuery(values, call), Params: params,
				})
			}
		}
	}

	result, err := executor.Execute(context.Background(), "SHOW FUNCTIONS YIELD name, argumentDescription RETURN name, argumentDescription", nil)
	require.NoError(t, err)
	for _, row := range result.Rows {
		name, _ := row[0].(string)
		described, _ := row[1].([]interface{})
		arguments := make([]bulkInputArgument, 0, len(described))
		for _, entry := range described {
			fields, _ := entry.(map[string]interface{})
			argumentName, _ := fields["name"].(string)
			typeName, _ := fields["type"].(string)
			_, defaulted := fields["default"]
			arguments = append(arguments, bulkInputArgument{name: argumentName, typeName: typeName, defaulted: defaulted})
		}
		generate("function", name, arguments, false, func(args string) string {
			return "WITH " + name + "(" + args + ") AS result RETURN result IS NULL AS done"
		})
	}

	for _, procedure := range cypher.ListRegisteredProcedures() {
		arguments := make([]bulkInputArgument, 0, len(procedure.Params))
		for _, param := range procedure.Params {
			arguments = append(arguments, bulkInputArgument{name: param.Name, typeName: param.Type, defaulted: param.Optional || param.Default != ""})
		}
		name, returns := procedure.Name, procedure.Returns
		writes := procedure.Mode != cypher.ProcedureModeRead
		generate("procedure", name, arguments, writes, func(args string) string {
			if len(returns) == 0 {
				return "CALL " + name + "(" + args + ") RETURN 1 AS done"
			}
			return "CALL " + name + "(" + args + ") YIELD `" + returns[0].Name + "` RETURN count(*) AS rows"
		})
	}
	sort.SliceStable(calls, func(i, j int) bool { return calls[i].ID() < calls[j].ID() })
	return calls
}

// BulkInputRun runs a statement with its parameters under ctx. It calls
// started once the statement has begun where it executes, and returns once
// the statement has ended there: a transport's client may stop waiting
// sooner.
type BulkInputRun func(ctx context.Context, query string, params map[string]interface{}, started func()) error

// BulkInputBudget is when the sweep cancels a call, after it starts, and how
// soon after that the call must end.
type BulkInputBudget struct {
	Cancel, Return time.Duration
}

// DefaultBulkInputBudget scales with the race detector, which slows every
// pass between two observations alike.
var DefaultBulkInputBudget = BulkInputBudget{Cancel: 20 * time.Millisecond, Return: 500 * time.Millisecond * raceSlowdown}

// DecodedBulkInputBudget is the budget for a transport that reports a call's
// start before it decodes the parameters: the cancellation comes after the
// large input's decoding.
var DecodedBulkInputBudget = BulkInputBudget{Cancel: 150 * time.Millisecond * raceSlowdown, Return: 500 * time.Millisecond * raceSlowdown}

// BulkInputReport counts the calls: ended before their cancellation (with
// or without an error), and canceled.
type BulkInputReport struct {
	Calls, Completed, Errored, Canceled int
	Slowest                             time.Duration
	SlowestCall                         string
}

// RunBulkInputSweep runs each call through run, canceling it budget.Cancel
// after it starts, and requires it to end within budget.Return of that.
// After a write procedure, rebuild restores the fixture.
func RunBulkInputSweep(t *testing.T, calls []BulkInputCall, run BulkInputRun, budget BulkInputBudget, rebuild func(t *testing.T)) BulkInputReport {
	t.Helper()
	var report BulkInputReport
	for _, call := range calls {
		call := call
		t.Run(call.ID(), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var once sync.Once
			var timer *time.Timer
			var canceledAt atomic.Int64
			started := func() {
				once.Do(func() {
					timer = time.AfterFunc(budget.Cancel, func() {
						canceledAt.Store(time.Now().UnixNano())
						cancel()
					})
				})
			}
			err := run(ctx, call.Query, call.Params, started)
			ended := time.Now()
			// Ordered after started (sync.Once): timer is set when it ran.
			once.Do(func() {})
			if timer != nil {
				timer.Stop()
			}
			report.Calls++
			at := canceledAt.Load()
			if at == 0 {
				report.Completed++
				if err != nil {
					report.Errored++
					t.Logf("ended before its cancellation with an error: %v", err)
				}
			} else {
				report.Canceled++
				late := ended.Sub(time.Unix(0, at))
				if late > report.Slowest {
					report.Slowest, report.SlowestCall = late, call.ID()
				}
				if late > budget.Return/2 {
					t.Logf("slow return: %v after its cancellation", late)
				}
				require.LessOrEqualf(t, late, budget.Return, "%s ran %v after its cancellation (budget %v): %s", call.ID(), late, budget.Return, call.Query)
			}
			if call.Writes && rebuild != nil {
				rebuild(t)
			}
		})
	}
	t.Logf("bulk-input sweep: %d calls, %d ended before their cancellation (%d with an error), %d canceled; slowest return %v (%s)",
		report.Calls, report.Completed, report.Errored, report.Canceled, report.Slowest, report.SlowestCall)
	return report
}
