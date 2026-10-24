package testutil

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

// The bulk-input cancellation sweep. Its inventory is generated from the
// registries: one call for each argument of a registered function (SHOW
// FUNCTIONS) or procedure (the procedure registry) that takes a list, a map,
// any value, a string or an integer, given a large one (a large integer
// drives a large output). Each call is first run at a small size, where it
// must succeed: the large call is then a valid call, and any error it ends
// with that is not its cancellation is a failure.
//
// The large call is canceled, or its deadline passes, in the middle of its
// work: in the probe build (cancellationprobe) at a check of the
// cancellation owner, otherwise a moment after it starts. It must then end
// with the cancellation or deadline error, and its passes must stop within
// one observation interval of the owner (the probe counts the owner's checks
// each pass makes after the cancellation). Without the race detector it must
// also end within a wall-clock budget.

// BulkInputSize is the size of the large inputs.
type BulkInputSize struct {
	Items int // a list's items; a string has 4 bytes, and an integer is 8, per item
	Keys  int // a map's keys
	Nodes int // the fixture's nodes: a list of nodes or relationships
}

// DefaultBulkInputSize is large enough that an unobserved sort, or a quadratic
// pass, runs for seconds.
var DefaultBulkInputSize = BulkInputSize{Items: 1 << 19, Keys: 1 << 17, Nodes: 1 << 13}

// smallBulkInputSize is the size a call is validated at.
var smallBulkInputSize = BulkInputSize{Items: 4, Keys: 4, Nodes: 4}

// BulkInputCall is one generated call.
type BulkInputCall struct {
	Kind     string // function or procedure
	Name     string
	Argument string // the argument given the large input
	Input    string // the large input: list, floats, strings, maps, lists, map, text, count, nodes or relationships
	Writes   bool   // a procedure that may change the database: the fixture is rebuilt after it
	Query    string
	Params   map[string]interface{}
	// SmallQuery and SmallParams are the same call at the small size.
	SmallQuery  string
	SmallParams map[string]interface{}
}

// BulkInputLimitations are the calls (a name, or a name and the argument given
// the large input) whose work on the large input no checkpoint can interrupt:
// library work (the cancellation guard's limitations ledger,
// boundaryLimitations), a storage read that takes no context, or the
// statement front end reading statement text a procedure runs. The sweep
// still requires the call to end with the cancellation's error, or with the
// client error its finished work is refused with, and logs the time after
// the cancellation as a LIMITATION line instead of holding it to the return
// budget.
var BulkInputLimitations = map[string]string{
	"string.regexReplace":                              "regexp: RE2 matching runs to its end",
	"text.locate/quote":                                "regexp: compiling the quote's pattern runs to its end",
	"tx.setMetaData/data":                              "storage: the transaction measures its metadata (formatting each value) under its lock, then refuses metadata over its limit",
	"db.temporal.asOf/systemSequence":                  "storage: the snapshot read of a label's nodes takes no context",
	"db.temporal.assertNoOverlap/systemSequence":       "storage: the snapshot read of a label's nodes takes no context",
	"apoc.periodic.commit/statement":                   "the statement front end reads the statement text it is given to its end (linear in the text, which the dynamic statement limit bounds)",
	"db.index.fulltext.queryRelationships/queryString": "storage: reading every relationship of the graph (AllEdges) takes no context",
}

// bulkInputLimitation is the stated limitation of a call, if any.
func bulkInputLimitation(call BulkInputCall) (string, bool) {
	if reason, limited := BulkInputLimitations[call.Name+"/"+call.Argument]; limited {
		return reason, true
	}
	reason, limited := BulkInputLimitations[call.Name]
	return reason, limited
}

// bulkInputLazyGoroutines are the goroutines a first use may start for good;
// a canceled call that leaked its work would leave one per call.
const bulkInputLazyGoroutines = 8

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
		// Words, separators and spaces, so a split, replace or scan has work.
		"bulkText":  strings.Repeat("ab c", size.Items),
		"bulkCount": int64(size.Items) * 8,
	}
}

// bulkInputKinds is the large inputs an argument of the type takes.
func bulkInputKinds(typeName string) []string {
	t := strings.ToUpper(strings.TrimSpace(typeName))
	t = strings.TrimSpace(strings.TrimSuffix(strings.TrimSuffix(t, " NOT NULL"), "?"))
	switch {
	case t == "ANY":
		return []string{"list", "map", "text"}
	case t == "STRING":
		return []string{"text"}
	case t == "INTEGER":
		return []string{"count"}
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
// relationships, at most limit of them when limit is positive; one node and
// relationship), then the call.
func bulkInputQuery(arguments []string, limit int, call func(args string) string) string {
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
	within := func(variable string) string {
		if limit <= 0 {
			return ""
		}
		return fmt.Sprintf(" WHERE %s.i <= %d", variable, limit)
	}
	if uses("nodes") {
		bind("MATCH (bulkNode:BulkInput)"+within("bulkNode"), []string{"collect(bulkNode) AS nodes"}, []string{"nodes"})
	}
	if uses("relationships") {
		bind("MATCH (:BulkInput)-[bulkRel:NEXT]->(:BulkInput)"+within("bulkRel"), []string{"collect(bulkRel) AS relationships"}, []string{"relationships"})
	}
	if uses("one", "oneRel") {
		bind("MATCH (one:BulkInput {i: 1})-[oneRel:NEXT]->()", []string{"one", "oneRel"}, []string{"one", "oneRel"})
	}
	return prefix.String() + call(strings.Join(arguments, ", "))
}

// BulkInputCalls generates the sweep's calls from the executor's registries.
func BulkInputCalls(t testing.TB, executor *cypher.StorageExecutor, size BulkInputSize) []BulkInputCall {
	t.Helper()
	inputs, small := bulkInputs(size), bulkInputs(smallBulkInputSize)
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
				params, smallParams := map[string]interface{}{}, map[string]interface{}{}
				if param != "" {
					params[param], smallParams[param] = inputs[param], small[param]
				}
				calls = append(calls, BulkInputCall{
					Kind: kind, Name: name, Argument: argument.name, Input: input, Writes: writes,
					Query: bulkInputQuery(values, 0, call), Params: params,
					SmallQuery: bulkInputQuery(values, smallBulkInputSize.Nodes, call), SmallParams: smallParams,
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

// BulkInputRun runs a statement with its parameters under ctx and returns
// the statement's own error where it executes (not a transport's). It calls
// started once the statement has begun there, and returns once the statement
// has ended there.
type BulkInputRun func(ctx context.Context, query string, params map[string]interface{}, started func()) error

// BulkInputBudget is when the sweep cancels a call after it starts (when no
// check of the owner cancels it first), and how soon after its cancellation
// the call must end. Return applies without the race detector only.
type BulkInputBudget struct {
	Cancel, Return time.Duration
	// Raced is set for a transport whose cancellation reaches the server
	// after the client sends it (Bolt's RESET, an HTTP client that leaves):
	// a statement that ends within Return of the cancellation with its own
	// outcome finished as the cancellation arrived, and passes.
	Raced bool
}

// DefaultBulkInputBudget is the budget of a transport that reports a call's
// start once its parameters are decoded.
var DefaultBulkInputBudget = BulkInputBudget{Cancel: 20 * time.Millisecond, Return: 100 * time.Millisecond}

// RacedBulkInputBudget is DefaultBulkInputBudget for a transport whose
// cancellation travels to the server (Bolt's RESET).
var RacedBulkInputBudget = BulkInputBudget{Cancel: 20 * time.Millisecond, Return: 100 * time.Millisecond, Raced: true}

// DecodedBulkInputBudget is the budget for a transport that reports a call's
// start before it decodes the parameters: the cancellation comes after the
// large input's decoding.
var DecodedBulkInputBudget = BulkInputBudget{Cancel: 150 * time.Millisecond, Return: 100 * time.Millisecond, Raced: true}

// BulkInputReport counts the calls: not valid at the small size, ended before
// their cancellation, and canceled (at a check of the owner, or by the
// clock).
type BulkInputReport struct {
	Calls, Invalid, InvalidLarge, Completed, Canceled, CanceledAtCheck int
	// Raced counts the calls of a raced transport that ended with their own
	// outcome within the budget of the cancellation.
	Raced       int
	Slowest     time.Duration
	SlowestCall string
	MostChecks  int
	// Limited counts the canceled calls of a stated limitation.
	Limited int
}

// sweepContext is canceled, or passes its deadline, when the sweep says.
type sweepContext struct {
	context.Context
	done     chan struct{}
	once     sync.Once
	deadline bool
	err      atomic.Value
	at       atomic.Int64
}

func newSweepContext(deadline bool) *sweepContext {
	return &sweepContext{Context: context.Background(), done: make(chan struct{}), deadline: deadline}
}

func (c *sweepContext) Done() <-chan struct{} { return c.done }

func (c *sweepContext) Err() error {
	if err, ok := c.err.Load().(error); ok {
		return err
	}
	return nil
}

// end cancels the context, or passes its deadline, once.
func (c *sweepContext) end() {
	c.once.Do(func() {
		err := context.Canceled
		if c.deadline {
			err = context.DeadlineExceeded
		}
		c.at.Store(time.Now().UnixNano())
		c.err.Store(err)
		close(c.done)
	})
}

// RunBulkInputSweep runs each call through run, validating it at the small
// size first, and requires its cancellation (alternately a cancellation and a
// passed deadline) to end it with that error, promptly. After a write
// procedure, rebuild restores the fixture.
func RunBulkInputSweep(t *testing.T, calls []BulkInputCall, run BulkInputRun, budget BulkInputBudget, rebuild func(t *testing.T)) BulkInputReport {
	t.Helper()
	var report BulkInputReport
	// No canceled call leaves work behind: once the sweep ends, the process
	// runs no more goroutines than before it, apart from the few a first use
	// starts for good (caches, pools).
	goroutines := runtime.NumGoroutine()
	defer func() {
		require.Eventuallyf(t, func() bool { return runtime.NumGoroutine() <= goroutines+bulkInputLazyGoroutines },
			10*time.Second, 50*time.Millisecond, "goroutines after the sweep: %d, before it: %d", runtime.NumGoroutine(), goroutines)
	}()
	for index, call := range calls {
		call, deadline := call, index%2 == 1
		t.Run(call.ID(), func(t *testing.T) {
			report.Calls++
			err := run(context.Background(), call.SmallQuery, call.SmallParams, func() {})
			if call.Writes && rebuild != nil {
				rebuild(t)
			}
			if err != nil {
				report.Invalid++
				t.Skipf("not a valid call at the small size: %v", err)
			}

			ctx := newSweepContext(deadline)
			var atCheck atomic.Bool
			probeMost()
			probeArm(probeInterval/2+1, func() {
				atCheck.Store(true)
				ctx.end()
			})
			var once sync.Once
			var timer *time.Timer
			started := func() {
				once.Do(func() { timer = time.AfterFunc(budget.Cancel, ctx.end) })
			}
			err = run(ctx, call.Query, call.Params, started)
			ended := time.Now()
			// Ordered after started (sync.Once): timer is set when it ran.
			once.Do(func() {})
			if timer != nil {
				timer.Stop()
			}
			most := probeMost()
			if call.Writes && rebuild != nil {
				rebuild(t)
			}
			at := ctx.at.Load()
			if at > ended.UnixNano() {
				// The cancellation fired after the call ended, before its
				// timer stopped: the call ended before its cancellation.
				at = 0
			}
			if at == 0 && err != nil {
				// The large input itself is rejected, before any cancellation:
				// a client error (a name or list beyond the procedure's limits)
				// is the call's outcome at this size, as at the small one.
				if code, _ := nornicerrors.Neo4jStatus(err); strings.HasPrefix(code, "Neo.ClientError.") {
					report.InvalidLarge++
					t.Skipf("not a valid call at the large size: %v", err)
				}
			}
			if at == 0 {
				report.Completed++
				require.NoErrorf(t, err, "%s ended with an error before its cancellation: %s", call.ID(), call.Query)
				return
			}
			report.Canceled++
			if atCheck.Load() {
				report.CanceledAtCheck++
			}
			reason, limited := bulkInputLimitation(call)
			code, _ := nornicerrors.Neo4jStatus(err)
			refused := limited && strings.HasPrefix(code, "Neo.ClientError.")
			canceled := errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
			raced := budget.Raced && !canceled && !refused && ended.Sub(time.Unix(0, at)) <= budget.Return
			require.Truef(t, canceled || refused || raced,
				"%s ended with %s after its %s, want the cancellation or deadline error: %s", call.ID(), sweepError(err), sweepEnd(deadline), call.Query)
			if raced {
				report.Raced++
				t.Logf("RACED %s ended with its own outcome %v after its %s: %s", call.ID(), ended.Sub(time.Unix(0, at)), sweepEnd(deadline), sweepError(err))
			}
			if most > report.MostChecks {
				report.MostChecks = most
			}
			if probeBuilt {
				require.LessOrEqualf(t, most, probeInterval, "%s: a pass made %d checks of the owner after its %s (interval %d): %s",
					call.ID(), most, sweepEnd(deadline), probeInterval, call.Query)
			}
			late := ended.Sub(time.Unix(0, at))
			if late > report.Slowest {
				report.Slowest, report.SlowestCall = late, call.ID()
			}
			if limited {
				report.Limited++
				t.Logf("LIMITATION %s ran %v after its %s (%s), ending with %s", call.ID(), late, sweepEnd(deadline), reason, sweepError(err))
				return
			}
			if !raceEnabled {
				require.LessOrEqualf(t, late, budget.Return, "%s ran %v after its %s (budget %v): %s", call.ID(), late, sweepEnd(deadline), budget.Return, call.Query)
			}
		})
	}
	t.Logf("bulk-input sweep: %d calls, %d not valid at the small size, %d not valid at the large size, %d ended before their cancellation, %d canceled (%d at a check of the owner, %d of a stated limitation, %d ending as the cancellation arrived); most checks after a cancellation %d; slowest return %v (%s)",
		report.Calls, report.Invalid, report.InvalidLarge, report.Completed, report.Canceled, report.CanceledAtCheck, report.Limited, report.Raced, report.MostChecks, report.Slowest, report.SlowestCall)
	return report
}

// sweepError is an outcome's error for a log line: an error that quotes a
// large input is cut, so the line stays readable.
func sweepError(err error) string {
	if err == nil {
		return "<nil>"
	}
	text := err.Error()
	if len(text) > 300 {
		return fmt.Sprintf("%s… (%d bytes)", text[:300], len(text))
	}
	return text
}

func sweepEnd(deadline bool) string {
	if deadline {
		return "deadline"
	}
	return "cancellation"
}
