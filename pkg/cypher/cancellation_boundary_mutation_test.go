package cypher

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// Each mutation adds one data-sized pass outside the cancellation owner to
// the type-checked production package; the guard must report it. Its
// observed twin, the same work through the owner, must pass.
var cancellationBoundaryMutations = []struct {
	name, mutation, observed, want string
}{
	{
		// The audit's PageRank form: the check sits in an inner loop, so a node
		// with no edges never reaches it.
		name: "nested_empty_edges",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/storage"
)
func mutationRanks(ctx context.Context, nodes []*storage.Node, edges map[storage.NodeID][]*storage.Edge) map[storage.NodeID]float64 {
	pass := bulk.NewCheckpoint(ctx)
	ranks := map[storage.NodeID]float64{}
	for _, node := range nodes {
		for range edges[node.ID] {
			pass.Check()
			ranks[node.ID]++
		}
	}
	return ranks
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"github.com/orneryd/nornicdb/pkg/storage"
)
func mutationRanks(ctx context.Context, nodes []*storage.Node, edges map[storage.NodeID][]*storage.Edge) map[storage.NodeID]float64 {
	pass := bulk.NewCheckpoint(ctx)
	ranks := map[storage.NodeID]float64{}
	for _, node := range observedItems(&pass, nodes) {
		for range observedItems(&pass, edges[node.ID]) {
			ranks[node.ID]++
		}
	}
	return ranks
}
`,
		want: "loop range nodes",
	},
	{
		name: "reflection_map_range",
		mutation: `package cypher
import "reflect"
func mutationReflectKeys(value interface{}) int {
	n := 0
	for it := reflect.ValueOf(value).MapRange(); it.Next(); {
		n++
	}
	return n
}
`,
		observed: `package cypher
import (
	"context"
	"reflect"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationReflectKeys(ctx context.Context, value interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := 0
	for it := reflect.ValueOf(value).MapRange(); it.Next(); {
		pass.Check()
		n++
	}
	return n
}
`,
		want: "loop for it.Next()",
	},
	{
		name: "aliased_length",
		mutation: `package cypher
func mutationAliased(list []interface{}) int {
	n := len(list)
	total := 0
	for i := 0; i < n; i++ {
		total++
	}
	return total
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationAliased(ctx context.Context, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := len(list)
	total := 0
	for i := 0; i < n; i++ {
		pass.Check()
		total++
	}
	return total
}
`,
		want: "loop for i < n",
	},
	{
		name: "reverse_length",
		mutation: `package cypher
func mutationReverse(list []interface{}) []interface{} {
	out := make([]interface{}, 0, len(list))
	for i := len(list) - 1; i >= 0; i-- {
		out = append(out, list[i])
	}
	return out
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationReverse(ctx context.Context, list []interface{}) []interface{} {
	pass := bulk.NewCheckpoint(ctx)
	out := make([]interface{}, 0, len(list))
	for i := len(list) - 1; i >= 0; i-- {
		pass.Check()
		out = append(out, list[i])
	}
	return out
}
`,
		want: "loop for i >= 0",
	},
	{
		name: "range_over_length",
		mutation: `package cypher
func mutationRangeLen(list []interface{}) int {
	total := 0
	for i := range len(list) {
		total += i
	}
	return total
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationRangeLen(ctx context.Context, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	total := 0
	for i := range observedCount(&pass, len(list)) {
		total += i
	}
	return total
}
`,
		want: "loop range len(list)",
	},
	{
		// A count a Cypher value supplies drives the output size.
		name: "scalar_driven_output",
		mutation: `package cypher
func mutationFill(value interface{}) []interface{} {
	n, _ := value.(int64)
	out := []interface{}{}
	for i := int64(0); i < n; i++ {
		out = append(out, i)
	}
	return out
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationFill(ctx context.Context, value interface{}) []interface{} {
	pass := bulk.NewCheckpoint(ctx)
	n, _ := value.(int64)
	out := []interface{}{}
	for i := int64(0); i < n; i++ {
		pass.Check()
		out = append(out, i)
	}
	return out
}
`,
		want: "loop for i < n",
	},
	{
		// One whole-list copy inside an observed pass is still a pass.
		name: "copy_on_first_change",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationFirstChange(ctx context.Context, list []interface{}) []interface{} {
	pass := bulk.NewCheckpoint(ctx)
	var out []interface{}
	for index, item := range list {
		pass.Check()
		if item == nil && out == nil {
			out = append([]interface{}(nil), list...)
			out[index] = false
		}
	}
	return out
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationFirstChange(ctx context.Context, list []interface{}) ([]interface{}, error) {
	pass := bulk.NewCheckpoint(ctx)
	var out []interface{}
	for index, item := range list {
		pass.Check()
		if item == nil && out == nil {
			clone, err := bulk.Clone(ctx, list)
			if err != nil {
				return nil, err
			}
			out = clone
			out[index] = false
		}
	}
	return out, nil
}
`,
		want: "append of all of list",
	},
	{
		name: "json_encoding",
		mutation: `package cypher
import "encoding/json"
func mutationEncode(value interface{}) []byte {
	encoded, _ := json.Marshal(value)
	return encoded
}
`,
		observed: `package cypher
func mutationEncode(value interface{}) interface{} {
	return value
}
`,
		want: "encoding/json.Marshal(value)",
	},
	{
		// A helper that recovers the stop swallows it, or returns a partial result.
		name: "swallowed_stop",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationSwallow(ctx context.Context, list []interface{}) (n int, err error) {
	defer bulk.RecoverStop(&err)
	pass := bulk.NewCheckpoint(ctx)
	for range observedItems(&pass, list) {
		n++
	}
	return n, nil
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationSwallow(ctx context.Context, list []interface{}) (n int) {
	pass := bulk.NewCheckpoint(ctx)
	for range observedItems(&pass, list) {
		n++
	}
	return n
}
`,
		want: "recover the owner's stop recovered outside a boundary",
	},
	{
		// Workers share one checkpoint and its counter.
		name: "shared_worker_checkpoint",
		mutation: `package cypher
import (
	"context"
	"sync"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationWorkers(ctx context.Context, parts [][]interface{}) {
	pass := bulk.NewCheckpoint(ctx)
	var wg sync.WaitGroup
	for _, part := range parts {
		wg.Add(1)
		go func() {
			var err error
			defer wg.Done()
			defer bulk.RecoverStop(&err)
			for range part {
				pass.Check()
			}
		}()
	}
	wg.Wait()
}
`,
		observed: `package cypher
import (
	"context"
	"sync"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationWorkers(ctx context.Context, parts [][]interface{}) {
	var wg sync.WaitGroup
	coordinator := bulk.NewCheckpoint(ctx)
	for _, part := range observedItems(&coordinator, parts) {
		wg.Add(1)
		go func() {
			var err error
			defer wg.Done()
			defer bulk.RecoverStop(&err)
			pass := bulk.NewCheckpoint(ctx)
			for range part {
				pass.Check()
			}
		}()
	}
	wg.Wait()
}
`,
		want: "go a worker checks a checkpoint it shares: pass",
	},
	{
		// A second recursive walker of Cypher values.
		name: "duplicate_value_walker",
		mutation: `package cypher
func mutationCount(value interface{}) int {
	switch typed := value.(type) {
	case []interface{}:
		n := 0
		for _, item := range observedItems(nil, typed) {
			n += mutationCount(item)
		}
		return n
	case map[string]interface{}:
		n := 0
		for _, item := range observedEntries(nil, typed) {
			n += mutationCount(item)
		}
		return n
	}
	return 1
}
`,
		observed: `package cypher
import (
	"context"
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)
func mutationCount(ctx context.Context, value interface{}) int {
	n := 0
	cypherfn.VisitValue(valueWalk(ctx), value, func(value interface{}) (bool, bool) {
		switch value.(type) {
		case []interface{}, map[string]interface{}:
			return true, false
		}
		n++
		return false, false
	})
	return n
}
`,
		want: "duplicate a recursive walk of Cypher values outside the value walker",
	},
	{
		name: "deep_equal_of_values",
		mutation: `package cypher
import "reflect"
func mutationEqual(left, right interface{}) bool {
	return reflect.DeepEqual(left, right)
}
`,
		observed: `package cypher
import "context"
func mutationEqual(ctx context.Context, left, right interface{}) bool {
	return mergeValueKey(valueWalk(ctx), left) == mergeValueKey(valueWalk(ctx), right)
}
`,
		want: "duplicate a deep comparison of values outside the equality owner",
	},
	{
		// A marker on no pass.
		name: "stale_marker",
		mutation: `package cypher
func mutationLabels() int {
	n := 0
	// bounded: at most 3
	n++
	return n
}
`,
		observed: `package cypher
func mutationLabels() int {
	n := 0
	n++
	return n
}
`,
		want: "marker a bounded: marker on no pass (stale)",
	},
	{
		// The round-8 review's fixture: a registered function summing its list.
		name: "registered_uncancellable",
		mutation: `package cypher
import cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
func init() { cypherfn.Register("review.uncancellable", reviewUncancellable) }
func reviewUncancellable(ctx cypherfn.Context, args []string) (interface{}, error) {
	value, err := ctx.Eval(args[0])
	if err != nil {
		return nil, err
	}
	list, _ := value.([]interface{})
	var total int64
	for _, item := range list {
		if n, ok := item.(int64); ok {
			total += n
		}
	}
	return total, nil
}
`,
		observed: `package cypher
import (
	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func init() { cypherfn.Register("review.cancellable", reviewCancellable) }
func reviewCancellable(ctx cypherfn.Context, args []string) (interface{}, error) {
	value, err := ctx.Eval(args[0])
	if err != nil {
		return nil, err
	}
	list, _ := value.([]interface{})
	var total int64
	pass := bulk.NewCheckpoint(ctx.StatementContext())
	for _, item := range list {
		pass.Check()
		if n, ok := item.(int64); ok {
			total += n
		}
	}
	return total, nil
}
`,
		want: "loop range list",
	},
	{
		name: "linear_list_loop",
		mutation: `package cypher
func mutationListLength(list []interface{}) int {
	n := 0
	for _, item := range list {
		if item != nil {
			n++
		}
	}
	return n
}
`,
		observed: `package cypher
func mutationListLength(work *materializedRowCheckpoint, list []interface{}) int {
	n := 0
	for _, item := range list {
		work.Check()
		if item != nil {
			n++
		}
	}
	return n
}
`,
		want: "loop range list",
	},
	{
		name: "unchunked_string_split",
		mutation: `package cypher
import "strings"
func mutationSplit(value interface{}) []string {
	text, _ := value.(string)
	return strings.Split(text, ",")
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationSplit(ctx context.Context, value interface{}) ([]string, error) {
	text, _ := value.(string)
	return bulk.Split(ctx, text, ",")
}
`,
		want: "call strings.Split(text)",
	},
	{
		name: "unchunked_string_replace",
		mutation: `package cypher
import "strings"
func mutationReplace(value interface{}) string {
	text, _ := value.(string)
	return strings.ReplaceAll(text, "a", "bc")
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationReplace(ctx context.Context, value interface{}) (string, error) {
	text, _ := value.(string)
	return bulk.ReplaceAll(ctx, text, "a", "bc")
}
`,
		want: "call strings.ReplaceAll(text)",
	},
	{
		// A scan of a data string, however cheap each byte: no speed bounds it.
		name: "unobserved_string_scan",
		mutation: `package cypher
func mutationCommas(value interface{}) int {
	text, _ := value.(string)
	n := 0
	for i := 0; i < len(text); i++ {
		if text[i] == ',' {
			n++
		}
	}
	return n
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationCommas(ctx context.Context, value interface{}) int {
	text, _ := value.(string)
	n := 0
	pass := bulk.NewCheckpoint(ctx)
	for i := 0; i < len(text); i++ {
		pass.Check()
		if text[i] == ',' {
			n++
		}
	}
	return n
}
`,
		want: "loop for i < len(text)",
	},
	{
		name: "context_free_adjacency_filter",
		mutation: `package cypher
import "github.com/orneryd/nornicdb/pkg/storage"
func mutationAdjacency(edges []*storage.Edge, relType string) []*storage.Edge {
	var out []*storage.Edge
	for _, edge := range edges {
		if edge.Type == relType {
			out = append(out, edge)
		}
	}
	return out
}
`,
		observed: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"github.com/orneryd/nornicdb/pkg/storage"
)
func mutationAdjacency(ctx context.Context, edges []*storage.Edge, relType string) []*storage.Edge {
	var out []*storage.Edge
	pass := bulk.NewCheckpoint(ctx)
	for _, edge := range edges {
		pass.Check()
		if edge.Type == relType {
			out = append(out, edge)
		}
	}
	return out
}
`,
		want: "loop range edges",
	},
	{
		name: "unchecked_export_loop",
		mutation: `package cypher
import (
	"encoding/json"
	"strings"
	"github.com/orneryd/nornicdb/pkg/storage"
)
func mutationExport(nodes []*storage.Node) string {
	var out strings.Builder
	for _, node := range nodes {
		encoded, _ := json.Marshal(node.Properties)
		out.Write(encoded)
	}
	return out.String()
}
`,
		observed: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"strings"
	"github.com/orneryd/nornicdb/pkg/storage"
)
func mutationExport(ctx context.Context, nodes []*storage.Node) string {
	var out strings.Builder
	pass := bulk.NewCheckpoint(ctx)
	for _, node := range nodes {
		pass.Check()
		encoded, _ := encodeJSONValue(&pass, node.Properties, "", "")
		writeObservedBytes(&pass, &out, encoded)
	}
	return out.String()
}
`,
		want: "loop range nodes",
	},
	{
		name: "bare_goroutine",
		mutation: `package cypher
func mutationWorker(results chan<- int, list []interface{}) {
	go func() {
		results <- len(list)
	}()
}
`,
		observed: `package cypher
import "github.com/orneryd/nornicdb/pkg/cypher/bulk"
func mutationWorker(results chan<- error, list []interface{}) {
	go func() {
		var err error
		defer func() { results <- err }()
		defer bulk.RecoverStop(&err)
		_ = len(list)
	}()
}
`,
		want: "go goroutine outside the owner's boundary",
	},
	{
		name: "marker_on_data_pass",
		mutation: `package cypher
func mutationMarked(list []interface{}) int {
	n := 0
	// bounded: the list is small
	for range list {
		n++
	}
	return n
}
`,
		observed: `package cypher
import "sort"
const mutationMaxLabels = 64
func mutationMarked(labels []string) []string {
	// bounded: mutationMaxLabels, the labels a schema declares
	sort.Strings(labels)
	return labels
}
`,
		want: "a bounded: marker cannot bound a pass over data",
	},
	{
		// An exported entry whose stop would reach a caller outside the package.
		name: "stop_escapes_exported_entry",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func MutationEntry(ctx context.Context, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := 0
	for range list {
		pass.Check()
		n++
	}
	return n
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func MutationEntry(ctx context.Context, list []interface{}) (n int, err error) {
	defer bulk.RecoverStop(&err)
	pass := bulk.NewCheckpoint(ctx)
	for range list {
		pass.Check()
		n++
	}
	return n, nil
}
`,
		want: "escape an exported entry can stop",
	},
	{
		// The stop is raised in a callback a package helper calls.
		name: "stop_escapes_through_callback",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationEach(list []interface{}, visit func(interface{})) {
	for _, item := range list {
		visit(item)
	}
}
func MutationVisit(ctx context.Context, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := 0
	mutationEach(list, func(interface{}) {
		pass.Check()
		n++
	})
	return n
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationEach(ctx context.Context, list []interface{}, visit func(interface{})) {
	pass := bulk.NewCheckpoint(ctx)
	for _, item := range list {
		pass.Check()
		visit(item)
	}
}
func MutationVisit(ctx context.Context, list []interface{}) (n int, err error) {
	defer bulk.RecoverStop(&err)
	pass := bulk.NewCheckpoint(ctx)
	mutationEach(ctx, list, func(interface{}) {
		pass.Check()
		n++
	})
	return n, nil
}
`,
		want: "escape an exported entry can stop",
	},
	{
		// A mutual recursion over a list's members whose levels never
		// observe; observing one level of the cycle observes every level.
		name: "unobserved_mutual_recursion",
		mutation: `package cypher
import "github.com/orneryd/nornicdb/pkg/cypher/bulk"
func mutationDepthA(walk *bulk.Checkpoint, value interface{}) int {
	list, ok := value.([]interface{})
	if !ok || len(list) == 0 {
		return 0
	}
	return mutationDepthB(walk, list[0]) + 1
}
func mutationDepthB(walk *bulk.Checkpoint, value interface{}) int {
	return mutationDepthA(walk, value)
}
`,
		observed: `package cypher
import "github.com/orneryd/nornicdb/pkg/cypher/bulk"
func mutationDepthA(walk *bulk.Checkpoint, value interface{}) int {
	list, ok := value.([]interface{})
	if !ok || len(list) == 0 {
		return 0
	}
	return mutationDepthB(walk, list[0]) + 1
}
func mutationDepthB(walk *bulk.Checkpoint, value interface{}) int {
	walk.Check()
	return mutationDepthA(walk, value)
}
`,
		want: "recursion a recursive pass over data whose levels do not start with an observation",
	},
	{
		// fmt's %v of a value walks it; %T names its type only.
		name: "formatted_value",
		mutation: `package cypher
import "fmt"
func mutationFormatted(value interface{}) string {
	return fmt.Sprintf("%T:%v", value, value)
}
`,
		observed: `package cypher
import "fmt"
func mutationFormatted(value interface{}) string {
	return fmt.Sprintf("%T", value)
}
`,
		want: "call fmt.Sprintf of a value",
	},
	{
		// Observing records the failure in the slot whose lock the pass
		// holds: a self-deadlock (the deferred snapshot's shape).
		name: "observe_under_failure_slot_lock",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationSlotCopy(ctx context.Context, failure *expressionFailure) (int, error) {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	checkpoint := bulk.NewCheckpoint(ctx)
	n := 0
	for range failure.mapKeyOrders {
		if err := checkpoint.Observe(); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationSlotCopy(ctx context.Context, failure *expressionFailure) (int, error) {
	failure.mu.Lock()
	defer failure.mu.Unlock()
	checkpoint := bulk.NewCheckpoint(ctx)
	n := 0
	for range failure.mapKeyOrders {
		if err := checkpoint.Err(); err != nil {
			return 0, err
		}
		n++
	}
	return n, nil
}
`,
		want: "records the failure under the slot's own lock",
	},
	{
		// A stop raised while a lock is held unwinds past the unlock.
		name: "stop_under_lock",
		mutation: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"sync"
)
func mutationLocked(ctx context.Context, mu *sync.Mutex, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	mu.Lock()
	n := 0
	for range list {
		pass.Check()
		n++
	}
	mu.Unlock()
	return n
}
`,
		observed: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"sync"
)
func mutationLocked(ctx context.Context, mu *sync.Mutex, list []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := 0
	for range list {
		pass.Check()
		n++
	}
	mu.Lock()
	mu.Unlock()
	return n
}
`,
		want: "lock can stop while it holds a lock",
	},
	{
		// An unlock on an early return's path does not release the lock on
		// the path that goes on to the pass.
		name: "stop_under_lock_after_early_unlock",
		mutation: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"sync"
)
func mutationLockedCopy(ctx context.Context, mu *sync.RWMutex, shell map[string]interface{}) map[string]interface{} {
	mu.RLock()
	if len(shell) == 0 {
		mu.RUnlock()
		return nil
	}
	out := make(map[string]interface{}, len(shell))
	pass := bulk.NewCheckpoint(ctx)
	for key, value := range shell {
		pass.Check()
		out[key] = value
	}
	mu.RUnlock()
	return out
}
`,
		observed: `package cypher
import (
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
	"context"
	"sync"
)
func mutationLockedCopy(ctx context.Context, mu *sync.RWMutex, shell map[string]interface{}) map[string]interface{} {
	mu.RLock()
	if len(shell) == 0 {
		mu.RUnlock()
		return nil
	}
	mu.RUnlock()
	out := make(map[string]interface{}, len(shell))
	pass := bulk.NewCheckpoint(ctx)
	for key, value := range shell {
		pass.Check()
		out[key] = value
	}
	return out
}
`,
		want: "lock can stop while it holds a lock",
	},
	{
		// Provenance: a helper whose list comes only from the statement's
		// text passes over the statement; the same helper handed a caller's
		// data passes over data.
		name: "data_reaches_statement_helper",
		mutation: `package cypher
func mutationListWidth(list []interface{}) int {
	n := 0
	for range list {
		n++
	}
	return n
}
func mutationStatementWidth() int {
	return mutationListWidth([]interface{}{int64(1), int64(2)})
}
func MutationDataWidth(rows []interface{}) int {
	return mutationListWidth(rows)
}
`,
		observed: `package cypher
func mutationListWidth(list []interface{}) int {
	n := 0
	for range list {
		n++
	}
	return n
}
func mutationStatementWidth() int {
	return mutationListWidth([]interface{}{int64(1), int64(2)})
}
`,
		want: "loop range list",
	},
	{
		// Provenance: data a callee writes into the map it is handed reaches
		// the caller's map.
		name: "data_written_into_parameter",
		mutation: `package cypher
func mutationFill(out map[string]interface{}, value interface{}) {
	out["x"] = value
}
func mutationMapWidth(m map[string]interface{}) int {
	n := 0
	for range m {
		n++
	}
	return n
}
func MutationFilledWidth(value []interface{}) int {
	out := map[string]interface{}{}
	mutationFill(out, value)
	return mutationMapWidth(out)
}
`,
		observed: `package cypher
func mutationFill(out map[string]interface{}, value interface{}) {
	out["x"] = value
}
func mutationMapWidth(m map[string]interface{}) int {
	n := 0
	for range m {
		n++
	}
	return n
}
func MutationFilledWidth(value []interface{}) int {
	out := map[string]interface{}{}
	mutationFill(out, "statement")
	return mutationMapWidth(out)
}
`,
		want: "loop range m",
	},
	{
		// Provenance: a write through a local alias of a parameter's map is
		// a write into the parameter.
		name: "data_written_through_alias",
		mutation: `package cypher
type mutationHolder struct{ props map[string]interface{} }
func mutationFillHolder(holder *mutationHolder, value interface{}) {
	props := holder.props
	props["x"] = value
}
func mutationHolderWidth(holder *mutationHolder) int {
	n := 0
	for range holder.props {
		n++
	}
	return n
}
func MutationHolderWidth(value []interface{}) int {
	holder := &mutationHolder{props: map[string]interface{}{}}
	mutationFillHolder(holder, value)
	return mutationHolderWidth(holder)
}
`,
		observed: `package cypher
type mutationHolder struct{ props map[string]interface{} }
func mutationFillHolder(holder *mutationHolder, value interface{}) {
	props := holder.props
	props["x"] = value
}
func mutationHolderWidth(holder *mutationHolder) int {
	n := 0
	for range holder.props {
		n++
	}
	return n
}
func MutationHolderWidth(value []interface{}) int {
	holder := &mutationHolder{props: map[string]interface{}{}}
	mutationFillHolder(holder, int64(1))
	return mutationHolderWidth(holder)
}
`,
		want: "loop range holder.props",
	},
	{
		// A string a package function computes from data is data: the
		// statement-free scan of it is a pass over data.
		name: "string_result_of_data",
		mutation: `package cypher
import "strings"
func mutationQueryText(args []interface{}) (string, error) {
	text, _ := args[0].(string)
	return text, nil
}
func MutationQueryWords(args []interface{}) int {
	query, _ := mutationQueryText(args)
	return len(strings.Fields(query))
}
`,
		observed: `package cypher
import "strings"
func mutationQueryText(args []interface{}) (string, error) {
	text, _ := args[0].(string)
	return text, nil
}
func MutationQueryWords(args []interface{}) int {
	query, _ := mutationQueryText([]interface{}{"statement text"})
	return len(strings.Fields(query))
}
`,
		want: "call strings.Fields(query)",
	},
	{
		// An owner started on context.Background() observes no statement
		// although the statement's context is in scope.
		name: "background_context_to_owner",
		mutation: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationBackgroundOwner(ctx context.Context, rows []interface{}) int {
	pass := bulk.NewCheckpoint(context.Background())
	n := 0
	for range rows {
		pass.Check()
		n++
	}
	return n
}
`,
		observed: `package cypher
import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher/bulk"
)
func mutationBackgroundOwner(ctx context.Context, rows []interface{}) int {
	pass := bulk.NewCheckpoint(ctx)
	n := 0
	for range rows {
		pass.Check()
		n++
	}
	return n
}
`,
		want: "context context.Background() handed to NewCheckpoint",
	},
}

func TestCancellationBoundaryMutations(t *testing.T) {
	for _, mutation := range cancellationBoundaryMutations {
		t.Run(mutation.name, func(t *testing.T) {
			pkg := loadCypherPackageForGuard(t, map[string]string{"cancellation_boundary_mutation.go": mutation.mutation})
			findings := cancellationBoundaryFindings(pkg, pkg.fixtures)
			texts := make([]string, len(findings))
			for i, finding := range findings {
				texts[i] = finding.String()
			}
			require.NotEmpty(t, texts, "the guard must reject the mutation")
			require.True(t, strings.Contains(strings.Join(texts, "\n"), mutation.want), "want %q in %v", mutation.want, texts)

			observed := loadCypherPackageForGuard(t, map[string]string{"cancellation_boundary_mutation.go": mutation.observed})
			require.Empty(t, cancellationBoundaryFindings(observed, observed.fixtures), "the same work through the owner passes")
		})
	}
}

// The baseline only shrinks: a pass it does not list fails, and so does an
// entry whose pass was converted. A pass's identity has no line number, so
// moving it leaves the baseline as it is.
func TestCancellationBoundaryBaselineRatchet(t *testing.T) {
	var mutation struct{ name, mutation, observed, want string }
	for _, candidate := range cancellationBoundaryMutations {
		if candidate.name == "linear_list_loop" {
			mutation = candidate
		}
	}
	require.NotEmpty(t, mutation.mutation)
	findingsOf := func(source string) []boundaryFinding {
		pkg := loadCypherPackageForGuard(t, map[string]string{"cancellation_boundary_mutation.go": source})
		return cancellationBoundaryFindings(pkg, pkg.fixtures)
	}

	findings := findingsOf(mutation.mutation)
	require.NotEmpty(t, findings)
	fresh, stale := baselineViolations(findings, map[string]int{})
	require.Len(t, fresh, len(findings), "a pass the baseline does not list fails")
	require.Empty(t, stale)

	listed := map[string]int{}
	for _, finding := range findings {
		listed[finding.baselineID()]++
	}
	fresh, stale = baselineViolations(findings, listed)
	require.Empty(t, fresh)
	require.Empty(t, stale)

	fresh, stale = baselineViolations(findingsOf(strings.Replace(mutation.mutation, "package cypher\n", "package cypher\n\n\n", 1)), listed)
	require.Empty(t, fresh, "a moved pass keeps its baseline entry")
	require.Empty(t, stale)

	fresh, stale = baselineViolations(findingsOf(mutation.observed), listed)
	require.Empty(t, fresh)
	require.Len(t, stale, len(listed), "an entry whose pass was converted fails until it is removed")
}
