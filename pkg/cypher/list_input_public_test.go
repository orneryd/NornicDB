package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/orneryd/nornicdb/testing/cypher/listinputs"
	"github.com/stretchr/testify/require"
)

func TestListInputPublicRepresentations(t *testing.T) {
	store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "list_inputs")
	exec := NewStorageExecutorWithQueryCachePolicy(store, 0, 0)
	ctx := context.Background()
	t.Cleanup(func() { rowValueProgramsDisabled.Store(false) })
	// Classify the fixtures before execution: a failing literal must never
	// turn a successful fromPairs case into an expected-error case. Develop's
	// inherited constructor skips non-list and incomplete pairs, so each of
	// these fixtures succeeds with an empty map (release keeps singleton keys).
	fromPairsCases := map[string]struct{ wantJSON, code string }{
		"any":           {wantJSON: `{}`},
		"string":        {wantJSON: `{}`},
		"int":           {wantJSON: `{}`},
		"int8":          {wantJSON: `{}`},
		"int16":         {wantJSON: `{}`},
		"int32":         {wantJSON: `{}`},
		"int64":         {wantJSON: `{}`},
		"uint":          {wantJSON: `{}`},
		"uint8":         {wantJSON: `{}`},
		"uint16":        {wantJSON: `{}`},
		"uint32":        {wantJSON: `{}`},
		"uint64":        {wantJSON: `{}`},
		"float32":       {wantJSON: `{}`},
		"float64":       {wantJSON: `{}`},
		"bool":          {wantJSON: `{}`},
		"array":         {wantJSON: `{}`},
		"string-array":  {wantJSON: `{}`},
		"named-strings": {wantJSON: `{}`},
		"named-ints":    {wantJSON: `{}`},
		"maps":          {wantJSON: `{}`},
		"nested":        {wantJSON: `{}`},
		"nested-string": {wantJSON: `{}`},
		"nested-array":  {wantJSON: `{}`},
		"mixed":         {wantJSON: `{}`},
	}
	inputs := listinputs.Inputs()
	require.NotEmpty(t, inputs)
	require.Len(t, fromPairsCases, len(inputs), "classify every fromPairs fixture")
	read := func(t *testing.T, query string, params map[string]interface{}) interface{} {
		t.Helper()
		result, err := exec.Execute(ctx, query, params)
		require.NoError(t, err, query)
		require.Len(t, result.Rows, 1, query)
		require.Len(t, result.Rows[0], 1, query)
		// JSON removes transport-specific numeric Go kinds without using the
		// implementation's comparison or list-normalization helpers.
		encoded, err := json.Marshal(result.Rows[0][0])
		require.NoError(t, err)
		var value interface{}
		require.NoError(t, json.Unmarshal(encoded, &value))
		return value
	}
	for _, disabled := range []bool{false, true} {
		rowValueProgramsDisabled.Store(disabled)
		for _, input := range inputs {
			t.Run(fmt.Sprintf("programsDisabled=%t/%s", disabled, input.Name), func(t *testing.T) {
				if !disabled {
					_, err := store.CreateNode(&storage.Node{ID: storage.NodeID(input.Name), Labels: []string{"ListInput"}, Properties: map[string]interface{}{"xs": input.PropertyValue(), "name": input.Name}})
					require.NoError(t, err)
				}
				params := map[string]interface{}{"xs": input.Value, "name": input.Name}
				for _, template := range listinputs.Queries() {
					t.Run(template, func(t *testing.T) {
						forms := []struct{ name, prefix, value string }{
							{"parameter", "", "$xs"},
							{"with", "WITH $xs AS xs ", "xs"},
							{"stored", "MATCH (n:ListInput {name:$name}) ", "n.xs"},
							{"nested-map", "WITH {items:$xs} AS box ", "box.items"},
						}
						if template == "apoc.map.fromPairs(%s)" {
							fixture, ok := fromPairsCases[input.Name]
							require.True(t, ok, "unclassified fromPairs fixture: %s", input.Name)
							var want interface{}
							if fixture.code == "" {
								require.NoError(t, json.Unmarshal([]byte(fixture.wantJSON), &want))
							}
							for _, form := range append([]struct{ name, prefix, value string }{{"literal", "", input.Literal}}, forms...) {
								t.Run(form.name, func(t *testing.T) {
									if fixture.code != "" {
										_, err := exec.Execute(ctx, form.prefix+listinputs.Query(template, form.value), params)
										requireStatusCode(t, err, fixture.code)
										return
									}
									require.Equal(t, want, read(t, form.prefix+listinputs.Query(template, form.value), params))
								})
							}
							return
						}
						want := read(t, listinputs.Query(template, input.Literal), nil)
						for _, form := range forms {
							t.Run(form.name, func(t *testing.T) {
								// The existing grammar contract distinguishes a binary
								// parameter from a list in IN positions. APOC's list
								// adapters accept its numeric sequence independently.
								if input.Name == "uint8" && form.name == "parameter" && (strings.HasPrefix(template, "reduce(") || strings.HasPrefix(template, "[x IN")) {
									_, err := exec.Execute(ctx, listinputs.Query(template, form.value), params)
									requireStatusCode(t, err, "Neo.ClientError.Statement.SyntaxError")
									return
								}
								require.Equal(t, want, read(t, form.prefix+listinputs.Query(template, form.value), params))
							})
						}
					})
				}
			})
		}
	}
	rowValueProgramsDisabled.Store(false)
}

func TestListInputReportedLoss(t *testing.T) {
	exec := NewStorageExecutorWithQueryCachePolicy(storage.NewNamespacedEngine(newTestMemoryEngine(t), "repro"), 0, 0)
	params := map[string]interface{}{"xs": []string{"a", "b", "a"}}
	for _, tc := range []struct {
		query string
		want  interface{}
	}{
		{"RETURN apoc.coll.union($xs,['c']) AS v", []interface{}{"a", "b", "c"}},
		{"RETURN apoc.coll.unionAll($xs,['c']) AS v", []interface{}{"a", "b", "a", "c"}},
		{"RETURN apoc.coll.frequencies($xs) AS v", []interface{}{map[string]interface{}{"item": "a", "count": int64(2)}, map[string]interface{}{"item": "b", "count": int64(1)}}},
	} {
		t.Run(tc.query, func(t *testing.T) {
			result, err := exec.Execute(context.Background(), tc.query, params)
			require.NoError(t, err)
			require.Equal(t, tc.want, result.Rows[0][0])
		})
	}
}
