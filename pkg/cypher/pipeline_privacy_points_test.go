package cypher

import (
	"context"
	"fmt"
	"strings"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineRegistryCancellationAtEveryReachablePoll(t *testing.T) {
	items := make([]interface{}, 513)
	typed := make([]int, len(items))
	fields := make(map[string]interface{}, len(items))
	for i := range items {
		items[i] = int64(i)
		typed[i] = i
		fields[fmt.Sprint(i)] = i
	}
	for _, entry := range []struct {
		function string
		args     []string
	}{
		{"coll.distinct", []string{"items"}},
		{"coll.flatten", []string{"nested", "depth"}},
		{"coll.indexOf", []string{"items", "missing"}},
		{"coll.insert", []string{"items", "offset", "missing"}},
		{"coll.remove", []string{"items", "offset"}},
		{"coll.min", []string{"items"}},
		{"coll.max", []string{"items"}},
		{"coll.sort", []string{"items"}},
		{"keys", []string{"fields"}},
		{"keys", []string{"node"}},
		{"keys", []string{"edge"}},
		{"toUpper", []string{"text"}},
		{"toLower", []string{"text"}},
		{"upper", []string{"text"}},
		{"lower", []string{"text"}},
		{"normalize", []string{"text"}},
		{"char_length", []string{"text"}},
		{"size", []string{"text"}},
		{"labels", []string{"node"}},
		{"tail", []string{"items"}},
		{"toIntegerList", []string{"items"}},
		{"toFloatList", []string{"items"}},
		{"toBooleanList", []string{"items"}},
		{"toStringList", []string{"items"}},
		{"nullIf", []string{"items", "items"}},
	} {
		for _, list := range []interface{}{items, typed} {
			t.Run(entry.function+"/"+entry.args[0]+fmt.Sprintf("/%T", list), func(t *testing.T) {
				run := func(probe *pipelineCancellation) interface{} {
					values := map[string]interface{}{
						"items": list, "nested": []interface{}{list}, "depth": int64(1),
						"offset": int64(256), "missing": int64(-1), "fields": fields,
						"node": &storage.Node{Properties: fields, Labels: make([]string, 513)}, "edge": &storage.Edge{Properties: fields},
						"text": strings.Repeat("ΟΣßİé ", 1<<15),
					}
					_, found, err := cypherfn.EvaluateFunction(entry.function, entry.args, cypherfn.Context{
						Cancellation: probe,
						Nodes:        map[string]*storage.Node{"node": values["node"].(*storage.Node)},
						Rels:         map[string]*storage.Edge{"edge": values["edge"].(*storage.Edge)},
						Eval:         func(name string) (interface{}, error) { return values[name], nil },
					})
					require.True(t, found)
					if probe.err != nil {
						require.ErrorIs(t, err, probe.err)
					}
					return err
				}
				if entry.function == "keys" {
					// Random map order changes sort work and which append lands
					// on a sampled source line. Sweep the shared entry budget
					// instead; direct fn tests replay every output-copy work unit.
					runPrivacyReaderSamples(t, run)
				} else {
					runPipelineCancelPointStage(t, func(ctx context.Context) error {
						probe := pipelineCancellation{ctx: ctx}
						err, _ := run(&probe).(error)
						return err
					})
				}
			})
		}
	}
}
