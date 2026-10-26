package cypher

import (
	"context"
	"fmt"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"time"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
)

// registryFunctionContext supplies execution state consistently to every
// registry dispatcher. Each caller supplies evaluation in its own bindings and
// may add Nodes/Rels fast paths without rebuilding the statement identity.
func (e *StorageExecutor) registryFunctionContext(ctx context.Context, args []braceView, evaluate func(braceView) (interface{}, error)) cypherfn.InvocationContext {
	base := cypherfn.Context{Storage: e.storage, Database: e.executionDatabaseName(ctx), Now: time.Now, Graphs: e, Cypher25: cypherVersionFromContext(ctx) == "25"}
	if evaluate == nil {
		return cypherfn.InvocationContext{Context: base}
	}
	// External Eval is a genuine independent expression input, never a locator
	// for an argument occurrence in this statement. Decision: Nornic2, 26 Oct.
	base.Eval = func(text string) (interface{}, error) {
		view := expressionInputView(text, cyphertext.RawSource)
		if err := view.admission(); err != nil {
			return nil, err
		}
		return evaluate(view)
	}
	return cypherfn.InvocationContext{Context: base, Eval: func(index int) (interface{}, error) {
		if index < 0 || index >= len(args) {
			return nil, fmt.Errorf("cypher: function argument index %d outside retained invocation", index)
		}
		return evaluate(args[index])
	}, EvalSpan: func(index int, span cyphertext.Span) (interface{}, error) {
		if index < 0 || index >= len(args) {
			return nil, fmt.Errorf("cypher: function argument index %d outside retained invocation", index)
		}
		if span.Start < 0 || span.End < span.Start || span.End > len(args[index].text()) {
			return nil, fmt.Errorf("cypher: function grammar extent outside retained argument")
		}
		return evaluate(args[index].spanWindow(span))
	}}
}

func functionArgumentTexts(views []braceView) []string {
	texts := make([]string, len(views))
	for index, view := range views {
		texts[index] = view.text()
	}
	return texts
}
