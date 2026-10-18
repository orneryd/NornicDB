package cypher

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestPipelineWholeValueExecuteDeadline(t *testing.T) {
	executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "privacy_execute"))
	items := make([]interface{}, 16<<20)
	text := strings.Repeat("ΟΣßİé", 1<<21)
	previous := PluginFunctionLookup
	PluginFunctionLookup = func(name string) (interface{}, bool) {
		switch name {
		case "privacy.largeitems":
			return func() interface{} { return items }, true
		case "privacy.largetext":
			return func() interface{} { return text }, true
		}
		if previous != nil {
			return previous(name)
		}
		return nil, false
	}
	t.Cleanup(func() { PluginFunctionLookup = previous })
	for _, query := range []string{
		"WITH privacy.largeitems() AS xs RETURN apoc.text.join([xs], '')",
		"WITH privacy.largetext() AS text RETURN toUpper(text)",
	} {
		t.Run(query, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			deadline, _ := ctx.Deadline()
			_, err := executor.Execute(ctx, query, nil)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Less(t, time.Since(deadline), 250*time.Millisecond)
		})
	}
}
