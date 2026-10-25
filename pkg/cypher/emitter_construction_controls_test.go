package cypher

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"text/template"

	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// These constructions deliberately exceed the smoke check's advertised scope.
// Their syntax differs; the behavioral oracle and expected failure do not.
var unquotedInitializer = fmt.Sprintf("[r IS %s]", "A|B")

func TestRelationshipEmitterConstructionControls(t *testing.T) {
	for _, name := range []string{"A|B", "tick`name"} {
		for i, form := range cyphernames.DeclarationForms {
			t.Run(fmt.Sprintf("%s/form-%d", name, i), func(t *testing.T) {
				for kind, emit := range map[string]func() string{
					"sprintf": func() string { return fmt.Sprintf("[r%s%s]", form, name) },
					"width":   func() string { return fmt.Sprintf("[r%s%*s]", form, 0, name) },
					"template": func() string {
						var b bytes.Buffer
						require.NoError(t, template.Must(template.New("emitter").Parse("[r{{.Form}}{{.Name}}]")).Execute(&b, map[string]string{"Form": form, "Name": name}))
						return b.String()
					},
				} {
					t.Run(kind, func(t *testing.T) {
						executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "negative_emitter"))
						require.Error(t, cyphernames.RoundTrip(emit(), name, nil, nil, emitterObservations(executor)), "unquoted construction escaped behavioral property")
					})
				}
			})
		}
	}
	t.Run("review-IS-count", func(t *testing.T) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "negative_emitter"))
		_, err := executor.Execute(context.Background(), "CREATE ()-[:`A|B`]->()", nil)
		require.NoError(t, err)
		raw, err := executor.Execute(context.Background(), "MATCH ()-[r IS A|B]->() RETURN count(r)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(0)}}, raw.Rows, "unquoted IS must not be mistaken for the literal-name result")
		quoted, err := executor.Execute(context.Background(), "MATCH ()-[r IS `A|B`]->() RETURN count(r)", nil)
		require.NoError(t, err)
		require.Equal(t, [][]any{{int64(1)}}, quoted.Rows)
	})
	t.Run("global-initializer", func(t *testing.T) {
		executor := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "negative_emitter"))
		require.Error(t, cyphernames.RoundTrip(unquotedInitializer, "A|B", nil, nil, emitterObservations(executor)))
	})
}
