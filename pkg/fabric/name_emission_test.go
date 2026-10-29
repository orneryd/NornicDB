package fabric

import (
	"context"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Columns are decoded identities, including whitespace. Observe both sides of
// the actual batch boundary: the parameter map key and the generated lookup.
func TestNameEmissionBatchDecodedColumns(t *testing.T) {
	catalog := NewCatalog()
	catalog.Register("db1", &LocationLocal{DBName: "db1"})
	capture := &pipelineCaptureCypherExecutor{}
	local := NewLocalFragmentExecutor(capture, func(string) (storage.Engine, error) { return &mockEngine{}, nil })
	e := NewFabricExecutor(catalog, local, nil)
	for _, name := range []string{"space ", " space", " ", "tick`key", "a.b"} {
		t.Run(name, func(t *testing.T) {
			inner := &FragmentExec{Input: &FragmentInit{}, Query: "RETURN " + cyphertext.QuoteName(name) + " AS value", GraphName: "db1"}
			_, used, err := e.executeApplyAsPipeline(context.Background(), nil, &ResultStream{Columns: []string{name}, Rows: [][]any{{int64(7)}}}, inner, nil, "")
			require.NoError(t, err)
			require.True(t, used)
			require.Contains(t, capture.lastQuery, "__fabric_row."+cyphertext.NameText(name)+" AS "+cyphertext.NameText(name))
			rows := capture.lastParams["__fabric_apply_rows"].([]map[string]any)
			require.Equal(t, []map[string]any{{name: int64(7)}}, rows)
			require.Equal(t, []string{name, "space"}, mergeImportColumns([]string{name}, []string{name, "space"}))
			require.Equal(t, map[string]int{name: 0}, buildColumnIndex([]string{name}))
			require.Equal(t, map[string]any{name: int64(7)}, bindingsFromParentAndRow(nil, []string{name}, []any{int64(7)}))
		})
	}
}

func TestNameEmissionImportSubjects(t *testing.T) {
	for _, name := range cyphernames.Names() {
		t.Run(name, func(t *testing.T) {
			e := &FabricExecutor{}
			fragment := &FragmentExec{Query: "RETURN 1", ScopeImports: []string{name}}
			text, params, scoped, err := e.importStatement(fragment, nil, map[string]any{name: int64(7)}, nil)
			require.NoError(t, err)
			require.Equal(t, []string{name}, scoped)
			require.Len(t, params, 1)
			for _, marker := range []string{" AS ", "CALL ("} {
				at := strings.Index(text, marker)
				require.GreaterOrEqual(t, at, 0)
				written := text[at+len(marker):]
				token, ok := cyphertext.ScanIdentifier(written, 0)
				got := token.Value
				require.True(t, ok, text)
				require.Equal(t, name, got)
			}
			require.Equal(t, cyphertext.IsIdentifier(name), cyphertext.IsIdentifier(name))
		})
	}
}
