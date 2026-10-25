package fabric_test

import (
	"context"
	"errors"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"os"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/cypher/resultmetadata"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

type headerBoundaryLocal struct {
	exec  *cypher.StorageExecutor
	cache map[string]resultmetadata.Names
	omit  bool

	// Input names observed by the cache-only provider.
	incoming []string
}

func (l *headerBoundaryLocal) CachedResultColumns(_ context.Context, query string, incoming []string) resultmetadata.Names {
	l.incoming = append([]string(nil), incoming...)
	if names, ok := l.cache[query]; ok {
		return names
	}
	return resultmetadata.Unknown()
}

func (l *headerBoundaryLocal) ExecuteQuery(ctx context.Context, _ string, _ storage.Engine, query string, params map[string]interface{}) ([]string, [][]interface{}, error) {
	if l.omit {
		return nil, nil, nil
	}
	result, err := l.exec.Execute(ctx, query, params)
	if err != nil {
		return nil, nil, err
	}
	return result.Columns, result.Rows, nil
}

func (l *headerBoundaryLocal) ExecuteQueryWithRecord(ctx context.Context, db string, engine storage.Engine, query string, params map[string]interface{}, recordBindings map[string]interface{}) ([]string, [][]interface{}, error) {
	if len(recordBindings) != 0 {
		return nil, nil, errors.New("header-only fixture does not implement correlated record bindings")
	}
	return l.ExecuteQuery(ctx, db, engine, query, params)
}

func TestResultHeaderFabricBoundary(t *testing.T) {
	ctx := context.Background()
	base := fixtureowner.Own(t, fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed))
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(base, "neo4j"))
	adapter := &headerBoundaryLocal{exec: exec, cache: map[string]resultmetadata.Names{}}
	catalog := fabric.NewCatalog()
	catalog.Register("comp.local", &fabric.LocationLocal{DBName: "neo4j"})
	local := fabric.NewLocalFragmentExecutor(adapter, func(string) (storage.Engine, error) { return base, nil })
	remote := fabric.NewRemoteFragmentExecutor()
	t.Cleanup(func() { require.NoError(t, remote.Close()) })
	var reference neo4j.DriverWithContext
	graphs := []string{"comp.local"}
	if uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_URI"); uri != "" {
		var err error
		reference, err = neo4j.NewDriverWithContext(uri, neo4j.NoAuth())
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, reference.Close(ctx)) })
		catalog.Register("comp.remote", &fabric.LocationRemote{URI: uri, DBName: "neo4j", AuthMode: "user_password"})
		graphs = append(graphs, "comp.remote")
	}
	fexec := fabric.NewFabricExecutor(catalog, local, remote)
	cases := []struct {
		query   string
		columns []string
	}{
		{"CALL { RETURN 1 AS before } CREATE (:HeaderR73)", []string{}},
		{"CALL { UNWIND [] AS before RETURN * } CREATE (:HeaderR73)", []string{}},
		{"UNWIND [] AS value RETURN *", []string{"value"}},
		{"CALL { UNWIND [] AS before RETURN * } RETURN *", []string{"before"}},
		{"WITH 1 AS value CALL db.labels() YIELD label RETURN * UNION ALL WITH 2 AS value CALL db.labels() YIELD label RETURN *", []string{"label", "value"}},
	}
	for _, seed := range []string{"WITH 1 AS value", "UNWIND [] AS value", "CALL { USE neo4j UNWIND [] AS value RETURN * }"} {
		for _, yielded := range []string{"label", "label AS kind"} {
			for projection, tail := range []string{"RETURN *", "WITH value RETURN *", "WITH count(*) AS total RETURN *"} {
				columns := []string{"label", "value"}
				if yielded != "label" {
					columns[0] = "kind"
				}
				if projection == 1 {
					columns = []string{"value"}
				} else if projection == 2 {
					columns = []string{"total"}
				}
				query := seed + " CALL db.labels() YIELD " + yielded + " " + tail
				for _, operator := range []string{"", " UNION ", " UNION ALL "} {
					full := query
					if operator != "" {
						full += operator + query
					}
					cases = append(cases, struct {
						query   string
						columns []string
					}{full, columns})
				}
			}
		}
	}
	for _, procedure := range []struct{ name, output string }{{"db.labels", "label"}, {"db.relationshipTypes", "relationshipType"}, {"db.propertyKeys", "propertyKey"}} {
		for _, suffix := range []string{"", " YIELD *", " YIELD " + procedure.output + " AS renamed"} {
			column := procedure.output
			if suffix != "" && suffix != " YIELD *" {
				column = "renamed"
			}
			cases = append(cases, struct {
				query   string
				columns []string
			}{"CALL " + procedure.name + "()" + suffix, []string{column}})
		}
	}
	for _, tc := range cases {
		t.Run(tc.query, func(t *testing.T) {
			if reference != nil {
				for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
					expected, err := neo4j.ExecuteQuery(ctx, reference, prefix+tc.query, nil, neo4j.EagerResultTransformer)
					require.NoError(t, err)
					require.Equal(t, tc.columns, append([]string{}, expected.Keys...))
				}
			}
			for _, graph := range graphs {
				t.Run(graph, func(t *testing.T) {
					for _, prefix := range []string{"", "PROFILE ", "EXPLAIN "} {
						result, err := fexec.Execute(ctx, nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: graph, Query: prefix + tc.query}, nil, "")
						require.NoError(t, err)
						require.Equal(t, tc.columns, append([]string{}, result.Columns...))
					}
				})
			}
			owner := exec.StatementColumns(tc.query)
			require.Equal(t, tc.columns, append([]string{}, owner...))
			adapter.cache[tc.query] = resultmetadata.Known(owner)
			adapter.omit = true
			result, err := fexec.Execute(ctx, nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: "comp.local", Query: tc.query}, nil, "")
			adapter.omit = false
			require.NoError(t, err)
			require.Equal(t, tc.columns, append([]string{}, result.Columns...), "missing constituent fields must use the supplied owner cache")
		})
	}
	// APPLY must propagate execution projection fields without an input record
	// or a warmed metadata cache, through both constituent dispatch routes.
	for _, graph := range graphs {
		t.Run("cold-empty-apply/"+graph, func(t *testing.T) {
			input := &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: graph, Query: "UNWIND [] AS value RETURN value"}
			inner := &fabric.FragmentExec{Input: &fabric.FragmentInit{Columns: []string{"value"}, ImportColumns: []string{"value"}}, GraphName: graph, Query: "WITH value RETURN value AS kept"}
			apply := &fabric.FragmentApply{Input: input, Inner: inner}
			projection := &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: graph, Query: "RETURN *, 2 AS extra"}
			result, err := fexec.Execute(ctx, nil, &fabric.FragmentApply{Input: apply, Inner: projection, Projection: true}, nil, "")
			require.NoError(t, err)
			require.Equal(t, []string{"kept", "value", "extra"}, result.Columns)
			require.Empty(t, result.Rows)
			if reference != nil {
				query := "UNWIND [] AS value CALL { WITH value RETURN value AS kept } RETURN *, 2 AS extra"
				expected, err := neo4j.ExecuteQuery(ctx, reference, query, nil, neo4j.EagerResultTransformer)
				require.NoError(t, err)
				require.Equal(t, expected.Keys, result.Columns)
			}
		})
	}
	for _, graph := range graphs {
		t.Run("cold-empty-projection-with-no-fields/"+graph, func(t *testing.T) {
			input := &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: graph, Query: "UNWIND [] AS seed CREATE (:HeaderNoWrite)"}
			projection := &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: graph, Query: "RETURN 1 AS projected"}
			result, err := fexec.Execute(ctx, nil, &fabric.FragmentApply{Input: input, Inner: projection, Projection: true}, nil, "")
			require.NoError(t, err)
			require.Equal(t, []string{"projected"}, result.Columns)
			require.Empty(t, result.Rows)
			if reference != nil {
				expected, err := neo4j.ExecuteQuery(ctx, reference, "UNWIND [] AS seed RETURN 1 AS projected", nil, neo4j.EagerResultTransformer)
				require.NoError(t, err)
				require.Equal(t, expected.Keys, result.Columns)
			}
		})
	}
	adapter.omit = true
	cachedQuery := "RETURN 1 AS cachedFieldsWithBindings"
	adapter.cache[cachedQuery] = resultmetadata.Known([]string{"cachedFieldsWithBindings"})
	boundContext := fabric.WithRecordBindings(ctx, map[string]interface{}{"importedValue": int64(1)})
	cached, err := fexec.Execute(boundContext, nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: "comp.local", Query: cachedQuery}, nil, "")
	require.NoError(t, err)
	require.Equal(t, []string{"cachedFieldsWithBindings"}, cached.Columns)
	require.Equal(t, []string{"importedValue"}, adapter.incoming, "cache provider must receive the incoming binding names")
	result, err := fexec.Execute(ctx, nil, &fabric.FragmentExec{Input: &fabric.FragmentInit{}, GraphName: "comp.local", Query: "RETURN unknownNeverProjected AS future"}, nil, "")
	require.NoError(t, err)
	require.Nil(t, result.Columns, "a cache miss must stay unknown, never query-parsed")
}

func (l *headerBoundaryLocal) ResultProjection(ctx context.Context, query string, incoming []string, stage cypher.SourceStage) resultmetadata.Projection {
	return l.exec.ResultProjection(ctx, query, incoming, stage)
}
