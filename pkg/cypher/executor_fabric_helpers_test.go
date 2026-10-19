package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/stretchr/testify/require"
)

func TestFabricHelpers_AccumulatorsAndMaps(t *testing.T) {
	acc := &queryStatsAccumulator{}
	acc.add(&QueryStats{NodesCreated: 1, RelationshipsCreated: 2})
	acc.add(&QueryStats{NodesDeleted: 3, PropertiesSet: 4})
	s := acc.snapshot()
	require.EqualValues(t, 1, s.NodesCreated)
	require.EqualValues(t, 3, s.NodesDeleted)
	require.EqualValues(t, 2, s.RelationshipsCreated)
	require.EqualValues(t, 4, s.PropertiesSet)

	require.Nil(t, fabricStatsAccumulatorFromContext(context.Background()))
	ctx := context.WithValue(context.Background(), fabricStatsAccumulatorKey{}, acc)
	require.NotNil(t, fabricStatsAccumulatorFromContext(ctx))

	m, ok := normalizeFabricRowMap(map[interface{}]interface{}{"k": "v"})
	require.True(t, ok)
	require.Equal(t, "v", m["k"])
	_, ok = normalizeFabricRowMap(123)
	require.False(t, ok)

	v, ok := lookupColumnBySourceIDInRows([]interface{}{map[string]interface{}{"sourceId": "s1", "name": "A"}}, "\"s1\"", "name")
	require.True(t, ok)
	require.Equal(t, "A", v)
	_, ok = lookupColumnBySourceIDInRows([]map[string]interface{}{{"sourceId": "s2", "name": "B"}}, "s1", "name")
	require.False(t, ok)

	require.True(t, sameSourceID("\"abc\"", "abc"))
	require.False(t, sameSourceID("a", "b"))
}

func TestFabricHelpers_InferColumnsAndRecordRewrite(t *testing.T) {
	exec := NewStorageExecutor(newTestMemoryEngine(t))

	require.Equal(t, []string{"n", "c"}, exec.inferTopLevelReturnColumns("MATCH (n) RETURN n, count(*) AS c ORDER BY c DESC LIMIT 2"))
	require.Nil(t, exec.inferTopLevelReturnColumns("MATCH (n)"))

	r := stripLeadingWithImportsForFabricRecord("WITH x, y MATCH (n) WHERE n.id = x RETURN n", map[string]interface{}{"x": int64(1), "y": int64(2)})
	require.Equal(t, "MATCH (n) WHERE n.id = $x RETURN n", r)

	// Non-simple WITH projection should not rewrite.
	r = stripLeadingWithImportsForFabricRecord("WITH x AS y MATCH (n) RETURN n", map[string]interface{}{"x": int64(1)})
	require.Equal(t, "WITH x AS y MATCH (n) RETURN n", r)

	require.Greater(t, findLeadingWithEndLocal("WITH x MATCH (n) RETURN n"), 0)
}

func TestFabricHelpers_ConstituentAndFragmentRouting(t *testing.T) {
	ref, ok := toConstituentRef(map[string]interface{}{
		"alias":         "east",
		"database_name": "db_east",
		"type":          "remote",
		"uri":           "bolt://x",
		"auth_mode":     "basic",
		"user":          "u",
		"password":      "p",
	})
	require.True(t, ok)
	require.Equal(t, "east", ref.Alias)
	require.Equal(t, "db_east", ref.DatabaseName)
	require.Equal(t, "remote", ref.Type)
	require.Equal(t, "v", mapString(map[string]interface{}{"k": "v"}, "k"))
	_, ok = toConstituentRef(42)
	require.False(t, ok)

	cat := fabric.NewCatalog()
	cat.Register("local", &fabric.LocationLocal{DBName: "local"})
	cat.Register("remote", &fabric.LocationRemote{DBName: "remote", URI: "bolt://remote"})

	require.False(t, fabricFragmentHasRemoteTarget(cat, nil))
	require.False(t, fabricFragmentHasRemoteTarget(cat, &fabric.FragmentInit{}))
	require.False(t, fabricFragmentHasRemoteTarget(cat, &fabric.FragmentExec{GraphName: "local"}))
	require.True(t, fabricFragmentHasRemoteTarget(cat, &fabric.FragmentExec{GraphName: "remote"}))
	require.True(t, fabricFragmentHasRemoteTarget(cat, &fabric.FragmentUnion{
		LHS: &fabric.FragmentExec{GraphName: "local"},
		RHS: &fabric.FragmentExec{GraphName: "remote"},
	}))
}
