package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"testing"
	"time"

	valueconvert "github.com/orneryd/nornicdb/pkg/convert"
	"github.com/stretchr/testify/require"
)

// HTTP API numbers keep their type both ways: a JSON integer is an exact
// int64, a number with a fraction or an exponent a float64, and a float
// parameter is written as a JSON float even when it is integral.
func TestRemoteHTTPNumbers(t *testing.T) {
	t.Run("read", func(t *testing.T) {
		for _, tt := range []struct {
			text string
			want interface{}
		}{
			{"-42", int64(-42)},
			{"9007199254740993", int64(9007199254740993)},
			{"-9007199254740993", int64(-9007199254740993)},
			{"9223372036854775807", int64(math.MaxInt64)},
			{"-9223372036854775808", int64(math.MinInt64)},
			{"-42.0", float64(-42)},
			{"2.5", 2.5},
			{"1.0E20", 1e20},
			{"1e3", float64(1000)},
			{"9223372036854775808", uint64(math.MaxInt64) + 1},
			{"18446744073709551615", uint64(math.MaxUint64)},
		} {
			var got interface{}
			require.NoError(t, valueconvert.PropertyJSON.Unmarshal([]byte(tt.text), &got))
			require.Equal(t, tt.want, got, tt.text)
		}
		var nested interface{}
		require.NoError(t, valueconvert.PropertyJSON.Unmarshal([]byte(`{"a":[{"n":-42,"f":2.0}],"t":"x"}`), &nested))
		require.Equal(t, map[string]interface{}{"a": []interface{}{map[string]interface{}{"n": int64(-42), "f": float64(2)}}, "t": "x"}, nested)
		for _, raw := range []string{`1 2`, `{`, `18446744073709551616`} {
			require.Error(t, valueconvert.PropertyJSON.Unmarshal([]byte(raw), &nested))
		}
		for value, want := range map[interface{}]float64{float64(1.5): 1.5, int64(3): 3} {
			got, ok := remoteHTTPFloat64(value)
			require.True(t, ok)
			require.Equal(t, want, got)
		}
		_, ok := remoteHTTPFloat64("1")
		require.False(t, ok)
	})
	t.Run("write", func(t *testing.T) {
		statement := remoteStatement{Statement: "RETURN $f", Parameters: map[string]interface{}{
			"f":   -42.0,
			"i":   int64(-42),
			"big": int64(9007199254740993),
			"l":   []interface{}{2.0, int64(3), nil, "s", true},
			"m":   map[string]interface{}{"x": float32(1), "y": float32(0.1)},
			"s":   []float64{1, 2.5},
			"n":   map[string][]int{"k": {1}},
			"b":   []byte("x"),
			"e":   1e21,
		}}
		raw, err := json.Marshal(statement)
		require.NoError(t, err)
		require.JSONEq(t, `{"statement":"RETURN $f","parameters":{"f":-42.0,"i":-42,"big":9007199254740993,"l":[2.0,3,null,"s",true],"m":{"x":1.0,"y":0.1},"s":[1.0,2.5],"n":{"k":[1]},"b":"eA==","e":1e+21}}`, string(raw))
		for _, text := range []string{`"f":-42.0`, `"x":1.0`, `"y":0.1`, `"s":[1.0,2.5]`, `"big":9007199254740993`} {
			require.Contains(t, string(raw), text)
		}
		raw, err = json.Marshal(remoteStatement{Statement: "RETURN 1"})
		require.NoError(t, err)
		require.JSONEq(t, `{"statement":"RETURN 1"}`, string(raw))
		// JSON has no NaN: it is sent as the text toFloat reads.
		raw, err = json.Marshal(remoteStatement{Statement: "RETURN $x", Parameters: map[string]interface{}{"x": math.NaN()}})
		require.NoError(t, err)
		require.JSONEq(t, `{"statement":"RETURN toFloat($x)","parameters":{"x":"NaN"}}`, string(raw))
		// A nil list or map of any type is null, at any depth; an empty one
		// stays empty.
		raw, err = json.Marshal(remoteStatement{Statement: "RETURN 1", Parameters: map[string]interface{}{
			"nil": nil, "list": []interface{}(nil), "strings": []string(nil), "floats": []float64(nil),
			"string_map": map[string]string(nil), "map": map[string]interface{}(nil),
			"nested": []interface{}{[]string(nil), map[string]string(nil), map[string]interface{}{"l": []int(nil), "f": []float64(nil)}},
			"empty":  []interface{}{[]interface{}{}, []string{}, map[string]string{}, map[string]interface{}{}},
		}})
		require.NoError(t, err)
		require.JSONEq(t, `{"statement":"RETURN 1","parameters":{"nil":null,"list":null,"strings":null,"floats":null,"string_map":null,"map":null,"nested":[null,null,{"l":null,"f":null}],"empty":[[],[],{},{}]}}`, string(raw))
	})
}

// A null parameter reaches Neo4j's HTTP API as null whatever its Go type, in
// autocommit and in an explicit transaction, for reads and for property
// writes (SET n.p = $x removes p).
func TestRemoteEngineHTTPNullParameters(t *testing.T) {
	uri := os.Getenv("NORNICDB_NEO4J_REFERENCE_HTTP_URI")
	if uri == "" {
		t.Skip("requires --ref neo-ref")
	}
	engine, err := NewRemoteEngine(RemoteEngineConfig{URI: uri, Database: "neo4j"})
	require.NoError(t, err)
	defer func() { _ = engine.Close() }()
	ctx := context.Background()
	run := fmt.Sprintf("remote-null-%d", time.Now().UnixNano())
	defer func() {
		_, _, _ = engine.QueryCypher(ctx, "MATCH (n:RemoteNullParameter {run: $run}) DETACH DELETE n", map[string]interface{}{"run": run}, RemoteAccessWrite)
	}()
	for _, value := range []struct {
		name  string
		value interface{}
	}{
		{"nil", nil}, {"list", []interface{}(nil)}, {"strings", []string(nil)}, {"floats", []float64(nil)},
		{"string_map", map[string]string(nil)}, {"map", map[string]interface{}(nil)},
	} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%v", value.name, explicit), func(t *testing.T) {
				query := func(ctx context.Context, statement string, params map[string]interface{}) ([]string, [][]interface{}, error) {
					return engine.QueryCypher(ctx, statement, params, RemoteAccessWrite)
				}
				if explicit {
					tx, err := engine.BeginCypherTx(ctx, RemoteAccessWrite)
					require.NoError(t, err)
					defer func() { _ = tx.Rollback(ctx) }()
					query = tx.QueryCypher
				}
				params := map[string]interface{}{"x": value.value, "run": run}
				_, rows, err := query(ctx, "RETURN $x IS NULL AS isNull", params)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{true}}, rows)
				_, rows, err = query(ctx, "CREATE (n:RemoteNullParameter {run: $run, p: 1}) SET n.p = $x RETURN n.p IS NULL AS removed", params)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{true}}, rows)
			})
		}
	}
	// UpdateNode with a nil collection removes the property instead of
	// storing an empty one.
	t.Run("update_node", func(t *testing.T) {
		id, err := engine.CreateNode(&Node{ID: NodeID(run + "-node"), Labels: []string{"RemoteNullParameter"}, Properties: map[string]interface{}{
			"run": run, "tags": []string{"a"}, "list": []interface{}{int64(1)}, "extra": "x", "kept": int64(7),
		}})
		require.NoError(t, err)
		require.NoError(t, engine.UpdateNode(&Node{ID: id, Properties: map[string]interface{}{
			"tags": []string(nil), "list": []interface{}(nil), "extra": map[string]string(nil),
		}}))
		_, rows, err := engine.QueryCypher(ctx, "MATCH (n:RemoteNullParameter {id: $id}) RETURN n.tags IS NULL, n.list IS NULL, n.extra IS NULL, n.kept", map[string]interface{}{"id": string(id)}, RemoteAccessRead)
		require.NoError(t, err)
		require.Equal(t, [][]interface{}{{true, true, true, int64(7)}}, rows)
	})
}
