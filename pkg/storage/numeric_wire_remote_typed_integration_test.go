package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNumericWireRemoteTypedIntegerComposition(t *testing.T) {
	for _, tc := range []struct {
		raw  string
		want any
	}{
		{"9007199254740993", int64(9007199254740993)},
		{"-9007199254740993", int64(-9007199254740993)},
		{"9223372036854775807", int64(math.MaxInt64)},
		{"-9223372036854775808", int64(math.MinInt64)},
		{"9223372036854775808", uint64(math.MaxInt64) + 1},
		{"18446744073709551615", uint64(math.MaxUint64)},
		{"2.0", float64(2)},
		{"5e-324", math.SmallestNonzeroFloat64},
	} {
		t.Run(tc.raw, func(t *testing.T) {
			host := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					Statements []struct {
						Statement  string
						Parameters map[string]json.RawMessage
					}
				}
				if r.ContentLength != 0 {
					if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
						t.Error(err)
						return
					}
				}
				if len(request.Statements) > 0 {
					assert.Contains(t, request.Statements[0].Statement, "date($day)")
					assert.JSONEq(t, `{"Whole":2.0}`, string(request.Statements[0].Parameters["envelope"]))
					var envelope map[string]json.Number
					assert.NoError(t, json.Unmarshal(request.Statements[0].Parameters["envelope"], &envelope))
					assert.Equal(t, json.Number("2.0"), envelope["Whole"])
				}
				w.Header().Set("Location", "http://"+r.Host+"/db/test/tx/1")
				_, _ = fmt.Fprintf(w, `{"commit":"http://%s/db/test/tx/1/commit","results":[{"columns":["value"],"data":[{"row":[{"v":%s,"nested":[%s,2.0],"day":"2024-02-29","point":{"type":"Point","coordinates":[2.0,5e-324],"crs":{"srid":7203}}}],"meta":[null,null,null,{"type":"date"},{"type":"point"}]}]}],"errors":[]}`, r.Host, tc.raw, tc.raw)
			}))
			defer host.Close()
			remote, err := NewRemoteEngine(RemoteEngineConfig{URI: host.URL, Database: "test"})
			require.NoError(t, err)
			defer remote.Close()
			params := map[string]any{"day": dbtype.Date(time.Date(2024, 2, 29, 0, 0, 0, 0, time.UTC)), "envelope": struct{ Whole float64 }{2}}
			want := map[string]any{"v": tc.want, "nested": []any{tc.want, float64(2)}, "day": RemoteTypedValue{Type: "date", Text: "2024-02-29"}, "point": RemoteTypedValue{Type: "point", SRID: 7203, Coordinates: []float64{2, math.SmallestNonzeroFloat64}}}
			_, rows, err := remote.QueryCypher(context.Background(), "RETURN $day, $envelope", params, RemoteAccessRead)
			require.NoError(t, err)
			require.Equal(t, want, rows[0][0])
			tx, err := remote.BeginCypherTx(context.Background(), RemoteAccessWrite)
			require.NoError(t, err)
			_, rows, err = tx.QueryCypher(context.Background(), "RETURN $day, $envelope", params)
			require.NoError(t, err)
			require.Equal(t, want, rows[0][0])
			require.NoError(t, tx.Rollback(context.Background()))
		})
	}
}
