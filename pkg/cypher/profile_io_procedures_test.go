package cypher

import (
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/queryprofile"
	"github.com/stretchr/testify/require"
)

func TestProfileLocalLoadersObserveNoDatabaseReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, tc := range []struct {
			name, content, column string
			rows                  int
		}{
			{"apoc.load.json", `{"v":"one"}`, "value", 1},
			{"apoc.load.json", `[{"v":"one"},{"v":"two"}]`, "value", 2},
			{"apoc.load.jsonArray", `{"v":"one"}`, "value", 1},
			{"apoc.load.jsonArray", `[{"v":"one"},{"v":"two"}]`, "value", 2},
			{"apoc.load.csv", "v\none\ntwo\n", "map", 2},
		} {
			t.Run(tc.name+"/"+tc.content, func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "input")
				require.NoError(t, os.WriteFile(file, []byte(tc.content), 0600))
				exec, _ := newProfileRuntimeTestExecutor(t)
				exec.SetAllowLocalAPOCFileAccess(true)
				rows, collector, plan := profiledProcedureStatement(t, exec, "CALL "+tc.name+"($file) YIELD "+tc.column+" RETURN "+tc.column, map[string]any{"file": file})
				require.Len(t, rows, tc.rows)
				require.Equal(t, map[string]interface{}{"v": "one"}, rows[0][0])
				if tc.rows == 2 {
					require.Equal(t, map[string]interface{}{"v": "two"}, rows[1][0])
				}
				require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
				require.Zero(t, plan.TotalDBHits)
				require.Empty(t, plan.Unobserved)
				require.Empty(t, collector.accessObservation(), "file rows are not graph reads")
			})
		}
	})
}

func TestProfileExportsObserveAcquiredPopulationAndNestedReads(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		for _, format := range []string{"json", "csv"} {
			for _, populated := range []bool{false, true} {
				name := format + "/empty"
				if populated {
					name = format + "/populated"
				}
				t.Run(name, func(t *testing.T) {
					var seeds []string
					if populated {
						seeds = []string{"CREATE (a:Exported {name:'a'})-[:LINK {v:1}]->(b:Exported {name:'b'})"}
					}
					exec, _ := newProfileRuntimeTestExecutor(t, seeds...)
					rows, collector, plan := profiledProcedureStatement(t, exec, "CALL apoc.export."+format+".all('')", nil)
					require.Len(t, rows, 1)
					require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
					expectedNodes, expectedEdges := int64(0), int64(0)
					if populated {
						expectedNodes, expectedEdges = 2, 1
					}
					for _, expect := range []struct {
						kind  string
						count int64
					}{{queryprofile.KindAllNodesScan, expectedNodes}, {queryprofile.KindAllRelationshipsScan, expectedEdges}} {
						reads := profileAccessesOfKind(collector, expect.kind)
						require.Len(t, reads, 1, "even an empty acquired population is known")
						require.Equal(t, expect.count, reads[0].Rows)
						require.Equal(t, expect.count, reads[0].Hits)
					}
					require.Equal(t, expectedNodes+expectedEdges, plan.TotalDBHits)
					require.Empty(t, plan.Unobserved)
				})
			}
			for _, query := range []string{"RETURN 7 AS n", "MATCH (n:Exported) RETURN n.name AS name ORDER BY name"} {
				t.Run(format+"/query/"+query, func(t *testing.T) {
					exec, _ := newProfileRuntimeTestExecutor(t, "CREATE (:Exported {name:'a'}),(:Exported {name:'b'})")
					rows, _, plan := profiledProcedureStatement(t, exec, "CALL apoc.export."+format+".query($query,'')", map[string]any{"query": query})
					require.Len(t, rows, 1)
					require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
					expected := int64(0)
					if strings.HasPrefix(query, "MATCH") {
						expected = 2
					}
					require.Equal(t, expected, plan.TotalDBHits)
					require.Empty(t, plan.Unobserved)
				})
			}
		}
	})
}

func TestProfileJSONImportUsesActualNativeMutationRequests(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		file := filepath.Join(t.TempDir(), "graph.json")
		require.NoError(t, os.WriteFile(file, []byte(`{"nodes":[{"id":"a","labels":["Imported"],"properties":{"v":1}},{"id":"b","labels":["Imported"],"properties":{"v":2}}],"relationships":[{"id":"ab","type":"LINK","startNode":"a","endNode":"b","properties":{"v":3}}]}`), 0600))
		var plainRows [][]interface{}
		for _, profiled := range []bool{false, true} {
			exec, _ := newProfileRuntimeTestExecutor(t)
			exec.SetAllowLocalAPOCFileAccess(true)
			query := "CALL apoc.import.json($file)"
			if profiled {
				query = "PROFILE " + query
			}
			result, err := exec.Execute(t.Context(), query, map[string]any{"file": file})
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{file, 2, 1}}, result.Rows)
			if !profiled {
				plainRows = result.Rows
			} else {
				require.Equal(t, plainRows, result.Rows)
				plan := executionPlanFromResult(t, result)
				require.True(t, plan.TotalDBHitsObserved, "%v", plan.Unobserved)
				require.Empty(t, plan.Unobserved)
				require.EqualValues(t, 3, plan.TotalDBHits)
				for _, expect := range []struct {
					kind  string
					count int64
				}{{profileKindNodeCreateRequest, 2}, {profileKindRelationshipCreateRequest, 1}} {
					op := planOperatorOfType(plan.Root, expect.kind)
					require.NotNil(t, op)
					require.Equal(t, expect.count, op.DBHits)
				}
			}
			nodes, err := exec.storage.AllNodes(t.Context())
			require.NoError(t, err)
			edges, err := exec.storage.AllEdges(t.Context())
			require.NoError(t, err)
			require.Len(t, nodes, 2)
			require.Len(t, edges, 1)
			require.Equal(t, "LINK", edges[0].Type)
			require.EqualValues(t, 3, edges[0].Properties["v"])
		}
	})
}

func TestProfileLoaderProvidersStayOpaqueOnlyOnActualInvocation(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		exec, _ := newProfileRuntimeTestExecutor(t)
		resolutions, requests := 0, 0
		exec.apocIO = exec.apocIO.WithRemoteURLAccess(true).WithRemoteURLAllowlist([]string{"controlled.example"}).WithHostResolver(func(context.Context, string) ([]net.IPAddr, error) {
			resolutions++
			return []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}, nil
		}).WithHTTPClient(&http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			requests++
			return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"v":"one"}`)), Header: make(http.Header)}, nil
		})})
		result, err := exec.Execute(t.Context(), "PROFILE CALL apoc.load.json('https://controlled.example/data') YIELD value RETURN value", nil)
		require.NoError(t, err)
		require.Equal(t, 1, resolutions)
		require.Equal(t, 1, requests)
		require.Equal(t, [][]interface{}{{map[string]interface{}{"v": "one"}}}, result.Rows)
		plan := executionPlanFromResult(t, result)
		require.False(t, plan.TotalDBHitsObserved)
		require.Contains(t, plan.Unobserved, "database reads: APOC host resolver")
		require.Contains(t, plan.Unobserved, "database reads: APOC HTTP client")
		require.NotContains(t, plan.Unobserved, "database reads: procedure callback: apoc.load.json")
		exec.apocIO = exec.apocIO.WithRemoteURLAccess(false)
		collector := newProfileRuntimeCollector()
		_, err = exec.Execute(withProfileRuntimeCollector(t.Context(), collector), "CALL apoc.load.json('https://controlled.example/data')", nil)
		require.Error(t, err)
		require.Equal(t, 1, resolutions, "denied access does not invoke providers")
		require.Equal(t, 1, requests)
		require.Empty(t, collector.accessObservation())
	})
}
