package cypher

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestDecodedNameCSVEmission(t *testing.T) {
	for _, name := range []string{"ordinary", " ", "  ", "\t", "left,right", "a\"b", "line\nbreak"} {
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_csv"))
			exec.SetAllowLocalAPOCFileAccess(true)
			ctx := context.Background()
			t.Run("query_header", func(t *testing.T) {
				file := filepath.Join(t.TempDir(), "query.csv")
				_, err := exec.Execute(ctx, "CALL apoc.export.csv.query($query, $file)", map[string]interface{}{"query": "RETURN 7 AS " + cyphertext.QuoteName(name), "file": file})
				require.NoError(t, err)
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				records, err := csv.NewReader(strings.NewReader(string(data))).ReadAll()
				require.NoError(t, err)
				require.Equal(t, [][]string{{name}, {"7"}}, records)
			})
			t.Run("graph_label_and_type", func(t *testing.T) {
				_, err := exec.Execute(ctx, fmt.Sprintf("CREATE (a:%s)-[:%s]->(b)", cyphertext.QuoteName(name), cyphertext.QuoteName(name)), nil)
				require.NoError(t, err)
				file := filepath.Join(t.TempDir(), "graph.csv")
				_, err = exec.Execute(ctx, "CALL apoc.export.csv.all($file)", map[string]interface{}{"file": file})
				require.NoError(t, err)
				data, err := os.ReadFile(file)
				require.NoError(t, err)
				sections := strings.Split(string(data), "# Relationships\n")
				require.Len(t, sections, 2)
				nodes := strings.TrimPrefix(sections[0], "# Nodes\n")
				nodeRows, err := csv.NewReader(strings.NewReader(nodes)).ReadAll()
				require.NoError(t, err)
				labels := []string{}
				for _, row := range nodeRows[1:] {
					if row[1] != "" {
						labels = append(labels, row[1])
					}
				}
				require.Equal(t, []string{name}, labels)
				edges, err := csv.NewReader(strings.NewReader(sections[1])).ReadAll()
				require.NoError(t, err)
				require.Len(t, edges, 2)
				require.Equal(t, name, edges[1][1])
			})
		})
	}
}

func TestDecodedNameSignatureOutputs(t *testing.T) {
	const procedure = "r5.signatureOutput"
	for _, name := range []string{"ordinary", " ", "a`b", "x::y"} {
		t.Run(name, func(t *testing.T) {
			cleanupAdmissionProcedure(t, procedure)
			require.NoError(t, RegisterUserProcedure(ProcedureSpec{Name: procedure, Mode: ProcedureModeRead, Signature: procedure + "() :: (" + cyphertext.QuoteName(name) + " :: INTEGER)"}, func(context.Context, *StorageExecutor, string, []interface{}) (*ExecuteResult, error) {
				return &ExecuteResult{Columns: []string{name}, Rows: [][]interface{}{{int64(7)}}}, nil
			}))
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "decoded_signature"))
			columns, err := exec.StatementResultColumns("CALL "+procedure+"()", RawSource)
			require.NoError(t, err)
			require.Equal(t, []string{name}, columns, "public metadata must agree with the runtime header")
			result, err := exec.Execute(context.Background(), "CALL "+procedure+"() YIELD "+cyphertext.QuoteName(name)+" AS value RETURN value", nil)
			require.NoError(t, err)
			require.Equal(t, [][]interface{}{{int64(7)}}, result.Rows)
		})
	}
}
