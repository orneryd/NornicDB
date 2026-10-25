package cypher

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Observe the installed metadata through SHOW, then reinstall the actual SHOW
// output. Delimiters are data inside each key, for every supported DDL mode.
func TestNameDefinitionDDLRoundTrip(t *testing.T) {
	for _, key := range []string{"a.b", "a,b", "a(b)", "space ", "tick`key"} {
		for _, entity := range []string{"NODE", "RELATIONSHIP"} {
			for _, mode := range []string{"RANGE", "VECTOR", "FULLTEXT", "UNIQUE_SCALAR", "UNIQUE", "EXISTS", "KEY", "TYPE", "DOMAIN", "TEMPORAL"} {
				t.Run(key+"/"+entity+"/"+mode, func(t *testing.T) {
					keys := []string{key}
					var text string
					kind := map[string]storage.ConstraintType{"UNIQUE": storage.ConstraintUnique, "UNIQUE_SCALAR": storage.ConstraintUnique, "EXISTS": storage.ConstraintExists, "KEY": storage.ConstraintNodeKey, "DOMAIN": storage.ConstraintDomain, "TEMPORAL": storage.ConstraintTemporal}[mode]
					if entity == "RELATIONSHIP" && mode == "KEY" {
						kind = storage.ConstraintRelationshipKey
					}
					if mode == "TEMPORAL" {
						keys = append(keys, "from,key", "to.key")
					}
					if mode == "KEY" || mode == "UNIQUE" {
						keys = append(keys, "second,key")
					}
					switch mode {
					case "RANGE", "VECTOR", "FULLTEXT":
						options := map[string]any(nil)
						if mode == "VECTOR" {
							options = map[string]any{"vector.dimensions": int64(3)}
						}
						text = showIndexCreateStatement(mode, key, entity, []string{"Target"}, keys, options)
					case "TYPE":
						text = showPropertyTypeCreateStatement(key, storage.ConstraintEntityType(entity), "Target", key, "INTEGER")
					default:
						text = showConstraintCreateStatement(storage.Constraint{Name: key, Type: kind, EntityType: storage.ConstraintEntityType(entity), Label: "Target", Properties: keys, AllowedValues: []any{int64(1)}}).(string)
					}
					for pass := 0; pass < 2; pass++ {
						e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "definition"))
						query := func(q string) ([][]any, error) {
							r, err := e.Execute(context.Background(), q, nil)
							if err != nil {
								return nil, err
							}
							return r.Rows, nil
						}
						require.NoError(t, cyphernames.SchemaRoundTrip(text, "Target", keys, query), text)
						show := "SHOW CONSTRAINTS"
						if strings.Contains(text, " INDEX ") {
							show = "SHOW INDEXES"
						}
						rows, err := query(show + " YIELD name, createStatement WHERE name = '" + key + "' RETURN createStatement")
						require.NoError(t, err)
						require.Len(t, rows, 1)
						text = rows[0][0].(string)
					}
				})
			}
		}
	}
}

func TestNameDefinitionProjectionIdentity(t *testing.T) {
	for _, name := range []string{"space ", "two words", "tick`name", "a.b", "a,b", "a(b)"} {
		t.Run(name, func(t *testing.T) {
			text := projectionVariableText(name)
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "projection_identity"))
			r, err := e.Execute(context.Background(), "WITH 1 AS "+cyphertext.QuoteName(name)+" RETURN "+text+" AS "+text, nil)
			require.NoError(t, err)
			require.Equal(t, []string{name}, r.Columns)
			require.EqualValues(t, 1, r.Rows[0][0])
		})
	}
}

type runtimeNameSubject struct {
	Route, Name, Text string
	Parameters        map[string]any
}

func runtimeNameSubjects(t *testing.T, unquoted bool) []runtimeNameSubject {
	t.Helper()
	cmd := exec.Command("node", "../../internal/testutil/cyphernames/runtime_subjects.mjs")
	if unquoted {
		cmd.Env = append(os.Environ(), "NORNIC_TEST_NAME_QUOTES=off")
	}
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, string(output))
	var subjects []runtimeNameSubject
	require.NoError(t, json.Unmarshal(output, &subjects), string(output))
	require.NotEmpty(t, subjects)
	return subjects
}

func TestNameDefinitionRuntimeSubjects(t *testing.T) {
	for _, s := range runtimeNameSubjects(t, false) {
		t.Run(s.Route+"/"+s.Name, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "runtime_subject"))
			ctx := context.Background()
			if s.Route == "browser" {
				_, err := e.Execute(ctx, "CREATE (:BrowserNode {id:'subject'})", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, s.Text, s.Parameters)
				require.NoError(t, err, s.Text)
				r, err := e.Execute(ctx, "MATCH (n:BrowserNode) RETURN n", nil)
				require.NoError(t, err)
				require.EqualValues(t, 2, r.Rows[0][0].(*storage.Node).Properties[s.Name])
			} else if s.Route == "upsert" {
				_, err := e.Execute(ctx, s.Text, s.Parameters)
				require.NoError(t, err, s.Text)
				r, err := e.Execute(ctx, "MATCH (n) RETURN labels(n)", nil)
				require.NoError(t, err)
				require.EqualValues(t, []any{s.Name}, r.Rows[0][0])
			} else if s.Route == "read" {
				_, err := e.Execute(ctx, "CREATE (:"+cyphertext.QuoteName(s.Name)+")", nil)
				require.NoError(t, err)
				r, err := e.Execute(ctx, s.Text, nil)
				require.NoError(t, err, s.Text)
				require.EqualValues(t, 1, r.Rows[0][0])
			} else {
				key, suffix, show := "qdrant_id", "_qdrant_id_unique", "SHOW CONSTRAINTS"
				if strings.Contains(s.Text, " INDEX ") {
					key, suffix, show = "embedding", "_embedding", "SHOW INDEXES"
				}
				query := func(q string) ([][]any, error) {
					r, err := e.Execute(ctx, q, nil)
					if err != nil {
						return nil, err
					}
					return r.Rows, nil
				}
				require.NoError(t, cyphernames.SchemaRoundTrip(s.Text, s.Name, []string{key}, query))
				r, err := e.Execute(ctx, show+" YIELD name WHERE name = $name RETURN name", map[string]any{"name": s.Name + suffix})
				require.NoError(t, err)
				require.Equal(t, [][]any{{s.Name + suffix}}, r.Rows)
			}
		})
	}
}

func TestNameDefinitionRuntimeQuotingControls(t *testing.T) {
	observed := map[string]int{}
	for _, s := range runtimeNameSubjects(t, true) {
		if s.Route == "browser" && s.Name != "two words" {
			continue
		}
		if s.Route != "browser" && s.Name != "customer-data_docs" {
			continue
		}
		t.Run(s.Route, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "unquoted_runtime"))
			if s.Route == "schema" {
				// The native outer DDL grammar historically admits some raw
				// hyphenated names. Canonical source-name reparsing still must
				// reject a truncated identifier: parser success alone is no oracle.
				marker, suffix := "CONSTRAINT ", "_qdrant_id_unique"
				if strings.Contains(s.Text, " INDEX ") {
					marker, suffix = "INDEX ", "_embedding"
				}
				at := strings.Index(s.Text, marker) + len(marker)
				token, ok := cyphertext.ScanIdentifier(s.Text, at)
				got := token.Value
				require.True(t, ok)
				require.NotEqual(t, s.Name+suffix, got, "quoting-removal control retained the full schema identifier")
			} else {
				_, err := e.Execute(context.Background(), s.Text, s.Parameters)
				require.Error(t, err, "unquoted production subject survived: %s", s.Text)
			}
			observed[s.Route]++
		})
	}
	require.Equal(t, map[string]int{"browser": 1, "schema": 2, "read": 1, "upsert": 1}, observed)
}

// Legacy ON :Label(bare-key) admission belongs to the native DDL branch. ANTLR
// independently rejects that legacy statement shape on the release base. This
// exercises the branch's actual reader/install and standard SHOW recreation,
// without claiming that the ANTLR public grammar accepts legacy ON syntax.
func TestNameDefinitionLegacyIndexReader(t *testing.T) {
	for _, key := range []string{"a.b", "a,b", "a(b)", "space ", "tick`key"} {
		t.Run(key, func(t *testing.T) {
			e := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "legacy_names"))
			text := "CREATE INDEX " + cyphertext.QuoteName(key) + " ON :Target(" + cyphertext.QuoteName(key) + ")"
			query := func(q string) ([][]any, error) {
				var r *ExecuteResult
				var err error
				if strings.Contains(q, " ON :") {
					r, err = e.executeCreateIndex(context.Background(), q)
				} else {
					r, err = e.Execute(context.Background(), q, nil)
				}
				if err != nil {
					return nil, err
				}
				return r.Rows, nil
			}
			require.NoError(t, cyphernames.SchemaRoundTrip(text, "Target", []string{key}, query))
			rows, err := query("SHOW INDEXES YIELD name, createStatement WHERE name = '" + key + "' RETURN createStatement")
			require.NoError(t, err)
			require.Len(t, rows, 1)
			other := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "legacy_recreated"))
			require.NoError(t, cyphernames.SchemaRoundTrip(rows[0][0].(string), "Target", []string{key}, func(q string) ([][]any, error) {
				r, err := other.Execute(context.Background(), q, nil)
				if err != nil {
					return nil, err
				}
				return r.Rows, nil
			}))
		})
	}
}
