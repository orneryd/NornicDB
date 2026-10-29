package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"strings"
	"testing"

	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// Embedded interfaces deliberately panic for unexpected calls. The actual
// streaming functions must visit their first and cursor pages, then terminate.
type nameCaptureSession struct {
	neo4j.SessionWithContext
	queries []string
}

func (s *nameCaptureSession) Run(_ context.Context, q string, _ map[string]any, _ ...func(*neo4j.TransactionConfig)) (neo4j.ResultWithContext, error) {
	s.queries = append(s.queries, q)
	return &nameCaptureResult{row: len(s.queries) == 1}, nil
}

type nameCaptureResult struct {
	neo4j.ResultWithContext
	row bool
}

func (r *nameCaptureResult) Next(context.Context) bool { row := r.row; r.row = false; return row }
func (r *nameCaptureResult) Err() error                { return nil }
func (r *nameCaptureResult) Record() *neo4j.Record {
	return &neo4j.Record{Keys: []string{"id"}, Values: []any{"cursor"}}
}

type migrationStatement struct{ Route, Text string }

func migrationStatements(t *testing.T, name string) []migrationStatement {
	t.Helper()
	out := []migrationStatement{{"create", edgeCreateCypher(name)}, {"props", edgePropsCypher(name)}}
	for _, edge := range []bool{false, true} {
		session := &nameCaptureSession{}
		yield := func([]map[string]any) error { return nil }
		var err error
		if edge {
			err = streamEdges(context.Background(), session, name, 1, yield)
		} else {
			err = streamNodes(context.Background(), session, name, 1, yield)
		}
		require.NoError(t, err)
		require.Len(t, session.queries, 2, "both source pagination branches")
		for i, text := range session.queries {
			route := "node"
			if edge {
				route = "edge"
			}
			out = append(out, migrationStatement{fmtRoute(route, i), text})
		}
	}
	for _, kind := range []string{"UNIQUENESS", "NODE_KEY", "NODE_PROPERTY_EXISTENCE", "RELATIONSHIP_PROPERTY_EXISTENCE", "RELATIONSHIP_UNIQUENESS"} {
		entity := "NODE"
		if strings.HasPrefix(kind, "RELATIONSHIP") {
			entity = "RELATIONSHIP"
		}
		row := map[string]any{"name": name, "type": kind, "entityType": entity, "labelsOrTypes": []any{name}, "properties": []any{name}}
		out = append(out, migrationStatement{kind, constraintToCypher(row)})
		if kind == "UNIQUENESS" || kind == "NODE_KEY" {
			row["properties"] = []any{name, "second`key"}
			out = append(out, migrationStatement{kind + "-composite", constraintToCypher(row)})
		}
	}
	for _, kind := range []string{"RANGE", "BTREE", "FULLTEXT", "VECTOR"} {
		row := map[string]any{"name": name, "type": kind, "entityType": "NODE", "labelsOrTypes": []any{name}, "properties": []any{name}, "options": map[string]any{"indexConfig": map[string]any{"vector.dimensions": int64(3)}}}
		out = append(out, migrationStatement{kind, indexToCypher(row)})
		if kind == "RANGE" || kind == "BTREE" || kind == "FULLTEXT" {
			row["properties"] = []any{name, "second`key"}
			out = append(out, migrationStatement{kind + "-composite", indexToCypher(row)})
		}
	}
	return out
}

func fmtRoute(route string, page int) string {
	if page == 0 {
		return route + "-first"
	}
	return route + "-cursor"
}

func TestMigrationEmitterProperty(t *testing.T) {
	names := cyphernames.Names()
	input, err := json.Marshal(names)
	require.NoError(t, err)
	peers := map[string]map[string][]migrationStatement{}
	for _, peer := range []struct{ name, command, script string }{{"node", "node", "migration_names_probe.mjs"}, {"python", "python3", "migration_names_probe.py"}} {
		cmd := exec.Command(peer.command, peer.script)
		cmd.Stdin = bytes.NewReader(input)
		var stderr bytes.Buffer
		cmd.Stderr = &stderr
		output, err := cmd.Output()
		require.NoError(t, err, "%s: %s", peer.name, stderr.String())
		var statements map[string][]migrationStatement
		require.NoError(t, json.Unmarshal(output, &statements), string(output))
		require.Len(t, statements, len(names))
		peers[peer.name] = statements
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			routes := map[string][]migrationStatement{"go": migrationStatements(t, name), "node": peers["node"][name], "python": peers["python"][name]}
			for language, statements := range routes {
				t.Run(language, func(t *testing.T) {
					require.Len(t, statements, len(routes["go"]))
					for _, statement := range statements {
						t.Run(statement.Route, func(t *testing.T) {
							text := statement.Text
							require.NotEmpty(t, text)
							if strings.Contains(text, "[r:") || statement.Route == "create" {
								params := map[string]any{"row": map[string]any{"id": "edge"}}
								expected := map[string]any{}
								if statement.Route == "create" || statement.Route == "props" {
									expected["_neo4j_id"] = "edge"
								}
								emittercheck.Check(t, text, name, params, expected)
								got, err := cyphernames.ReadType(text)
								require.NoError(t, err, text)
								require.Equal(t, name, got)

							} else {
								start := strings.Index(text, "(n:") + 3
								require.GreaterOrEqual(t, start, 3, text)
								token, ok := cyphertext.ScanIdentifier(text, start)
								got, end := token.Value, token.End
								require.True(t, ok, text)
								require.Equal(t, name, got)
								require.Equal(t, byte(')'), text[end], text)
							}
							if strings.HasPrefix(text, "CREATE ") {
								// DDL name and property identifiers use the same canonical contract.
								require.Contains(t, text, cyphertext.QuoteName(name)+" IF NOT EXISTS")
								variable := "n"
								if strings.Contains(text, "[r:") {
									variable = "r"
								}
								require.Contains(t, text, variable+"."+cyphertext.QuoteName(name))
								keys := []string{name}
								if strings.HasSuffix(statement.Route, "-composite") {
									keys = append(keys, "second`key")
								}
								executor := migrationExecutor(t)
								require.NoError(t, cyphernames.SchemaRoundTrip(text, name, keys, func(query string) ([][]any, error) {
									result, err := executor.Execute(context.Background(), query, nil)
									if err != nil {
										return nil, err
									}
									return result.Rows, nil
								}))
							}
						})
					}
					// Re-import the actual emitted relationship brackets, preserving the
					// type and migration key. Fixed _Migrated endpoint labels are hidden
					// by ordinary-database policy; system graph queries are disallowed.
					// That separate pre-existing CLI policy is not this name property.
					executor := migrationExecutor(t)
					ctx := context.Background()
					row := map[string]any{"id": "edge", "props": map[string]any{name: int64(7)}}
					for _, statement := range statements {
						if statement.Route == "create" || statement.Route == "props" {
							open := strings.IndexByte(statement.Text, '[')
							require.GreaterOrEqual(t, open, 0)
							close := cyphertext.FindMatchingDelimiter(statement.Text, open, '[', ']', cyphertext.OpeningDelimiter)
							require.Greater(t, close, open)
							bracket := statement.Text[open : close+1]
							query := "WITH $row AS row MATCH ()-" + bracket + "->() SET r += row.props"
							if statement.Route == "create" {
								bracket = "[r" + bracket[1:]
								query = "WITH $row AS row CREATE ()-" + bracket + "->()"
							}
							_, err := executor.Execute(ctx, query, map[string]any{"row": row})
							require.NoError(t, err, query)
						}
					}
					result, err := executor.Execute(ctx, "MATCH ()-[r]->() RETURN r", nil)
					require.NoError(t, err)
					require.Len(t, result.Rows, 1)
					edge := result.Rows[0][0].(*storage.Edge)
					require.Equal(t, name, edge.Type)
					require.EqualValues(t, 7, edge.Properties[name])
				})
			}
		})
	}
}

func migrationExecutor(t *testing.T) *cypher.StorageExecutor {
	t.Helper()
	store := testfixture.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return cypher.NewStorageExecutor(storage.NewNamespacedEngine(store, "migration_names"))
}

func TestMigrationReviewExact(t *testing.T) {
	t.Run("pipe-constraint", func(t *testing.T) {
		text := constraintToCypher(map[string]any{"name": "c", "type": "RELATIONSHIP_PROPERTY_EXISTENCE", "entityType": "RELATIONSHIP", "labelsOrTypes": []any{"A|B"}, "properties": []any{"p"}})
		_, err := migrationExecutor(t).Execute(context.Background(), text, nil)
		require.NoError(t, err, text)
		got, err := cyphernames.ReadType(text)
		require.NoError(t, err, text)
		require.Equal(t, "A|B", got)
	})
	t.Run("backtick-create", func(t *testing.T) {
		text := edgeCreateCypher("A`B")
		got, err := cyphernames.ReadType(text)
		require.NoError(t, err, text)
		require.Equal(t, "A`B", got)
	})
}
