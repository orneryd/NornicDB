package cypher

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// The inside of a relationship pattern is read by one owner
// (relationshipDeclarationOf) wherever a statement uses it. The oracle holds
// one statement stream, run in order on one database, and the output Neo4j
// 5.26.30 gave for each statement (cypher-shell --format plain lines; an
// EXPLAIN that succeeds is "<explain ok>"). It covers dynamic types holding
// braces, stars, ranges, quotes and escapes, CASE, backtick-quoted types
// holding quotes and arrows, nested map access in property maps, inline
// WHERE, and legacy ranges with dynamic types, in MATCH, OPTIONAL MATCH,
// CREATE, MERGE, UNWIND … MERGE, write stretches, schema statements and
// EXPLAIN / PROFILE.
func TestRelationshipPatternReaderMatchesNeo4j(t *testing.T) {
	data, err := os.ReadFile("testdata/relationship_pattern_oracle.json")
	require.NoError(t, err)
	var steps []struct {
		Query  string   `json:"query"`
		Output []string `json:"output"`
	}
	require.NoError(t, json.Unmarshal(data, &steps))
	for _, explicit := range []bool{false, true} {
		name := "autocommit"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			exec := NewStorageExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "relpattern"))
			ctx := context.Background()
			for index, step := range steps {
				got := relationshipPatternOracleRun(ctx, exec, step.Query, explicit)
				if strings.Join(got, "\n") != strings.Join(step.Output, "\n") {
					t.Errorf("statement %d: %s\n  Neo4j: %q\n  got:   %q", index+1, step.Query, step.Output, got)
				}
			}
		})
	}
}

// relationshipPatternOracleRun runs one statement (in its own explicit
// transaction when explicit) and returns its output as cypher-shell prints it.
func relationshipPatternOracleRun(ctx context.Context, exec *StorageExecutor, query string, explicit bool) (lines []string) {
	defer func() {
		if p := recover(); p != nil {
			lines = []string{fmt.Sprintf("panic: %v", p)}
		}
	}()
	isSchema := strings.HasPrefix(query, "CREATE INDEX") || strings.HasPrefix(query, "CREATE CONSTRAINT") ||
		strings.HasPrefix(query, "CREATE FULLTEXT") || strings.HasPrefix(query, "SHOW ")
	explicit = explicit && !isSchema
	if explicit {
		if _, err := exec.Execute(ctx, "BEGIN", nil); err != nil {
			return []string{"error: " + err.Error()}
		}
	}
	result, err := exec.Execute(ctx, query, nil)
	if explicit {
		if err == nil {
			_, err = exec.Execute(ctx, "COMMIT", nil)
		} else {
			_, _ = exec.Execute(ctx, "ROLLBACK", nil)
		}
	}
	if err != nil {
		return []string{"error: " + err.Error()}
	}
	if strings.HasPrefix(query, "EXPLAIN ") {
		return []string{"<explain ok>"}
	}
	if result == nil || len(result.Rows) == 0 {
		return nil
	}
	lines = append(lines, strings.Join(result.Columns, ", "))
	for _, row := range result.Rows {
		values := make([]string, len(row))
		for i, value := range row {
			values[i] = cypherShellPlainValue(value)
		}
		lines = append(lines, strings.Join(values, ", "))
	}
	return lines
}

// cypherShellPlainValue is a scalar, list or map as cypher-shell's plain
// format prints it.
func cypherShellPlainValue(value interface{}) string {
	switch v := value.(type) {
	case nil:
		return "NULL"
	case bool:
		if v {
			return "TRUE"
		}
		return "FALSE"
	case string:
		quoted, _ := json.Marshal(v)
		return strings.NewReplacer(`\u003c`, "<", `\u003e`, ">", `\u0026`, "&").Replace(string(quoted))
	case []string:
		items := make([]interface{}, len(v))
		for i, item := range v {
			items[i] = item
		}
		return cypherShellPlainValue(items)
	case []interface{}:
		items := make([]string, len(v))
		for i, item := range v {
			items[i] = cypherShellPlainValue(item)
		}
		return "[" + strings.Join(items, ", ") + "]"
	case map[string]interface{}:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		items := make([]string, len(keys))
		for i, key := range keys {
			items[i] = key + ": " + cypherShellPlainValue(v[key])
		}
		return "{" + strings.Join(items, ", ") + "}"
	default:
		return fmt.Sprint(v)
	}
}
