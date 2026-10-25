package heimdall

import (
	"context"
	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/internal/testutil/cyphernames"
	"github.com/orneryd/nornicdb/internal/testutil/emittercheck"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
)

type autocompleteNameDB struct {
	memoryDBRouter
	exec *cypher.StorageExecutor
	name string
}

func (db *autocompleteNameDB) Query(ctx context.Context, database, query string, params map[string]any) ([]map[string]any, error) {
	switch {
	case strings.Contains(query, "db.labels()"):
		return []map[string]any{{"label": db.name}}, nil
	case strings.Contains(query, "db.relationshipTypes()"):
		return []map[string]any{{"relationshipType": db.name}}, nil
	case strings.Contains(query, "UNWIND props"):
		return []map[string]any{{"prop": db.name}}, nil
	}
	result, err := db.exec.Execute(ctx, query, params)
	if err != nil {
		return nil, err
	}
	rows := make([]map[string]any, len(result.Rows))
	for index, row := range result.Rows {
		rows[index] = map[string]any{}
		for column, key := range result.Columns {
			rows[index][key] = row[column]
		}
	}
	return rows, nil
}

func TestAutocompleteRelationshipNamesRoundTrip(t *testing.T) {
	for _, name := range append(cyphernames.Names(), "$(R)") {
		t.Run(name, func(t *testing.T) {
			engine, err := storage.NewBadgerEngineInMemory()
			require.NoError(t, err)
			defer engine.Close()
			exec := cypher.NewStorageExecutor(storage.NewNamespacedEngine(engine, "test"))
			quoted := cyphertext.QuoteName(name)
			created, err := exec.Execute(context.Background(), "CREATE (a:"+quoted+" {"+quoted+":1})-[:"+quoted+"]->(b) RETURN a", nil)
			require.NoError(t, err)
			matchedID := created.Rows[0][0].(*storage.Node).ID
			// A reader that looks up the written quoted bytes selects this wrong node.
			_, err = exec.Execute(context.Background(), "CREATE (c {"+cyphertext.QuoteName(quoted)+":2})", nil)
			require.NoError(t, err)
			ctx := newActionCtx(map[string]any{"query": "MATCH"})
			ctx.Database = &autocompleteNameDB{exec: exec, name: name}
			result, err := (&WatcherPlugin{}).actionAutocompleteSuggest(ctx)
			require.NoError(t, err)
			data := result.Data
			suggestions := data["suggestions"].([]string)
			require.Len(t, suggestions, 3)
			for index, query := range suggestions {
				if index == 1 {
					emittercheck.Check(t, query, name, nil, nil)
				}
				require.Contains(t, query, quoted)
				rows, err := exec.Execute(context.Background(), query, nil)
				require.NoError(t, err, query)
				require.Len(t, rows.Rows, 1, query)
				if index == 1 {
					require.Equal(t, name, rows.Rows[0][0].(*storage.Edge).Type)
				}
				if index == 2 {
					node := rows.Rows[0][0].(*storage.Node)
					require.Equal(t, matchedID, node.ID, query)
					require.Equal(t, int64(1), node.Properties[name], query)
				}
			}
		})
	}
}
