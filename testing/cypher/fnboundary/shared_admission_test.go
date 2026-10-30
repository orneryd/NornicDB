package fnboundary_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"github.com/orneryd/nornicdb/pkg/storage"
	"strings"
	"testing"
)

// Author: #1663 implementation lane. These public entrances must consume the
// same contextual role and admission record, including standalone Fabric.
func TestSharedOriginalBraceAdmission(t *testing.T) {
	for _, item := range []struct{ name, query, want string }{
		{"pattern_keyword", "MATCH (CALL { RETURN 1 }) RETURN CALL", "pattern property maps require key-value entries"},
		{"nested_body", "CALL { RETURN {broken} AS x } RETURN x", "map literal keys must be valid symbolic names"},
		{"pattern_map", "MATCH (n {x: 1}) RETURN n", ""},
		{"literal", "RETURN {x: 1} AS x", ""},
	} {
		t.Run(item.name, func(t *testing.T) {
			executor := cypher.NewStorageExecutor(storage.NewMemoryEngine())
			_, executionErr := executor.Execute(context.Background(), item.query, nil)
			_, planningErr := fabric.NewFabricPlanner(nil).Plan(context.Background(), item.query, "nornic", fabric.RawSource)
			for name, err := range map[string]error{"cypher": executionErr, "fabric": planningErr} {
				if item.want == "" {
					if err != nil {
						t.Fatalf("%s rejected control: %v", name, err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), item.want) {
					t.Fatalf("%s admission: want %q, got %v", name, item.want, err)
				}
			}
		})
	}
}
