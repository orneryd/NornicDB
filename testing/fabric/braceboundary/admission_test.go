package braceboundary_test

import (
	"context"
	"github.com/orneryd/nornicdb/pkg/fabric"
	"strings"
	"testing"
)

// Author: #1663 implementation lane. Standalone public planning has no Cypher
// callback and must consume the same final contextual admission record.
func TestStandaloneOriginalBraceAdmission(t *testing.T) {
	for _, item := range []struct{ name, query, want string }{
		{"pattern_keyword", "MATCH (CALL { RETURN 1 }) RETURN CALL", "pattern property maps require key-value entries"},
		{"nested_body", "CALL { RETURN {broken} AS x } RETURN x", "map literal keys must be valid symbolic names"},
		{"pattern_map", "MATCH (n {x: 1}) RETURN n", ""},
		{"literal", "RETURN {x: 1} AS x", ""},
	} {
		t.Run(item.name, func(t *testing.T) {
			planner := fabric.NewFabricPlanner(nil)
			_, planErr := planner.Plan(context.Background(), item.query, "nornic", fabric.RawSource)
			_, sourceErr := planner.SingleGraphStatement(context.Background(), item.query, fabric.RawSource)
			for name, err := range map[string]error{"Plan": planErr, "SingleGraphStatement": sourceErr} {
				if item.want == "" {
					if err != nil {
						t.Fatalf("%s rejected control: %v", name, err)
					}
					continue
				}
				if err == nil || !strings.Contains(err.Error(), item.want) {
					t.Fatalf("%s: want %q, got %v", name, item.want, err)
				}
			}
		})
	}
}
