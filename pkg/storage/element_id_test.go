package storage

import "testing"

func TestElementIdentifiersIncludeDatabaseAndEntityKind(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{name: "node in tenant database", got: NodeElementID("tenant_a", "node-1"), want: "4:tenant_a:node-1"},
		{name: "relationship in tenant database", got: RelationshipElementID("tenant_a", "edge-1"), want: "5:tenant_a:edge-1"},
		{name: "default database", got: NodeElementID("", "node-1"), want: "4:nornic:node-1"},
		{name: "existing node identifier", got: NodeElementID("tenant_b", "4:tenant_a:node-1"), want: "4:tenant_a:node-1"},
		{name: "existing relationship identifier", got: RelationshipElementID("tenant_b", "5:tenant_a:edge-1"), want: "5:tenant_a:edge-1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.got != test.want {
				t.Fatalf("got %q, want %q", test.got, test.want)
			}
		})
	}
}
