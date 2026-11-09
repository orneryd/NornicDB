package auth

import (
	"context"
	"testing"
)

func TestBoundReadUsesCanonicalLiveReadOnly(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		t.Run(map[bool]string{false: "complete", true: "legacy"}[legacy], func(t *testing.T) {
			readable := true
			calls := 0
			ctx := WithRequestDatabaseScope(context.Background(), NewRequestDatabaseScope("named", map[string]string{"alias": "named", "named": "named"}))
			check := func(database string) bool {
				calls++
				if database != "named" {
					t.Errorf("database=%s", database)
				}
				return readable
			}
			if legacy {
				ctx = WithRequestResolvedAccessResolver(ctx, func(database string) ResolvedAccess { return ResolvedAccess{Read: check(database)} })
			} else {
				ctx = WithRequestEntitlementResolver(ctx, func(database string, permission Permission) bool {
					if permission != PermRead {
						t.Errorf("unused permission=%s", permission)
					}
					return check(database)
				})
			}
			database, allows := BindRequestDatabaseRead(ctx, "alias")
			if database != "named" || calls != 0 {
				t.Fatalf("binding evaluated policy or lost canonical name: %s %d", database, calls)
			}
			for _, want := range []bool{true, false, true} {
				readable = want
				if allows() != want {
					t.Fatalf("stale decision: want %v", want)
				}
			}
			if calls != 3 {
				t.Fatalf("calls=%d", calls)
			}
			_, denied := BindRequestDatabaseRead(ctx, "outside-scope")
			if denied() || calls != 3 {
				t.Fatal("refused scope queried policy")
			}
		})
	}
}
