package auth

import (
	"fmt"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"reflect"
	"testing"
	"time"
)

var populatedUserReadSink *User

// populatedUserReadFixture is the single populated stored-user record shared by
// BenchmarkUserRecordPopulatedRead and TestUserRecordPopulatedReadAllocBudget.
// It carries every original field with its canonical stored type, both
// timestamps, two roles and two metadata entries, through the historical
// constructor only: no time is written into the properties, so the benchmark
// and the budget test read the same allocation shape.
func populatedUserReadFixture() *storage.Node {
	return &storage.Node{
		ID:     storage.NodeID("user:alice"),
		Labels: []string{"_User", "_System"},
		Properties: map[string]any{
			"id":            "user-alice",
			"username":      "alice",
			"email":         "alice@example.com",
			"password_hash": "$2a$10$fixturefixturefixturefixturefi",
			"roles":         `["admin","viewer"]`,
			"created_at":    int64(1700000000),
			"updated_at":    int64(1700000900),
			"last_login":    int64(1700001800),
			"failed_logins": int64(2),
			"locked_until":  int64(0),
			"disabled":      true,
			"metadata":      `{"one":"two","three":"four"}`,
		},
		CreatedAt: time.Unix(1600000000, 0).UTC(),
		UpdatedAt: time.Unix(1600000100, 0).UTC(),
	}
}

func BenchmarkUserRecordPopulatedRead(b *testing.B) {
	node := populatedUserReadFixture()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		populatedUserReadSink = mapUserRecord(loadUserRecord, nil, node).User
	}
}

func TestUserRecordReadRetainsDetachedDecodedContainers(t *testing.T) {
	const roles = `["admin","viewer"]`
	const metadata = `{"one":"two","three":"four"}`
	node := &storage.Node{Properties: map[string]any{"roles": roles, "metadata": metadata}}
	first := mapUserRecord(loadUserRecord, nil, node).User
	for i := 0; i < 256; i++ {
		other := mapUserRecord(loadUserRecord, nil, &storage.Node{Properties: map[string]any{"roles": `["viewer"]`, "metadata": `{"other":"value"}`}}).User
		if len(other.Roles) != 1 || other.Metadata["other"] != "value" {
			t.Fatal("intervening decode lost fields")
		}
	}
	if len(first.Roles) != 2 || first.Roles[0] != RoleAdmin || first.Roles[1] != RoleViewer || first.Metadata["one"] != "two" || first.Metadata["three"] != "four" {
		t.Fatalf("retained decode changed: %#v", first)
	}
	first.Roles[0] = RoleViewer
	first.Metadata["one"] = "edited"
	second := mapUserRecord(loadUserRecord, nil, node).User
	if second.Roles[0] != RoleAdmin || second.Metadata["one"] != "two" || node.Properties["roles"] != roles || node.Properties["metadata"] != metadata {
		t.Fatal("decoded containers alias the source or another read")
	}
}

func BenchmarkUserRecordRoleShapes(b *testing.B) {
	for _, count := range []int{0, 1, 2, 3, 8, 256} {
		roles := make([]Role, count)
		for i := range roles {
			roles[i] = Role(fmt.Sprintf("role-%d", i))
		}
		encoded, err := jsontext.Marshal(roles)
		if err != nil {
			b.Fatal(err)
		}
		for _, metadata := range []bool{false, true} {
			b.Run(fmt.Sprintf("roles=%d/metadata=%t", count, metadata), func(b *testing.B) {
				node := populatedUserReadFixture()
				node.Properties["roles"] = string(encoded)
				if !metadata {
					delete(node.Properties, "metadata")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					populatedUserReadSink = mapUserRecord(loadUserRecord, nil, node).User
				}
			})
		}
	}
}

func BenchmarkUserRecordRoleEmptyRepresentations(b *testing.B) {
	cases := []struct {
		name    string
		value   any
		missing bool
	}{{"missing", nil, true}, {"nonstring", false, false}, {"null", "null", false}, {"whitespace-null", " null ", false}, {"empty", "[]", false}, {"whitespace-empty", " [ ] ", false}}
	for _, tc := range cases {
		for _, metadata := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/metadata=%t", tc.name, metadata), func(b *testing.B) {
				node := populatedUserReadFixture()
				if tc.missing {
					delete(node.Properties, "roles")
				} else {
					node.Properties["roles"] = tc.value
				}
				if !metadata {
					delete(node.Properties, "metadata")
				}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					populatedUserReadSink = mapUserRecord(loadUserRecord, nil, node).User
				}
			})
		}
	}
}

func TestUserRecordRoleCapacityAndReset(t *testing.T) {
	for _, count := range []int{0, 1, 2, 3, 8, 256} {
		roles := make([]Role, count)
		for i := range roles {
			roles[i] = Role(fmt.Sprintf("role-%d", i))
		}
		encoded, err := jsontext.Marshal(roles)
		if err != nil {
			t.Fatal(err)
		}
		node := populatedUserReadFixture()
		node.Properties["roles"] = string(encoded)
		got := mapUserRecord(loadUserRecord, nil, node).User
		if !reflect.DeepEqual(roles, got.Roles) {
			t.Fatalf("count %d roles=%#v", count, got.Roles)
		}
		again := mapUserRecord(loadUserRecord, nil, node).User
		if count > 0 {
			got.Roles[0] = "edited"
			if again.Roles[0] != roles[0] {
				t.Fatal("independent reads share role backing")
			}
		}
	}
	for _, encoded := range []any{nil, false, "null", " null ", `["admin",4]`, `["admin",`} {
		node := populatedUserReadFixture()
		node.Properties["roles"] = encoded
		if got := mapUserRecord(loadUserRecord, nil, node).User; got.Roles != nil {
			t.Fatalf("rejected/null roles %q=%#v", encoded, got.Roles)
		}
	}
	for _, encoded := range []string{"[]", " [ ] "} {
		node := populatedUserReadFixture()
		node.Properties["roles"] = encoded
		if got := mapUserRecord(loadUserRecord, nil, node).User; got.Roles == nil || len(got.Roles) != 0 {
			t.Fatalf("empty roles %q=%#v", encoded, got.Roles)
		}
	}
}
