package auth

import (
	"reflect"
	"testing"
	"time"
)

// TestUserRecordPopulatedReadAllocBudget preserves the complete stored-user
// result and the measured legacy allocation budget for two roles/two metadata
// entries. The result escapes, and only the actual read is inside measurement.
func TestUserRecordPopulatedReadAllocBudget(t *testing.T) {
	const budget = 7
	node := populatedUserReadFixture()
	want := &User{
		ID:           "user-alice",
		Username:     "alice",
		Email:        "alice@example.com",
		PasswordHash: "$2a$10$fixturefixturefixturefixturefi",
		Roles:        []Role{RoleAdmin, RoleViewer},
		CreatedAt:    time.Unix(1700000000, 0),
		UpdatedAt:    time.Unix(1700000900, 0),
		LastLogin:    time.Unix(1700001800, 0),
		FailedLogins: 2,
		LockedUntil:  time.Time{},
		Disabled:     true,
		Metadata:     map[string]string{"one": "two", "three": "four"},
	}
	assertRecord := func(got *User) {
		t.Helper()
		if !reflect.DeepEqual(want, got) {
			t.Fatalf("stored user=%#v want %#v", got, want)
		}
	}
	assertRecord(mapUserRecord(loadUserRecord, nil, node).User)
	var sink *User
	allocs := testing.AllocsPerRun(200, func() { sink = mapUserRecord(loadUserRecord, nil, node).User })
	assertRecord(sink)
	if allocs > budget {
		t.Fatalf("populated user-record READ allocations=%v want <= %d (legacy 7, regressed 44)", allocs, budget)
	}
}
