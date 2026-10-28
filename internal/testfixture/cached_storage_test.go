package testfixture

import "testing"

func TestCachedStorageReturnsStableIdentity(t *testing.T) {
	instance := &struct{ name string }{name: "synthetic-cached-store"}
	calls := 0
	got := CachedStorage(t, func() (*struct{ name string }, error) {
		calls++
		return instance, nil
	})
	if got != instance {
		t.Fatal("helper did not return the cached instance")
	}
	if calls != 2 {
		t.Fatalf("helper performed %d lookups, want 2", calls)
	}
}

func TestCachedStorageValueTypeReturnsFirstLookup(t *testing.T) {
	calls := 0
	got := CachedStorage(t, func() (int, error) {
		calls++
		return 7, nil
	})
	if got != 7 {
		t.Fatalf("helper returned %d, want the first lookup result", got)
	}
	if calls != 2 {
		t.Fatalf("helper performed %d lookups, want 2", calls)
	}
}
