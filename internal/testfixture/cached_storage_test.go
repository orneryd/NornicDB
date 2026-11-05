package testfixture

import (
	"errors"
	"testing"
)

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

// cachedStorageFatal records the assertion protocol without terminating a real
// test goroutine. Only its private sentinel is recovered by the contract test.
type cachedStorageFatal struct {
	testing.TB
	failed bool
}
type cachedStorageFatalSignal struct{}

func (*cachedStorageFatal) Helper() {}
func (f *cachedStorageFatal) Fatalf(string, ...any) {
	f.failed = true
	panic(cachedStorageFatalSignal{})
}

func TestCachedStorageRejectsInvalidLookups(t *testing.T) {
	first, second := new(int), new(int)
	rejected := errors.New("synthetic storage lookup rejection")
	for _, tc := range []struct {
		name   string
		values [2]*int
		errors [2]error
		calls  int
	}{
		{name: "first lookup error", errors: [2]error{rejected, nil}, calls: 1},
		{name: "zero first result", calls: 1},
		{name: "second lookup error", values: [2]*int{first, nil}, errors: [2]error{nil, rejected}, calls: 2},
		{name: "changed cached identity", values: [2]*int{first, second}, calls: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			recorder := &cachedStorageFatal{}
			calls := 0
			func() {
				defer func() {
					if signal := recover(); signal != nil {
						if _, expected := signal.(cachedStorageFatalSignal); !expected {
							panic(signal)
						}
					}
				}()
				CachedStorage(recorder, func() (*int, error) {
					index := calls
					calls++
					return tc.values[index], tc.errors[index]
				})
			}()
			if !recorder.failed {
				t.Error("invalid cache lookup did not fail the assertion")
			}
			if calls != tc.calls {
				t.Errorf("performed %d lookups, want %d before fatal stop", calls, tc.calls)
			}
		})
	}
}
