package fn

import (
	"strings"
	"testing"
	"time"
)

func BenchmarkIdentityFunctionPublicLookup(b *testing.B) {
	names := []string{"identity_case.region", "identity_case.ärea"}
	mu.Lock()
	previous := map[string]registeredFunction{}
	present := map[string]bool{}
	for _, name := range names {
		key := strings.ToLower(name)
		previous[key], present[key] = registry[key]
	}
	mu.Unlock()
	b.Cleanup(func() {
		mu.Lock()
		defer mu.Unlock()
		for key, value := range previous {
			if present[key] {
				registry[key] = value
			} else {
				delete(registry, key)
			}
		}
	})
	for _, name := range names {
		Register(name, func(Context, []string) (interface{}, error) { return int64(7), nil })
	}
	ctx := Context{Eval: func(string) (interface{}, error) { return nil, nil }, Now: time.Now}
	for _, name := range []string{"identity_case.region", "IDENTITY_CASE.REGION", "identity_case.ärea", "IDENTITY_CASE.ÄREA"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				value, found, err := EvaluateFunction(name, nil, ctx)
				if err != nil || !found || value != int64(7) {
					b.Fatalf("lookup failed: %v", err)
				}
			}
		})
	}
}
