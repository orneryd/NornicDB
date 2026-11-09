package replication

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// replayStateEntries counts every entry in every map the replay cache
// holds, so a test sees all the state it keeps, whatever its layout.
func replayStateEntries(c *replayCache) int {
	v := reflect.ValueOf(c).Elem()
	n := 0
	for i := 0; i < v.NumField(); i++ {
		if v.Field(i).Kind() == reflect.Map {
			n += v.Field(i).Len()
		}
	}
	return n
}

// All replay state stays within the cache limit, whatever sender IDs arrive
// and including admissions the full cache refuses; and an entry's size
// doesn't grow with the sender ID.
func TestReplayCache_AllStateBoundedAcrossSenders(t *testing.T) {
	cache := newReplayCache()
	cache.limit = 2
	now := time.Now().UnixNano()
	for i := 0; i < 20; i++ {
		_ = cache.admit(fmt.Sprintf("sender-%d-%s", i, strings.Repeat("x", 1024)), newMessageNonce(), now+int64(time.Minute), now)
	}
	require.LessOrEqual(t, replayStateEntries(cache), cache.limit)

	v := reflect.ValueOf(cache).Elem()
	for i := 0; i < v.NumField(); i++ {
		if f := v.Field(i); f.Kind() == reflect.Map {
			require.False(t, containsString(f.Type().Key()), "field %s keys hold a sender ID", v.Type().Field(i).Name)
		}
	}
}

func containsString(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String:
		return true
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if containsString(t.Field(i).Type) {
				return true
			}
		}
	case reflect.Array:
		return containsString(t.Elem())
	}
	return false
}
