package bolt

import (
	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"

	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/storage"
)

func TestStatementCacheFamilyBoltTransactionCacheFamily(t *testing.T) {
	for _, auth := range []string{"", "Bearer forwarded-token"} {
		t.Run(auth, func(t *testing.T) {
			store := runtimeFixtureOwner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
			t.Cleanup(func() { _ = store.Close() })
			manager := &mockDBManager{stores: map[string]storage.Engine{"nornic": store}, defaultDB: "nornic"}
			session := &Session{server: &Server{config: DefaultConfig(), dbManager: manager, executors: make(map[string]QueryExecutor)}, forwardedAuthHeader: auth}
			first, err := session.getTransactionalExecutorForDatabase("nornic")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := first.Execute(context.Background(), "MATCH (n:StatementCacheFamily) RETURN count(n) AS c", nil); err != nil {
				t.Fatal(err)
			}
			second, err := session.getTransactionalExecutorForDatabase("nornic")
			if err != nil {
				t.Fatal(err)
			}
			a := first.(*transactionalBoltQueryExecutorAdapter).executor
			b := second.(*transactionalBoltQueryExecutorAdapter).executor
			if a == b {
				t.Fatal("transactions shared an execution-state object")
			}
			if auth != "" && len(session.server.executors) != 0 {
				t.Fatal("bearer-authenticated execution state entered the shared executor cache")
			}
			for _, field := range []string{"syntaxValidationCache", "upperQueryCache"} {
				left := reflect.ValueOf(a).Elem().FieldByName(field).Pointer()
				right := reflect.ValueOf(b).Elem().FieldByName(field).Pointer()
				if left == 0 || left != right {
					t.Errorf("%s warmed in first transaction lost by second: %x != %x", field, left, right)
				}
			}
		})
	}
}
