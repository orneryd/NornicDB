package server
import "github.com/orneryd/nornicdb/pkg/cypher"


import (
	"context"
	"reflect"
	"testing"

	"github.com/orneryd/nornicdb/pkg/multidb"
)

func TestStatementCacheFamilyHTTPExecutorCacheFamily(t *testing.T) {
	server, _ := setupTestServer(t)
	if err := server.dbManager.CreateDatabase("review1477local"); err != nil {
		t.Fatal(err)
	}
	if err := server.dbManager.CreateCompositeDatabase("review1477comp", []multidb.ConstituentRef{
		{Alias: "local1", DatabaseName: "review1477local", Type: "local", AccessMode: "read_write"},
		{Alias: "r1", DatabaseName: "remote_db", Type: "remote", AccessMode: "read", URI: "http://remote.example"},
	}); err != nil {
		t.Fatal(err)
	}
	first, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("review1477comp", "Bearer remote-token"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Execute(context.Background(), "CALL { USE review1477comp.local1 RETURN 1 AS x } RETURN x", nil); err != nil {
		t.Fatal(err)
	}
	second, err := cypher.NewDatabaseScopedExecutor(server.databaseScopedExecutorOptions("review1477comp", "Bearer remote-token"))
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("authenticated requests shared an execution-state object")
	}
	for _, field := range []string{"syntaxValidationCache", "upperQueryCache"} {
		left := reflect.ValueOf(first).Elem().FieldByName(field).Pointer()
		right := reflect.ValueOf(second).Elem().FieldByName(field).Pointer()
		if left == 0 || left != right {
			t.Errorf("%s warmed in first authenticated HTTP request lost by second: %x != %x", field, left, right)
		}
	}
}
