package apoc

import (
	"reflect"
	"sort"
	"testing"

	"github.com/orneryd/nornicdb/apoc/storage"
)

// Mechanical inventory: declarations, registered handlers and plugin exports
// must all be inspected; the map namespace alone is not the family boundary.
func TestAPOCMapReturnInventory(t *testing.T) {
	if err := Initialize(storage.NewInMemoryStorage(), nil); err != nil {
		t.Fatal(err)
	}
	exported := map[string]bool{}
	for _, name := range PluginFunctionNames() {
		exported[name] = true
	}
	names := List()
	sort.Strings(names)
	for _, name := range names {
		info, _ := GetInfo(name)
		typ := reflect.TypeOf(info.Handler)
		if typ.Kind() == reflect.Func && typ.NumOut() > 0 && typ.Out(0).Kind() == reflect.Map {
			t.Logf("MAP INVENTORY %s builtin=%v plugin=%v procedure=%v handler=%s", name, IsBuiltInFunction(name), exported[name], isProcedureName(name), typ)
		}
	}
}
