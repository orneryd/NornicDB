package workcensus

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestNilFlowCapabilityActualMutations(t *testing.T) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		t.Skip("opt-in actual-source mutants")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name, file, from, to, append, marker string
		positive                             bool
	}{
		{name: "value method revokes actual schema", file: "function_catalog.go", append: `
func(cypherFunctionSpec) capabilityProbe(){}
`},
		{name: "pointer method revokes actual schema", file: "function_catalog.go", append: `
func(*cypherFunctionSpec) capabilityProbe(){}
`},
		{name: "actual callable map early length", file: "functions_neo5.go", from: `var singleValueFunctions =`, to: `var capabilityEarly=len(singleValueFunctions)
var singleValueFunctions =`, marker: "capabilityEarly"},
		{name: "actual sealed hidden construction remains sealed", file: "functions_neo5.go", from: `"abs":     absValue,`, to: `"abs":     capabilityHiddenValue,`, append: `
type capabilityBridge struct{}
func(capabilityBridge)Read(){(&QueryEmbedderBinding{}).BeginUse(context.Background())}
func capabilityHiddenValue(value interface{})(interface{},error){
	var bridge interface{Read()}=capabilityBridge{}
	bridge.Read()
	return value,nil
}
`, positive: true},
		{name: "actual hidden construction callable early invoke", file: "functions_neo5.go", from: `"abs":     absValue,`, to: `"abs":     capabilityHiddenValue,`, append: `
type capabilityBridge struct{}
func(capabilityBridge)Read(){(&QueryEmbedderBinding{}).BeginUse(context.Background())}
func capabilityHiddenValue(value interface{})(interface{},error){
	var bridge interface{Read()}=capabilityBridge{}
	bridge.Read()
	return value,nil
}
var capabilityEarly,capabilityEarlyErr=singleValueFunctions["abs"](nil)
`, marker: "capabilityEarly"},
		{name: "actual constructor aggregate gains consumer", file: "bounded_cache.go", from: `for i := range c.shards {`, to: `capabilityConsume(c.shards)
	for i := range c.shards {`, append: `
func capabilityConsume[T any](T){}
`},
		{name: "actual constructor discarded hash is not aggregate", file: "bounded_cache.go", from: `shardLimit := limit / boundedCacheShardCount`, to: `_ = c.hash
	shardLimit := limit / boundedCacheShardCount`},
		{name: "actual foreign interface owned override", file: "plugin_typed_invocation.go", from: `reflect.TypeOf((*context.Context)(nil)).Elem()`, to: `capabilityForeignType().Elem()`, append: `
type capabilityTypeWrapper struct{reflect.Type}
func(capabilityTypeWrapper)Elem()reflect.Type{
	(&QueryEmbedderBinding{}).BeginUse(context.Background())
	return nil
}
func capabilityForeignType()reflect.Type{return capabilityTypeWrapper{}}
`, marker: "pluginContextType"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(root, "pkg", "cypher", tc.file)
			original, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			raw := string(original)
			if tc.from != "" {
				if strings.Count(raw, tc.from) != 1 {
					t.Fatal("actual mutation anchor changed")
				}
				raw = strings.Replace(raw, tc.from, tc.to, 1)
			}
			raw += tc.append
			p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/cypher"}, Overlay: map[string][]byte{path: []byte(raw)}})
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := p.LoadedSource(path)
			if err != nil || !bytes.Equal(parsed, []byte(raw)) {
				t.Fatal("mutation not exact parser input")
			}
			var g *ssa.Global
			var reader *ssa.Function
			for _, fn := range p.Functions {
				if fn.Pkg == nil || fn.Pkg.Pkg.Path() != "github.com/orneryd/nornicdb/pkg/cypher" {
					continue
				}
				if fn.Name() == "BeginUse" {
					reader = fn
					g = fn.Pkg.Members["ErrQueryEmbedderClosed"].(*ssa.Global)
					break
				}
			}
			if g == nil || reader == nil {
				t.Fatal("actual reader/sentinel absent")
			}
			f := newNilFlow(p)
			fact := f.globalInitializer(g)
			if fact == nil || !fact.nonNil {
				t.Fatal("mutant lost G0 source fact")
			}
			if tc.marker != "" {
				var marker *ssa.Store
				for _, b := range fact.owner.Func("init").Blocks {
					for _, ins := range b.Instrs {
						if st, ok := ins.(*ssa.Store); ok {
							if global, ok := st.Addr.(*ssa.Global); ok && global.Name() == tc.marker {
								marker = st
							}
						}
					}
				}
				if marker == nil || !marker.Block().Dominates(fact.store.Block()) || (marker.Block() == fact.store.Block() && instructionIndex(marker) >= instructionIndex(fact.store)) {
					t.Fatal("actual mutation not in sentinel prefix")
				}
			}
			if got := f.initializerPrecedesFunction(fact, reader); got != tc.positive {
				t.Fatalf("actual mutant proved=%v want%v", got, tc.positive)
			}
		})
	}
}
