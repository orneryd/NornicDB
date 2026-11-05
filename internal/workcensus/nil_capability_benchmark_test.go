package workcensus

import (
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func BenchmarkInitializerCapabilityActual(b *testing.B) {
	if os.Getenv("WORKCENSUS_CAPABILITY_ACTUAL") != "1" {
		b.Skip("opt-in full actual prefix workloads")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		b.Fatal(err)
	}
	p, err := LoadProgram(Config{Dir: root, Patterns: []string{"./pkg/cypher", "./pkg/bolt"}})
	if err != nil {
		b.Fatal(err)
	}
	for _, name := range []string{"BeginUse", "beginServe"} {
		var fn *ssa.Function
		for _, candidate := range p.Functions {
			if candidate.Pkg == nil {
				continue
			}
			path := candidate.Pkg.Pkg.Path()
			if candidate.Name() == name && (path == "github.com/orneryd/nornicdb/pkg/cypher" || path == "github.com/orneryd/nornicdb/pkg/bolt") {
				fn = candidate
				break
			}
		}
		if fn == nil {
			b.Fatal("actual root absent")
		}
		field := "ErrQueryEmbedderClosed"
		if name == "beginServe" {
			field = "errServerClosed"
		}
		f := newNilFlow(p)
		fact := f.globalInitializer(fn.Pkg.Members[field].(*ssa.Global))
		if fact == nil || !fact.nonNil {
			b.Fatal("actual source fact absent")
		}
		b.Run(name, func(b *testing.B) {
			b.Run("cold", func(b *testing.B) {
				b.ReportAllocs()
				for i := 0; i < b.N; i++ {
					f.initializationOrder = nil
					f.initializerPrecedesFunction(fact, fn)
				}
			})
			b.Run("cached", func(b *testing.B) {
				f.initializerPrecedesFunction(fact, fn)
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					f.initializerPrecedesFunction(fact, fn)
				}
			})
		})
	}
}
