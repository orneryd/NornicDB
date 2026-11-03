package workcensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/tools/go/ssa"
)

func TestNilFlowGlobalRefusalInitializationIsCallsiteLocal(t *testing.T) {
	for _, order := range [][]string{{"Imported", "OwnEarly", "Unknown"}, {"OwnEarly", "Unknown", "Imported"}} {
		t.Run(strings.Join(order, "_then_"), func(t *testing.T) {
			dir := t.TempDir()
			files := map[string]string{
				"go.mod": "module census.callphase\n\ngo 1.24\n",
				"dep/dep.go": `package dep
import "context"
func sink(ctx context.Context){_=ctx.Err()}
func Unknown(fn func(bool)(context.Context,error)){ctx,err:=fn(true);if err!=nil{return};sink(ctx)}
`,
				"owner/owner.go": `package owner
import("context";"errors";"census.callphase/dep")
func sink(ctx context.Context){_=ctx.Err()}
type starter interface{Start()int}
type early struct{}
var boot starter=early{}
var Early=boot.Start()
func(early)Start()int{OwnEarly(true);return 1}
var Closed=errors.New("closed")
func Produce(fail bool)(context.Context,error){if fail{return nil,Closed};return context.Background(),nil}
func OwnEarly(fail bool){ctx,err:=Produce(fail);if err!=nil{return};sink(ctx)}
func Deliver(){dep.Unknown(Produce)}
`,
				"consumer/consumer.go": `package consumer
import("context";"census.callphase/owner")
func sink(ctx context.Context){_=ctx.Err()}
func Imported(fail bool){ctx,err:=owner.Produce(fail);if err!=nil{return};sink(ctx)}
`,
			}
			for name, body := range files {
				path := filepath.Join(dir, name)
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			c := genericConfig(t)
			c.Dir = dir
			c.Patterns = []string{"./..."}
			c.Roots = []Root{{Package: "census.callphase/consumer", Name: "Imported", Required: true}}
			c.IncludeProgram = true
			inv := mustExtract(t, c)
			for _, name := range order {
				found := false
				for _, fn := range inv.Program.Functions {
					if fn.Name() != name {
						continue
					}
					for _, b := range fn.Blocks {
						for _, ins := range b.Instrs {
							call, ok := ins.(*ssa.Call)
							if !ok || call.Common().StaticCallee() == nil || call.Common().StaticCallee().Name() != "sink" {
								continue
							}
							found = true
							witness, why := inv.Program.MayBeNilWhy(call.Common().Args[0], call)
							wantNil := name != "Imported"
							if (witness != nil) != wantNil {
								t.Errorf("%s nil=%v want%v witness=%v why=%v", name, witness != nil, wantNil, witness, why)
							}
							if name == "Imported" && !strings.Contains(strings.Join(why, " | "), "exact checked callsite") {
								t.Errorf("importer lacked callsite source proof: %v", why)
							}
						}
					}
				}
				if !found {
					t.Fatalf("missing actual %s sink", name)
				}
			}
		})
	}
}
