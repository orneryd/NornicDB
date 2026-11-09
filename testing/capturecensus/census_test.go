package capturecensus

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func fixture(t *testing.T, body string) (workcensus.Config, Policy) {
	t.Helper()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module capture.fixture\n\ngo 1.26\n"), 0600))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "owner.go"), []byte(body), 0600))
	return workcensus.Config{Dir: dir, Patterns: []string{"./..."}}, Policy{Package: "capture.fixture", Native: "Engine", Entities: []string{"Entity"}, Capture: []string{"(*capture.fixture.Engine[T]).capture"}}
}

func TestCompiledCaptureReviewedDistinctions(t *testing.T) {
	c, p := fixture(t, `package fixture
type Entity struct{}
type Engine[T any] struct{}
func (e *Engine[T]) Read()*Entity{return &Entity{}}
var _ = (*Engine[int]).Read
`)
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.Len(t, i.Reads, 1)
	read := i.Reads[0]
	id := ""
	for _, d := range i.SourceDeclarations {
		if d.Symbol == read.Symbol {
			id = d.ID
		}
	}
	require.NotEmpty(t, id)
	p.Distinct = map[string]Disposition{read.Symbol: {ID: id, Digest: read.Digest, Reason: "pretends to be metadata"}}
	i, err = Extract(c, p)
	require.NoError(t, err)
	require.Len(t, i.Violations, 1)
	require.Contains(t, i.Violations[0], "entity delivery cannot be a distinct responsibility")
}

// Named/blank/unnamed, pointer/value, generic and aliased receivers are ordinary
// compiled declarations. Literals and method values retain their actual owner.
func TestCompiledCaptureDeclarationPopulation(t *testing.T) {
	c, p := fixture(t, `package fixture
type Entity struct{}
type Engine[T any] struct{}
type Alias = Engine[int]
func (e *Engine[T]) capture() *Entity { return &Entity{} }
func (e *Engine[T]) Named() *Entity { return e.capture() }
func (_ *Engine[T]) Blank() uint64 { return 0 }
func (*Engine[T]) Unnamed() uint64 { return 0 }
func (e Engine[T]) Value() *Entity { return e.capture() }
func Arbitrary(e *Alias) *Entity { f:=e.Named; return f() }
func Literal(e *Alias) *Entity { f:=func()*Entity{return e.Named()}; return f() }
func Factory(e *Alias) func()*Entity {return e.Named}
func Channel(e *Alias) <-chan *Entity {c:=make(chan *Entity,1);c<-e.Named();return c}
func Double(e *Alias) []*Entity {return []*Entity{e.Named(),e.Named()}}
func (e *Engine[T]) Visit(fn func(*Entity)) { fn(e.Named()) }
type Other struct{}
func (*Other) Named() *Entity { return &Entity{} }
var _ = func(e *Alias)*Entity {return e.Named()}
var _ = func(e *Alias)*Entity {return e.Named()}
var _ = Engine[int].Value
var _ = (*Engine[int]).Visit
`)
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.Empty(t, i.Violations)
	bindings := map[string]bool{}
	for _, d := range i.Declarations {
		if d.Native {
			bindings[d.Binding] = true
		}
	}
	require.True(t, bindings[""])
	require.True(t, bindings["_"])
	require.True(t, bindings["e"])
	require.Len(t, i.Reads, 8)
	columns := map[int]bool{}
	for _, c := range i.Consumers {
		if c.Caller == "capture.fixture.Double" && strings.HasSuffix(c.Callee, ".Named") {
			require.Positive(t, c.Column)
			columns[c.Column] = true
		}
	}
	require.Len(t, columns, 2, "same-line consumer calls must remain distinguishable")

	// The new file is in a different package and has an arbitrary name. The
	// same real extraction must discover a new native consumer with no owner.
	require.NoError(t, os.Mkdir(filepath.Join(c.Dir, "outside"), 0700))
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "outside", "new.go"), []byte(`package outside
import f "capture.fixture"
func Unfamiliar(e *f.Alias)*f.Entity{return &f.Entity{}}
`), 0600))
	negative, err := Extract(c, p)
	require.NoError(t, err)
	require.Len(t, negative.Reads, len(i.Reads)+1)
	require.Len(t, negative.Violations, 1)
	require.Contains(t, negative.Violations[0], "outside.Unfamiliar")
}

func TestCompiledCaptureRemovalAndBorrowingControls(t *testing.T) {
	body := `package fixture
import "context"
type Entity struct{}
type Engine[T any] struct{}
type borrowed struct{cached func()}
func (e *Engine[T]) capture() *Entity { return &Entity{} }
func (e *Engine[T]) Read() *Entity { f:=func()*Entity{return e.capture()}; return f() }
var _ = (*Engine[int]).Read
var _ context.Context
`
	c, p := fixture(t, body)
	p.BorrowedOptions = []string{"borrowed"}
	i, err := Extract(c, p)
	require.NoError(t, err)
	require.Empty(t, i.Violations)
	// Remove the observation of the owner, retaining the entity delivery.
	removed := strings.Replace(body, "return e.capture()", "return &Entity{}", 1)
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "owner.go"), []byte(removed), 0600))
	i, err = Extract(c, p)
	require.NoError(t, err)
	require.Len(t, i.Reads, 1)
	require.Len(t, i.Violations, 1)
	require.Contains(t, i.Violations[0], "no captured live entity read owner")
	// Reintroduce the owner but couple a request interface to borrowed callbacks.
	coupled := strings.Replace(body, "cached func()", "ctx context.Context; cached func()", 1)
	require.NoError(t, os.WriteFile(filepath.Join(c.Dir, "owner.go"), []byte(coupled), 0600))
	i, err = Extract(c, p)
	require.NoError(t, err)
	require.Len(t, i.Violations, 1)
	require.Contains(t, i.Violations[0], "request context shares borrowed callback options")
}
