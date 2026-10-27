package antlr

import (
	"runtime"
	"strings"
	"testing"
	"unsafe"
	"weak"

	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/stretchr/testify/require"
)

func TestRetiredSyntaxErrorsCollectible(t *testing.T) {
	owner := &parseErrorListener{}
	reference := func() weak.Pointer[byte] {
		message := strings.Repeat("retired", 64)
		owner.errors = []localization.Message{{Fallback: message}}
		ref := weak.Make(unsafe.StringData(message))
		owner.reset()
		return ref
	}()
	runtime.GC()
	require.Nil(t, reference.Value())
	runtime.KeepAlive(owner)
}
