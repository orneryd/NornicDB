package testfixture

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
)

// OpenStores counts Badger stores, including allocations which a failed
// constructor never returns. Each open DB has exactly one doWrites worker;
// DB.Close waits for it. This needs no hook in the production allocator.
func OpenStores() (int, string) {
	var stacks bytes.Buffer
	if err := pprof.Lookup("goroutine").WriteTo(&stacks, 2); err != nil {
		panic(err)
	}
	profile := stacks.String()
	return strings.Count(profile, "github.com/dgraph-io/badger/v4.(*DB).doWrites("), profile
}

// StoreBoundary checks a whole package after all test cleanups have run.
// A package boundary avoids counting another parallel test's live fixtures as
// leaks. It observes stores independently of constructor names and return values.
type StoreBoundary struct{ start int }

// BeginStoreBoundary records the starting population before testing.M.Run.
func BeginStoreBoundary() StoreBoundary {
	count, _ := OpenStores()
	return StoreBoundary{start: count}
}

// Check preserves a failed test exit code and also rejects leaked stores.
// Call it before disposing any deliberate process-lifetime negative control.
func (boundary StoreBoundary) Check(code int, output io.Writer, name string) int {
	count, stacks := OpenStores()
	if count != boundary.start {
		fmt.Fprintf(output, "%s: %d Badger store(s) left open after tests (before=%d after=%d)\n%s", name, count-boundary.start, boundary.start, count, stacks)
		return 1
	}
	return code
}

// Run checks the real store population at a package's TestMain boundary.
func Run(m *testing.M, name string) int {
	boundary := BeginStoreBoundary()
	return boundary.Check(m.Run(), os.Stderr, name)
}
