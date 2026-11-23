package tck

import (
    "bytes"
    "fmt"
    "os"
    "os/exec"
    "sync"
    "testing"
)

// TestT08ConcurrentSharedReferenceDriver reproduces the report's four-process
// collision while keeping all children inside one lab job and one package.
func TestT08ConcurrentSharedReferenceDriver(t *testing.T) {
    if os.Getenv("NORNICDB_NEO4J_REFERENCE_URI") == "" {
        t.Fatal("the shared Neo4j reference must be supplied with --ref neo-ref")
    }
    parsers := []string{"nornic", "antlr", "nornic", "antlr"}
    type child struct {
        parser string
        cmd    *exec.Cmd
		stdout bytes.Buffer
		stderr bytes.Buffer
    }
    children := make([]*child, 0, len(parsers))
    for i, parser := range parsers {
        c := &child{parser: parser}
        c.cmd = exec.Command(os.Args[0], "-test.run=^TestFixedDifferentialCorpusMatchesPinnedNeo4j$", "-test.v")
        c.cmd.Env = append(os.Environ(), fmt.Sprintf("NORNICDB_PARSER=%s", parser), fmt.Sprintf("T08_CHILD=%d", i))
		c.cmd.Stdout = &c.stdout
		c.cmd.Stderr = &c.stderr
        if err := c.cmd.Start(); err != nil {
            t.Fatalf("start %s differential child: %v", parser, err)
        }
        children = append(children, c)
    }
    var wg sync.WaitGroup
    for _, c := range children {
        wg.Add(1)
        go func(c *child) {
            defer wg.Done()
            if err := c.cmd.Wait(); err != nil {
				t.Errorf("%s differential child failed: %v\nstdout:\n%s\nstderr:\n%s", c.parser, err, c.stdout.String(), c.stderr.String())
            }
        }(c)
    }
    wg.Wait()
}
