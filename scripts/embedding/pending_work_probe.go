//go:build ignore

package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

func main() {
	out, err := exec.Command("go", "env", "GOMODCACHE").Output()
	if err != nil {
		panic(err)
	}
	srcPath := filepath.Join(strings.TrimSpace(string(out)), "github.com/dgraph-io/badger/v4@v4.9.6/iterator.go")
	src, err := os.ReadFile(srcPath)
	if err != nil {
		panic(err)
	}
	if strings.Count(string(src), "func (it *Iterator) parseItem() bool {") != 1 {
		panic("Badger iterator changed: counter instrumentation needs review")
	}
	instrumented := strings.Replace(string(src), "import (", "import (\n\"expvar\"", 1)
	instrumented = strings.Replace(instrumented, "func (it *Iterator) parseItem() bool {", "var embedPendingParseWork = expvar.NewInt(\"embed_pending_parse_work\")\n\nfunc (it *Iterator) parseItem() bool {\n embedPendingParseWork.Add(1)", 1)
	dir, err := os.MkdirTemp("", "embed-pending-probe-")
	if err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	moduleDir := filepath.Join(dir, "badger")
	err = filepath.WalkDir(filepath.Dir(srcPath), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, _ := filepath.Rel(filepath.Dir(srcPath), path)
		target := filepath.Join(moduleDir, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0600)
	})
	if err != nil {
		panic(err)
	}
	dst := filepath.Join(moduleDir, "iterator.go")
	if err := os.WriteFile(dst, []byte(instrumented), 0600); err != nil {
		panic(err)
	}
	mod, _ := os.ReadFile("go.mod")
	mod = append(mod, []byte("\nreplace github.com/dgraph-io/badger/v4 => "+moduleDir+"\n")...)
	modPath := filepath.Join(dir, "go.mod")
	if err := os.WriteFile(modPath, mod, 0600); err != nil {
		panic(err)
	}
	sum, _ := os.ReadFile("go.sum")
	_ = os.WriteFile(filepath.Join(dir, "go.sum"), sum, 0600)
	args := []string{"test", "-modfile", modPath, "./pkg/storage", "-run", "^TestPendingClaimPhysicalWork$", "-count=1", "-v"}
	args = append(args, os.Args[1:]...)
	fmt.Println("INSTRUMENTED_BADGER_PARSE_COUNTER", srcPath)
	cmd := exec.Command("go", args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = append(os.Environ(), "NORNICDB_EMBED_PENDING_WORK_COUNTER=required")
	if err := cmd.Run(); err != nil {
		os.Exit(1)
	}
}
