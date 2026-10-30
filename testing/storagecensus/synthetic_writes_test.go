package storagecensus

// Author: #1667 implementation lane (Codex). Exercise the real loader, effect
// extraction, fixed point and report; dependencies have only export data.

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSyntheticWrapperWritesHaveSourceOrigins(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"go.mod":           "module github.com/orneryd/nornicdb\n\ngo 1.26\nrequire github.com/dgraph-io/badger/v4 v4.0.0\nreplace github.com/dgraph-io/badger/v4 => ./badger\n",
		"badger/go.mod":    "module github.com/dgraph-io/badger/v4\n\ngo 1.26\n",
		"badger/badger.go": "package badger\ntype DB struct{}\nfunc (*DB)DropAll()error{return nil}\ntype Txn struct{}\nfunc (*Txn)Set(key,value []byte)error{return nil}\n",
	}
	for name, body := range files {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, []byte(body), 0600))
	}
	for _, probe := range []struct{ name, body, origin string }{
		{"direct", "return db.DropAll()", "return db.DropAll()"},
		{"bound", "f:=db.DropAll\n return f()", "return f()"},
		{"passed", "return invoke(db.DropAll)", "return f()"},
		{"promoted", "p:=&promoted{db};f:=p.DropAll\n return f()", "return f()"},
		{"interface", "var d dropper=db;f:=d.DropAll\n return f()", "return f()"},
		{"stored", "saved=db.DropAll;return nil", "saved=db.DropAll"},
		{"async", "go db.DropAll();return nil", "go db.DropAll()"},
		{"key-bound", "tx:=&badger.Txn{};f:=tx.Set\n return f([]byte{prefixNode},nil)", "return f([]byte{prefixNode},nil)"},
		{"key-passed", "tx:=&badger.Txn{};return invokeSet(tx.Set)", "return f([]byte{prefixNode},nil)"},
	} {
		t.Run(probe.name, func(t *testing.T) {
			source := "package storage\nimport badger \"github.com/dgraph-io/badger/v4\"\nconst prefixMVCCMeta byte=0xff\nconst prefixNode byte=1\ntype promoted struct{*badger.DB}\ntype dropper interface{DropAll()error}\nvar saved func()error\nfunc invoke(f func()error)error{\n return f()\n}\nfunc invokeSet(f func([]byte,[]byte)error)error{\n return f([]byte{prefixNode},nil)\n}\nfunc Root(db *badger.DB)error{\n " + probe.body + "\n}\n"
			path := filepath.Join(dir, "pkg/storage/fixture.go")
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, []byte(source), 0600))
			report, err := Census(dir, nil)
			require.NoError(t, err)
			line := 1 + strings.Count(source[:strings.LastIndex(source, probe.origin)], "\n")
			want := "pkg/storage/fixture.go:" + fmt.Sprint(line)
			space := "badger.DB.DropAll"
			if strings.HasPrefix(probe.name, "key-") {
				space = "prefixNode"
			}
			var writes []write
			for _, row := range report.Writes {
				if row.Space == space {
					writes = append(writes, row)
					require.Equal(t, want, row.Origin, "synthetic effects must retain their nearest real invocation, or their unresolved escaping source reference")
					require.NotContains(t, row.Func, " -")
				}
			}
			require.NotEmpty(t, writes, "a physical write through a compiler wrapper cannot disappear")
			foundRoot := false
			for _, row := range report.Unordered {
				if row.Space == space {
					require.Equal(t, want, row.Origin)
					for _, entry := range row.Entries {
						foundRoot = foundRoot || entry == "pkg/storage.Root"
					}
				}
			}
			require.True(t, foundRoot, "an unacquired synthetic write must reach its real public entry")
		})
	}
}
