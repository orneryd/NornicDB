package storagecensus

// Author: #1667 implementation lane (Codex). The complete effect gate must
// distinguish concrete dispatch from a conservative union of VTA targets.

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolvedWriterTargetsRetainReceiverConditions(t *testing.T) {
	dir := t.TempDir()
	for name, source := range map[string]string{
		"go.mod":           "module github.com/orneryd/nornicdb\n\ngo 1.26\nrequire github.com/dgraph-io/badger/v4 v4.0.0\nreplace github.com/dgraph-io/badger/v4 => ./badger\n",
		"badger/go.mod":    "module github.com/dgraph-io/badger/v4\n\ngo 1.26\n",
		"badger/source.go": "package badger\ntype Txn struct{}\nfunc(*Txn)Set(key,value []byte)error{return nil}\n",
		"pkg/storage/source.go": `package storage
import badger "github.com/dgraph-io/badger/v4"
const prefixMVCCMeta byte=0xff
const prefixNode byte=1
type writer interface{Set([]byte,[]byte)error}
type staged struct{}
func (*staged)Set([]byte,[]byte)error{return nil}
func call(w writer)error{return w.Set([]byte{prefixNode},nil)}
func captured(w writer)error{return func()error{return w.Set([]byte{prefixNode},nil)}()}
func Native(w *badger.Txn)error{return call(w)}
func Staged()error{return call(&staged{})}
func Unknown(w writer)error{return call(w)}
func Mixed(w *badger.Txn,b bool)error{var v writer=w;if b{v=&staged{}};return call(v)}
func CapturedNative(w *badger.Txn)error{return captured(w)}
func CapturedStaged()error{return captured(&staged{})}
func capturedChanged(w writer)error{f:=func()error{return w.Set([]byte{prefixNode},nil)};w=&badger.Txn{};return f()}
func CapturedChanged()error{return capturedChanged(&staged{})}
func callFunction(f func([]byte,[]byte)error)error{return f([]byte{prefixNode},nil)}
func FunctionNative(w *badger.Txn)error{return callFunction(w.Set)}
func FunctionStaged()error{return callFunction((&staged{}).Set)}
func FunctionUnknown(f func([]byte,[]byte)error)error{return callFunction(f)}
func FunctionMixed(w *badger.Txn,b bool)error{f:=w.Set;if b{f=(&staged{}).Set};return callFunction(f)}
func method(w writer)error{return callFunction(w.Set)}
func FunctionMethodNative(w *badger.Txn)error{return method(w)}
func FunctionMethodStaged()error{return method(&staged{})}
`,
	} {
		path := filepath.Join(dir, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
		require.NoError(t, os.WriteFile(path, []byte(source), 0600))
	}
	r, err := Census(dir, nil)
	require.NoError(t, err)
	entries := map[string]bool{}
	for _, row := range r.Unordered {
		if row.Space == "prefixNode" {
			for _, name := range row.Entries {
				entries[name] = true
			}
		}
	}
	for _, probe := range []struct {
		name string
		want bool
	}{
		{"Native", true}, {"Staged", false}, {"Unknown", true}, {"Mixed", true}, {"CapturedNative", true}, {"CapturedStaged", false}, {"CapturedChanged", true},
		{"FunctionNative", true}, {"FunctionStaged", false}, {"FunctionUnknown", true}, {"FunctionMixed", true}, {"FunctionMethodNative", true}, {"FunctionMethodStaged", false},
	} {
		t.Run(probe.name, func(t *testing.T) {
			require.Equal(t, probe.want, entries["pkg/storage."+probe.name], "only an impossible concrete target removes an effect; unknown/mixed receivers retain it")
		})
	}
}
