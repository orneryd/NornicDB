package storage_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/capabilitycensus"
	"github.com/orneryd/nornicdb/internal/workcensus"
	"github.com/stretchr/testify/require"
)

func TestCapabilityCommitTypesAndRenamedProviders(t *testing.T) {
	root := t.TempDir()
	files := map[string]string{
		"go.mod": "module github.com/orneryd/nornicdb\ngo 1.26.0\n",
		"pkg/storage/engine.go": `package storage
import "context"
type Engine interface{Core()}
type BadgerTransaction struct{}
type TransactionCommitter interface{CommitTransaction(context.Context,*BadgerTransaction)error}
`,
		"plugins/unfamiliar/commit.go": `package unfamiliar
import "context"
import "github.com/orneryd/nornicdb/pkg/storage"
type Renamed struct{}
func(*Renamed)CommitTransaction(context.Context,*storage.BadgerTransaction)error{return nil}
func renamedWrapper(e storage.Engine)(storage.TransactionCommitter,bool){return &Renamed{},true}
func renamedConsumer(e storage.Engine){owner,_:=renamedWrapper(e);_ = owner.CommitTransaction(context.Background(),nil)}
func renamedMethodValue(e storage.Engine){owner,_:=renamedWrapper(e);invoke:=owner.CommitTransaction;_ = invoke(context.Background(),nil)}
func CommitTransaction(context.Context,*storage.BadgerTransaction)error{return nil}
func EntityCommitSubmitterFor(string)int{return 1}
func BeginEntityTransaction(string)int{return 1}
func managedEntityOwnerFor(string)int{return 1}
func nameOnly(){_ = EntityCommitSubmitterFor("x");_ = BeginEntityTransaction("x");_ = managedEntityOwnerFor("x");_ = CommitTransaction(context.Background(),nil)}
type SameNameService struct{}
func(*SameNameService)CommitTransaction(string)string{return "value"}
func nameOnlyMethod(service *SameNameService){_ = service.CommitTransaction("x")}
`,
	}
	for name, source := range files {
		path := filepath.Join(root, name)
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(source), 0644))
	}
	inventory, err := workcensus.Extract(capabilitycensus.Config(root))
	require.NoError(t, err)
	result, err := capabilitycensus.Interpret(inventory)
	require.NoError(t, err)
	found := map[string]bool{}
	aliasCall := false
	consumerCall := false
	for _, site := range result.Sites {
		require.Empty(t, site.Disposition, "type facts never approve a semantic disposition")
		require.Empty(t, site.Reason, "unread members remain OPEN")
		if site.Population != "commit_owners" {
			continue
		}
		if site.Kind == "call" && strings.HasPrefix(site.Detail, "invoke(") {
			aliasCall = true
		}
		if site.Kind == "call" && strings.HasPrefix(site.Detail, "owner.CommitTransaction(") {
			consumerCall = true
		}
		for _, name := range []string{"renamedWrapper", "renamedConsumer", "renamedMethodValue"} {
			if strings.HasSuffix(site.Function, "."+name) {
				found[name] = true
			}
		}
		for _, name := range []string{"CommitTransaction", "EntityCommitSubmitterFor", "BeginEntityTransaction", "managedEntityOwnerFor", "nameOnly", "nameOnlyMethod"} {
			require.False(t, strings.HasSuffix(site.Function, "/unfamiliar."+name), "same-named non-commit free/service function was classified by spelling: %s", site.Key)
		}
	}
	require.True(t, aliasCall, "the actual bound-method invocation must resolve through canonical alias type facts")
	require.True(t, consumerCall, "the public typed protocol call itself must be found")
	require.Len(t, found, 3, "actual type-based producer, consumer and bound method must survive arbitrary wrapper names")
}
