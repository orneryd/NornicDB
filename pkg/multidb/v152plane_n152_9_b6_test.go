package multidb

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"

	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// nor183ScenarioCompositeTwoRemoteSuccess is the ordinary multi-remote composite
// success path. The other NOR-183 scenarios cover one remote constituent and its
// rejection paths; this one selects a composite root that resolves two distinct
// remote aliases plus a local constituent, and then reads the real data the
// assembled composite delivers.
const nor183ScenarioCompositeTwoRemoteSuccess = "composite-two-remote-success"

const (
	nor183CompositeName      = "nor183_cmp"
	nor183CompositeLocal     = "nor183_local"
	nor183CompositeAlphaURI  = "http://nor183-alpha.invalid"
	nor183CompositeBetaURI   = "http://nor183-beta.invalid"
	nor183CompositeToken     = "Bearer nor183-forwarded-token"
	nor183CompositeAlphaUser = "alpha-user"
	nor183CompositeAlphaPass = "synthetic-nor183-alpha-password-739154"
	nor183CompositeCipherKey = "synthetic-nor183-composite-key-547128"
)

// TestNOR183CompositeTwoRemoteSuccess runs the two-remote composite root
// selection in the isolated child the NOR-183 watchdog owns, so a regression that
// takes the metadata lock across a factory callback fails with the caller's
// goroutine dump instead of hanging the package run.
func TestNOR183CompositeTwoRemoteSuccess(t *testing.T) {
	output := nor183RunCallbackScenario(t, nor183ScenarioCompositeTwoRemoteSuccess)
	require.Contains(t, output, nor183CompletedMarker+" scenario="+nor183ScenarioCompositeTwoRemoteSuccess,
		"NOR-183 two-remote composite child must reach the scenario completion marker: %s", output)
}

// nor183RemoteCall is one observed remote-engine factory invocation. It records
// the runtime identity the manager handed the factory and the public catalog the
// factory read while the manager was creating that constituent.
type nor183RemoteCall struct {
	ref     ConstituentRef
	token   string
	catalog []string
}

// nor183RemoteView is one remote constituent's engine. It reads and owns the
// storage of the remote database it represents, like the connection-owning engine
// a real remote factory returns: closing it releases that remote storage, which
// the manager itself never owns.
type nor183RemoteView struct {
	*storage.NamespacedEngine
	closeOnce sync.Once
	closeErr  error
}

func (view *nor183RemoteView) Close() error {
	view.closeOnce.Do(func() {
		view.closeErr = view.NamespacedEngine.GetInnerEngine().Close()
	})
	return view.closeErr
}

// nor183TwoRemoteObservation is what one composite root selection actually
// assembled, read back through the composite engine the caller received: the
// composite's real node total, the per-source count derived from the returned
// nodes' database provenance, and the stored property of one node read per
// source through its qualified element ID.
type nor183TwoRemoteObservation struct {
	total       int64
	byDatabase  map[string]int64
	sourceAlpha string
	sourceBeta  string
	sourceLocal string
}

// nor183PrepareCompositeTwoRemote seeds three distinguishable real sources, then
// selects the composite root through the public manager. The factories reenter
// ListDatabaseNames, which reads the metadata lock: under the NOR-183 regression
// that call blocks forever because the manager still holds the lock it took to
// build the constituent.
//
// The selection/source contract this scenario exercises, taken from the source
// this worktree contains:
//
//   - resolveStorageSelection (storage_view.go) is the single resolution owner.
//     Under m.mu it records either a cached view, a local namespace view, or, per
//     remote alias, that alias's immutable *runtimeConstituent snapshot plus the
//     stored DatabaseName and AccessMode. It never invokes a remote factory, and
//     buildCompositeStorageSelection completes the assembly after the lock is
//     released. That is why every factory below can read the public catalog.
//   - runtimeConstituent records are write-once. resolveRuntimeConstituent
//     (remote_credentials.go) is the only writer of its fields; the only
//     publisher, rebuildRuntimeConstituentsFor, builds a fresh map and assigns
//     database.runtimeConstituentsByAlias (an unchanged remote registration may
//     reuse its previous record when cached.ref == ref, which is safe only
//     because that record is never mutated). The two eager rebuild entry points are
//     catalog publication (catalog_transition.go publishCatalog, under m.mu) and
//     catalog load (metadata.go loadMetadata, during construction), and the lazy
//     rebuilds in resolveStorageSelection and resolveCompositeConstituentInternal
//     also run under m.mu. A later catalog write therefore replaces the map and
//     its records; it never mutates a record a caller already captured, so the
//     snapshot read outside the lock cannot observe a partially updated identity.
//   - Only mutable manager state stays lock-protected: m.engines, m.databases and
//     m.pendingDrops. The assembled CompositeEngine is not cached by the
//     manager, so later selections resolve current catalog state. The caller
//     owns the returned wrapper's lifetime; pendingDrops/status admission is
//     still checked under the lock while the selection is resolved.
//   - Close keeps its accepted owners. DatabaseManager.Close cancels the manager
//     lifetime, replaces the cached-view map and closes the inner store; it never
//     owned factory-returned remote engines. CompositeEngine.Close closes every
//     constituent, so the wrapper releases the remote engines the factories
//     returned, as this scenario asserts.
//   - Metadata snapshots stay detached: cloneDatabaseInfo copies the stored
//     constituents (snapshotConstituents) and never copies
//     runtimeConstituentsByAlias, so listing a database cannot expose a resolved
//     runtime identity.
func nor183PrepareCompositeTwoRemote(t *testing.T) (*nor183Scenario, error) {
	t.Helper()
	ctx := t.Context()
	inner := fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed)
	remotes := map[string]*nor183RemoteView{
		"alpha": fixtureowner.Track(t, &nor183RemoteView{NamespacedEngine: storage.NewNamespacedEngine(
			fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed), "alpha_db")}, storage.ErrStorageClosed),
		"beta": fixtureowner.Track(t, &nor183RemoteView{NamespacedEngine: storage.NewNamespacedEngine(
			fixtureowner.Track(t, storage.NewMemoryEngine(), storage.ErrStorageClosed), "beta_db")}, storage.ErrStorageClosed),
	}

	// Two remotes with deliberate overlap in local node IDs: only the qualified
	// read and the per-source provenance can tell them apart.
	for _, seed := range []struct {
		alias string
		count int
	}{{"alpha", 2}, {"beta", 3}} {
		for i := 0; i < seed.count; i++ {
			if _, err := remotes[seed.alias].CreateNode(ctx, &storage.Node{
				ID:         storage.NodeID("shared-" + strconv.Itoa(i)),
				Labels:     []string{"Nor183"},
				Properties: map[string]any{"source": seed.alias},
			}); err != nil {
				return nil, err
			}
		}
	}

	var manager *DatabaseManager
	selections := make(map[string][]nor183RemoteCall)
	manager, err := NewDatabaseManager(ctx, inner, &Config{
		RemoteCredentialEncryptionKey: nor183CompositeCipherKey,
		RemoteEngineFactory: func(ref ConstituentRef, authToken string) (storage.Engine, error) {
			selections[ref.Alias] = append(selections[ref.Alias], nor183RemoteCall{
				ref:     ref,
				token:   authToken,
				catalog: manager.ListDatabaseNames(),
			})
			engine, exists := remotes[ref.Alias]
			if !exists {
				return nil, fmt.Errorf("unexpected remote alias %q", ref.Alias)
			}
			return engine, nil
		},
	})
	if err != nil {
		return nil, err
	}

	if err := manager.CreateDatabase(ctx, DatabaseCreateRequest{Name: nor183CompositeLocal}); err != nil {
		return nil, err
	}
	if _, err := storage.NewNamespacedEngine(inner, nor183CompositeLocal).CreateNode(ctx, &storage.Node{
		ID:         "local-0",
		Labels:     []string{"Nor183"},
		Properties: map[string]any{"source": "local"},
	}); err != nil {
		return nil, err
	}
	// alpha carries explicit credentials and beta forwards the caller's token, so
	// each factory must observe its own accepted auth identity.
	if err := manager.CreateCompositeDatabase(ctx, nor183CompositeName, []ConstituentRef{
		{Alias: "local", DatabaseName: nor183CompositeLocal, Type: "local", AccessMode: "read_write"},
		{Alias: "alpha", DatabaseName: "alpha_db", Type: "remote", AccessMode: "read_write",
			URI: nor183CompositeAlphaURI, AuthMode: "user_password", User: nor183CompositeAlphaUser, Password: nor183CompositeAlphaPass},
		{Alias: "beta", DatabaseName: "beta_db", Type: "remote", AccessMode: "read_write",
			URI: nor183CompositeBetaURI, AuthMode: "oidc_forwarding"},
	}); err != nil {
		return nil, err
	}

	var observed nor183TwoRemoteObservation
	return &nor183Scenario{
		trigger: func() nor183Outcome {
			outcome := nor183Outcome{}
			engine, err := manager.GetStorageWithAuth(nor183CompositeName, nor183CompositeToken)
			outcome.callErr = err
			if err != nil {
				outcome.closeErr = manager.Close()
				return outcome
			}
			outcome.engineReadErr = nor183ObserveTwoRemoteComposite(ctx, engine, &observed)
			outcome.engineCloseErr = engine.Close()
			outcome.closeErr = manager.Close()
			outcome.names = observed.summary()
			return outcome
		},
		verify: func(t *testing.T, got nor183Outcome) {
			require.NoError(t, got.callErr)
			require.NoError(t, got.engineReadErr)
			require.NoError(t, got.engineCloseErr)
			require.NoError(t, got.closeErr)

			// One root selection builds each remote alias exactly once.
			require.Len(t, selections, 2)
			alpha := nor183SingleRemoteCall(t, selections, "alpha")
			beta := nor183SingleRemoteCall(t, selections, "beta")

			// Each factory sees its own alias, backing database and auth identity.
			require.Equal(t, "alpha", alpha.ref.Alias)
			require.Equal(t, "read_write", alpha.ref.AccessMode)
			require.Equal(t, "alpha_db", alpha.ref.DatabaseName)
			require.Equal(t, nor183CompositeAlphaURI, alpha.ref.URI)
			require.Equal(t, nor183CompositeAlphaUser, alpha.ref.User)
			require.Equal(t, nor183CompositeAlphaPass, alpha.ref.Password)
			require.Equal(t, "user_password", alpha.ref.AuthMode)
			require.Equal(t, nor183CompositeToken, alpha.token)

			require.Equal(t, "beta", beta.ref.Alias)
			require.Equal(t, "read_write", beta.ref.AccessMode)
			require.Equal(t, "beta_db", beta.ref.DatabaseName)
			require.Equal(t, nor183CompositeBetaURI, beta.ref.URI)
			require.Empty(t, beta.ref.User, "a forwarding constituent must not carry stored credentials")
			require.Empty(t, beta.ref.Password, "a forwarding constituent must not carry stored credentials")
			require.Equal(t, "oidc_forwarding", beta.ref.AuthMode)
			require.Equal(t, nor183CompositeToken, beta.token)

			// Both reentries into the public catalog completed with the catalog
			// that existed when the selection was resolved.
			for _, call := range []nor183RemoteCall{alpha, beta} {
				require.Contains(t, call.catalog, nor183CompositeName)
				require.Contains(t, call.catalog, nor183CompositeLocal)
			}

			// Each source contributed its own real stored data to the composite.
			require.EqualValues(t, 6, observed.total)
			require.Equal(t, map[string]int64{"alpha_db": 2, "beta_db": 3, nor183CompositeLocal: 1}, observed.byDatabase)
			require.Equal(t, "alpha", observed.sourceAlpha)
			require.Equal(t, "beta", observed.sourceBeta)
			require.Equal(t, "local", observed.sourceLocal)

			// The composite wrapper owns the factory engines it assembled; the
			// factories' remote storage was released with it, so no fixture leaks.
			_, alphaErr := remotes["alpha"].GetInnerEngine().NodeCount(t.Context())
			_, betaErr := remotes["beta"].GetInnerEngine().NodeCount(t.Context())
			require.ErrorIs(t, alphaErr, storage.ErrStorageClosed, "composite close must release alpha's remote storage")
			require.ErrorIs(t, betaErr, storage.ErrStorageClosed, "composite close must release beta's remote storage")
		},
	}, nil
}

// nor183ObserveTwoRemoteComposite reads the assembled composite through its
// public engine interface. Every expectation below is a product effect: a stored
// property, a delivered node count, or a node's source database.
func nor183ObserveTwoRemoteComposite(ctx context.Context, engine storage.Engine, observed *nor183TwoRemoteObservation) error {
	var result error
	total, err := engine.NodeCount(ctx)
	result = errors.Join(result, err)
	observed.total = total

	for _, read := range []struct {
		qualified storage.NodeID
		into      *string
		label     string
	}{
		{"4:alpha_db:shared-0", &observed.sourceAlpha, "alpha"},
		{"4:beta_db:shared-0", &observed.sourceBeta, "beta"},
		{"4:" + nor183CompositeLocal + ":local-0", &observed.sourceLocal, "local"},
	} {
		node, err := engine.GetNode(ctx, read.qualified)
		if err != nil {
			result = errors.Join(result, fmt.Errorf("read %s: %w", read.qualified, err))
			continue
		}
		source, _ := node.Properties["source"].(string)
		*read.into = source
		if source != read.label {
			result = errors.Join(result, fmt.Errorf("%s delivered source %q, expected %q", read.qualified, source, read.label))
		}
	}

	nodes, err := engine.AllNodes(ctx)
	if err != nil {
		return errors.Join(result, err)
	}
	observed.byDatabase = make(map[string]int64, 3)
	for _, node := range nodes {
		observed.byDatabase[node.RuntimeMetadata().Database]++
	}
	return result
}

// nor183SingleRemoteCall returns the alias's only factory call and fails the
// child if the alias was built more than once for one root selection.
func nor183SingleRemoteCall(t *testing.T, selections map[string][]nor183RemoteCall, alias string) nor183RemoteCall {
	t.Helper()
	calls := selections[alias]
	require.Len(t, calls, 1, "remote alias %q must be built exactly once per root selection", alias)
	return calls[0]
}

// summary is the per-source evidence the child prints on completion.
func (observation nor183TwoRemoteObservation) summary() []string {
	databases := make([]string, 0, len(observation.byDatabase))
	for database := range observation.byDatabase {
		databases = append(databases, database)
	}
	sort.Strings(databases)
	summary := make([]string, 0, len(databases)+1)
	for _, database := range databases {
		summary = append(summary, database+"="+strconv.FormatInt(observation.byDatabase[database], 10))
	}
	return append(summary, "total="+strconv.FormatInt(observation.total, 10))
}
