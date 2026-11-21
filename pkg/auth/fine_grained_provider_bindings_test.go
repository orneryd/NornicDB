package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/orneryd/nornicdb/internal/testfixture/storagefault"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// errProviderBindingStorage is the injected storage failure for the binding
// transaction tests. It is distinct from the binding contract errors so a
// failed write cannot be mistaken for a refused one.
var errProviderBindingStorage = errors.New("provider binding storage failed")

// writeStagedProviderBindings writes one user's bindings the way the user
// directory does: inside the caller's system-store transaction.
func writeStagedProviderBindings(ctx context.Context, engine storage.Engine, userID string, providers map[string]string) error {
	return WithStoreTransaction(ctx, engine, func(view storage.Engine) error {
		return writeFineGrainedProviderBindings(ctx, view, userID, providers)
	})
}

func TestFineGrainedProviderBindingsStoreSeparatelyFromUsers(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	userID := "usr-synthetic-a"

	bindings, err := readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.NotNil(t, bindings)
	require.Empty(t, bindings)

	wanted := map[string]string{
		"synthetic-oidc": "synthetic-subject-1",
		"synthetic-saml": "synthetic-subject-2",
	}
	require.NoError(t, writeStagedProviderBindings(ctx, system, userID, wanted))
	stored, err := readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Equal(t, wanted, stored)
	for provider, subject := range wanted {
		owner, err := fineGrainedProviderUserID(ctx, system, provider, subject)
		require.NoError(t, err)
		require.Equal(t, userID, owner)
	}
	_, err = fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "synthetic-subject-unknown")
	require.ErrorIs(t, err, storage.ErrNotFound)

	// The binding list and the unique lookup are system nodes of their own.
	record, err := system.GetNode(ctx, fineGrainedProviderBindingID(userID))
	require.NoError(t, err)
	require.Equal(t, []string{"_System", fineGrainedProviderBindingsLabel}, record.Labels)
	require.NotContains(t, record.Properties, "password_hash")
	lookup, err := system.GetNode(ctx, fineGrainedProviderSubjectID("synthetic-oidc", "synthetic-subject-1"))
	require.NoError(t, err)
	require.Equal(t, []string{"_System", fineGrainedProviderSubjectLabel}, lookup.Labels)

	// The RBAC user record keeps its own node and stays the only _User node,
	// so binding storage neither reads nor rewrites it.
	users := &Authenticator{storage: system, users: map[string]*User{}}
	require.NoError(t, users.saveUser(ctx, &User{ID: userID, Username: "synthetic-user", Roles: []Role{RoleViewer}}, nil))
	userNodes, err := system.GetNodesByLabel(ctx, "_User")
	require.NoError(t, err)
	require.Len(t, userNodes, 1)
	require.Equal(t, storage.NodeID("user:synthetic-user"), userNodes[0].ID)
	stored, err = readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Equal(t, wanted, stored)
	_, err = system.GetNode(ctx, storage.NodeID("user:"+userID))
	require.ErrorIs(t, err, storage.ErrNotFound)
}

func TestFineGrainedProviderBindingsReplaceAndRemove(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	userID := "usr-synthetic-a"

	require.NoError(t, writeStagedProviderBindings(ctx, system, userID, map[string]string{"synthetic-oidc": "subject-one"}))
	require.NoError(t, writeStagedProviderBindings(ctx, system, userID, map[string]string{
		"synthetic-oidc": "subject-one",
		"synthetic-saml": "subject-two",
	}))
	stored, err := readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"synthetic-oidc": "subject-one",
		"synthetic-saml": "subject-two",
	}, stored)

	// Replacing one subject releases the previous one and the removed provider.
	require.NoError(t, writeStagedProviderBindings(ctx, system, userID, map[string]string{"synthetic-oidc": "subject-three"}))
	stored, err = readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"synthetic-oidc": "subject-three"}, stored)
	for _, released := range [][2]string{{"synthetic-oidc", "subject-one"}, {"synthetic-saml", "subject-two"}} {
		_, err := fineGrainedProviderUserID(ctx, system, released[0], released[1])
		require.ErrorIs(t, err, storage.ErrNotFound)
	}
	owner, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-three")
	require.NoError(t, err)
	require.Equal(t, userID, owner)

	// Removing every binding deletes the binding list record itself.
	require.NoError(t, writeStagedProviderBindings(ctx, system, userID, nil))
	stored, err = readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Empty(t, stored)
	_, err = system.GetNode(ctx, fineGrainedProviderBindingID(userID))
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-three")
	require.ErrorIs(t, err, storage.ErrNotFound)

	// A released subject is bindable by another user afterwards.
	require.NoError(t, writeStagedProviderBindings(ctx, system, "usr-synthetic-b", map[string]string{"synthetic-oidc": "subject-three"}))
	owner, err = fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-three")
	require.NoError(t, err)
	require.Equal(t, "usr-synthetic-b", owner)
}

func TestFineGrainedProviderBindingsRefuseSubjectBoundToAnotherUser(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	ownerID, otherID := "usr-synthetic-a", "usr-synthetic-b"
	require.NoError(t, writeStagedProviderBindings(ctx, system, ownerID, map[string]string{"synthetic-oidc": "shared-subject"}))

	require.ErrorIs(t, writeStagedProviderBindings(ctx, system, otherID, map[string]string{
		"synthetic-oidc": "shared-subject",
	}), errFineGrainedProviderSubjectBound)
	_, err := system.GetNode(ctx, fineGrainedProviderBindingID(otherID))
	require.ErrorIs(t, err, storage.ErrNotFound)
	owner, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "shared-subject")
	require.NoError(t, err)
	require.Equal(t, ownerID, owner)

	// A refused write leaves none of its other bindings behind.
	require.ErrorIs(t, writeStagedProviderBindings(ctx, system, otherID, map[string]string{
		"synthetic-free": "free-subject",
		"synthetic-oidc": "shared-subject",
	}), errFineGrainedProviderSubjectBound)
	_, err = fineGrainedProviderUserID(ctx, system, "synthetic-free", "free-subject")
	require.ErrorIs(t, err, storage.ErrNotFound)
	_, err = system.GetNode(ctx, fineGrainedProviderBindingID(otherID))
	require.ErrorIs(t, err, storage.ErrNotFound)

	// The provider name is part of the key, so the same external subject under
	// another provider is a different binding.
	require.NoError(t, writeStagedProviderBindings(ctx, system, otherID, map[string]string{"synthetic-saml": "shared-subject"}))
	owner, err = fineGrainedProviderUserID(ctx, system, "synthetic-saml", "shared-subject")
	require.NoError(t, err)
	require.Equal(t, otherID, owner)

	// Re-writing a binding its owner already holds is not a conflict.
	require.NoError(t, writeStagedProviderBindings(ctx, system, ownerID, map[string]string{"synthetic-oidc": "shared-subject"}))
}

func TestFineGrainedProviderBindingsRequireIdentifiers(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	userID := "usr-synthetic-a"

	_, err := readFineGrainedProviderBindings(ctx, system, "")
	require.ErrorIs(t, err, errFineGrainedProviderBinding)
	require.ErrorIs(t, writeStagedProviderBindings(ctx, system, "", map[string]string{"synthetic-oidc": "subject-one"}), errFineGrainedProviderBinding)
	require.ErrorIs(t, writeStagedProviderBindings(ctx, system, userID, map[string]string{"": "subject-one"}), errFineGrainedProviderBinding)
	require.ErrorIs(t, writeStagedProviderBindings(ctx, system, userID, map[string]string{"synthetic-oidc": ""}), errFineGrainedProviderBinding)
	_, err = system.GetNode(ctx, fineGrainedProviderBindingID(userID))
	require.ErrorIs(t, err, storage.ErrNotFound)

	_, err = fineGrainedProviderUserID(ctx, system, "", "subject-one")
	require.ErrorIs(t, err, errFineGrainedProviderBinding)
	_, err = fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "")
	require.ErrorIs(t, err, errFineGrainedProviderBinding)

	// A stored record that does not hold the contract is reported, not read as
	// an empty binding list or an unknown subject.
	_, err = system.CreateNode(ctx, &storage.Node{
		ID:         fineGrainedProviderBindingID(userID),
		Labels:     []string{"_System", fineGrainedProviderBindingsLabel},
		Properties: map[string]any{fineGrainedProviderBindingsProperty: "not json"},
	})
	require.NoError(t, err)
	_, err = readFineGrainedProviderBindings(ctx, system, userID)
	require.ErrorIs(t, err, errFineGrainedProviderBinding)

	_, err = system.CreateNode(ctx, &storage.Node{
		ID:         fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"),
		Labels:     []string{"_System", fineGrainedProviderSubjectLabel},
		Properties: map[string]any{"unrelated": "value"},
	})
	require.NoError(t, err)
	_, err = fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-one")
	require.ErrorIs(t, err, errFineGrainedProviderBinding)
}

func TestFineGrainedProviderBindingsRefuseInconsistentLookup(t *testing.T) {
	ctx := context.Background()
	userID, otherID := "usr-synthetic-a", "usr-synthetic-b"
	binding := map[string]string{"synthetic-oidc": "subject-one"}

	t.Run("lookup bound to another user", func(t *testing.T) {
		system := newSystemEngine(t)
		require.NoError(t, writeStagedProviderBindings(ctx, system, userID, binding))
		lookupID := fineGrainedProviderSubjectID("synthetic-oidc", "subject-one")
		require.NoError(t, system.DeleteNode(ctx, lookupID))
		_, err := system.CreateNode(ctx, &storage.Node{
			ID:         lookupID,
			Labels:     []string{"_System", fineGrainedProviderSubjectLabel},
			Properties: map[string]any{fineGrainedProviderOwnerProperty: otherID},
		})
		require.NoError(t, err)

		require.ErrorIs(t, writeStagedProviderBindings(ctx, system, userID, nil), errFineGrainedProviderBinding)
		owner, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-one")
		require.NoError(t, err)
		require.Equal(t, otherID, owner)
		stored, err := readFineGrainedProviderBindings(ctx, system, userID)
		require.NoError(t, err)
		require.Equal(t, binding, stored)
	})

	t.Run("lookup missing", func(t *testing.T) {
		system := newSystemEngine(t)
		require.NoError(t, writeStagedProviderBindings(ctx, system, userID, binding))
		require.NoError(t, system.DeleteNode(ctx, fineGrainedProviderSubjectID("synthetic-oidc", "subject-one")))

		require.ErrorIs(t, writeStagedProviderBindings(ctx, system, userID, nil), errFineGrainedProviderBinding)
		stored, err := readFineGrainedProviderBindings(ctx, system, userID)
		require.NoError(t, err)
		require.Equal(t, binding, stored)
	})
}

func TestFineGrainedProviderBindingsRollBackOnFailure(t *testing.T) {
	ctx := context.Background()
	system := newSystemEngine(t)
	fault := storagefault.NewMutationFailureEngine(system, "", errProviderBindingStorage, 0, WithStoreTransaction)
	userID := "usr-synthetic-a"
	held := map[string]string{"synthetic-oidc": "subject-one"}
	require.NoError(t, writeStagedProviderBindings(ctx, fault, userID, held))

	t.Run("binding list create", func(t *testing.T) {
		otherID := "usr-synthetic-b"
		fault.Select(storagefault.MutationCreate, fineGrainedProviderBindingID(otherID), 0)
		t.Cleanup(func() { fault.Select("", "", 0) })
		require.ErrorIs(t, writeStagedProviderBindings(ctx, fault, otherID, map[string]string{
			"synthetic-oidc": "subject-two",
		}), errProviderBindingStorage)
		_, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-two")
		require.ErrorIs(t, err, storage.ErrNotFound)
		_, err = system.GetNode(ctx, fineGrainedProviderBindingID(otherID))
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("subject claim", func(t *testing.T) {
		fault.Select(storagefault.MutationCreate, fineGrainedProviderSubjectID("synthetic-saml", "subject-two"), 0)
		t.Cleanup(func() { fault.Select("", "", 0) })
		require.ErrorIs(t, writeStagedProviderBindings(ctx, fault, userID, map[string]string{
			"synthetic-oidc": "subject-one",
			"synthetic-saml": "subject-two",
		}), errProviderBindingStorage)
		stored, err := readFineGrainedProviderBindings(ctx, system, userID)
		require.NoError(t, err)
		require.Equal(t, held, stored)
		_, err = fineGrainedProviderUserID(ctx, system, "synthetic-saml", "subject-two")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("released subject", func(t *testing.T) {
		fault.Select(storagefault.MutationDelete, fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"), 0)
		t.Cleanup(func() { fault.Select("", "", 0) })
		require.ErrorIs(t, writeStagedProviderBindings(ctx, fault, userID, nil), errProviderBindingStorage)
		stored, err := readFineGrainedProviderBindings(ctx, system, userID)
		require.NoError(t, err)
		require.Equal(t, held, stored)
		owner, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-one")
		require.NoError(t, err)
		require.Equal(t, userID, owner)
	})

	t.Run("binding list update", func(t *testing.T) {
		fault.Select(storagefault.MutationUpdate, fineGrainedProviderBindingID(userID), 0)
		t.Cleanup(func() { fault.Select("", "", 0) })
		require.ErrorIs(t, writeStagedProviderBindings(ctx, fault, userID, map[string]string{
			"synthetic-oidc": "subject-three",
		}), errProviderBindingStorage)
		_, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-three")
		require.ErrorIs(t, err, storage.ErrNotFound)
		owner, err := fineGrainedProviderUserID(ctx, system, "synthetic-oidc", "subject-one")
		require.NoError(t, err)
		require.Equal(t, userID, owner)
		stored, err := readFineGrainedProviderBindings(ctx, system, userID)
		require.NoError(t, err)
		require.Equal(t, held, stored)
	})

	t.Run("binding list read", func(t *testing.T) {
		fault.Select(storagefault.MutationGet, fineGrainedProviderBindingID(userID), 0)
		t.Cleanup(func() { fault.Select("", "", 0) })
		require.ErrorIs(t, writeStagedProviderBindings(ctx, fault, userID, held), errProviderBindingStorage)
	})

	require.NoError(t, writeStagedProviderBindings(ctx, fault, userID, map[string]string{"synthetic-oidc": "subject-four"}))
	stored, err := readFineGrainedProviderBindings(ctx, system, userID)
	require.NoError(t, err)
	require.Equal(t, map[string]string{"synthetic-oidc": "subject-four"}, stored)
}

func TestFineGrainedProviderBindingKeysAreBoundedAndDistinct(t *testing.T) {
	// Golden digests pin the stored keys: a committed binding is read with the
	// same key after a restart, so the derivation may not change silently.
	require.Equal(t,
		storage.NodeID("fine_grained_provider_subject:aa559b1d01f51e09e64489cf43014f546fd725f854c05dcd8a31b5f6f5e88f0b"),
		fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"))
	require.Equal(t,
		storage.NodeID("fine_grained_provider_binding:0c16605ad61a710d070b806c62d9a0ae0b6c813bd50232bca35df1abac955db7"),
		fineGrainedProviderBindingID("usr-synthetic-a"))

	require.Equal(t, fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"), fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"))
	require.NotEqual(t, fineGrainedProviderSubjectID("ab", "c"), fineGrainedProviderSubjectID("a", "bc"))
	require.NotEqual(t, fineGrainedProviderSubjectID("synthetic-oidc", "subject-one"), fineGrainedProviderSubjectID("synthetic-saml", "subject-one"))
	require.NotEqual(t, fineGrainedProviderBindingID("usr-synthetic-a"), fineGrainedProviderBindingID("usr-synthetic-b"))

	long := fineGrainedProviderSubjectID(strings.Repeat("provider", 1024), strings.Repeat("subject", 1024))
	require.Len(t, string(long), len(fineGrainedProviderSubjectPrefix)+hex.EncodedLen(sha256.Size))
	require.True(t, strings.HasPrefix(string(long), fineGrainedProviderSubjectPrefix))
}
