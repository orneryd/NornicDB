package auth

// Durable configuration for named, externally managed user providers.
//
// A provider binding is external configuration: which external subject of
// which named provider belongs to one user. The binding list of a user is
// its own system node, and each (provider name, external subject) pair has its
// own unique lookup node, so an RBAC startup that loads _User and _Role
// records never reads or rewrites a binding. Both records are keyed by a
// fixed system namespace prefix followed by a stable digest, so a stored key
// never embeds an identifier and its length never depends on one.
//
// These functions store configuration only. Authentication, credentials,
// token claims, provider callbacks and principal selection are owned
// elsewhere.

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/orneryd/nornicdb/pkg/convert"
	"github.com/orneryd/nornicdb/pkg/storage"
)

const (
	// fineGrainedProviderBindingPrefix is the fixed system namespace of the
	// per-user binding lists; the rest of the ID is a digest of the user's
	// stable ID.
	fineGrainedProviderBindingPrefix = "fine_grained_provider_binding:"
	// fineGrainedProviderSubjectPrefix is the fixed system namespace of the
	// unique lookup from one provider name and external subject to its user.
	fineGrainedProviderSubjectPrefix = "fine_grained_provider_subject:"
	// fineGrainedProviderBindingsLabel labels a stored binding list.
	fineGrainedProviderBindingsLabel = "_FineGrainedProviderBindings"
	// fineGrainedProviderSubjectLabel labels a stored unique subject lookup.
	fineGrainedProviderSubjectLabel = "_FineGrainedProviderSubject"
	// fineGrainedProviderBindingsProperty holds the stored binding list.
	fineGrainedProviderBindingsProperty = "providers"
	// fineGrainedProviderOwnerProperty holds the bound user's stable ID in a
	// stored unique subject lookup.
	fineGrainedProviderOwnerProperty = "user_id"
)

var (
	// errFineGrainedProviderBinding marks a binding request, or a stored
	// binding record, that does not hold the identifiers and the consistency
	// this storage contract requires.
	errFineGrainedProviderBinding = errors.New("invalid fine-grained provider binding")
	// errFineGrainedProviderSubjectBound refuses a binding whose provider name
	// and external subject are already bound to another user.
	errFineGrainedProviderSubjectBound = errors.New("fine-grained provider subject is bound to another user")
)

// fineGrainedProviderDigest returns the hex SHA-256 digest of parts. Every
// part is prefixed with its 8-byte big-endian length, so different tuple
// boundaries ("ab", "c") and ("a", "bc") never produce the same digest, and a
// stored key stays bounded instead of embedding identifiers.
func fineGrainedProviderDigest(parts ...string) string {
	framed := make([]byte, 0, len(parts)*8)
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		framed = append(framed, size[:]...)
		framed = append(framed, part...)
	}
	sum := sha256.Sum256(framed)
	return hex.EncodeToString(sum[:])
}

// fineGrainedProviderBindingID is the record ID of one user's binding list,
// derived from the user's stable ID rather than from a mutable username.
func fineGrainedProviderBindingID(userID string) storage.NodeID {
	return storage.NodeID(fineGrainedProviderBindingPrefix + fineGrainedProviderDigest(userID))
}

// fineGrainedProviderSubjectID is the record ID of the unique lookup for one
// provider name and external subject.
func fineGrainedProviderSubjectID(provider, subject string) storage.NodeID {
	return storage.NodeID(fineGrainedProviderSubjectPrefix + fineGrainedProviderDigest(provider, subject))
}

// readFineGrainedProviderBindings returns the stored provider name to external
// subject bindings of userID, the user's stable ID. A user with no binding list
// has no bindings and gets an empty map, never nil.
//
// ctx is the lifetime of the caller's directory operation, and view is that
// operation's staged system engine.
func readFineGrainedProviderBindings(ctx context.Context, view storage.Engine, userID string) (map[string]string, error) {
	if userID == "" {
		return nil, fmt.Errorf("%w: user ID is required", errFineGrainedProviderBinding)
	}
	node, err := view.GetNode(ctx, fineGrainedProviderBindingID(userID))
	if errors.Is(err, storage.ErrNotFound) {
		return map[string]string{}, nil
	}
	if err != nil {
		return nil, err
	}
	raw, ok := node.Properties[fineGrainedProviderBindingsProperty].(string)
	providers := make(map[string]string)
	if !ok || convert.PropertyJSON.Unmarshal([]byte(raw), &providers) != nil {
		return nil, fmt.Errorf("%w: malformed stored binding list", errFineGrainedProviderBinding)
	}
	for provider, subject := range providers {
		if provider == "" || subject == "" {
			return nil, fmt.Errorf("%w: malformed stored binding list", errFineGrainedProviderBinding)
		}
	}
	return providers, nil
}

// fineGrainedProviderUserID returns the stable ID of the user that provider
// and subject are bound to. A pair with no unique lookup record returns
// storage.ErrNotFound.
func fineGrainedProviderUserID(ctx context.Context, view storage.Engine, provider, subject string) (string, error) {
	if provider == "" || subject == "" {
		return "", fmt.Errorf("%w: provider name and external subject are required", errFineGrainedProviderBinding)
	}
	node, err := view.GetNode(ctx, fineGrainedProviderSubjectID(provider, subject))
	if errors.Is(err, storage.ErrNotFound) {
		return "", storage.ErrNotFound
	}
	if err != nil {
		return "", err
	}
	userID, ok := node.Properties[fineGrainedProviderOwnerProperty].(string)
	if !ok || userID == "" {
		return "", fmt.Errorf("%w: malformed stored subject binding", errFineGrainedProviderBinding)
	}
	return userID, nil
}

// writeFineGrainedProviderBindings replaces the binding list stored for userID
// with providers, one provider name to external subject entry each. An empty
// providers removes the user's binding list.
//
// Bindings the user no longer holds are released and new ones are claimed
// before the binding list is written. A subject already bound to another user
// is refused with errFineGrainedProviderSubjectBound, and a stored lookup that
// disagrees with the binding list it is released from is reported instead of
// deleted.
//
// The caller owns admission: view is its staged system engine, so the list and
// every affected unique lookup become durable in the caller's transaction or
// not at all.
func writeFineGrainedProviderBindings(ctx context.Context, view storage.Engine, userID string, providers map[string]string) error {
	if userID == "" {
		return fmt.Errorf("%w: user ID is required", errFineGrainedProviderBinding)
	}
	desired := make(map[string]string, len(providers))
	for provider, subject := range providers {
		if provider == "" || subject == "" {
			return fmt.Errorf("%w: provider name and external subject are required", errFineGrainedProviderBinding)
		}
		desired[provider] = subject
	}
	current, err := readFineGrainedProviderBindings(ctx, view, userID)
	if err != nil {
		return err
	}
	for provider, subject := range current {
		if held, kept := desired[provider]; kept && held == subject {
			continue
		}
		if err := releaseFineGrainedProviderSubject(ctx, view, userID, provider, subject); err != nil {
			return err
		}
	}
	for provider, subject := range desired {
		if held, kept := current[provider]; kept && held == subject {
			continue
		}
		if err := claimFineGrainedProviderSubject(ctx, view, userID, provider, subject); err != nil {
			return err
		}
	}
	raw, err := convert.PropertyJSON.MarshalState(desired)
	if err != nil {
		return err
	}
	return putRoleRecord(ctx, view, &storage.Node{
		ID:         fineGrainedProviderBindingID(userID),
		Labels:     []string{"_System", fineGrainedProviderBindingsLabel},
		Properties: map[string]any{fineGrainedProviderBindingsProperty: string(raw)},
	}, len(desired) == 0)
}

// claimFineGrainedProviderSubject records that userID owns one provider name
// and external subject pair. A pair bound to another user is refused with
// errFineGrainedProviderSubjectBound; a pair this user already owns keeps its
// stored lookup unchanged.
func claimFineGrainedProviderSubject(ctx context.Context, view storage.Engine, userID, provider, subject string) error {
	bound, err := fineGrainedProviderUserID(ctx, view, provider, subject)
	if errors.Is(err, storage.ErrNotFound) {
		_, err = view.CreateNode(ctx, &storage.Node{
			ID:         fineGrainedProviderSubjectID(provider, subject),
			Labels:     []string{"_System", fineGrainedProviderSubjectLabel},
			Properties: map[string]any{fineGrainedProviderOwnerProperty: userID},
		})
		return err
	}
	if err != nil {
		return err
	}
	if bound != userID {
		return fmt.Errorf("%w: provider %q", errFineGrainedProviderSubjectBound, provider)
	}
	return nil
}

// releaseFineGrainedProviderSubject deletes the unique lookup of one provider
// name and external subject pair that userID no longer binds. A missing lookup
// or one owned by another user disagrees with the binding list the release
// comes from, so it is reported rather than deleted.
func releaseFineGrainedProviderSubject(ctx context.Context, view storage.Engine, userID, provider, subject string) error {
	bound, err := fineGrainedProviderUserID(ctx, view, provider, subject)
	if errors.Is(err, storage.ErrNotFound) {
		return fmt.Errorf("%w: subject binding is missing", errFineGrainedProviderBinding)
	}
	if err != nil {
		return err
	}
	if bound != userID {
		return fmt.Errorf("%w: subject binding belongs to another user", errFineGrainedProviderBinding)
	}
	return view.DeleteNode(ctx, fineGrainedProviderSubjectID(provider, subject))
}
