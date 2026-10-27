package multidb

import (
	"context"
	"errors"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"net/url"
	"strings"

	"github.com/orneryd/nornicdb/pkg/observability"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// NewCatalogUpgradeV4 prepares the one configured catalog's pure credential
// transformer. Native storage owns admission, atomic writes and the V4 marker.
// An absent key is permitted only when no connection requires encryption.
// For example, bootstrap passes this plan before opening an authorized upgrade.
// Durable custom engines must explicitly apply this transform through their
// authoritative upgrade owner before manager construction. For native storage,
// supply the plan to an authorized startup open: a live manager retains a catalog
// snapshot, so a manual migration cannot safely replace that snapshot. Existing
// nonempty native stores require an explicit plan even when its exact configured
// catalog record is absent; storage does not guess arbitrary catalog namespaces.
// Manager load/save
// never performs a credential migration. Use the same stable key at runtime;
// the plan certifies only this configured catalog, not arbitrary metadata nodes.
func NewCatalogUpgradeV4(systemDatabase, key string) (*storage.CatalogUpgradeV4, error) {
	if systemDatabase == "" {
		systemDatabase = "system"
	}
	manager := &DatabaseManager{}
	if key = strings.TrimSpace(key); key != "" {
		cipher, err := newRemoteCredentialCipher(key)
		if err != nil {
			return nil, err
		}
		manager.remoteCredentialCipher = cipher
	}
	return &storage.CatalogUpgradeV4{
		NodeID:    storage.NewNamespacedEngine(nil, systemDatabase).PrefixNodeID(storage.NodeID(metadataNodeID)),
		Transform: manager.transformCatalogV3Credentials,
	}, nil
}

// remoteConnectionNeedsDescriptor classifies credential roles through the
// existing URL owner, without changing any old URI bytes or cipher semantics.
func remoteConnectionNeedsDescriptor(ref ConstituentRef) bool {
	if ref.ConnectionV1 != "" || ref.Password != "" || ref.User != "" {
		return true
	}
	endpoint, err := url.Parse(ref.URI)
	if err != nil {
		return true
	} // Preserve opaque legacy input inside the descriptor.
	needed := false
	err = observability.MapURLCredentials(endpoint, func(user *url.Userinfo) *url.Userinfo { needed = true; return user }, func(value string, decodeErr error) (string, error) { needed = true; return value, decodeErr })
	return needed || err != nil
}

func (m *DatabaseManager) transformCatalogV3Credentials(ctx context.Context, data string) (string, bool, error) {
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	var catalog map[string]jsontext.RawMessage
	if err := jsontext.Unmarshal([]byte(data), &catalog); err != nil {
		return "", false, err
	}
	changed := false
	for name, raw := range catalog {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		var database map[string]jsontext.RawMessage
		if err := jsontext.Unmarshal(raw, &database); err != nil {
			return "", false, err
		}
		constituents, exists := database["constituents"]
		if !exists {
			continue
		}
		var refs []jsontext.RawMessage
		if err := jsontext.Unmarshal(constituents, &refs); err != nil {
			return "", false, err
		}
		databaseChanged := false
		for i, encoded := range refs {
			if err := ctx.Err(); err != nil {
				return "", false, err
			}
			var ref ConstituentRef
			if err := jsontext.Unmarshal(encoded, &ref); err != nil {
				return "", false, err
			}
			if ref.Type != "remote" {
				continue
			}
			var fields map[string]jsontext.RawMessage
			if err := jsontext.Unmarshal(encoded, &fields); err != nil {
				return "", false, err
			}
			if _, tagged := fields["connection_v1"]; tagged {
				if ref.ConnectionV1 == "" {
					return "", false, errors.New("empty remote connection descriptor")
				}
				if _, err := m.openRemoteConnection(ref); err != nil {
					return "", false, err
				}
				continue
			}
			if !remoteConnectionNeedsDescriptor(ref) {
				continue
			}
			secured, err := m.sealLegacyRemoteConnection(ref)
			if err != nil {
				return "", false, err
			}
			ciphertext, err := jsontext.Marshal(secured.ConnectionV1)
			if err != nil {
				return "", false, err
			}
			// Every other stored field, including unknown fields and exact names,
			// retains its original raw JSON value. There is only one connection authority.
			for _, field := range []string{"uri", "user", "password", "auth_mode"} {
				delete(fields, field)
			}
			fields["connection_v1"] = ciphertext
			refs[i], err = jsontext.Marshal(fields)
			if err != nil {
				return "", false, err
			}
			databaseChanged = true
		}
		if databaseChanged {
			encoded, err := jsontext.Marshal(refs)
			if err != nil {
				return "", false, err
			}
			database["constituents"] = encoded
			catalog[name], err = jsontext.Marshal(database)
			if err != nil {
				return "", false, err
			}
			changed = true
		}
	}
	if err := ctx.Err(); err != nil {
		return "", false, err
	}
	if !changed {
		return data, false, nil
	}
	encoded, err := jsontext.Marshal(catalog)
	if err != nil {
		return "", false, err
	}
	return string(encoded), true, nil
}
