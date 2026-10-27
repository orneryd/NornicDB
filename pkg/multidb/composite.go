// Package multidb provides composite database support for multi-database functionality.
//
// Composite databases are virtual databases that span multiple physical databases,
// allowing queries to transparently access data from multiple constituent databases.
package multidb

import (
	"context"
	"iter"
	"slices"
	"strings"
	"time"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/localization"
	"github.com/orneryd/nornicdb/pkg/storage"
)

// RemoteEngineFactory creates storage engines for remote composite constituents.
// authToken is the original caller's auth token/header value, forwarded to preserve
// authentication context across distributed constituent queries.
type RemoteEngineFactory func(ref ConstituentRef, authToken string) (storage.Engine, error)

// ConstituentRef represents a reference to a constituent database within a composite database.
type ConstituentRef struct {
	// ConnectionV1 is the sole stored connection authority for the tagged V1
	// format. Its ciphertext contains the original URI, credentials and origin;
	// legacy URI/User/Password/AuthMode fields are absent in this representation.
	ConnectionV1 string `json:"connection_v1,omitempty"`

	// Alias is the name used within the composite database to reference this constituent.
	Alias string `json:"alias"`

	// DatabaseName is the actual database name (or alias) that this constituent points to.
	DatabaseName string `json:"database_name"`

	// Type is the type of constituent: "local" (same instance) or "remote" (another instance).
	Type string `json:"type"` // "local", "remote"

	// AccessMode controls what operations are allowed: "read", "write", "read_write".
	AccessMode string `json:"access_mode"` // "read", "write", "read_write"

	// URI points to the remote NornicDB endpoint when Type == "remote".
	URI string `json:"uri,omitempty"`

	// SecretRef identifies credentials/token material for remote access.
	// The actual secret is resolved outside of metadata persistence.
	SecretRef string `json:"secret_ref,omitempty"`

	// User and Password carry explicit authentication at admission/runtime.
	// Legacy catalogs store Password in its historical encrypted field. Tagged
	// catalogs store the original URI and these values only inside ConnectionV1;
	// URI-origin fallback credentials are derived at runtime, retaining token
	// precedence, rather than changing the meaning of persisted USER fields.
	User     string `json:"user,omitempty"`
	Password string `json:"password,omitempty"`

	// AuthMode defines remote auth behavior:
	// - "oidc_forwarding": forward caller Authorization header
	// - "user_password": use explicit User/Password for outbound Basic auth
	// Empty is treated as "oidc_forwarding" for remote constituents.
	AuthMode string `json:"auth_mode,omitempty"`
}

// Validate validates a constituent reference.
func (c *ConstituentRef) Validate() error {
	if c.Alias == "" {
		return localizedError(localization.MultidbConstituentAliasRequired(), nil)
	}
	if c.DatabaseName == "" {
		return localizedError(localization.MultidbConstituentDatabaseRequired(), nil)
	}
	if c.Type != "local" && c.Type != "remote" {
		return localizedError(localization.MultidbConstituentTypeInvalid(), nil)
	}
	if c.AccessMode != "read" && c.AccessMode != "write" && c.AccessMode != "read_write" {
		return localizedError(localization.MultidbAccessModeInvalid(), nil)
	}
	if c.Type == "remote" && c.URI == "" {
		return localizedError(localization.MultidbRemoteURIRequired(), nil)
	}
	if c.Type == "remote" {
		mode := strings.ToLower(strings.TrimSpace(c.AuthMode))
		if mode == "" {
			mode = "oidc_forwarding"
		}
		if mode != "oidc_forwarding" && mode != "user_password" {
			return localizedError(localization.MultidbRemoteAuthModeInvalid(), nil)
		}
		if mode == "user_password" {
			if strings.TrimSpace(c.User) == "" {
				return localizedError(localization.MultidbRemoteUserRequired(), nil)
			}
			if strings.TrimSpace(c.Password) == "" {
				return localizedError(localization.MultidbRemotePasswordRequired(), nil)
			}
		}
		if mode == "oidc_forwarding" {
			if strings.TrimSpace(c.User) != "" || strings.TrimSpace(c.Password) != "" {
				return localizedError(localization.MultidbOIDCCredentialsForbidden(), nil)
			}
		}
	}
	return nil
}

// CreateCompositeDatabase creates a new composite database.
func (m *DatabaseManager) CreateCompositeDatabase(ctx context.Context, name string, constituents []ConstituentRef) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		// Validate name
		if name == "" {
			return ErrInvalidDatabaseName
		}

		// Check if database already exists
		if _, exists := m.databases[name]; exists || m.pendingDrops[name] {
			return ErrDatabaseExists
		}

		// Validate all constituents
		for i, ref := range constituents {
			if err := ref.Validate(); err != nil {
				return localizedError(localization.MultidbInvalidConstituent(i, err), err)
			}

			if ref.Type == "local" {
				// Check if constituent database exists
				// Resolve alias if needed
				actualName, err := m.resolveDatabaseInternal(ref.DatabaseName)
				if err != nil {
					return localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(ref.DatabaseName, err), err)
				}

				// Cannot use composite database as constituent (prevent cycles)
				if info, exists := m.databases[actualName]; exists && info.Type == "composite" {
					return localizedError(localization.MultidbCompositeDatabaseAsConstituent(actualName), nil)
				}
			}
		}

		// Check for duplicate aliases
		aliasMap := make(map[string]bool)
		for _, ref := range constituents {
			if aliasMap[ref.Alias] {
				return localizedError(localization.MultidbCompositeDuplicateAlias(ref.Alias), nil)
			}
			aliasMap[ref.Alias] = true
		}

		// Prepare only new input; existing catalog records are never migrated here.
		encrypted := make([]ConstituentRef, len(constituents))
		copy(encrypted, constituents)
		for i := range encrypted {
			ref, err := m.prepareRemoteConnectionInput(encrypted[i])
			if err != nil {
				return err
			}
			encrypted[i] = ref
		}

		// Create composite database info
		m.databases[name] = &DatabaseInfo{
			Name:         name,
			CreatedAt:    time.Now(),
			Status:       "online",
			Type:         "composite",
			IsDefault:    false,
			UpdatedAt:    time.Now(),
			Constituents: encrypted,
		}

		return nil

	})
}

// DropCompositeDatabase removes a composite database.
//
// This only removes the composite database metadata. The constituent databases
// remain unchanged.
func (m *DatabaseManager) DropCompositeDatabase(ctx context.Context, name string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		info, exists := m.databases[name]
		if !exists {
			return ErrDatabaseNotFound
		}

		if info.Type != "composite" {
			return localizedError(localization.MultidbCompositeDatabaseNotComposite(name), nil)
		}

		// Remove from metadata
		delete(m.databases, name)
		return nil

	})
}

// AddConstituent adds a constituent to an existing composite database.
func (m *DatabaseManager) AddConstituent(ctx context.Context, compositeName string, constituent ConstituentRef) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		info, exists := m.databases[compositeName]
		if !exists {
			return ErrDatabaseNotFound
		}

		if info.Type != "composite" {
			return localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
		}

		// Validate constituent
		if err := constituent.Validate(); err != nil {
			return err
		}

		// Check if constituent database exists for local constituents
		if constituent.Type == "local" {
			_, err := m.resolveDatabaseInternal(constituent.DatabaseName)
			if err != nil {
				return localizedError(localization.MultidbCompositeConstituentDatabaseNotFound(constituent.DatabaseName, err), err)
			}
		}

		// Check for duplicate alias
		for _, existing := range info.Constituents {
			if existing.Alias == constituent.Alias {
				return localizedError(localization.MultidbCompositeAliasExists(constituent.Alias), nil)
			}
		}

		encrypted, err := m.prepareRemoteConnectionInput(constituent)
		if err != nil {
			return err
		}

		// Add constituent
		info.Constituents = append(info.Constituents, encrypted)
		info.UpdatedAt = time.Now()

		return nil

	})
}

// RemoveConstituent removes a constituent from a composite database.
func (m *DatabaseManager) RemoveConstituent(ctx context.Context, compositeName string, alias string) error {
	return m.transitionCatalog(ctx, catalogTransitionPolicy{}, func(m *DatabaseManager) error {

		info, exists := m.databases[compositeName]
		if !exists {
			return ErrDatabaseNotFound
		}

		if info.Type != "composite" {
			return localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
		}

		// Remove the selected registration, using the same folded last-winner
		// rule as catalog, execution and admission. Earlier registrations remain
		// available if the winning registration is explicitly removed.
		if i, _, found := findCanonicalConstituent(info.Constituents, alias); found {
			info.Constituents = slices.Delete(info.Constituents, i, i+1)
			info.UpdatedAt = time.Now()
			return nil
		}

		return localizedError(localization.MultidbCompositeAliasNotFound(alias), nil)

	})
}

// GetCompositeConstituents returns stored registration snapshots for the visible
// aliases. It never decrypts credentials. Resolve execution with GetStorage and
// project metadata with RemoteConstituentForDisplay; tagged URI/User/Password
// fields are absent because ConnectionV1 is their sole stored authority.
func (m *DatabaseManager) GetCompositeConstituents(compositeName string) ([]ConstituentRef, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[compositeName]
	if !exists {
		return nil, ErrDatabaseNotFound
	}

	if info.Type != "composite" {
		return nil, localizedError(localization.MultidbCompositeDatabaseNotComposite(compositeName), nil)
	}

	return snapshotConstituents(info.Constituents), nil
}

// canonicalConstituents is the shared catalog/manager reader of graph aliases.
// Preserve the catalog's last-registration-wins rule for case-folded duplicates.
// Walk backward so a single lookup can stop at its winner without copying the
// catalog. Empty wanted enumerates all winners; a lookup yields only its last
// matching alias and needs no duplicate-tracking map. Keys use the catalog's
// exact case normalization.
func canonicalConstituents(refs []ConstituentRef, wanted string) iter.Seq2[string, ConstituentRef] {
	return func(yield func(string, ConstituentRef) bool) {
		var seen map[string]struct{}
		if wanted == "" {
			seen = make(map[string]struct{}, len(refs))
		}
		for i := len(refs) - 1; i >= 0; i-- {
			ref := refs[i]
			key := cyphertext.NameLower(ref.Alias)
			if wanted != "" {
				if !constituentAliasMatches(key, wanted) {
					continue
				}
			} else {
				if _, exists := seen[key]; exists {
					continue
				}
				seen[key] = struct{}{}
			}
			if !yield(key, ref) || wanted != "" {
				return
			}
		}
	}
}

// constituentAliasMatches compares against the catalog's lowercase key. ASCII
// lookups avoid allocating another lowercase string; non-ASCII input retains
// strings.ToLower semantics, including distinctions broader EqualFold erases.
func constituentAliasMatches(key, alias string) bool {
	if len(key) != len(alias) {
		return key == cyphertext.NameLower(alias)
	}
	for i := 0; i < len(alias); i++ {
		c := alias[i]
		if c >= 128 {
			return key == cyphertext.NameLower(alias)
		}
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		if key[i] != c {
			return false
		}
	}
	return true
}

// constituentAliasValuesMatch compares raw alias values without allocating on
// the common ASCII path. The Unicode fallback matches the catalog's ToLower
// normalization exactly.
func constituentAliasValuesMatch(stored, wanted string) bool {
	if len(stored) != len(wanted) {
		for i := 0; i < len(stored); i++ {
			if stored[i] >= 0x80 {
				return cyphertext.NameLower(stored) == cyphertext.NameLower(wanted)
			}
		}
		for i := 0; i < len(wanted); i++ {
			if wanted[i] >= 0x80 {
				return cyphertext.NameLower(stored) == cyphertext.NameLower(wanted)
			}
		}
		// ASCII case folding never changes a string's byte length.
		return false
	}
	for i := 0; i < len(stored); i++ {
		a, b := stored[i], wanted[i]
		if a >= 128 || b >= 128 {
			return cyphertext.NameLower(stored) == cyphertext.NameLower(wanted)
		}
		if a >= 'A' && a <= 'Z' {
			a += 'a' - 'A'
		}
		if b >= 'A' && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return false
		}
	}
	return true
}

// findCanonicalConstituent returns the last registered case-folded alias and
// its index without building an iterator or normalized key strings.
func findCanonicalConstituent(refs []ConstituentRef, wanted string) (int, ConstituentRef, bool) {
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]
		if constituentAliasValuesMatch(ref.Alias, wanted) {
			return i, ref, true
		}
	}
	return -1, ConstituentRef{}, false
}

// snapshotConstituents returns visible winners in their stored order for
// public metadata/catalog readers. Full admitted-state persistence uses the
// full purpose of cloneDatabaseInfo instead.
func snapshotConstituents(refs []ConstituentRef) []ConstituentRef {
	if refs == nil {
		return nil
	}
	result := make([]ConstituentRef, 0, len(refs))
	for _, ref := range canonicalConstituents(refs, "") {
		result = append(result, ref)
	}
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}
	return result
}

// ListCompositeDatabases returns all composite databases.
func (m *DatabaseManager) ListCompositeDatabases() []*DatabaseInfo {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var result []*DatabaseInfo
	for _, info := range m.databases {
		if info.Type == "composite" {
			result = append(result, cloneDatabaseInfo(info, m.pendingDrops[info.Name], databaseSnapshotVisible))
		}
	}
	return result
}

// IsCompositeDatabase checks if a database is a composite database.
func (m *DatabaseManager) IsCompositeDatabase(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	info, exists := m.databases[name]
	return exists && info.Type == "composite"
}

// ExistsOrIsConstituent returns true if the name refers to an existing database
// or a valid composite constituent in dotted "composite.alias" form.
// This is used by protocol entry paths (Bolt/HTTP) to avoid premature
// DB-not-found rejection for constituent graph references.
func (m *DatabaseManager) ExistsOrIsConstituent(name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Direct database match.
	if m.databases[name] != nil {
		return true
	}

	// Check alias match.
	for _, info := range m.databases {
		for _, alias := range info.Aliases {
			if alias == name {
				return true
			}
		}
	}

	// Dotted composite.alias form.
	if dotIdx := strings.IndexByte(name, '.'); dotIdx > 0 && dotIdx < len(name)-1 {
		compositeName := name[:dotIdx]
		constituentAlias := name[dotIdx+1:]
		info, exists := m.databases[compositeName]
		if exists && info.Type == "composite" {
			for range canonicalConstituents(info.Constituents, constituentAlias) {
				return true
			}
		}
	}

	return false
}

// resolveDatabaseInternal resolves a database name or alias to the actual database name.
// Must be called with lock held.
func (m *DatabaseManager) resolveDatabaseInternal(nameOrAlias string) (string, error) {
	// Check if it's an actual database name
	if _, exists := m.databases[nameOrAlias]; exists {
		return nameOrAlias, nil
	}

	// Check if it's an alias
	for dbName, info := range m.databases {
		for _, alias := range info.Aliases {
			if alias == nameOrAlias {
				return dbName, nil
			}
		}
	}

	return "", ErrDatabaseNotFound
}
