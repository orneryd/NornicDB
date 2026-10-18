package resolvers

import (
	"context"
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

// graphqlDatabaseManagerAdapter wraps multidb.DatabaseManager to satisfy
// cypher.DatabaseManagerInterface for GraphQL executor paths.
type graphqlDatabaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *graphqlDatabaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}
func (a *graphqlDatabaseManagerAdapter) DropDatabase(name string) error {
	return a.manager.DropDatabase(name)
}
func (a *graphqlDatabaseManagerAdapter) DropDatabaseContext(ctx context.Context, name string) error {
	return a.manager.DropDatabaseContext(ctx, name)
}
func (a *graphqlDatabaseManagerAdapter) Exists(name string) bool { return a.manager.Exists(name) }
func (a *graphqlDatabaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}
func (a *graphqlDatabaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}
func (a *graphqlDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}
func (a *graphqlDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}
func (a *graphqlDatabaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}
func (a *graphqlDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}
func (a *graphqlDatabaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        adapterString(m, "alias"),
					DatabaseName: adapterString(m, "database_name"),
					Type:         adapterString(m, "type"),
					AccessMode:   adapterString(m, "access_mode"),
					URI:          adapterString(m, "uri"),
					SecretRef:    adapterString(m, "secret_ref"),
					AuthMode:     adapterString(m, "auth_mode"),
					User:         adapterString(m, "user"),
					Password:     adapterString(m, "password"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}
func (a *graphqlDatabaseManagerAdapter) DropCompositeDatabase(name string) error {
	return a.manager.DropCompositeDatabase(name)
}
func (a *graphqlDatabaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	if m, ok := constituent.(map[string]interface{}); ok {
		return a.manager.AddConstituent(compositeName, multidb.ConstituentRef{
			Alias:        adapterString(m, "alias"),
			DatabaseName: adapterString(m, "database_name"),
			Type:         adapterString(m, "type"),
			AccessMode:   adapterString(m, "access_mode"),
			URI:          adapterString(m, "uri"),
			SecretRef:    adapterString(m, "secret_ref"),
			AuthMode:     adapterString(m, "auth_mode"),
			User:         adapterString(m, "user"),
			Password:     adapterString(m, "password"),
		})
	}
	ref, ok := constituent.(multidb.ConstituentRef)
	if !ok {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}
func (a *graphqlDatabaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}
func (a *graphqlDatabaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
	cons, err := a.manager.GetCompositeConstituents(compositeName)
	if err != nil {
		return nil, err
	}
	out := make([]interface{}, len(cons))
	for i, c := range cons {
		out[i] = c
	}
	return out, nil
}
func (a *graphqlDatabaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &graphqlDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *graphqlDatabaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &graphqlDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *graphqlDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}
func (a *graphqlDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

type graphqlDatabaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *graphqlDatabaseInfoAdapter) Name() string         { return a.info.Name }
func (a *graphqlDatabaseInfoAdapter) Type() string         { return a.info.Type }
func (a *graphqlDatabaseInfoAdapter) Status() string       { return a.info.Status }
func (a *graphqlDatabaseInfoAdapter) IsDefault() bool      { return a.info.IsDefault }
func (a *graphqlDatabaseInfoAdapter) CreatedAt() time.Time { return a.info.CreatedAt }

func adapterString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
