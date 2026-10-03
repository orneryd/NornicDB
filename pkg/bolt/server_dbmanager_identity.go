package bolt

import (
	"fmt"
	"time"

	"github.com/orneryd/nornicdb/pkg/cypher"
	"github.com/orneryd/nornicdb/pkg/multidb"
)

type boltDatabaseManagerAdapter struct {
	manager *multidb.DatabaseManager
}

func (a *boltDatabaseManagerAdapter) CreateDatabase(name string) error {
	return a.manager.CreateDatabase(name)
}
func (a *boltDatabaseManagerAdapter) DropDatabase(name string) error {
	return a.manager.DropDatabase(name)
}
func (a *boltDatabaseManagerAdapter) Exists(name string) bool { return a.manager.Exists(name) }
func (a *boltDatabaseManagerAdapter) CreateAlias(alias, databaseName string) error {
	return a.manager.CreateAlias(alias, databaseName)
}
func (a *boltDatabaseManagerAdapter) DropAlias(alias string) error {
	return a.manager.DropAlias(alias)
}
func (a *boltDatabaseManagerAdapter) ListAliases(databaseName string) map[string]string {
	return a.manager.ListAliases(databaseName)
}
func (a *boltDatabaseManagerAdapter) ResolveDatabase(nameOrAlias string) (string, error) {
	return a.manager.ResolveDatabase(nameOrAlias)
}
func (a *boltDatabaseManagerAdapter) SetDatabaseLimits(databaseName string, limits interface{}) error {
	limitsPtr, ok := limits.(*multidb.Limits)
	if !ok {
		return fmt.Errorf("invalid limits type")
	}
	return a.manager.SetDatabaseLimits(databaseName, limitsPtr)
}
func (a *boltDatabaseManagerAdapter) GetDatabaseLimits(databaseName string) (interface{}, error) {
	return a.manager.GetDatabaseLimits(databaseName)
}
func (a *boltDatabaseManagerAdapter) CreateCompositeDatabase(name string, constituents []interface{}) error {
	refs := make([]multidb.ConstituentRef, len(constituents))
	for i, c := range constituents {
		ref, ok := c.(multidb.ConstituentRef)
		if !ok {
			if m, ok := c.(map[string]interface{}); ok {
				ref = multidb.ConstituentRef{
					Alias:        getStringFromMap(m, "alias"),
					DatabaseName: getStringFromMap(m, "database_name"),
					Type:         getStringFromMap(m, "type"),
					AccessMode:   getStringFromMap(m, "access_mode"),
					URI:          getStringFromMap(m, "uri"),
					SecretRef:    getStringFromMap(m, "secret_ref"),
					AuthMode:     getStringFromMap(m, "auth_mode"),
					User:         getStringFromMap(m, "user"),
					Password:     getStringFromMap(m, "password"),
				}
			} else {
				return fmt.Errorf("invalid constituent type at index %d", i)
			}
		}
		refs[i] = ref
	}
	return a.manager.CreateCompositeDatabase(name, refs)
}
func (a *boltDatabaseManagerAdapter) DropCompositeDatabase(name string) error {
	return a.manager.DropCompositeDatabase(name)
}
func (a *boltDatabaseManagerAdapter) AddConstituent(compositeName string, constituent interface{}) error {
	if m, ok := constituent.(map[string]interface{}); ok {
		return a.manager.AddConstituent(compositeName, multidb.ConstituentRef{
			Alias:        getStringFromMap(m, "alias"),
			DatabaseName: getStringFromMap(m, "database_name"),
			Type:         getStringFromMap(m, "type"),
			AccessMode:   getStringFromMap(m, "access_mode"),
			URI:          getStringFromMap(m, "uri"),
			SecretRef:    getStringFromMap(m, "secret_ref"),
			AuthMode:     getStringFromMap(m, "auth_mode"),
			User:         getStringFromMap(m, "user"),
			Password:     getStringFromMap(m, "password"),
		})
	}
	ref, ok := constituent.(multidb.ConstituentRef)
	if !ok {
		return fmt.Errorf("invalid constituent type")
	}
	return a.manager.AddConstituent(compositeName, ref)
}
func (a *boltDatabaseManagerAdapter) RemoveConstituent(compositeName string, alias string) error {
	return a.manager.RemoveConstituent(compositeName, alias)
}
func (a *boltDatabaseManagerAdapter) GetCompositeConstituents(compositeName string) ([]interface{}, error) {
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
func (a *boltDatabaseManagerAdapter) ListDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &boltDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *boltDatabaseManagerAdapter) ListCompositeDatabases() []cypher.DatabaseInfoInterface {
	dbs := a.manager.ListCompositeDatabases()
	out := make([]cypher.DatabaseInfoInterface, len(dbs))
	for i, db := range dbs {
		out[i] = &boltDatabaseInfoAdapter{info: db}
	}
	return out
}
func (a *boltDatabaseManagerAdapter) IsCompositeDatabase(name string) bool {
	return a.manager.IsCompositeDatabase(name)
}
func (a *boltDatabaseManagerAdapter) GetStorageForUse(name string, authToken string) (interface{}, error) {
	return a.manager.GetStorageWithAuth(name, authToken)
}

type boltDatabaseInfoAdapter struct {
	info *multidb.DatabaseInfo
}

func (a *boltDatabaseInfoAdapter) Name() string         { return a.info.Name }
func (a *boltDatabaseInfoAdapter) Type() string         { return a.info.Type }
func (a *boltDatabaseInfoAdapter) Status() string       { return a.info.Status }
func (a *boltDatabaseInfoAdapter) IsDefault() bool      { return a.info.IsDefault }
func (a *boltDatabaseInfoAdapter) CreatedAt() time.Time { return a.info.CreatedAt }

func getStringFromMap(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func (a *boltDatabaseManagerAdapter) ServerIdentity() (string, time.Time) {
	return a.manager.ServerIdentity()
}

func (a *boltDatabaseManagerAdapter) DatabaseIdentity(name string) (string, time.Time) {
	return a.manager.DatabaseIdentity(name)
}

func (a *boltDatabaseManagerAdapter) SystemDatabaseName() string {
	return a.manager.SystemDatabaseName()
}
