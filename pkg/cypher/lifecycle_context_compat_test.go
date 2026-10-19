package cypher

import "context"

// Test adapters retain legacy fixture behavior while satisfying the required context API.
func (m *aliasTestDatabaseManager) CreateDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateDatabase(name)
}
func (m *aliasTestDatabaseManager) DropDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropDatabase(name)
}
func (m *aliasTestDatabaseManager) CreateCompositeDatabaseContext(ctx context.Context, name string, constituents []interface{}) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateCompositeDatabase(name, constituents)
}
func (m *aliasTestDatabaseManager) DropCompositeDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropCompositeDatabase(name)
}
func (m *cypherCompositeErrorManager) CreateCompositeDatabaseContext(ctx context.Context, name string, constituents []interface{}) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateCompositeDatabase(name, constituents)
}
func (m *cypherAdminErrorManager) CreateDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateDatabase(name)
}
func (m *cypherAdminErrorManager) DropDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropDatabase(name)
}
func (m *mockDatabaseManager) CreateDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateDatabase(name)
}
func (m *mockDatabaseManager) DropDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropDatabase(name)
}
func (m *mockDatabaseManager) CreateCompositeDatabaseContext(ctx context.Context, name string, constituents []interface{}) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateCompositeDatabase(name, constituents)
}
func (m *mockDatabaseManager) DropCompositeDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropCompositeDatabase(name)
}
func (m *useAuthDBManager) CreateDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateDatabase(name)
}
func (m *useAuthDBManager) DropDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropDatabase(name)
}
func (m *useAuthDBManager) CreateCompositeDatabaseContext(ctx context.Context, name string, constituents []interface{}) error {
 if err := ctx.Err(); err != nil { return err }; return m.CreateCompositeDatabase(name, constituents)
}
func (m *useAuthDBManager) DropCompositeDatabaseContext(ctx context.Context, name string) error {
 if err := ctx.Err(); err != nil { return err }; return m.DropCompositeDatabase(name)
}
