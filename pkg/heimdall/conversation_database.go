package heimdall

import "github.com/orneryd/nornicdb/pkg/auth"

// Context-bound tools default to the database selected by the caller, rather
// than the server's global default. Reads still authorize in the host router.
type conversationDatabase struct {
	DatabaseRouter
	source  string
	canRead func(string) bool
}

func (d conversationDatabase) DefaultDatabaseName() string { return d.source }
func (d conversationDatabase) ListDatabases() []string {
	result := make([]string, 0)
	for _, name := range d.DatabaseRouter.ListDatabases() {
		if d.canRead(name) {
			result = append(result, name)
		}
	}
	return result
}
func (d conversationDatabase) ResolveDatabase(name string) (string, error) {
	if name == "" {
		name = d.source
	}
	return d.DatabaseRouter.ResolveDatabase(name)
}
func newConversationDatabase(base DatabaseRouter, prompt *PromptContext) DatabaseRouter {
	source := prompt.SourceDatabase
	if scope := auth.RequestDatabaseScopeFromContext(prompt.Context); scope != nil {
		if canonical, ok := scope.Resolve(""); ok {
			source = canonical
		}
	}
	return conversationDatabase{DatabaseRouter: base, source: source, canRead: func(database string) bool {
		return auth.RequestDatabasePermissionAllowed(prompt.Context, database, auth.PermRead)
	}}
}
