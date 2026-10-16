package cypher

import (
	"context"
	"testing"

	"github.com/orneryd/nornicdb/pkg/auth"
	"github.com/stretchr/testify/require"
)

func TestPrivilegeAdministrationRejectsUnimplementedGrantForms(t *testing.T) {
	executor, system := newImpersonationExecutor(t)
	for _, query := range []string{
		"GRANT EXECUTE PROCEDURE db.ping ON DBMS TO svc",
		"GRANT EXECUTE PROCEDURES * ON DBMS TO svc",
		"REVOKE EXECUTE PROCEDURE db.ping ON DBMS FROM svc",
		"REVOKE GRANT EXECUTE PROCEDURES * ON DBMS FROM svc",
		"DENY EXECUTE PROCEDURE db.ping ON DBMS TO svc",
		"GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO svc",
		"REVOKE EXECUTE ADMIN PROCEDURES ON DBMS FROM svc",
		"GRANT EXECUTE FUNCTION sin ON DBMS TO svc",
		"GRANT EXECUTE BOOSTED FUNCTIONS * ON DBMS TO svc",
		"DENY EXECUTE FUNCTION sin ON DBMS TO svc",
		"REVOKE EXECUTE FUNCTION sin ON DBMS FROM svc",
		"GRANT SHOW SETTING db.* ON DBMS TO svc",
		"DENY SHOW SETTING db.* ON DBMS TO svc",
		"REVOKE SHOW SETTING db.* ON DBMS FROM svc",
	} {
		t.Run(query, func(t *testing.T) {
			_, err := executor.Execute(context.Background(), query, nil)
			requireStatusCode(t, err, "Neo.ClientError.Statement.UnsupportedAdministrationCommand")
		})
	}
	grants, err := auth.NewProcedurePrivilegeStore(system).List(context.Background())
	require.NoError(t, err)
	require.Empty(t, grants, "unsupported configuration never records a grant")
}
