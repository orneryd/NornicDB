package fn

import "github.com/orneryd/nornicdb/pkg/auth"

// PrivilegeClass is shared metadata. The parent Cypher authorizer owns policy.
type PrivilegeClass = auth.OperationClass

const (
	PrivilegeNone  = auth.OperationNone
	PrivilegeRead  = auth.OperationRead
	PrivilegeWrite = auth.OperationWrite
)
