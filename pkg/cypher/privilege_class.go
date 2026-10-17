package cypher

import "github.com/orneryd/nornicdb/pkg/auth"

// PrivilegeClass is the required effect contract of a registered callable.
// The type is shared with function/plugin descriptors; policy lives here.
type PrivilegeClass = auth.OperationClass

const (
	PrivilegeNone        = auth.OperationNone
	PrivilegeRead        = auth.OperationRead
	PrivilegeWrite       = auth.OperationWrite
	PrivilegeSchema      = auth.OperationSchema
	PrivilegeAdmin       = auth.OperationAdmin
	PrivilegeDBMS        = auth.OperationDBMS
	PrivilegeSharedState = auth.OperationSharedState
	PrivilegeFileIO      = auth.OperationFileIO
	PrivilegeRemote      = auth.OperationRemote
)

// requirementsForClass is the common mapping from a declared effect to the
// currently supported entitlements. Resolved file output adds write through
// this same policy before output; file input and provider calls retain their
// configured feature gates. Remote database impersonation is a selection rule.
func requirementsForClass(class PrivilegeClass) PermissionRequirements {
	if !class.Valid() {
		return PermissionRequirements{Unclassified: true}
	}
	return PermissionRequirements{
		Read:        class.Has(PrivilegeRead),
		Write:       class.Has(PrivilegeWrite),
		Schema:      class.Has(PrivilegeSchema),
		Admin:       class.Has(PrivilegeAdmin | PrivilegeDBMS | PrivilegeSharedState),
		UpdatesDBMS: class.Has(PrivilegeDBMS),
	}
}

// PrivilegeClassForMode converts an explicitly declared extension mode. An
// absent or unknown mode yields an invalid class and cannot be registered.
func PrivilegeClassForMode(mode ProcedureMode) PrivilegeClass {
	switch mode {
	case ProcedureModeRead:
		return PrivilegeRead
	case ProcedureModeWrite:
		return PrivilegeRead | PrivilegeWrite
	case ProcedureModeSchema:
		return PrivilegeSchema
	case ProcedureModeDBMS:
		return PrivilegeAdmin
	case ProcedureModeAdmin:
		return PrivilegeDBMS
	default:
		return 0
	}
}
