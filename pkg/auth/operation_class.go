package auth

// OperationClass identifies the privilege contract an exposed operation declares.
// It describes effects independently of protocol or procedure display modes.
// Effects compose: reading data while writing a file declares both. The zero
// value is invalid; value-only operations explicitly declare None.
type OperationClass uint16

const (
	OperationNone OperationClass = 1 << iota
	OperationRead
	OperationWrite
	OperationSchema
	OperationAdmin
	OperationDBMS
	OperationSharedState
	OperationFileIO
	OperationRemote
	// OperationUnrestrictedView reads storage around a principal's graph
	// view (collection rules), so a principal restricted on the database
	// is refused it.
	OperationUnrestrictedView
)

// Valid reports whether c is an explicit supported contract class.
func (c OperationClass) Valid() bool {
	const known = OperationNone | OperationRead | OperationWrite | OperationSchema |
		OperationAdmin | OperationDBMS | OperationSharedState | OperationFileIO | OperationRemote |
		OperationUnrestrictedView
	return c != 0 && c & ^known == 0 && (c == OperationNone || c&OperationNone == 0)
}

// Has reports whether the contract declares any of the supplied effects.
func (c OperationClass) Has(effects OperationClass) bool { return c&effects != 0 }
