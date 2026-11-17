package bolt

import (
	"fmt"
)

// requestedAccessMode is the access mode a Bolt message asked for in its "mode"
// option, as Neo4j's drivers send it: "r" for a read request, "w" (or no
// option) for a write one. A READ request restricts the statement whatever the
// principal's grants and the database's own configuration allow; it never
// widens them.
type requestedAccessMode uint8

const (
	// accessModeUnconstrained leaves the statement to the principal's grants
	// and the database's configuration.
	accessModeUnconstrained requestedAccessMode = iota
	// accessModeRead refuses every statement whose effects are not read-only
	// with Neo.ClientError.Statement.AccessMode, as Neo4j 5.26 does.
	accessModeRead
)

// accessModeFromMetadata reads a RUN or BEGIN message's "mode" option. An
// absent option or "w" is a write request; "r" is a read request. A value Neo4j
// does not define is not an access mode: the message is invalid.
func accessModeFromMetadata(metadata map[string]any) (requestedAccessMode, bool) {
	raw, present := metadata["mode"]
	if !present {
		return accessModeUnconstrained, true
	}
	value, isText := raw.(string)
	if !isText {
		return accessModeUnconstrained, false
	}
	switch value {
	case "r":
		return accessModeRead, true
	case "w":
		return accessModeUnconstrained, true
	}
	return accessModeUnconstrained, false
}

// invalidRequestedAccessMode is the diagnostic for a "mode" value that is not
// an access mode. The message owner reports it as
// Neo.ClientError.Request.Invalid, as Neo4j does for malformed message options.
func invalidRequestedAccessMode(metadata map[string]any) error {
	return fmt.Errorf("invalid transaction mode: %v", metadata["mode"])
}

// statementAccessMode is the access mode restricting one RUN: the explicit
// transaction's BEGIN mode and the RUN's own option, whichever restricts more,
// so a message can only narrow the transaction it runs in.
func (s *Session) statementAccessMode(runMode requestedAccessMode) requestedAccessMode {
	if s.inTransaction && s.txAccessMode == accessModeRead {
		return accessModeRead
	}
	return runMode
}
