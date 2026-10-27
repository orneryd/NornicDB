package multidb

import "github.com/orneryd/nornicdb/pkg/observability"

// RemoteConstituentForDisplay projects a stored registration without exposing
// its connection authority. Execution and catalog/cache identity must continue
// to use the original stored registration. For example, SHOW CONSTITUENTS can
// display its URI/public explicit USER without receiving PASSWORD or ciphertext.
func (m *DatabaseManager) RemoteConstituentForDisplay(ref ConstituentRef) (ConstituentRef, error) {
	if ref.ConnectionV1 == "" {
		ref.URI = observability.RedactText(ref.URI, observability.ClientCredentials(ref.URI, ref.Password)...)
		ref.User = RemoteUserForDisplay(ref.User, ref.AuthMode)
		ref.Password = ""
		return ref, nil
	}
	stored := ref
	connection, err := m.openRemoteConnection(ref)
	if err != nil {
		return ConstituentRef{}, err
	}
	ref.URI = observability.RedactText(connection.uri, observability.ClientCredentials(connection.uri, connection.password)...)
	ref.AuthMode = connection.authMode
	ref.User = ""
	if connection.origin == remoteConnectionExplicit {
		ref.User = connection.user
	} else {
		runtime, err := m.resolveRemoteCredentials(stored, "")
		if err != nil {
			return ConstituentRef{}, err
		}
		if runtime.User != "" {
			ref.User = observability.RedactedPlaceholder()
		}
	}
	ref.Password, ref.ConnectionV1 = "", ""
	return ref, nil
}
