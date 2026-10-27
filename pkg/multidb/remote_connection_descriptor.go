package multidb

import (
	"encoding/binary"
	"errors"
	"strings"

	"github.com/orneryd/nornicdb/pkg/localization"
)

// A descriptor is a new ciphertext domain. Legacy URI and USER strings never
// acquire encryption semantics from their contents. Versioned byte framing
// preserves every admitted string byte without a second Unicode/JSON codec.
type remoteConnection struct {
	uri, user, password, authMode string
	origin                        byte
}

const (
	remoteConnectionExplicit byte = 1
	remoteConnectionFallback byte = 2
)

func (m *DatabaseManager) sealRemoteConnection(ref ConstituentRef) (ConstituentRef, error) {
	origin := remoteConnectionFallback
	if strings.EqualFold(strings.TrimSpace(ref.AuthMode), "user_password") {
		origin = remoteConnectionExplicit
	}
	data := []byte{1, origin}
	for _, value := range []string{ref.URI, ref.User, ref.Password, ref.AuthMode} {
		data = binary.AppendUvarint(data, uint64(len(value)))
		data = append(data, value...)
	}
	encrypted, err := m.encryptRemoteCredential(remoteCredentialAllowEmpty, string(data))
	if err != nil {
		return ConstituentRef{}, err
	}
	ref.ConnectionV1 = encrypted
	ref.URI, ref.User, ref.Password, ref.AuthMode = "", "", "", ""
	return ref, nil
}

func (m *DatabaseManager) openRemoteConnection(ref ConstituentRef) (remoteConnection, error) {
	if ref.URI != "" || ref.User != "" || ref.Password != "" || ref.AuthMode != "" {
		return remoteConnection{}, errors.New("remote connection has conflicting stored authorities")
	}
	if m.remoteCredentialCipher == nil {
		return remoteConnection{}, localizedError(localization.MultidbCredentialDecryptKeyRequired(), nil)
	}
	plaintext, err := m.remoteCredentialCipher.decrypt(ref.ConnectionV1)
	if err != nil {
		return remoteConnection{}, err
	}
	data := []byte(plaintext)
	if len(data) < 2 || data[0] != 1 || (data[1] != remoteConnectionExplicit && data[1] != remoteConnectionFallback) {
		return remoteConnection{}, errors.New("invalid remote connection descriptor")
	}
	connection := remoteConnection{origin: data[1]}
	data = data[2:]
	values := []*string{&connection.uri, &connection.user, &connection.password, &connection.authMode}
	for _, value := range values {
		length, n := binary.Uvarint(data)
		if n <= 0 || length > uint64(len(data)-n) {
			return remoteConnection{}, errors.New("invalid remote connection descriptor extent")
		}
		data = data[n:]
		*value = string(data[:int(length)])
		data = data[int(length):]
	}
	if len(data) != 0 {
		return remoteConnection{}, errors.New("unexpected remote connection descriptor tail")
	}
	explicit := strings.EqualFold(strings.TrimSpace(connection.authMode), "user_password")
	if explicit != (connection.origin == remoteConnectionExplicit) {
		return remoteConnection{}, errors.New("invalid remote connection descriptor origin")
	}
	return connection, nil
}

// sealLegacyRemoteConnection retains the established Password-field decoder
// only. Raw URI and USER values, including enc:v1-looking text, stay literal.
func (m *DatabaseManager) sealLegacyRemoteConnection(ref ConstituentRef) (ConstituentRef, error) {
	if ref.ConnectionV1 != "" {
		_, err := m.openRemoteConnection(ref)
		return ref, err
	}
	if ref.Password != "" || strings.EqualFold(strings.TrimSpace(ref.AuthMode), "user_password") {
		password, err := m.decryptStoredRemotePassword(ref.Password)
		if err != nil {
			return ConstituentRef{}, err
		}
		ref.Password = password
	}
	return m.sealRemoteConnection(ref)
}
