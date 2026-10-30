package nornicdb

// RemoteCredentialEncryptionKey returns the immutable key selection used when
// this DB opened, and its nonsecret source. Server catalog construction reuses
// it so an upgrade and subsequent decryption cannot choose different keys.
// The key is secret material and must never be logged or sent to clients.
//
// For example, pass the key to multidb.Config.RemoteCredentialEncryptionKey
// when constructing the manager over GetBaseStorageForManager().
func (db *DB) RemoteCredentialEncryptionKey() (key, source string) {
	return db.remoteCredentialKey, db.remoteCredentialKeySource
}
