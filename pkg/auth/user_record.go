package auth

import (
	stdjson "encoding/json"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"time"
)

// userRecordBinding applies the same stored field contract in both directions.
// Reading and writing deliberately retain their historical dynamic-type rules.
type userRecordBinding struct {
	properties map[string]any
	writing    bool
}

func bindUserScalar[T string | bool | int | int64](b userRecordBinding, key string, field *T) bool {
	if b.writing {
		b.properties[key] = *field
		return true
	}
	value, ok := b.properties[key].(T)
	if ok {
		*field = value
	}
	return ok
}

// Failed-logins historically writes int but reads only a persisted int64.
func bindUserCounter(b userRecordBinding, key string, field *int) {
	if b.writing {
		bindUserScalar(b, key, field)
		return
	}
	var value int64
	bindUserScalar(b, key, &value)
	*field = int(value)
}

// Roles commit only a complete decode; metadata retains partial-map decoding.
func bindUserJSON[T []Role | map[string]string](b userRecordBinding, key string, field *T, atomic bool) {
	if b.writing {
		if *field == nil {
			b.properties[key] = "null"
			return
		}
		encoded, _ := jsontext.Marshal(*field)
		b.properties[key] = string(encoded)
		return
	}
	encoded, ok := b.properties[key].(string)
	if !ok {
		return
	}
	if encoded == "null" {
		var empty T
		*field = empty
		return
	}
	if atomic {
		var decoded T
		if err := jsontext.Unmarshal([]byte(encoded), &decoded); err == nil {
			*field = decoded
		}
		return
	}
	// Stored metadata applies valid string entries in source order and stops
	// before the first invalid entry. Use the canonical token/value reader so
	// a decoder upgrade cannot insert zero values for rejected stored entries.
	raw := []byte(encoded)
	if !stdjson.Valid(raw) {
		return
	}
	reader := jsontext.NewTokenReader(raw)
	token, err := reader.Token()
	if err != nil {
		return
	}
	if token == nil {
		var empty T
		*field = empty
		return
	}
	if token != stdjson.Delim('{') {
		return
	}
	metadata := any(field).(*map[string]string)
	if *metadata == nil {
		*metadata = make(map[string]string)
	}
	for reader.More() {
		name, err := reader.Token()
		if err != nil {
			return
		}
		var value string
		if err := reader.Decode(&value); err != nil {
			return
		}
		(*metadata)[name.(string)] = value
	}
}

func bindUserRecord(b userRecordBinding, user *User, node *storage.Node) {
	// Convert only at the wire boundary. Keeping the four wire slots here lets
	// their common scalar binding inline instead of calling a mode-aware time
	// codec four times; no stored key or time-admission rule changes.
	var created, updated, lastLogin, lockedUntil int64
	if b.writing {
		created, updated = user.CreatedAt.Unix(), user.UpdatedAt.Unix()
		if !user.LastLogin.IsZero() {
			lastLogin = user.LastLogin.Unix()
		}
		if !user.LockedUntil.IsZero() {
			lockedUntil = user.LockedUntil.Unix()
		}
	}
	bindUserScalar(b, "id", &user.ID)
	bindUserScalar(b, "username", &user.Username)
	bindUserScalar(b, "email", &user.Email)
	bindUserScalar(b, "password_hash", &user.PasswordHash)
	bindUserJSON(b, "roles", &user.Roles, true)
	hasCreated := bindUserScalar(b, "created_at", &created)
	hasUpdated := bindUserScalar(b, "updated_at", &updated)
	bindUserScalar(b, "last_login", &lastLogin)
	bindUserCounter(b, "failed_logins", &user.FailedLogins)
	bindUserScalar(b, "locked_until", &lockedUntil)
	bindUserScalar(b, "disabled", &user.Disabled)
	bindUserJSON(b, "metadata", &user.Metadata, false)
	if !b.writing {
		if hasCreated {
			user.CreatedAt = time.Unix(created, 0)
		} else {
			user.CreatedAt = node.CreatedAt
		}
		if hasUpdated {
			user.UpdatedAt = time.Unix(updated, 0)
		} else {
			user.UpdatedAt = node.UpdatedAt
		}
		if lastLogin > 0 {
			user.LastLogin = time.Unix(lastLogin, 0)
		}
		if lockedUntil > 0 {
			user.LockedUntil = time.Unix(lockedUntil, 0)
		}
	}
}

type userRecordDirection uint8

const (
	storeUserRecord userRecordDirection = iota
	loadUserRecord
)

type mappedUserRecord struct {
	User *User
	Node *storage.Node
}

func mapUserRecord(direction userRecordDirection, user *User, node *storage.Node) mappedUserRecord {
	switch direction {
	case storeUserRecord:
		node = &storage.Node{ID: storage.NodeID("user:" + user.Username), Labels: []string{"_User", "_System"}, Properties: make(map[string]any, 12), CreatedAt: user.CreatedAt, UpdatedAt: user.UpdatedAt}
	case loadUserRecord:
		user = &User{Metadata: make(map[string]string)}
	default:
		panic("invalid user-record direction")
	}
	bindUserRecord(userRecordBinding{node.Properties, direction == storeUserRecord}, user, node)
	return mappedUserRecord{User: user, Node: node}
}
