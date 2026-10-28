package auth

import (
	stdjson "encoding/json"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/storage"
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

// bindUserJSON commits a complete typed decode for normal stored JSON. Roles
// reject type errors atomically; metadata applies only the ordered valid prefix
// of a well-formed object when a value has the wrong type.
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
	encoded, present := b.properties[key].(string)
	var raw []byte
	if present {
		if encoded == "null" {
			var empty T
			*field = empty
			return
		}
		raw = []byte(encoded)
		// Read bindings belong to a fresh User. Metadata can decode directly
		// into that initially nil field; atomic roles need a separate target.
		target := field
		if atomic {
			target = new(T)
		}
		if err := jsontext.Unmarshal(raw, target); err == nil {
			*field = *target
			return
		}
	}
	if atomic {
		return
	}
	// A failed direct decode must never publish its partially filled map.
	var empty T
	*field = empty
	// The default empty metadata map is needed only when a value is absent or
	// rejected. Successful object decoding allocates its map directly; JSON null
	// has already committed nil without allocating or changing the stored format.
	metadata := any(field).(*map[string]string)
	if *metadata == nil {
		*metadata = make(map[string]string)
	}
	if !present || !stdjson.Valid(raw) {
		return
	}
	reader := jsontext.NewTokenReader(raw)
	token, err := reader.Token()
	if err != nil || token != stdjson.Delim('{') {
		return
	}
	// Typed decoding may have filled entries after a type error. Its result
	// was discarded above; replay admits only this validated object's prefix.
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
		user = &User{}
	default:
		panic("invalid user-record direction")
	}
	b := userRecordBinding{node.Properties, direction == storeUserRecord}
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
	// The historical wire contract writes int and admits only int64 on read.
	if b.writing {
		bindUserScalar(b, "failed_logins", &user.FailedLogins)
	} else {
		var failed int64
		bindUserScalar(b, "failed_logins", &failed)
		user.FailedLogins = int(failed)
	}
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
	return mappedUserRecord{User: user, Node: node}
}
