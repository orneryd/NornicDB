package auth

import (
	"bytes"
	stdjson "encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/storage"
)

type userRecordCompatibilityCase struct {
	name      string
	direction userRecordDirection
	user      *User
	node      *storage.Node
	roundtrip bool
}

func userRecordCompatibilityCases() []userRecordCompatibilityCase {
	unit := string([]byte{0xed, 0xa0, 0x80})
	fields := []string{"id", "username", "email", "password_hash", "roles", "created_at", "updated_at", "last_login", "failed_logins", "locked_until", "disabled", "metadata"}
	values := []any{nil, false, true, int(0), int(4), int64(-2), int64(0), int64(4), float64(4), "", "null", "{}", "[]", `["admin"]`, `["admin",4]`, `["admin",`, `{"a":"one"}`, `{"a":"one","b":3}`, `{"a":"one",`, "literal\\uD800", unit, []string{"admin"}, map[string]string{"a": "one"}}
	header := time.Unix(1234, 567).UTC()
	var cases []userRecordCompatibilityCase
	for _, field := range fields {
		for index, value := range values {
			cases = append(cases, userRecordCompatibilityCase{name: fmt.Sprintf("read/%s/%d", field, index), direction: loadUserRecord, node: &storage.Node{Properties: map[string]any{field: value}, CreatedAt: header, UpdatedAt: header.Add(time.Second)}})
		}
	}
	for index := 0; index < 34; index++ {
		user := &User{ID: unit + fmt.Sprint(index), Username: "name" + unit, Email: "a@b", PasswordHash: "existing-hash", FailedLogins: index, Disabled: index%2 == 0}
		if index%2 == 0 {
			user.Roles = []Role{RoleAdmin, RoleViewer}
			user.Metadata = map[string]string{"name" + unit: unit, "literal": "\\uD800"}
		}
		if index%3 == 0 {
			user.CreatedAt = header
			user.UpdatedAt = header.Add(time.Second)
			user.LastLogin = header
			user.LockedUntil = header.Add(time.Hour)
		}
		if index%5 == 0 {
			user.CreatedAt = time.Unix(-5, 123).UTC()
			user.LastLogin = time.Unix(-3, 0).UTC()
			user.LockedUntil = time.Unix(0, 0).UTC()
		}
		switch index {
		case 30:
			user.Roles = []Role{}
			user.Metadata = map[string]string{}
		case 31:
			user.Roles = nil
			user.Metadata = map[string]string{}
		case 32:
			user.Roles = []Role{}
			user.Metadata = nil
		case 33:
			user.Roles = nil
			user.Metadata = nil
		}
		cases = append(cases, userRecordCompatibilityCase{name: fmt.Sprintf("write/%d", index), direction: storeUserRecord, user: user}, userRecordCompatibilityCase{name: fmt.Sprintf("roundtrip/%d", index), direction: loadUserRecord, user: user, roundtrip: true})
	}
	return cases
}

// The fixture records every exported field and each stored property's dynamic
// type. UTC presentation makes time instants portable; the separate location
// contract below retains exact Go timestamp fallback behavior.
func userRecordCompatibilitySnapshot(value any) ([]byte, error) {
	item := reflect.Indirect(reflect.ValueOf(value))
	fields := make(map[string]any, item.NumField())
	for index := 0; index < item.NumField(); index++ {
		field := item.Type().Field(index)
		if field.PkgPath != "" {
			continue
		}
		entry := item.Field(index).Interface()
		if timestamp, ok := entry.(time.Time); ok {
			entry = timestamp.UTC()
		}
		fields[field.Name] = entry
		if field.Name == "Properties" {
			kinds := map[string]string{}
			for key, value := range entry.(map[string]any) {
				kinds[key] = fmt.Sprintf("%T", value)
			}
			fields["PropertyTypes"] = kinds
		}
	}
	// Preserve the frozen logical-node oracle after the Go fields move behind
	// typed accessors; the expected fixtures and property-type checks stay exact.
	if node, ok := value.(*storage.Node); ok {
		metadata := node.RuntimeMetadata()
		fields["Database"] = metadata.Database
		fields["EmbeddingsStoredSeparately"] = metadata.EmbeddingsStoredSeparately
		fields["VisibilitySuppressed"] = metadata.VisibilitySuppressed
	}
	return jsontext.Marshal(fields)
}

func TestUserRecordStoredCompatibility(t *testing.T) {
	data, err := os.ReadFile("testdata/user_record_compatibility.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]stdjson.RawMessage
	if err = stdjson.Unmarshal(data, &expected); err != nil {
		t.Fatal(err)
	}
	cases := userRecordCompatibilityCases()
	if len(expected) != len(cases) {
		t.Fatalf("fixture cases=%d, input population=%d", len(expected), len(cases))
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			node := test.node
			if test.roundtrip {
				node = mapUserRecord(storeUserRecord, test.user, nil).Node
			}
			mapped := mapUserRecord(test.direction, test.user, node)
			var value any = mapped.User
			if test.direction == storeUserRecord {
				value = mapped.Node
			}
			actual, err := userRecordCompatibilitySnapshot(value)
			if err != nil {
				t.Fatal(err)
			}
			var compact bytes.Buffer
			if err = stdjson.Compact(&compact, expected[test.name]); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(actual, compact.Bytes()) {
				t.Fatalf("stored contract changed\nexpected %s\nactual   %s", compact.Bytes(), actual)
			}
		})
	}
}

func TestUserRecordTimestampLocations(t *testing.T) {
	header := time.Now().In(time.FixedZone("fixture", 1234))
	node := &storage.Node{Properties: map[string]any{}, CreatedAt: header, UpdatedAt: header}
	user := mapUserRecord(loadUserRecord, nil, node).User
	if user.CreatedAt != header || user.UpdatedAt != header {
		t.Fatal("header fallback changed Go timestamp")
	}
	node.Properties["created_at"] = int64(-2)
	node.Properties["updated_at"] = int64(0)
	user = mapUserRecord(loadUserRecord, nil, node).User
	if user.CreatedAt != time.Unix(-2, 0) || user.UpdatedAt != time.Unix(0, 0) {
		t.Fatal("stored Unix timestamps changed Go representation")
	}
}

func TestUserRecordMetadataEntryAdmission(t *testing.T) {
	for _, test := range []struct {
		name, encoded string
		want          map[string]string
	}{
		{"valid prefix before invalid value", `{"first":"ok","bad":3,"later":"ignored"}`, map[string]string{"first": "ok"}},
		{"invalid first entry", `{"bad":3,"later":"ignored"}`, map[string]string{}},
		{"invalid nested value", `{"first":"ok","bad":{"nested":"value"},"later":"ignored"}`, map[string]string{"first": "ok"}},
		{"malformed document applies nothing", `{"first":"ok",`, map[string]string{}},
		{"whitespace null", " null ", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			user := mapUserRecord(loadUserRecord, nil, &storage.Node{Properties: map[string]any{"metadata": test.encoded}}).User
			if !reflect.DeepEqual(test.want, user.Metadata) {
				t.Fatalf("metadata=%#v want %#v", user.Metadata, test.want)
			}
		})
	}
}
