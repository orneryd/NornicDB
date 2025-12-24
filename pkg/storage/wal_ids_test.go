package storage

import "testing"

func TestParseDatabasePrefix(t *testing.T) {
	tests := []struct {
		name       string
		in         string
		wantDB     string
		wantID     string
		wantOK     bool
	}{
		{name: "valid", in: "db:node-1", wantDB: "db", wantID: "node-1", wantOK: true},
		{name: "multiple-colons", in: "db:n1:extra", wantDB: "db", wantID: "n1:extra", wantOK: true},
		{name: "no-colon", in: "node-1", wantOK: false},
		{name: "empty-db", in: ":node-1", wantOK: false},
		{name: "empty-id", in: "db:", wantOK: false},
		{name: "single-colon", in: ":", wantOK: false},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			gotDB, gotID, gotOK := ParseDatabasePrefix(tt.in)
			if gotOK != tt.wantOK {
				t.Fatalf("ok mismatch: got=%v want=%v", gotOK, tt.wantOK)
			}
			if gotDB != tt.wantDB || gotID != tt.wantID {
				t.Fatalf("split mismatch: got=(%q,%q) want=(%q,%q)", gotDB, gotID, tt.wantDB, tt.wantID)
			}
		})
	}
}

func TestStripDatabasePrefix(t *testing.T) {
	tests := []struct {
		name   string
		db     string
		in     string
		want   string
	}{
		{name: "strip-matching", db: "db", in: "db:node-1", want: "node-1"},
		{name: "no-strip-different-db", db: "db", in: "other:node-1", want: "other:node-1"},
		{name: "no-strip-empty-db", db: "", in: "db:node-1", want: "db:node-1"},
		{name: "no-strip-empty-id", db: "db", in: "", want: ""},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := StripDatabasePrefix(tt.db, tt.in); got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}

func TestEnsureDatabasePrefix(t *testing.T) {
	tests := []struct {
		name string
		db   string
		in   string
		want string
	}{
		{name: "add-prefix", db: "db", in: "node-1", want: "db:node-1"},
		{name: "already-prefixed-same", db: "db", in: "db:node-1", want: "db:node-1"},
		{name: "already-prefixed-other", db: "db", in: "other:node-1", want: "other:node-1"},
		{name: "empty-db", db: "", in: "node-1", want: "node-1"},
		{name: "empty-id", db: "db", in: "", want: ""},
		// Invalid prefix format (":id") should be treated as "no valid prefix" and get ensured.
		{name: "invalid-prefix-format", db: "db", in: ":node-1", want: "db::node-1"},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			if got := EnsureDatabasePrefix(tt.db, tt.in); got != tt.want {
				t.Fatalf("got=%q want=%q", got, tt.want)
			}
		})
	}
}

