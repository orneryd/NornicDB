package storage

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQualifiedEntityIDPreservesDatabaseIdentity(t *testing.T) {
	tenant := NewNamespacedEngine(nil, "tenant")
	wrapped := &WALEngine{engine: tenant}
	self := &WALEngine{}
	self.engine = self
	for _, tc := range []struct {
		name     string
		engine   Engine
		id, want string
	}{
		{"local", tenant, "plain", "tenant:plain"},
		{"colon in local ID", tenant, "a:b", "tenant:a:b"},
		{"qualified", tenant, "tenant:a:b", "tenant:a:b"},
		{"another namespace is a local ID", tenant, "other:id", "tenant:other:id"},
		{"outer wrapper", wrapped, "a:b", "tenant:a:b"},
		{"unscoped", &BadgerEngine{}, "a:b", "a:b"},
		{"nil", nil, "a:b", "a:b"},
		{"nil inner", &WALEngine{}, "plain", "plain"},
		{"self wrapper", self, "plain", "plain"},
		{"empty outer scope", NewNamespacedEngine(tenant, ""), "a:b", "tenant:a:b"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, QualifiedEntityID(tc.engine, tc.id))
		})
	}
}
