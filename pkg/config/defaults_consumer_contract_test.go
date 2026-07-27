package config

import "testing"

// TestDefaults_ConsumerContract pins the parsed defaults that downstream
// Bolt consumers depend on. Flipping any of these is a deliberate,
// coordinated change — see docs/plans/consumer-pinned-error-contract-plan.md
// §2.6 for the full rationale.
//
// Values match what LoadDefaults() produces today. If you intentionally
// change one of these, update §2.6 of the plan in the same PR and
// coordinate with known consumers.
func TestDefaults_ConsumerContract(t *testing.T) {
	c := LoadDefaults()

	if got, want := c.Database.AsyncWritesEnabled, true; got != want {
		t.Errorf(
			"Database.AsyncWritesEnabled default changed: got %v want %v "+
				"(consumer contract — see consumer-pinned-error-contract-plan.md §2.6)",
			got, want,
		)
	}
	if got, want := c.Database.PersistSearchIndexes, false; got != want {
		t.Errorf(
			"Database.PersistSearchIndexes default changed: got %v want %v "+
				"(consumer contract — see consumer-pinned-error-contract-plan.md §2.6)",
			got, want,
		)
	}
	if got, want := c.Auth.Enabled, true; got != want {
		t.Errorf(
			"Auth.Enabled default changed: got %v want %v "+
				"(consumer contract — see consumer-pinned-error-contract-plan.md §2.6)",
			got, want,
		)
	}
	if got, want := c.Server.BoltPort, 7687; got != want {
		t.Errorf(
			"Server.BoltPort default changed: got %v want %v "+
				"(consumer contract — see consumer-pinned-error-contract-plan.md §2.6)",
			got, want,
		)
	}
	if got, want := c.Server.HTTPPort, 7474; got != want {
		t.Errorf(
			"Server.HTTPPort default changed: got %v want %v "+
				"(consumer contract — see consumer-pinned-error-contract-plan.md §2.6)",
			got, want,
		)
	}
}
