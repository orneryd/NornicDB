package storage

import (
	encjson "encoding/json"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/dgraph-io/badger/v4"
	"github.com/orneryd/nornicdb/internal/jsontext"
	"github.com/orneryd/nornicdb/pkg/knowledgepolicy"
	"github.com/stretchr/testify/require"

	runtimeFixtureOwner "github.com/orneryd/nornicdb/internal/testfixture"
)

// schemaTransactionEscapedUnit returns the in-memory string identity the
// canonical internal/jsontext codec admits for the standard JSON escape of one
// lone UTF16 unit. It is derived from JSON text so the fixture is the exact
// identity the persistence codec must round-trip, not a hand-built surrogate.
func schemaTransactionEscapedUnit(t *testing.T, unit string) string {
	t.Helper()
	var payload struct {
		Name string `json:"name"`
	}
	require.NoError(t, jsontext.Unmarshal([]byte(`{"name":"\u`+unit+`"}`), &payload))
	require.NotEqual(t, "\ufffd", payload.Name, "fixture must not already be a replacement character")
	return payload.Name
}

func schemaTransactionOpen(t *testing.T, directory string) *BadgerEngine {
	t.Helper()
	engine, err := runtimeFixtureOwner.TrackResult[*BadgerEngine](t, ErrStorageClosed)(NewBadgerEngine(directory))
	require.NoError(t, err)
	t.Cleanup(func() { _ = engine.Close() })
	return engine
}

func schemaTransactionBegin(t *testing.T, engine *BadgerEngine, namespace string) *BadgerTransaction {
	t.Helper()
	transaction, err := engine.BeginTransaction(t.Context())
	require.NoError(t, err)
	require.NoError(t, transaction.SetNamespace(namespace))
	t.Cleanup(func() { _ = transaction.Rollback() })
	return transaction
}

// schemaTransactionPayload returns the exact persisted bytes of the namespace's
// schema definition, so a test can assert the stored key value it observes is
// the one the codec wrote.
func schemaTransactionPayload(t *testing.T, engine *BadgerEngine, namespace string) []byte {
	t.Helper()
	var payload []byte
	require.NoError(t, engine.db.View(func(transaction *badger.Txn) error {
		item, err := transaction.Get(schemaKey(namespace))
		if err != nil {
			return err
		}
		return item.Value(func(value []byte) error {
			payload = append([]byte(nil), value...)
			return nil
		})
	}))
	return payload
}

func schemaTransactionSeedPolicy(t *testing.T, view *SchemaManager, profile, policy, label, predicate string) {
	t.Helper()
	require.NoError(t, view.CreatePromotionProfile(knowledgepolicy.PromotionProfileDef{
		Name: profile, Scope: knowledgepolicy.ScopeNode, Multiplier: 1.5, ScoreFloor: 0, ScoreCap: 1, Enabled: true,
	}))
	require.NoError(t, view.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
		Name:         policy,
		TargetLabels: []string{label},
		Enabled:      true,
		WhenClauses: []knowledgepolicy.PromotionPolicyWhenClause{
			{ProfileRef: profile, Predicate: predicate, Order: 1},
		},
	}))
}

// TestKnowledgePolicySchemaTransactionPreservesEscapedSurrogate pins the
// approved exact-surrogate contract of the transaction schema owner: a policy
// name, target label and predicate admitted through the canonical JSON codec
// must keep their exact string identity through the transaction write, the
// published snapshot, the stored schema key payload and a durable reopen.
func TestKnowledgePolicySchemaTransactionPreservesEscapedSurrogate(t *testing.T) {
	high := schemaTransactionEscapedUnit(t, "d800")
	// WTF8 encoding of U+D800: the canonical in-memory identity, which is not
	// valid UTF8 and which encoding/json replaces with U+FFFD.
	require.Equal(t, "\xed\xa0\x80", high)

	directory := t.TempDir()
	namespace := "native"
	engine := schemaTransactionOpen(t, directory)

	profile := "profile" + high
	policy := "policy" + high
	label := "Label" + high
	predicate := "mutationCount > 3 OR name = '" + high + "'"

	transaction := schemaTransactionBegin(t, engine, namespace)
	view, err := transaction.Schema(KnowledgePolicyChanges)
	require.NoError(t, err)
	schemaTransactionSeedPolicy(t, view, profile, policy, label, predicate)
	require.NoError(t, transaction.Commit())

	published := engine.GetSchemaForNamespace(namespace)
	require.Len(t, published.ShowPromotionProfiles(), 1)
	require.Equal(t, profile, published.ShowPromotionProfiles()[0].Name)
	require.Len(t, published.ShowPromotionPolicies(), 1)
	require.Equal(t, policy, published.ShowPromotionPolicies()[0].Name)
	require.Equal(t, []string{label}, published.ShowPromotionPolicies()[0].TargetLabels)
	require.Equal(t, predicate, published.ShowPromotionPolicies()[0].WhenClauses[0].Predicate)

	payload := schemaTransactionPayload(t, engine, namespace)
	require.True(t, encjson.Valid(payload), "persisted schema payload must stay standard JSON")
	require.True(t, utf8.Valid(payload), "persisted schema payload must stay valid UTF8")
	require.Contains(t, string(payload), `\ud800`, "escaped lone unit must persist as its standard JSON escape")
	require.NotContains(t, string(payload), "\ufffd", "escaped lone unit must not be normalized on write")

	// A standard JSON reader still accepts the payload; exact identity is the
	// canonical codec's contract, not the old decoder's.
	var legacyRead SchemaDefinition
	require.NoError(t, encjson.Unmarshal(payload, &legacyRead))

	require.NoError(t, engine.Close())
	reopened := schemaTransactionOpen(t, directory)
	loaded := reopened.GetSchemaForNamespace(namespace)
	require.Len(t, loaded.ShowPromotionProfiles(), 1)
	require.Equal(t, profile, loaded.ShowPromotionProfiles()[0].Name)
	require.Len(t, loaded.ShowPromotionPolicies(), 1)
	require.Equal(t, policy, loaded.ShowPromotionPolicies()[0].Name)
	require.Equal(t, []string{label}, loaded.ShowPromotionPolicies()[0].TargetLabels)
	require.Equal(t, predicate, loaded.ShowPromotionPolicies()[0].WhenClauses[0].Predicate)

	// The transaction read owner must return the same exact identity.
	readView, err := schemaTransactionBegin(t, reopened, namespace).Schema(KnowledgePolicyChanges)
	require.NoError(t, err)
	require.Equal(t, policy, readView.ShowPromotionPolicies()[0].Name)
	require.Equal(t, predicate, readView.ShowPromotionPolicies()[0].WhenClauses[0].Predicate)
}

// TestKnowledgePolicySchemaTransactionPayloadLayout pins the compatibility
// contract of the same owner: an ordinary (surrogate-free) payload keeps
// exactly the bytes encoding/json wrote, and a payload written by the previous
// codec stays readable through both schema owners.
func TestKnowledgePolicySchemaTransactionPayloadLayout(t *testing.T) {
	t.Run("ordinary payload bytes are unchanged", func(t *testing.T) {
		namespace := "native"
		engine := schemaTransactionOpen(t, t.TempDir())
		transaction := schemaTransactionBegin(t, engine, namespace)
		view, err := transaction.Schema(KnowledgePolicyChanges)
		require.NoError(t, err)
		schemaTransactionSeedPolicy(t, view, "base_profile", "base_policy", "BaseLabel", "mutationCount > 3")
		require.NoError(t, transaction.Commit())

		committed := engine.GetSchemaForNamespace(namespace)
		expected, err := encjson.Marshal(schemaDefinitionForTest(t, committed))
		require.NoError(t, err)
		payload := schemaTransactionPayload(t, engine, namespace)
		require.Equal(t, string(expected), string(payload),
			"ordinary schema payload must keep the exact encoding/json layout")

		// Re-encoding the stored payload through the old codec is byte
		// idempotent: the new writer produces no escaping or ordering change.
		var decoded SchemaDefinition
		require.NoError(t, encjson.Unmarshal(payload, &decoded))
		reencoded, err := encjson.Marshal(&decoded)
		require.NoError(t, err)
		require.Equal(t, string(payload), string(reencoded))
	})

	t.Run("legacy payload stays readable", func(t *testing.T) {
		// A definition written by the previous owner: encoding/json output with
		// no lookup_indexes, escaped non-ASCII, a surrogate pair and an inline
		// knowledge policy.
		legacy := strings.Join([]string{
			`{"version":1,`,
			`"label_tokens":["Caf\u00e9","\ud83d\ude00"],`,
			`"constraints":[{"name":"uq_email","type":"UNIQUE","entity_type":"NODE",`,
			`"label":"Person","properties":["email"]}],`,
			`"promotion_profiles":[{"name":"legacy_profile","scope":"NODE","multiplier":1.5,`,
			`"scoreFloor":0,"scoreCap":1,"enabled":true}],`,
			`"promotion_policies":[{"name":"legacy_policy","targetLabels":["Caf\u00e9"],`,
			`"isWildcard":false,"isEdge":false,"whenClauses":[{"predicate":"mutationCount > 3",`,
			`"profileRef":"legacy_profile","order":1}],"enabled":true}]}`,
		}, "")
		require.True(t, encjson.Valid([]byte(legacy)))

		namespace := "native"
		directory := t.TempDir()
		engine := schemaTransactionOpen(t, directory)
		require.NoError(t, engine.db.Update(func(transaction *badger.Txn) error {
			return transaction.Set(schemaKey(namespace), []byte(legacy))
		}))

		view, err := schemaTransactionBegin(t, engine, namespace).Schema(KnowledgePolicyChanges)
		require.NoError(t, err)
		require.Equal(t, "legacy_policy", view.ShowPromotionPolicies()[0].Name)
		require.Equal(t, []string{"Café"}, view.ShowPromotionPolicies()[0].TargetLabels)
		require.Equal(t, "legacy_profile", view.ShowPromotionProfiles()[0].Name)

		require.NoError(t, engine.Close())
		reopened := schemaTransactionOpen(t, directory)
		loaded := reopened.GetSchemaForNamespace(namespace)
		require.Equal(t, "legacy_policy", loaded.ShowPromotionPolicies()[0].Name)
		require.Equal(t, []string{"Café"}, loaded.ShowPromotionPolicies()[0].TargetLabels)
		require.Equal(t, "legacy_profile", loaded.ShowPromotionProfiles()[0].Name)
		require.Equal(t, []string{"Café", "😀"}, schemaDefinitionForTest(t, loaded).LabelTokens)

		// A transaction rewrite of the legacy definition keeps the values and
		// writes an ordinary payload the old codec still reads.
		transaction := schemaTransactionBegin(t, reopened, namespace)
		rewrite, err := transaction.Schema(KnowledgePolicyChanges)
		require.NoError(t, err)
		require.NoError(t, rewrite.CreatePromotionPolicy(knowledgepolicy.PromotionPolicyDef{
			Name: "second_policy", TargetLabels: []string{"Second"}, Enabled: true,
		}))
		require.NoError(t, transaction.Commit())
		payload := schemaTransactionPayload(t, reopened, namespace)
		require.True(t, utf8.Valid(payload))
		var decoded SchemaDefinition
		require.NoError(t, encjson.Unmarshal(payload, &decoded))
		require.Len(t, decoded.PromotionPolicies, 2)
		require.Equal(t, "second_policy", reopened.GetSchemaForNamespace(namespace).ShowPromotionPolicies()[1].Name)
	})
}
