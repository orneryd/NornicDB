package cypher

// ON-clause identity tests (PR1667 G1-2). A schema statement names its
// properties in an ON clause. The one quote-aware readers (splitTopLevelComma
// and splitPostfixPropertyAccess) decide where an item and a reference end, so
// a dot, a comma or a doubled backtick inside a backtick-quoted name is part of
// that name and the declaration stores exactly the identity the node holds.

import (
	"context"
	"fmt"
	"testing"

	"github.com/orneryd/nornicdb/internal/cyphertext"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// ddlSurrogateName is one unpaired UTF-16 surrogate in storage's canonical
// WTF-8 form: an identity a client can name with \ud800 and that must survive
// the generated DDL text unchanged.
func ddlSurrogateName() string { return "mark" + string([]byte{0xed, 0xa0, 0x80}) }

// TestOnClausePropertyReaderIsQuoteAware pins the shared readers as the ON
// clause's one property parser: quoted names keep dots, commas and backticks,
// unquoted names keep their Unicode identity, and an unterminated quote yields
// no property instead of a truncated name.
func TestOnClausePropertyReaderIsQuoteAware(t *testing.T) {
	surrogate := ddlSurrogateName()
	for _, tc := range []struct {
		in   string
		want []string
	}{
		{"n.name", []string{"name"}},
		{"n.`a,b`", []string{"a,b"}},
		{"n.`a.b`", []string{"a.b"}},
		{"n.`a``b`", []string{"a`b"}},
		{"n.a, n.`b,c`", []string{"a", "b,c"}},
		{"n.`名前`", []string{"名前"}},
		{"n.`🚀`", []string{"🚀"}},
		{"n.`" + surrogate + "`", []string{surrogate}},
		{"n.a, n.b", []string{"a", "b"}},
		{"n.` key `", []string{" key "}},
		{"n.`  `", []string{"  "}},
	} {
		require.Equal(t, tc.want, indexPropertiesFromOnClause(tc.in, false), tc.in)
	}
	require.Equal(t, []string{"a.b"}, indexPropertiesFromOnClause("`a.b`", true),
		"the legacy bare-property spelling keeps a quoted dot")
	require.Equal(t, []string{"  "}, indexPropertiesFromOnClause("`  `", true),
		"the legacy bare-property spelling keeps a whitespace-only quoted name")
	require.Empty(t, indexPropertiesFromOnClause("n.`unterminated", false),
		"an unterminated quoted name is not a truncated property")
}

// TestIndexOnPropertiesPreserveQuotedIdentities declares and seeks an index for
// labels and properties that are not plain identifiers. The declaration stores
// the exact identity the node holds, and an equality read written with the same
// quoted names resolves that node through the published index.
func TestIndexOnPropertiesPreserveQuotedIdentities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "onclause")
		exec := NewStorageExecutor(store)
		for i, tc := range []struct{ label, property string }{
			{"OnQuoted.Person", "given.name"},
			{"OnQuoted,Person", "given,name"},
			{"OnQuoted`Person", "given`name"},
			{"OnQuotedPerson", "名前"},
			{"OnQuotedPerson", "🚀"},  // one scalar above the BMP (a UTF-16 surrogate pair)
			{"OnQuotedPerson", "  "}, // whitespace-only name: a valid quoted identity
			{"OnQuotedPerson", ddlSurrogateName()},
			{"OnQuoted" + ddlSurrogateName(), "name"},
		} {
			label := cyphertext.PrintIdentifier(tc.label, nil, cyphertext.RawSource)
			property := cyphertext.PrintIdentifier(tc.property, nil, cyphertext.RawSource)
			value := fmt.Sprintf("value-%d", i)
			_, err := exec.Execute(context.Background(),
				"CREATE (n:"+label+" {"+property+": $value})", map[string]any{"value": value})
			require.NoError(t, err, tc.label)

			index := fmt.Sprintf("ix_on_quoted_%d", i)
			_, err = exec.Execute(context.Background(),
				"CREATE INDEX "+index+" FOR (n:"+label+") ON (n."+property+")", nil)
			require.NoError(t, err, tc.property)

			declared, ok := store.GetSchema().GetCompositeIndex(index)
			require.True(t, ok, index)
			require.Equal(t, tc.label, declared.Label, "the declaration renamed the label")
			require.Equal(t, []string{tc.property}, declared.Properties, "the declaration renamed the property")

			result, err := exec.Execute(context.Background(),
				"MATCH (n:"+label+") WHERE n."+property+" = $value RETURN n."+property+" AS v",
				map[string]any{"value": value})
			require.NoError(t, err, tc.property)
			require.Len(t, result.Rows, 1, "the quoted equality read lost the stored node")
			require.Equal(t, value, result.Rows[0][0])
		}
	})
}

// TestIndexOnPropertiesPreserveBoundarySpaceIdentities declares an index for a
// backtick-quoted property name whose value starts and ends with a space. The
// spaces are inside the quotes, so they are part of the property's identity:
// the declaration stores that identity, the published index files the node
// under it, and an equality read written the same way finds the node.
func TestIndexOnPropertiesPreserveBoundarySpaceIdentities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "onclausespace")
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		const property = " key "
		quoted := cyphertext.PrintIdentifier(property, nil, cyphertext.RawSource)
		require.Equal(t, "` key `", quoted)

		_, err := exec.Execute(ctx,
			"CREATE (n:OnBoundarySpace {"+quoted+": $value})", map[string]any{"value": "v"})
		require.NoError(t, err)
		_, err = exec.Execute(ctx,
			"CREATE INDEX ix_on_boundary_space FOR (n:OnBoundarySpace) ON (n."+quoted+")", nil)
		require.NoError(t, err)

		declared, ok := store.GetSchema().GetCompositeIndex("ix_on_boundary_space")
		require.True(t, ok)
		require.Equal(t, []string{property}, declared.Properties,
			"the declaration trimmed a boundary space inside the quoted name")

		ids, err := store.GetSchema().PropertyIndexLookup(ctx, "OnBoundarySpace", property, "v")
		require.NoError(t, err)
		require.Len(t, ids, 1,
			"the published index does not hold the node under the quoted property")

		result, err := exec.Execute(ctx,
			"MATCH (n:OnBoundarySpace) WHERE n."+quoted+" = $value RETURN n."+quoted+" AS v",
			map[string]any{"value": "v"})
		require.NoError(t, err)
		require.Len(t, result.Rows, 1, "the quoted equality read lost the stored node")
		require.Equal(t, "v", result.Rows[0][0])
	})
}

// TestCompositeUniqueConstraintPreservesBoundarySpacePropertyIdentities pins
// the same identity in a composite uniqueness constraint: the constraint lists
// the quoted properties exactly as the nodes hold them, so a second node with
// the same quoted property values is a violation.
func TestCompositeUniqueConstraintPreservesBoundarySpacePropertyIdentities(t *testing.T) {
	forEachParser(t, func(t *testing.T) {
		store := storage.NewNamespacedEngine(newTestMemoryEngine(t), "onclausespaceunique")
		exec := NewStorageExecutor(store)
		ctx := context.Background()
		first, last := " first ", "last "
		quotedFirst := cyphertext.PrintIdentifier(first, nil, cyphertext.RawSource)
		quotedLast := cyphertext.PrintIdentifier(last, nil, cyphertext.RawSource)

		_, err := exec.Execute(ctx,
			"CREATE (n:OnBoundaryUnique {"+quotedFirst+": $first, "+quotedLast+": $last})",
			map[string]any{"first": "a", "last": "b"})
		require.NoError(t, err)
		_, err = exec.Execute(ctx,
			"CREATE CONSTRAINT c_on_boundary_unique FOR (n:OnBoundaryUnique) REQUIRE (n."+
				quotedFirst+", n."+quotedLast+") IS UNIQUE", nil)
		require.NoError(t, err)

		all := store.GetSchema().GetAllConstraints()
		found := false
		for _, constraint := range all {
			if constraint.Name != "c_on_boundary_unique" {
				continue
			}
			found = true
			require.Equal(t, []string{first, last}, constraint.Properties,
				"the constraint trimmed a boundary space inside a quoted name")
		}
		require.True(t, found, "composite uniqueness constraint not found")

		_, err = exec.Execute(ctx,
			"CREATE (n:OnBoundaryUnique {"+quotedFirst+": $first, "+quotedLast+": $last})",
			map[string]any{"first": "a", "last": "b"})
		require.Error(t, err, "a duplicate under the quoted property identity is a constraint violation")
	})
}

// TestOnClausePropertyReaderPreservesSurrogateIdentity keeps the source
// projection, the raw emitter and the ON-clause reader on one identity: the DDL
// text PrintIdentifier generates for a name holding an unpaired UTF-16
// surrogate decodes back to exactly that name, so the declaration a statement
// carries names the property the node holds.
//
// Public creation cases above cover the same identity through the native
// transaction schema owner, which uses the existing lossless JSON codec.
func TestOnClausePropertyReaderPreservesSurrogateIdentity(t *testing.T) {
	name := ddlSurrogateName()
	quoted := cyphertext.PrintIdentifier(name, nil, cyphertext.RawSource)
	require.Contains(t, quoted, `\ud800`, "the raw emitter must spell the surrogate as its source escape")

	projected := cyphertext.NormalizeSource("n." + quoted)
	require.NoError(t, projected.Err)
	require.Equal(t, []string{name}, indexPropertiesFromOnClause(projected.Text, false),
		"the accepted ON clause must carry the stored identity, not the source escape")
}
