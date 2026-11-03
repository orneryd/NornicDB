package cypher

import (
	"context"
	fixtureowner "github.com/orneryd/nornicdb/internal/testfixture"
	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPublicUniqueIdentityReview(t *testing.T) {
	restoreParserTypeAfter(t)
	for _, parser := range []string{"nornic", "antlr"} {
		t.Run(parser, func(t *testing.T) {
			config.SetParserType(parser)
			require.Equal(t, parser, config.GetParserType())
			t.Run("colon_tuple", func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
				require.NoError(t, err)
				fixtureowner.Own(t, b)
				t.Cleanup(func() { _ = b.Close() })
				e := NewStorageExecutor(storage.NewNamespacedEngine(b, "identity"))
				ctx := context.Background()
				for _, q := range []string{
					"CREATE CONSTRAINT colon_left FOR (n:`A:B`) REQUIRE n.c IS UNIQUE",
					"CREATE CONSTRAINT colon_right FOR (n:A) REQUIRE n.`B:c` IS UNIQUE",
					"CREATE (:`A:B` {c:'shared'})",
				} {
					_, err = e.Execute(ctx, q, nil)
					require.NoError(t, err, q)
				}
				_, err = e.Execute(ctx, "CREATE (:A {`B:c`:'shared'})", nil)
				assert.NoError(t, err, "independent label/property tuples may reuse a value")
				_, err = e.Execute(ctx, "CREATE (:`A:B` {c:'shared'})", nil)
				require.Error(t, err, "same left tuple remains UNIQUE")
				require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintValidationFailed")
				_, err = e.Execute(ctx, "CREATE (:A {`B:c`:'right_only'})", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "CREATE (:A {`B:c`:'right_only'})", nil)
				require.Error(t, err, "same right tuple remains UNIQUE")
				require.Contains(t, statusText(err), "Neo.ClientError.Schema.ConstraintValidationFailed")

				for _, tc := range []struct{ query, value string }{
					{"MATCH (n:`A:B`) WHERE n.c='shared' RETURN n.c AS value", "shared"},
					{"MATCH (n:A) WHERE n.`B:c`='shared' RETURN n.`B:c` AS value", "shared"},
					{"MATCH (n:A) WHERE n.`B:c`='right_only' RETURN n.`B:c` AS value", "right_only"},
				} {
					result, readErr := e.Execute(ctx, tc.query, nil)
					require.NoError(t, readErr, tc.query)
					require.Equal(t, [][]interface{}{{tc.value}}, result.Rows, tc.query)
				}

				leftIndex, leftFound, leftErr := e.storage.GetSchema().GetPropertyIndex(ctx, "A:B", "c")
				rightIndex, rightFound, rightErr := e.storage.GetSchema().GetPropertyIndex(ctx, "A", "B:c")
				require.NoError(t, leftErr)
				require.NoError(t, rightErr)
				require.True(t, leftFound)
				require.True(t, rightFound)
				require.NotSame(t, leftIndex, rightIndex, "filled equality indexes must retain exact tuple identity")
				require.Equal(t, "A:B", leftIndex.Label)
				require.Equal(t, []string{"c"}, leftIndex.Properties)
				require.Equal(t, "A", rightIndex.Label)
				require.Equal(t, []string{"B:c"}, rightIndex.Properties)
			})
			t.Run("node_does_not_constrain_relationships", func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
				require.NoError(t, err)
				fixtureowner.Own(t, b)
				t.Cleanup(func() { _ = b.Close() })
				e := NewStorageExecutor(storage.NewNamespacedEngine(b, "identity"))
				ctx := context.Background()
				for _, q := range []string{"CREATE CONSTRAINT node_only FOR (n:Shared) REQUIRE n.code IS UNIQUE", "CREATE (:Shared {code:'same'})", "CREATE ()-[:Shared {code:'same'}]->()", "CREATE ()-[:Shared {code:'same'}]->()"} {
					_, err = e.Execute(ctx, q, nil)
					require.NoError(t, err, q)
				}
				_, err = e.Execute(ctx, "CREATE (:Shared {code:'same'})", nil)
				require.Error(t, err)
			})

			t.Run("relationship_does_not_constrain_nodes", func(t *testing.T) {
				b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngineInMemory())
				require.NoError(t, err)
				fixtureowner.Own(t, b)
				t.Cleanup(func() { _ = b.Close() })
				e := NewStorageExecutor(storage.NewNamespacedEngine(b, "identity"))
				ctx := context.Background()
				_, err = e.Execute(ctx, "CREATE CONSTRAINT rel_only FOR ()-[r:Shared]-() REQUIRE r.code IS UNIQUE", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "CREATE (:Shared {code:'same'})", nil)
				require.NoError(t, err)
				_, err = e.Execute(ctx, "CREATE (:Shared {code:'same'})", nil)
				assert.NoError(t, err, "relationship UNIQUE must allow duplicate node values")
				_, err = e.Execute(ctx, "CREATE ()-[:Shared {code:'same'}]->()", nil)
				require.NoError(t, err, "node and relationship values are independent")
				_, err = e.Execute(ctx, "CREATE ()-[:Shared {code:'same'}]->()", nil)
				require.Error(t, err, "relationship UNIQUE remains enforced")
			})
			for _, drop := range []string{"node_same", "rel_same"} {
				t.Run("coexist_drop_"+drop, func(t *testing.T) {
					path := t.TempDir()
					b, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(path))
					require.NoError(t, err)
					fixtureowner.Own(t, b)
					t.Cleanup(func() { _ = b.Close() })
					target := storage.NewNamespacedEngine(b, "identity")
					e := NewStorageExecutor(target)
					ctx := context.Background()
					for _, q := range []string{"CREATE CONSTRAINT node_same FOR (n:Shared) REQUIRE n.code IS UNIQUE", "CREATE CONSTRAINT rel_same FOR ()-[r:Shared]-() REQUIRE r.code IS UNIQUE", "CREATE (:Shared {code:'same'})", "CREATE ()-[:Shared {code:'same'}]->()"} {
						_, err = e.Execute(ctx, q, nil)
						require.NoError(t, err, q)
					}
					_, err = e.Execute(ctx, "DROP CONSTRAINT "+drop, nil)
					require.NoError(t, err)
					definition := schemaDefinitionForTest(t, target.GetSchema())
					require.Len(t, definition.Constraints, 1, "only the other entity's rule remains")
					require.NoError(t, b.Close())
					reopened, err := fixtureowner.TrackResult[*storage.BadgerEngine](t, storage.ErrStorageClosed)(storage.NewBadgerEngine(path))
					require.NoError(t, err)
					fixtureowner.Own(t, reopened)
					t.Cleanup(func() { _ = reopened.Close() })
					target = storage.NewNamespacedEngine(reopened, "identity")
					e = NewStorageExecutor(target)
					require.Equal(t, definition, schemaDefinitionForTest(t, target.GetSchema()), "ordinary persisted schema survives reopen without a format migration")
					nodeWrite := "CREATE (:Shared {code:'same'})"
					relWrite := "CREATE ()-[:Shared {code:'same'}]->()"
					allowed, rejected := nodeWrite, relWrite
					if drop == "rel_same" {
						allowed, rejected = relWrite, nodeWrite
					}
					_, err = e.Execute(ctx, allowed, nil)
					require.NoError(t, err, "dropping one entity's rule releases only that entity")
					_, err = e.Execute(ctx, rejected, nil)
					require.Error(t, err, "the other entity's UNIQUE rule remains enforced after reopen")
					for _, q := range []string{"MATCH (n:Shared) RETURN n.code AS code", "MATCH ()-[r:Shared]->() RETURN r.code AS code"} {
						result, readErr := e.Execute(ctx, q, nil)
						require.NoError(t, readErr)
						require.NotEmpty(t, result.Rows)
					}
				})
			}

		})
	}
}
