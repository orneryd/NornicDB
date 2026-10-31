// Author: Claude, NOR-180 one MERGE retry decision on every protocol, 2026-10-31.
// A MERGE-only transaction that loses a commit-time UNIQUE race gets the same
// retryable status on every public path, and the retried MERGE finds the
// winner's node, as a racing MERGE does in Neo4j. Uses #1655's public clients.
package storage_test

import (
	"context"
	"fmt"
	"testing"

	nornicerrors "github.com/orneryd/nornicdb/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMergeCommitRaceIsRetryableOnEveryProtocol(t *testing.T) {
	// Applying the constrained label after MERGE lets both explicit
	// transactions stage the new key before either commits.
	merge := func(by string) string {
		return "MERGE (n {id: 'claimed'}) SET n:RaceDoc, n.by = '" + by + "'"
	}
	for _, wb := range []bool{false, true} {
		for _, route := range []string{"Execute", "Bolt", "HTTP"} {
			t.Run(fmt.Sprintf("wb=%v/%s", wb, route), func(t *testing.T) {
				srv, fault := openPR1655ProtocolFixture(t, wb)
				setup := pr1655OpenPublicClient(t, srv, route)
				_, err := setup.statement("CREATE CONSTRAINT race_id FOR (n:RaceDoc) REQUIRE n.id IS UNIQUE", false)
				require.NoError(t, err)
				loser, peer := pr1655OpenPublicClient(t, srv, route), pr1655OpenPublicClient(t, srv, route)
				for _, staged := range []struct {
					client *pr1655PublicClient
					by     string
				}{{loser, "loser"}, {peer, "peer"}} {
					require.NoError(t, staged.client.begin())
					_, err = staged.client.run(merge(staged.by))
					require.NoError(t, err, "both transactions stage the new key")
				}
				require.NoError(t, peer.commit())
				err = loser.commit()
				require.Error(t, err, "the later commit loses the race")
				require.Equal(t, nornicerrors.TransientOutdated, pr1655PublicStatus(err), "commit error: %v", err)

				_, err = loser.statement(merge("retry"), true)
				require.NoError(t, err, "the retried MERGE finds the winner's node")
				rows, err := setup.statement("MATCH (n:RaceDoc {id: 'claimed'}) RETURN n.by", false)
				require.NoError(t, err)
				require.Equal(t, [][]interface{}{{"retry"}}, rows, "one node, written by the retry")
				require.False(t, fault.fired.Load(), "no corruption fixture was armed")
				require.NoError(t, fault.engine.FlushWriteBehind(context.Background()))
			})
		}
	}
}

// The decision rests on the statement execution accepted, for every accepted
// spelling: a grouped numeric literal the grammar can't read until execution
// rewrites it.
func TestMergeCommitRaceDecidesOnAcceptedStatements(t *testing.T) {
	merge := "MERGE (n {id: 'claimed'}) SET n:RaceDoc, n.by = '%s'"
	parameterized := "MERGE (n {id: $id}) SET n:RaceDoc, n.by = $by, n.amount = 1.0_0"
	for _, route := range []string{"Execute", "Bolt", "HTTP"} {
		for _, tc := range []struct {
			name, companion, loserMerge string
			want                        string
		}{
			// The record is what executes: the statement after USE, under its
			// internal parameter name, with its effective bindings.
			{"use_routed_merge_retries", "", "USE nornic MERGE (n {id: 'claimed'}) SET n:RaceDoc, n.by = 'loser'", nornicerrors.TransientOutdated},
			// A companion counts only on a read-only proof from what it executes.
			{"show_continuation_write_stays_hard", "CYPHER 25 SHOW FUNCTIONS YIELD name WITH name WHERE name = 'abs' CREATE (:Audit) RETURN name", fmt.Sprintf(merge, "loser"), nornicerrors.ConstraintValidationFailed},
			{"plain_show_is_read_only", "SHOW FUNCTIONS", fmt.Sprintf(merge, "loser"), nornicerrors.TransientOutdated},
			{"quoted_parameter_merge_retries", "", "MERGE (n {id: 'claimed'}) SET n:RaceDoc, n += $`patch values`", nornicerrors.TransientOutdated},
			{"mixed_write_stays_hard", "CREATE (:Audit {amount: 1.0_0})", fmt.Sprintf(merge, "loser"), nornicerrors.ConstraintValidationFailed},
			{"rewritten_parameterized_merge_retries", "", parameterized, nornicerrors.TransientOutdated},
		} {
			t.Run(route+"/"+tc.name, func(t *testing.T) {
				srv, _ := openPR1655ProtocolFixture(t, false)
				setup := pr1655OpenPublicClient(t, srv, route)
				_, err := setup.statement("CREATE CONSTRAINT race_id FOR (n:RaceDoc) REQUIRE n.id IS UNIQUE", false)
				require.NoError(t, err)
				loser, peer := pr1655OpenPublicClient(t, srv, route), pr1655OpenPublicClient(t, srv, route)
				require.NoError(t, loser.begin())
				if tc.companion != "" {
					_, err = loser.run(tc.companion)
					require.NoError(t, err, "execution accepts the rewritten spelling")
				}
				loser.setParams(map[string]interface{}{"id": "claimed", "by": "loser", "patch values": map[string]interface{}{"by": "loser"}})
				_, err = loser.run(tc.loserMerge)
				require.NoError(t, err)
				loser.setParams(nil)
				require.NoError(t, peer.begin())
				_, err = peer.run(fmt.Sprintf(merge, "peer"))
				require.NoError(t, err)
				require.NoError(t, peer.commit())
				err = loser.commit()
				require.Error(t, err, "the later commit loses the race")
				require.Equal(t, tc.want, pr1655PublicStatus(err), "commit error: %v", err)
			})
		}
	}
}

// Shell parameters are effective bindings: the record has the map execution reads.
func TestMergeCommitRaceReadsSessionParameters(t *testing.T) {
	srv, _ := openPR1655ProtocolFixture(t, false)
	setup := pr1655OpenPublicClient(t, srv, "Execute")
	_, err := setup.statement("CREATE CONSTRAINT race_id FOR (n:RaceDoc) REQUIRE n.id IS UNIQUE", false)
	require.NoError(t, err)
	loser, peer := pr1655OpenPublicClient(t, srv, "Execute"), pr1655OpenPublicClient(t, srv, "Execute")
	_, err = loser.run(":param patch => {by: 'loser'}")
	require.NoError(t, err)
	require.NoError(t, loser.begin())
	_, err = loser.run("MERGE (n {id: 'claimed'}) SET n:RaceDoc, n += $patch")
	require.NoError(t, err)
	require.NoError(t, peer.begin())
	_, err = peer.run("MERGE (n {id: 'claimed'}) SET n:RaceDoc, n.by = 'peer'")
	require.NoError(t, err)
	require.NoError(t, peer.commit())
	err = loser.commit()
	require.Error(t, err, "the later commit loses the race")
	require.Equal(t, nornicerrors.TransientOutdated, pr1655PublicStatus(err), "commit error: %v", err)
}
