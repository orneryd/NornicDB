package bolt

// Close-out lane L7 (scratch branch only; test-side, not product code).
// L-H7-017: TestClusterJournalParity/ha-async/embedded/below failed under -race
// with the follower at {nodes:24 flagged:0 embedded:24} when paritySettle
// returned (it stops when the follower state has not changed for 3 s). This
// runs the same case, records the state paritySettle returns, then keeps
// reading the follower for up to 180 s and logs every change, to tell a late
// apply from a lost one.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestL7017HAAsyncEmbeddedBelowFollowerApply(t *testing.T) {
	const count = 24
	leader, follower := parityHAPair(t, "async")
	ctx := context.Background()
	label := "Emb"
	vectors := journalVectors(parityChunks, 0)
	for attempt := 0; attempt < 5 && leader.state(t, label).nodes < count; attempt++ {
		for i := 0; i < count; i++ {
			id := storage.NodeID(fmt.Sprintf("emb-%d", i))
			if _, err := leader.db.GetStorage().GetNode(ctx, id); err == nil {
				continue
			}
			_, err := leader.db.GetStorage().CreateNode(ctx, &storage.Node{
				ID: id, Labels: []string{"Emb"},
				Properties:      map[string]any{"text": "kept", "flag": false},
				ChunkEmbeddings: vectors,
				EmbedMeta:       map[string]any{"has_embedding": true, "chunk_count": parityChunks, "embedding_dimensions": journalDims, "embedding_model": "synthetic"},
			})
			if err != nil {
				t.Logf("seeding embedded node %d: %s", i, parityError(err))
			}
		}
		time.Sleep(time.Second)
	}
	seededLeader, seededFollower := paritySettle(t, leader, follower, label)
	require.Equal(t, parityState{nodes: count, embedded: count}, seededLeader)
	require.Equal(t, seededLeader, seededFollower, "the seed replicated")
	before := parityQuiet(t, leader, follower)

	start := time.Now()
	_, txErr := leader.lab.run("MATCH (n:Emb) SET n.flag = true", nil, false)
	committed := time.Since(start)
	onLeader, onFollower := paritySettle(t, leader, follower, label)
	settled := time.Since(start)
	t.Logf("L7_017 commit_returned_after=%s err=%s settle_returned_after=%s leader=%+v follower=%+v journals_before: %s",
		committed, parityError(txErr), settled, onLeader, onFollower, before)

	want := parityState{nodes: count, flagged: count, embedded: count}
	last := follower.state(t, label)
	reached := time.Duration(-1)
	if last == want {
		reached = time.Since(start)
	}
	for deadline := time.Now().Add(180 * time.Second); reached < 0 && time.Now().Before(deadline); {
		time.Sleep(250 * time.Millisecond)
		current := follower.state(t, label)
		if current != last {
			t.Logf("L7_017 follower changed at %s: %+v -> %+v", time.Since(start), last, current)
			last = current
		}
		if current == want {
			reached = time.Since(start)
		}
	}
	t.Logf("L7_017 RESULT settle_follower=%+v final_follower=%+v follower_reached_leader_after=%s journals_after: %s",
		onFollower, last, reached, parityQuiet(t, leader, follower))
	require.NoError(t, txErr)
	require.Equal(t, want, onLeader)
	require.Equal(t, want, last, "the follower eventually holds what the leader holds")
	require.Equal(t, want, onFollower, "the follower holds what the leader holds when paritySettle returns (the original assertion)")
}
