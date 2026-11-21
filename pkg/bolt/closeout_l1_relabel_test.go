package bolt

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCloseoutTransientRelationshipAndTwoPeerRelabels(t *testing.T) {
	for _, control := range []bool{false, true} {
		t.Run(fmt.Sprintf("retained_relationship=%t", control), func(t *testing.T) {
			port, _ := nor213RelationshipStore(t, false)
			peer := openBoltTestConn(t, port)
			defer peer.Close()
			runBoltQueryWithParams(t, peer, "CREATE (:Probe {id:'x'}), (:Held {id:'y'}), (:Probe {id:'z'})", nil)
			if control {
				runBoltQueryWithParams(t, peer, "MATCH (z {id:'z'}), (y {id:'y'}) CREATE (z)-[:OTHER {id:'keep'}]->(y)", nil)
			}
			nor213RequireCountsMatchGraph(t, port, "before")
			old := openBoltTestConn(t, port)
			defer old.Close()
			nor213BoltBegin(t, old)
			runBoltQueryWithParams(t, old, "MATCH (y {id:'y'}) RETURN y.id", nil)
			runBoltQueryWithParams(t, peer, "MATCH (y {id:'y'}) REMOVE y:Held SET y:Middle", nil)
			runBoltQueryWithParams(t, peer, "MATCH (y {id:'y'}) REMOVE y:Middle SET y:After", nil)
			runBoltQueryWithParams(t, old, "MATCH (x {id:'x'}), (y {id:'y'}) CREATE (x)-[:OTHER {id:'transient'}]->(y)", nil)
			runBoltQueryWithParams(t, old, "MATCH ()-[r:OTHER {id:'transient'}]->() DELETE r", nil)
			require.NoError(t, nor213BoltCommitResult(t, old))
			nor213RequireCountsMatchGraph(t, port, "after commit")
		})
	}
}
