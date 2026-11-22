package cypher

// NOR-120 tie investigation coverage: reproduction and contract pinning only.
// No production behaviour is changed by this file.
//
// The reported observation (Linear NOR-120, comments on release head
// 31208b17f, lab job i20261104230718-11424-6047, and on b098f7e70, lab job
// i20261104232037-24076-0e1a) is: "stable when repeated on one graph (30 of
// 30), but the same data loaded again gave 8 to 10 different top-2 answers in
// 12 loads, because ties fall to random node IDs".
//
// The supported contract in this checkout (d6ec7766e) is the one the fix
// commit 74e55097d wrote and the v1.4.2 changelog recorded: "Link prediction
// orders equal scores by target node ID, so results are stable between runs"
// (pkg/linkpredict/topology.go rankedBefore; CHANGELOG v1.4.2, "Fixed").
// The documented tie key is the target node ID, so the guarantee is keyed on
// the graph's node identities: repeats on one unchanged graph keep the same
// rows in the same order and the same top-K candidates, while two loads that
// give the same business nodes different node IDs are, for that key, two
// different graphs. Nothing in the shipped documentation (docs/features/
// link-prediction.md, docs/neo4j-migration/feature-parity.md) promises a tie
// order that survives a reload, and the procedures are NornicDB extensions
// with no GDS tie contract to inherit.
//
// The tests below therefore keep two kinds of statement apart:
//
//   - Contract assertions: repeated public queries on one unchanged graph
//     return the same rows, in the same order, with the same scores, and the
//     top-K cut keeps the same candidates; distinct scores rank, score and
//     count as documented; k=1, k above the candidate count, a source with no
//     candidates, an empty database and topK 0 behave as documented; graph and
//     database identity and the documented filters keep their rows.
//   - One observed pin, marked "OBSERVED PIN": the tie cut is resolved by the
//     internal node ID, so the same business graph loaded with a different ID
//     assignment selects different *business* candidates while selecting the
//     same *identifiers*. That is recorded as the current behaviour, not as a
//     requirement; whether the operator wants business-identity stability
//     across loads is a contract decision this file does not make.
//
// Every case runs through the public StorageExecutor.Execute on both configured
// statement paths (config.WithNornicParser and config.WithANTLRParser). The
// result cache is disabled for these executors, so each repeat re-executes the
// statement and rebuilds the graph instead of answering from a cache.

import (
	"context"
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/orneryd/nornicdb/pkg/config"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

// linkPredictionRepeatRuns is how often a repeat case re-executes one
// statement: the number of repeats the lab report ran on one graph.
const linkPredictionRepeatRuns = 30

// linkPredictionLane is one public statement path. The parser configuration is
// process-wide, so a lane is a subtest and never parallel.
type linkPredictionLane struct {
	name  string
	apply func() func()
}

func linkPredictionLanes() []linkPredictionLane {
	return []linkPredictionLane{
		{"nornic", config.WithNornicParser},
		{"antlr", config.WithANTLRParser},
	}
}

// linkPredictionExecutor is the procedures' public entry point with the
// database-scoped result cache disabled, so a repeat is a real re-execution.
func linkPredictionExecutor(engine storage.Engine) *StorageExecutor {
	return NewStorageExecutor(engine, QueryCachePolicy{MaxEntries: 0})
}

func linkPredictionNode(t *testing.T, engine storage.Engine, id string) {
	t.Helper()
	_, err := engine.CreateNode(t.Context(), &storage.Node{
		ID:         storage.NodeID(id),
		Labels:     []string{"P"},
		Properties: map[string]interface{}{"key": id},
	})
	require.NoError(t, err)
}

func linkPredictionEdge(t *testing.T, engine storage.Engine, id, from, to, kind string) {
	t.Helper()
	require.NoError(t, engine.CreateEdge(t.Context(), &storage.Edge{
		ID:        storage.EdgeID(id),
		StartNode: storage.NodeID(from),
		EndNode:   storage.NodeID(to),
		Type:      kind,
	}))
}

// linkPredictionStar builds the equal-score graph: a hub with the source and
// every spoke hanging off it, as pkg/linkpredict's star fixture does. The
// source is one leaf, so its two-hop candidates are the other leaves, and each
// of them ties with every other under all five scoring algorithms: one common
// neighbour (the hub) of one degree, a leaf degree of one, and a pair union of
// one over the two leaf neighbour sets.
//
// Every tie here is a single-term score, so repeated runs compute the same
// float rather than the same sum in a different order. That is what lets the
// repeat cases below assert exact row equality. A tie whose score is a sum of
// several terms (Adamic-Adar or resource allocation over three or more common
// neighbours of different degrees) is not bit-stable across runs in this
// checkout; that observation is reported to the parent as separate evidence and
// is deliberately not asserted here, because it cannot be pinned without
// depending on Go's map iteration order.
func linkPredictionStar(t *testing.T, database, source string, spokes []string) storage.Engine {
	t.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), database)
	linkPredictionNode(t, engine, "hub")
	linkPredictionNode(t, engine, source)
	linkPredictionEdge(t, engine, "hub-source", "hub", source, "R")
	for index, spoke := range spokes {
		linkPredictionNode(t, engine, spoke)
		linkPredictionEdge(t, engine, fmt.Sprintf("hub-%d", index), "hub", spoke, "R")
	}
	return engine
}

// linkPredictionHubDegree is the hub's degree in linkPredictionStar: the
// source plus one per spoke.
func linkPredictionHubDegree(spokes int) float64 {
	return float64(spokes + 1)
}

// linkPredictionDistinctGraph builds a graph whose three candidates have
// strictly decreasing scores under every algorithm:
//
//	s-K->a, s-K->b, s-K->c, a-x, b-x, c-x, a-y, b-y, a-z
//
// with candidate degrees x:3, y:2, z:1 and hub degrees a:4, b:3, c:2, so
// common neighbours are 3/2/1, the Jaccard overlap is 1, 2/3, 1/3, Adamic-Adar
// adds 1/ln(4)+1/ln(3)+1/ln(2), 1/ln(4)+1/ln(3), 1/ln(4), resource allocation
// 1/4+1/3+1/2, 1/4+1/3, 1/4, and preferential attachment 3*3, 3*2, 3*1.
func linkPredictionDistinctGraph(t *testing.T, database string) storage.Engine {
	t.Helper()
	engine := storage.NewNamespacedEngine(newTestMemoryEngine(t), database)
	for _, id := range []string{"s", "a", "b", "c", "x", "y", "z"} {
		linkPredictionNode(t, engine, id)
	}
	for index, edge := range [][3]string{
		{"s", "a", "K"}, {"s", "b", "K"}, {"s", "c", "K"},
		{"a", "x", "K"}, {"b", "x", "K"}, {"c", "x", "K"},
		{"a", "y", "K"}, {"b", "y", "K"},
		{"a", "z", "K"},
	} {
		linkPredictionEdge(t, engine, fmt.Sprintf("e%d", index), edge[0], edge[1], edge[2])
	}
	return engine
}

// linkPredictionAnswer is one statement's answer as the caller sees it: every
// row exactly as rendered, plus the predicted node and score columns.
type linkPredictionAnswer struct {
	rows    []string
	targets []string
	scores  []float64
}

func linkPredictionAnswerOf(t *testing.T, exec *StorageExecutor, query string) linkPredictionAnswer {
	t.Helper()
	result, err := exec.Execute(context.Background(), query, nil)
	require.NoError(t, err, query)
	answer := linkPredictionAnswer{
		rows:    make([]string, len(result.Rows)),
		targets: make([]string, len(result.Rows)),
		scores:  make([]float64, len(result.Rows)),
	}
	for index, row := range result.Rows {
		require.GreaterOrEqual(t, len(row), 3, query)
		answer.rows[index] = fmt.Sprint(row)
		target, ok := row[1].(string)
		require.True(t, ok, "%s: node2 is %T, not a rendered node id", query, row[1])
		score, ok := row[2].(float64)
		require.True(t, ok, "%s: score is %T, not a float64", query, row[2])
		answer.targets[index] = target
		answer.scores[index] = score
	}
	return answer
}

// TestLinkPredictionEqualScoresKeepOneOrderAndOneTopK is the contract case for
// an unchanged graph whose candidates all tie and whose top-K cuts inside the
// tie set: the answer, its order and its cut are the same on every repeat, and
// the cut keeps the candidates the documented key (target node ID) names.
func TestLinkPredictionEqualScoresKeepOneOrderAndOneTopK(t *testing.T) {
	const source = "s"
	spokes := []string{"a1", "a2", "a3", "a4"}
	exec := linkPredictionExecutor(linkPredictionStar(t, "ties", source, spokes))
	hubDegree := linkPredictionHubDegree(len(spokes))

	// Every spoke shares the hub and nothing else, so each algorithm gives all
	// four spokes the same raw score.
	topology := []struct {
		procedure string
		score     float64
	}{
		{"commonNeighbors", 1},
		{"jaccard", 1},
		{"adamicAdar", 1 / math.Log(hubDegree)},
		{"resourceAllocation", 1 / hubDegree},
		{"preferentialAttachment", 1},
	}
	// Equal scores are ordered by target node ID, so a top-2 cut inside this
	// four-way tie keeps the two lowest IDs.
	wantTopK := []string{"a1", "a2"}

	for _, lane := range linkPredictionLanes() {
		lane := lane
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()

			for _, procedure := range topology {
				query := fmt.Sprintf("CALL gds.linkPrediction.%s.stream({sourceNode: '%s', topK: 2})", procedure.procedure, source)
				first := linkPredictionAnswerOf(t, exec, query)
				require.Equal(t, wantTopK, first.targets, "%s: equal scores come in target node ID order", procedure.procedure)
				require.Len(t, first.scores, 2)
				for _, score := range first.scores {
					require.InDelta(t, procedure.score, score, 1e-12, procedure.procedure)
				}
				for run := 1; run < linkPredictionRepeatRuns; run++ {
					next := linkPredictionAnswerOf(t, exec, query)
					require.Equal(t, first.rows, next.rows,
						"%s run %d of %d on an unchanged graph: same rows, same order, same scores",
						procedure.procedure, run+1, linkPredictionRepeatRuns)
				}
			}

			for _, algorithm := range []string{"common_neighbors", "ensemble"} {
				query := fmt.Sprintf("CALL gds.linkPrediction.predict.stream({sourceNode: '%s', algorithm: '%s', topologyWeight: 1.0, semanticWeight: 0.0, topK: 2})", source, algorithm)
				first := linkPredictionAnswerOf(t, exec, query)
				require.Equal(t, wantTopK, first.targets, "predict.stream algorithm %q: equal scores come in target node ID order", algorithm)
				require.Len(t, first.scores, 2)
				require.Equal(t, first.scores[0], first.scores[1], "predict.stream algorithm %q: the tie is an exact tie", algorithm)
				for run := 1; run < linkPredictionRepeatRuns; run++ {
					next := linkPredictionAnswerOf(t, exec, query)
					require.Equal(t, first.rows, next.rows,
						"predict.stream algorithm %q run %d of %d on an unchanged graph: same rows, same order, same scores",
						algorithm, run+1, linkPredictionRepeatRuns)
				}
			}
		})
	}
}

// TestLinkPredictionSameBusinessDataAcrossLoads reconstructs the reported
// observation without any randomness: the same business star (one hub, one
// source and three tying spokes) is loaded once per assignment of the same
// three node IDs to the three business spokes, which is what a loader that
// generates its own IDs can produce on a reload. Every assignment is a separate
// database, so the loads share no state.
//
// Contract assertion per load: 30 repeats on one unchanged graph answer the
// same rows (the lab's 30 of 30).
// OBSERVED PIN: whichever business node an ID names, the top-2 cut keeps the
// two lowest IDs of the tie set, so the selected identifiers are the same on
// every load while the selected business nodes are not.
func TestLinkPredictionSameBusinessDataAcrossLoads(t *testing.T) {
	const source = "u-00"
	spokes := []string{"b1", "b2", "b3"}
	ids := []string{"u-01", "u-02", "u-03"}
	// Every assignment of the three IDs to the three spokes.
	assignments := [][3]int{
		{0, 1, 2}, {0, 2, 1}, {1, 0, 2}, {1, 2, 0}, {2, 0, 1}, {2, 1, 0},
	}

	// businessAnswer names the business spokes behind a load's answer.
	businessAnswer := func(assignment [3]int, loadIDs []string) []string {
		answer := make([]string, len(loadIDs))
		for index, id := range loadIDs {
			for spokeIndex, assigned := range assignment {
				if ids[assigned] == id {
					answer[index] = spokes[spokeIndex]
				}
			}
		}
		return answer
	}

	distinctAnswers := map[string]bool{}
	for index, assignment := range assignments {
		assignment := assignment
		loadIDs := []string{ids[assignment[0]], ids[assignment[1]], ids[assignment[2]]}
		engine := linkPredictionStar(t, fmt.Sprintf("load%d", index), source, loadIDs)
		exec := linkPredictionExecutor(engine)
		query := fmt.Sprintf("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s', topK: 2})", source)

		for _, lane := range linkPredictionLanes() {
			lane := lane
			t.Run(fmt.Sprintf("load %d %s=%s %s", index, spokes[0], loadIDs[0], lane.name), func(t *testing.T) {
				restore := lane.apply()
				defer restore()

				first := linkPredictionAnswerOf(t, exec, query)
				require.Len(t, first.targets, 2)
				for run := 1; run < linkPredictionRepeatRuns; run++ {
					next := linkPredictionAnswerOf(t, exec, query)
					require.Equal(t, first.rows, next.rows,
						"run %d of %d on one unchanged graph: the same rows, in the same order",
						run+1, linkPredictionRepeatRuns)
				}
				// Contract: the tie key is the target node ID, so the two lowest
				// IDs are the ones kept, on every repeat and every lane.
				require.Equal(t, []string{ids[0], ids[1]}, first.targets,
					"the top-2 cut keeps the two lowest target node IDs of the tie set")
				// OBSERVED PIN: those identifiers name different business nodes
				// depending on the assignment the load used, and in a different
				// rendered order when the assignment reverses them.
				answer := businessAnswer(assignment, first.targets)
				require.Equal(t, businessAnswer(assignment, []string{ids[0], ids[1]}), answer)
				pair := append([]string(nil), answer...)
				slices.Sort(pair)
				distinctAnswers[fmt.Sprint(pair)] = true
				t.Logf("business spokes %v given node IDs %v: selected node IDs %v, business spokes %v",
					spokes, loadIDs, first.targets, answer)
			})
		}
	}

	// OBSERVED PIN: the same business graph under the six possible ID
	// assignments takes three different business pairs (b1b2, b1b3, b2b3) and,
	// with them, three different rendered orders, which illustrates the identity-key mechanism in the report. It does
	// not replay its unavailable original graph or its 8-to-10-of-12 statistic.
	require.Equal(t, map[string]bool{"[b1 b2]": true, "[b1 b3]": true, "[b2 b3]": true}, distinctAnswers,
		"the selected identifiers are stable, the business nodes behind them are not")
}

// TestLinkPredictionDistinctScoresRankScoreAndCount is the contract case for
// known, distinct scores: the ranking, the raw scores and the row count are
// the documented ones, at a top-K inside the candidate set and above it.
func TestLinkPredictionDistinctScoresRankScoreAndCount(t *testing.T) {
	engine := linkPredictionDistinctGraph(t, "scores")
	exec := linkPredictionExecutor(engine)

	// x, y, z share 3, 2 and 1 common neighbours; those neighbours have
	// degrees 4+3+2, 4+3 and 4, so each algorithm's three scores are distinct.
	expectations := []struct {
		procedure      string
		high, mid, low float64
	}{
		{"commonNeighbors", 3, 2, 1},
		{"jaccard", 1, 2.0 / 3.0, 1.0 / 3.0},
		{"adamicAdar",
			1/math.Log(4) + 1/math.Log(3) + 1/math.Log(2),
			1/math.Log(4) + 1/math.Log(3),
			1 / math.Log(4)},
		{"resourceAllocation", 1.0/4 + 1.0/3 + 1.0/2, 1.0/4 + 1.0/3, 1.0 / 4},
		{"preferentialAttachment", 9, 6, 3},
	}

	for _, lane := range linkPredictionLanes() {
		lane := lane
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()

			for _, expectation := range expectations {
				cut := linkPredictionAnswerOf(t, exec, fmt.Sprintf(
					"CALL gds.linkPrediction.%s.stream({sourceNode: 's', topK: 2})", expectation.procedure))
				require.Equal(t, []string{"x", "y"}, cut.targets, "%s: ranking at topK 2", expectation.procedure)
				require.Len(t, cut.scores, 2)
				require.InDelta(t, expectation.high, cut.scores[0], 1e-12, expectation.procedure)
				require.InDelta(t, expectation.mid, cut.scores[1], 1e-12, expectation.procedure)

				above := linkPredictionAnswerOf(t, exec, fmt.Sprintf(
					"CALL gds.linkPrediction.%s.stream({sourceNode: 's', topK: 10})", expectation.procedure))
				require.Equal(t, []string{"x", "y", "z"}, above.targets, "%s: every candidate once topK is above the candidate count", expectation.procedure)
				require.Len(t, above.scores, 3)
				require.InDelta(t, expectation.high, above.scores[0], 1e-12, expectation.procedure)
				require.InDelta(t, expectation.mid, above.scores[1], 1e-12, expectation.procedure)
				require.InDelta(t, expectation.low, above.scores[2], 1e-12, expectation.procedure)
			}

			// The same rows through the procedure's declared YIELD columns.
			yielded, err := exec.Execute(context.Background(),
				"CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 2}) YIELD node2, score RETURN node2, score", nil)
			require.NoError(t, err)
			require.Equal(t, []string{"node2", "score"}, yielded.Columns)
			require.Len(t, yielded.Rows, 2)
			require.Equal(t, "x", yielded.Rows[0][0])
			require.Equal(t, "y", yielded.Rows[1][0])
			require.InDelta(t, 3.0, yielded.Rows[0][1], 1e-12)
			require.InDelta(t, 2.0, yielded.Rows[1][1], 1e-12)
		})
	}
}

// TestLinkPredictionTopKBoundaries is the contract case for the documented
// boundaries: k=1, a k above the candidate count, a source with no candidate at
// all, an empty database, and a topK below the documented range.
func TestLinkPredictionTopKBoundaries(t *testing.T) {
	const source = "s"
	star := linkPredictionExecutor(linkPredictionStar(t, "bounds", source, []string{"a1", "a2", "a3", "a4"}))

	// A database whose source has no relationship at all, and one other node.
	lonely := storage.NewNamespacedEngine(newTestMemoryEngine(t), "lonely")
	linkPredictionNode(t, lonely, source)
	linkPredictionNode(t, lonely, "iso")
	lonelyExec := linkPredictionExecutor(lonely)

	empty := linkPredictionExecutor(storage.NewNamespacedEngine(newTestMemoryEngine(t), "empty"))

	for _, lane := range linkPredictionLanes() {
		lane := lane
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()

			first := linkPredictionAnswerOf(t, star, fmt.Sprintf(
				"CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s', topK: 1})", source))
			require.Equal(t, []string{"a1"}, first.targets, "topK 1 keeps the first candidate in target node ID order")

			above := linkPredictionAnswerOf(t, star, fmt.Sprintf(
				"CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s', topK: 10})", source))
			require.Equal(t, []string{"a1", "a2", "a3", "a4"}, above.targets, "a topK above the candidate count returns every candidate")

			none := linkPredictionAnswerOf(t, lonelyExec, fmt.Sprintf(
				"CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s'})", source))
			require.Empty(t, none.rows, "a source with no two-hop candidate has no common-neighbour prediction")

			// Preferential attachment's candidate rule is every node that isn't a
			// neighbour (pkg/linkpredict TopologyScores), so the isolated node is
			// a candidate at score 0.
			attachment := linkPredictionAnswerOf(t, lonelyExec, fmt.Sprintf(
				"CALL gds.linkPrediction.preferentialAttachment.stream({sourceNode: '%s'})", source))
			require.Equal(t, []string{"iso"}, attachment.targets)
			require.InDelta(t, 0.0, attachment.scores[0], 1e-12)

			_, err := empty.Execute(context.Background(),
				fmt.Sprintf("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s'})", source), nil)
			require.ErrorContains(t, err, "does not exist in the graph", "a source outside an empty graph is refused")

			_, err = star.Execute(context.Background(),
				fmt.Sprintf("CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: '%s', topK: 0})", source), nil)
			require.ErrorContains(t, err, "must be within the range [1, 2147483647]", "topK 0 is outside the documented range")
		})
	}
}

// TestLinkPredictionGraphAndDatabaseIdentityAndFilters is the contract case for
// identity and filters: a named projection reads the graph it projected, two
// databases keep their own rows, and relationshipTypes and targetNodes narrow
// the candidates without leaking rows or reordering them.
//
// dbA: s-K->a-K->x and s-L->m-K->n, so the whole database has two tied
// candidates (x through a, n through m).
// dbB: the same node IDs s and a with s-K->a-K->z, so a row from dbA can only
// come from dbA.
func TestLinkPredictionGraphAndDatabaseIdentityAndFilters(t *testing.T) {
	dbA := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor120_dbA")
	for _, id := range []string{"s", "a", "x", "m", "n"} {
		linkPredictionNode(t, dbA, id)
	}
	linkPredictionEdge(t, dbA, "a1", "s", "a", "K")
	linkPredictionEdge(t, dbA, "a2", "a", "x", "K")
	linkPredictionEdge(t, dbA, "a3", "s", "m", "L")
	linkPredictionEdge(t, dbA, "a4", "m", "n", "K")
	execA := linkPredictionExecutor(dbA)

	dbB := storage.NewNamespacedEngine(newTestMemoryEngine(t), "nor120_dbB")
	for _, id := range []string{"s", "a", "z"} {
		linkPredictionNode(t, dbB, id)
	}
	linkPredictionEdge(t, dbB, "b1", "s", "a", "K")
	linkPredictionEdge(t, dbB, "b2", "a", "z", "K")
	execB := linkPredictionExecutor(dbB)

	// The same projection name in each database: one catalog per database.
	for _, project := range []struct {
		exec *StorageExecutor
		name string
	}{{execA, "gk"}, {execB, "gk"}} {
		_, err := project.exec.Execute(context.Background(),
			fmt.Sprintf("CALL gds.graph.project('%s', 'P', 'K')", project.name), nil)
		require.NoError(t, err)
		project := project
		t.Cleanup(func() {
			_, err := project.exec.Execute(context.Background(), fmt.Sprintf("CALL gds.graph.drop('%s')", project.name), nil)
			require.NoError(t, err)
		})
	}

	for _, lane := range linkPredictionLanes() {
		lane := lane
		t.Run(lane.name, func(t *testing.T) {
			restore := lane.apply()
			defer restore()

			whole := linkPredictionAnswerOf(t, execA, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 10})")
			require.Equal(t, []string{"n", "x"}, whole.targets, "the two tied candidates, in target node ID order")
			require.InDelta(t, 1.0, whole.scores[0], 1e-12)
			require.InDelta(t, 1.0, whole.scores[1], 1e-12)

			filtered := linkPredictionAnswerOf(t, execA, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 10, relationshipTypes: ['K']})")
			require.Equal(t, []string{"x"}, filtered.targets, "relationshipTypes K leaves n, whose only relationship is L, unreachable")

			projected := linkPredictionAnswerOf(t, execA, "CALL gds.linkPrediction.commonNeighbors.stream('gk', {sourceNode: 's', topK: 10})")
			require.Equal(t, []string{"x"}, projected.targets, "the projected graph carries the K relationships it projected")

			targeted := linkPredictionAnswerOf(t, execA, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 10, targetNodes: ['x', 'n']})")
			require.Equal(t, []string{"n", "x"}, targeted.targets, "targetNodes keeps the tied candidates in the same order")
			narrowed := linkPredictionAnswerOf(t, execA, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 10, targetNodes: ['n']})")
			require.Equal(t, []string{"n"}, narrowed.targets)

			other := linkPredictionAnswerOf(t, execB, "CALL gds.linkPrediction.commonNeighbors.stream({sourceNode: 's', topK: 10})")
			require.Equal(t, []string{"z"}, other.targets, "dbB's own graph is the only graph its procedure reads")
			otherProjected := linkPredictionAnswerOf(t, execB, "CALL gds.linkPrediction.commonNeighbors.stream('gk', {sourceNode: 's', topK: 10})")
			require.Equal(t, []string{"z"}, otherProjected.targets, "dbB's projection of the same name reads dbB")
			require.NotContains(t, otherProjected.targets, "x", "no row from dbA may appear in dbB's answer")
			require.NotContains(t, otherProjected.targets, "n", "no row from dbA may appear in dbB's answer")
		})
	}
}
