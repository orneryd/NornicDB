package cyphertext

import (
	"reflect"
	"testing"
)

func TestConstraintEntryBoundariesPreservePatternTokens(t *testing.T) {
	for _, pattern := range []string{
		"(n)-[:R]->(:End)",
		"(n)-[:`A|B`]->(:`End|One`)",
		"(n)-[:`A``B`]->(:`End``One`)",
		"(n)-[:关系类型]->(:节点)",
		"(n)-[:`$(R)`]->(:`$(L)`)",
		"(n)-[:`A};\nB`]->(:`End}]\nOne`)",
	} {
		first, second := "COUNT { "+pattern+" } >= 1", "NOT EXISTS { "+pattern+" }"
		for _, separator := range []string{"\n", ";"} {
			entries, ok := SplitConstraintEntries(first + separator + second)
			if !ok || !reflect.DeepEqual(entries, []string{first, second}) {
				t.Errorf("pattern tokens changed: %q => %#v, valid=%v", pattern, entries, ok)
			}
		}
	}
	for _, body := range []string{"COUNT { (n)-[:R]->(:End) ", "n.x = 'unfinished", "n.x = `unfinished", "n.x = 1 }"} {
		if _, ok := SplitConstraintEntries(body); ok {
			t.Errorf("malformed existing contract enclosure accepted: %q", body)
		}
	}
}

func TestConstraintPatternNodeHeadsRetainAdmission(t *testing.T) {
	for _, example := range []struct {
		text, direction, relType string
		labels                   []string
	}{
		{"(n:Source:Extra)-[:R]->(:End)", "OUTGOING", "R", []string{"End"}},
		{"(`a)b`)<-[:`$(R)`]-(`c:d`:`A``B`)", "INCOMING", "$(R)", []string{"A`B"}},
		{"(n:`Source):Inner`)-[:`A|B`]->(m:`End):Part`:`$(L)`)", "OUTGOING", "A|B", []string{"End):Part", "$(L)"}},
		{"(关系:人物)-[:关系类型]->(\u2003节点:节点类型)", "OUTGOING", "关系类型", []string{"节点类型"}},
		{"(n)-[ : R\u2003 ]->(:End)", "OUTGOING", "R", []string{"End"}},
		{"(n)<-[:`A``]B`]-(:End)", "INCOMING", "A`]B", []string{"End"}},
		{"(n)-[:`A]B|C`]->(:End)", "OUTGOING", "A]B|C", []string{"End"}},
	} {
		pattern, ok := CompileConstraintPattern(example.text)
		if !ok || pattern.Direction() != example.direction || !pattern.MatchesType(example.relType) ||
			pattern.TargetLabelCount() != len(example.labels) || !pattern.MatchesTargetLabels(example.labels) {
			t.Errorf("node head changed: %q => %#v, valid=%v", example.text, pattern, ok)
		}
	}
	for _, text := range []string{
		"(n:A|B)-[:R]->(:End)", "(n {k: 1})-[:R]->(:End)",
		"(n WHERE n.k = 1)-[:R]->(:End)", "(:A:)-[:R]->(:End)",
		"(:A::B)-[:R]->(:End)", "(:$(x))-[:R]->(:End)",
		"(n)-[:R]->(:End) trailing", "(n)-[:R]->(:`End`garbage)",
		"(n)-[:R]->(:End", "(n)-[:R]->(:`End)",
		"(n)-[:9R]->(:End)", "(n)-[:\u20039R]->(:End)",
		"(n)-[:R|S]->(:End)", "(n)-[:R:S]->(:End)",
		"(n)-[:R {k: 1}]->(:End)", "(n)-[:R WHERE true]->(:End)",
		"(n)-[:$(x)]->(:End)", "(n)-[r:R]->(:End)",
		"(n)-[:``]->(:End)", "(n)-[:R // comment\n]->(:End)",
		"(n)-[:R]->(:\u20039End)",
	} {
		if _, ok := CompileConstraintPattern(text); ok {
			t.Errorf("unsupported constraint node head admitted: %q", text)
		}
	}
}
