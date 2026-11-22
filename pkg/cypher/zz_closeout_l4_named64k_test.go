package cypher

// v1.5.2 close-out L4, CL4-NEW-NAMED-64K, Cypher side: Cypher has no way to
// write named embeddings (vectors set from Cypher are ordinary properties,
// call_compat.go:715 callSetVectorProperty). This sweep shows what a client
// sees when one node carries k vector properties of dimension 2,048 through
// CREATE, SET and db.create.setNodeVectorProperty, on the server's stack.

import (
	"fmt"
	"strings"
	"testing"
)

func TestCloseoutL4Named64KCypherVectorProperties(t *testing.T) {
	const dims = 2048
	vector := make([]float64, dims)
	for j := range vector {
		vector[j] = float64((j+3)%97) / 97
	}
	for _, route := range []string{"create", "set", "setNodeVectorProperty"} {
		firstFail := 0
		outcomes := []string{}
		for k := 1; k <= 16; k++ {
			disk := l4OpenDisk(t, t.TempDir())
			exec := NewStorageExecutor(disk.engine)
			var err error
			switch route {
			case "create":
				props := make([]string, k)
				for i := range props {
					props[i] = fmt.Sprintf("v%d: $v", i)
				}
				_, err = exec.Execute(t.Context(), "CREATE (:VecDoc {"+strings.Join(props, ", ")+"})", map[string]any{"v": vector})
			case "set":
				l4Run(t, exec, "CREATE (:VecDoc {id: 1})", nil)
				for i := 0; i < k && err == nil; i++ {
					_, err = exec.Execute(t.Context(), fmt.Sprintf("MATCH (n:VecDoc {id: 1}) SET n.v%d = $v", i), map[string]any{"v": vector})
				}
			case "setNodeVectorProperty":
				l4Run(t, exec, "CREATE (:VecDoc {id: 1})", nil)
				for i := 0; i < k && err == nil; i++ {
					_, err = exec.Execute(t.Context(), fmt.Sprintf("MATCH (n:VecDoc {id: 1}) CALL db.create.setNodeVectorProperty(n, 'v%d', $v)", i), map[string]any{"v": vector})
				}
			}
			disk.close()
			if err == nil {
				outcomes = append(outcomes, fmt.Sprintf("k=%d ok", k))
				continue
			}
			msg := err.Error()
			if len(msg) > 160 {
				msg = msg[:160]
			}
			outcomes = append(outcomes, fmt.Sprintf("k=%d error: %s", k, msg))
			if firstFail == 0 {
				firstFail = k
			}
			if k >= firstFail+1 {
				break
			}
		}
		l4Report(t, "CL4-NEW-NAMED-64K", "cypher-vector-properties/"+route, true, map[string]any{"dims": dims, "firstFailingCount": firstFail, "outcomes": outcomes})
	}
}
