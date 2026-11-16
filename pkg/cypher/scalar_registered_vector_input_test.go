package cypher

import (
	"context"
	"os"
	"os/exec"
	"testing"

	cypherfn "github.com/orneryd/nornicdb/pkg/cypher/fn"
	"github.com/orneryd/nornicdb/pkg/storage"
	"github.com/stretchr/testify/require"
)

func TestRegisteredInputPreservesDeclaredVectorScope(t *testing.T) {
	const child = "NORNIC_TEST_REGISTERED_VECTOR_INPUT"
	mode := os.Getenv(child)
	if mode == "" {
		for _, mode := range []string{"full", "argument", "metadata"} {
			command := exec.Command(os.Args[0], "-test.run=^TestRegisteredInputPreservesDeclaredVectorScope$", "-test.count=1")
			command.Env = append(os.Environ(), child+"="+mode)
			output, err := command.CombinedOutput()
			require.NoError(t, err, "%s: %s", mode, output)
		}
		return
	}
	image, name, query, want := cypherfn.FullInputImage, "toupper", "MATCH (n:EIBig) RETURN toUpper('unrelated')", int64(1)
	if mode != "full" {
		name, query = "id", "MATCH (n:EIBig) RETURN id(n)"
		image = cypherfn.ArgumentInputImage
		if mode == "metadata" {
			image, want = cypherfn.MetadataArgumentInputImage, 0
		}
	}
	cypherfn.Register(name, func(_ context.Context, scope cypherfn.Context, args []string) (interface{}, error) {
		if mode == "full" {
			value, err := scope.Eval("n")
			if err != nil {
				return nil, err
			}
			return int64(len(value.(*storage.Node).ChunkEmbeddings)), nil
		}
		value, err := scope.Eval(args[0])
		if err != nil {
			return nil, err
		}
		return int64(len(value.(*storage.Node).ChunkEmbeddings)), nil
	}, cypherfn.OpaqueReads, image)
	graph := newScalarNodeDemandGraph(t, true)
	result := graph.run(t, query)
	require.Equal(t, [][]interface{}{{want}}, result.Rows)
}
