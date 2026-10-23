//go:build !cgovulkan

package vulkan

import (
	"math"
	"os"
	"os/exec"
	"reflect"
	"runtime"
	"testing"
)

// Grow between the Go binding and the native call. A uintptr cannot keep a
// Go stack allocation alive or follow it when the runtime moves the stack.
// efence frees old stacks instead of leaving stale addresses readable.
func TestPushConstantsSurviveStackGrowth(t *testing.T) {
	child := os.Getenv("NORNICDB_VULKAN_STACK_CHILD") == "1"
	if !IsAvailable() {
		if child {
			t.Fatal("Vulkan unavailable in native lifetime child")
		}
		t.Skip("Vulkan unavailable")
	}
	if !child {
		binary, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		command := exec.Command(binary, "-test.run=^TestPushConstantsSurviveStackGrowth$", "-test.v", "-test.timeout=2m")
		command.Env = append(os.Environ(), "NORNICDB_VULKAN_STACK_CHILD=1", "GODEBUG=efence=1")
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("native push-constant lifetime check: %v\n%s", err, output)
		}
		return
	}
	device, err := NewDevice(0)
	if err != nil {
		t.Fatal(err)
	}
	defer device.Release()
	compute, err := device.NewComputeContext()
	if err != nil {
		t.Fatal(err)
	}
	defer compute.Release()
	// Reflect preserves the binding's actual signature, allowing the same
	// regression to exercise both the old integer and corrected pointer ABI.
	binding := reflect.ValueOf(&vkCmdPushConstants).Elem()
	original := binding.Interface()
	native := reflect.ValueOf(original)
	binding.Set(reflect.MakeFunc(binding.Type(), func(arguments []reflect.Value) []reflect.Value {
		growPushConstantStack(16)
		runtime.GC()
		return native.Call(arguments)
	}))
	defer binding.Set(reflect.ValueOf(original))
	vector, err := device.NewBuffer([]float32{3, 4, 0})
	if err != nil {
		t.Fatal(err)
	}
	defer vector.Release()
	if err := compute.NormalizeVectorsGPU(vector, 1, 3); err != nil {
		t.Fatal(err)
	}
	values := vector.ReadFloat32(3)
	if len(values) != 3 || math.Abs(float64(values[0]-0.6)) > 0.001 || math.Abs(float64(values[1]-0.8)) > 0.001 {
		t.Fatalf("invalid normalized vector after stack growth: %v", values)
	}
	hits, err := compute.SearchGPU(vector, []float32{0.6, 0.8, 0}, 1, 3, 1, true)
	if err != nil || len(hits) != 1 || hits[0].Index != 0 || math.Abs(float64(hits[0].Score-1)) > 0.001 {
		t.Fatalf("invalid search after stack growth: %v, %v", hits, err)
	}
	scores, err := device.NewEmptyBuffer(1)
	if err != nil {
		t.Fatal(err)
	}
	defer scores.Release()
	indices, err := device.NewEmptyBuffer(1)
	if err != nil {
		t.Fatal(err)
	}
	defer indices.Release()
	topScores, err := device.NewEmptyBuffer(1)
	if err != nil {
		t.Fatal(err)
	}
	defer topScores.Release()
	if err := compute.HNSWBuildTopK(vector, vector, scores, indices, topScores, 1, 1, 3, 1); err != nil {
		t.Fatal(err)
	}
	ids, similarities := indices.ReadUint32(1), topScores.ReadFloat32(1)
	if len(ids) != 1 || ids[0] != 0 || len(similarities) != 1 || math.Abs(float64(similarities[0]-1)) > 0.001 {
		t.Fatalf("invalid HNSW candidates after stack growth: %v, %v", ids, similarities)
	}
}

//go:noinline
func growPushConstantStack(depth int) uint64 {
	var scratch [1024]uint64
	for i := range scratch {
		scratch[i] = uint64(i + depth)
	}
	var result uint64
	if depth > 0 {
		result = growPushConstantStack(depth - 1)
	}
	result += scratch[depth]
	runtime.KeepAlive(&scratch)
	return result
}
