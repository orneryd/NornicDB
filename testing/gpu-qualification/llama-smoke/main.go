//go:build linux && cuda && cgo

// llama-smoke checks the public local model adapter against a real CUDA device.
package main

/*
#cgo CFLAGS: -I/usr/local/cuda/include
#cgo LDFLAGS: -L/usr/local/cuda/lib64 -lcudart
#include <cuda_runtime_api.h>
static int gpu_free_bytes(size_t *free_bytes, size_t *total_bytes) {
    if (cudaSetDevice(0) != cudaSuccess) return 0;
    return cudaMemGetInfo(free_bytes, total_bytes) == cudaSuccess;
}
*/
import "C"

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"

	"github.com/orneryd/nornicdb/pkg/localllm"
)

func main() {
	modelPath := flag.String("model", "", "BGE-M3 GGUF path")
	output := flag.String("output", "", "optional JSON GPU embedding for comparison with a pinned baseline")
	gpuLayers := flag.Int("gpu-layers", -1, "GPU layer count; positive values exercise partial CPU/GPU offload")
	flag.Parse()
	if err := qualify(*modelPath, *output, *gpuLayers); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func qualify(path, output string, gpuLayers int) error {
	if path == "" {
		return fmt.Errorf("--model is required")
	}
	var before, after, total C.size_t
	if C.gpu_free_bytes(&before, &total) == 0 {
		return fmt.Errorf("CUDA device 0 is unavailable; CPU fallback does not qualify")
	}
	opts := localllm.DefaultOptions(path)
	opts.ContextSize, opts.BatchSize, opts.Threads = 128, 128, 2
	opts.GPULayers = gpuLayers
	model, err := localllm.LoadModel(opts)
	if err != nil {
		return err
	}
	const input = "Qualify native CUDA embeddings across buffer boundaries."
	vectors, err := model.EmbedBatch(context.Background(), []string{input, input})
	if err == nil && C.gpu_free_bytes(&after, &total) == 0 {
		err = fmt.Errorf("cannot measure CUDA model residency")
	}
	if err == nil && (after >= before || uint64(before-after) < 128<<20) {
		err = fmt.Errorf("model did not retain at least 128 MiB on CUDA device 0: before=%d after=%d", uint64(before), uint64(after))
	}
	_ = model.Close()
	if err != nil {
		return err
	}
	if len(vectors) != 2 {
		return fmt.Errorf("missing GPU embeddings")
	}
	for _, values := range vectors {
		if len(values) != 1024 {
			return fmt.Errorf("BGE-M3 returned %d dimensions, expected 1024", len(values))
		}
		var squaredNorm float64
		for _, value := range values {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return fmt.Errorf("non-finite GPU embedding")
			}
			squaredNorm += float64(value) * float64(value)
		}
		if math.Abs(squaredNorm-1) > 0.01 {
			return fmt.Errorf("GPU embedding squared norm=%g", squaredNorm)
		}
	}
	for i, value := range vectors[0] {
		if math.Abs(float64(value-vectors[1][i])) > 1e-4 {
			return fmt.Errorf("identical inputs produced different GPU embeddings")
		}
	}
	if output != "" {
		data, err := json.Marshal(vectors[0])
		if err != nil {
			return err
		}
		if err := os.WriteFile(output, data, 0600); err != nil {
			return err
		}
	}
	opts.GPULayers = 0
	cpu, err := localllm.LoadModel(opts)
	if err != nil {
		return err
	}
	defer cpu.Close()
	reference, err := cpu.Embed(context.Background(), input)
	if err != nil {
		return err
	}
	if len(reference) != len(vectors[0]) {
		return fmt.Errorf("CPU/GPU dimensions differ")
	}
	var agreement float64
	for i, value := range reference {
		agreement += float64(value) * float64(vectors[0][i])
	}
	if agreement < 0.999 || math.IsNaN(agreement) {
		return fmt.Errorf("CPU/GPU embedding agreement=%g", agreement)
	}
	fmt.Printf("CUDA model residency=%d MiB; dimensions=1024; normalized and deterministic; CPU/GPU agreement=%.6f\n", uint64(before-after)>>20, agreement)
	return nil
}
