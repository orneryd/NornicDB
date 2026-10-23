# Native inference qualification probe

`llama-smoke` exercises the public local model adapter on Linux with CUDA and a
real BGE-M3 GGUF. It requires CUDA residency, 1,024 finite normalized coordinates,
and deterministic results for identical inputs. Use `--gpu-layers 12` to exercise
partial CPU/GPU offload, or the default `-1` for full offload.

```sh
go run -tags 'cuda localllm' ./testing/gpu-qualification/llama-smoke \
  --model /path/to/bge-m3.gguf --gpu-layers 12 --output /tmp/bge-gpu.json
```

This is an unfinished qualification experiment, not a passing release gate.
Its provisional CPU/GPU cosine requirement of 0.999 currently fails with an
observed value of 0.9969246196888057 on the local v0.7.0 CUDA build. Residency,
normalization, dimensions, and deterministic-output checks passed in that run.
The old hosted v0.4.1 archive hit an illegal CPU instruction on this host before
a matching comparison could be collected. A portable old-version source build
is still needed to establish the same-backend baseline and interpret the
cross-backend difference. Do not infer a dependency regression or a quality pass
from the current experiment. The threshold remains unchanged pending that work.

The probe is excluded from ordinary builds by its `linux && cuda && cgo` build
constraint. Full-scale HNSW qualification is documented separately in
`docs/development/gpu-large-build-qualification.md`.
