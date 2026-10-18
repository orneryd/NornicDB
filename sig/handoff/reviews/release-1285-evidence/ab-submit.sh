#!/bin/bash
set -euo pipefail
bash sig/tnrun.sh ab wt-release-1285-base wt-cp144-1285 --no-wait --rounds 10 -- ./pkg/storage -bench 'Benchmark(StreamNodes_Full|BadgerEngine_UpdateNode_MVCC|DeleteNode_SeparateEmbeddings|UpdateNodeEmbeddingSidecar)$' -benchtime=100ms
