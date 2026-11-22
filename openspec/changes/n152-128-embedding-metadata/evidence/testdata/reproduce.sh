#!/bin/bash
set -eu
export GOMAXPROCS=2 GOGC=50 GOMEMLIMIT=1500MiB
go test ./pkg/storage -run '^TestNOR330EmbeddingEncodeKeepsCallerEmbedMetaOwned$' -count=1 -p=1 -tags=noui,nolocalllm -v -timeout=180s
