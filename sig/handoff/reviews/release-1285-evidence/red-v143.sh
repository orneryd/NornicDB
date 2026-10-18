#!/bin/bash
set -euo pipefail
bash sig/tnrun.sh go wt-release-1285-v143 --parser default -- ./pkg/storage -run 'Test(DeleteNode_RemovesEverySeparatelyStoredEmbeddingKey|UpdateNode_ToInlineRemovesEverySeparatelyStoredEmbeddingKey)$' -count=1 -v
