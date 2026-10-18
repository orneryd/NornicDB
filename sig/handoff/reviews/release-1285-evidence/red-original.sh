#!/bin/bash
set -euo pipefail
bash sig/tnrun.sh go wt-release-1285-red --parser default -- ./pkg/storage -run 'Test(DeleteNode_RemovesEverySeparatelyStoredEmbeddingKey|UpdateNode_ToInlineRemovesEverySeparatelyStoredEmbeddingKey)$' -count=1 -v
