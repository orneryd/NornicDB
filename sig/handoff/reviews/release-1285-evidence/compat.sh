#!/bin/bash
set -euo pipefail
bash sig/tnrun.sh go wt-cp144-1285 --allow-dirty --parser default -- run ./release_1285_probe.go
