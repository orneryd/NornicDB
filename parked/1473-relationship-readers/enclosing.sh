#!/bin/bash
# map "./file.go:LINE:..." hits to the enclosing top-level func (file:funcline funcname)
cd "$1"; shift
while IFS=: read -r f l rest; do
  f=${f#./}
  awk -v L="$l" -v F="$f" '/^func /{n=$0; sub(/\{.*$/,"",n); s=NR} NR==L{ if (n=="") print F":"L" <top-level var>"; else print F":"s" "n; exit}' "$f"
done
