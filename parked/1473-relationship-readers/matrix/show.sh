#!/bin/bash
# show.sh <a.norm> <b.norm> <stmt>...: print the statement and both outputs
A=$1; B=$2; shift 2
for s in "$@"; do
  echo "#### $s: $(awk -v S="$s" 'BEGIN{n=0} /^\/\//{next} {if ($0 ~ /^RETURN [0-9]+ AS stmt;$/) {split($0,a," "); cur=a[2]; next} if (cur==S) print}' matrix.cypher | cut -c1-260)"
  for f in "$A" "$B"; do echo "  [$f]"; awk -v S="$s" '/^stmt$/{getline n; cur=n; next} cur==S{print "    " $0}' "$f" | cut -c1-260; done
done
