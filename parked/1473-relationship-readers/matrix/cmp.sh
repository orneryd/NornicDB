#!/bin/bash
# cmp.sh <a.norm> <b.norm>: list statement numbers whose output differs
awk 'FNR==1{f++} /^stmt$/{getline n; cur=n; next} {out[f, cur]=out[f, cur] $0 "\n"; seen[cur]=1}
 END{for (s in seen) if (out[1,s]!=out[2,s]) print s}' "$1" "$2" | sort -n | tr '\n' ' '; echo
