#!/bin/bash
# Extractor E6: per Go function in pkg/cypher (non-test), flag bodies that
#  (a) locate a bracket: '[' ']' "[" "]" "-[" "]-" "]->" "<-[" or a relationship-bracket helper, AND
#  (b) split the inside on relationship syntax: ':' '|' '*' '{' "*" ".." or "|" , AND
#  (c) mention relationships: rel/Rel/edge/Edge/relationship/Relationship/pattern/Pattern.
# Output: file:line funcname  [a-hits b-hits]
cd "$1"
for f in $(ls *.go | grep -v _test.go); do
  awk -v F="$f" '
  function flush() {
    if (name != "" && a>0 && b>0 && c>0) printf "%s:%d %s a=%d b=%d\n", F, start, name, a, b
  }
  /^func / { flush(); name=$0; sub(/\{.*$/, "", name); start=NR; a=0;b=0;c=0 }
  name != "" {
    line=$0
    if (line ~ /\x27\[\x27|\x27\]\x27|"\["|"\]"|"-\["|"\]-"|"\]->"|"<-\["|[Rr]elationshipBracket|findMatchingBracket|findMatchingDelimiter/) a++
    if (line ~ /\x27:\x27|\x27\|\x27|\x27\*\x27|\x27\{\x27|":"|"\|"|"\*"|"\.\."|"\{"/) b++
    if (line ~ /[Rr]el|[Ee]dge|[Pp]attern/) c++
  }
  END { flush() }' "$f"
done
