# mkoracle.awk matrix.cypher neo.norm > oracle.json
# JSON array: {"query": statement, "output": [cypher-shell plain lines]}; the
# "RETURN n AS stmt" markers align the two files and are not written.
function esc(s,   o, i, c) { o = ""; for (i = 1; i <= length(s); i++) { c = substr(s, i, 1); if (c == "\\" || c == "\"") o = o "\\" c; else o = o c } return o }
FNR == 1 { f++ }
f == 1 && (/^\/\// || $0 == "") { next }
f == 1 && /^RETURN [0-9]+ AS stmt;$/ { split($0, a, " "); cur = a[2]; next }
f == 1 { q = $0; sub(/;$/, "", q); query[cur] = q; n = cur; next }
f == 2 && $0 == "stmt" { getline num; cur = num; next }
f == 2 { cnt[cur]++; line[cur, cnt[cur]] = $0 }
END {
  print "["
  for (i = 1; i <= n; i++) {
    lines = ""
    for (j = 1; j <= cnt[i]; j++) lines = lines (j > 1 ? ", " : "") "\"" esc(line[i, j]) "\""
    printf "  {\"query\": \"%s\", \"output\": [%s]}%s\n", esc(query[i]), lines, (i < n ? "," : "")
  }
  print "]"
}
