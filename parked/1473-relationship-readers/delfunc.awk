# delfunc.awk -v NAMES="a,b" file.go : drop top-level funcs/types named in NAMES,
# with their directly preceding // comment block.
BEGIN { n = split(NAMES, a, ","); for (i = 1; i <= n; i++) want[a[i]] = 1 }
{ lines[NR] = $0 }
END {
  i = 1
  while (i <= NR) {
    l = lines[i]; name = ""
    if (match(l, /^func (\([^)]*\) )?[A-Za-z_][A-Za-z0-9_]*/)) { s = substr(l, RSTART, RLENGTH); sub(/^func (\([^)]*\) )?/, "", s); name = s }
    else if (match(l, /^type [A-Za-z_][A-Za-z0-9_]*/)) { s = substr(l, RSTART, RLENGTH); sub(/^type /, "", s); name = s }
    if (name != "" && (name in want)) {
      # drop preceding comment lines already emitted
      while (outn > 0 && out[outn] ~ /^\/\//) outn--
      if (l ~ /\{$/ || l ~ /\($/) { j = i + 1; while (j <= NR && lines[j] !~ /^[})]/) j++; i = j + 1 } else { i = i + 1 }
      # drop one following blank line
      if (i <= NR && lines[i] == "" && outn > 0 && out[outn] == "") i++
      continue
    }
    out[++outn] = l; i++
  }
  for (k = 1; k <= outn; k++) print out[k]
}
