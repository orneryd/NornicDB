#!/bin/bash
# generate matrix.cypher = template.txt per form (forms.tsv: name, label, type text, UNWIND type text) + extra.cypher;
# every statement is preceded by "RETURN <n> AS stmt;" so the Neo4j and NornicDB outputs align statement by statement.
cd "$(dirname "$0")"; shopt -u patsub_replacement 2>/dev/null; raw=$(mktemp); out=matrix.cypher; : > $out
while IFS=$'\t' read -r name lab x xu; do echo "// form $name" >> $raw; while IFS= read -r line; do l=${line//@L@/$lab}; l=${l//@XU@/$xu}; l=${l//@X@/$x}; echo "$l" >> $raw; done < template.txt; done < forms.tsv
cat extra.cypher >> $raw
n=0; while IFS= read -r line; do case "$line" in "//"*) echo "$line" >> $out;; *) n=$((n+1)); echo "RETURN $n AS stmt;" >> $out; echo "$line" >> $out;; esac; done < $raw
rm -f $raw
