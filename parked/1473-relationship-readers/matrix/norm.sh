#!/bin/bash
# norm.sh neo <neo.log>          -> comparable stream from a tnrun cypher log
# norm.sh go <go.log> <A|X> <n>  -> stream of mode A/X from the n-th parser section (1 default, 2 antlr) of a tnrun go log
case "$1" in
neo) awk '/^tnrun|^database wiped|^---$|^exit [0-9]/{next}
  /^Plan: "EXPLAIN"/{print "<explain ok>"; next}
  /^(Plan|Statement|Version|Planner|Runtime|Time|DbHits|Rows|Memory \(Bytes\)): /{next} {print}' "$2";;
go) awk -v M="$3|" -v N="$4" '/=== RUN   TestZZRelInvMatrix$/{sec++} sec==N && index($0,M)==1 {print substr($0,3)}' "$2";;
esac
