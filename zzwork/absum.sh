#!/bin/bash
# Median ns/op, B/op, allocs/op per benchmark and side from a tnrun ab raw out.txt; delta head vs base.
awk '
/^=== round [0-9]+ (base|head)$/ {side=$4; next}
/^Benchmark/ && side!="" {
  name=$1; sub(/-[0-9]+$/,"",name)
  for(i=3;i<=NF;i++){ if($(i+1)=="ns/op") v["ns",side,name]=v["ns",side,name] " " $i;
    if($(i+1)=="B/op") v["B",side,name]=v["B",side,name] " " $i;
    if($(i+1)=="allocs/op") v["al",side,name]=v["al",side,name] " " $i }
  names[name]=1 }
function med(s,  a,n,i,j,t){n=split(s,a," "); for(i=1;i<=n;i++)for(j=i+1;j<=n;j++)if(a[j]+0<a[i]+0){t=a[i];a[i]=a[j];a[j]=t}; if(n==0)return -1; return (n%2)?a[(n+1)/2]:(a[n/2]+a[n/2+1])/2}
END{ printf "%-78s %12s %12s %8s | %8s %8s | %7s %7s\n","benchmark","base ns","head ns","delta","B base","B head","al base","al head"
  for(n in names){ b=med(v["ns","base",n]); h=med(v["ns","head",n]); if(b<=0||h<0)continue
    printf "%-78s %12.0f %12.0f %+7.1f%% | %8.0f %8.0f | %7.0f %7.0f\n", substr(n,1,78), b, h, 100*(h-b)/b, med(v["B","base",n]), med(v["B","head",n]), med(v["al","base",n]), med(v["al","head",n]) } }' "$1" | sort
